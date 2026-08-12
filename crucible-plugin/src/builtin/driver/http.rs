//! The HTTP driver plugin.

use std::{net::SocketAddr, time::Duration};

use crucible_core::{
    plan,
    schema::{ClauseDecl, ClauseShape, HeadPattern, OpSig, Param, ParamType},
    verdict::{Ack, Outcome},
};
use reqwest::{Client, Method, StatusCode};

use crate::{
    error::Error as PluginError,
    role::{Action, BoxFuture, Driver, DriverRuntime, Targeted},
};

/// How long a request waits before the caller is left in doubt.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The clause carrying a request payload.
const BODY: &str = "body";

/// Drives HTTP requests against a service.
pub struct Http {
    client: Client,
}

/// One request, in the terms this plugin runs it.
#[derive(Debug, PartialEq, Eq)]
pub struct Request {
    pub method: Method,
    pub service: String,
    pub path: String,
    pub body: Option<Vec<u8>>,
    /// The status the step says this answers with.
    pub expect: Option<plan::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("`{0}` is not an HTTP method")]
    Method(String),
    #[error("a request names a service and a path")]
    Arguments,
    #[error("`{0}` is not a path: a path starts with `/`")]
    Path(String),
    #[error("`{0}` takes no body")]
    Body(Method),
    #[error("a body carries {0:?}, whose millisecond count does not fit a `u64`")]
    Duration(Duration),
    #[error(transparent)]
    Client(#[from] reqwest::Error),
    #[error(transparent)]
    Encode(#[from] serde_json::Error),
}

impl From<Error> for PluginError {
    fn from(e: Error) -> Self {
        Self::new(Http::NAME, e)
    }
}

impl Http {
    /// A driver sharing one client, and so one connection pool, across the
    /// steps it runs.
    ///
    /// # Errors
    /// Errors if the HTTP client cannot be built.
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            client: Client::builder().timeout(REQUEST_TIMEOUT).build()?,
        })
    }
}

/// A body is authored as plan values and sent as a JSON object.
fn encode(fields: &[(String, plan::Value)]) -> Result<Vec<u8>, Error> {
    let map: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|(key, value)| Ok((key.clone(), json(value)?)))
        .collect::<Result<_, Error>>()?;
    Ok(serde_json::to_vec(&map)?)
}

fn json(value: &plan::Value) -> Result<serde_json::Value, Error> {
    let json = match value {
        plan::Value::Null => serde_json::Value::Null,
        plan::Value::Str(s) | plan::Value::Ident(s) => serde_json::Value::from(s.clone()),
        plan::Value::Int(n) => serde_json::Value::from(*n),
        plan::Value::Bool(b) => serde_json::Value::from(*b),
        plan::Value::Duration(d) => u64::try_from(d.as_millis())
            .map(serde_json::Value::from)
            .map_err(|_| Error::Duration(*d))?,
        plan::Value::List(items) => items
            .iter()
            .map(json)
            .collect::<Result<Vec<_>, Error>>()?
            .into(),
        plan::Value::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), json(value)?)))
                .collect::<Result<_, Error>>()?,
        ),
    };
    Ok(json)
}

impl Driver for Http {
    const NAME: &'static str = "http";
    type Action = Request;
    type Error = Error;

    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::action(HeadPattern::exact("POST"), request_params())
                .with_clause(ClauseDecl::new(BODY, ClauseShape::Block)),
            OpSig::action(HeadPattern::exact("GET"), request_params()),
            OpSig::action(HeadPattern::exact("DELETE"), request_params()),
        ]
    }

    fn bind(step: &plan::Step) -> Result<Self::Action, Self::Error> {
        // Exactly the operations `signatures` advertises, so this driver never
        // runs something it did not offer.
        let method = match step.operation.as_str() {
            "POST" => Method::POST,
            "GET" => Method::GET,
            "DELETE" => Method::DELETE,
            other => return Err(Error::Method(other.to_owned())),
        };
        let [service, path] = step.args.as_slice() else {
            return Err(Error::Arguments);
        };
        let (Some(service), Some(path)) = (service.as_service_ref(), path.as_str()) else {
            return Err(Error::Arguments);
        };
        // A relative path would concatenate onto the address and address some
        // other port entirely, so it is rejected here rather than sent.
        if !path.starts_with('/') {
            return Err(Error::Path(path.to_owned()));
        }
        if step.body.is_some() && !takes_body(&step.operation) {
            return Err(Error::Body(method));
        }
        Ok(Request {
            method,
            service: service.to_owned(),
            path: path.to_owned(),
            body: step.body.as_deref().map(encode).transpose()?,
            expect: step.expect.clone(),
        })
    }
}

impl DriverRuntime for Http {
    fn prepare(&self, step: &plan::Step) -> Result<Box<dyn Action>, PluginError> {
        Ok(Box::new(Call {
            request: Http::bind(step)?,
            client: self.client.clone(),
        }))
    }
}

/// Whether the named operation declares a `body` clause, so what this driver
/// accepts follows what it advertises.
fn takes_body(operation: &str) -> bool {
    Http::signatures()
        .iter()
        .filter(|sig| matches!(&sig.head, HeadPattern::Exact(name) if name == operation))
        .any(|sig| sig.clauses.iter().any(|clause| clause.keyword == BODY))
}

fn request_params() -> Vec<Param> {
    vec![
        Param::required("service", ParamType::ServiceRef),
        Param::required("path", ParamType::Path),
    ]
}

/// A bound request and the client that sends it.
struct Call {
    request: Request,
    client: Client,
}

impl Targeted for Call {
    fn kind(&self) -> &str {
        Http::NAME
    }

    fn target(&self) -> &str {
        &self.request.service
    }
}

impl Action for Call {
    fn run(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<Outcome, PluginError>> {
        Box::pin(async move { self.send(endpoint).await.map_err(PluginError::from) })
    }
}

impl Call {
    /// A transport failure is an outcome rather than an error: a scenario that
    /// could not reach the fleet is what a fault is meant to cause. A request
    /// that never became a request is not, so it errors instead of being
    /// reported as something the fleet did.
    async fn send(&self, endpoint: SocketAddr) -> Result<Outcome, Error> {
        let request = &self.request;
        let operation = format!("{} {}", request.method, request.path);
        let sent = request.body.clone().unwrap_or_default();
        let url = format!("http://{endpoint}{}", request.path);

        let mut builder = self.client.request(request.method.clone(), url);
        if let Some(body) = request.body.clone() {
            builder = builder
                .header("content-type", "application/json")
                .body(body);
        }

        let outcome = match builder.send().await {
            Ok(response) => {
                let ack = classify(response.status(), self.request.expect.as_ref());
                let body = response.bytes().await.unwrap_or_default();
                Outcome {
                    operation,
                    ack,
                    request: sent,
                    response: body.to_vec(),
                }
            }
            // Nothing was ever sent, so there is no outcome to report: saying
            // the fleet left this in doubt would count it towards a verdict.
            Err(e) if e.is_builder() => return Err(Error::Client(e)),
            // A refused connection never reached the service, so the request
            // definitively did not happen; anything else leaves it in doubt.
            Err(e) => Outcome {
                operation,
                ack: if e.is_connect() {
                    Ack::Rejected
                } else {
                    Ack::Unknown
                },
                request: sent,
                response: e.to_string().into_bytes(),
            },
        };
        Ok(outcome)
    }
}

/// Whether the response is what the step said it would be.
///
/// A step that states its status is held to that one, so a request written to
/// be refused counts as delivered when it is refused, and as a failure when it
/// succeeds. Without a stated status the step is held to the protocol: a
/// success delivers, a client error does not, and a server error leaves the
/// caller unable to tell.
fn classify(status: StatusCode, stated: Option<&plan::Value>) -> Ack {
    match stated.and_then(plan::Value::as_int) {
        Some(expected) if i64::from(status.as_u16()) == expected => Ack::Acked,
        Some(_) => Ack::Rejected,
        None if status.is_success() => Ack::Acked,
        None if status.is_client_error() => Ack::Rejected,
        None => Ack::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::{Ack, Error, Http, Request, StatusCode, classify};
    use crate::role::Driver;
    use crucible_core::{
        plan,
        schema::{ClauseShape, HeadPattern},
    };
    use reqwest::Method;

    fn step(operation: &str, args: Vec<plan::Value>) -> plan::Step {
        plan::Step {
            driver: "http".into(),
            operation: operation.into(),
            args,
            body: None,
            expect: None,
        }
    }

    fn post_to(service: &str, path: &str) -> plan::Step {
        step(
            "POST",
            vec![
                plan::Value::Ident(service.into()),
                plan::Value::Str(path.into()),
            ],
        )
    }

    #[test]
    fn a_stated_status_is_what_the_step_is_held_to() {
        let stated = Some(plan::Value::Int(409));
        assert_eq!(classify(StatusCode::CONFLICT, stated.as_ref()), Ack::Acked);
        assert_eq!(
            classify(StatusCode::CREATED, stated.as_ref()),
            Ack::Rejected
        );
    }

    #[test]
    fn an_unstated_status_is_held_to_the_protocol() {
        assert_eq!(classify(StatusCode::CREATED, None), Ack::Acked);
        assert_eq!(classify(StatusCode::CONFLICT, None), Ack::Rejected);
        assert_eq!(classify(StatusCode::BAD_GATEWAY, None), Ack::Unknown);
    }

    #[test]
    fn exposes_post_get_and_delete() {
        let heads: Vec<String> = Http::signatures()
            .into_iter()
            .filter_map(|sig| match sig.head {
                HeadPattern::Exact(name) => Some(name),
                HeadPattern::Wildcard { .. } => None,
            })
            .collect();
        assert_eq!(heads, ["POST", "GET", "DELETE"]);
    }

    #[test]
    fn post_takes_a_body_clause() {
        let post = Http::signatures()
            .into_iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Exact(name) if name == "POST"))
            .expect("http has a POST action");
        assert!(
            post.clauses
                .iter()
                .any(|clause| clause.keyword == "body" && clause.shape == ClauseShape::Block),
        );
    }

    #[test]
    fn a_step_binds_to_a_request() {
        let bound = Http::bind(&post_to("api", "/orders")).expect("binds");
        assert_eq!(
            bound,
            Request {
                method: Method::POST,
                service: "api".into(),
                path: "/orders".into(),
                body: None,
                expect: None,
            },
        );
    }

    #[test]
    fn a_body_binds_to_json() {
        let mut step = post_to("api", "/orders");
        step.body = Some(vec![
            ("item".into(), plan::Value::Str("book".into())),
            ("quantity".into(), plan::Value::Int(4)),
        ]);
        let bound = Http::bind(&step).expect("binds");
        let body = bound.body.expect("a body was authored");
        let sent: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(sent["item"], "book");
        assert_eq!(sent["quantity"], 4);
    }

    #[test]
    fn an_operation_that_is_not_a_method_is_rejected() {
        let bound = Http::bind(&step("SEND", vec![plan::Value::Ident("api".into())]));
        assert!(matches!(bound, Err(Error::Method(_))));
    }

    #[test]
    fn an_operation_that_declares_no_body_will_not_take_one() {
        let mut step = step(
            "GET",
            vec![
                plan::Value::Ident("api".into()),
                plan::Value::Str("/orders".into()),
            ],
        );
        step.body = Some(vec![("item".into(), plan::Value::Str("book".into()))]);
        assert!(matches!(Http::bind(&step), Err(Error::Body(_))));
    }

    #[test]
    fn a_relative_path_is_rejected() {
        // It would concatenate onto the address, addressing some other port,
        // and the failure would look like the fleet leaving a write in doubt.
        let bound = Http::bind(&post_to("api", "orders"));
        assert!(matches!(bound, Err(Error::Path(_))));
    }

    #[test]
    fn a_request_without_a_path_is_rejected() {
        let bound = Http::bind(&step("GET", vec![plan::Value::Ident("api".into())]));
        assert!(matches!(bound, Err(Error::Arguments)));
    }
}
