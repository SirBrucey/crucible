//! The HTTP observer plugin: reads settled state out of an API's own answers.

use std::{net::SocketAddr, time::Duration};

use crucible_core::{
    plan,
    schema::{
        AttrDecl, AttrSchema, ClauseDecl, ClauseShape, CmpOp, HeadPattern, OpSig, Param, ParamType,
        ValueType,
    },
};
use reqwest::Client;
use serde_json_path::JsonPath;

use crate::{
    error::Error as PluginError,
    role::{BoxFuture, Observer, ObserverRuntime, Query, Targeted},
};

/// How long a reading waits before the fleet is taken to have not answered.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The clause naming which part of the answer to read.
const AT: &str = "at";

/// Reads state a service will only answer for over HTTP.
pub struct Http {
    /// Sent with every reading, for an API that answers only the credentialed.
    headers: Vec<(String, String)>,
    /// Built once and shared, so a reading is a request rather than a new
    /// connection pool and TLS stack.
    client: Client,
}

/// One reading: the path to ask for, and which value of the answer to take.
#[derive(Debug)]
pub struct Get {
    pub service: String,
    pub path: String,
    pub at: String,
    /// `at`, compiled when the check bound so a malformed selector is refused
    /// before a replica is spent on it.
    selector: JsonPath,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("`{0}` is not an observable this reads")]
    Observable(String),
    #[error("a reading names the path to ask for")]
    NoPath,
    #[error("`{0}` is not a path: a path starts with `/`")]
    Path(String),
    #[error("a reading names `at`, which part of the answer to take")]
    NoSelector,
    #[error("`{selector}` does not select: {source}")]
    Selector {
        selector: String,
        source: serde_json_path::ParseError,
    },
    #[error("`{0}` answered with something that is not JSON")]
    NotJson(String),
    #[error("`{at}` names nothing in what `{path}` answered")]
    Nothing { at: String, path: String },
    #[error("`{at}` names {count} values in what `{path}` answered, and a reading is of one")]
    NotOne {
        at: String,
        path: String,
        count: usize,
    },
    #[error("`{0}` holds nothing a plan can carry")]
    Unreadable(String),
    #[error(transparent)]
    Client(#[from] reqwest::Error),
}

impl From<Error> for PluginError {
    fn from(e: Error) -> Self {
        Self::new(Http::NAME, e)
    }
}

impl Observer for Http {
    const NAME: &'static str = "http";
    type Query = Get;
    type Error = Error;

    /// Reads with the headers the service declares, which is how an API that
    /// answers only the credentialed is read at all.
    fn runtime(service: &plan::Service) -> Self {
        Self {
            // Only a broken TLS or resolver setup fails this, and neither is
            // something a reading could carry on without.
            client: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("a client with only a timeout set builds"),
            headers: service
                .attr("headers")
                .and_then(plan::Value::as_map)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|(name, value)| {
                            Some((name.clone(), value.as_str()?.to_owned()))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    /// One observable: ask for a path, and take one value out of the answer.
    ///
    /// The kind of value is whatever the answer holds; what the scenario
    /// compares it against says which was meant.
    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::observable(
                HeadPattern::exact("get"),
                ValueType::Scalar,
                CmpOp::ALL.to_vec(),
            )
            .with_param(Param::required("path", ParamType::Path))
            .with_clause(ClauseDecl::new(AT, ClauseShape::Value(ValueType::Str))),
        ]
    }

    /// Only the headers to read with are the author's to give.
    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![AttrDecl::optional(
            "headers",
            ValueType::MapOf(Box::new(ValueType::Str)),
        )])
    }

    fn bind(check: &plan::Check) -> Result<Self::Query, Self::Error> {
        let [reading] = check.observable.as_slice() else {
            return Err(Error::Observable(check.observable.join(".")));
        };
        if reading != "get" {
            return Err(Error::Observable(reading.clone()));
        }
        let Some(path) = check.args.first().and_then(plan::Value::as_str) else {
            return Err(Error::NoPath);
        };
        // A relative path would concatenate onto the address and address some
        // other port entirely, so it is rejected here rather than sent.
        if !path.starts_with('/') {
            return Err(Error::Path(path.to_owned()));
        }
        let Some(at) = check.clauses.get(AT).and_then(plan::Value::as_str) else {
            return Err(Error::NoSelector);
        };
        let selector = JsonPath::parse(at).map_err(|source| Error::Selector {
            selector: at.to_owned(),
            source,
        })?;
        Ok(Get {
            service: check.service.clone(),
            path: path.to_owned(),
            at: at.to_owned(),
            selector,
        })
    }
}

impl ObserverRuntime for Http {
    fn prepare<'a>(
        &'a self,
        check: &'a plan::Check,
    ) -> BoxFuture<'a, Result<Box<dyn Query>, PluginError>> {
        Box::pin(async move {
            Ok(Box::new(Read {
                get: Http::bind(check)?,
                headers: self.headers.clone(),
                client: self.client.clone(),
            }) as Box<dyn Query>)
        })
    }
}

/// A bound reading and the headers to take it with.
struct Read {
    get: Get,
    headers: Vec<(String, String)>,
    client: Client,
}

impl Targeted for Read {
    fn kind(&self) -> &str {
        Http::NAME
    }

    fn target(&self) -> &str {
        &self.get.service
    }
}

impl Query for Read {
    fn read(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<plan::Value, PluginError>> {
        Box::pin(async move { self.take(endpoint).await.map_err(PluginError::from) })
    }
}

impl Read {
    async fn take(&self, endpoint: SocketAddr) -> Result<plan::Value, Error> {
        let mut request = self
            .client
            .get(format!("http://{endpoint}{}", self.get.path));
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        let body = request.send().await?.text().await?;

        let answer: serde_json::Value =
            serde_json::from_str(&body).map_err(|_| Error::NotJson(self.get.path.clone()))?;
        let found = self.get.selector.query(&answer).all();
        match found.as_slice() {
            [] => Err(Error::Nothing {
                at: self.get.at.clone(),
                path: self.get.path.clone(),
            }),
            [only] => reading(only).ok_or_else(|| Error::Unreadable(self.get.at.clone())),
            several => Err(Error::NotOne {
                at: self.get.at.clone(),
                path: self.get.path.clone(),
                count: several.len(),
            }),
        }
    }
}

/// What a scenario compares against, as whatever JSON made of it.
///
/// A number stays a number. A verdict reads a count as a quantity the fleet
/// moved, and reading one back as its digits would leave it a label that only
/// ever matches or does not. An object or a list is not one value, so it is not
/// a reading at all.
fn reading(value: &serde_json::Value) -> Option<plan::Value> {
    Some(match value {
        serde_json::Value::String(s) => plan::Value::Str(s.clone()),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(whole) => plan::Value::Int(whole),
            // Nothing a scenario states is fractional, so a number that is not
            // whole is compared as it was written rather than rounded into one.
            None => plan::Value::Str(n.to_string()),
        },
        serde_json::Value::Bool(b) => plan::Value::Bool(*b),
        serde_json::Value::Null => plan::Value::Null,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(path: &str, at: Option<&str>) -> plan::Check {
        plan::Check {
            service: "pdns".into(),
            observer: "http".into(),
            observable: vec!["get".into()],
            args: vec![plan::Value::Str(path.into())],
            filter: None,
            clauses: at
                .map(|at| {
                    [("at".to_owned(), plan::Value::Str(at.to_owned()))]
                        .into_iter()
                        .collect()
                })
                .unwrap_or_default(),
            op: CmpOp::Eq,
            value: plan::Value::Str("3".into()),
        }
    }

    /// The shape that needs a selector rather than a field name: a list whose
    /// entries are told apart by a sibling key, which is how a nameserver
    /// answers for a zone's metadata.
    const METADATA: &str = r#"[
        {"kind": "SOA-EDIT-API", "metadata": ["DEFAULT"]},
        {"kind": "X-PDNS-Update-Sequence", "metadata": ["3"]}
    ]"#;

    fn select(at: &str) -> Result<plan::Value, Error> {
        let answer: serde_json::Value = serde_json::from_str(METADATA).expect("valid json");
        let found = JsonPath::parse(at)
            .expect("valid selector")
            .query(&answer)
            .all();
        match found.as_slice() {
            [only] => reading(only).ok_or_else(|| Error::Unreadable(at.to_owned())),
            [] => Err(Error::Nothing {
                at: at.to_owned(),
                path: "/".to_owned(),
            }),
            several => Err(Error::NotOne {
                at: at.to_owned(),
                path: "/".to_owned(),
                count: several.len(),
            }),
        }
    }

    #[test]
    fn a_check_binds_to_a_path_and_a_selector() {
        let bound =
            Http::bind(&check("/api/v1/zones/x/metadata", Some("$[0].kind"))).expect("binds");
        assert_eq!(bound.path, "/api/v1/zones/x/metadata");
        assert_eq!(bound.at, "$[0].kind");
    }

    /// The reading a scenario needs of this fleet: pick the entry by its kind,
    /// then take the value out of it.
    #[test]
    fn a_selector_searches_a_list_by_a_sibling_key() {
        assert_eq!(
            select("$[?@.kind == 'X-PDNS-Update-Sequence'].metadata[0]").ok(),
            Some(plan::Value::Str("3".into())),
        );
    }

    /// A number stays a number, so a verdict can read it as a quantity the
    /// fleet moved rather than a label.
    #[test]
    fn a_number_reads_as_a_number() {
        let answer: serde_json::Value = serde_json::json!({"count": 3});
        let found = JsonPath::parse("$.count").unwrap().query(&answer).all();
        assert_eq!(reading(found[0]), Some(plan::Value::Int(3)));
    }

    /// The same field quoted is what the fleet holds, and is read as it stands.
    /// The two are told apart by the answer, not by what a scenario hoped for.
    #[test]
    fn a_quoted_number_reads_as_text() {
        let answer: serde_json::Value = serde_json::json!({"count": "3"});
        let found = JsonPath::parse("$.count").unwrap().query(&answer).all();
        assert_eq!(reading(found[0]), Some(plan::Value::Str("3".into())));
    }

    #[test]
    fn a_selector_naming_nothing_is_not_a_reading() {
        assert!(matches!(
            select("$[?@.kind == 'absent'].metadata[0]"),
            Err(Error::Nothing { .. })
        ));
    }

    /// A reading is of one value, so a selector that names several has not
    /// narrowed enough to compare.
    #[test]
    fn a_selector_naming_several_is_not_a_reading() {
        assert!(matches!(
            select("$[*].kind"),
            Err(Error::NotOne { count: 2, .. })
        ));
    }

    /// An object is not one value, so it is nothing a comparison can carry.
    #[test]
    fn a_selector_naming_an_object_is_not_a_reading() {
        assert!(matches!(select("$[0]"), Err(Error::Unreadable(_))));
    }

    #[test]
    fn a_malformed_selector_is_refused_before_a_fleet_is_asked() {
        assert!(matches!(
            Http::bind(&check("/x", Some("not a selector"))),
            Err(Error::Selector { .. })
        ));
    }

    #[test]
    fn a_reading_without_a_selector_is_refused() {
        assert!(matches!(
            Http::bind(&check("/x", None)),
            Err(Error::NoSelector)
        ));
    }

    #[test]
    fn a_relative_path_is_rejected() {
        assert!(matches!(
            Http::bind(&check("api/v1", Some("$.x"))),
            Err(Error::Path(_))
        ));
    }
}
