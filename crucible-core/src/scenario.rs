//! Orders scenario: POSTs a small sequence of orders and captures the responses.

use std::{net::SocketAddr, time::Duration};

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::verdict::{Ack, Observations, Outcome};

/// An HTTP status decides whether the service took responsibility: a success
/// acknowledges the write, a client error refuses it, and a server error leaves
/// the caller unable to tell.
fn classify(status: reqwest::StatusCode) -> Ack {
    if status.is_success() {
        Ack::Acked
    } else if status.is_client_error() {
        Ack::Rejected
    } else {
        Ack::Unknown
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Orders {
    client: Client,
}

impl Orders {
    /// # Errors
    /// Errors if the HTTP client cannot be built.
    pub fn new() -> Result<Self> {
        let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
        Ok(Self { client })
    }

    /// Run the orders sequence against `api`, capturing each request and response.
    /// Transport failures are recorded as outcomes rather than aborting the run.
    ///
    /// # Errors
    /// Errors if a request body cannot be serialized to JSON.
    pub async fn run(&self, api: SocketAddr) -> Result<Observations> {
        let mut observations = Observations::empty();
        for (item, quantity) in [("book", 4), ("noodles", 10), ("usb-c cable", 1)] {
            let request = OrderRequest {
                item: item.to_string(),
                quantity,
            };
            let request_body = serde_json::to_vec(&request)?;
            let outcome = match self
                .client
                .post(format!("http://{api}/orders"))
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    let ack = classify(response.status());
                    let bytes = response.bytes().await.unwrap_or_default();
                    Outcome {
                        operation: "POST /orders".into(),
                        ack,
                        request: request_body,
                        response: bytes.to_vec(),
                    }
                }
                // A refused connection never reached the service, so the write
                // definitively did not happen; anything else leaves it in doubt.
                Err(e) => Outcome {
                    operation: "POST /orders".into(),
                    ack: if e.is_connect() {
                        Ack::Rejected
                    } else {
                        Ack::Unknown
                    },
                    request: request_body,
                    response: e.to_string().into_bytes(),
                },
            };
            observations.outcomes.push(outcome);
        }
        Ok(observations)
    }
}

#[derive(Serialize, Deserialize)]
struct OrderRequest {
    item: String,
    quantity: i32,
}
