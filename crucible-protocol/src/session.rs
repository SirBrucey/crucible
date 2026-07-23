use serde::{Deserialize, Serialize};

use crate::ConnId;

/// One TCP session observed by a sidecar proxy during a scenario run.
/// `closed_ns` and the byte counts are `None` when the session was still open
/// when the framework stopped observing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Session {
    pub service: String,
    pub conn_id: ConnId,
    pub peer: String,
    pub opened_ns: u128,
    pub closed_ns: Option<u128>,
    pub bytes_client_to_upstream: Option<u64>,
    pub bytes_upstream_to_client: Option<u64>,
}

/// Identifier for a `Session`; carries just the fields a schedule needs to name
/// which edge to target.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct SessionRef {
    pub service: String,
    pub conn_id: ConnId,
}

impl SessionRef {
    pub fn new(service: impl Into<String>, conn_id: ConnId) -> Self {
        Self {
            service: service.into(),
            conn_id,
        }
    }
}

impl From<&Session> for SessionRef {
    fn from(session: &Session) -> Self {
        Self {
            service: session.service.clone(),
            conn_id: session.conn_id,
        }
    }
}
