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
