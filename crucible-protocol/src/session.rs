use serde::{Deserialize, Serialize};

use crate::{ConnId, Direction};

/// One TCP session observed by a sidecar proxy during a scenario run.
/// `closed_ns` is `None` when the session was still open when the framework
/// stopped observing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Session {
    pub service: String,
    pub conn_id: ConnId,
    pub peer: String,
    pub opened_ns: u128,
    pub closed_ns: Option<u128>,
    pub writes: Vec<WriteRecord>,
    /// Where the plugin reading this said a fault could go.
    pub placements: Vec<crate::Placement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct WriteRecord {
    pub ts_ns: u128,
    pub direction: Direction,
    pub bytes: u64,
}
