use serde::{Deserialize, Serialize};

use crate::ConnId;

/// One TCP session observed by a sidecar proxy during a scenario run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Session {
    /// Fleet service the sidecar fronts.
    pub service: String,
    /// Proxy-local connection id.
    pub conn_id: ConnId,
    /// Peer address as reported by the sidecar.
    pub peer: String,
    /// Wall-clock nanoseconds since the Unix epoch when the sidecar accepted the connection.
    pub opened_ns: u128,
    /// Wall-clock nanoseconds since the Unix epoch when the sidecar closed the connection.
    pub closed_ns: u128,
    /// Bytes forwarded from the client to the upstream over the session lifetime.
    pub bytes_client_to_upstream: u64,
    /// Bytes forwarded from the upstream to the client over the session lifetime.
    pub bytes_upstream_to_client: u64,
}
