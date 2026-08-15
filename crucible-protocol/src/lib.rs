//! Shared wire types used across Crucible's components.

mod fault;
mod primitive;
mod proxy;
mod session;

use std::time::{SystemTime, UNIX_EPOCH};

pub use fault::{FaultMissReason, FaultReport, FaultResult};
pub use primitive::Primitive;
pub use proxy::{ConnEvent, ConnEventKind, ConnId, Direction};
pub use session::{Session, WriteRecord};

use serde::{Deserialize, Serialize};

/// Per-service fault anchors derived by the Learn run, split by direction. Each
/// entry is a packet count `K`: freeze once the service has forwarded `K`
/// packets on that direction. The learn pass clusters observed packets into
/// bursts and keeps only the before/during/after edge of each burst (sampling
/// down if a run is unusually busy), so this is bounded by the number of anchors
/// rather than raw packet count and the catalogue always fits the IPC frame.
/// Faults land relative to observed traffic rather than a wall clock.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ServiceProfile {
    pub service: String,
    /// Anchor packet-counts on the client-to-upstream direction (requests in).
    pub client_to_upstream: Vec<u32>,
    /// Anchor packet-counts on the upstream-to-client direction (responses out).
    pub upstream_to_client: Vec<u32>,
}

/// Wall-clock nanoseconds since the Unix epoch, read from the host kernel clock.
///
/// # Panics
/// Panics if the system clock is set before the Unix epoch.
#[must_use]
pub fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_nanos()
}
