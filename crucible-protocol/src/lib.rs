//! Shared wire types used across Crucible's components.

mod fault;
mod primitive;
mod proxy;
mod session;

use std::time::{SystemTime, UNIX_EPOCH};

pub use fault::{At, FaultMissReason, FaultReport, FaultResult};
pub use primitive::Primitive;
pub use proxy::{ConnEvent, ConnEventKind, ConnId, Direction};
pub use session::{Session, WriteRecord};

use serde::{Deserialize, Serialize};

/// The bursts of traffic the learn run saw a service carry, split by direction.
/// Faults land relative to observed traffic rather than a wall clock, so this is
/// what the scheduler places them against. Bursts are bounded by sampling if a
/// run is unusually busy, so the catalogue always fits the IPC frame.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ServiceProfile {
    pub service: String,
    /// Bursts on the client-to-upstream direction (requests in).
    pub client_to_upstream: Vec<Burst>,
    /// Bursts on the upstream-to-client direction (responses out).
    pub upstream_to_client: Vec<Burst>,
}

/// One run of packets with no long gap in it, given as the three points a fault
/// can be placed against.
///
/// Each point is a packet count `K`: freeze once the service has forwarded `K`
/// packets on that direction. `start` is `0` for the first burst on an edge,
/// which is not a placeable anchor, since freezing there kills the service
/// before the scenario has driven anything across the edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct Burst {
    /// Just before the burst: its first packet has not yet crossed.
    pub start: u32,
    /// Part way through.
    pub mid: u32,
    /// Just after the burst: its last packet has crossed.
    pub end: u32,
    /// How many packets it carried, which is how many chances it offers to lose
    /// something.
    pub packets: u32,
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
