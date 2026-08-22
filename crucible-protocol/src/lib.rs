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

/// Reads one direction of one connection as the operations it carries.
///
/// What a fault is placed against is an operation the fleet performs, not a
/// chunk the kernel happened to hand over: those vary run to run, and a schedule
/// that named the second one would land somewhere else next time. Telling one
/// from the next is the protocol's business, so it is a plugin's.
///
/// One is made per direction, and holds whatever has not finished arriving.
pub trait Operations: Send {
    /// Take the next `bytes` off the wire and return how many operations they
    /// completed.
    fn read(&mut self, bytes: &[u8]) -> usize;
}

/// A link the proxy carries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Edge {
    /// The service that dialled, or `None` when the traffic came from outside
    /// the fleet, such as the framework driving a step.
    pub client: Option<String>,
    pub upstream: String,
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.client {
            Some(client) => write!(f, "{client} -> {}", self.upstream),
            None => write!(f, "-> {}", self.upstream),
        }
    }
}

/// The bursts of traffic the learn run saw one edge carry, split by direction.
/// Faults land relative to observed traffic rather than a wall clock, so this is
/// what the scheduler places them against. Bursts are bounded by sampling if a
/// run is unusually busy, so the catalogue always fits the IPC frame.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EdgeProfile {
    pub edge: Edge,
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
