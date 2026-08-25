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

/// What the framework is exercising.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Property {
    /// What is accepted, is kept.
    Durable,
    /// Doing the same thing twice leaves what doing it once would.
    Idempotent,
    /// Regardless of order, the fleet settles the same way.
    Converges,
    /// A degraded fleet catches up on what it accepted while it was down.
    Recovers,
}

/// Where a fault can go.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Placement {
    /// Which way the traffic it watches for runs.
    pub direction: Direction,
    /// What the plugin is waiting for.
    pub mark: String,
    /// What faulting here would catch.
    pub why: String,
    /// Which property this is exercising.
    pub exercises: Property,
}

/// What a plugin made of some bytes.
pub struct Carried<'a> {
    /// What goes on the wire. Borrowed unless the plugin needs to mutate the
    /// wire.
    pub forward: std::borrow::Cow<'a, [u8]>,
    /// How far into `forward` the fleet is held.
    pub freeze_after: Option<usize>,
    /// Where a fault could go, found in these bytes.
    pub found: Vec<Placement>,
    /// What the plugin did.
    pub did: Option<Did>,
}

/// A plugin's report, in terms the framework can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Did {
    /// What it was asked to do landed.
    Placed(String),
    /// It cannot be done here, so the run proves nothing.
    Unplaceable(String),
}

/// One direction of one connection, read by whatever understands it.
///
/// The framework knows edges, bytes, and the property under test. What those
/// bytes mean is the protocol's business, so it is delegated to the plugin.
///
/// One is made per direction, and holds whatever has not finished arriving.
pub trait Kind: Send {
    /// Take the next bytes off the wire and say what goes on it in their place.
    fn carry<'a>(&mut self, bytes: &'a [u8]) -> Carried<'a>;
}

/// A link the proxy carries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Edge {
    /// The service that dialled, or `None` when the traffic came from outside
    /// the fleet, such as the framework driving a step.
    pub client: Option<String>,
    pub upstream: String,
}

impl Edge {
    /// Whether the fleet holds both ends of this.
    ///
    /// The other kind is the framework reaching in to drive a step. Breaking
    /// that stops the scenario, not the fleet.
    #[must_use]
    pub fn within_fleet(&self) -> bool {
        self.client.is_some()
    }
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
    /// Where the plugin reading this edge said a fault could go. Empty for an
    /// edge nothing can read, which is what the bursts are for.
    pub placements: Vec<Placement>,
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
