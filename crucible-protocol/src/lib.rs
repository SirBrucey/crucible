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

/// Where a fault can go.
///
/// A moment says where it is and what breaking the fleet there is. What that
/// shows about the fleet is not the moment's to say: the same broken frame
/// leaves one publisher retrying and another giving up, and only the run says
/// which happened.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Placement {
    /// Which way the traffic it watches for runs.
    pub direction: Direction,
    /// What the plugin is waiting for.
    pub mark: String,
    /// What faulting here would catch.
    pub why: String,
    /// What breaking the fleet here is.
    pub doing: Doing,
}

/// What is done at a moment to break the fleet there.
///
/// Two sorts, and the difference is who does it. Taking something away happens
/// outside the byte stream, so the moment is a place to hold the fleet and the
/// framework picks what to take. Changing what crosses happens in the stream,
/// so only the plugin reading it can do it, and it says which way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Doing {
    /// Hold the fleet here for something to be taken away from outside.
    Holding,
    /// Change what crosses as it passes, the one way this names.
    Rewriting(Primitive),
}

/// What a plugin made of some bytes.
pub struct Carried<'a> {
    /// Ordered buffer of bytes from the wire.
    pub forward: Vec<std::borrow::Cow<'a, [u8]>>,
    /// How many of them go before the fleet is held.
    pub freeze_after: Option<usize>,
    /// Where a fault could go, found in these bytes.
    pub found: Vec<Placement>,
    /// What the plugin did.
    pub did: Option<Did>,
}

/// What a plugin made of the fault it was asked to place.
///
/// Only [`Did::Placed`] is a fault the run met. The rest are reasons it did
/// not, and a run that did not meet its fault has tested nothing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum Did {
    /// Set in motion. Whether it happens is the fleet's to say, so this alone
    /// proves nothing.
    Asked,
    /// Seen to happen, so the fleet met it.
    Placed(String),
    /// Cannot be done here.
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
    ///
    /// `placing` says whether a fault may be placed on this connection now:
    /// the scenario has started, and this is the edge the schedule named.
    /// Neither is anything the bytes can say, so the framework says it.
    fn carry<'a>(&mut self, bytes: &'a [u8], placing: bool) -> Carried<'a>;
}

/// A service in the fleet and the host its container answers at.
///
/// The deployment writes these and the proxy reads them, so the two crates
/// would otherwise agree on the spelling by convention. Written `NAME=HOST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceHost {
    pub name: String,
    pub host: String,
}

impl std::fmt::Display for ServiceHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.name, self.host)
    }
}

impl std::str::FromStr for ServiceHost {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        let (name, host) = spec
            .split_once('=')
            .ok_or_else(|| format!("`{spec}` must be in the form SERVICE=HOST"))?;
        Ok(Self {
            name: name.to_owned(),
            host: host.to_owned(),
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The deployment writes these and the proxy reads them, so what one says
    /// has to be what the other takes.
    #[test]
    fn a_service_host_survives_being_written_and_read_back() {
        let host = ServiceHost {
            name: "pdns_update".to_owned(),
            host: "pdns_update-actual".to_owned(),
        };
        assert_eq!(host.to_string().parse::<ServiceHost>(), Ok(host));
    }

    #[test]
    fn a_service_without_a_host_is_refused() {
        assert!("broker".parse::<ServiceHost>().is_err());
    }
}
