//! Where a fault lands, in terms of what the fleet has been observed to do.

use crucible_protocol::Direction;

/// Where to freeze a fleet: once `service` has forwarded `k` packets on
/// `direction`. Anchoring to observed traffic rather than a wall clock is what
/// makes a schedule reproducible across replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Anchor {
    pub service: String,
    pub direction: Direction,
    pub k: u32,
}
