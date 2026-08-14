//! Where a fault lands, in terms of what the fleet has been observed to do.

use crucible_protocol::Direction;

use crate::verdict::Invariant;

/// Something that can be done to a running fleet to put an invariant under
/// pressure. A plugin offers one by implementing it, so this is the vocabulary a
/// campaign uses to say what it could and could not reach.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum Primitive {
    /// Take a service out of the fleet and put it back.
    Kill,
    /// Deliver a message the fleet has already handled a second time.
    Redeliver,
    /// Hold a message back until a later one has passed it.
    Reorder,
}

impl std::fmt::Display for Primitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Primitive::Kill => "kill a service",
            Primitive::Redeliver => "redeliver a message",
            Primitive::Reorder => "reorder messages",
        })
    }
}

/// What to break and how to drive the operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Fault {
    /// Kill the anchored service, bring it back, and read what survived.
    Durable(Anchor),
}

impl Fault {
    /// The invariant this fault is meant to put under pressure, whose driver
    /// reads the verdict.
    #[must_use]
    pub fn invariant(&self) -> Invariant {
        match self {
            Fault::Durable(_) => Invariant::Durable,
        }
    }

    /// Where in the observed traffic the fault lands.
    #[must_use]
    pub fn anchor(&self) -> &Anchor {
        match self {
            Fault::Durable(anchor) => anchor,
        }
    }
}

/// Where to freeze a fleet: once `service` has forwarded `k` packets on
/// `direction`. Anchoring to observed traffic rather than a wall clock is what
/// makes a schedule reproducible across replicas.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Anchor {
    pub service: String,
    pub direction: Direction,
    pub k: u32,
}
