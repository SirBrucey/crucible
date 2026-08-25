//! Where a fault lands, in terms of what the fleet has been observed to do.

pub use crucible_protocol::Primitive;
use crucible_protocol::{Direction, Edge};

use crate::verdict::Invariant;

/// What to break and how to drive the operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Fault {
    /// Break the fleet at the anchor and read what survived.
    Durable { anchor: Anchor, by: Taking },
    /// Run the whole scenario degraded, then put it back and see whether the
    /// fleet catches up on what it accepted while it was.
    Recovers { by: Taking },
}

/// What a fault takes away. Narrower than [`Primitive`], so nothing downstream
/// has to answer for a fault that could never have been built.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Taking {
    /// A service, taken away and brought back. Every edge it holds goes with it;
    /// an anchor only says when.
    Kill(String),
    /// One edge. The services on both ends keep running.
    Cut(Edge),
}

impl Taking {
    #[must_use]
    pub fn primitive(&self) -> Primitive {
        match self {
            Taking::Kill(_) => Primitive::Kill,
            Taking::Cut(_) => Primitive::Cut,
        }
    }

    /// What is done to, spelled for a report to name.
    #[must_use]
    pub fn target(&self) -> String {
        match self {
            Taking::Kill(service) => service.clone(),
            Taking::Cut(edge) => edge.to_string(),
        }
    }
}

impl std::fmt::Display for Taking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Taking::Kill(service) => write!(f, "killing {service}"),
            Taking::Cut(edge) => write!(f, "cutting {edge}"),
        }
    }
}

/// A way of making the fleet lose something in flight, before it is aimed at
/// anything. Narrower than [`Primitive`] on the same grounds as [`Taking`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, strum::EnumIter,
)]
pub enum Losing {
    Kill,
    Cut,
}

impl From<Losing> for Primitive {
    fn from(losing: Losing) -> Self {
        match losing {
            Losing::Kill => Primitive::Kill,
            Losing::Cut => Primitive::Cut,
        }
    }
}

impl TryFrom<Primitive> for Losing {
    type Error = Primitive;

    /// `Err` for a primitive that changes what the fleet does rather than
    /// costing it something, which is a different invariant's business.
    fn try_from(primitive: Primitive) -> Result<Self, Primitive> {
        match primitive {
            Primitive::Kill => Ok(Self::Kill),
            Primitive::Cut => Ok(Self::Cut),
            Primitive::Redeliver | Primitive::Reorder => Err(primitive),
        }
    }
}

impl Fault {
    /// The invariant this fault is meant to put under pressure, whose driver
    /// reads the verdict.
    #[must_use]
    pub fn invariant(&self) -> Invariant {
        match self {
            Fault::Durable { .. } => Invariant::Durable,
            Fault::Recovers { .. } => Invariant::Recovers,
        }
    }

    /// Where in the observed traffic the fault lands.
    #[must_use]
    pub fn anchor(&self) -> Option<&Anchor> {
        match self {
            Fault::Durable { anchor, .. } => Some(anchor),
            // Imposed before there is any traffic to land in.
            Fault::Recovers { .. } => None,
        }
    }

    /// What the fault takes away.
    #[must_use]
    pub fn taking(&self) -> &Taking {
        match self {
            Fault::Durable { by, .. } | Fault::Recovers { by, .. } => by,
        }
    }

    /// What is done to the fleet.
    #[must_use]
    pub fn primitive(&self) -> Primitive {
        self.taking().primitive()
    }
}

/// Where a fault lands: at the moment `mark` names, on `direction` of `edge`.
/// Anchoring to observed traffic rather than a wall clock is what makes a
/// schedule reproducible across replicas.
///
/// The edge supplies the moment, not the target. A kill on either of its ends
/// takes that whole service, every other edge it holds included.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Anchor {
    pub edge: Edge,
    pub direction: Direction,
    /// The moment to place it at, in the terms of whatever reads this edge. A
    /// plugin names what the moment is; an edge nothing can read counts reads.
    pub mark: String,
    /// What faulting here catches, for a report to say why it was worth doing.
    pub why: String,
}
