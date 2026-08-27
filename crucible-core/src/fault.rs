//! Where a fault lands, in terms of what the fleet has been observed to do.

pub use crucible_protocol::Primitive;
use crucible_protocol::{Direction, Edge};

use crate::verdict::Invariant;

/// What to break and how to drive the operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Fault {
    /// Break the fleet at the anchor and read what survived.
    Durable { anchor: Anchor, by: By },
    /// Have the fleet do the same work twice at the anchor, and read whether
    /// doing it twice left what doing it once would.
    Idempotent { anchor: Anchor, by: By },
    /// Tell the fleet things in an order it was not told them in, and read
    /// whether it settled where it would have anyway.
    Converges { anchor: Anchor, by: By },
    /// Run the whole scenario degraded, then put it back and see whether the
    /// fleet catches up on what it accepted while it was.
    Recovers { by: By },
}

/// How a fault drives the fleet, and what it drives. Narrower than
/// [`Primitive`], so nothing downstream has to answer for a fault that could
/// never have been built.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum By {
    /// A service, taken away and brought back. Every edge it holds goes with it;
    /// an anchor only says when.
    Kill(String),
    /// One edge. The services on both ends keep running.
    Cut(Edge),
    /// One edge, carrying a message it already carried, so whoever reads it is
    /// asked to do the same work twice.
    Repeat(Edge),
    /// One edge, carrying its messages in an order the broker did not send
    /// them in, so a fleet relying on that order is asked to do without it.
    Reorder(Edge),
}

impl By {
    #[must_use]
    pub fn primitive(&self) -> Primitive {
        match self {
            By::Kill(_) => Primitive::Kill,
            By::Cut(_) => Primitive::Cut,
            By::Repeat(_) => Primitive::Redeliver,
            By::Reorder(_) => Primitive::Reorder,
        }
    }

    /// What is done to, spelled for a report to name.
    #[must_use]
    pub fn target(&self) -> String {
        match self {
            By::Kill(service) => service.clone(),
            By::Cut(edge) | By::Repeat(edge) | By::Reorder(edge) => edge.to_string(),
        }
    }
}

impl std::fmt::Display for By {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            By::Kill(service) => write!(f, "killing {service}"),
            By::Cut(edge) => write!(f, "cutting {edge}"),
            By::Repeat(edge) => write!(f, "repeating a message on {edge}"),
            By::Reorder(edge) => write!(f, "reordering messages on {edge}"),
        }
    }
}

/// A way of driving the fleet, before it is aimed at anything. Narrower than
/// [`Primitive`] on the same grounds as [`By`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, strum::EnumIter,
)]
pub enum Drive {
    Kill,
    Cut,
    Repeat,
    Reorder,
}

impl From<Drive> for Primitive {
    fn from(losing: Drive) -> Self {
        match losing {
            Drive::Kill => Primitive::Kill,
            Drive::Cut => Primitive::Cut,
            Drive::Repeat => Primitive::Redeliver,
            Drive::Reorder => Primitive::Reorder,
        }
    }
}

impl TryFrom<Primitive> for Drive {
    type Error = Primitive;

    /// `Err` for a primitive nothing can yet aim at an edge.
    fn try_from(primitive: Primitive) -> Result<Self, Primitive> {
        match primitive {
            Primitive::Kill => Ok(Self::Kill),
            Primitive::Cut => Ok(Self::Cut),
            Primitive::Redeliver => Ok(Self::Repeat),
            Primitive::Reorder => Ok(Self::Reorder),
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
            Fault::Idempotent { .. } => Invariant::Idempotent,
            Fault::Converges { .. } => Invariant::Converges,
            Fault::Recovers { .. } => Invariant::Recovers,
        }
    }

    /// Where in the observed traffic the fault lands.
    #[must_use]
    pub fn anchor(&self) -> Option<&Anchor> {
        match self {
            Fault::Durable { anchor, .. }
            | Fault::Idempotent { anchor, .. }
            | Fault::Converges { anchor, .. } => Some(anchor),
            // Imposed before there is any traffic to land in.
            Fault::Recovers { .. } => None,
        }
    }

    /// What the fault takes away.
    #[must_use]
    pub fn taking(&self) -> &By {
        match self {
            Fault::Durable { by, .. }
            | Fault::Idempotent { by, .. }
            | Fault::Converges { by, .. }
            | Fault::Recovers { by, .. } => by,
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
