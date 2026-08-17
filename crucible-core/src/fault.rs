//! Where a fault lands, in terms of what the fleet has been observed to do.

use crucible_protocol::Direction;
pub use crucible_protocol::Primitive;

use crate::verdict::Invariant;

/// What to break and how to drive the operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Fault {
    /// Break the fleet at the anchor and read what survived. Killing the service
    /// takes its process and everything it held; cutting the edge leaves both
    /// sides running and only the caller the wiser.
    Durable { anchor: Anchor, by: Losing },
}

/// A way of making the fleet lose something in flight. Narrower than
/// [`Primitive`], so nothing downstream has to answer for a fault that could
/// never have been built.
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
        }
    }

    /// Where in the observed traffic the fault lands.
    #[must_use]
    pub fn anchor(&self) -> &Anchor {
        match self {
            Fault::Durable { anchor, .. } => anchor,
        }
    }

    /// What is done to the fleet at that point.
    #[must_use]
    pub fn primitive(&self) -> Primitive {
        match self {
            Fault::Durable { by, .. } => (*by).into(),
        }
    }
}

/// Where a fault lands: once `service` has forwarded `k` packets on
/// `direction`. Anchoring to observed traffic rather than a wall clock is what
/// makes a schedule reproducible across replicas.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Anchor {
    pub service: String,
    pub direction: Direction,
    pub k: u32,
}
