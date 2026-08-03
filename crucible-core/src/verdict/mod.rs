//! Invariants, observations, and drivers that produce verdicts.

pub mod drivers;

pub use drivers::{Converges, Durable, Idempotent, Recovers};
use serde::{Deserialize, Serialize};
use strum::EnumIter;

use crate::ipc::Verdict;

/// The four canonical event-driven invariants Crucible checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, EnumIter)]
pub enum Invariant {
    Idempotent,
    Converges,
    Durable,
    Recovers,
}

/// Observations captured during schedule execution and fed to a [`Driver`].
#[derive(Debug, Default)]
pub struct Observations {
    pub outcomes: Vec<Outcome>,
    /// What the scenario's checks read once the fleet settled, in the order the
    /// scenario states them.
    pub checks: Vec<Observed>,
    pub sessions: Vec<crucible_protocol::Session>,
    pub kill: Option<crucible_protocol::KillReport>,
}

/// A check and what the fleet was actually holding when it was read.
#[derive(Debug)]
pub struct Observed {
    pub check: crate::plan::Check,
    pub value: crate::plan::Value,
}

impl Observations {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// How many driven operations the system acknowledged.
    #[must_use]
    pub fn acked(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.ack == Ack::Acked)
            .count()
    }

    /// How many driven operations left the caller in doubt.
    #[must_use]
    pub fn unknown(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.ack == Ack::Unknown)
            .count()
    }
}

/// Whether the system took responsibility for a driven operation. The driver
/// that ran the operation decides, by the rules of the protocol it speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    /// Acknowledged: the system accepted responsibility for the write.
    Acked,
    /// Refused: the system definitively did not accept it.
    Rejected,
    /// In doubt: the caller cannot tell whether it was accepted.
    Unknown,
}

/// The result of one operation a driver ran. The payloads are opaque; only the
/// driver that produced them knows how to read them.
#[derive(Debug)]
pub struct Outcome {
    /// What was run, for reporting.
    pub operation: String,
    pub ack: Ack,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
}

/// Produces a [`Verdict`] from a set of observations for one invariant.
pub trait Driver {
    fn drive(&mut self, observations: &Observations) -> Verdict;
}

/// Return the stub driver for the given invariant.
#[must_use]
pub fn driver_for(invariant: Invariant) -> Box<dyn Driver> {
    match invariant {
        Invariant::Idempotent => Box::new(Idempotent),
        Invariant::Converges => Box::new(Converges),
        Invariant::Durable => Box::new(Durable),
        Invariant::Recovers => Box::new(Recovers),
    }
}
