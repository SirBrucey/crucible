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
    pub http_outcomes: Vec<HttpOutcome>,
}

impl Observations {
    pub fn empty() -> Self {
        Self::default()
    }
}

/// The result of one HTTP call made by a scenario.
#[derive(Debug)]
pub struct HttpOutcome {
    pub method: String,
    pub path: String,
    pub status: u16,
    pub body: Vec<u8>,
}

/// Produces a [`Verdict`] from a set of observations for one invariant.
pub trait Driver {
    fn drive(&mut self, observations: &Observations) -> Verdict;
}

/// Return the stub driver for the given invariant.
pub fn driver_for(invariant: Invariant) -> Box<dyn Driver> {
    match invariant {
        Invariant::Idempotent => Box::new(Idempotent),
        Invariant::Converges => Box::new(Converges),
        Invariant::Durable => Box::new(Durable),
        Invariant::Recovers => Box::new(Recovers),
    }
}
