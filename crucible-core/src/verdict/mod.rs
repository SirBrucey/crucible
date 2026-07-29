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
// FIXME(#83): shape belongs to a kind plugin, not this shared type.
#[derive(Debug, Default)]
pub struct Observations {
    pub http_outcomes: Vec<HttpOutcome>,
    pub db_state: Option<DbState>,
    pub sessions: Vec<crucible_protocol::Session>,
    pub kill: Option<crucible_protocol::KillReport>,
}

impl Observations {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}

/// The result of one HTTP call made by a scenario.
#[derive(Debug)]
pub struct HttpOutcome {
    pub method: String,
    pub path: String,
    pub request_body: Vec<u8>,
    pub status: u16,
    pub body: Vec<u8>,
}

/// Snapshot of the fleet's persisted state after the scenario finished.
#[derive(Debug, Default)]
pub struct DbState {
    pub orders: Vec<OrderRow>,
    pub stock: Vec<StockRow>,
}

#[derive(Debug)]
pub struct OrderRow {
    pub id: u64,
    pub item: String,
    pub quantity: i32,
}

#[derive(Debug)]
pub struct StockRow {
    pub item: String,
    pub level: i32,
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
