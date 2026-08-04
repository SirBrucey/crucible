//! One unit of work a worker runs: a fleet to bring up, what to do to it, what
//! to read afterwards, and what to break in the middle.

use crucible_protocol::Direction;

use crate::plan;

/// Everything a worker needs to run once. Derived from a plan rather than
/// copied out of it: the steps and checks are the scenario's, plus whatever the
/// invariant being tested calls for, so what runs may differ from what the
/// author wrote.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Schedule {
    pub id: u32,
    /// The fleet to bring up.
    pub fleet: plan::Fleet,
    /// The actions to drive, in order.
    pub steps: Vec<plan::Step>,
    /// What to read once the fleet has settled.
    pub checks: Vec<plan::Check>,
    /// What to break, applied in the order listed. Empty is the fault-free run
    /// that every other schedule is judged against.
    pub faults: Vec<Fault>,
}

impl Schedule {
    /// Whether this is the fault-free run.
    #[must_use]
    pub fn is_fault_free(&self) -> bool {
        self.faults.is_empty()
    }
}

/// Killing a service once its proxy has carried `packet_index` packets on
/// `direction`, so the fault lands relative to observed traffic rather than a
/// clock.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Fault {
    pub service: String,
    pub direction: Direction,
    pub packet_index: u32,
}
