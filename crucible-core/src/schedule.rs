//! One unit of work a worker runs: a fleet to bring up, what to do to it, what
//! to read afterwards, and what to break in the middle.

use crate::{fault::Fault, plan, verdict::Checkpoint};

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
    /// What to break and what that tests. None is the fault-free run every
    /// other schedule is judged against.
    pub fault: Option<Fault>,
    /// What the fault-free run left after each step, which we judge against.
    /// Empty for the fault-free run itself.
    pub trajectory: Vec<Checkpoint>,
}

impl Schedule {
    /// The id the fault-free run carries. Faulted schedules are numbered from
    /// one, so a journal entry says which run it came from.
    pub const LEARN_ID: u32 = 0;

    /// The fault-free run: the work, with nothing to break.
    #[must_use]
    pub fn learn(fleet: plan::Fleet, steps: Vec<plan::Step>, checks: Vec<plan::Check>) -> Self {
        Self {
            id: Self::LEARN_ID,
            fleet,
            steps,
            checks,
            fault: None,
            trajectory: Vec::new(),
        }
    }

    /// The same work with something broken part way through it, judged against
    /// where the fault-free run got to.
    #[must_use]
    pub fn faulted(
        id: u32,
        fleet: plan::Fleet,
        steps: Vec<plan::Step>,
        checks: Vec<plan::Check>,
        fault: Fault,
        trajectory: Vec<Checkpoint>,
    ) -> Self {
        Self {
            id,
            fleet,
            steps,
            checks,
            fault: Some(fault),
            trajectory,
        }
    }
}
