//! One unit of work a worker runs: a fleet to bring up, what to do to it, what
//! to read afterwards, and what to break in the middle.

use std::time::Duration;

use crate::{fault::Fault, plan, verdict::Baseline};

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
    /// What the run is for.
    pub purpose: Purpose,
    /// What the fault-free run left after each step and where it settled, which
    /// is what we judge against.
    pub fault_free: Baseline,
    /// How long the fleet may take to settle, as the scenario states it.
    pub consistent_within: Duration,
}

/// What a run is for, which decides what the worker does with its result.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Purpose {
    /// The fault-free run. What the fleet is and what the workload does to it.
    /// Every other run is judged against what this one found.
    Learn,
    /// A subset of the steps driven with nothing broken, this says where landing
    /// exactly those leaves the fleet
    Reference { landed: Vec<usize> },
    /// Something broken part way through the work.
    Break(Box<Fault>),
}

/// How far through a run a worker is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Phase {
    /// Bringing the replica up.
    Setup,
    /// Driving the scenario's steps.
    Driving,
    /// Waiting for the fleet to come to rest.
    Healing,
    /// Reading where the fleet settled.
    Observing,
    /// Taking the replica down.
    Tearing,
}

impl Schedule {
    /// The steps this run drove on their own, if it is a reference run.
    #[must_use]
    pub fn landed(&self) -> Option<&[usize]> {
        match &self.purpose {
            Purpose::Reference { landed } => Some(landed),
            Purpose::Learn | Purpose::Break(_) => None,
        }
    }

    /// What this run breaks, if it breaks anything.
    #[must_use]
    pub fn fault(&self) -> Option<&Fault> {
        match &self.purpose {
            Purpose::Break(fault) => Some(fault),
            Purpose::Learn | Purpose::Reference { .. } => None,
        }
    }

    /// The id the fault-free run carries. Faulted schedules are numbered from
    /// one, so a journal entry says which run it came from.
    pub const LEARN_ID: u32 = 0;

    /// The fault-free run: the work, with nothing to break.
    #[must_use]
    pub fn learn(
        fleet: plan::Fleet,
        steps: Vec<plan::Step>,
        checks: Vec<plan::Check>,
        consistent_within: Duration,
    ) -> Self {
        Self {
            id: Self::LEARN_ID,
            fleet,
            steps,
            checks,
            purpose: Purpose::Learn,
            fault_free: Baseline::default(),
            consistent_within,
        }
    }

    /// A run of `steps`, which are the steps `landed` names, with nothing
    /// broken.
    #[must_use]
    pub fn reference(
        id: u32,
        fleet: plan::Fleet,
        steps: Vec<plan::Step>,
        checks: Vec<plan::Check>,
        landed: Vec<usize>,
        consistent_within: Duration,
    ) -> Self {
        Self {
            id,
            fleet,
            steps,
            checks,
            purpose: Purpose::Reference { landed },
            fault_free: Baseline::default(),
            consistent_within,
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
        fault_free: Baseline,
        consistent_within: Duration,
    ) -> Self {
        Self {
            id,
            fleet,
            steps,
            checks,
            purpose: Purpose::Break(Box::new(fault)),
            fault_free,
            consistent_within,
        }
    }
}
