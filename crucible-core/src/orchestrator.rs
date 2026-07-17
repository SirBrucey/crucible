//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use crate::{
    ipc::Verdict,
    scheduler::Schedule,
    verdict::{Observations, driver_for},
};

/// Per-worker orchestrator that owns the replica lifecycle around each schedule.
pub struct Orchestrator {}

impl Orchestrator {
    pub fn new() -> Self {
        Self {}
    }

    /// Bring up the fleet replica this worker will drive.
    pub fn setup(&mut self) {}

    /// Execute one schedule and produce a verdict from the observations.
    pub fn execute(&mut self, schedule: &Schedule) -> Verdict {
        let observations = Observations::empty();
        driver_for(schedule.invariant).drive(&observations)
    }

    /// Tear down the replica.
    pub fn teardown(&mut self) {}
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verdict::Invariant;

    #[test]
    fn execute_yields_inconclusive_for_every_invariant() {
        let mut orchestrator = Orchestrator::new();
        for invariant in [
            Invariant::Idempotent,
            Invariant::Converges,
            Invariant::Durable,
            Invariant::Recovers,
        ] {
            let schedule = Schedule {
                schedule_id: 0,
                invariant,
                payload: Vec::new(),
            };
            assert_eq!(orchestrator.execute(&schedule), Verdict::Inconclusive);
        }
    }
}
