//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use crate::{
    deployment::Deployment,
    ipc::Verdict,
    scheduler::Schedule,
    verdict::{Observations, driver_for},
};

/// Per-worker orchestrator that owns the replica lifecycle around each schedule.
pub struct Orchestrator<D>
where
    D: Deployment,
{
    deployment: D,
}

impl<D> Orchestrator<D>
where
    D: Deployment,
{
    pub fn new(deployment: D) -> Self {
        Self { deployment }
    }

    /// Bring the fleet replica up and wait for every service to become ready.
    pub async fn setup(&mut self) -> Result<(), D::Error> {
        self.deployment.setup().await?;
        self.deployment.wait_ready().await
    }

    /// Execute one schedule and produce a verdict from the observations.
    pub fn execute(&mut self, schedule: &Schedule) -> Verdict {
        let observations = Observations::empty();
        driver_for(schedule.invariant).drive(&observations)
    }

    /// Tear down the replica.
    pub async fn teardown(&mut self) -> Result<(), D::Error> {
        self.deployment.teardown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{deployment::Noop, verdict::Invariant};

    #[test]
    fn execute_yields_inconclusive_for_every_invariant() {
        let mut orchestrator = Orchestrator::new(Noop);
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
