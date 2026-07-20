//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use crate::{
    deployment::Deployment,
    ipc::Verdict,
    scenario::{self, Orders},
    scheduler::Schedule,
    verdict::driver_for,
};

/// Per-worker orchestrator that owns the replica lifecycle around each schedule.
pub struct Orchestrator<D>
where
    D: Deployment,
{
    deployment: D,
    scenario: Orders,
}

impl<D> Orchestrator<D>
where
    D: Deployment,
{
    pub fn new(deployment: D, scenario: Orders) -> Self {
        Self {
            deployment,
            scenario,
        }
    }

    /// Bring the fleet replica up and wait for every service to become ready.
    pub async fn setup(&mut self) -> Result<(), D::Error> {
        self.deployment.setup().await?;
        self.deployment.wait_ready().await
    }

    /// Run the scenario against the fleet and produce a verdict from the observations.
    pub async fn execute(&mut self, schedule: &Schedule) -> Result<Verdict, scenario::Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let observations = self.scenario.run(api).await?;
        Ok(driver_for(schedule.invariant).drive(&observations))
    }

    /// Tear down the replica.
    pub async fn teardown(&mut self) -> Result<(), D::Error> {
        self.deployment.teardown().await
    }
}
