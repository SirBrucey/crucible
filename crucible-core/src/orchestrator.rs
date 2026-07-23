//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use crucible_protocol::Session;

use crate::{
    deployment::{Deployment, docker},
    ipc::Verdict,
    observer::{self, DbObserver},
    scenario::{self, Orders},
    scheduler::Schedule,
    verdict::{Invariant, Observations, driver_for},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Scenario(#[from] scenario::Error),
    #[error(transparent)]
    Observer(#[from] observer::Error),
    #[error(transparent)]
    Docker(#[from] docker::Error),
    #[error("observer not installed; call set_observer after setup")]
    ObserverMissing,
}

/// Per-worker orchestrator that owns the replica lifecycle around each schedule.
pub struct Orchestrator<D>
where
    D: Deployment,
{
    deployment: D,
    scenario: Orders,
    // FIXME(#84): stopgap until an orchestrator state machine models the phases;
    // the observer only exists after setup.
    observer: Option<DbObserver>,
}

impl<D> Orchestrator<D>
where
    D: Deployment,
{
    pub fn new(deployment: D, scenario: Orders) -> Self {
        Self {
            deployment,
            scenario,
            observer: None,
        }
    }

    /// Bring the fleet replica up and wait for every service to become ready.
    pub async fn setup(&mut self) -> Result<(), D::Error> {
        self.deployment.setup().await?;
        self.deployment.wait_ready().await
    }

    pub fn set_observer(&mut self, observer: DbObserver) {
        self.observer = Some(observer);
    }

    pub fn deployment(&self) -> &D {
        &self.deployment
    }

    /// Run the scenario against the fleet and produce a verdict from the observations.
    pub async fn execute(&mut self, _schedule: &Schedule) -> Result<Verdict, Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let observer = self.observer.as_ref().ok_or(Error::ObserverMissing)?;
        let mut observations: Observations = self.scenario.run(api).await?;
        observer.observe(&mut observations).await?;
        Ok(driver_for(Invariant::Durable).drive(&observations))
    }

    /// Run the scenario fault-free and return the sessions observed by the sidecars.
    pub async fn learn(&mut self) -> Result<Vec<Session>, Error>
    where
        Error: From<<D as Deployment>::Error>,
    {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        self.scenario.run(api).await?;
        Ok(self.deployment.collect_sessions().await?)
    }

    /// Tear down the replica.
    pub async fn teardown(&mut self) -> Result<(), D::Error> {
        self.deployment.teardown().await
    }
}
