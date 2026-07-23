//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use crucible_protocol::Session;

use crate::{
    deployment::{Deployment, Docker, docker},
    ipc::Verdict,
    observer::{self, DbObserver, SessionObserver},
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
pub struct Orchestrator {
    deployment: Docker,
    scenario: Orders,
    // FIXME(#84): stopgap until an orchestrator state machine models the phases;
    // the observers only exist after setup.
    db_observer: Option<DbObserver>,
    session_observer: Option<SessionObserver>,
}

impl Orchestrator {
    pub fn new(deployment: Docker, scenario: Orders) -> Self {
        Self {
            deployment,
            scenario,
            db_observer: None,
            session_observer: None,
        }
    }

    /// Bring the fleet replica up, wait for readiness, and start the session observer.
    pub async fn setup(&mut self) -> Result<(), docker::Error> {
        self.deployment.setup().await?;
        self.deployment.wait_ready().await?;
        self.session_observer = Some(self.deployment.start_session_observer());
        Ok(())
    }

    pub fn set_db_observer(&mut self, observer: DbObserver) {
        self.db_observer = Some(observer);
    }

    pub fn deployment(&self) -> &Docker {
        &self.deployment
    }

    /// Run the scenario against the fleet and produce a verdict from the observations.
    pub async fn execute(&mut self, _schedule: &Schedule) -> Result<Verdict, Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let db_observer = self.db_observer.as_ref().ok_or(Error::ObserverMissing)?;
        let session_observer = self
            .session_observer
            .as_mut()
            .ok_or(Error::ObserverMissing)?;
        let mut observations: Observations = self.scenario.run(api).await?;
        db_observer.observe(&mut observations).await?;
        session_observer.observe(&mut observations);
        Ok(driver_for(Invariant::Durable).drive(&observations))
    }

    /// Run the scenario fault-free and return the sessions observed by the sidecars.
    pub async fn learn(&mut self) -> Result<Vec<Session>, Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let session_observer = self
            .session_observer
            .as_mut()
            .ok_or(Error::ObserverMissing)?;
        let mut observations: Observations = self.scenario.run(api).await?;
        session_observer.observe(&mut observations);
        Ok(observations.sessions)
    }

    /// Tear down the replica and stop the session observer.
    pub async fn teardown(&mut self) -> Result<(), docker::Error> {
        if let Some(observer) = self.session_observer.take() {
            observer.shutdown().await;
        }
        self.deployment.teardown().await
    }
}
