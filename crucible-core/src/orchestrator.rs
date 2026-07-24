//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use std::time::Duration;

use crucible_protocol::{KillMissReason, KillReport, KillResult, Session, SessionRef, now_ns};

use crate::{
    deployment::{Deployment, Docker, docker},
    ipc::Verdict,
    observer::{self, DbObserver, SessionObserver, session::WaitError},
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
        // Start streaming before wait_ready so we capture app-boot pool
        // connections as they open, not after they've already flown.
        self.session_observer = Some(self.deployment.start_session_observer());
        self.deployment.wait_ready().await?;
        Ok(())
    }

    pub fn set_db_observer(&mut self, observer: DbObserver) {
        self.db_observer = Some(observer);
    }

    pub fn deployment(&self) -> &Docker {
        &self.deployment
    }

    /// Run the scenario against the fleet with the schedule's kill fault firing
    /// in parallel; produce a verdict and the kill report.
    pub async fn execute(&mut self, schedule: &Schedule) -> Result<(Verdict, KillReport), Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let db_observer = self.db_observer.as_ref().ok_or(Error::ObserverMissing)?;
        let session_observer_ref = self
            .session_observer
            .as_ref()
            .ok_or(Error::ObserverMissing)?;

        let (scenario_end_tx, mut scenario_end_rx) = tokio::sync::oneshot::channel::<()>();

        let scenario_fut = async {
            let result = self.scenario.run(api).await;
            let _ = scenario_end_tx.send(());
            result
        };

        let kill_fut = async {
            tokio::select! {
                biased;
                report = perform_kill(
                    session_observer_ref,
                    &self.deployment,
                    &schedule.session,
                    schedule.schedule_id,
                    schedule.fault_offset_ns,
                ) => report,
                _ = &mut scenario_end_rx => KillReport {
                    schedule_id: schedule.schedule_id,
                    session: schedule.session.clone(),
                    result: KillResult::Missed(KillMissReason::ScenarioEndedBeforeTargetOpened),
                },
            }
        };

        let (scenario_result, kill_report) = tokio::join!(scenario_fut, kill_fut);
        let mut observations = scenario_result?;
        observations.kill = Some(kill_report.clone());

        // Restart whenever the kill fired so post-fault observers can query.
        // The driver decides the verdict from observations.kill and the http /
        // db state; a run with no acked writes just yields Inconclusive.
        if matches!(kill_report.result, KillResult::Fired { .. }) {
            self.deployment
                .restart_service(&kill_report.session.service)
                .await?;
        }

        db_observer.observe(&mut observations).await?;
        self.session_observer
            .as_ref()
            .expect("session observer present after setup")
            .observe(&mut observations);
        let verdict = driver_for(Invariant::Durable).drive(&observations);
        Ok((verdict, kill_report))
    }

    /// Run the scenario fault-free and return the sessions observed by the sidecars.
    pub async fn learn(&mut self) -> Result<Vec<Session>, Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let session_observer = self
            .session_observer
            .as_ref()
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

async fn perform_kill(
    observer: &SessionObserver,
    deployment: &Docker,
    target: &SessionRef,
    schedule_id: u32,
    requested_offset_ns: u128,
) -> KillReport {
    let opened_at_ns = match observer.wait_for(&target.service, target.conn_id).await {
        Ok(ts) => ts,
        Err(WaitError::Closed) => {
            return KillReport {
                schedule_id,
                session: target.clone(),
                result: KillResult::Missed(KillMissReason::ObserverStreamClosed),
            };
        }
    };
    let elapsed = now_ns().saturating_sub(opened_at_ns);
    let sleep_ns = requested_offset_ns.saturating_sub(elapsed);
    let sleep = Duration::from_nanos(u64::try_from(sleep_ns).expect("offset fits in u64"));
    tokio::time::sleep(sleep).await;
    let killed_at_ns = match deployment.kill_service(&target.service).await {
        Ok(ts) => ts,
        Err(e) => {
            return KillReport {
                schedule_id,
                session: target.clone(),
                result: KillResult::Missed(KillMissReason::KillFailed(e.to_string())),
            };
        }
    };
    let actual_offset_ns = killed_at_ns.saturating_sub(opened_at_ns);
    KillReport {
        schedule_id,
        session: target.clone(),
        result: KillResult::Fired {
            opened_at_ns,
            killed_at_ns,
            requested_offset_ns,
            actual_offset_ns,
        },
    }
}
