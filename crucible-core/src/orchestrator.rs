//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use std::time::{Duration, Instant};

use crucible_protocol::{KillMissReason, KillReport, KillResult, ServiceProfile, now_ns};

use crate::{
    deployment::{Deployment, Docker, docker},
    ipc::Verdict,
    observer::{self, DbObserver, SessionObserver},
    proxy_log::service_profiles_from_sessions,
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

    pub async fn setup(&mut self) -> Result<(), docker::Error> {
        self.deployment.setup().await?;
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

    /// Run the scenario with the schedule's kill fault firing at
    /// `fault_offset_ns` after scenario start; produce a verdict and report.
    pub async fn execute(&mut self, schedule: &Schedule) -> Result<(Verdict, KillReport), Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let db_observer = self.db_observer.as_ref().ok_or(Error::ObserverMissing)?;
        let session_observer = self
            .session_observer
            .as_ref()
            .ok_or(Error::ObserverMissing)?;

        let (scenario_end_tx, mut scenario_end_rx) = tokio::sync::oneshot::channel::<()>();
        let scenario_start = Instant::now();
        let scenario_fut = async {
            let result = self.scenario.run(api).await;
            let _ = scenario_end_tx.send(());
            result
        };

        let kill_fut = async {
            let sleep = Duration::from_nanos(
                u64::try_from(schedule.fault_offset_ns).expect("offset fits in u64"),
            );
            tokio::select! {
                biased;
                () = tokio::time::sleep(sleep) => match self.deployment.kill_service(&schedule.service).await {
                    Ok(killed_at_ns) => {
                        let actual = scenario_start.elapsed().as_nanos();
                        KillReport {
                            schedule_id: schedule.schedule_id,
                            service: schedule.service.clone(),
                            result: KillResult::Fired {
                                requested_offset_ns: schedule.fault_offset_ns,
                                actual_offset_ns: actual,
                                killed_at_ns,
                            },
                        }
                    }
                    Err(e) => KillReport {
                        schedule_id: schedule.schedule_id,
                        service: schedule.service.clone(),
                        result: KillResult::Missed(KillMissReason::KillFailed(e.to_string())),
                    },
                },
                _ = &mut scenario_end_rx => KillReport {
                    schedule_id: schedule.schedule_id,
                    service: schedule.service.clone(),
                    result: KillResult::Missed(KillMissReason::ScenarioEndedBeforeOffset),
                },
            }
        };

        let (scenario_result, kill_report) = tokio::join!(scenario_fut, kill_fut);
        let mut observations = scenario_result?;
        observations.kill = Some(kill_report.clone());

        if matches!(kill_report.result, KillResult::Fired { .. }) {
            self.deployment
                .restart_service(&kill_report.service)
                .await?;
        }

        db_observer.observe(&mut observations).await?;
        session_observer.observe(&mut observations);
        let verdict = driver_for(Invariant::Durable).drive(&observations);
        Ok((verdict, kill_report))
    }

    /// Run the scenario fault-free and return per-service work histograms so
    /// the scheduler can bin+cluster into bursts.
    pub async fn learn(&mut self) -> Result<Vec<ServiceProfile>, Error> {
        let api = self
            .deployment
            .endpoint("api")
            .expect("api endpoint present after setup");
        let session_observer = self
            .session_observer
            .as_ref()
            .ok_or(Error::ObserverMissing)?;
        let scenario_start_ns = now_ns();
        let mut observations: Observations = self.scenario.run(api).await?;
        session_observer.observe(&mut observations);
        Ok(service_profiles_from_sessions(
            &observations.sessions,
            scenario_start_ns,
        ))
    }

    pub async fn teardown(&mut self) -> Result<(), docker::Error> {
        if let Some(observer) = self.session_observer.take() {
            observer.shutdown().await;
        }
        self.deployment.teardown().await
    }
}
