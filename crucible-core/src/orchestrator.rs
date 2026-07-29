//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use std::time::{Duration, Instant};

use crucible_protocol::{KillMissReason, KillReport, KillResult, ServiceProfile, now_ns};

use crate::{
    deployment::{Deployment, Docker, docker, docker::HEAL_BUDGET},
    ipc::Verdict,
    observer::{self, DbObserver, SessionObserver},
    proxy_log::service_profiles_from_sessions,
    scenario::{self, Orders},
    scheduler::Schedule,
    verdict::{Invariant, Observations, driver_for},
};

/// After a restart, wait this long before judging the fleet quiescent, so
/// recovery traffic has a chance to start.
const HEAL_MIN_SETTLE: Duration = Duration::from_millis(500);
/// Consider the fleet quiescent once no sidecar has forwarded traffic for this
/// long. Comfortably larger than a DB write plus docker-log delivery latency.
const HEAL_QUIESCENCE_IDLE: Duration = Duration::from_secs(1);
/// Backstop for the fault anchor: if the target never reaches its Kth packet,
/// stop waiting. In practice the scenario ending fires first (a shorter path to
/// the same "missed" outcome); this only guards a pathological hang.
const ANCHOR_TIMEOUT: Duration = Duration::from_mins(1);

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

    /// Run the scenario; the proxy self-freezes the fleet once the target has
    /// forwarded the schedule's `fault_packet_index` packets on its direction,
    /// then the kill lands against that held flow. Produce a verdict and report.
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

        // Arm the anchor as the scenario starts: the proxy resets and counts
        // scenario packets from here, and wait_for_packet captures its observer
        // baseline at the same moment, so both share the scenario-start origin.
        let _ = self.deployment.arm_anchor().await;

        let (scenario_end_tx, mut scenario_end_rx) = tokio::sync::oneshot::channel::<()>();
        let scenario_start = Instant::now();
        let scenario_fut = async {
            let result = self.scenario.run(api).await;
            let _ = scenario_end_tx.send(());
            result
        };

        let missed = |reason| KillReport {
            schedule_id: schedule.schedule_id,
            service: schedule.service.clone(),
            result: KillResult::Missed(reason),
        };

        let kill_fut = async {
            tokio::select! {
                biased;
                reached = session_observer.wait_for_packet(
                    &schedule.service,
                    schedule.direction,
                    schedule.fault_packet_index,
                    ANCHOR_TIMEOUT,
                ) => {
                    if !reached {
                        // The target never reached its Kth packet; release any
                        // partial freeze and record a miss.
                        let _ = self.deployment.resume_proxies().await;
                        return missed(KillMissReason::ScenarioEndedBeforeAnchor);
                    }
                    // The proxy froze the fleet to place the kill precisely on the
                    // anchored packet. Kill the target, then release the flow
                    // immediately and bring the target back concurrently: the
                    // scenario runs against the dead-then-recovering service in real
                    // time. Its real recovery time is part of the fault (ops in the
                    // outage window fail; ops once it is back succeed) rather than
                    // the world stopping while we heal.
                    let killed = self.deployment.kill_service(&schedule.service).await;
                    match killed {
                        Ok(killed_at_ns) => {
                            let actual = scenario_start.elapsed().as_nanos();
                            let _ = self.deployment.resume_proxies().await;
                            let _ = self.deployment.restart_service(&schedule.service).await;
                            KillReport {
                                schedule_id: schedule.schedule_id,
                                service: schedule.service.clone(),
                                result: KillResult::Fired {
                                    requested_direction: schedule.direction,
                                    requested_packet_index: schedule.fault_packet_index,
                                    actual_offset_ns: actual,
                                    killed_at_ns,
                                },
                            }
                        }
                        Err(e) => {
                            let _ = self.deployment.resume_proxies().await;
                            missed(KillMissReason::KillFailed(e.to_string()))
                        }
                    }
                }
                _ = &mut scenario_end_rx => {
                    // Scenario finished before the target reached its Kth packet;
                    // release any freeze so nothing is left wedged.
                    let _ = self.deployment.resume_proxies().await;
                    missed(KillMissReason::ScenarioEndedBeforeAnchor)
                }
            }
        };

        let (scenario_result, kill_report) = tokio::join!(scenario_fut, kill_fut);
        let mut observations = scenario_result?;
        observations.kill = Some(kill_report.clone());

        if matches!(kill_report.result, KillResult::Fired { .. }) {
            // The target was restarted concurrently with the scenario; just wait
            // for the fleet to settle (giving any outbox redelivery its chance)
            // before reading the durability state.
            session_observer
                .wait_for_quiescence(HEAL_MIN_SETTLE, HEAL_QUIESCENCE_IDLE, HEAL_BUDGET)
                .await;
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
