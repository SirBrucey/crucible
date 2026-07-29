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
        // A failed arm means the proxy never freezes, so the whole run would be
        // meaningless; surface it rather than pressing on.
        self.deployment.arm_anchor().await?;

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
                        // The target never reached its Kth packet; release the
                        // freeze and record a miss.
                        self.deployment.resume_proxies().await?;
                        return Ok::<_, Error>(missed(KillMissReason::ScenarioEndedBeforeAnchor));
                    }
                    // The proxy froze the fleet to place the kill precisely on the
                    // anchored packet. Kill the target, then release the flow and
                    // bring the target back concurrently: the scenario runs against
                    // the dead-then-recovering service in real time. Its real
                    // recovery time is part of the fault (ops in the outage window
                    // fail; ops once it is back succeed) rather than the world
                    // stopping while we heal.
                    let actual = scenario_start.elapsed().as_nanos();
                    Ok(fire_kill(&self.deployment, schedule, actual).await?)
                }
                _ = &mut scenario_end_rx => {
                    // Scenario finished before the target reached its Kth packet;
                    // release the freeze so nothing is left wedged.
                    self.deployment.resume_proxies().await?;
                    Ok(missed(KillMissReason::ScenarioEndedBeforeAnchor))
                }
            }
        };

        let (scenario_result, kill_result) = tokio::join!(scenario_fut, kill_fut);
        let kill_report = kill_result?;
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

/// Apply the kill against the already-frozen fleet and produce its report. Kill
/// the target, then release the freeze and bring the target back so the scenario
/// runs against the dead-then-recovering service.
///
/// Releasing the freeze and restarting the target must both succeed: otherwise
/// the fleet is left wedged or the target permanently dead, and the durability
/// verdict read afterwards would be meaningless. Those failures propagate as an
/// error rather than being reported as a successful kill. A failed kill, by
/// contrast, is a miss (nothing fired), not an error.
async fn fire_kill<D>(
    deployment: &D,
    schedule: &Schedule,
    actual_offset_ns: u128,
) -> Result<KillReport, D::Error>
where
    D: Deployment,
    D::Error: std::fmt::Display,
{
    let missed = |reason| KillReport {
        schedule_id: schedule.schedule_id,
        service: schedule.service.clone(),
        result: KillResult::Missed(reason),
    };

    let killed_at_ns = match deployment.kill_service(&schedule.service).await {
        Ok(killed_at_ns) => killed_at_ns,
        Err(e) => {
            // The kill never landed, so nothing is dead; release the freeze the
            // proxy is holding and report the miss. A resume failure here still
            // leaves the fleet wedged, so it is fatal.
            deployment.resume_proxies().await?;
            return Ok(missed(KillMissReason::KillFailed(e.to_string())));
        }
    };
    deployment.resume_proxies().await?;
    deployment.restart_service(&schedule.service).await?;
    Ok(KillReport {
        schedule_id: schedule.schedule_id,
        service: schedule.service.clone(),
        result: KillResult::Fired {
            requested_direction: schedule.direction,
            requested_packet_index: schedule.fault_packet_index,
            actual_offset_ns,
            killed_at_ns,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crucible_protocol::Direction;

    use super::*;

    #[derive(Debug)]
    struct FakeError(&'static str);

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    /// A deployment whose fault primitives can be told to fail, so `fire_kill`'s
    /// error handling can be exercised without a real fleet.
    #[derive(Default)]
    struct FakeDeployment {
        kill_fails: bool,
        resume_fails: bool,
        restart_fails: bool,
    }

    impl Deployment for FakeDeployment {
        type Error = FakeError;

        async fn setup(&mut self) -> Result<(), FakeError> {
            Ok(())
        }
        async fn wait_ready(&self) -> Result<(), FakeError> {
            Ok(())
        }
        async fn teardown(&mut self) -> Result<(), FakeError> {
            Ok(())
        }
        fn endpoint(&self, _name: &str) -> Option<SocketAddr> {
            None
        }
        async fn arm_anchor(&self) -> Result<(), FakeError> {
            Ok(())
        }
        async fn resume_proxies(&self) -> Result<(), FakeError> {
            if self.resume_fails {
                Err(FakeError("resume failed"))
            } else {
                Ok(())
            }
        }
        async fn kill_service(&self, _name: &str) -> Result<u128, FakeError> {
            if self.kill_fails {
                Err(FakeError("kill failed"))
            } else {
                Ok(42)
            }
        }
        async fn restart_service(&self, _name: &str) -> Result<u128, FakeError> {
            if self.restart_fails {
                Err(FakeError("restart failed"))
            } else {
                Ok(99)
            }
        }
    }

    fn schedule() -> Schedule {
        Schedule {
            schedule_id: 1,
            service: "db".into(),
            direction: Direction::ClientToUpstream,
            fault_packet_index: 3,
            payload: Vec::new(),
        }
    }

    #[tokio::test]
    async fn fire_kill_reports_fired_when_recovery_succeeds() {
        let deployment = FakeDeployment::default();
        let report = fire_kill(&deployment, &schedule(), 0)
            .await
            .expect("recovery succeeded, so fire_kill returns a report");
        assert!(matches!(report.result, KillResult::Fired { .. }));
    }

    #[tokio::test]
    async fn fire_kill_errors_when_restart_fails() {
        // A failed restart leaves the target dead; reporting Fired would compute a
        // durability verdict against a dead fleet, so it must surface as an error.
        let deployment = FakeDeployment {
            restart_fails: true,
            ..Default::default()
        };
        let result = fire_kill(&deployment, &schedule(), 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fire_kill_errors_when_resume_fails() {
        // A failed resume leaves the fleet frozen; the verdict would be read
        // against a wedged fleet, so it must surface as an error.
        let deployment = FakeDeployment {
            resume_fails: true,
            ..Default::default()
        };
        let result = fire_kill(&deployment, &schedule(), 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fire_kill_reports_miss_when_kill_fails() {
        // A failed kill fired nothing; that is a miss, not an infra error.
        let deployment = FakeDeployment {
            kill_fails: true,
            ..Default::default()
        };
        let report = fire_kill(&deployment, &schedule(), 0)
            .await
            .expect("kill-failure is a miss, not an error");
        assert!(matches!(
            report.result,
            KillResult::Missed(KillMissReason::KillFailed(_))
        ));
    }
}
