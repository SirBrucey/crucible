//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use crucible_protocol::{KillMissReason, KillReport, KillResult, ServiceProfile, now_ns};

use crate::{
    deployment::{Deployment, Docker, docker, docker::HEAL_BUDGET},
    ipc::Verdict,
    observer::{self, DbObserver, SessionObserver},
    proxy_log::service_profiles_from_sessions,
    scenario::{self, Orders},
    scheduler::Schedule,
    verdict::{Invariant, driver_for},
};

/// After a restart, wait this long before judging the fleet quiescent, so
/// recovery traffic has a chance to start.
const HEAL_MIN_SETTLE: Duration = Duration::from_millis(500);
/// Before the learn snapshot, wait at least this long so post-ack async writes
/// have a chance to begin before the fleet can be judged quiescent.
const LEARN_SETTLE: Duration = Duration::from_millis(100);
/// Consider the fleet quiescent once no sidecar has forwarded traffic for this
/// long. Comfortably larger than a DB write plus docker-log delivery latency.
const QUIESCENCE_IDLE: Duration = Duration::from_secs(1);
/// Backstop for the fault anchor: if the target never reaches its Kth packet,
/// stop waiting. In practice the scenario ending fires first (a shorter path to
/// the same "missed" outcome); this only guards a pathological hang.
const ANCHOR_TIMEOUT: Duration = Duration::from_mins(1);
/// A freeze is observed through the docker log stream, which lags real traffic.
/// If the scenario ends before a freeze has been seen, wait this long for a
/// freeze triggered by a post-ack edge to become visible before concluding the
/// anchor was missed. Comfortably larger than docker-log delivery latency.
const FREEZE_GRACE: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Scenario(#[from] scenario::Error),
    #[error(transparent)]
    Observer(#[from] observer::Error),
    #[error(transparent)]
    Docker(#[from] docker::Error),
}

/// Per-worker orchestrator that owns the replica lifecycle around one scenario,
/// modelled as a typestate so a phase can only reach for what earlier phases
/// produced. `New` holds just the deployment and scenario; [`setup`] brings up
/// the fleet and its session observer to reach [`Ready`], from which the replica
/// runs exactly one scenario, fault-free via [`learn`] or with a fault via
/// [`execute`], leaving the orchestrator [`Done`] with only teardown remaining.
///
/// [`setup`]: Orchestrator::<New>::setup
/// [`learn`]: Orchestrator::<Ready>::learn
/// [`execute`]: Orchestrator::<Ready>::execute
pub struct Orchestrator<S> {
    deployment: Docker,
    scenario: Orders,
    state: S,
}

/// Before the fleet is up.
pub struct New;

/// Fleet up and the session observer streaming: ready to run one scenario. The
/// api endpoint setup published is captured here, so a scenario uses a
/// proven-present address rather than looking it up again.
pub struct Ready {
    session_observer: SessionObserver,
    api: SocketAddr,
}

/// The scenario has run; only teardown remains.
pub struct Done {
    session_observer: SessionObserver,
}

impl Orchestrator<New> {
    #[must_use]
    pub fn new(deployment: Docker, scenario: Orders) -> Self {
        Self {
            deployment,
            scenario,
            state: New,
        }
    }

    /// Bring up the fleet, start streaming its session observer, and capture the
    /// api endpoint. Tears the replica down on any failure so a half-built fleet
    /// does not leak.
    ///
    /// # Errors
    /// Errors if the fleet fails to come up or become ready, or if the api
    /// endpoint is absent once it has (which the fleet always publishes).
    pub async fn setup(mut self) -> Result<Orchestrator<Ready>, docker::Error> {
        if let Err(e) = self.deployment.setup().await {
            let _ = self.deployment.teardown().await;
            return Err(e);
        }
        let session_observer = self.deployment.start_session_observer();
        if let Err(e) = self.deployment.wait_ready().await {
            session_observer.shutdown().await;
            let _ = self.deployment.teardown().await;
            return Err(e);
        }
        let Some(api) = self.deployment.endpoint("api") else {
            session_observer.shutdown().await;
            let _ = self.deployment.teardown().await;
            return Err(docker::Error::EndpointMissing("api".to_string()));
        };
        Ok(Orchestrator {
            deployment: self.deployment,
            scenario: self.scenario,
            state: Ready {
                session_observer,
                api,
            },
        })
    }
}

impl Orchestrator<Ready> {
    #[must_use]
    pub fn deployment(&self) -> &Docker {
        &self.deployment
    }

    /// Run the scenario fault-free and return per-service profiles so the
    /// scheduler can cluster them into bursts.
    ///
    /// # Errors
    /// Errors if the scenario fails to run against the fleet.
    pub async fn learn(self) -> Result<(Vec<ServiceProfile>, Orchestrator<Done>), Error> {
        let Orchestrator {
            deployment,
            scenario,
            state: Ready {
                session_observer,
                api,
            },
        } = self;
        let scenario_start_ns = now_ns();
        let mut observations = match scenario.run(api).await {
            Ok(observations) => observations,
            Err(e) => {
                // A failed scenario leaves the replica up; tear it down rather
                // than dropping the only handle to it and its observer tasks.
                let _ = teardown_replica(deployment, session_observer).await;
                return Err(e.into());
            }
        };
        // Let the fleet fall quiescent before snapshotting, so writes that land
        // after their HTTP response is acked (the async consumer path) are
        // observed and yield anchors. Without this the snapshot is taken while
        // that traffic is still in flight, so the async write path gets no
        // anchors and the scheduler never faults it.
        session_observer
            .wait_for_quiescence(LEARN_SETTLE, QUIESCENCE_IDLE, HEAL_BUDGET)
            .await;
        session_observer.observe(&mut observations);
        let profiles = service_profiles_from_sessions(&observations.sessions, scenario_start_ns);
        let done = Orchestrator {
            deployment,
            scenario,
            state: Done { session_observer },
        };
        Ok((profiles, done))
    }

    /// Run the scenario; the proxy self-freezes the fleet once the target has
    /// forwarded the schedule's `fault_packet_index` packets on its direction,
    /// then the kill lands against that held flow. `db_observer` reads the
    /// durability state after the fleet settles. Produce a verdict and report,
    /// leaving the orchestrator [`Done`].
    ///
    /// # Errors
    /// Errors if arming, resuming, killing, or restarting the fleet fails, if the
    /// scenario fails to run, or if the durability state cannot be read.
    pub async fn execute(
        self,
        schedule: &Schedule,
        db_observer: DbObserver,
    ) -> Result<((Verdict, KillReport), Orchestrator<Done>), Error> {
        let Orchestrator {
            deployment,
            scenario,
            state: Ready {
                session_observer,
                api,
            },
        } = self;

        // Run the fault sequence borrowing the replica handles, so whatever the
        // outcome we still own them afterwards: on success they move into `Done`
        // for the caller to tear down, on error we tear down here rather than
        // dropping the only handle to the replica and its observer tasks.
        let outcome: Result<(Verdict, KillReport), Error> = async {
            // Arm the anchor as the scenario starts: the proxy resets and counts
            // scenario packets from here, and wait_for_freeze captures its observer
            // baseline at the same moment, so both share the scenario-start origin.
            // A failed arm means the proxy never freezes, so the whole run would be
            // meaningless; surface it rather than pressing on.
            deployment.arm_anchor().await?;

            let (scenario_end_tx, mut scenario_end_rx) = tokio::sync::oneshot::channel::<()>();
            let scenario_start = Instant::now();
            let scenario_fut = async {
                let result = scenario.run(api).await;
                let _ = scenario_end_tx.send(());
                result
            };

            let kill_fut = async {
                tokio::select! {
                    biased;
                    frozen = session_observer.wait_for_freeze(ANCHOR_TIMEOUT) => {
                        if !frozen {
                            // The proxy never reported freezing (the target did not
                            // reach its Kth packet); release the freeze in case it is
                            // mid-flight and record a miss.
                            deployment.resume_proxy().await?;
                            return Ok::<_, Error>(missed(schedule, KillMissReason::ScenarioEndedBeforeAnchor));
                        }
                        // The proxy froze the fleet to place the kill precisely on the
                        // anchored packet. Kill the target, then release the flow and
                        // bring the target back concurrently: the scenario runs against
                        // the dead-then-recovering service in real time. Its real
                        // recovery time is part of the fault (ops in the outage window
                        // fail; ops once it is back succeed) rather than the world
                        // stopping while we heal.
                        let actual = scenario_start.elapsed().as_nanos();
                        Ok(fire_kill(&deployment, schedule, actual).await?)
                    }
                    _ = &mut scenario_end_rx => {
                        // The scenario finished before a freeze was seen. The freeze
                        // is observed through the docker log stream, which lags real
                        // traffic, so a freeze triggered by a post-ack edge may just
                        // not be visible yet. Give it a short grace window before
                        // concluding the anchor was missed.
                        if session_observer.wait_for_freeze(FREEZE_GRACE).await {
                            let actual = scenario_start.elapsed().as_nanos();
                            Ok(fire_kill(&deployment, schedule, actual).await?)
                        } else {
                            // Genuinely no freeze; release it in case one is
                            // mid-flight and record the miss.
                            deployment.resume_proxy().await?;
                            Ok(missed(schedule, KillMissReason::ScenarioEndedBeforeAnchor))
                        }
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
                    .wait_for_quiescence(HEAL_MIN_SETTLE, QUIESCENCE_IDLE, HEAL_BUDGET)
                    .await;
            }

            db_observer.observe(&mut observations).await?;
            session_observer.observe(&mut observations);
            let verdict = driver_for(Invariant::Durable).drive(&observations);
            Ok((verdict, kill_report))
        }
        .await;

        match outcome {
            Ok((verdict, kill_report)) => {
                let done = Orchestrator {
                    deployment,
                    scenario,
                    state: Done { session_observer },
                };
                Ok(((verdict, kill_report), done))
            }
            Err(e) => {
                let _ = teardown_replica(deployment, session_observer).await;
                Err(e)
            }
        }
    }

    /// Shut down the observer and remove the fleet replica.
    ///
    /// # Errors
    /// Errors if the fleet's containers or network cannot be removed.
    pub async fn teardown(self) -> Result<(), docker::Error> {
        teardown_replica(self.deployment, self.state.session_observer).await
    }
}

impl Orchestrator<Done> {
    /// Shut down the observer and remove the fleet replica.
    ///
    /// # Errors
    /// Errors if the fleet's containers or network cannot be removed.
    pub async fn teardown(self) -> Result<(), docker::Error> {
        teardown_replica(self.deployment, self.state.session_observer).await
    }
}

/// Shut down the session observer, then remove the fleet replica.
async fn teardown_replica(
    mut deployment: Docker,
    session_observer: SessionObserver,
) -> Result<(), docker::Error> {
    session_observer.shutdown().await;
    deployment.teardown().await
}

/// A [`KillReport`] for a fault that did not fire, for the given reason.
fn missed(schedule: &Schedule, reason: KillMissReason) -> KillReport {
    KillReport {
        schedule_id: schedule.schedule_id,
        service: schedule.service.clone(),
        result: KillResult::Missed(reason),
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
    let killed_at_ns = match deployment.kill_service(&schedule.service).await {
        Ok(killed_at_ns) => killed_at_ns,
        Err(e) => {
            // The kill never landed, so nothing is dead; release the freeze the
            // proxy is holding and report the miss. A resume failure here still
            // leaves the fleet wedged, so it is fatal.
            deployment.resume_proxy().await?;
            return Ok(missed(schedule, KillMissReason::KillFailed(e.to_string())));
        }
    };
    deployment.resume_proxy().await?;
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
        async fn resume_proxy(&self) -> Result<(), FakeError> {
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
