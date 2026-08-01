//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crucible_protocol::{KillMissReason, KillReport, KillResult, ServiceProfile, now_ns};

use crucible_core::{
    HEAL_BUDGET,
    ipc::Verdict,
    observer::{self, DbObserver, SessionObserver},
    proxy_log::service_profiles_from_sessions,
    verdict::{Invariant, Observations, driver_for},
};
use crucible_plugin::{Action, DeploymentRuntime, FaultPrimitives};

use crate::scheduler::Schedule;

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
    Observer(#[from] observer::Error),
    #[error(transparent)]
    Plugin(#[from] crucible_plugin::Error),
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
    deployment: Box<dyn DeploymentRuntime>,
    actions: Vec<Box<dyn Action>>,
    state: S,
}

/// Before the fleet is up.
pub struct New;

/// Fleet up and the session observer streaming: ready to run one scenario. Every
/// action's target was resolved during setup, so running one uses a
/// proven-present address rather than looking it up again.
pub struct Ready {
    session_observer: SessionObserver,
    endpoints: HashMap<String, SocketAddr>,
}

/// The scenario has run; only teardown remains.
pub struct Done {
    session_observer: SessionObserver,
}

impl Orchestrator<New> {
    #[must_use]
    pub fn new(deployment: Box<dyn DeploymentRuntime>, actions: Vec<Box<dyn Action>>) -> Self {
        Self {
            deployment,
            actions,
            state: New,
        }
    }

    /// Bring up the fleet, start streaming its session observer, and resolve
    /// where every action's target is reachable. Tears the replica down on any
    /// failure so a half-built fleet does not leak.
    ///
    /// # Errors
    /// Errors if the fleet fails to come up or become ready, or if it publishes
    /// no endpoint for a service an action targets.
    pub async fn setup(mut self) -> Result<Orchestrator<Ready>, crucible_plugin::Error> {
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
        let endpoints = match self.resolve_targets() {
            Ok(endpoints) => endpoints,
            Err(e) => {
                session_observer.shutdown().await;
                let _ = self.deployment.teardown().await;
                return Err(e);
            }
        };
        Ok(Orchestrator {
            deployment: self.deployment,
            actions: self.actions,
            state: Ready {
                session_observer,
                endpoints,
            },
        })
    }

    /// Where each service an action targets is reachable.
    fn resolve_targets(&self) -> Result<HashMap<String, SocketAddr>, crucible_plugin::Error> {
        self.actions
            .iter()
            .map(|action| {
                let target = action.target();
                self.deployment
                    .endpoint(target)
                    .map(|endpoint| (target.to_owned(), endpoint))
                    .ok_or_else(|| {
                        crucible_plugin::Error::new(
                            "orchestrator",
                            format!("the fleet published no endpoint for `{target}`"),
                        )
                    })
            })
            .collect()
    }
}

impl Orchestrator<Ready> {
    #[must_use]
    pub fn deployment(&self) -> &dyn DeploymentRuntime {
        self.deployment.as_ref()
    }

    /// Run the scenario fault-free and return per-service profiles so the
    /// scheduler can cluster them into bursts.
    ///
    /// # Errors
    /// Errors if the scenario fails to run against the fleet.
    pub async fn learn(self) -> Result<(Vec<ServiceProfile>, Orchestrator<Done>), Error> {
        let Orchestrator {
            deployment,
            actions,
            state:
                Ready {
                    session_observer,
                    endpoints,
                },
        } = self;
        let scenario_start_ns = now_ns();
        let mut observations = match run_actions(&actions, &endpoints).await {
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
            actions,
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
            actions,
            state:
                Ready {
                    session_observer,
                    endpoints,
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
                let result = run_actions(&actions, &endpoints).await;
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
                            deployment.resume().await?;
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
                        Ok(fire_kill(deployment.as_ref(), schedule, actual).await?)
                    }
                    _ = &mut scenario_end_rx => {
                        // The scenario finished before a freeze was seen. The freeze
                        // is observed through the docker log stream, which lags real
                        // traffic, so a freeze triggered by a post-ack edge may just
                        // not be visible yet. Give it a short grace window before
                        // concluding the anchor was missed.
                        if session_observer.wait_for_freeze(FREEZE_GRACE).await {
                            let actual = scenario_start.elapsed().as_nanos();
                            Ok(fire_kill(deployment.as_ref(), schedule, actual).await?)
                        } else {
                            // Genuinely no freeze; release it in case one is
                            // mid-flight and record the miss.
                            deployment.resume().await?;
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
                    actions,
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
    pub async fn teardown(self) -> Result<(), crucible_plugin::Error> {
        teardown_replica(self.deployment, self.state.session_observer).await
    }
}

impl Orchestrator<Done> {
    /// Shut down the observer and remove the fleet replica.
    ///
    /// # Errors
    /// Errors if the fleet's containers or network cannot be removed.
    pub async fn teardown(self) -> Result<(), crucible_plugin::Error> {
        teardown_replica(self.deployment, self.state.session_observer).await
    }
}

/// Shut down the session observer, then remove the fleet replica.
async fn teardown_replica(
    mut deployment: Box<dyn DeploymentRuntime>,
    session_observer: SessionObserver,
) -> Result<(), crucible_plugin::Error> {
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

/// Run a scenario's actions in order against the fleet, collecting what each
/// one observed. Every action's target was resolved during setup, so the
/// endpoint is present.
async fn run_actions(
    actions: &[Box<dyn Action>],
    endpoints: &HashMap<String, SocketAddr>,
) -> Result<Observations, crucible_plugin::Error> {
    let mut observations = Observations::empty();
    for action in actions {
        let endpoint = endpoints[action.target()];
        observations.outcomes.push(action.run(endpoint).await?);
    }
    Ok(observations)
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
async fn fire_kill(
    deployment: &dyn FaultPrimitives,
    schedule: &Schedule,
    actual_offset_ns: u128,
) -> Result<KillReport, crucible_plugin::Error> {
    let killed_at_ns = match deployment.kill(&schedule.service).await {
        Ok(killed_at_ns) => killed_at_ns,
        Err(e) => {
            // The kill never landed, so nothing is dead; release the freeze the
            // proxy is holding and report the miss. A resume failure here still
            // leaves the fleet wedged, so it is fatal.
            deployment.resume().await?;
            return Ok(missed(schedule, KillMissReason::KillFailed(e.to_string())));
        }
    };
    deployment.resume().await?;
    deployment.restart(&schedule.service).await?;
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
    use crucible_plugin::role::BoxFuture;
    use crucible_protocol::Direction;

    use super::*;

    /// A deployment whose fault primitives can be told to fail, so `fire_kill`'s
    /// error handling can be exercised without a real fleet.
    #[derive(Default)]
    struct FakeDeployment {
        kill_fails: bool,
        resume_fails: bool,
        restart_fails: bool,
    }

    impl FaultPrimitives for FakeDeployment {
        fn arm_anchor(&self) -> BoxFuture<'_, Result<(), crucible_plugin::Error>> {
            Box::pin(async { Ok(()) })
        }

        fn resume(&self) -> BoxFuture<'_, Result<(), crucible_plugin::Error>> {
            Box::pin(async move {
                if self.resume_fails {
                    Err(fake("resume failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn kill(&self, _service: &str) -> BoxFuture<'_, Result<u128, crucible_plugin::Error>> {
            Box::pin(async move {
                if self.kill_fails {
                    Err(fake("kill failed"))
                } else {
                    Ok(42)
                }
            })
        }

        fn restart(&self, _service: &str) -> BoxFuture<'_, Result<u128, crucible_plugin::Error>> {
            Box::pin(async move {
                if self.restart_fails {
                    Err(fake("restart failed"))
                } else {
                    Ok(99)
                }
            })
        }
    }

    fn fake(message: &'static str) -> crucible_plugin::Error {
        crucible_plugin::Error::new("fake", message)
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
