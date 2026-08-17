//! L4 orchestrator: brings up the fleet replica, executes one schedule, tears it down.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crucible_protocol::{FaultMissReason, FaultReport, FaultResult, now_ns};

use crucible_core::{
    HEAL_BUDGET,
    fault::{Anchor, Fault, Primitive},
    ipc::Verdict,
    learned::Learned,
    observer::SessionObserver,
    proxy_log::service_profiles_from_sessions,
    verdict::{Checkpoint, Observations, Observed, StepWindow, driver_for},
};
use crucible_plugin::{
    Action, DeploymentRuntime, Kill, Substrate, Targeted, registry::PreparedCheck,
};

/// After a restart, wait this long before judging the fleet quiescent, so
/// recovery traffic has a chance to start.
const HEAL_MIN_SETTLE: Duration = Duration::from_millis(500);
/// Before the learn snapshot, wait at least this long so post-ack async writes
/// have a chance to begin before the fleet can be judged quiescent.
const LEARN_SETTLE: Duration = Duration::from_millis(100);
/// Between steps, wait at least this long before the fleet can be judged to
/// have finished with one, so work a step starts after answering its caller has
/// begun before anything looks for it.
const STEP_SETTLE: Duration = Duration::from_millis(100);
/// How long a step's effects have to settle before the next one starts. A fleet
/// still working after this is one the scenario has outrun, which is a result
/// rather than something to keep waiting on.
const STEP_BUDGET: Duration = Duration::from_secs(15);
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

/// Per-worker orchestrator that owns the replica lifecycle around one scenario,
/// modelled as a typestate so a phase can only reach for what earlier phases
/// produced. `New` holds the deployment and the actions to run; [`setup`] brings up
/// the fleet and its session observer to reach [`Ready`], from which the replica
/// runs exactly one scenario, fault-free via [`learn`] or with a fault via
/// [`execute`], leaving the orchestrator [`Done`] with only teardown remaining.
///
/// [`setup`]: Orchestrator::<New>::setup
/// [`learn`]: Orchestrator::<Ready>::learn
/// [`execute`]: Orchestrator::<Ready>::execute
pub struct Orchestrator<S> {
    deployment: Box<dyn DeploymentRuntime>,
    state: S,
}

/// Before the fleet is up, holding the scenario's actions until there is
/// somewhere to run them.
pub struct New {
    actions: Vec<Box<dyn Action>>,
}

/// An action and the address its target answers on.
type TargetedAction = (Box<dyn Action>, SocketAddr);

/// Fleet up and the session observer streaming: ready to run one scenario. Each
/// action carries the address its target answers on, resolved during setup.
pub struct Ready {
    session_observer: SessionObserver,
    actions: Vec<TargetedAction>,
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
            state: New { actions },
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
        let actions = match resolve_targets(self.deployment.as_ref(), self.state.actions) {
            Ok(actions) => actions,
            Err(e) => {
                session_observer.shutdown().await;
                let _ = self.deployment.teardown().await;
                return Err(e);
            }
        };
        Ok(Orchestrator {
            deployment: self.deployment,
            state: Ready {
                session_observer,
                actions,
            },
        })
    }
}

/// Pair each of `bound` with the address its target answers on.
fn resolve_targets<T: Targeted + ?Sized>(
    deployment: &dyn DeploymentRuntime,
    bound: Vec<Box<T>>,
) -> Result<Vec<(Box<T>, SocketAddr)>, crucible_plugin::Error> {
    bound
        .into_iter()
        .map(|bound| {
            let endpoint = endpoint_for_data_plane(deployment, bound.as_ref())?;
            Ok((bound, endpoint))
        })
        .collect()
}

/// The data plane address for whatever this is bound to. Steps drive the fleet
/// here, so the traffic counts as the fleet's.
fn endpoint_for_data_plane(
    deployment: &dyn DeploymentRuntime,
    bound: &(impl Targeted + ?Sized),
) -> Result<SocketAddr, crucible_plugin::Error> {
    let (target, kind) = (bound.target(), bound.kind());
    deployment
        .endpoint(target, kind)
        .ok_or_else(|| unreachable(target, kind))
}

/// The control plane address for whatever this is bound to. Checks read here, so
/// the reading is not mistaken for the fleet doing something.
fn endpoint_for_control_plane(
    deployment: &dyn DeploymentRuntime,
    bound: &(impl Targeted + ?Sized),
) -> Result<SocketAddr, crucible_plugin::Error> {
    let (target, kind) = (bound.target(), bound.kind());
    deployment
        .control_endpoint(target, kind)
        .ok_or_else(|| unreachable(target, kind))
}

/// A schedule reached a worker that the worker cannot carry out, which means the
/// scheduler and the plugins it asked disagree about what this fleet can do.
fn unreachable_fault(fault: &Fault) -> crucible_plugin::Error {
    crucible_plugin::Error::new(
        "orchestrator",
        format!(
            "a schedule was generated for `{:?}`, which cannot be run against this fleet",
            fault.invariant()
        ),
    )
}

fn unreachable(target: &str, kind: &str) -> crucible_plugin::Error {
    crucible_plugin::Error::new(
        "orchestrator",
        format!("the fleet published no endpoint for `{target}` speaking `{kind}`"),
    )
}

impl Orchestrator<Ready> {
    #[must_use]
    pub fn deployment(&self) -> &dyn DeploymentRuntime {
        self.deployment.as_ref()
    }

    /// Run the scenario fault-free, returning per-service profiles for the
    /// scheduler to cluster into bursts, and the state each step left behind
    /// for every other run to be judged against.
    ///
    /// # Errors
    /// Errors if the scenario fails to run against the fleet, or if a check
    /// cannot be read.
    pub async fn learn(
        self,
        queries: &[PreparedCheck],
        primitives: BTreeSet<Primitive>,
    ) -> Result<(Learned, Orchestrator<Done>), crucible_plugin::Error> {
        let Orchestrator {
            deployment,
            state: Ready {
                session_observer,
                actions,
            },
        } = self;
        let scenario_start_ns = now_ns();
        let mut observations = match run_actions(
            deployment.as_ref(),
            &actions,
            queries,
            &session_observer,
            Instant::now(),
        )
        .await
        {
            Ok(observations) => observations,
            Err(e) => {
                // A failed scenario leaves the replica up; tear it down
                // rather than dropping the only handle to it and its
                // observer tasks.
                let _ = teardown_replica(deployment, session_observer).await;
                return Err(e);
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
        let learned = Learned {
            profiles: service_profiles_from_sessions(&observations.sessions, scenario_start_ns),
            trajectory: observations.trajectory,
            primitives,
        };
        let done = Orchestrator {
            deployment,
            state: Done { session_observer },
        };
        Ok((learned, done))
    }

    /// Run the scenario; the proxy self-freezes the fleet once `fault`'s target
    /// has forwarded its Kth packet on its direction, then the kill lands
    /// against that held flow. The scenario's checks read what the fleet settled
    /// on. Produce a verdict and report, leaving the orchestrator [`Done`].
    ///
    /// # Errors
    /// Errors if arming, resuming, killing, or restarting the fleet fails, if the
    /// scenario fails to run, or if a check cannot be read.
    pub async fn execute(
        self,
        schedule_id: u32,
        fault: &Fault,
        queries: Vec<PreparedCheck>,
        fault_free: Vec<Checkpoint>,
    ) -> Result<((Verdict, FaultReport), Orchestrator<Done>), crucible_plugin::Error> {
        let Orchestrator {
            deployment,
            state: Ready {
                session_observer,
                actions,
            },
        } = self;
        let anchor = fault.anchor();
        // The scheduler only emits a schedule whose fault it can place and whose
        // verdict it can read, so both of these resolve for anything it produced.
        let placement = Placement::of(fault, deployment.as_ref())?;
        let mut driver = driver_for(fault.invariant()).ok_or_else(|| unreachable_fault(fault))?;

        // Run the fault sequence borrowing the replica handles, so whatever the
        // outcome we still own them afterwards: on success they move into `Done`
        // for the caller to tear down, on error we tear down here rather than
        // dropping the only handle to the replica and its observer tasks.
        let outcome: Result<(Verdict, FaultReport), crucible_plugin::Error> = async {
            // Arm the anchor as the scenario starts: the proxy resets and counts
            // scenario packets from here, and wait_for_freeze captures its observer
            // baseline at the same moment, so both share the scenario-start origin.
            // A failed arm means the proxy never freezes, so the whole run would be
            // meaningless; surface it rather than pressing on.
            deployment.substrate().arm_anchor().await?;

            let (scenario_end_tx, mut scenario_end_rx) = tokio::sync::oneshot::channel::<()>();
            let scenario_start = Instant::now();
            // The proxy timestamps the freeze on its own clock, so the fault is
            // timed against a wall-clock origin rather than against when this
            // side noticed.
            let scenario_start_ns = now_ns();
            let scenario_fut = async {
                let result = run_actions(
                    deployment.as_ref(),
                    &actions,
                    &queries,
                    &session_observer,
                    scenario_start,
                )
                .await;
                let _ = scenario_end_tx.send(());
                result
            };

            let fault_fut = async {
                let frozen = tokio::select! {
                    biased;
                    frozen = session_observer.wait_for_freeze(ANCHOR_TIMEOUT) => frozen,
                    // The scenario finished before a freeze was seen. The freeze is
                    // observed through the docker log stream, which lags real traffic,
                    // so one triggered by a post-ack edge may just not be visible yet.
                    _ = &mut scenario_end_rx => session_observer.wait_for_freeze(FREEZE_GRACE).await,
                };
                if !frozen {
                    // The target never reached its Kth packet. Let the fleet go in
                    // case a freeze is mid-flight, and record the miss.
                    deployment.substrate().abandon().await?;
                    return Ok::<_, crucible_plugin::Error>(missed(
                        schedule_id,
                        anchor,
                        FaultMissReason::ScenarioEndedBeforeAnchor,
                    ));
                }
                let placed = placed_at(&session_observer, scenario_start_ns);
                place(fault, placement, deployment.substrate(), schedule_id, placed).await
            };

            let (scenario_result, fault_result) = tokio::join!(scenario_fut, fault_fut);
            let fault_report = fault_result?;
            let mut observations = scenario_result?;
            observations.fault = Some(fault_report.clone());

            if matches!(fault_report.result, FaultResult::Fired { .. }) {
                // The target was restarted concurrently with the scenario; just wait
                // for the fleet to settle (giving any outbox redelivery its chance)
                // before reading the durability state.
                session_observer
                    .wait_for_quiescence(HEAL_MIN_SETTLE, QUIESCENCE_IDLE, HEAL_BUDGET)
                    .await;
            }

            observations.checks = read_checks(deployment.as_ref(), &queries).await?;
            observations.fault_free = fault_free;
            session_observer.observe(&mut observations);
            let verdict = driver.drive(&observations);
            Ok((verdict, fault_report))
        }
        .await;

        match outcome {
            Ok((verdict, fault_report)) => {
                let done = Orchestrator {
                    deployment,
                    state: Done { session_observer },
                };
                Ok(((verdict, fault_report), done))
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

/// A [`FaultReport`] for a fault that did not fire, for the given reason.
fn missed(id: u32, fault: &Anchor, reason: FaultMissReason) -> FaultReport {
    FaultReport {
        schedule_id: id,
        service: fault.service.clone(),
        result: FaultResult::Missed(reason),
    }
}

/// Read what the fleet settled on, one reading per check the scenario states.
async fn read_checks(
    deployment: &dyn DeploymentRuntime,
    queries: &[PreparedCheck],
) -> Result<Vec<Observed>, crucible_plugin::Error> {
    let mut readings = Vec::with_capacity(queries.len());
    for (check, query) in queries {
        let endpoint = endpoint_for_control_plane(deployment, query.as_ref())?;
        readings.push(Observed {
            value: query.read(endpoint).await?,
            check: check.clone(),
        });
    }
    Ok(readings)
}

/// What every check reads right now, for the trajectory rather than for a
/// verdict: the values alone, since which check each answers is the scenario's
/// and does not change between one point of a run and another.
async fn read_checkpoint(
    deployment: &dyn DeploymentRuntime,
    queries: &[PreparedCheck],
) -> Checkpoint {
    let mut checkpoint = Vec::with_capacity(queries.len());
    for (check, query) in queries {
        let reading = match endpoint_for_control_plane(deployment, query.as_ref()) {
            Ok(endpoint) => query.read(endpoint).await,
            Err(e) => Err(e),
        };
        checkpoint.push(match reading {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::debug!(
                    observable = %check.observable.join("."),
                    service = %check.service,
                    error = %e,
                    "state could not be read at this point of the run",
                );
                None
            }
        });
    }
    checkpoint
}

/// Run the scenario's steps one at a time, each starting only once the fleet
/// has stopped working on the one before.
///
/// A step's effects outlast its response: the caller is answered before a
/// consumer has read what the step published. Overlapping steps therefore leave
/// a state no one step accounts for, and the traffic a fault is anchored in
/// belongs to whichever step happened to be in flight.
async fn run_actions(
    deployment: &dyn DeploymentRuntime,
    actions: &[TargetedAction],
    queries: &[PreparedCheck],
    session_observer: &SessionObserver,
    scenario_start: Instant,
) -> Result<Observations, crucible_plugin::Error> {
    let mut observations = Observations::empty();
    // The state before anything ran, so a step that changed nothing has a
    // checkpoint equal to the one before it rather than no checkpoint at all.
    observations
        .trajectory
        .push(read_checkpoint(deployment, queries).await);
    for (action, endpoint) in actions {
        let start_ns = scenario_start.elapsed().as_nanos();
        observations.outcomes.push(action.run(*endpoint).await?);
        session_observer
            .wait_for_quiescence(STEP_SETTLE, QUIESCENCE_IDLE, STEP_BUDGET)
            .await;
        // The step is only over once the fleet has stopped working on it, so a
        // fault that lands on the consumer's half of the step is still that
        // step's.
        observations.windows.push(StepWindow {
            start_ns,
            end_ns: scenario_start.elapsed().as_nanos(),
        });
        observations
            .trajectory
            .push(read_checkpoint(deployment, queries).await);
    }
    Ok(observations)
}

/// When the fault landed and where that sits in the scenario, both by the
/// proxy's clock. Falling back to the observer's own reading only matters if the
/// proxy froze without saying when, which it does not do.
struct Placed {
    at_ns: u128,
    offset_ns: u128,
}

fn placed_at(session_observer: &SessionObserver, scenario_start_ns: u128) -> Placed {
    let at_ns = session_observer.froze_at_ns().unwrap_or_else(now_ns);
    Placed {
        at_ns,
        offset_ns: at_ns.saturating_sub(scenario_start_ns),
    }
}

/// How this schedule's fault gets placed, resolved before the scenario starts so
/// nothing needed mid-run can turn out to be missing.
enum Placement<'a> {
    /// The proxy holds the fleet while this side kills the service and brings it
    /// back.
    Kill(&'a dyn Kill),
    /// The proxy severs the edge itself and releases the fleet.
    Cut,
}

impl<'a> Placement<'a> {
    /// What the deployment must offer to place `fault`.
    fn of(
        fault: &Fault,
        deployment: &'a dyn DeploymentRuntime,
    ) -> Result<Self, crucible_plugin::Error> {
        match fault.primitive() {
            Primitive::Kill => deployment
                .kills()
                .map(Placement::Kill)
                .ok_or_else(|| unreachable_fault(fault)),
            // The substrate does this one on its own, so what it offers is all
            // there is to check.
            Primitive::Cut => deployment
                .substrate()
                .primitives()
                .contains(&Primitive::Cut)
                .then_some(Placement::Cut)
                .ok_or_else(|| unreachable_fault(fault)),
            Primitive::Redeliver | Primitive::Reorder => Err(unreachable_fault(fault)),
        }
    }
}

/// Place the fault against the already-frozen fleet and report it.
///
/// The kill releases the flow and restarts concurrently, so the scenario meets
/// the dead-then-recovering service in real time. Both must succeed or the fleet
/// is left wedged and the verdict would be meaningless, so they error. A failed
/// kill is a miss, not an error.
async fn place(
    fault: &Fault,
    placement: Placement<'_>,
    substrate: &dyn Substrate,
    id: u32,
    placed: Placed,
) -> Result<FaultReport, crucible_plugin::Error> {
    let anchor = fault.anchor();
    let placed_at_ns = match placement {
        Placement::Cut => placed.at_ns,
        Placement::Kill(kills) => match kills.kill(&anchor.service).await {
            Ok(placed_at_ns) => {
                substrate.proceed().await?;
                kills.restart(&anchor.service).await?;
                placed_at_ns
            }
            Err(e) => {
                // Nothing is dead, so let the fleet go and report the miss. A
                // failure here still leaves it wedged, so it is fatal.
                substrate.abandon().await?;
                return Ok(missed(id, anchor, FaultMissReason::Failed(e.to_string())));
            }
        },
    };
    Ok(FaultReport {
        schedule_id: id,
        service: anchor.service.clone(),
        result: FaultResult::Fired {
            by: fault.primitive(),
            requested_direction: anchor.direction,
            requested_packet_index: anchor.k,
            actual_offset_ns: placed.offset_ns,
            placed_at_ns,
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

    impl crucible_plugin::Faults for FakeDeployment {}

    impl Substrate for FakeDeployment {
        fn arm_anchor(&self) -> BoxFuture<'_, Result<(), crucible_plugin::Error>> {
            Box::pin(async { Ok(()) })
        }

        fn proceed(&self) -> BoxFuture<'_, Result<(), crucible_plugin::Error>> {
            Box::pin(async move {
                if self.resume_fails {
                    Err(fake("resume failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn abandon(&self) -> BoxFuture<'_, Result<(), crucible_plugin::Error>> {
            Box::pin(async move {
                if self.resume_fails {
                    Err(fake("abandon failed"))
                } else {
                    Ok(())
                }
            })
        }
    }

    impl Kill for FakeDeployment {
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

    /// A kill of `db`, which is what a burst schedule carries.
    fn fault() -> Fault {
        Fault::Durable {
            anchor: Anchor {
                service: "db".into(),
                direction: Direction::ClientToUpstream,
                k: 3,
            },
            by: Primitive::Kill,
        }
    }

    #[tokio::test]
    async fn a_fault_reports_fired_when_recovery_succeeds() {
        let deployment = FakeDeployment::default();
        let report = place(
            &fault(),
            Placement::Kill(&deployment),
            &deployment,
            1,
            Placed {
                at_ns: 42,
                offset_ns: 0,
            },
        )
        .await
        .expect("recovery succeeded, so a report comes back");
        assert!(matches!(report.result, FaultResult::Fired { .. }));
    }

    #[tokio::test]
    async fn a_kill_errors_when_restart_fails() {
        // A failed restart leaves the target dead; reporting Fired would compute a
        // durability verdict against a dead fleet, so it must surface as an error.
        let deployment = FakeDeployment {
            restart_fails: true,
            ..Default::default()
        };
        let result = place(
            &fault(),
            Placement::Kill(&deployment),
            &deployment,
            1,
            Placed {
                at_ns: 42,
                offset_ns: 0,
            },
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_kill_errors_when_resume_fails() {
        // A failed resume leaves the fleet frozen; the verdict would be read
        // against a wedged fleet, so it must surface as an error.
        let deployment = FakeDeployment {
            resume_fails: true,
            ..Default::default()
        };
        let result = place(
            &fault(),
            Placement::Kill(&deployment),
            &deployment,
            1,
            Placed {
                at_ns: 42,
                offset_ns: 0,
            },
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_fault_reports_miss_when_it_fails() {
        // A failed kill fired nothing; that is a miss, not an infra error.
        let deployment = FakeDeployment {
            kill_fails: true,
            ..Default::default()
        };
        let report = place(
            &fault(),
            Placement::Kill(&deployment),
            &deployment,
            1,
            Placed {
                at_ns: 42,
                offset_ns: 0,
            },
        )
        .await
        .expect("kill-failure is a miss, not an error");
        assert!(matches!(
            report.result,
            FaultResult::Missed(FaultMissReason::Failed(_))
        ));
    }
}
