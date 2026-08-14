//! Typestate machine driving the worker's side of the runner IPC protocol.
//!
//! `Worker<S>` carries per-worker identity and the IPC connection across states;
//! `state: S` holds what changes. Transitions consume `self` and return the
//! next `Worker<_>`; illegal sequences are compile errors.
//!
//! The fleet is brought up only after the worker knows its work, so a schedule
//! can arm the proxy's fault anchor on its command line at startup.

use std::sync::Arc;

use crucible_core::{
    fault::Fault,
    ipc::{
        HEARTBEAT_INTERVAL, RunnerToWorker, WorkerEvent, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    schedule::Schedule,
};
use crucible_engine::orchestrator::{Done, Orchestrator, Ready};
use crucible_plugin::Registry;
use tokio::{
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Mutex, Notify},
    time::interval,
};

use crate::error::{Error, Result};

/// The worker's IPC connection, split so a background heartbeat task can write
/// while the main flow reads. The write half is shared behind a mutex, which
/// serializes the heartbeat against the protocol frames the worker sends.
struct Conn {
    read: OwnedReadHalf,
    write: Arc<Mutex<OwnedWriteHalf>>,
}

impl Conn {
    fn new(stream: UnixStream) -> Self {
        let (read, write) = stream.into_split();
        Self {
            read,
            write: Arc::new(Mutex::new(write)),
        }
    }

    async fn send(&self, message: &WorkerToRunner) -> Result<()> {
        let mut write = self.write.lock().await;
        write_frame(&mut *write, message).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<RunnerToWorker> {
        Ok(read_frame(&mut self.read).await?)
    }

    /// Start heartbeating so the runner's watchdog sees the worker is alive
    /// through a long bring-up or run. The returned guard stops the task when
    /// dropped, i.e. once the caller's work is done.
    fn start_heartbeat(&self) -> Heartbeat {
        let write = self.write.clone();
        let stop = Arc::new(Notify::new());
        let signal = stop.clone();
        tokio::spawn(async move {
            let mut ticker = interval(HEARTBEAT_INTERVAL);
            loop {
                tokio::select! {
                    biased;
                    () = signal.notified() => break,
                    _ = ticker.tick() => {
                        let mut write = write.lock().await;
                        if write_frame(&mut *write, &WorkerToRunner::Heartbeat)
                            .await
                            .is_err()
                        {
                            // The runner has gone; nothing left to reassure.
                            break;
                        }
                    }
                }
            }
        });
        Heartbeat { stop }
    }
}

/// Stops its heartbeat task when dropped. It signals rather than aborts, so the
/// task always finishes any in-flight write and never leaves a half-written
/// frame on the wire.
struct Heartbeat {
    stop: Arc<Notify>,
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.stop.notify_one();
    }
}

pub struct Worker<S> {
    id: u32,
    conn: Conn,
    state: S,
}

impl<S> Worker<S> {
    /// Advance to the next state, carrying the worker's identity and connection.
    fn transition<T>(self, state: T) -> Worker<T> {
        Worker {
            id: self.id,
            conn: self.conn,
            state,
        }
    }
}

pub struct Handshaking {
    version: String,
}

pub struct Idle;

pub struct Learning {
    schedule: Schedule,
}

/// The fault is lifted out of the schedule on the way in, so reaching this
/// state is what proves there is one to inject.
pub struct Executing {
    schedule: Schedule,
    fault: Fault,
}

pub struct ShuttingDown {
    orchestrator: Orchestrator<Done>,
}

pub enum IdleNext {
    Learn(Worker<Learning>),
    Work(Worker<Executing>),
}

/// Build the fleet the schedule names and the orchestrator that runs its steps,
/// arming the proxy with the schedule's fault if it has one. Tears the replica
/// down on any bring-up failure so a worker that dies here (a flaky readiness
/// timeout, a crash) does not leak its containers.
async fn bring_up(
    registry: &Registry,
    worker_id: u32,
    schedule: &Schedule,
) -> Result<Orchestrator<Ready>> {
    let anchor = schedule.fault.as_ref().map(|fault| fault.anchor().clone());
    let deployment = registry.deployment_for(&schedule.fleet, worker_id, anchor)?;
    let actions = registry.actions_for(&schedule.steps)?;
    let orchestrator = Orchestrator::new(deployment, actions).setup().await?;
    Ok(orchestrator)
}

impl Worker<Handshaking> {
    pub fn new(stream: UnixStream, id: u32, version: String) -> Self {
        Self {
            id,
            conn: Conn::new(stream),
            state: Handshaking { version },
        }
    }

    pub async fn handshake(mut self) -> Result<Worker<Idle>> {
        self.conn
            .send(&WorkerToRunner::Hello {
                worker_version: self.state.version.clone(),
                worker_id: self.id,
            })
            .await?;
        match self.conn.recv().await? {
            RunnerToWorker::HelloAck { runner_version } => {
                // A runner built against a different framework version speaks a
                // protocol we cannot rely on; reject it with a clear error
                // rather than failing later with an opaque decode error.
                if runner_version != self.state.version {
                    return Err(Error::VersionMismatch {
                        ours: self.state.version,
                        theirs: runner_version,
                    });
                }
                tracing::info!(worker_id = self.id, %runner_version, "handshake ok");
                Ok(self.transition(Idle))
            }
            other @ RunnerToWorker::Run(_) => Err(Error::UnexpectedMessage {
                state: "Handshaking",
                expected: "HelloAck",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Worker<Idle> {
    pub async fn await_work(mut self) -> Result<IdleNext> {
        self.conn.send(&WorkerToRunner::Ready).await?;
        tracing::debug!(worker_id = self.id, "sent READY");

        match self.conn.recv().await? {
            RunnerToWorker::Run(schedule) => match schedule.fault.clone() {
                // A schedule with no fault is the fault-free run every other
                // schedule is judged against.
                None => {
                    tracing::info!(
                        worker_id = self.id,
                        schedule_id = schedule.id,
                        "received learn"
                    );
                    Ok(IdleNext::Learn(self.transition(Learning { schedule })))
                }
                Some(fault) => {
                    tracing::info!(
                        worker_id = self.id,
                        schedule_id = schedule.id,
                        service = fault.anchor().service,
                        invariant = ?fault.invariant(),
                        "received schedule"
                    );
                    Ok(IdleNext::Work(
                        self.transition(Executing { schedule, fault }),
                    ))
                }
            },
            other @ RunnerToWorker::HelloAck { .. } => Err(Error::UnexpectedMessage {
                state: "Idle",
                expected: "Run",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Worker<Learning> {
    pub async fn execute_learn(self) -> Result<Worker<ShuttingDown>> {
        let registry = Registry::load().await;
        let heartbeat = self.conn.start_heartbeat();
        let orchestrator = bring_up(&registry, self.id, &self.state.schedule).await?;
        let schedule = &self.state.schedule;
        // The checks are read at every step, so the fault-free run says what
        // state each step left behind rather than only where it ended.
        let queries = match registry
            .queries_for(&schedule.fleet, &schedule.checks)
            .await
        {
            Ok(queries) => queries,
            Err(e) => {
                let _ = orchestrator.teardown().await;
                return Err(e.into());
            }
        };
        let (services, trajectory, orchestrator) = orchestrator
            .learn(&queries)
            .await
            .inspect_err(|e| tracing::error!(worker_id = self.id, error = %e, "learn failed"))?;
        drop(heartbeat);
        let count = services.len();
        self.conn
            .send(&WorkerToRunner::SessionCatalogue {
                services,
                trajectory,
            })
            .await?;
        tracing::info!(worker_id = self.id, count, "sent session catalogue");
        Ok(self.transition(ShuttingDown { orchestrator }))
    }
}

impl Worker<Executing> {
    pub async fn execute_and_report(self) -> Result<Worker<ShuttingDown>> {
        let schedule = &self.state.schedule;
        let schedule_id = schedule.id;
        let registry = Registry::load().await;
        let heartbeat = self.conn.start_heartbeat();
        let orchestrator = bring_up(&registry, self.id, schedule).await?;

        // A check reads through the proxy's stable host port: it survives the
        // target being killed and restarted (the alias re-resolves to the new
        // container), where a direct ephemeral port would not. The anchor is
        // dormant during setup and released before a check reads, so the proxy
        // path is never actually frozen under it.
        let queries = match registry
            .queries_for(&schedule.fleet, &schedule.checks)
            .await
        {
            Ok(queries) => queries,
            Err(e) => {
                let _ = orchestrator.teardown().await;
                return Err(e.into());
            }
        };

        let ((verdict, kill_report), orchestrator) = orchestrator
            .execute(
                schedule_id,
                &self.state.fault,
                queries,
                schedule.trajectory.clone(),
            )
            .await
            .inspect_err(
                |e| tracing::error!(worker_id = self.id, schedule_id, error = %e, "execute failed"),
            )?;
        drop(heartbeat);
        self.conn
            .send(&WorkerToRunner::Event(WorkerEvent::Kill(
                kill_report.clone(),
            )))
            .await?;
        tracing::info!(
            worker_id = self.id,
            schedule_id,
            ?kill_report,
            "sent kill event"
        );
        self.conn
            .send(&WorkerToRunner::RunResult {
                schedule_id,
                verdict: verdict.clone(),
            })
            .await?;
        tracing::info!(
            worker_id = self.id,
            schedule_id,
            ?verdict,
            "sent run result"
        );
        Ok(self.transition(ShuttingDown { orchestrator }))
    }
}

impl Worker<ShuttingDown> {
    pub async fn teardown(self) -> Result<()> {
        self.state.orchestrator.teardown().await?;
        tracing::info!(worker_id = self.id, "teardown complete");
        Ok(())
    }
}
