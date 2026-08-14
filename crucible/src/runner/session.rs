//! Typestate machine driving the runner's side of one worker session.
//!
//! `Session<S>` carries the IPC stream across states; `state: S` holds what
//! changes. Transitions consume `self` and return the next `Session<_>`; illegal
//! sequences are compile errors.

use crucible_core::{
    ipc::{
        HEARTBEAT_TIMEOUT, RunnerToWorker, Verdict, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    learned::Learned,
    schedule::Schedule,
};
use crucible_engine::event_bus::{EventBus, RunnerEvent};
use tokio::{net::UnixStream, time::timeout};

use crate::error::{Error, Result};

pub struct Session<S> {
    stream: UnixStream,
    state: S,
}

impl<S> Session<S> {
    /// Advance to the next state, carrying the IPC stream.
    fn transition<T>(self, state: T) -> Session<T> {
        Session {
            stream: self.stream,
            state,
        }
    }
}

pub struct Handshaking {
    runner_version: String,
}

pub struct Dispatching {
    worker_id: u32,
}

pub struct AwaitingResult {
    worker_id: u32,
}

impl Session<Handshaking> {
    pub fn new(stream: UnixStream, runner_version: String) -> Self {
        Self {
            stream,
            state: Handshaking { runner_version },
        }
    }

    pub async fn handshake(mut self, bus: &EventBus) -> Result<Session<Dispatching>> {
        let hello = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
        let worker_id = match &hello {
            WorkerToRunner::Hello {
                worker_id,
                worker_version,
            } => {
                // A worker built against a different framework version speaks a
                // protocol we cannot rely on; reject it with a clear error
                // rather than letting it fail later with an opaque decode error.
                if *worker_version != self.state.runner_version {
                    return Err(Error::VersionMismatch {
                        ours: self.state.runner_version.clone(),
                        theirs: worker_version.clone(),
                    });
                }
                *worker_id
            }
            other => {
                return Err(Error::UnexpectedMessage {
                    state: "Handshaking",
                    expected: "Hello",
                    got: format!("{other:?}"),
                });
            }
        };
        journal_in(bus, worker_id, hello).await;

        let ack = RunnerToWorker::HelloAck {
            runner_version: self.state.runner_version.clone(),
        };
        write_frame(&mut self.stream, &ack).await?;
        journal_out(bus, worker_id, ack).await;

        Ok(self.transition(Dispatching { worker_id }))
    }
}

impl Session<Dispatching> {
    /// Read the next frame, requiring it to be `Ready`, and journal it. `state`
    /// names the caller's state for the error message.
    async fn read_ready(&mut self, bus: &EventBus, state: &'static str) -> Result<()> {
        let ready = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
        if !matches!(&ready, WorkerToRunner::Ready) {
            return Err(Error::UnexpectedMessage {
                state,
                expected: "Ready",
                got: format!("{ready:?}"),
            });
        }
        journal_in(bus, self.state.worker_id, ready).await;
        Ok(())
    }

    /// Drive the fault-free run: the same schedule shape as any other, with
    /// nothing to break, so what it observes describes the workload the faulted
    /// runs perform. Reads frames until the catalogue arrives, journalling every
    /// intervening `Event(_)` as [`Session::<AwaitingResult>::await_result`]
    /// does.
    pub async fn learn(mut self, bus: &EventBus, schedule: Schedule) -> Result<Learned> {
        self.read_ready(bus, "Learning").await?;

        let outbound = RunnerToWorker::Run(schedule);
        write_frame(&mut self.stream, &outbound).await?;
        journal_out(bus, self.state.worker_id, outbound).await;

        loop {
            let msg = read_live(&mut self.stream).await?;
            let learned = match &msg {
                WorkerToRunner::SessionCatalogue(learned) => Some(learned.clone()),
                WorkerToRunner::Event(_) => None,
                other => {
                    return Err(Error::UnexpectedMessage {
                        state: "Learning",
                        expected: "SessionCatalogue or Event",
                        got: format!("{other:?}"),
                    });
                }
            };
            journal_in(bus, self.state.worker_id, msg).await;
            if let Some(learned) = learned {
                return Ok(learned);
            }
        }
    }

    pub async fn dispatch(
        mut self,
        bus: &EventBus,
        schedule: Schedule,
    ) -> Result<Session<AwaitingResult>> {
        self.read_ready(bus, "Dispatching").await?;

        let outbound = RunnerToWorker::Run(schedule);
        write_frame(&mut self.stream, &outbound).await?;
        journal_out(bus, self.state.worker_id, outbound).await;

        let worker_id = self.state.worker_id;
        Ok(self.transition(AwaitingResult { worker_id }))
    }
}

impl Session<AwaitingResult> {
    /// Read frames until `RunResult`; every intervening `Event(_)` is journaled
    /// via the bus, and heartbeats are consumed to reset the watchdog. A worker
    /// that sends nothing for [`HEARTBEAT_TIMEOUT`] is deemed unresponsive; an
    /// ill-behaved worker streaming frames forever is still bounded by the
    /// per-schedule deadline (see [`crate::wait_worker`]).
    pub async fn await_result(mut self, bus: &EventBus) -> Result<Verdict> {
        loop {
            let msg = read_live(&mut self.stream).await?;
            let verdict = match &msg {
                WorkerToRunner::RunResult { verdict, .. } => Some(verdict.clone()),
                WorkerToRunner::Event(_) => None,
                other => {
                    return Err(Error::UnexpectedMessage {
                        state: "AwaitingResult",
                        expected: "RunResult or Event",
                        got: format!("{other:?}"),
                    });
                }
            };
            journal_in(bus, self.state.worker_id, msg).await;
            if let Some(v) = verdict {
                return Ok(v);
            }
        }
    }
}

/// Record a frame received from the worker in the journal.
async fn journal_in(bus: &EventBus, worker_id: u32, message: WorkerToRunner) {
    bus.publish(RunnerEvent::WorkerMessage { worker_id, message })
        .await
        .expect("journal receiver alive");
}

/// Record a frame sent to the worker in the journal.
async fn journal_out(bus: &EventBus, worker_id: u32, message: RunnerToWorker) {
    bus.publish(RunnerEvent::RunnerMessage { worker_id, message })
        .await
        .expect("journal receiver alive");
}

/// Read the next non-heartbeat frame, treating each heartbeat as proof of life
/// that resets the watchdog. Returns [`Error::WorkerUnresponsive`] if nothing
/// arrives within [`HEARTBEAT_TIMEOUT`], i.e. the worker has hung or died.
async fn read_live(stream: &mut UnixStream) -> Result<WorkerToRunner> {
    loop {
        match timeout(HEARTBEAT_TIMEOUT, read_frame::<WorkerToRunner, _>(stream)).await {
            Ok(frame) => {
                let msg = frame?;
                if matches!(msg, WorkerToRunner::Heartbeat) {
                    continue;
                }
                return Ok(msg);
            }
            Err(_) => return Err(Error::WorkerUnresponsive(HEARTBEAT_TIMEOUT)),
        }
    }
}
