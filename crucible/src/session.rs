//! Typestate machine driving the runner's side of one worker session.
//!
//! `Session<S>` carries the IPC stream across states; `state: S` holds what
//! changes. Transitions consume `self` and return the next `Session<_>`; illegal
//! sequences are compile errors.

use crucible_core::{
    event_bus::{EventBus, RunnerEvent},
    ipc::{
        HEARTBEAT_TIMEOUT, RunnerToWorker, ServiceProfile, Verdict, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    scheduler::Schedule,
};
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
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id,
            message: hello,
        })
        .await
        .expect("journal receiver alive");

        let ack = RunnerToWorker::HelloAck {
            runner_version: self.state.runner_version.clone(),
        };
        write_frame(&mut self.stream, &ack).await?;
        bus.publish(RunnerEvent::RunnerMessage {
            worker_id,
            message: ack,
        })
        .await
        .expect("journal receiver alive");

        Ok(self.transition(Dispatching { worker_id }))
    }
}

impl Session<Dispatching> {
    pub async fn learn(mut self, bus: &EventBus) -> Result<Vec<ServiceProfile>> {
        let ready = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
        if !matches!(&ready, WorkerToRunner::Ready) {
            return Err(Error::UnexpectedMessage {
                state: "Learning",
                expected: "Ready",
                got: format!("{ready:?}"),
            });
        }
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: self.state.worker_id,
            message: ready,
        })
        .await
        .expect("journal receiver alive");

        let outbound = RunnerToWorker::Learn;
        write_frame(&mut self.stream, &outbound).await?;
        bus.publish(RunnerEvent::RunnerMessage {
            worker_id: self.state.worker_id,
            message: outbound,
        })
        .await
        .expect("journal receiver alive");

        let msg = read_live(&mut self.stream).await?;
        let services = match &msg {
            WorkerToRunner::SessionCatalogue { services } => services.clone(),
            other => {
                return Err(Error::UnexpectedMessage {
                    state: "Learning",
                    expected: "SessionCatalogue",
                    got: format!("{other:?}"),
                });
            }
        };
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: self.state.worker_id,
            message: msg,
        })
        .await
        .expect("journal receiver alive");

        Ok(services)
    }

    pub async fn dispatch(
        mut self,
        bus: &EventBus,
        schedule: Schedule,
    ) -> Result<Session<AwaitingResult>> {
        let ready = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
        if !matches!(&ready, WorkerToRunner::Ready) {
            return Err(Error::UnexpectedMessage {
                state: "Dispatching",
                expected: "Ready",
                got: format!("{ready:?}"),
            });
        }
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: self.state.worker_id,
            message: ready,
        })
        .await
        .expect("journal receiver alive");

        let outbound = RunnerToWorker::from(schedule);
        write_frame(&mut self.stream, &outbound).await?;
        bus.publish(RunnerEvent::RunnerMessage {
            worker_id: self.state.worker_id,
            message: outbound,
        })
        .await
        .expect("journal receiver alive");

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
            bus.publish(RunnerEvent::WorkerMessage {
                worker_id: self.state.worker_id,
                message: msg,
            })
            .await
            .expect("journal receiver alive");
            if let Some(v) = verdict {
                return Ok(v);
            }
        }
    }
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
