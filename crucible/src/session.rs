//! Typestate machine driving the runner's side of one worker session.
//!
//! `Session<S>` carries the IPC stream and this runner's version across states;
//! `state: S` holds what changes. Transitions consume `self` and return the
//! next `Session<_>`; illegal sequences are compile errors.

use crucible_core::{
    event_bus::{EventBus, RunnerEvent},
    ipc::{
        RunnerToWorker, ServiceProfile, Verdict, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    scheduler::Schedule,
};
use tokio::net::UnixStream;

use crate::error::{Error, Result};

pub struct Session<S> {
    stream: UnixStream,
    runner_version: String,
    state: S,
}

pub struct Handshaking;

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
            runner_version,
            state: Handshaking,
        }
    }

    pub async fn handshake(mut self, bus: &EventBus) -> Result<Session<Dispatching>> {
        let hello = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
        let worker_id = match &hello {
            WorkerToRunner::Hello { worker_id, .. } => *worker_id,
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
            runner_version: self.runner_version.clone(),
        };
        write_frame(&mut self.stream, &ack).await?;
        bus.publish(RunnerEvent::RunnerMessage {
            worker_id,
            message: ack,
        })
        .await
        .expect("journal receiver alive");

        Ok(Session {
            stream: self.stream,
            runner_version: self.runner_version,
            state: Dispatching { worker_id },
        })
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

        let msg = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
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

        Ok(Session {
            stream: self.stream,
            runner_version: self.runner_version,
            state: AwaitingResult {
                worker_id: self.state.worker_id,
            },
        })
    }
}

impl Session<AwaitingResult> {
    /// Read frames until `RunResult`; every intervening `Event(_)` is journaled
    /// via the bus.
    ///
    /// SAFETY: an ill-behaved worker could stream `Event`s indefinitely and
    /// hang this loop. The runner runs each schedule inside a per-schedule
    /// wall-clock deadline (see [`crate::wait_worker`]), so an unbounded
    /// worker gets killed at the parent level.
    pub async fn await_result(mut self, bus: &EventBus) -> Result<Verdict> {
        loop {
            let msg = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
            let verdict = match &msg {
                WorkerToRunner::RunResult { verdict, .. } => Some(*verdict),
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
