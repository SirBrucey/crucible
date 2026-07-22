//! Typestate machine driving the runner's side of one worker session.
//!
//! `Session<S>` carries the IPC stream and this runner's version across states;
//! `state: S` holds what changes. Transitions consume `self` and return the
//! next `Session<_>`; illegal sequences are compile errors.

use crucible_core::{
    event_bus::{EventBus, RunnerEvent},
    ipc::{
        RunnerToWorker, Session as ProxySession, WorkerToRunner,
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

pub enum DispatchNext {
    More(Session<AwaitingResult>),
    Done,
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
    pub async fn learn(
        mut self,
        bus: &EventBus,
    ) -> Result<(Session<Dispatching>, Vec<ProxySession>)> {
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
        let sessions = match &msg {
            WorkerToRunner::SessionCatalogue { sessions } => sessions.clone(),
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

        Ok((
            Session {
                stream: self.stream,
                runner_version: self.runner_version,
                state: Dispatching {
                    worker_id: self.state.worker_id,
                },
            },
            sessions,
        ))
    }

    pub async fn dispatch(
        mut self,
        bus: &EventBus,
        schedule: Option<Schedule>,
    ) -> Result<DispatchNext> {
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

        let outbound = schedule.map_or(RunnerToWorker::Shutdown, RunnerToWorker::from);
        let is_shutdown = matches!(outbound, RunnerToWorker::Shutdown);
        write_frame(&mut self.stream, &outbound).await?;
        bus.publish(RunnerEvent::RunnerMessage {
            worker_id: self.state.worker_id,
            message: outbound,
        })
        .await
        .expect("journal receiver alive");

        if is_shutdown {
            Ok(DispatchNext::Done)
        } else {
            Ok(DispatchNext::More(Session {
                stream: self.stream,
                runner_version: self.runner_version,
                state: AwaitingResult {
                    worker_id: self.state.worker_id,
                },
            }))
        }
    }
}

impl Session<AwaitingResult> {
    pub async fn await_result(mut self, bus: &EventBus) -> Result<Session<Dispatching>> {
        let msg = read_frame::<WorkerToRunner, _>(&mut self.stream).await?;
        if !matches!(&msg, WorkerToRunner::RunResult { .. }) {
            return Err(Error::UnexpectedMessage {
                state: "AwaitingResult",
                expected: "RunResult",
                got: format!("{msg:?}"),
            });
        }
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: self.state.worker_id,
            message: msg,
        })
        .await
        .expect("journal receiver alive");

        Ok(Session {
            stream: self.stream,
            runner_version: self.runner_version,
            state: Dispatching {
                worker_id: self.state.worker_id,
            },
        })
    }
}
