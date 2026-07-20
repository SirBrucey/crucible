//! Typestate machine driving the worker's side of the runner IPC protocol.
//!
//! `Worker<S>` carries per-worker identity and the IPC stream across states;
//! `state: S` holds what changes. Transitions consume `self` and return the
//! next `Worker<_>`; illegal sequences are compile errors.

use crucible_core::{
    deployment::Docker,
    fleet,
    ipc::{
        RunnerToWorker, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    orchestrator::Orchestrator,
    scheduler::Schedule,
};
use tokio::net::UnixStream;

use crate::error::{Error, Result};

pub struct Worker<S> {
    id: u32,
    version: String,
    stream: UnixStream,
    state: S,
}

pub struct Handshaking;

pub struct Idle {
    orchestrator: Orchestrator<Docker>,
}

pub struct Executing {
    orchestrator: Orchestrator<Docker>,
    schedule: Schedule,
}

pub struct ShuttingDown {
    orchestrator: Orchestrator<Docker>,
}

pub enum IdleNext {
    Work(Worker<Executing>),
    Shutdown(Worker<ShuttingDown>),
}

impl Worker<Handshaking> {
    pub fn new(stream: UnixStream, id: u32, version: String) -> Self {
        Self {
            id,
            version,
            stream,
            state: Handshaking,
        }
    }

    pub async fn handshake(mut self) -> Result<Worker<Idle>> {
        write_frame(
            &mut self.stream,
            &WorkerToRunner::Hello {
                worker_version: self.version.clone(),
                worker_id: self.id,
            },
        )
        .await?;
        match read_frame::<RunnerToWorker, _>(&mut self.stream).await? {
            RunnerToWorker::HelloAck { runner_version } => {
                tracing::info!(worker_id = self.id, %runner_version, "handshake ok");
                let docker = Docker::new(self.id, &fleet::EXAMPLE)?;
                let mut orchestrator = Orchestrator::new(docker);
                orchestrator.setup().await?;
                Ok(Worker {
                    id: self.id,
                    version: self.version,
                    stream: self.stream,
                    state: Idle { orchestrator },
                })
            }
            other => Err(Error::UnexpectedMessage {
                state: "Handshaking",
                expected: "HelloAck",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Worker<Idle> {
    pub async fn await_work(mut self) -> Result<IdleNext> {
        write_frame(&mut self.stream, &WorkerToRunner::Ready).await?;
        tracing::debug!(worker_id = self.id, "sent READY");

        match read_frame::<RunnerToWorker, _>(&mut self.stream).await? {
            RunnerToWorker::Schedule {
                schedule_id,
                invariant,
                payload,
            } => {
                tracing::info!(
                    worker_id = self.id,
                    schedule_id,
                    ?invariant,
                    "received schedule"
                );
                Ok(IdleNext::Work(Worker {
                    id: self.id,
                    version: self.version,
                    stream: self.stream,
                    state: Executing {
                        orchestrator: self.state.orchestrator,
                        schedule: Schedule {
                            schedule_id,
                            invariant,
                            payload,
                        },
                    },
                }))
            }
            RunnerToWorker::Shutdown => {
                tracing::info!(worker_id = self.id, "received shutdown");
                Ok(IdleNext::Shutdown(Worker {
                    id: self.id,
                    version: self.version,
                    stream: self.stream,
                    state: ShuttingDown {
                        orchestrator: self.state.orchestrator,
                    },
                }))
            }
            other @ RunnerToWorker::HelloAck { .. } => Err(Error::UnexpectedMessage {
                state: "Idle",
                expected: "Schedule or Shutdown",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Worker<Executing> {
    pub async fn execute_and_report(mut self) -> Result<Worker<Idle>> {
        let schedule_id = self.state.schedule.schedule_id;
        let verdict = self.state.orchestrator.execute(&self.state.schedule);
        write_frame(
            &mut self.stream,
            &WorkerToRunner::RunResult {
                schedule_id,
                verdict,
            },
        )
        .await?;
        tracing::info!(
            worker_id = self.id,
            schedule_id,
            ?verdict,
            "sent run result"
        );
        Ok(Worker {
            id: self.id,
            version: self.version,
            stream: self.stream,
            state: Idle {
                orchestrator: self.state.orchestrator,
            },
        })
    }
}

impl Worker<ShuttingDown> {
    pub async fn teardown(mut self) -> Result<()> {
        self.state.orchestrator.teardown().await?;
        tracing::info!(worker_id = self.id, "teardown complete");
        Ok(())
    }
}
