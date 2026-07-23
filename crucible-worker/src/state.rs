//! Typestate machine driving the worker's side of the runner IPC protocol.
//!
//! `Worker<S>` carries per-worker identity and the IPC stream across states;
//! `state: S` holds what changes. Transitions consume `self` and return the
//! next `Worker<_>`; illegal sequences are compile errors.

use crucible_core::{
    deployment::{Deployment, Docker},
    fleet,
    ipc::{
        RunnerToWorker, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    observer::DbObserver,
    orchestrator::Orchestrator,
    scenario::Orders,
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

pub struct Learning {
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
    Learn(Worker<Learning>),
    Work(Worker<Executing>),
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
                let mut orchestrator =
                    Orchestrator::new(Docker::new(self.id, &fleet::EXAMPLE)?, Orders::new()?);
                orchestrator.setup().await?;
                let db_addr = orchestrator
                    .deployment()
                    .endpoint("db")
                    .expect("db endpoint present after setup");
                let db_url = format!("mysql://root@{db_addr}/orders");
                orchestrator.set_observer(DbObserver::connect(&db_url).await?);
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
            RunnerToWorker::Learn => {
                tracing::info!(worker_id = self.id, "received learn");
                Ok(IdleNext::Learn(Worker {
                    id: self.id,
                    version: self.version,
                    stream: self.stream,
                    state: Learning {
                        orchestrator: self.state.orchestrator,
                    },
                }))
            }
            RunnerToWorker::Schedule {
                schedule_id,
                session,
                fault_offset_ns,
                payload,
            } => {
                tracing::info!(
                    worker_id = self.id,
                    schedule_id,
                    service = %session.service,
                    conn_id = session.conn_id,
                    fault_offset_ns,
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
                            session,
                            fault_offset_ns,
                            payload,
                        },
                    },
                }))
            }
            other @ RunnerToWorker::HelloAck { .. } => Err(Error::UnexpectedMessage {
                state: "Idle",
                expected: "Learn or Schedule",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Worker<Learning> {
    pub async fn execute_learn(mut self) -> Result<Worker<ShuttingDown>> {
        let sessions =
            self.state.orchestrator.learn().await.inspect_err(
                |e| tracing::error!(worker_id = self.id, error = %e, "learn failed"),
            )?;
        let count = sessions.len();
        write_frame(
            &mut self.stream,
            &WorkerToRunner::SessionCatalogue { sessions },
        )
        .await?;
        tracing::info!(worker_id = self.id, count, "sent session catalogue");
        Ok(Worker {
            id: self.id,
            version: self.version,
            stream: self.stream,
            state: ShuttingDown {
                orchestrator: self.state.orchestrator,
            },
        })
    }
}

impl Worker<Executing> {
    pub async fn execute_and_report(mut self) -> Result<Worker<ShuttingDown>> {
        let schedule_id = self.state.schedule.schedule_id;
        let verdict = self
            .state
            .orchestrator
            .execute(&self.state.schedule)
            .await
            .inspect_err(
                |e| tracing::error!(worker_id = self.id, schedule_id, error = %e, "execute failed"),
            )?;
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
            state: ShuttingDown {
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
