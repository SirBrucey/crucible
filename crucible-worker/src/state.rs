//! Typestate machine driving the worker's side of the runner IPC protocol.
//!
//! `Worker<S>` carries per-worker identity and the IPC stream across states;
//! `state: S` holds what changes. Transitions consume `self` and return the
//! next `Worker<_>`; illegal sequences are compile errors.
//!
//! The fleet is brought up only after the worker knows its work, so a schedule
//! can arm the proxy's fault anchor on its command line at startup.

use crucible_core::{
    deployment::{Deployment, Docker, ProxyAnchor},
    fleet,
    ipc::{
        RunnerToWorker, WorkerEvent, WorkerToRunner,
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

pub struct Idle;

pub struct Learning;

pub struct Executing {
    schedule: Schedule,
}

pub struct ShuttingDown {
    orchestrator: Orchestrator,
}

pub enum IdleNext {
    Learn(Worker<Learning>),
    Work(Worker<Executing>),
}

/// Build the fleet and the orchestrator, arming the proxy with `anchor` if set.
/// Tears the replica down on any bring-up failure so a worker that dies here (a
/// flaky readiness timeout, a crash) does not leak its containers.
async fn bring_up(worker_id: u32, anchor: Option<ProxyAnchor>) -> Result<Orchestrator> {
    let mut orchestrator = Orchestrator::new(
        Docker::new(worker_id, &fleet::EXAMPLE, anchor)?,
        Orders::new()?,
    );
    if let Err(e) = orchestrator.setup().await {
        let _ = orchestrator.teardown().await;
        return Err(e.into());
    }
    Ok(orchestrator)
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
                Ok(Worker {
                    id: self.id,
                    version: self.version,
                    stream: self.stream,
                    state: Idle,
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
                    state: Learning,
                }))
            }
            RunnerToWorker::Schedule {
                schedule_id,
                service,
                direction,
                fault_packet_index,
                payload,
            } => {
                tracing::info!(
                    worker_id = self.id,
                    schedule_id,
                    %service,
                    ?direction,
                    fault_packet_index,
                    "received schedule"
                );
                Ok(IdleNext::Work(Worker {
                    id: self.id,
                    version: self.version,
                    stream: self.stream,
                    state: Executing {
                        schedule: Schedule {
                            schedule_id,
                            service,
                            direction,
                            fault_packet_index,
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
        let mut orchestrator = bring_up(self.id, None).await?;
        let services = orchestrator
            .learn()
            .await
            .inspect_err(|e| tracing::error!(worker_id = self.id, error = %e, "learn failed"))?;
        let count = services.len();
        write_frame(
            &mut self.stream,
            &WorkerToRunner::SessionCatalogue { services },
        )
        .await?;
        tracing::info!(worker_id = self.id, count, "sent session catalogue");
        Ok(Worker {
            id: self.id,
            version: self.version,
            stream: self.stream,
            state: ShuttingDown { orchestrator },
        })
    }
}

impl Worker<Executing> {
    pub async fn execute_and_report(mut self) -> Result<Worker<ShuttingDown>> {
        let schedule = &self.state.schedule;
        let schedule_id = schedule.schedule_id;
        let anchor = ProxyAnchor {
            service: schedule.service.clone(),
            direction: schedule.direction,
            k: schedule.fault_packet_index,
        };
        let mut orchestrator = bring_up(self.id, Some(anchor)).await?;

        // Read the database through the proxy's stable host port: it survives the
        // db being killed and restarted (the alias re-resolves to the new
        // container), where a direct ephemeral port would not. The anchor is
        // dormant during setup and released before this observer queries, so the
        // proxy path is never actually frozen under it.
        let db_addr = orchestrator
            .deployment()
            .endpoint("db")
            .expect("db endpoint present after setup");
        let db_url = format!("mysql://root@{db_addr}/orders");
        let db_observer = match DbObserver::connect(&db_url).await {
            Ok(observer) => observer,
            Err(e) => {
                let _ = orchestrator.teardown().await;
                return Err(e.into());
            }
        };
        orchestrator.set_db_observer(db_observer);

        let (verdict, kill_report) = orchestrator
            .execute(&self.state.schedule)
            .await
            .inspect_err(
                |e| tracing::error!(worker_id = self.id, schedule_id, error = %e, "execute failed"),
            )?;
        write_frame(
            &mut self.stream,
            &WorkerToRunner::Event(WorkerEvent::Kill(kill_report.clone())),
        )
        .await?;
        tracing::info!(
            worker_id = self.id,
            schedule_id,
            ?kill_report,
            "sent kill event"
        );
        write_frame(
            &mut self.stream,
            &WorkerToRunner::RunResult {
                schedule_id,
                verdict: verdict.clone(),
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
            state: ShuttingDown { orchestrator },
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
