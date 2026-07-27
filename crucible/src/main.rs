mod error;
mod session;

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use crucible_core::{
    deployment::docker::HEAL_BUDGET,
    event_bus::EventBus,
    ipc::Verdict,
    journal,
    scheduler::{BurstScheduler, Schedule, Scheduler},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixListener,
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    error::{Error, Result},
    session::{Dispatching, Session},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const TOTAL_BUDGET: Duration = Duration::from_mins(5);
const SCHEDULE_MARGIN: Duration = Duration::from_secs(30);
const LEARN_MARGIN: Duration = Duration::from_secs(30);

fn worker_bin_path() -> Result<PathBuf> {
    let runner = std::env::current_exe()?;
    let mut path = runner
        .parent()
        .ok_or(Error::RunnerExeParentless)?
        .to_path_buf();
    path.push("crucible-worker");
    Ok(path)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let result = run().await;
    if let Err(e) = &result {
        tracing::error!(error = %e, "runner exiting with error");
    }
    result
}

async fn run() -> Result<()> {
    let socket_path = PathBuf::from(format!("/tmp/crucible-{}.sock", std::process::id()));
    let _ = tokio::fs::remove_file(&socket_path).await;

    let (bus, journal_rx) = EventBus::new();

    let journal_path = journal::default_path(std::process::id());
    tracing::info!(path = %journal_path.display(), "journal ready");
    let journal_task = tokio::spawn(journal::run(journal_rx, journal_path));

    let mut observer_rx = bus.subscribe();
    let observer_task = tokio::spawn(async move {
        while let Ok(event) = observer_rx.recv().await {
            tracing::info!(target: "observer", event = ?event, "");
        }
    });

    let listener = UnixListener::bind(&socket_path)?;
    tracing::info!(socket = %socket_path.display(), "listening");

    let workload = drive(&listener, &bus, &socket_path).await;

    let _ = tokio::fs::remove_file(&socket_path).await;
    drop(bus);
    journal_task.await.expect("journal task should not panic")?;
    let _ = observer_task.await;

    workload
}

async fn drive(listener: &UnixListener, bus: &EventBus, socket_path: &Path) -> Result<()> {
    let campaign_start = Instant::now();
    let mut worker_id: u32 = 0;

    let (mut child, stderr_relay) = spawn_worker(socket_path, worker_id)?;
    let session = accept_and_handshake(listener, bus).await?;
    let learn_start = Instant::now();
    let services = session.learn(bus).await?;
    let run_cost = learn_start.elapsed();
    tracing::info!(
        services = services.len(),
        run_cost_ms = run_cost.as_millis(),
        "session catalogue received"
    );
    wait_worker(&mut child, stderr_relay, run_cost + LEARN_MARGIN).await?;
    worker_id += 1;

    let schedule_budget = run_cost + HEAL_BUDGET + SCHEDULE_MARGIN;
    let mut scheduler = BurstScheduler::new(&services);
    let total = scheduler.total();
    let mut ran: usize = 0;
    let mut failed: usize = 0;
    // TOTAL_BUDGET is a wall-clock cap: dispatch schedules until it is reached
    // (the in-flight schedule at the cap runs to completion, so the campaign
    // overshoots by at most one schedule). Schedules are emitted round-robin
    // across bursts, so a truncated campaign still samples every burst evenly.
    while campaign_start.elapsed() < TOTAL_BUDGET {
        let Some(schedule) = scheduler.next() else {
            break;
        };
        let schedule_id = schedule.schedule_id;
        // A single worker's failure (a flaky bring-up, a crash) is recorded
        // against its schedule and the campaign moves on, rather than aborting.
        match run_one_schedule(
            listener,
            bus,
            socket_path,
            worker_id,
            schedule,
            schedule_budget,
        )
        .await
        {
            Ok(verdict) => {
                tracing::info!(schedule_id, ?verdict, "run result");
                ran += 1;
            }
            Err(e) => {
                tracing::warn!(schedule_id, error = %e, "schedule failed; continuing campaign");
                failed += 1;
            }
        }
        worker_id += 1;
    }

    let elapsed_s = campaign_start.elapsed().as_secs();
    if ran + failed < total {
        tracing::warn!(
            ran,
            failed,
            total,
            elapsed_s,
            budget_s = TOTAL_BUDGET.as_secs(),
            "campaign hit its wall-clock budget; remaining schedules skipped"
        );
    } else {
        tracing::info!(ran, failed, total, elapsed_s, "campaign complete");
    }

    Ok(())
}

/// Run one schedule on a fresh worker and return its verdict. Reaps the worker
/// on every path so a failure cannot leave a zombie. A pipeline error or a
/// worker that exceeds `schedule_budget` is returned as `Err` for the caller to
/// record; only the failing schedule is affected.
async fn run_one_schedule(
    listener: &UnixListener,
    bus: &EventBus,
    socket_path: &Path,
    worker_id: u32,
    schedule: Schedule,
    schedule_budget: Duration,
) -> Result<Verdict> {
    let (mut child, stderr_relay) = spawn_worker(socket_path, worker_id)?;
    let pipeline = async {
        let session = accept_and_handshake(listener, bus).await?;
        session
            .dispatch(bus, schedule)
            .await?
            .await_result(bus)
            .await
    };
    match tokio::time::timeout(schedule_budget, pipeline).await {
        Ok(Ok(verdict)) => {
            // Success: let the worker finish its teardown and exit cleanly.
            wait_worker(&mut child, stderr_relay, schedule_budget).await?;
            Ok(verdict)
        }
        Ok(Err(e)) => {
            reap_worker(&mut child, stderr_relay).await;
            Err(e)
        }
        Err(_) => {
            reap_worker(&mut child, stderr_relay).await;
            Err(Error::WorkerTimeout(schedule_budget))
        }
    }
}

/// Kill (if still alive) and wait on a worker child, draining its stderr relay,
/// so a failed schedule leaves no zombie process behind.
async fn reap_worker(child: &mut Child, stderr_relay: JoinHandle<()>) {
    let _ = child.kill().await;
    let _ = child.wait().await;
    let _ = stderr_relay.await;
}

fn spawn_worker(socket_path: &Path, worker_id: u32) -> Result<(Child, JoinHandle<()>)> {
    let mut command = Command::new(worker_bin_path()?);
    command
        .arg("--socket")
        .arg(socket_path)
        .arg("--worker-id")
        .arg(worker_id.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped());
    // SAFETY: `pre_exec` runs after `fork()` and before `exec()` in the child.
    // `prctl(PR_SET_PDEATHSIG, SIGKILL)` sets a per-process flag with no aliasing
    // or shared-state concerns, and its side effect (kill worker if runner dies)
    // is exactly the intent.
    unsafe {
        command.pre_exec(|| {
            let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            if ret == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    let mut child = command.spawn()?;
    let child_pid = child.id().ok_or(Error::ChildPidMissing)?;
    tracing::info!(worker_id, pid = child_pid, "spawned worker");

    let worker_stderr = child
        .stderr
        .take()
        .expect("stderr set to piped so child has one");
    let stderr_relay = tokio::spawn(async move {
        let mut lines = BufReader::new(worker_stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("{line}");
        }
    });

    Ok((child, stderr_relay))
}

async fn accept_and_handshake(
    listener: &UnixListener,
    bus: &EventBus,
) -> Result<Session<Dispatching>> {
    let (stream, _addr) = timeout(HANDSHAKE_TIMEOUT, listener.accept())
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    tracing::info!("accepted worker connection");

    timeout(
        HANDSHAKE_TIMEOUT,
        Session::new(stream, env!("CARGO_PKG_VERSION").to_string()).handshake(bus),
    )
    .await
    .map_err(|_| Error::HandshakeTimeout)?
}

async fn wait_worker(
    child: &mut Child,
    stderr_relay: JoinHandle<()>,
    deadline: Duration,
) -> Result<()> {
    let Ok(status) = tokio::time::timeout(deadline, child.wait()).await else {
        tracing::error!(?deadline, "worker exceeded wall-clock budget; killing");
        let _ = child.kill().await;
        let _ = stderr_relay.await;
        return Err(Error::WorkerTimeout(deadline));
    };
    let status = status?;
    tracing::info!(?status, "worker exited");
    let _ = stderr_relay.await;
    if status.success() {
        Ok(())
    } else {
        Err(Error::WorkerExitedNonZero(status))
    }
}
