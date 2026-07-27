mod error;
mod session;

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use crucible_core::{
    deployment::docker::{Docker, HEAL_BUDGET},
    event_bus::EventBus,
    fleet,
    ipc::{ServiceProfile, Verdict},
    journal,
    scheduler::{BurstScheduler, Schedule, Scheduler},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixListener,
    process::{Child, Command},
    task::{JoinHandle, JoinSet},
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
/// Number of schedule workers (each with its own fleet replica) to run at once.
/// Overridable with `CRUCIBLE_CONCURRENCY`.
const DEFAULT_CONCURRENCY: usize = 3;

fn concurrency() -> usize {
    std::env::var("CRUCIBLE_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_CONCURRENCY)
}

/// Per-worker unix socket path. Each worker gets its own so that under
/// concurrency the runner accepts exactly one, provably-correct connection
/// rather than racing to correlate arbitrary accepts on a shared socket.
fn worker_socket_path(worker_id: u32) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/crucible-{}-{}.sock",
        std::process::id(),
        worker_id
    ))
}

async fn bind_worker_listener(worker_id: u32) -> Result<(PathBuf, UnixListener)> {
    let path = worker_socket_path(worker_id);
    let _ = tokio::fs::remove_file(&path).await;
    let listener = UnixListener::bind(&path)?;
    Ok((path, listener))
}

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

    let workload = drive(&bus).await;

    drop(bus);
    journal_task.await.expect("journal task should not panic")?;
    let _ = observer_task.await;
    cleanup_sockets();

    workload
}

/// Force-remove a worker's fleet by id, best-effort. On the happy path the
/// worker has already torn itself down and this is a no-op; when the worker was
/// killed before it could (a setup that outran its budget, a crash), this
/// reclaims the replica so its containers and network do not leak and starve the
/// host of the concurrent workers still running.
async fn reclaim_fleet(worker_id: u32) {
    if let Err(e) = Docker::reclaim(worker_id, &fleet::EXAMPLE).await {
        tracing::warn!(worker_id, error = %e, "failed to reclaim worker fleet");
    }
}

/// Remove this invocation's per-worker socket files, which unix listeners do not
/// clean up on their own.
fn cleanup_sockets() {
    let prefix = format!("crucible-{}-", std::process::id());
    if let Ok(entries) = std::fs::read_dir("/tmp") {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

async fn drive(bus: &EventBus) -> Result<()> {
    let campaign_start = Instant::now();
    let mut worker_id: u32 = 0;

    // Learn is a barrier: schedules derive from its observed traffic profiles.
    let (services, run_cost) = run_learn(bus, worker_id).await?;
    worker_id += 1;
    tracing::info!(
        services = services.len(),
        run_cost_ms = run_cost.as_millis(),
        "session catalogue received"
    );

    let schedule_budget = run_cost + HEAL_BUDGET + SCHEDULE_MARGIN;
    let max_inflight = concurrency();
    let mut scheduler = BurstScheduler::new(&services);
    let total = scheduler.total();
    let mut ran: usize = 0;
    let mut failed: usize = 0;

    // Run up to `max_inflight` schedule workers at once, each on its own isolated
    // fleet replica. TOTAL_BUDGET caps when we stop dispatching; the in-flight
    // workers run to completion. Schedules are emitted round-robin across bursts,
    // so a budget-truncated campaign still samples every burst evenly, and a
    // worker's failure is recorded against its schedule while the others carry on.
    let mut inflight: JoinSet<(u32, Result<Verdict>)> = JoinSet::new();
    let mut exhausted = false;
    loop {
        while inflight.len() < max_inflight && !exhausted && campaign_start.elapsed() < TOTAL_BUDGET
        {
            match scheduler.next() {
                Some(schedule) => {
                    inflight.spawn(run_one_schedule(
                        bus.clone(),
                        worker_id,
                        schedule,
                        schedule_budget,
                    ));
                    worker_id += 1;
                }
                None => exhausted = true,
            }
        }
        let Some(joined) = inflight.join_next().await else {
            break;
        };
        match joined {
            Ok((schedule_id, Ok(verdict))) => {
                tracing::info!(schedule_id, ?verdict, "run result");
                ran += 1;
            }
            Ok((schedule_id, Err(e))) => {
                tracing::warn!(schedule_id, error = %e, "schedule failed; continuing campaign");
                failed += 1;
            }
            Err(join_err) => {
                tracing::warn!(error = %join_err, "schedule task panicked; continuing campaign");
                failed += 1;
            }
        }
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

/// Run the fault-free learn pass on its own worker and return the observed
/// service profiles plus how long the pass took, always reclaiming the worker's
/// replica afterwards so a killed learn worker leaves nothing behind.
async fn run_learn(bus: &EventBus, worker_id: u32) -> Result<(Vec<ServiceProfile>, Duration)> {
    let outcome = execute_learn(bus, worker_id).await;
    reclaim_fleet(worker_id).await;
    outcome
}

async fn execute_learn(bus: &EventBus, worker_id: u32) -> Result<(Vec<ServiceProfile>, Duration)> {
    let (socket_path, listener) = bind_worker_listener(worker_id).await?;
    let (mut child, stderr_relay) = spawn_worker(&socket_path, worker_id)?;
    let learn_start = Instant::now();
    let session = accept_and_handshake(&listener, bus).await?;
    let services = session.learn(bus).await?;
    let run_cost = learn_start.elapsed();
    wait_worker(&mut child, stderr_relay, run_cost + LEARN_MARGIN).await?;
    Ok((services, run_cost))
}

/// Run one schedule and pair its verdict (or the error that ended it) with the
/// schedule id, so the pool can match completions that arrive out of order.
/// Owns everything it needs, so it can be spawned onto a `JoinSet`.
async fn run_one_schedule(
    bus: EventBus,
    worker_id: u32,
    schedule: Schedule,
    schedule_budget: Duration,
) -> (u32, Result<Verdict>) {
    let schedule_id = schedule.schedule_id;
    let verdict = run_worker(&bus, worker_id, schedule, schedule_budget).await;
    reclaim_fleet(worker_id).await;
    (schedule_id, verdict)
}

/// Bring up a worker on its own socket and fleet replica, run the schedule, and
/// reap the worker on every path so a failure leaves no zombie. A worker that
/// exceeds `schedule_budget` is cut off.
async fn run_worker(
    bus: &EventBus,
    worker_id: u32,
    schedule: Schedule,
    schedule_budget: Duration,
) -> Result<Verdict> {
    let (socket_path, listener) = bind_worker_listener(worker_id).await?;
    let (mut child, stderr_relay) = spawn_worker(&socket_path, worker_id)?;
    let pipeline = async {
        let session = accept_and_handshake(&listener, bus).await?;
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
