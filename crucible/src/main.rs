mod error;
mod session;

use std::{
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
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
    signal::unix::{SignalKind, signal},
    task::{JoinError, JoinHandle, JoinSet},
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
/// Bound for one learn attempt. A healthy learn brings the fleet up (bounded by
/// the deployment's own readiness timeout), runs the scenario, and settles well
/// inside this, so exceeding it means a hung worker rather than a slow one.
const LEARN_BUDGET: Duration = Duration::from_secs(150);
/// Learn is a barrier the whole campaign depends on, so a transient failure is
/// retried on a fresh replica up to this many total attempts before giving up.
const LEARN_MAX_ATTEMPTS: u32 = 2;
/// A failed schedule is respawned on a fresh worker up to this many total
/// attempts before it is recorded as errored.
const MAX_ATTEMPTS: u32 = 3;
/// If this many schedules fail every attempt back to back, the campaign gives
/// up: something is systemically wrong (e.g. Docker is unavailable).
const GIVE_UP_AFTER: u32 = 3;
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

/// Crucible: fault-injection testing for event-driven microservice fleets.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a fault-injection campaign against the example fleet.
    Run,
    /// Parse and check a `.cru` scenario file, reporting diagnostics.
    Check {
        /// Path to the scenario file.
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command.unwrap_or(Cmd::Run) {
        Cmd::Check { file } => run_check(&file),
        Cmd::Run => run_campaign().await,
    }
}

/// Initialise logging and run a fault-injection campaign to completion.
async fn run_campaign() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    match run().await {
        Ok(outcome) => outcome.exit_code(),
        Err(e) => {
            tracing::error!(error = %e, "runner exiting with error");
            ExitCode::from(2)
        }
    }
}

/// Lex and parse a `.cru` file, rendering diagnostics on failure. Exits 0 on
/// success, 1 on lexing or parsing diagnostics, and 2 if the file is not a
/// readable `.cru` file.
fn run_check(file: &Path) -> ExitCode {
    if file.extension().and_then(std::ffi::OsStr::to_str) != Some("cru") {
        eprintln!(
            "crucible check: expected a `.cru` file, got `{}`",
            file.display()
        );
        return ExitCode::from(2);
    }
    let name = file.display().to_string();
    let src = match std::fs::read_to_string(file) {
        Ok(src) => src,
        Err(e) => {
            eprintln!("crucible check: cannot read {name}: {e}");
            return ExitCode::from(2);
        }
    };

    let (tokens, lex_errors) = crucible_dsl::lexer::lex(&src);
    if !lex_errors.is_empty() {
        let diags: Vec<_> = lex_errors
            .iter()
            .map(|e| crucible_dsl::diagnostics::Diag::new(e.span, e.message.clone()))
            .collect();
        return render_and_fail(&name, &src, &diags);
    }

    match crucible_dsl::parser::parse(tokens) {
        Ok(ast) => {
            let fleet = &ast.fleet.node;
            println!(
                "{name}: ok (fleet `{}` via `{}`, {} service(s), {} scenario(s))",
                fleet.name.node,
                fleet.deployment.node,
                fleet.services.len(),
                ast.scenarios.len(),
            );
            ExitCode::SUCCESS
        }
        Err(diags) => render_and_fail(&name, &src, &diags),
    }
}

/// Render diagnostics to stderr and return the check-failed exit code.
fn render_and_fail(name: &str, src: &str, diags: &[crucible_dsl::diagnostics::Diag]) -> ExitCode {
    if let Err(e) = crucible_dsl::diagnostics::emit_to_stderr(name, src, diags) {
        eprintln!("crucible check: failed to render diagnostics: {e}");
    }
    ExitCode::from(1)
}

async fn run() -> Result<CampaignOutcome> {
    let (bus, journal_rx) = EventBus::new();

    let journal_path = journal::default_path(std::process::id());
    tracing::info!(path = %journal_path.display(), "journal ready");
    let journal_task = tokio::spawn(journal::run(journal_rx, journal_path));

    let mut observer_rx = bus.subscribe();
    let observer_task = tokio::spawn(async move {
        loop {
            match observer_rx.recv().await {
                Ok(event) => tracing::info!(target: "observer", event = ?event, ""),
                // Lagging drops only these log lines; the journal (an mpsc) still
                // records every event, so keep going rather than losing the
                // observer log for the rest of the run.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(target: "observer", skipped, "observer lagged behind; dropped events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
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

/// The campaign's overall outcome, mapped to the process exit code so automation
/// can tell a real fault from a run that could not render a clean verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CampaignOutcome {
    /// Every schedule that ran passed.
    Clean,
    /// At least one fault was found.
    FaultsFound,
    /// No fault, but some schedule could not be decided (a worker errored or a
    /// verdict was inconclusive).
    Indecisive,
}

impl CampaignOutcome {
    fn exit_code(self) -> ExitCode {
        match self {
            CampaignOutcome::Clean => ExitCode::SUCCESS,
            CampaignOutcome::FaultsFound => ExitCode::from(1),
            CampaignOutcome::Indecisive => ExitCode::from(2),
        }
    }
}

/// Running tally of schedule outcomes across the campaign. A `Fail` verdict is a
/// fault the framework found (the point of the run), kept with its schedule id
/// and reason; `errored` counts workers that never produced a verdict (crash,
/// timeout, panic).
#[derive(Default)]
struct Outcomes {
    passed: usize,
    faults: Vec<(u32, String)>,
    inconclusive: usize,
    errored: usize,
}

impl Outcomes {
    fn record_verdict(&mut self, schedule_id: u32, verdict: Verdict) {
        match verdict {
            Verdict::Pass => {
                tracing::info!(schedule_id, "pass");
                self.passed += 1;
            }
            Verdict::Fail { reason } => {
                tracing::warn!(schedule_id, %reason, "fault found");
                self.faults.push((schedule_id, reason));
            }
            Verdict::Inconclusive { reason } => {
                tracing::info!(schedule_id, %reason, "inconclusive");
                self.inconclusive += 1;
            }
        }
    }

    /// Record a worker that never produced a verdict. `schedule_id` is `None`
    /// only when the task panicked before it could report which schedule it ran.
    fn record_error(&mut self, schedule_id: Option<u32>, error: impl std::fmt::Display) {
        if let Some(schedule_id) = schedule_id {
            tracing::warn!(schedule_id, %error, "schedule failed");
        } else {
            tracing::warn!(%error, "schedule task panicked");
        }
        self.errored += 1;
    }

    /// Schedules that produced any outcome (verdict or error), i.e. were
    /// dispatched and joined.
    fn completed(&self) -> usize {
        self.passed + self.faults.len() + self.inconclusive + self.errored
    }

    /// The campaign's overall outcome: a found fault dominates; failing that, any
    /// worker error or inconclusive verdict means some schedule could not be
    /// decided.
    fn outcome(&self) -> CampaignOutcome {
        if !self.faults.is_empty() {
            CampaignOutcome::FaultsFound
        } else if self.errored > 0 || self.inconclusive > 0 {
            CampaignOutcome::Indecisive
        } else {
            CampaignOutcome::Clean
        }
    }

    /// Log the end-of-campaign summary. `total` is how many schedules the
    /// scheduler produced; fewer completed means the wall-clock budget cut the
    /// run short.
    fn report(&self, total: usize, elapsed_s: u64) {
        if self.completed() < total {
            tracing::warn!(
                passed = self.passed,
                faults = self.faults.len(),
                inconclusive = self.inconclusive,
                errored = self.errored,
                total,
                elapsed_s,
                budget_s = TOTAL_BUDGET.as_secs(),
                "campaign hit its wall-clock budget; remaining schedules skipped"
            );
        } else {
            tracing::info!(
                passed = self.passed,
                faults = self.faults.len(),
                inconclusive = self.inconclusive,
                errored = self.errored,
                total,
                elapsed_s,
                "campaign complete"
            );
        }
    }
}

/// Tracks respawn attempts and consecutive whole-schedule failures, so the
/// campaign retries transient worker deaths but abandons a systemically broken
/// run.
#[derive(Default)]
struct Recovery {
    consecutive_failures: u32,
}

impl Recovery {
    /// Whether a schedule that has just failed its `attempt`th try (1-based) has
    /// attempts left to respawn.
    fn may_respawn(attempt: u32) -> bool {
        attempt < MAX_ATTEMPTS
    }

    /// A schedule produced a verdict; the failure streak resets.
    fn reset(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Record a schedule that failed every attempt, extending the streak.
    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
    }

    /// Whether enough schedules have failed in a row to abandon the campaign.
    fn is_exhausted(&self) -> bool {
        self.consecutive_failures >= GIVE_UP_AFTER
    }
}

/// The schedule dispatch pool: the in-flight schedule workers, kept filled up to
/// the concurrency cap, plus the running tally and the counters that respawn and
/// give-up decisions read. Each schedule runs on its own isolated fleet replica,
/// and one worker's failure is recorded against its schedule while the others
/// carry on.
struct Pool<'a> {
    bus: &'a EventBus,
    inflight: JoinSet<(Schedule, u32, Result<Verdict>)>,
    outcomes: Outcomes,
    recovery: Recovery,
    worker_id: u32,
    schedule_budget: Duration,
    campaign_start: Instant,
    max_inflight: usize,
    exhausted: bool,
    gave_up: bool,
}

impl<'a> Pool<'a> {
    fn new(
        bus: &'a EventBus,
        worker_id: u32,
        schedule_budget: Duration,
        campaign_start: Instant,
        max_inflight: usize,
    ) -> Self {
        Self {
            bus,
            inflight: JoinSet::new(),
            outcomes: Outcomes::default(),
            recovery: Recovery::default(),
            worker_id,
            schedule_budget,
            campaign_start,
            max_inflight,
            exhausted: false,
            gave_up: false,
        }
    }

    /// Whether the campaign's wall-clock budget still allows dispatching.
    fn within_budget(&self) -> bool {
        self.campaign_start.elapsed() < TOTAL_BUDGET
    }

    /// Spawn one schedule attempt on the next worker id and its own replica.
    fn spawn(&mut self, schedule: Schedule, attempt: u32) {
        self.inflight.spawn(run_one_schedule(
            self.bus.clone(),
            self.worker_id,
            schedule,
            attempt,
            self.schedule_budget,
        ));
        self.worker_id += 1;
    }

    /// Fill the in-flight set up to the concurrency cap, until the scheduler
    /// drains, the campaign gives up, or the wall-clock budget runs out.
    fn fill(&mut self, scheduler: &mut BurstScheduler) {
        while self.inflight.len() < self.max_inflight
            && !self.exhausted
            && !self.gave_up
            && self.within_budget()
        {
            match scheduler.next() {
                Some(schedule) => self.spawn(schedule, 1),
                None => self.exhausted = true,
            }
        }
    }

    /// Record one completed (or panicked) schedule: tally its verdict, retry a
    /// transient failure on a fresh replica while attempts and budget remain, or
    /// record it errored. Enough failures back to back give the campaign up.
    fn record(&mut self, joined: std::result::Result<(Schedule, u32, Result<Verdict>), JoinError>) {
        match joined {
            Ok((schedule, _attempt, Ok(verdict))) => {
                self.recovery.reset();
                self.outcomes.record_verdict(schedule.schedule_id, verdict);
            }
            // A worker that outran its budget did not crash; we simply have no
            // verdict in the time allowed. Record it inconclusive rather than
            // retrying into the campaign's hard cap.
            Ok((schedule, _attempt, Err(Error::WorkerTimeout(budget)))) => {
                self.recovery.reset();
                self.outcomes.record_verdict(
                    schedule.schedule_id,
                    Verdict::Inconclusive {
                        reason: format!("worker exceeded its {budget:?} budget"),
                    },
                );
            }
            Ok((schedule, attempt, Err(e)))
                if !self.gave_up && self.within_budget() && Recovery::may_respawn(attempt) =>
            {
                tracing::warn!(
                    schedule_id = schedule.schedule_id,
                    attempt,
                    error = %e,
                    "worker failed; respawning on a fresh replica"
                );
                self.spawn(schedule, attempt + 1);
            }
            Ok((schedule, attempt, Err(e))) => {
                let schedule_id = schedule.schedule_id;
                tracing::warn!(schedule_id, attempts = attempt, error = %e, "worker failed and will not be retried");
                self.outcomes.record_error(Some(schedule_id), e);
                self.recovery.record_failure();
            }
            Err(join_err) => {
                // A panicked task loses its schedule, so it cannot be respawned.
                self.outcomes.record_error(None, join_err);
                self.recovery.record_failure();
            }
        }
        if self.recovery.is_exhausted() && !self.gave_up {
            self.gave_up = true;
            tracing::error!(
                consecutive = GIVE_UP_AFTER,
                "too many schedules failed in a row; abandoning the campaign"
            );
        }
    }

    /// Stop the in-flight tasks and force-reclaim every replica spawned this run.
    /// Reclaiming an already-torn-down id is a no-op, so this covers the
    /// still-running workers without tracking which are which.
    async fn reclaim_all(&mut self) {
        self.inflight.shutdown().await;
        for id in 0..self.worker_id {
            reclaim_fleet(id).await;
        }
    }
}

async fn drive(bus: &EventBus) -> Result<CampaignOutcome> {
    let campaign_start = Instant::now();
    let mut worker_id: u32 = 0;

    // Learn is a barrier: schedules derive from its observed traffic profiles.
    let (services, run_cost) = run_learn(bus, &mut worker_id).await?;
    tracing::info!(
        services = services.len(),
        run_cost_ms = run_cost.as_millis(),
        "session catalogue received"
    );

    let mut scheduler = BurstScheduler::new(&services);
    let total = scheduler.total();
    let mut pool = Pool::new(
        bus,
        worker_id,
        run_cost + HEAL_BUDGET + SCHEDULE_MARGIN,
        campaign_start,
        concurrency(),
    );

    // Interrupting a run must not orphan its in-flight replicas or sockets, so
    // catch SIGINT/SIGTERM, stop dispatching, and reclaim on the way out rather
    // than letting the default handler kill the runner mid-campaign.
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut interrupted = false;
    loop {
        pool.fill(&mut scheduler);
        tokio::select! {
            biased;
            _ = sigint.recv() => { interrupted = true; break; }
            _ = sigterm.recv() => { interrupted = true; break; }
            joined = pool.inflight.join_next() => {
                let Some(joined) = joined else {
                    break;
                };
                pool.record(joined);
            }
        }
    }

    if interrupted {
        tracing::warn!("interrupted; stopping dispatch and reclaiming replicas");
        pool.reclaim_all().await;
    }

    pool.outcomes
        .report(total, campaign_start.elapsed().as_secs());
    Ok(pool.outcomes.outcome())
}

/// Run the fault-free learn pass, retrying on a fresh replica up to
/// `LEARN_MAX_ATTEMPTS` so a single transient failure does not abort the whole
/// campaign. Each attempt runs on its own worker and is reclaimed afterwards,
/// whatever the outcome, so a killed learn worker leaves nothing behind.
/// Advances `worker_id` past every attempt so each gets its own socket and fleet.
async fn run_learn(bus: &EventBus, worker_id: &mut u32) -> Result<(Vec<ServiceProfile>, Duration)> {
    let mut attempt = 1;
    loop {
        let id = *worker_id;
        *worker_id += 1;
        let outcome = execute_learn(bus, id).await;
        reclaim_fleet(id).await;
        match outcome {
            Ok(result) => return Ok(result),
            Err(e) if attempt < LEARN_MAX_ATTEMPTS => {
                tracing::warn!(attempt, error = %e, "learn failed; retrying on a fresh replica");
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Run one learn attempt on its own worker, bounding the whole pipeline so a
/// hung worker (for instance one that connects but never sends `Ready`) cannot
/// wedge the campaign, and reaping the child on every path so a failure leaves
/// no zombie.
async fn execute_learn(bus: &EventBus, worker_id: u32) -> Result<(Vec<ServiceProfile>, Duration)> {
    let (socket_path, listener) = bind_worker_listener(worker_id).await?;
    let (mut child, stderr_relay) = spawn_worker(&socket_path, worker_id)?;
    let learn_start = Instant::now();
    let pipeline = async {
        let session = accept_and_handshake(&listener, bus).await?;
        let services = session.learn(bus).await?;
        Ok::<_, Error>((services, learn_start.elapsed()))
    };
    match tokio::time::timeout(LEARN_BUDGET, pipeline).await {
        Ok(Ok((services, run_cost))) => {
            // The catalogue is in hand; a failure while the worker finishes
            // teardown must not discard it. wait_worker has already reaped the
            // child on every error path, so here we only log and keep it.
            if let Err(e) = wait_worker(&mut child, stderr_relay, run_cost + LEARN_MARGIN).await {
                tracing::warn!(
                    worker_id,
                    error = %e,
                    "learn worker teardown failed after delivering its catalogue; keeping it"
                );
            }
            Ok((services, run_cost))
        }
        Ok(Err(e)) => {
            reap_worker(&mut child, stderr_relay).await;
            Err(e)
        }
        Err(_) => {
            reap_worker(&mut child, stderr_relay).await;
            Err(Error::WorkerTimeout(LEARN_BUDGET))
        }
    }
}

/// Run one schedule and hand back the schedule and its `attempt` alongside the
/// verdict (or the error that ended it), so the pool can match out-of-order
/// completions and respawn a failed schedule on a fresh worker. Owns everything
/// it needs, so it can be spawned onto a `JoinSet`.
async fn run_one_schedule(
    bus: EventBus,
    worker_id: u32,
    schedule: Schedule,
    attempt: u32,
    schedule_budget: Duration,
) -> (Schedule, u32, Result<Verdict>) {
    let verdict = run_worker(&bus, worker_id, schedule.clone(), schedule_budget).await;
    reclaim_fleet(worker_id).await;
    (schedule, attempt, verdict)
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
            // The worker already delivered its verdict; a failure while it
            // finishes teardown must not discard it, or a found fault would be
            // mis-tallied as an error and flip the campaign's exit code. A
            // fault-perturbed fleet is also the most likely to hit a docker
            // teardown race, so this correlates with exactly the runs that found
            // something. wait_worker has already reaped the child on every error
            // path, and reclaim_fleet removes the replica afterwards, so here we
            // only log and keep the verdict.
            if let Err(e) = wait_worker(&mut child, stderr_relay, schedule_budget).await {
                tracing::warn!(
                    worker_id,
                    error = %e,
                    "worker teardown failed after delivering its verdict; keeping the verdict"
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_tally_by_verdict_kind() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(1, Verdict::Pass);
        outcomes.record_verdict(
            2,
            Verdict::Fail {
                reason: "acked write missing".into(),
            },
        );
        outcomes.record_verdict(
            3,
            Verdict::Inconclusive {
                reason: "never quiesced".into(),
            },
        );
        outcomes.record_verdict(
            4,
            Verdict::Fail {
                reason: "duplicate applied".into(),
            },
        );
        outcomes.record_error(Some(5), "worker exited non-zero");
        outcomes.record_error(None, "task panicked");

        assert_eq!(outcomes.passed, 1);
        assert_eq!(outcomes.inconclusive, 1);
        assert_eq!(outcomes.errored, 2);
        assert_eq!(outcomes.completed(), 6);
        // Faults keep their schedule id and reason, in arrival order.
        assert_eq!(
            outcomes.faults,
            vec![
                (2, "acked write missing".to_string()),
                (4, "duplicate applied".to_string()),
            ]
        );
    }

    #[test]
    fn all_pass_is_clean() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(1, Verdict::Pass);
        assert_eq!(outcomes.outcome(), CampaignOutcome::Clean);
    }

    #[test]
    fn a_fault_dominates_even_with_errors() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(1, Verdict::Pass);
        outcomes.record_verdict(2, Verdict::Fail { reason: "x".into() });
        outcomes.record_error(Some(3), "boom");
        assert_eq!(outcomes.outcome(), CampaignOutcome::FaultsFound);
    }

    #[test]
    fn a_worker_error_is_indecisive() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(1, Verdict::Pass);
        outcomes.record_error(Some(2), "boom");
        assert_eq!(outcomes.outcome(), CampaignOutcome::Indecisive);
    }

    #[test]
    fn an_inconclusive_verdict_is_indecisive() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(1, Verdict::Inconclusive { reason: "y".into() });
        assert_eq!(outcomes.outcome(), CampaignOutcome::Indecisive);
    }

    #[test]
    fn a_schedule_respawns_until_the_attempt_cap() {
        assert!(Recovery::may_respawn(1));
        assert!(Recovery::may_respawn(MAX_ATTEMPTS - 1));
        assert!(!Recovery::may_respawn(MAX_ATTEMPTS));
    }

    #[test]
    fn the_campaign_is_exhausted_after_consecutive_failures() {
        let mut recovery = Recovery::default();
        for _ in 0..GIVE_UP_AFTER - 1 {
            recovery.record_failure();
            assert!(!recovery.is_exhausted());
        }
        recovery.record_failure();
        assert!(recovery.is_exhausted());
    }

    #[test]
    fn a_verdict_resets_the_failure_streak() {
        let mut recovery = Recovery::default();
        for _ in 0..GIVE_UP_AFTER {
            recovery.record_failure();
        }
        recovery.reset();
        assert!(!recovery.is_exhausted());
    }
}
