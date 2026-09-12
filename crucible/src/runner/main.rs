mod bench;
mod error;
mod session;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use crucible_core::{
    fault::Primitive,
    ipc::Verdict,
    learned::Learned,
    plan,
    schedule::Schedule,
    verdict::{self, Invariant, Readings},
};
use crucible_engine::{
    event_bus::EventBus,
    journal,
    scheduler::{self, Budget, BurstScheduler, Chain, RecoveryScheduler, Scheduler, recovery},
};
use strum::IntoEnumIterator;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixListener,
    process::{Child, Command},
    signal::unix::{SignalKind, signal},
    task::{Id as TaskId, JoinError, JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    bench::{Bench, Taking},
    error::{Error, Result},
    session::{Dispatching, Session},
};

/// The worker binary's name, the same in a build tree and once installed.
const WORKER_BIN: &str = "crucible-worker";
/// Where a package puts the parts of crucible that are not commands.
const LIBEXEC_DIR: &str = "/usr/lib/crucible";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
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
/// How long an interrupt waits for the cancelled runs to report the schedules
/// that were cancelled before force sweeping.
const INTERRUPT_DRAIN: Duration = Duration::from_secs(30);

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

/// Where the worker binary is, beside the runner in a build tree and in the
/// install's own directory once packaged, since it is not a command anyone runs.
fn worker_bin_path() -> Result<PathBuf> {
    let runner = std::env::current_exe()?;
    let beside = runner
        .parent()
        .ok_or(Error::RunnerExeParentless)?
        .join(WORKER_BIN);
    if beside.is_file() {
        return Ok(beside);
    }
    let installed = PathBuf::from(LIBEXEC_DIR).join(WORKER_BIN);
    if installed.is_file() {
        return Ok(installed);
    }
    Err(Error::WorkerBinMissing { beside, installed })
}

/// Crucible: fault-injection testing for event-driven microservice fleets.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a fault-injection campaign against a `.cru` scenario file.
    Run {
        /// Path to the scenario file.
        file: PathBuf,
    },
    /// Parse and check a `.cru` scenario file, reporting diagnostics.
    Check {
        /// Path to the scenario file.
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command {
        Cmd::Check { file } => run_check(&file).await,
        Cmd::Run { file } => run_campaign(&file).await,
    }
}

/// Initialise logging and run a fault-injection campaign to completion.
async fn run_campaign(file: &Path) -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let plan = match load(file).await {
        Ok(plan) => plan,
        // Already rendered to stderr with source context.
        Err(Error::ScenarioRejected(_)) => return ExitCode::from(2),
        Err(e) => {
            tracing::error!(error = %e, "cannot run this scenario");
            return ExitCode::from(2);
        }
    };

    match run(&plan).await {
        Ok(outcome) => outcome.exit_code(),
        Err(e) => {
            tracing::error!(error = %e, "runner exiting with error");
            ExitCode::from(2)
        }
    }
}

/// Lower a `.cru` file to the plan a campaign runs, rendering diagnostics for
/// anything that stops it. Both commands come through here, so both accept
/// exactly the same files and reject them for the same reasons.
async fn load(file: &Path) -> Result<plan::Plan> {
    let name = file.display().to_string();
    if file.extension().and_then(std::ffi::OsStr::to_str) != Some("cru") {
        return Err(Error::NotAScenarioFile(name));
    }
    let src = std::fs::read_to_string(file).map_err(|source| Error::ScenarioUnreadable {
        path: name.clone(),
        source,
    })?;
    let registry = crucible_plugin::Registry::load().await;
    crucible_dsl::compile(&src, &registry).map_err(|diags| {
        if let Err(e) = crucible_dsl::diagnostics::emit_to_stderr(&name, &src, &diags) {
            eprintln!("crucible: failed to render diagnostics: {e}");
        }
        Error::ScenarioRejected(name)
    })
}

/// Check a `.cru` file and describe what it says. Exits 0 on success, 1 if the
/// file was read but says something we cannot run, and 2 if it could not be
/// read at all.
async fn run_check(file: &Path) -> ExitCode {
    let plan = match load(file).await {
        Ok(plan) => plan,
        Err(Error::ScenarioRejected(_)) => return ExitCode::from(1),
        Err(e) => {
            eprintln!("crucible check: {e}");
            return ExitCode::from(2);
        }
    };

    let fleet = &plan.fleet;
    println!(
        "{}: ok (fleet `{}` via `{}`, {} service(s), {} scenario(s), spec {:016x})",
        file.display(),
        fleet.name,
        fleet.deployment,
        fleet.services.len(),
        plan.scenarios.len(),
        plan.spec_hash().0,
    );
    ExitCode::SUCCESS
}

async fn run(plan: &plan::Plan) -> Result<CampaignOutcome> {
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

    let workload = drive(&bus, plan).await;

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
async fn reclaim_fleet(worker_id: u32, fleet: &plan::Fleet) {
    if let Err(e) = reclaim(worker_id, fleet).await {
        tracing::warn!(worker_id, error = %e, "failed to reclaim worker fleet");
    }
}

/// Remove a worker's replica without any live worker state: the containers are
/// named from the worker id and the fleet's services, and removing one already
/// gone is a no-op.
async fn reclaim(
    worker_id: u32,
    fleet: &plan::Fleet,
) -> std::result::Result<(), crucible_plugin::Error> {
    let mut deployment =
        crucible_plugin::Registry::builtins().deployment_for(fleet, worker_id, None)?;
    deployment.teardown().await
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
    faults: Vec<(u32, Option<Invariant>, String)>,
    inconclusive: usize,
    errored: usize,
    /// Ids of interrupted schedules.
    abandoned: Vec<u32>,
}

impl Outcomes {
    fn record_verdict(&mut self, schedule_id: u32, verdict: Verdict) {
        match verdict {
            Verdict::Pass => {
                tracing::info!(schedule_id, "pass");
                self.passed += 1;
            }
            Verdict::Fail { invariant, reason } => {
                tracing::warn!(schedule_id, %reason, "fault found");
                self.faults.push((schedule_id, invariant, reason));
            }
            Verdict::Inconclusive { reason } => {
                tracing::info!(schedule_id, %reason, "inconclusive");
                self.inconclusive += 1;
            }
        }
    }

    /// Which invariants the campaign showed broken, and how often.
    ///
    /// A run that settled where none of them describes is counted apart.
    fn shown(&self) -> String {
        let mut counted: BTreeMap<Invariant, usize> = BTreeMap::new();
        let mut unattributed = 0;
        for (_, invariant, _) in &self.faults {
            match invariant {
                Some(invariant) => *counted.entry(*invariant).or_default() += 1,
                None => unattributed += 1,
            }
        }
        let mut spelled: Vec<String> = counted
            .into_iter()
            .map(|(invariant, times)| format!("{invariant}: {times}"))
            .collect();
        if unattributed > 0 {
            spelled.push(format!("unattributed: {unattributed}"));
        }
        spelled.join(", ")
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
    /// scheduler produced; `stopped` says why the rest were not run, when some
    /// were not.
    fn report(&self, total: usize, elapsed_s: u64, stopped: Stopped) {
        let shown = self.shown();
        if self.completed() < total {
            let mut stopped_ids = self.abandoned.clone();
            stopped_ids.sort_unstable();
            let abandoned = spelled_ids(&stopped_ids);
            tracing::warn!(
                passed = self.passed,
                faults = self.faults.len(),
                %shown,
                inconclusive = self.inconclusive,
                errored = self.errored,
                %abandoned,
                total,
                elapsed_s,
                "{stopped}, so the remaining schedules were skipped"
            );
        } else {
            tracing::info!(
                passed = self.passed,
                faults = self.faults.len(),
                %shown,
                inconclusive = self.inconclusive,
                errored = self.errored,
                total,
                elapsed_s,
                "campaign complete"
            );
        }
    }
}
/// Schedule ids as a report says them, or `none` for an empty set.
fn spelled_ids(ids: &[u32]) -> String {
    let spelled: Vec<String> = ids.iter().map(ToString::to_string).collect();
    match spelled.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => "none".to_owned(),
    }
}

/// Why a campaign stopped dispatching with schedules left.
#[derive(Clone, Copy, Debug)]
enum Stopped {
    Budget,
    GaveUp,
    Interrupted(Phase),
}

/// Which part of a campaign an interrupt arrived in.
#[derive(Clone, Copy, Debug)]
enum Phase {
    Learning,
    Dispatching,
}

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stopped::Budget => f.write_str("the campaign ran out of wall-clock budget"),
            Stopped::GaveUp => f.write_str("too many schedules failed in a row"),
            Stopped::Interrupted(Phase::Learning) => {
                f.write_str("the campaign was interrupted during its fault-free run")
            }
            Stopped::Interrupted(Phase::Dispatching) => f.write_str("the campaign was interrupted"),
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

    /// A schedule came back with something to read; the failure streak resets.
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

/// One finished worker. The schedule it ran, which attempt it was, and what the
/// run read.
type Ran = (Schedule, u32, Result<Readings>);

/// The schedule dispatch pool: the in-flight schedule workers, kept filled up to
/// the concurrency cap, plus the running tally and the counters that respawn and
/// give-up decisions read. Each schedule runs on its own isolated fleet replica,
/// and one worker's failure is recorded against its schedule while the others
/// carry on.
struct Pool<'a> {
    bus: &'a EventBus,
    /// The fleet every replica of this campaign runs, so one can be reclaimed
    /// without the worker that owned it.
    fleet: &'a plan::Fleet,
    /// Judges what each run read, fetching the reference runs it needs.
    bench: Bench<'a>,
    /// What each in-flight task is running, so a panicked one can still be
    /// told from a schedule the campaign is judged on.
    drove: BTreeMap<TaskId, Option<Vec<usize>>>,
    inflight: JoinSet<Ran>,
    outcomes: Outcomes,
    recovery: Recovery,
    /// How long the whole campaign may take work on for.
    campaign_budget: Option<Duration>,
    worker_id: u32,
    schedule_budget: Duration,
    campaign_start: Instant,
    max_inflight: usize,
    exhausted: bool,
    gave_up: bool,
    /// Cancelled when the campaign is interrupted, so an in-flight run stops
    /// and reports the schedule it was on rather than being aborted.
    interrupt: CancellationToken,
}

/// What a pool needs to start dispatching.
struct Dispatch<'a> {
    bus: &'a EventBus,
    fleet: &'a plan::Fleet,
    scenario: &'a plan::Scenario,
    /// The first id not spent on the fault-free run.
    worker_id: u32,
    schedule_budget: Duration,
    campaign_start: Instant,
    max_inflight: usize,
    interrupt: CancellationToken,
}

impl<'a> Pool<'a> {
    fn new(dispatch: Dispatch<'a>) -> Self {
        let Dispatch {
            bus,
            fleet,
            scenario,
            worker_id,
            schedule_budget,
            campaign_start,
            max_inflight,
            interrupt,
        } = dispatch;
        Self {
            bus,
            fleet,
            bench: Bench::new(fleet, scenario, max_inflight),
            drove: BTreeMap::new(),
            inflight: JoinSet::new(),
            outcomes: Outcomes::default(),
            recovery: Recovery::default(),
            campaign_budget: scenario.budget,
            worker_id,
            schedule_budget,
            campaign_start,
            max_inflight,
            exhausted: false,
            gave_up: false,
            interrupt,
        }
    }

    /// Whether the campaign's wall-clock budget still allows dispatching. An
    /// unbounded campaign always does.
    fn within_budget(&self) -> bool {
        self.campaign_budget
            .is_none_or(|budget| self.campaign_start.elapsed() < budget)
    }

    /// How much more work the campaign will take on.
    fn taking(&self) -> Taking {
        if self.gave_up {
            Taking::Nothing
        } else if self.within_budget() {
            Taking::More
        } else {
            Taking::Finishing
        }
    }

    /// Spawn one schedule attempt on the next worker id and its own replica.
    fn spawn(&mut self, schedule: Schedule, attempt: u32) {
        let landed = schedule.landed().map(<[usize]>::to_vec);
        let task = self.inflight.spawn(run_one_schedule(
            self.bus.clone(),
            self.worker_id,
            schedule,
            attempt,
            self.schedule_budget,
            self.interrupt.clone(),
        ));
        // A panicked task comes back without its schedule, so what it was for
        // is written down here while there is still something to read it from.
        self.drove.insert(task.id(), landed);
        self.worker_id += 1;
    }

    /// Fill the in-flight set up to the concurrency cap, until the scheduler
    /// drains or the campaign stops taking work on.
    fn fill(&mut self, scheduler: &mut dyn Scheduler) {
        while self.inflight.len() < self.max_inflight {
            let taking = self.taking();
            // A parked run is holding its result on one of these, so they go
            // before work that could only add more.
            if let Some(reference) = self.bench.wanted(taking) {
                self.spawn(reference, 1);
                continue;
            }
            if taking != Taking::More || self.exhausted {
                break;
            }
            match scheduler.next() {
                Some(schedule) => self.spawn(schedule, 1),
                None => self.exhausted = true,
            }
        }
    }

    /// Record what the bench has decided, which is nothing until a run has
    /// every state it answers to.
    fn record_judged(&mut self, judged: impl IntoIterator<Item = (u32, Verdict)>) {
        for (schedule_id, verdict) in judged {
            self.outcomes.record_verdict(schedule_id, verdict);
        }
    }

    /// Take in a reference run. Where it left the fleet, or its failure to
    /// say.
    ///
    /// It is never respawned. Whether the set is worth another run is the
    /// bench's to decide.
    fn reference_done(&mut self, landed: &[usize], result: Result<Readings>) {
        match result {
            Ok(readings) => {
                let judged = self.bench.arrived(landed, &readings);
                self.record_judged(judged);
            }
            Err(e) => self.bench.failed(landed, &e),
        }
    }

    /// Take in a run the campaign is judged on. Its verdict, a retry on a
    /// fresh replica while attempts and appetite remain, or the error that
    /// ends it.
    fn schedule_done(&mut self, schedule: Schedule, attempt: u32, result: Result<Readings>) {
        match result {
            Ok(readings) => {
                self.recovery.reset();
                let judged = self.bench.judge(schedule.id, readings);
                self.record_judged(judged);
            }
            // An interrupt stopped this run, so it says nothing about the fleet.
            // Kept apart from the errors, which is what the stop report reads to
            // name the schedules it abandoned.
            Err(Error::Interrupted) => {
                self.outcomes.abandoned.push(schedule.id);
            }
            // A worker that outran its budget did not crash; we simply have no
            // verdict in the time allowed. Record it inconclusive rather than
            // retrying into the campaign's hard cap.
            Err(Error::WorkerTimeout(budget)) => {
                self.recovery.reset();
                self.outcomes.record_verdict(
                    schedule.id,
                    Verdict::Inconclusive {
                        reason: format!("worker exceeded its {budget:?} budget"),
                    },
                );
            }
            Err(e)
                if e.is_transient()
                    && self.taking() == Taking::More
                    && Recovery::may_respawn(attempt) =>
            {
                tracing::warn!(
                    schedule_id = schedule.id,
                    attempt,
                    error = %e,
                    "worker failed; respawning on a fresh replica"
                );
                self.spawn(schedule, attempt + 1);
            }
            Err(e) => {
                let schedule_id = schedule.id;
                tracing::warn!(schedule_id, attempts = attempt, error = %e, "worker failed and will not be retried");
                self.outcomes.record_error(Some(schedule_id), e);
                self.recovery.record_failure();
            }
        }
    }

    /// Record what every run still waiting on a reference run settles for.
    fn settle_parked(&mut self) {
        let settled = self.bench.settle();
        self.record_judged(settled);
    }

    /// Record one completed (or panicked) run.
    ///
    /// A reference run answers a question the campaign asked itself, and never
    /// counts as a result of the campaign.
    fn record(&mut self, joined: std::result::Result<(TaskId, Ran), JoinError>) {
        match joined {
            Ok((task, (schedule, attempt, result))) => {
                self.drove.remove(&task);
                match schedule.landed() {
                    Some(landed) => self.reference_done(landed, result),
                    None => self.schedule_done(schedule, attempt, result),
                }
            }
            Err(join_err) => {
                // A reference run that panicked is still not a result of the
                // campaign. It cannot be respawned, so the set has spent a run
                // and the bench decides whether it is worth another.
                if let Some(landed) = self.drove.remove(&join_err.id()).flatten() {
                    self.bench.failed(&landed, &join_err);
                } else {
                    self.outcomes.record_error(None, join_err);
                    self.recovery.record_failure();
                }
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

    /// Take in the cancelled runs, then force-reclaim every replica spawned this
    /// run.
    ///
    /// A cancelled run returns the schedule it was on, so draining is what lets
    /// the campaign say which schedules it abandoned. Bounded, so one wedged
    /// worker cannot hold up an interrupt, and the sweep afterwards covers
    /// whatever did not come back. Reclaiming an already-torn-down id is a
    /// no-op.
    async fn reclaim_all(&mut self) {
        let drained = tokio::time::timeout(INTERRUPT_DRAIN, async {
            while let Some(joined) = self.inflight.join_next_with_id().await {
                self.record(joined);
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                after = ?INTERRUPT_DRAIN,
                "some workers did not stop in time; abandoning them"
            );
        }
        self.inflight.shutdown().await;
        for id in 0..self.worker_id {
            reclaim_fleet(id, self.fleet).await;
        }
    }
}

/// Say which invariants a run against this fleet could show broken, and return
/// the ones worth scheduling.
///
/// Could show, not did.
fn report_reach(available: &BTreeSet<Primitive>) {
    for invariant in Invariant::iter() {
        match invariant.showable(available) {
            Ok(ways) => {
                let spelled: Vec<String> = ways.iter().map(ToString::to_string).collect();
                tracing::info!(
                    "{invariant} could be shown broken against this fleet, by: {}",
                    spelled.join(", ")
                );
            }
            Err(why) => tracing::info!("{invariant} is out of reach: {why}"),
        }
    }
}

/// Every schedule the campaign will run, in the order it will run them.
///
/// Recovery comes off the top. The bursts take what is left.
fn fit(
    plan: &plan::Plan,
    scenario: &plan::Scenario,
    learned: &Learned,
    campaign_start: Instant,
    cost: Duration,
    concurrency: usize,
) -> Chain<RecoveryScheduler, BurstScheduler> {
    report_reach(&learned.primitives);
    let ways = recovery::ways(&learned.primitives);
    let budget = scenario.budget.map(|budget| Budget {
        left: budget.saturating_sub(campaign_start.elapsed()),
        cost,
        concurrency,
    });

    let bursts = BurstScheduler::new(
        &plan.fleet,
        scenario,
        learned,
        budget.map(|budget| budget.after(RecoveryScheduler::count(&plan.fleet, learned, &ways))),
    );
    let degraded = RecoveryScheduler::new(
        &plan.fleet,
        scenario,
        learned,
        &ways,
        u32::try_from(bursts.total()).unwrap_or(u32::MAX) + 1,
    );

    let total = bursts.total() + degraded.total();
    tracing::info!(
        schedules = total,
        recovery = degraded.total(),
        budget_s = budget.map(|budget| budget.left.as_secs()),
        cost_s = cost.as_secs(),
        concurrency,
        estimate_s = scheduler::runtime(total, cost, concurrency).as_secs(),
        "campaign fitted to {}",
        bursts.coverage()
    );

    Chain(degraded, bursts)
}

/// A token cancelled by the first SIGINT or SIGTERM to arrive.
///
/// Cancellation reaches a phase as an outcome rather than a dropped future, so
/// each one runs the teardown it already has and says what it was doing.
///
/// # Errors
/// Errors if either signal handler cannot be installed.
fn interrupt_token() -> Result<CancellationToken> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let token = CancellationToken::new();
    let cancel = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
        cancel.cancel();
    });
    Ok(token)
}

async fn drive(bus: &EventBus, plan: &plan::Plan) -> Result<CampaignOutcome> {
    let campaign_start = Instant::now();
    let mut worker_id: u32 = 0;

    // One scenario for now; a plan describing several would run each in turn.
    let scenario = plan
        .scenarios
        .first()
        .expect("the grammar requires a scenario, so a lowered plan states one");

    // Interrupting a campaign must not orphan a replica.
    let interrupt = interrupt_token()?;

    // Learn is a barrier: schedules derive from its observed traffic profiles.
    let (learned, cycle_cost) =
        match run_learn(bus, &mut worker_id, &plan.fleet, scenario, &interrupt).await {
            Ok(learned) => learned,
            // run_learn reclaims its replica on the way out, so there is nothing
            // left to take down here.
            Err(Error::Interrupted) => {
                tracing::warn!("{}", Stopped::Interrupted(Phase::Learning));
                return Ok(CampaignOutcome::Indecisive);
            }
            Err(e) => return Err(e),
        };
    let readings = learned.fault_free.trail.iter().flatten();
    tracing::info!(
        edges = learned.profiles.len(),
        checkpoints = learned.fault_free.trail.len(),
        read = readings.clone().filter(|r| r.is_some()).count(),
        unread = readings.filter(|r| r.is_none()).count(),
        cycle_cost_ms = cycle_cost.as_millis(),
        "session catalogue received"
    );
    for profile in &learned.profiles {
        tracing::debug!(
            edge = %profile.edge,
            requests = ?profile.client_to_upstream,
            responses = ?profile.upstream_to_client,
            "learned"
        );
    }

    // The scenario is held to where the fleet settled, which is the reading
    // taken once it had the whole of `consistent_within` to go quiet in.
    let settled = learned.fault_free.settled.as_slice();
    if let Some(unmet) = verdict::unmet(&scenario.checks, settled) {
        tracing::error!("the scenario states {unmet} in its own fault-free run");
        return Ok(CampaignOutcome::Indecisive);
    }

    let concurrency = concurrency();
    let cost = cycle_cost + scenario.consistent_within;

    let mut scheduler = fit(plan, scenario, &learned, campaign_start, cost, concurrency);
    let mut pool = Pool::new(Dispatch {
        bus,
        fleet: &plan.fleet,
        scenario,
        worker_id,
        schedule_budget: cost + SCHEDULE_MARGIN,
        campaign_start,
        max_inflight: concurrency,
        interrupt: interrupt.clone(),
    });

    let mut interrupted = false;
    loop {
        pool.fill(&mut scheduler);
        tokio::select! {
            biased;
            () = interrupt.cancelled() => { interrupted = true; break; }
            joined = pool.inflight.join_next_with_id() => {
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
    // Whatever stopped the campaign, a run still waiting on a reference has to
    // answer with what it has.
    pool.settle_parked();

    let stopped = if interrupted {
        Stopped::Interrupted(Phase::Dispatching)
    } else if pool.gave_up {
        Stopped::GaveUp
    } else {
        Stopped::Budget
    };
    pool.outcomes.report(
        scheduler.total(),
        campaign_start.elapsed().as_secs(),
        stopped,
    );
    Ok(pool.outcomes.outcome())
}

/// Run the fault-free learn pass, retrying on a fresh replica up to
/// `LEARN_MAX_ATTEMPTS` so a single transient failure does not abort the whole
/// campaign. Each attempt runs on its own worker and is reclaimed afterwards,
/// whatever the outcome, so a killed learn worker leaves nothing behind.
/// Advances `worker_id` past every attempt so each gets its own socket and fleet.
async fn run_learn(
    bus: &EventBus,
    worker_id: &mut u32,
    fleet: &plan::Fleet,
    scenario: &plan::Scenario,
    interrupt: &CancellationToken,
) -> Result<(Learned, Duration)> {
    let mut attempt = 1;
    loop {
        let id = *worker_id;
        *worker_id += 1;
        let schedule = Schedule::learn(
            fleet.clone(),
            scenario.steps.clone(),
            scenario.checks.clone(),
            scenario.consistent_within,
        );
        // Timed around the reclaim as well as the run: every schedule brings a
        // replica up and takes it down again, so that is what one costs.
        let cycle = Instant::now();
        let outcome = execute_learn(bus, id, schedule, interrupt).await;
        reclaim_fleet(id, fleet).await;
        match outcome {
            Ok((learned, _)) => return Ok((learned, cycle.elapsed())),
            Err(e) if e.is_transient() && attempt < LEARN_MAX_ATTEMPTS => {
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
async fn execute_learn(
    bus: &EventBus,
    worker_id: u32,
    schedule: Schedule,
    interrupt: &CancellationToken,
) -> Result<(Learned, Duration)> {
    let (socket_path, listener) = bind_worker_listener(worker_id).await?;
    let (mut child, stderr_relay) = spawn_worker(&socket_path, worker_id)?;
    let learn_start = Instant::now();
    let pipeline = async {
        let session = accept_and_handshake(&listener, bus).await?;
        let learned = session.learn(bus, schedule).await?;
        Ok::<_, Error>((learned, learn_start.elapsed()))
    };
    let raced = tokio::select! {
        biased;
        () = interrupt.cancelled() => Err(Error::Interrupted),
        outcome = tokio::time::timeout(LEARN_BUDGET, pipeline) => match outcome {
            Ok(learned) => learned,
            Err(_) => Err(Error::WorkerTimeout(LEARN_BUDGET)),
        },
    };
    match raced {
        Ok((learned, run_cost)) => {
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
            Ok((learned, run_cost))
        }
        Err(e) => {
            reap_worker(&mut child, stderr_relay).await;
            Err(e)
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
    interrupt: CancellationToken,
) -> Ran {
    let readings = run_worker(
        &bus,
        worker_id,
        schedule.clone(),
        schedule_budget,
        &interrupt,
    )
    .await;
    reclaim_fleet(worker_id, &schedule.fleet).await;
    (schedule, attempt, readings)
}

/// Bring up a worker on its own socket and fleet replica, run the schedule, and
/// reap the worker on every path so a failure leaves no zombie. A worker that
/// exceeds `schedule_budget` is cut off.
async fn run_worker(
    bus: &EventBus,
    worker_id: u32,
    schedule: Schedule,
    schedule_budget: Duration,
    interrupt: &CancellationToken,
) -> Result<Readings> {
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
    let raced = tokio::select! {
        biased;
        () = interrupt.cancelled() => Err(Error::Interrupted),
        outcome = tokio::time::timeout(schedule_budget, pipeline) => match outcome {
            Ok(readings) => readings,
            Err(_) => Err(Error::WorkerTimeout(schedule_budget)),
        },
    };
    match raced {
        Ok(readings) => {
            // The worker already delivered its readings; a failure while it
            // finishes teardown must not discard them, or a found fault would be
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
                    "worker teardown failed after delivering its readings; keeping them"
                );
            }
            Ok(readings)
        }
        Err(e) => {
            reap_worker(&mut child, stderr_relay).await;
            Err(e)
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
                invariant: Some(Invariant::Durable),
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
                invariant: Some(Invariant::Idempotent),
                reason: "duplicate applied".into(),
            },
        );
        outcomes.record_error(Some(5), "worker exited non-zero");
        outcomes.record_error(None, "task panicked");

        assert_eq!(outcomes.passed, 1);
        assert_eq!(outcomes.inconclusive, 1);
        assert_eq!(outcomes.errored, 2);
        assert_eq!(outcomes.completed(), 6);
        // Faults keep their schedule id, what they showed and why, in arrival
        // order.
        assert_eq!(
            outcomes.faults,
            vec![
                (
                    2,
                    Some(Invariant::Durable),
                    "acked write missing".to_string()
                ),
                (
                    4,
                    Some(Invariant::Idempotent),
                    "duplicate applied".to_string()
                ),
            ]
        );
    }

    #[test]
    fn an_interrupted_schedule_is_abandoned_rather_than_errored() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(1, Verdict::Pass);
        outcomes.abandoned.push(2);
        outcomes.abandoned.push(3);

        assert_eq!(outcomes.errored, 0);
        assert_eq!(outcomes.completed(), 1);
        assert_eq!(outcomes.outcome(), CampaignOutcome::Clean);
    }

    #[test]
    fn a_stop_report_names_the_schedules_it_abandoned() {
        assert_eq!(spelled_ids(&[]), "none");
        assert_eq!(spelled_ids(&[4]), "4");
        assert_eq!(spelled_ids(&[1, 2, 3, 4, 5]), "1, 2, 3, 4 and 5");
    }

    #[test]
    fn an_interrupt_is_not_transient() {
        assert!(!Error::Interrupted.is_transient());
    }

    /// The campaign reports what its runs turned out to show, and a run whose
    /// fault could have shown several and whose readings named none of them is
    /// not counted as any of them.
    #[test]
    fn the_campaign_says_what_it_showed_and_what_it_could_not_name() {
        let mut outcomes = Outcomes::default();
        outcomes.record_verdict(
            1,
            Verdict::Fail {
                invariant: Some(Invariant::Idempotent),
                reason: "duplicate applied".into(),
            },
        );
        outcomes.record_verdict(
            2,
            Verdict::Fail {
                invariant: None,
                reason: "settled somewhere nothing describes".into(),
            },
        );
        assert_eq!(outcomes.shown(), "idempotency: 1, unattributed: 1");
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
        outcomes.record_verdict(
            2,
            Verdict::Fail {
                invariant: None,
                reason: "x".into(),
            },
        );
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
