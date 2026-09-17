//! What a campaign wrote down, rendered for a reader.
//!
//! Both the screen and the end-of-run report read the journal.

use std::{borrow::Cow, collections::BTreeMap, io, path::Path};

use crucible_core::{
    ipc::{RunnerToWorker, Verdict, WorkerEvent, WorkerToRunner},
    plan::SpecHash,
    schedule::{Phase, Progress, Purpose},
    verdict::Invariant,
};
use crucible_engine::event_bus::RunnerEvent;
use crucible_protocol::{At, FaultResult};

/// Display forms for types the report and the screen render but do not own.
pub trait Short {
    fn short(&self) -> Cow<'static, str>;
}

impl Short for Verdict {
    fn short(&self) -> Cow<'static, str> {
        match self {
            Verdict::Pass => "Pass".into(),
            Verdict::Fail {
                invariant: Some(invariant),
                ..
            } => format!("Fail {invariant}").into(),
            Verdict::Fail {
                invariant: None, ..
            } => "Fail".into(),
            Verdict::Inconclusive { .. } => "Inconclusive".into(),
        }
    }
}

impl Short for Progress {
    fn short(&self) -> Cow<'static, str> {
        match self {
            Progress::Pending => "Pending".into(),
            Progress::Running { .. } => "Running".into(),
            Progress::CounterExample { .. } => "Counter Example".into(),
            Progress::Requeued => "Requeued".into(),
            Progress::Abandoned => "Abandoned".into(),
            Progress::Skipped => "Skipped".into(),
            Progress::Errored => "Errored".into(),
            Progress::Complete(verdict) => verdict.short(),
        }
    }
}

impl Short for Phase {
    fn short(&self) -> Cow<'static, str> {
        match self {
            Phase::Setup => "setting up".into(),
            Phase::Driving => "driving".into(),
            Phase::Healing => "healing".into(),
            Phase::Observing => "observing".into(),
            Phase::Tearing => "cleaning up".into(),
        }
    }
}

pub fn recorded(event: &RunnerEvent) -> Vec<String> {
    match event {
        RunnerEvent::Moved { to, .. } => vec![format!("became {}", to.short())],
        RunnerEvent::RunnerMessage {
            message: RunnerToWorker::Run(schedule),
            ..
        } => match &schedule.purpose {
            Purpose::Reference { landed } => vec![format!(
                "a clean run of steps {} answered it",
                landed
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )],
            _ => Vec::new(),
        },
        RunnerEvent::WorkerMessage { message, .. } => match message {
            WorkerToRunner::Event(WorkerEvent::Fault(report)) => {
                vec![match &report.result {
                    FaultResult::Fired { by, at, .. } => {
                        format!("{by:?} on {} {}", report.service, placed(at))
                    }
                    FaultResult::Missed(why) => {
                        format!("nothing was done to {}: {why:?}", report.service)
                    }
                }]
            }
            WorkerToRunner::RunResult { readings, .. } => read_out(readings),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Where in the run a fault landed.
fn placed(at: &At) -> String {
    match at {
        At::Throughout => "for the whole run".to_owned(),
        At::Moment { mark, why, .. } => {
            format!("at {mark}, on {why}")
        }
    }
}

/// What a run read, a step at a time.
///
/// Each step gets a line saying what the fleet answered and under it any
/// reading that differs from the fault-free run.
fn read_out(readings: &crucible_core::verdict::Readings) -> Vec<String> {
    let named: Vec<String> = readings
        .checks
        .iter()
        .map(|observed| observed.check.observable())
        .collect();
    let mut lines = Vec::new();
    for (step, outcome) in readings.outcomes.iter().enumerate() {
        lines.push(format!("step {}  {:?}", step + 1, outcome.ack));
        let (Some(drove), Some(learned)) = (
            readings.trajectory.at(step + 1),
            readings.fault_free.trail.at(step + 1),
        ) else {
            continue;
        };
        lines.extend(apart(&named, drove, learned).map(|line| format!("   {line}")));
    }
    lines.push(String::new());
    lines.extend(readings.checks.iter().map(|observed| {
        format!(
            "{} settled {}, a full run leaves {}",
            observed.check.observable(),
            observed
                .value
                .as_ref()
                .map_or("nothing".to_owned(), ToString::to_string),
            observed.check.value,
        )
    }));
    lines
}

/// The readings that differ from the fault-free run.
fn apart<'a>(
    named: &'a [String],
    drove: &'a crucible_core::verdict::Checkpoint,
    learned: &'a crucible_core::verdict::Checkpoint,
) -> impl Iterator<Item = String> + 'a {
    named.iter().enumerate().filter_map(move |(at, name)| {
        let (drove, learned) = (drove.get(at)?.as_ref(), learned.get(at)?.as_ref());
        (drove != learned).then(|| {
            format!(
                "{name} {}, fault-free {}",
                drove.map_or("nothing".to_owned(), ToString::to_string),
                learned.map_or("nothing".to_owned(), ToString::to_string),
            )
        })
    })
}

/// What the report is called: the day it ran and the scenario it ran.
#[must_use]
pub fn file_name(spec: SpecHash) -> String {
    let today = time::OffsetDateTime::now_utc().date();
    format!(
        "{:04}{:02}{:02}-{:016x}-report.md",
        today.year(),
        u8::from(today.month()),
        today.day(),
        spec.0,
    )
}

/// Write the campaign's report to `path`, rendered from `journal`.
///
/// # Errors
/// Returns an [`io::Error`] if the journal cannot be read or the report cannot
/// be written.
pub async fn write(journal: &Path, path: &Path, scenario: &str) -> io::Result<()> {
    let events = crucible_engine::journal::read(journal).await?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, render(&events, scenario)).await
}

/// The campaign as Markdown.
///
/// What it found and every schedule it ran.
fn render(events: &[RunnerEvent], scenario: &str) -> String {
    let verdicts = settled(events);
    let fitted = fitted(events);

    let mut out = format!("# {scenario}\n\n{} schedules ran.\n", fitted.len());
    for outcome in OUTCOMES {
        // By id rather than the order they ran, so a reader can find one.
        let mut ran: Vec<&(u32, Purpose)> = fitted
            .iter()
            .filter(|(schedule, _)| told(&verdicts, *schedule) == outcome)
            .collect();
        ran.sort_by_key(|(schedule, _)| *schedule);
        if ran.is_empty() {
            continue;
        }
        out.push_str(&format!("\n## {outcome} ({})\n", ran.len()));
        for (schedule, purpose) in ran {
            out.push_str(&format!("\n### #{schedule}\n\n"));
            out.push_str(&format!("{}\n\n", purpose.drove()));
            if let Some(why) = verdicts.get(schedule).and_then(Verdict::reason) {
                out.push_str(&format!("{why}\n\n"));
            }
            out.push_str("```\n");
            for line in events
                .iter()
                .filter(|event| event.about() == Some(*schedule))
                .flat_map(recorded)
            {
                out.push_str(&line);
                out.push('\n');
            }
            out.push_str("```\n");
        }
    }
    out
}

/// The sections a report has.
///
/// Ordered such that invariant failures come first.
const OUTCOMES: [&str; 8] = [
    "Durability broken",
    "Idempotency broken",
    "Recovery broken",
    "Convergence broken",
    "Broken, unattributed",
    "Inconclusive",
    "Passed",
    "Never finished",
];

/// Which section a schedule belongs under.
fn told(verdicts: &BTreeMap<u32, Verdict>, schedule: u32) -> &'static str {
    verdicts
        .get(&schedule)
        .map_or("Never finished", Verdict::kind)
}

/// The schedules the campaign set out to run, in the order it fitted them.
fn fitted(events: &[RunnerEvent]) -> Vec<(u32, Purpose)> {
    events
        .iter()
        .find_map(|event| match event {
            RunnerEvent::Fitted { schedules, .. } => Some(schedules.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// The verdict each schedule reached, where it reached one.
fn settled(events: &[RunnerEvent]) -> BTreeMap<u32, Verdict> {
    events
        .iter()
        .filter_map(|event| match event {
            RunnerEvent::Moved {
                schedule,
                to: Progress::Complete(verdict),
            } => Some((*schedule, verdict.clone())),
            _ => None,
        })
        .collect()
}

/// Which section of the report a verdict falls under.
trait Kind {
    fn kind(&self) -> &'static str;
}

impl Kind for Verdict {
    fn kind(&self) -> &'static str {
        match self {
            Verdict::Pass => "Passed",
            Verdict::Fail {
                invariant: Some(Invariant::Durable),
                ..
            } => "Durability broken",
            Verdict::Fail {
                invariant: Some(Invariant::Idempotent),
                ..
            } => "Idempotency broken",
            Verdict::Fail {
                invariant: Some(Invariant::Converges),
                ..
            } => "Convergence broken",
            Verdict::Fail {
                invariant: Some(Invariant::Recovers),
                ..
            } => "Recovery broken",
            Verdict::Fail {
                invariant: None, ..
            } => "Broken, unattributed",
            Verdict::Inconclusive { .. } => "Inconclusive",
        }
    }
}

/// What a schedule drove.
trait Drove {
    fn drove(&self) -> String;
}

impl Drove for Purpose {
    fn drove(&self) -> String {
        match self {
            Purpose::Learn => "The fault-free run.".to_owned(),
            Purpose::Reference { landed } => format!(
                "A clean run of steps {}.",
                landed
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Purpose::Break(fault) => format!("Drove the scenario while {}.", fault.taking()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crucible_core::{
        fault::{By, Fault},
        ipc::Verdict,
    };

    use super::*;

    fn fitted(schedules: Vec<(u32, Purpose)>) -> RunnerEvent {
        RunnerEvent::Fitted {
            schedules,
            eta: std::time::Duration::from_secs(60),
            workers: 3,
        }
    }

    fn settled(schedule: u32, verdict: Verdict) -> RunnerEvent {
        RunnerEvent::Moved {
            schedule,
            to: Progress::Complete(verdict),
        }
    }

    #[test]
    fn a_campaign_reports_every_schedule_it_fitted() {
        let events = vec![
            fitted(vec![
                (
                    1,
                    Purpose::Break(Box::new(Fault::throughout(By::Kill("db".to_owned())))),
                ),
                (2, Purpose::Reference { landed: vec![1, 2] }),
            ]),
            settled(1, Verdict::Pass),
        ];

        let report = render(&events, "orders");

        assert!(report.contains("#1"), "{report}");
        assert!(report.contains("killing db"), "{report}");
        assert!(
            report.contains("## Never finished (1)") && report.contains("#2"),
            "a schedule that never settled is still listed:\n{report}"
        );
    }

    #[test]
    fn a_fault_is_counted_under_the_invariant_it_broke() {
        let events = vec![
            fitted(vec![(1, Purpose::Learn), (2, Purpose::Learn)]),
            settled(1, Verdict::Pass),
            settled(
                2,
                Verdict::Fail {
                    invariant: Some(Invariant::Durable),
                    reason: "`orders.applied` held 4, wanted 5".to_owned(),
                },
            ),
        ];

        let report = render(&events, "orders");

        assert!(report.contains("## Durability broken (1)"), "{report}");
        assert!(report.contains("## Passed (1)"), "{report}");
        assert!(
            report.contains("`orders.applied` held 4, wanted 5"),
            "the reason a verdict gives is what the report is for:\n{report}"
        );
    }
}
