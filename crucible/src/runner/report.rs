//! What a campaign wrote down, rendered for a reader.
//!
//! Whatever renders a run reads the journal.

use std::borrow::Cow;

use crucible_core::{
    ipc::{RunnerToWorker, Verdict, WorkerEvent, WorkerToRunner},
    schedule::{Phase, Progress, Purpose},
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
        .map(|observed| observed.check.observable.join("."))
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
            "{} settled {}, wanted {:?}",
            observed.check.observable.join("."),
            observed
                .value
                .as_ref()
                .map_or("nothing".to_owned(), |value| format!("{value:?}")),
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
                drove.map_or("nothing".to_owned(), |value| format!("{value:?}")),
                learned.map_or("nothing".to_owned(), |value| format!("{value:?}")),
            )
        })
    })
}
