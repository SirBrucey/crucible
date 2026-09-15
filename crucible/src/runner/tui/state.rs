//! What the screen shows.
//!
//! The display frame is rendered directly from the [`State`]. Only the state
//! is mutated, and then the frame is re-rendered.

use std::time::Duration;

use crucible_core::{
    ipc::Verdict,
    schedule::{Phase, Purpose},
    verdict::Invariant,
};
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent},
    widgets::ListState,
};

/// Everything one frame of the screen shows.
pub struct State<S> {
    /// The scenario file the campaign is running.
    pub scenario: String,
    /// How long the campaign has been running.
    pub elapsed: Duration,
    /// How long the campaign is allowed to run.
    pub budget: Option<Duration>,
    /// The spec hash.
    pub spec: String,
    /// The plugins the fleet loaded.
    pub plugins: Vec<String>,
    /// One card per worker.
    pub workers: Vec<Worker>,
    pub stage: S,
}

/// Bringing the fleet up and driving the fault-free run.
pub struct Learning;

/// Running the schedules the scheduler produced.
pub struct Dispatching {
    /// The campaign's schedules, most recently changed first.
    pub schedules: Vec<Row>,
    /// The row the detail region is showing.
    pub selected: ListState,
    /// How long the campaign has left.
    pub eta: Duration,
}

impl State<Learning> {
    /// The campaign once the scheduler has fitted it, holding a row per
    /// schedule.
    pub fn dispatch(self, schedules: Vec<Row>, eta: Duration) -> State<Dispatching> {
        State {
            scenario: self.scenario,
            elapsed: self.elapsed,
            budget: self.budget,
            spec: self.spec,
            plugins: self.plugins,
            workers: self.workers,
            stage: Dispatching {
                schedules,
                selected: ListState::default().with_selected(Some(0)),
                eta,
            },
        }
    }
}

impl State<Dispatching> {
    /// The row the detail region is showing.
    pub fn showing(&self) -> Option<&Row> {
        self.stage.schedules.get(self.stage.selected.selected()?)
    }

    /// Take a key, saying whether it asks to quit.
    pub fn on_press(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('q' | 'Q') => return true,
            KeyCode::Down => self.select(1),
            KeyCode::Up => self.select(-1),
            _ => {}
        }
        false
    }

    /// Move the selection `by` rows, stopping at either end of the list.
    fn select(&mut self, by: isize) {
        let last = self.stage.schedules.len().saturating_sub(1);
        let at = self.stage.selected.selected().unwrap_or(0);
        self.stage
            .selected
            .select(Some(at.saturating_add_signed(by).min(last)));
    }

    /// Every verdict the campaign has reached.
    ///
    /// The stats panel's counters are folds over this.
    pub fn verdicts(&self) -> impl Iterator<Item = &Verdict> {
        self.stage
            .schedules
            .iter()
            .filter_map(|row| match &row.state {
                RowState::Complete(verdict) => Some(verdict),
                _ => None,
            })
    }

    /// What the campaign has found so far.
    pub fn stats(&self) -> Stats {
        let mut stats = Stats::default();
        for verdict in self.verdicts() {
            match verdict {
                Verdict::Pass => stats.passed += 1,
                Verdict::Inconclusive { .. } => stats.inconclusive += 1,
                Verdict::Fail { invariant, .. } => {
                    stats.failed += 1;
                    match invariant {
                        Some(Invariant::Idempotent) => stats.idempotent += 1,
                        Some(Invariant::Converges) => stats.converges += 1,
                        Some(Invariant::Durable) => stats.durable += 1,
                        Some(Invariant::Recovers) => stats.recovers += 1,
                        None => stats.unattributed += 1,
                    }
                }
            }
        }
        stats
    }
}

/// What a campaign has found, folded from its rows.
#[derive(Default)]
pub struct Stats {
    pub passed: usize,
    pub failed: usize,
    pub inconclusive: usize,
    pub idempotent: usize,
    pub converges: usize,
    pub durable: usize,
    pub recovers: usize,
    /// Failures that named no invariant. Counted rather than dropped: it is a
    /// defect the campaign found and could not attribute.
    pub unattributed: usize,
}

/// One worker's card.
pub struct Worker {
    /// The worker's ID, this is generated when the runner spawns the worker.
    pub id: u32,
    pub doing: Doing,
}

/// What a worker is doing.
pub enum Doing {
    /// Driving a schedule.
    Running {
        schedule: u32,
        purpose: Purpose,
        phase: Phase,
        /// Steps driven.
        step: Option<Step>,
        /// How long it has been in this phase.
        held: Duration,
    },
    /// Holding, because the user paused.
    Paused,
    /// Waiting for a schedule.
    Idle,
    /// The schedule it last drove failed.
    Failed { schedule: u32 },
}

/// How far through the scenario's steps a run has got.
pub struct Step {
    pub taken: usize,
    pub of: usize,
}

/// One schedule.
pub struct Row {
    /// The schedule's id.
    pub schedule: u32,
    /// What the schedule tests.
    pub purpose: Purpose,
    pub state: RowState,
}

/// Where a schedule has got to.
pub enum RowState {
    /// Not yet sent to a worker.
    Pending,
    /// A worker is driving it.
    Running { worker: u32 },
    /// Deferred until a reference run has completed.
    CounterExample,
    /// A transient worker failure; it is being rescheduled.
    Requeued,
    /// An interrupt stopped it.
    Abandoned,
    /// It reached a verdict.
    Complete(Verdict),
}
