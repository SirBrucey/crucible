//! What the screen shows.
//!
//! The display frame is rendered directly from the [`State`]. Only the state
//! is mutated, and then the frame is re-rendered.

use std::{collections::BTreeMap, time::Duration};

use crucible_core::{
    ipc::Verdict,
    schedule::{Phase, Progress, Purpose, Step},
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
    /// Where the run's report is written.
    pub report: std::path::PathBuf,
    pub stage: S,
}

/// The campaign is bringing the fleet up and driving the fault-free run.
pub struct Learning;

/// The campaign is running the schedules the scheduler produced.
pub struct Dispatching {
    /// The campaign's schedules, most recently changed first.
    pub schedules: Vec<Row>,
    /// The row the detail region is showing.
    pub selected: ListState,
    /// How long the campaign had left when it was last worked out.
    pub eta: Duration,
    /// How long the campaign has been running when it was worked out.
    pub eta_at: Duration,
    /// Whether the campaign is paused.
    pub held: bool,
    /// How far through its run each worker says it is.
    pub doing: BTreeMap<u32, (Phase, Option<Step>)>,
    /// The journal lines for the row the user asked about. Cleared when the selection moves.
    pub evidence: Option<(u32, Vec<String>)>,
    /// How many runs the campaign has going at once.
    pub workers: usize,
    /// Whether the help is open.
    pub helping: bool,
    /// Whether the campaign has finished.
    pub over: bool,
    /// Which of the two lists the arrow keys are moving through.
    pub looking: Looking,
    /// Where the cursor sits in the found list.
    pub found: ListState,
}

impl State<Learning> {
    /// Move to dispatching, with a row per schedule the scheduler fitted.
    pub fn dispatch(
        &self,
        schedules: Vec<Row>,
        eta: Duration,
        workers: usize,
    ) -> State<Dispatching> {
        State {
            scenario: self.scenario.clone(),
            elapsed: self.elapsed,
            budget: self.budget,
            spec: self.spec.clone(),
            plugins: self.plugins.clone(),
            report: self.report.clone(),
            stage: Dispatching {
                schedules,
                selected: ListState::default().with_selected(Some(0)),
                eta,
                eta_at: Duration::ZERO,
                held: false,
                doing: BTreeMap::new(),
                evidence: None,
                workers,
                helping: false,
                over: false,
                looking: Looking::Queued,
                found: ListState::default().with_selected(Some(0)),
            },
        }
    }
}

impl State<Dispatching> {
    /// This campaign's [`queued`] rows.
    pub fn queued(&self) -> Vec<&Row> {
        queued(&self.stage.schedules)
    }

    /// This campaign's [`found`] rows.
    pub fn found(&self) -> Vec<&Row> {
        found(&self.stage.schedules)
    }

    /// The selected row, from whichever list is being looked through.
    pub fn showing(&self) -> Option<&Row> {
        match self.stage.looking {
            Looking::Found => return self.found().into_iter().nth(self.stage.found.selected()?),
            Looking::Queued => {}
        }
        self.queued()
            .into_iter()
            .nth(self.stage.selected.selected()?)
    }

    /// Handle a key press. Returns true if asked to quit.
    pub fn on_press(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('q' | 'Q') => return true,
            KeyCode::Down => self.select(1),
            KeyCode::Up => self.select(-1),
            KeyCode::Left => self.look(Looking::Queued),
            KeyCode::Right => self.look(Looking::Found),
            KeyCode::Tab => self.look(match self.stage.looking {
                Looking::Queued => Looking::Found,
                Looking::Found => Looking::Queued,
            }),
            _ => {}
        }
        false
    }

    /// Record what a worker says it is doing.
    pub fn doing(&mut self, worker: u32, phase: Phase, step: Option<Step>) {
        self.stage.doing.insert(worker, (phase, step));
    }

    /// Record a schedule's new state, moving its row to the top of the list.
    ///
    /// Most recently changed first, so a burst of verdicts reads in the order
    /// it arrived.
    pub fn moved(&mut self, schedule: u32, to: Progress) {
        let Some(at) = self
            .stage
            .schedules
            .iter()
            .position(|row| row.schedule == schedule)
        else {
            return;
        };
        // Remember the schedule each cursor is on, not its position. Rows move
        // as they change, and the user should stay on the row they are reading.
        let (queued, found) = (self.on(Looking::Queued), self.on(Looking::Found));

        let finished = to.finished();
        let mut row = self.stage.schedules.remove(at);
        if let (Progress::Running { worker }, false) =
            (&row.state, matches!(to, Progress::Running { .. }))
        {
            self.stage.doing.remove(worker);
        }
        row.state = to;
        self.stage.schedules.insert(0, row);

        self.back(Looking::Queued, queued);
        self.back(Looking::Found, found);
        if finished {
            self.price();
        }
    }

    /// Which schedule a list's cursor is on.
    fn on(&self, looking: Looking) -> Option<u32> {
        let (rows, cursor) = match looking {
            Looking::Queued => (self.queued(), &self.stage.selected),
            Looking::Found => (self.found(), &self.stage.found),
        };
        rows.get(cursor.selected()?).map(|row| row.schedule)
    }

    /// Put a list's cursor back on the schedule it was on. If that schedule
    /// has left the list, keep the cursor as close as it can.
    fn back(&mut self, looking: Looking, was: Option<u32>) {
        let rows = match looking {
            Looking::Queued => self.queued(),
            Looking::Found => self.found(),
        };
        let at = was
            .and_then(|schedule| rows.iter().position(|row| row.schedule == schedule))
            .unwrap_or_else(|| {
                let cursor = match looking {
                    Looking::Queued => &self.stage.selected,
                    Looking::Found => &self.stage.found,
                };
                cursor.selected().unwrap_or(0)
            })
            .min(rows.len().saturating_sub(1));
        match looking {
            Looking::Queued => self.stage.selected.select(Some(at)),
            Looking::Found => self.stage.found.select(Some(at)),
        }
    }

    /// Show the journal lines for a row.
    pub fn read(&mut self, schedule: u32, evidence: Vec<String>) {
        self.stage.evidence = Some((schedule, evidence));
    }

    /// Close the journal.
    pub fn close(&mut self) {
        self.stage.evidence = None;
    }

    /// The journal lines for the selected row, if the journal is open.
    pub fn evidence(&self) -> Option<&[String]> {
        let (schedule, lines) = self.stage.evidence.as_ref()?;
        (self.showing()?.schedule == *schedule).then_some(lines.as_slice())
    }

    /// Switch to the other list. Closes the journal.
    fn look(&mut self, at: Looking) {
        self.stage.looking = at;
        self.stage.evidence = None;
    }

    /// Move the selection `by` rows, stopping at either end of the list.
    fn select(&mut self, by: isize) {
        let last = match self.stage.looking {
            Looking::Queued => self.queued().len(),
            Looking::Found => self.found().len(),
        }
        .saturating_sub(1);
        let cursor = match self.stage.looking {
            Looking::Queued => &mut self.stage.selected,
            Looking::Found => &mut self.stage.found,
        };
        let at = cursor.selected().unwrap_or(0);
        cursor.select(Some(at.saturating_add_signed(by).min(last)));
    }

    /// Every verdict the campaign has reached. The stats panel counts these.
    pub fn verdicts(&self) -> impl Iterator<Item = &Verdict> {
        self.stage
            .schedules
            .iter()
            .filter_map(|row| match &row.state {
                Progress::Complete(verdict) => Some(verdict),
                _ => None,
            })
    }

    /// How long the campaign has left.
    ///
    /// The fit's estimate until schedules start settling, then based on what this
    /// campaign is actually costing.
    pub fn eta(&self) -> Duration {
        self.stage
            .eta
            .saturating_sub(self.elapsed.saturating_sub(self.stage.eta_at))
    }

    /// Work out what is left from what the campaign has cost so far.
    // Only called when a schedule settles.
    fn price(&mut self) {
        let settled = u32::try_from(self.found().len()).unwrap_or(u32::MAX);
        if settled == 0 {
            return;
        }
        let left = u32::try_from(self.stage.schedules.len())
            .unwrap_or(u32::MAX)
            .saturating_sub(settled);
        self.stage.eta = (self.elapsed / settled).saturating_mul(left);
        self.stage.eta_at = self.elapsed;
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

/// The schedules still to run or running, in the order they are shown.
// Takes the rows rather than the whole state so the screen can order a list and
// write the cursor beside it in one borrow. The cursor indexes into this order,
// so both have to use it.
pub fn queued(schedules: &[Row]) -> Vec<&Row> {
    // Running first, then waiting. A run going now matters more to the reader
    // than one that has not started.
    let (running, waiting): (Vec<&Row>, Vec<&Row>) = schedules
        .iter()
        .filter(|row| !row.state.finished())
        .partition(|row| matches!(row.state, Progress::Running { .. }));
    running.into_iter().chain(waiting).collect()
}

/// The schedules that have finished, most recently changed first.
pub fn found(schedules: &[Row]) -> Vec<&Row> {
    schedules
        .iter()
        .filter(|row| row.state.finished())
        .collect()
}

/// Which of the two lists the user is moving through.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Looking {
    Queued,
    Found,
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

/// One schedule.
pub struct Row {
    /// The schedule's id.
    pub schedule: u32,
    /// What the schedule tests.
    pub purpose: Purpose,
    pub state: Progress,
}
