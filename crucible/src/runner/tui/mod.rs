//! The screen `crucible run` draws while a campaign runs.

mod panels;
mod state;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crucible_core::{
    ipc::{RunnerToWorker, WorkerEvent, WorkerToRunner},
    schedule::{Progress, Purpose},
};
use crucible_engine::event_bus::RunnerEvent;
use crucible_protocol::{At, FaultResult};
use panels::Short;
use ratatui::{
    Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent},
    layout::{Constraint, Layout},
    text::Line,
};
use state::{Dispatching, Learning, Looking, Row, State};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::controls::Controls;

/// How long a draw waits for a key before looking at the bus again.
const TICK: Duration = Duration::from_millis(100);

/// Draw what the campaign reports until it finishes or the user leaves.
///
/// Leaving cancels `interrupt`, so closing the screen stops the campaign.
pub async fn live(
    watching: State<Learning>,
    mut events: broadcast::Receiver<Arc<RunnerEvent>>,
    interrupt: CancellationToken,
    controls: Controls,
    journal: std::path::PathBuf,
) {
    let started = Instant::now();
    let mut terminal = ratatui::init();
    let mut screen = Screen::Learning(watching);
    loop {
        screen.ran(started.elapsed(), controls.paused());
        if let Err(e) = terminal.draw(|frame| screen.render(frame)) {
            tracing::error!(error = %e, "cannot draw the screen; leaving it");
            break;
        }
        // Drain the bus, then read the keyboard. Taking one event a turn would
        // let a busy campaign starve the keyboard.
        let mut stay = true;
        loop {
            match events.try_recv() {
                Ok(event) => screen.take(&event),
                Err(broadcast::error::TryRecvError::Closed) => {
                    stay = screen.over();
                    break;
                }
                // The screen is behind. The events still to come say where the
                // campaign is, so the dropped ones do not matter.
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(broadcast::error::TryRecvError::Empty) => break,
            }
        }
        if !stay {
            break;
        }
        // Waiting for a key is a blocking read, so it goes on its own thread.
        // Inlined it would hold a runtime worker for the whole tick.
        let waited = tokio::task::spawn_blocking(|| event::poll(TICK))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        match waited {
            Ok(false) => {}
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) if key.is_press() => {
                    if matches!(key.code, KeyCode::Enter | KeyCode::Esc) {
                        screen.toggle_journal(&journal).await;
                    } else if screen.reading() {
                        // The journal is over the screen; nothing under it moves.
                    } else if screen.on_press(key, &controls) {
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error = %e, "cannot read the keyboard; leaving the screen");
                    break;
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "cannot wait on the keyboard; leaving the screen");
                break;
            }
        }
    }
    ratatui::restore();
    interrupt.cancel();
}

/// What the screen shows before the scheduler has fitted anything.
pub fn watching(
    plan: &crucible_core::plan::Plan,
    scenario: &crucible_core::plan::Scenario,
    registry: &crucible_plugin::Registry,
) -> State<Learning> {
    // Only the plugins this campaign loads.
    let named: std::collections::BTreeSet<&str> = std::iter::once(plan.fleet.deployment.as_str())
        .chain(
            plan.fleet
                .services
                .iter()
                .flat_map(|service| service.kinds.iter().map(String::as_str)),
        )
        .chain(scenario.steps.iter().map(|step| step.driver.as_str()))
        .chain(scenario.checks.iter().map(|check| check.observer.as_str()))
        .collect();
    let plugins = registry
        .loaded()
        .into_iter()
        .filter(|(name, _)| named.contains(name.as_str()))
        .map(|(name, doing)| format!("{name}:{}", doing.join("/")))
        .collect();
    State {
        scenario: scenario.name.clone(),
        elapsed: Duration::ZERO,
        budget: scenario.budget,
        spec: format!("{:x}", plan.spec_hash().0),
        plugins,
        stage: Learning,
    }
}

fn recorded(event: &RunnerEvent) -> Vec<String> {
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

/// Draw a campaign that has nothing to show yet.
fn learn(frame: &mut Frame, state: &State<Learning>) {
    let [header, saying] =
        Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).areas(frame.area());
    frame.render_widget(panels::Header(state), header);
    frame.render_widget(
        Line::from("Bringing the fleet up and driving the fault-free run.").centered(),
        saying,
    );
}

/// What stage the campaign is in.
enum Screen {
    Learning(State<Learning>),
    Dispatching(State<Dispatching>),
}

impl Screen {
    /// Update the clock and the paused flag, so the screen moves between
    /// events.
    fn ran(&mut self, elapsed: Duration, held: bool) {
        match self {
            Screen::Learning(state) => state.elapsed = elapsed,
            Screen::Dispatching(state) => {
                state.elapsed = elapsed;
                state.stage.held = held;
            }
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        match self {
            Screen::Learning(state) => learn(frame, state),
            Screen::Dispatching(state) => dispatch(frame, state),
        }
    }

    /// Handle an event from the campaign.
    fn take(&mut self, event: &RunnerEvent) {
        match event {
            RunnerEvent::Fitted {
                schedules,
                eta,
                workers,
            } => {
                let rows = schedules
                    .iter()
                    .map(|(schedule, purpose)| Row {
                        schedule: *schedule,
                        purpose: purpose.clone(),
                        state: Progress::Pending,
                    })
                    .collect();
                if let Screen::Learning(state) = self {
                    *self = Screen::Dispatching(state.dispatch(rows, *eta, *workers));
                }
            }
            RunnerEvent::WorkerMessage {
                worker_id,
                message: WorkerToRunner::Event(WorkerEvent::Doing { phase, step }),
            } => {
                if let Screen::Dispatching(state) = self {
                    state.doing(*worker_id, *phase, *step);
                }
            }
            RunnerEvent::Moved { schedule, to } => {
                if let Screen::Dispatching(state) = self {
                    state.moved(*schedule, to.clone());
                }
            }
            _ => {}
        }
    }

    /// Mark the campaign finished.
    ///
    /// Returns whether there is anything left to look at.
    fn over(&mut self) -> bool {
        match self {
            Screen::Dispatching(state) => {
                state.stage.over = true;
                true
            }
            Screen::Learning(_) => false,
        }
    }

    /// Whether the journal is open over the screen.
    fn reading(&self) -> bool {
        match self {
            Screen::Dispatching(state) => state.evidence().is_some(),
            Screen::Learning(_) => false,
        }
    }

    /// Toggle what the journal recorded about the row.
    async fn toggle_journal(&mut self, journal: &std::path::Path) {
        let Screen::Dispatching(state) = self else {
            return;
        };
        if state.evidence().is_some() {
            state.close();
            return;
        }
        let Some(schedule) = state.showing().map(|row| row.schedule) else {
            return;
        };
        let lines = match crucible_engine::journal::about(journal, schedule).await {
            Ok(events) => events.iter().flat_map(recorded).collect(),
            Err(e) => vec![format!("the journal could not be read: {e}")],
        };
        state.read(schedule, lines);
    }

    fn on_press(&mut self, key: KeyEvent, controls: &Controls) -> bool {
        if let Screen::Dispatching(state) = &mut *self {
            // The help is open, so it takes the keys.
            if state.stage.helping {
                if matches!(key.code, KeyCode::Char('?' | 'q' | 'Q') | KeyCode::Esc) {
                    state.stage.helping = false;
                }
                return false;
            }
            if key.code == KeyCode::Char('?') {
                state.stage.helping = true;
                return false;
            }
        }
        if let KeyCode::Char('p' | 'P') = key.code {
            controls.pause(!controls.paused());
            return false;
        }
        if let KeyCode::Char('S') = key.code {
            controls.finish();
            return false;
        }
        if let (KeyCode::Char('s'), Screen::Dispatching(state)) = (key.code, &*self) {
            if let Some(row) = state.showing() {
                controls.skip(row.schedule);
            }
            return false;
        }
        match self {
            Screen::Learning(_) => matches!(key.code, KeyCode::Char('q' | 'Q')),
            Screen::Dispatching(state) => state.on_press(key),
        }
    }
}

/// Draw one frame of a campaign running the schedules the fit produced.
///
/// The regions are sized to leave the screen legible at the 80 by 24 the TUI
/// asks for, and the schedule list takes whatever a larger terminal gives.
pub fn dispatch(frame: &mut Frame, state: &mut State<Dispatching>) {
    let [header, fleet, schedules, detail, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(6),
        Constraint::Min(5),
        Constraint::Length(6),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    let [workers, stats] =
        Layout::horizontal([Constraint::Min(40), Constraint::Length(24)]).areas(fleet);

    frame.render_widget(panels::Header(state), header);
    frame.render_widget(
        panels::Workers {
            driving: state
                .stage
                .schedules
                .iter()
                .filter(|row| matches!(row.state, Progress::Running { .. }))
                .collect(),
            doing: &state.stage.doing,
            panes: state.stage.workers,
        },
        workers,
    );
    frame.render_widget(panels::Stats(state), stats);
    let [queued_area, found_area] =
        Layout::horizontal([Constraint::Percentage(45), Constraint::Percentage(55)])
            .areas(schedules);
    let (queued, found) = (
        state::queued(&state.stage.schedules),
        state::found(&state.stage.schedules),
    );
    let looking = state.stage.looking;
    frame.render_stateful_widget(
        panels::Schedules {
            rows: queued,
            titled: "Queued",
            looking: looking == Looking::Queued,
        },
        queued_area,
        &mut state.stage.selected,
    );
    frame.render_stateful_widget(
        panels::Schedules {
            rows: found,
            titled: "Found",
            looking: looking == Looking::Found,
        },
        found_area,
        &mut state.stage.found,
    );
    frame.render_widget(panels::Detail(state.showing()), detail);
    if let (Some(row), Some(lines)) = (state.showing(), state.evidence()) {
        frame.render_widget(panels::Journal(row.schedule, lines), frame.area());
    }
    if state.stage.helping {
        frame.render_widget(panels::Help, frame.area());
    }
    frame.render_widget(
        Line::from(if state.stage.held {
            " held · [p] resume  [s]kip  [S] finish  ↑↓ select  [q]uit"
        } else {
            " [p]ause  [s]kip  [S] finish  ↑↓←→ select  ↵ journal  [?] help  [q]uit"
        })
        .centered(),
        footer,
    );
}

#[cfg(test)]
mod tests {
    use crucible_core::{
        fault::{By, Edge, Fault},
        ipc::Verdict,
        verdict::Invariant,
    };
    use ratatui::{
        Terminal,
        backend::TestBackend,
        buffer::Buffer,
        crossterm::event::{KeyCode, KeyEvent},
        layout::Rect,
        widgets::Widget,
    };

    use super::*;

    /// A campaign part way through, with a row in every state.
    fn example() -> State<Dispatching> {
        let killing = || Purpose::Break(Box::new(Fault::throughout(By::Kill("db".to_owned()))));
        let cutting = || {
            Purpose::Break(Box::new(Fault::throughout(By::Cut(Edge {
                client: Some("api".to_owned()),
                upstream: "broker".to_owned(),
            }))))
        };
        let row = |schedule, purpose, state| Row {
            schedule,
            purpose,
            state,
        };

        State {
            scenario: "orders.cru".to_owned(),
            elapsed: Duration::from_secs(323),
            budget: Some(Duration::from_secs(1800)),
            spec: "a31c".to_owned(),
            plugins: vec!["amqp".to_owned(), "http".to_owned(), "mariadb".to_owned()],
            stage: Learning,
        }
        .dispatch(
            vec![
                row(
                    182,
                    killing(),
                    Progress::Complete(Verdict::Fail {
                        invariant: Some(Invariant::Durable),
                        reason: "`db` was killed during step 4, on 33 reads into what this edge \
                                 carried. The fleet took 5 steps which left `orders.applied.count` \
                                 at `4`, expected value `5`."
                            .to_owned(),
                    }),
                ),
                row(181, killing(), Progress::Complete(Verdict::Pass)),
                row(
                    180,
                    cutting(),
                    Progress::CounterExample {
                        wants: vec![vec![1, 2, 4, 5]],
                    },
                ),
                row(179, cutting(), Progress::Running { worker: 3 }),
                row(178, killing(), Progress::Pending),
                row(177, cutting(), Progress::Requeued),
                row(176, Purpose::Learn, Progress::Abandoned),
            ],
            Duration::from_secs(1260),
            3,
        )
    }

    fn drawn(width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("a test terminal");
        terminal
            .draw(|frame| dispatch(frame, &mut example()))
            .expect("the screen draws");
        let buf = terminal.backend().buffer();
        (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .filter_map(|x| buf.cell((x, y)).map(|cell| cell.symbol().to_owned()))
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_clock_moves_between_events_that_change_nothing_else() {
        let mut screen = Screen::Dispatching(example());
        screen.ran(Duration::from_secs(323), false);

        let mut buf = Buffer::empty(Rect::new(0, 0, 80, 3));
        match &screen {
            Screen::Dispatching(state) => panels::Header(state).render(buf.area, &mut buf),
            Screen::Learning(_) => unreachable!("built as dispatching"),
        }
        let top: String = (0..buf.area.width)
            .filter_map(|x| buf.cell((x, 0)).map(|cell| cell.symbol().to_owned()))
            .collect();

        assert!(top.contains("0:05:23"), "{top}");
    }

    #[test]
    fn the_detail_follows_the_selection() {
        let mut state = example();
        assert_eq!(
            state.showing().map(|row| row.schedule),
            Some(179),
            "the run a worker has, above the ones waiting"
        );

        state.on_press(KeyEvent::from(KeyCode::Down));
        assert_eq!(state.showing().map(|row| row.schedule), Some(180));
    }

    #[test]
    fn the_selection_stops_at_either_end_of_the_queue() {
        let mut state = example();
        for _ in 0..50 {
            state.on_press(KeyEvent::from(KeyCode::Down));
        }
        assert_eq!(state.showing().map(|row| row.schedule), Some(177));

        for _ in 0..50 {
            state.on_press(KeyEvent::from(KeyCode::Up));
        }
        assert_eq!(state.showing().map(|row| row.schedule), Some(179));
    }

    #[test]
    fn a_schedule_that_finishes_leaves_the_queue() {
        let mut state = example();
        let queued = state.queued().len();

        state.moved(179, Progress::Complete(Verdict::Pass));

        assert_eq!(state.queued().len(), queued - 1);
        assert!(
            state.queued().iter().all(|row| row.schedule != 179),
            "a settled schedule is no longer waiting"
        );
    }

    #[test]
    fn help() {
        let mut state = example();
        state.stage.helping = true;
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("a test terminal");
        terminal
            .draw(|frame| dispatch(frame, &mut state))
            .expect("the screen draws");
        let buf = terminal.backend().buffer();
        let screen: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .filter_map(|x| buf.cell((x, y)).map(|cell| cell.symbol().to_owned()))
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(screen.contains("What the screen says"), "{screen}");
        assert!(screen.contains("give up on the selected"), "{screen}");
        assert!(screen.contains("observe"), "{screen}");
    }

    #[test]
    fn the_eta_is_read_from_what_the_campaign_has_cost() {
        let mut state = example();
        state.elapsed = Duration::from_secs(300);

        state.moved(179, Progress::Complete(Verdict::Pass));

        assert_eq!(state.eta(), Duration::from_secs(225));
    }

    #[test]
    fn the_eta_counts_down_between_the_schedules_that_revise_it() {
        let mut state = example();
        state.elapsed = Duration::from_secs(300);
        state.moved(179, Progress::Complete(Verdict::Pass));

        state.elapsed = Duration::from_secs(360);

        assert_eq!(state.eta(), Duration::from_secs(165));
    }

    #[test]
    fn a_run_picked_up_does_not_make_the_campaign_look_longer() {
        let mut state = example();
        state.elapsed = Duration::from_secs(300);
        state.moved(179, Progress::Complete(Verdict::Pass));
        let priced = state.eta();

        state.elapsed = Duration::from_secs(400);
        state.moved(178, Progress::Running { worker: 4 });

        assert!(
            state.eta() < priced,
            "an estimate that climbs while the campaign waits is counting up, not down"
        );
    }

    #[test]
    fn a_finished_campaign_leaves_the_screen_up() {
        let mut screen = Screen::Dispatching(example());
        assert!(screen.over(), "there is something to look at");

        let Screen::Dispatching(state) = &screen else {
            unreachable!("built as dispatching")
        };
        assert!(state.stage.over);
    }

    #[test]
    fn a_verdict_landing_leaves_the_reader_where_they_were() {
        let mut state = example();
        state.on_press(KeyEvent::from(KeyCode::Right));
        state.on_press(KeyEvent::from(KeyCode::Down));
        let reading = state.showing().map(|row| row.schedule);
        assert_eq!(reading, Some(181));
        state.read(181, vec!["what the journal said".to_owned()]);

        // Another schedule settles, so a row arrives above the one being read.
        state.moved(179, Progress::Complete(Verdict::Pass));

        assert_eq!(
            state.showing().map(|row| row.schedule),
            Some(181),
            "still on the row that was being read"
        );
        assert!(state.evidence().is_some(), "and its journal is still up");
    }

    #[test]
    fn the_arrows_reach_what_was_found() {
        let mut state = example();
        assert_eq!(state.showing().map(|row| row.schedule), Some(179));

        state.on_press(KeyEvent::from(KeyCode::Right));

        assert_eq!(
            state.showing().map(|row| row.schedule),
            Some(182),
            "the first of what was found"
        );
    }

    #[test]
    fn the_screen_fits_the_smallest_terminal_it_asks_for() {
        let screen = drawn(80, 24);

        assert!(screen.contains("crucible run orders.cru"), "{screen}");
        assert!(screen.contains("Workers"), "{screen}");
        assert!(screen.contains("Stats"), "{screen}");
        assert!(screen.contains("Queued"), "{screen}");
        assert!(screen.contains("Found"), "{screen}");
        assert!(
            screen.lines().all(|line| line.chars().count() <= 80),
            "{screen}"
        );
    }

    #[test]
    fn the_row_under_the_cursor_is_the_row_the_detail_describes() {
        let state = example();
        let showing = state
            .showing()
            .map(|row| row.schedule)
            .expect("a selection");
        let screen = drawn(80, 24);

        let highlighted = screen
            .lines()
            .find(|line| line.contains("> #"))
            .expect("a highlighted row");

        assert!(
            highlighted.contains(&format!("#{showing}")),
            "the cursor sits on the row the detail is about, not one beside it:\n{screen}"
        );
        assert!(
            screen.contains(&format!("#{showing} ·")),
            "the detail is titled with that same row:\n{screen}"
        );
    }
}
