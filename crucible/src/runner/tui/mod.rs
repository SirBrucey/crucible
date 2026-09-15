//! The screen `crucible run` draws while a campaign runs.

mod panels;
mod state;

use std::time::Duration;

use crucible_core::{
    fault::{By, Edge, Fault},
    ipc::Verdict,
    schedule::{Phase, Purpose},
    verdict::Invariant,
};
use ratatui::{
    Frame,
    crossterm::event::{self, Event},
    layout::{Constraint, Layout},
    text::Line,
};
use state::{Dispatching, Doing, Learning, Row, RowState, State, Step, Worker};

/// Example until messaging is wired in.
pub fn preview() -> std::io::Result<()> {
    let mut state = example();
    let mut terminal = ratatui::init();
    let drawn = loop {
        if let Err(e) = terminal.draw(|frame| dispatch(frame, &mut state)) {
            break Err(e);
        }
        match event::read() {
            Ok(Event::Key(key)) if key.is_press() && state.on_press(key) => break Ok(()),
            Ok(_) => {}
            Err(e) => break Err(e),
        }
    };
    ratatui::restore();
    drawn
}

/// A campaign part way through, with every kind of row and worker on it.
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
        workers: vec![
            Worker {
                id: 0,
                doing: Doing::Running {
                    schedule: 182,
                    purpose: killing(),
                    phase: Phase::Driving,
                    step: Some(Step { taken: 3, of: 5 }),
                    held: Duration::from_secs(2),
                },
            },
            Worker {
                id: 1,
                doing: Doing::Paused,
            },
            Worker {
                id: 2,
                doing: Doing::Idle,
            },
            Worker {
                id: 3,
                doing: Doing::Failed { schedule: 177 },
            },
        ],
        stage: Learning,
    }
    .dispatch(
        vec![
            row(
                182,
                killing(),
                RowState::Complete(Verdict::Fail {
                    invariant: Some(Invariant::Durable),
                    reason: "`db` was killed during step 4, on 33 reads into what this edge \
                             carried. The fleet took 5 steps which left `orders.applied.count` \
                             at `4`, expected value `5`."
                        .to_owned(),
                }),
            ),
            row(181, killing(), RowState::Complete(Verdict::Pass)),
            row(180, cutting(), RowState::CounterExample),
            row(179, cutting(), RowState::Running { worker: 3 }),
            row(178, killing(), RowState::Pending),
            row(177, cutting(), RowState::Requeued),
            row(176, Purpose::Learn, RowState::Abandoned),
        ],
        Duration::from_secs(1260),
    )
}

/// Draw one frame of a campaign running the schedules the fit produced.
///
/// The regions are sized to leave the screen legible at the 80 by 24 the TUI
/// asks for, and the schedule list takes whatever a larger terminal gives.
pub fn dispatch(frame: &mut Frame, state: &mut State<Dispatching>) {
    let [header, fleet, schedules, detail, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(8),
        Constraint::Min(5),
        Constraint::Length(6),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    let [workers, stats] =
        Layout::horizontal([Constraint::Min(40), Constraint::Length(24)]).areas(fleet);

    frame.render_widget(panels::Header(state), header);
    frame.render_widget(panels::Workers(&state.workers), workers);
    frame.render_widget(panels::Stats(state), stats);
    frame.render_stateful_widget(
        panels::Schedules(&state.stage.schedules),
        schedules,
        &mut state.stage.selected,
    );
    frame.render_widget(panels::Detail(state.showing()), detail);
    frame.render_widget(
        Line::from(" [p]ause [s]kip [f]ocus  ↑↓ select  ↵ detail  [q]uit").centered(),
        footer,
    );
}

#[cfg(test)]
mod tests {
    use ratatui::{
        Terminal,
        backend::TestBackend,
        crossterm::event::{KeyCode, KeyEvent},
    };

    use super::*;

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
    fn the_detail_follows_the_selection() {
        let mut state = example();
        assert_eq!(state.showing().map(|row| row.schedule), Some(182));

        state.on_press(KeyEvent::from(KeyCode::Down));
        assert_eq!(state.showing().map(|row| row.schedule), Some(181));
    }

    #[test]
    fn the_selection_stops_at_either_end_of_the_list() {
        let mut state = example();
        for _ in 0..50 {
            state.on_press(KeyEvent::from(KeyCode::Down));
        }
        assert_eq!(state.showing().map(|row| row.schedule), Some(176));

        for _ in 0..50 {
            state.on_press(KeyEvent::from(KeyCode::Up));
        }
        assert_eq!(state.showing().map(|row| row.schedule), Some(182));
    }

    #[test]
    fn the_screen_fits_the_smallest_terminal_it_asks_for() {
        let screen = drawn(80, 24);

        assert!(screen.contains("crucible run orders.cru"), "{screen}");
        assert!(screen.contains("Workers"), "{screen}");
        assert!(screen.contains("Stats"), "{screen}");
        assert!(screen.contains("Schedules"), "{screen}");
        assert!(screen.contains("> #182"), "{screen}");
        assert!(screen.contains("#182 · fail durability"), "{screen}");
        assert!(
            screen.lines().all(|line| line.chars().count() <= 80),
            "{screen}"
        );
    }
}
