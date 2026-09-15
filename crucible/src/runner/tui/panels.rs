//! The panels the screen is made of.

use std::{borrow::Cow, time::Duration};

use crucible_core::{
    ipc::Verdict,
    schedule::{Phase, Purpose},
};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    text::Line,
    widgets::{Block, List, ListState, Paragraph, StatefulWidget, Widget, Wrap},
};

use super::state::{Dispatching, Doing, Row, RowState, State, Step, Worker};

/// What is being run, how long it has been running, and what it loaded.
pub struct Header<'a>(pub &'a State<Dispatching>);

impl Widget for Header<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title_top(format!(" crucible run {} ", self.0.scenario))
            .title_top(
                Line::from(format!(" {} ", clock(self.0.elapsed, self.0.budget))).right_aligned(),
            );
        let [left, right] = Layout::horizontal([Constraint::Min(20), Constraint::Length(12)])
            .areas(block.inner(area));
        block.render(area, buf);

        Line::from(format!(
            "spec: {}  plugins: {}",
            self.0.spec,
            self.0.plugins.join(", ")
        ))
        .render(left, buf);
        Line::from(format!("workers: {}", self.0.workers.len()))
            .right_aligned()
            .render(right, buf);
    }
}

/// A card per worker.
pub struct Workers<'a>(pub &'a [Worker]);

impl Widget for Workers<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered().title_top(" Workers ");
        let inner = block.inner(area);
        block.render(area, buf);

        let cards = Layout::horizontal(self.0.iter().map(|_| Constraint::Length(10)))
            .spacing(1)
            .split(inner);
        for (worker, card) in self.0.iter().zip(cards.iter()) {
            Card(worker).render(*card, buf);
        }
    }
}

/// One worker.
struct Card<'a>(&'a Worker);

impl Widget for Card<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered().title_top(format!(" W{} ", self.0.id));
        let inner = block.inner(area);
        block.render(area, buf);

        for (text, row) in self.lines().into_iter().zip(inner.rows()) {
            text.render(row, buf);
        }
    }
}

impl<'a> Card<'a> {
    /// What the card has room to say, a line at a time.
    fn lines(&self) -> Vec<Line<'a>> {
        match &self.0.doing {
            Doing::Running {
                schedule,
                purpose,
                phase,
                step,
                held,
            } => vec![
                purpose.short().into(),
                match step {
                    Some(Step { taken, of }) => format!("{} {taken}/{of}", phase.short()).into(),
                    None => format!("{} {:.1}s", phase.short(), held.as_secs_f32()).into(),
                },
                format!("#{schedule}").into(),
            ],
            Doing::Paused => vec!["paused".into()],
            Doing::Idle => vec!["idle".into()],
            Doing::Failed { schedule } => vec!["failed".into(), format!("#{schedule}").into()],
        }
    }
}

/// How much of the campaign has settled, and what it found.
pub struct Stats<'a>(pub &'a State<Dispatching>);

impl Widget for Stats<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title_top(" Stats ")
            .title_top(Line::from(format!(" ETA {} ", hms(self.0.stage.eta))).right_aligned());
        let inner = block.inner(area);
        block.render(area, buf);

        let found = self.0.stats();
        let settled = found.passed + found.failed + found.inconclusive;
        let mut lines = vec![
            format!("Schedules: {settled}/{}", self.0.stage.schedules.len()).into(),
            format!("  pass: {}", found.passed).into(),
            format!("  fail: {}", found.failed).into(),
            format!("  inc:  {}", found.inconclusive).into(),
        ];
        lines.extend(named(&found).map(|(what, count)| format!("{what}: {count}").into()));

        for (text, row) in lines.into_iter().zip(inner.rows()) {
            Line::render(text, row, buf);
        }
    }
}

/// The invariants a campaign has shown broken, and how often.
fn named(found: &super::state::Stats) -> impl Iterator<Item = (&'static str, usize)> {
    [
        ("durability", found.durable),
        ("idempotency", found.idempotent),
        ("recovery", found.recovers),
        ("convergence", found.converges),
        ("unattributed", found.unattributed),
    ]
    .into_iter()
    .filter(|(_, count)| *count > 0)
}

/// Every schedule the campaign holds, most recently changed first.
pub struct Schedules<'a>(pub &'a [Row]);

impl StatefulWidget for Schedules<'_> {
    type State = ListState;

    fn render(self, area: Rect, buf: &mut Buffer, selected: &mut ListState) {
        let rows = self.0.iter().map(|row| {
            Line::from(format!(
                "#{:<5} {:<16} {}",
                row.schedule,
                row.state.short(),
                row.summary()
            ))
        });
        StatefulWidget::render(
            List::new(rows)
                .block(Block::bordered().title_top(format!(" Schedules ({}) ", self.0.len())))
                .highlight_symbol("> "),
            area,
            buf,
            selected,
        );
    }
}

impl Row {
    fn summary(&self) -> Cow<'_, str> {
        match (&self.state, &self.purpose) {
            (
                RowState::Complete(Verdict::Fail { reason, .. } | Verdict::Inconclusive { reason }),
                _,
            ) => reason
                .split_once(". ")
                .map_or(reason.as_str(), |(first, _)| first)
                .into(),
            (_, Purpose::Learn) => "the fault-free run".into(),
            (_, Purpose::Reference { landed }) => format!(
                "steps {}",
                landed
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .into(),
            (_, Purpose::Break(fault)) => fault.taking().to_string().into(),
        }
    }
}

/// The selected row, at length.
pub struct Detail<'a>(pub Option<&'a Row>);

impl Widget for Detail<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let Some(row) = self.0 else {
            Block::bordered().render(area, buf);
            return;
        };
        let block = Block::bordered().title_top(format!(
            " #{} · {} ",
            row.schedule,
            row.state.short().to_lowercase()
        ));
        let inner = block.inner(area);
        block.render(area, buf);

        Paragraph::new(row.detail())
            .wrap(Wrap { trim: true })
            .render(inner, buf);
    }
}

impl Row {
    fn detail(&self) -> String {
        match &self.state {
            RowState::Complete(Verdict::Fail { reason, .. } | Verdict::Inconclusive { reason }) => {
                reason.clone()
            }
            RowState::Complete(Verdict::Pass) => {
                format!("The fleet held under {}.", self.summary())
            }
            RowState::CounterExample => format!(
                "The fleet turned some steps away, so this run cannot be compared step by step. \
                 A clean run of just the steps it accepted is under way, to see where they leave \
                 the fleet. It drives {}.",
                self.summary()
            ),
            RowState::Pending => format!("Not yet run. Will drive {}.", self.summary()),
            RowState::Running { worker } => {
                format!("Running on worker {worker}, driving {}.", self.summary())
            }
            RowState::Requeued => format!("A worker failed; driving {} again.", self.summary()),
            RowState::Abandoned => {
                "Stopped by an interrupt, so it says nothing about the fleet.".to_owned()
            }
        }
    }
}

/// Display forms for types the panels render but do not own.
trait Short {
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

impl Short for RowState {
    fn short(&self) -> Cow<'static, str> {
        match self {
            RowState::Pending => "Pending".into(),
            RowState::Running { .. } => "Running".into(),
            RowState::CounterExample => "Counter Example".into(),
            RowState::Requeued => "Requeued".into(),
            RowState::Abandoned => "Abandoned".into(),
            RowState::Complete(verdict) => verdict.short(),
        }
    }
}

impl Short for Purpose {
    fn short(&self) -> Cow<'static, str> {
        match self {
            Purpose::Learn => "learn".into(),
            Purpose::Reference { .. } => "ref".into(),
            Purpose::Break(_) => "break".into(),
        }
    }
}

impl Short for Phase {
    fn short(&self) -> Cow<'static, str> {
        match self {
            Phase::Setup => "SU".into(),
            Phase::Driving => "DR".into(),
            Phase::Healing => "HL".into(),
            Phase::Observing => "OB".into(),
            Phase::Tearing => "TD".into(),
        }
    }
}

/// How long a campaign has run, against what it is allowed.
fn clock(elapsed: Duration, budget: Option<Duration>) -> String {
    match budget {
        Some(budget) => format!("{} / {}", hms(elapsed), hms(budget)),
        None => hms(elapsed),
    }
}

/// A duration as hours, minutes and seconds.
fn hms(of: Duration) -> String {
    let seconds = of.as_secs();
    format!(
        "{}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
}

#[cfg(test)]
mod tests {
    use crucible_core::{
        fault::{By, Fault},
        ipc::Verdict,
        schedule::Purpose,
        verdict::Invariant,
    };
    use rstest::rstest;

    use super::*;
    use crate::tui::state::{Learning, Row, RowState};

    fn running(budget: Option<Duration>, schedules: Vec<Row>) -> State<Dispatching> {
        State {
            scenario: "orders.cru".to_owned(),
            elapsed: Duration::from_secs(323),
            budget,
            spec: "a31c".to_owned(),
            plugins: vec!["amqp".to_owned(), "http".to_owned()],
            workers: Vec::new(),
            stage: Learning,
        }
        .dispatch(schedules, Duration::from_secs(1260))
    }

    fn line(buf: &Buffer, y: u16) -> String {
        (0..buf.area.width)
            .filter_map(|x| buf.cell((x, y)).map(|cell| cell.symbol().to_owned()))
            .collect()
    }

    #[rstest]
    #[case(Some(Duration::from_secs(1800)), "0:05:23 / 0:30:00")]
    #[case(None, "0:05:23")]
    fn clock_reads_against_a_budget_only_when_one_is_given(
        #[case] budget: Option<Duration>,
        #[case] expected: &str,
    ) {
        assert_eq!(clock(Duration::from_secs(323), budget), expected);
    }

    fn row(schedule: u32, state: RowState) -> Row {
        Row {
            schedule,
            purpose: Purpose::Learn,
            state,
        }
    }

    fn failed(invariant: Option<Invariant>) -> RowState {
        RowState::Complete(Verdict::Fail {
            invariant,
            reason: String::new(),
        })
    }

    #[test]
    fn a_failure_that_named_nothing_is_counted_rather_than_dropped() {
        let state = running(
            None,
            vec![
                row(1, RowState::Complete(Verdict::Pass)),
                row(2, failed(Some(Invariant::Durable))),
                row(3, failed(None)),
                row(4, RowState::Pending),
            ],
        );

        let found = state.stats();
        assert_eq!(found.failed, 2);
        assert_eq!(found.durable, 1);
        assert_eq!(found.unattributed, 1);
    }

    #[test]
    fn stats_count_only_the_rows_that_reached_a_verdict() {
        let state = running(
            None,
            vec![
                row(1, RowState::Complete(Verdict::Pass)),
                row(2, RowState::Pending),
                row(3, RowState::Running { worker: 0 }),
                row(4, RowState::CounterExample),
                row(5, RowState::Requeued),
                row(6, RowState::Abandoned),
            ],
        );

        let found = state.stats();
        assert_eq!(found.passed, 1);
        assert_eq!(found.failed + found.inconclusive, 0);
    }

    #[rstest]
    #[case(Doing::Paused, "paused")]
    #[case(Doing::Idle, "idle")]
    #[case(Doing::Failed { schedule: 12 }, "failed")]
    fn a_card_says_what_a_worker_that_is_not_driving_is_doing(
        #[case] doing: Doing,
        #[case] expected: &str,
    ) {
        let mut buf = Buffer::empty(Rect::new(0, 0, 12, 6));
        Card(&Worker { id: 0, doing }).render(buf.area, &mut buf);

        assert!(line(&buf, 1).contains(expected), "{buf:?}");
    }

    #[rstest]
    #[case(RowState::Requeued, "driving killing db again")]
    #[case(RowState::Abandoned, "Stopped by an interrupt")]
    #[case(RowState::Pending, "Not yet run")]
    fn a_row_that_has_not_reached_a_verdict_says_where_it_has_got_to(
        #[case] state: RowState,
        #[case] expected: &str,
    ) {
        let row = Row {
            schedule: 12,
            purpose: Purpose::Break(Box::new(Fault::throughout(By::Kill("db".to_owned())))),
            state,
        };

        assert!(row.detail().contains(expected), "{}", row.detail());
    }

    #[test]
    fn header_carries_the_clock_and_what_the_campaign_loaded() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 70, 3));
        Header(&running(Some(Duration::from_secs(1800)), Vec::new())).render(buf.area, &mut buf);

        assert!(line(&buf, 0).contains("0:05:23 / 0:30:00"), "{buf:?}");
        assert!(line(&buf, 1).contains("plugins: amqp, http"), "{buf:?}");
    }
}
