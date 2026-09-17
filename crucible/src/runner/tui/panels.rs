//! The panels the screen is made of.

use std::{borrow::Cow, collections::BTreeMap, time::Duration};

use crucible_core::{
    ipc::Verdict,
    schedule::{Phase, Progress, Purpose, Step},
};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    text::{Line, Text},
    widgets::{Block, Clear, List, ListState, Paragraph, StatefulWidget, Widget, Wrap},
};

use super::state::{Dispatching, Row, State};
use crate::report::Short;

/// What is being run, how long it has been running, and what it loaded.
pub struct Header<'a, S>(pub &'a State<S>);

impl<S> Widget for Header<'_, S> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title_top(format!(" crucible run {} ", self.0.scenario))
            .title_top(
                Line::from(format!(" {} ", clock(self.0.elapsed, self.0.budget))).right_aligned(),
            );
        let inner = block.inner(area);
        block.render(area, buf);

        Line::from(format!(
            "spec: {}  plugins: {}",
            self.0.spec,
            self.0.plugins.join(", ")
        ))
        .render(inner, buf);
    }
}

/// A pane per worker the campaign can have going at once.
///
/// The panes are always drawn. A campaign running three at a
/// time gets three panes, and an empty one means that worker is waiting for a schedule.
pub struct Workers<'a> {
    pub driving: Vec<&'a Row>,
    pub doing: &'a BTreeMap<u32, (Phase, Option<Step>)>,
    pub panes: usize,
}

impl Widget for Workers<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered().title_top(format!(
            " Workers ({}/{}) ",
            self.driving.len(),
            self.panes
        ));
        let inner = block.inner(area);
        block.render(area, buf);

        let panes = Layout::horizontal((0..self.panes).map(|_| Constraint::Length(17)))
            .spacing(1)
            .split(inner);
        for (pane, area) in panes.iter().enumerate() {
            Card(self.driving.get(pane).copied(), self.doing).render(*area, buf);
        }
    }
}

/// One pane, holding a run or waiting for one.
struct Card<'a>(Option<&'a Row>, &'a BTreeMap<u32, (Phase, Option<Step>)>);

impl Widget for Card<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let Some(row) = self.0 else {
            let block = Block::bordered();
            let inner = block.inner(area);
            block.render(area, buf);
            Line::from("waiting").render(inner, buf);
            return;
        };
        let worker = match row.state {
            Progress::Running { worker } => worker,
            _ => return,
        };
        let block = Block::bordered().title_top(format!(" W{worker} "));
        let inner = block.inner(area);
        block.render(area, buf);

        let at = match self.1.get(&worker) {
            Some((phase, Some(step))) => format!("{} {}/{}", phase.short(), step.taken, step.of),
            Some((phase, None)) => phase.short().into_owned(),
            None => "starting".to_owned(),
        };
        for (text, area) in [Line::from(at), Line::from(format!("#{}", row.schedule))]
            .into_iter()
            .zip(inner.rows())
        {
            text.render(area, buf);
        }
    }
}

/// How much of the campaign has settled, and what it found.
pub struct Stats<'a>(pub &'a State<Dispatching>);

impl Widget for Stats<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let held = if self.0.stage.held { " held " } else { "" };
        let block = Block::bordered()
            .title_top(format!(" Stats {held}"))
            .title_top(
                Line::from(if self.0.stage.over {
                    " done ".to_owned()
                } else {
                    format!(" ETA {} ", hms(self.0.eta()))
                })
                .right_aligned(),
            );
        let inner = block.inner(area);
        block.render(area, buf);

        let found = self.0.stats();
        let settled = found.passed + found.failed + found.inconclusive;
        let mut lines = vec![
            format!("{settled} of {} settled", self.0.stage.schedules.len()).into(),
            format!(
                "pass {}  fail {}  inc {}",
                found.passed, found.failed, found.inconclusive
            )
            .into(),
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

/// One of the two lists of schedules.
pub struct Schedules<'a> {
    pub rows: Vec<&'a Row>,
    pub titled: &'a str,
    /// Whether this is the list the arrows are moving through.
    pub looking: bool,
}

impl StatefulWidget for Schedules<'_> {
    type State = ListState;

    fn render(self, area: Rect, buf: &mut Buffer, cursor: &mut ListState) {
        StatefulWidget::render(
            List::new(self.rows.iter().map(|row| Line::from(row.line())))
                .block(Block::bordered().title_top(format!(
                    " {} ({}) ",
                    self.titled,
                    self.rows.len()
                )))
                .highlight_symbol(if self.looking { "> " } else { "  " }),
            area,
            buf,
            cursor,
        );
    }
}

impl Row {
    /// The row as one line.
    fn line(&self) -> String {
        format!(
            "#{:<5} {:<15} {}",
            self.schedule,
            self.state.short(),
            self.summary()
        )
    }

    fn summary(&self) -> Cow<'_, str> {
        match (&self.state, &self.purpose) {
            (
                Progress::Complete(Verdict::Fail { reason, .. } | Verdict::Inconclusive { reason }),
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
            Progress::Complete(Verdict::Fail { reason, .. } | Verdict::Inconclusive { reason }) => {
                reason.clone()
            }
            Progress::Complete(Verdict::Pass) => {
                format!("The fleet held under {}.", self.summary())
            }
            Progress::CounterExample { .. } => format!(
                "The fleet turned some steps away, so this run cannot be compared step by step. \
                 A clean run of just the steps it accepted is under way, to see where they leave \
                 the fleet. It drives {}.",
                self.summary()
            ),
            Progress::Pending => format!("Not yet run. Will drive {}.", self.summary()),
            Progress::Running { worker } => {
                format!("Running on worker {worker}, driving {}.", self.summary())
            }
            Progress::Requeued => format!("A worker failed; driving {} again.", self.summary()),
            Progress::Abandoned => {
                "Stopped by an interrupt, so it says nothing about the fleet.".to_owned()
            }
            Progress::Skipped => "You gave up on this one, so it never ran.".to_owned(),
            Progress::Errored => {
                "A worker kept failing on this one, so it was left. Nothing it did say can be \
                 trusted to be about the fleet rather than the worker."
                    .to_owned()
            }
        }
    }
}

/// The journal, drawn over the screen.
pub struct Journal<'a>(pub u32, pub &'a [String]);

impl Widget for Journal<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let over = centred(area, 78, 80);
        Clear.render(over, buf);
        let block = Block::bordered()
            .title_top(format!(" #{} · journal ", self.0))
            .title_bottom(Line::from(" ↵ close ").centered());
        let inner = block.inner(over);
        block.render(over, buf);

        // Borrow line by line rather than joining. A journal is long, and this
        // redraws every tick it is open.
        Paragraph::new(Text::from_iter(
            self.1.iter().map(String::as_str).map(Line::from),
        ))
        .wrap(Wrap { trim: true })
        .render(inner, buf);
    }
}

fn centred(area: Rect, width: u16, height: u16) -> Rect {
    let [_, middle, _] = Layout::vertical([
        Constraint::Percentage((100 - height) / 2),
        Constraint::Percentage(height),
        Constraint::Percentage((100 - height) / 2),
    ])
    .areas(area);
    let [_, over, _] = Layout::horizontal([
        Constraint::Percentage((100 - width) / 2),
        Constraint::Percentage(width),
        Constraint::Percentage((100 - width) / 2),
    ])
    .areas(middle);
    over
}

/// The help, drawn over the screen.
pub struct Help;

impl Widget for Help {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let over = centred(area, 70, 80);
        Clear.render(over, buf);
        let block = Block::bordered()
            .title_top(" What the screen says ")
            .title_bottom(Line::from(" ? close ").centered());
        let inner = block.inner(over);
        block.render(over, buf);

        let said = [
            "A worker pane says which schedule it is driving and where that run",
            "has got to: setting up, driving a step, healing, observing, or",
            "cleaning up again.",
            "",
            "A schedule waiting on a counter example has had some steps turned",
            "away, and a clean run of the ones it did accept is under way to say",
            "where those leave the fleet.",
            "",
            "A plugin is listed with what it is loaded to do: deploy brings the",
            "fleet up, drive works it, observe reads it. One plugin can do more",
            "than one.",
            "",
            "  [p] hold, and let it carry on again",
            "  [s] give up on the selected schedule",
            "  [S] take nothing else on and report on what there is",
            "  [↵] what the journal recorded about the selected schedule",
            "  [q] stop the campaign",
        ];
        Paragraph::new(said.join("\n")).render(inner, buf);
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
    use crate::tui::state::{Learning, Row};

    fn running(budget: Option<Duration>, schedules: Vec<Row>) -> State<Dispatching> {
        State {
            scenario: "orders.cru".to_owned(),
            elapsed: Duration::from_secs(323),
            budget,
            spec: "a31c".to_owned(),
            plugins: vec!["amqp".to_owned(), "http".to_owned()],
            stage: Learning,
        }
        .dispatch(schedules, Duration::from_secs(1260), 3)
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

    fn row(schedule: u32, state: Progress) -> Row {
        Row {
            schedule,
            purpose: Purpose::Learn,
            state,
        }
    }

    fn failed(invariant: Option<Invariant>) -> Progress {
        Progress::Complete(Verdict::Fail {
            invariant,
            reason: String::new(),
        })
    }

    #[test]
    fn a_failure_that_named_nothing_is_counted_rather_than_dropped() {
        let state = running(
            None,
            vec![
                row(1, Progress::Complete(Verdict::Pass)),
                row(2, failed(Some(Invariant::Durable))),
                row(3, failed(None)),
                row(4, Progress::Pending),
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
                row(1, Progress::Complete(Verdict::Pass)),
                row(2, Progress::Pending),
                row(3, Progress::Running { worker: 0 }),
                row(
                    4,
                    Progress::CounterExample {
                        wants: vec![vec![1, 2, 4, 5]],
                    },
                ),
                row(5, Progress::Requeued),
                row(6, Progress::Abandoned),
            ],
        );

        let found = state.stats();
        assert_eq!(found.passed, 1);
        assert_eq!(found.failed + found.inconclusive, 0);
    }

    #[rstest]
    #[case(Progress::Requeued, "driving killing db again")]
    #[case(Progress::Abandoned, "Stopped by an interrupt")]
    #[case(Progress::Pending, "Not yet run")]
    #[case(Progress::Skipped, "gave up on this one")]
    #[case(Progress::Errored, "kept failing on this one")]
    fn a_row_that_has_not_reached_a_verdict_says_where_it_has_got_to(
        #[case] state: Progress,
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
