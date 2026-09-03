//! Reading a run: what its observations say the fleet did, and which invariant
//! that broke.
//!
//! One reading serves all four. Every invariant asks the same of the settled
//! state, that the fleet holds what it took responsibility for and no more;
//! what separates them is what the reading turns out to say.

use std::cmp::Ordering;

use crucible_protocol::{At, FaultReport, FaultResult};

use crate::schema::Moves;

use super::{Ack, Checkpoint, Invariant, Observations, StepWindow, Trajectory};
use crate::fault::Primitive;
use crate::ipc::Verdict;
use crate::plan::Value;

impl Observations {
    /// What a run's observations say of the fleet.
    ///
    /// The fleet holds what it took responsibility for and no more. Where it
    /// settled is the only state this turns on: diverging part way through and
    /// coming back is a fleet that recovered, not a fleet that lost something.
    #[must_use]
    pub fn verdict(&self) -> Verdict {
        // No fault fired => nothing to test.
        let fault = match &self.fault {
            None => {
                return Verdict::Inconclusive {
                    reason: "no fault was scheduled".into(),
                };
            }
            Some(FaultReport {
                result: FaultResult::Missed(miss),
                ..
            }) => {
                return Verdict::Inconclusive {
                    reason: format!("fault did not fire: {miss:?}"),
                };
            }
            Some(FaultReport {
                service,
                result: FaultResult::Fired { by, at, .. },
                ..
            }) => Placed {
                service,
                by: *by,
                at,
            },
        };

        if self.outcomes.is_empty() {
            return Verdict::Inconclusive {
                reason: "the scenario drove nothing, so nothing was put at risk".into(),
            };
        }
        if self.checks.is_empty() {
            return Verdict::Inconclusive {
                reason: "the scenario states nothing to check after heal".into(),
            };
        }

        // A checkpoint says where the fleet stands once the first N steps have
        // landed. If every step landed we hold the run to the last one, in
        // whatever order they got there. A step whose ack was lost leaves more
        // than one count admissible, and the fleet answers to any of them.
        let landed = self.steps_landed();
        if landed.is_empty() {
            return Verdict::Inconclusive {
                reason: self.refused_then_landed(),
            };
        }
        let admissible: Vec<(usize, &Checkpoint)> = landed
            .iter()
            .filter_map(|n| self.fault_free.at(*n).map(|at| (*n, at)))
            .collect();
        let Some(&(landed, expected)) = admissible.last() else {
            return Verdict::Inconclusive {
                reason: format!(
                    "the fault-free run left {} checkpoint(s), so it cannot say where {} step(s) leave the fleet",
                    self.fault_free.len(),
                    counts(&landed),
                ),
            };
        };

        let settled: Checkpoint = self
            .checks
            .iter()
            .map(|observed| observed.value.clone())
            .collect();
        // Any outcome the run admits is enough: what is in doubt is what the
        // fleet accepted, and it is not held to the strictest reading of that.
        if admissible
            .iter()
            .any(|(_, expected)| matches!(differing(&settled, expected), Ok(None)))
        {
            return Verdict::Pass;
        }
        // Judged against the most it can have accepted, which is the most it can
        // owe.
        match differing(&settled, expected) {
            Ok(None) => Verdict::Pass,
            Ok(Some(at)) => {
                let moves: Vec<Moves> = self
                    .checks
                    .iter()
                    .map(|observed| observed.check.moves)
                    .collect();
                let went = Went::of(&settled, &admissible, &self.fault_free, &moves);
                Verdict::Fail {
                    invariant: fault.broke(went),
                    reason: self.failure(fault, &settled, &admissible, landed, at, went),
                }
            }
            Err(at) => Verdict::Inconclusive {
                reason: format!(
                    "`{}` could not be read in both runs",
                    self.observable_at(at)
                ),
            },
        }
    }

    /// How many steps the fleet may have taken responsibility for, fewest first.
    ///
    /// A step whose ack was lost may have landed or not, so a run admits every count
    /// its unknowns allow. A count is admissible when nothing before it was refused
    /// and nothing after it was acknowledged, since a checkpoint describes a run that
    /// landed a prefix of the steps and nothing else.
    ///
    /// Empty when a refusal precedes an acknowledgement, which no checkpoint
    /// describes however the unknowns fall.
    fn steps_landed(&self) -> Vec<usize> {
        let acks: Vec<Ack> = self.outcomes.iter().map(|outcome| outcome.ack).collect();
        (0..=acks.len())
            .filter(|n| {
                acks[..*n].iter().all(|ack| *ack != Ack::Rejected)
                    && acks[*n..].iter().all(|ack| *ack != Ack::Acked)
            })
            .collect()
    }

    /// Why no count describes this run: a step was refused and a later one landed,
    /// so what the fleet accepted is not a prefix of what the scenario drove.
    fn refused_then_landed(&self) -> String {
        let refused = self
            .outcomes
            .iter()
            .position(|outcome| outcome.ack == Ack::Rejected)
            .map_or(0, |at| at + 1);
        format!("step {refused} was refused and a later one landed, which no checkpoint describes")
    }

    /// What the fault was, where it landed, what should have been true, and the step
    /// the fleet started getting it wrong at.
    fn failure(
        &self,
        fault: Placed<'_>,
        settled: &Checkpoint,
        admissible: &[(usize, &Checkpoint)],
        landed: usize,
        at: usize,
        went: Went,
    ) -> String {
        let observable = self.observable_at(at);
        let expected = admissible.last().map(|(_, expected)| *expected);
        let reason = match (
            &settled[at],
            expected.and_then(|expected| expected[at].as_ref()),
        ) {
            (Some(settled), Some(expected)) => format!(
                "The fleet took {} which left `{observable}` at `{settled}`, expected value \
                 `{expected}`{}",
                steps(landed),
                fault.told(went, settled, admissible, at)
            ),
            _ => format!("`{observable}` disagrees with the fault-free run"),
        };
        // Only worth saying when the state parted on the way to the step being
        // judged. Parting after that is downstream of the verdict, not evidence for
        // it, and reads as a contradiction next to the count of steps that landed.
        let diverged = match self
            .diverged_at()
            .filter(|step| (1..=landed).contains(step))
        {
            Some(step) => format!(". It first differed after step {step}"),
            None => String::new(),
        };
        format!(
            "`{}` {} {}. {reason}{diverged}",
            fault.service,
            fault.done(),
            fault.when(&self.windows)
        )
    }

    /// The step this run's state first parted from the fault-free run's.
    fn diverged_at(&self) -> Option<usize> {
        self.trajectory
            .iter()
            .zip(&self.fault_free)
            .position(|(reached, expected)| reached != expected)
    }

    fn observable_at(&self, i: usize) -> String {
        self.checks.get(i).map_or_else(
            || format!("observable {i}"),
            |observed| observed.check.observable(),
        )
    }
}

/// What the fleet did that the fault-free run did not.
///
/// The fleet is judged on where it settled, so this is a reading of that
/// against every state the run's own acknowledgements admit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Went {
    /// It settled short of the fewest steps the run admits, so a step it took
    /// responsibility for left nothing behind.
    Lost,
    /// It settled where taking one of its steps twice would have left it.
    Twice,
    /// It settled where one of its steps arriving last would have left it, so
    /// the order decided the outcome.
    OutOfOrder,
    /// None of those, so nothing about losing, repeating or resequencing work
    /// describes where it ended up.
    Elsewhere,
    /// The readings cannot say: working out what a step taken twice would leave
    /// needs a count, and these checks are not counts.
    Unreadable,
}

impl Went {
    /// What `settled` says the fleet did, given the states the run admits and
    /// where the fault-free run stood after each step.
    fn of(
        settled: &Checkpoint,
        admissible: &[(usize, &Checkpoint)],
        fault_free: &Trajectory,
        moves: &[Moves],
    ) -> Self {
        let fewest = admissible.first().map_or(0, |(n, _)| *n);
        if fault_free
            .iter()
            .take(fewest)
            .any(|earlier| earlier == settled)
        {
            return Went::Lost;
        }
        let mut readable = true;
        for (n, _) in admissible {
            for again in 1..=*n {
                match fault_free.twice(moves, *n, again) {
                    Some(doubled) if doubled == *settled => return Went::Twice,
                    Some(_) => {}
                    None => readable = false,
                }
            }
            // Taking the last step last is the order the scenario drove, so it
            // is not a reordering of it.
            for last in 1..*n {
                match fault_free.reordered(moves, *n, last) {
                    Some(out_of_order) if out_of_order == *settled => return Went::OutOfOrder,
                    Some(_) => {}
                    None => readable = false,
                }
            }
        }
        if readable {
            Went::Elsewhere
        } else {
            Went::Unreadable
        }
    }

    /// Which invariant this breaks, or `None` where nothing about the settled
    /// state names one.
    fn shows(self) -> Option<Invariant> {
        match self {
            Went::Lost => Some(Invariant::Durable),
            Went::Twice => Some(Invariant::Idempotent),
            Went::OutOfOrder => Some(Invariant::Converges),
            Went::Elsewhere | Went::Unreadable => None,
        }
    }
}

impl Trajectory {
    /// Where `n` steps would have left the fleet had step `again` been taken a
    /// second time.
    ///
    /// `None` where the readings cannot say: a step's second application is
    /// arithmetic on a count, and a reading that is not a count can only say
    /// whether the step moved it.
    fn twice(&self, moves: &[Moves], n: usize, again: usize) -> Option<Checkpoint> {
        self.projected(moves, n, again, repeated)
    }

    /// Where `n` steps would have left the fleet had step `last` arrived after
    /// the rest rather than in the order the scenario drove.
    ///
    /// `None` where a check this rests on went unread in either run.
    fn reordered(&self, moves: &[Moves], n: usize, last: usize) -> Option<Checkpoint> {
        self.projected(moves, n, last, |at, was, now, moves| {
            Some(taken_last(at, was, now, moves))
        })
    }

    /// Where `n` steps would have left the fleet had `step` fallen differently
    /// among them, with `rule` saying what that does to one reading.
    fn projected(
        &self,
        moves: &[Moves],
        n: usize,
        step: usize,
        rule: impl Fn(&Value, &Value, &Value, Moves) -> Option<Value>,
    ) -> Option<Checkpoint> {
        let after = self.at(n)?;
        let was = self.at(step.checked_sub(1)?)?;
        let now = self.at(step)?;
        if after.len() != was.len() || was.len() != now.len() || after.len() != moves.len() {
            return None;
        }
        let mut projected = Vec::with_capacity(after.len());
        for (((at, was), now), moves) in after.iter().zip(was).zip(now).zip(moves) {
            projected.push(Some(rule(
                at.as_ref()?,
                was.as_ref()?,
                now.as_ref()?,
                *moves,
            )?));
        }
        Some(projected)
    }
}

/// `at`, with the step that took the fleet from `was` to `now` taken a second
/// time. A step that left a reading alone leaves it alone again; one that moved
/// a count moves it as far again.
fn repeated(at: &Value, was: &Value, now: &Value, moves: Moves) -> Option<Value> {
    if was == now {
        return Some(at.clone());
    }
    match moves {
        Moves::Counts => match (at, was, now) {
            (Value::Int(at), Value::Int(was), Value::Int(now)) => {
                Some(Value::Int(at.checked_add(now.checked_sub(*was)?)?))
            }
            // A count the plugin does not answer with a number.
            _ => None,
        },
        Moves::Sets => Some(at.clone()),
    }
}

/// `at`, with the step that took the fleet from `was` to `now` arriving after
/// the rest.
///
/// A step that left a reading alone leaves it alone wherever it falls, and a
/// count reaches the same total whichever order its steps arrive in. A reading
/// its step sets ends where whichever step arrived last set it.
fn taken_last(at: &Value, was: &Value, now: &Value, moves: Moves) -> Value {
    if was == now {
        return at.clone();
    }
    match moves {
        Moves::Counts => at.clone(),
        Moves::Sets => now.clone(),
    }
}

/// The first observable the two readings disagree on, `None` if they agree, and
/// the index of one that could not be read on either side.
fn differing(reached: &Checkpoint, expected: &Checkpoint) -> Result<Option<usize>, usize> {
    for (i, (reached, expected)) in reached.iter().zip(expected).enumerate() {
        match (reached, expected) {
            (Some(reached), Some(expected)) if reached == expected => {}
            (Some(_), Some(_)) => return Ok(Some(i)),
            _ => return Err(i),
        }
    }
    Ok(None)
}

/// The fault this run was judging, once it is known to have fired.
#[derive(Clone, Copy)]
struct Placed<'a> {
    service: &'a str,
    by: Primitive,
    at: &'a At,
}

impl Placed<'_> {
    /// What was done, as a verdict says it happened.
    fn done(self) -> &'static str {
        match self.by {
            Primitive::Kill => "was killed",
            Primitive::Cut => "was cut off",
            Primitive::Redeliver => "was redelivered to",
            Primitive::Reorder => "was reordered around",
            Primitive::Drop => "had a message dropped on it",
        }
    }

    /// The invariants breaking the fleet this way could show, of which the run
    /// shows at most one.
    fn could_show(self) -> &'static [Invariant] {
        Invariant::could_show(matches!(self.at, At::Throughout), self.by)
    }

    /// When it was done, against the steps the scenario drove, and what it
    /// caught there.
    fn when(self, windows: &[StepWindow]) -> String {
        match self.at {
            At::Moment { offset_ns, why, .. } => {
                format!("{}, on {why}", placement(windows, *offset_ns))
            }
            At::Throughout => "for the whole run".into(),
        }
    }

    /// Which invariant the run showed broken, or `None` where the settled state
    /// names none of the ones this fault could have shown.
    ///
    /// A way of breaking the fleet that can show one thing shows that. Anything
    /// broader is decided by where the fleet settled, since that is the outcome and
    /// an outcome is what an invariant is about.
    fn broke(self, went: Went) -> Option<Invariant> {
        match self.could_show() {
            [only] => Some(*only),
            could => went.shows().filter(|shown| could.contains(shown)),
        }
    }

    /// What the run says broke, and the evidence for saying it.
    ///
    /// A way of breaking the fleet that can show only one thing says so and needs
    /// no evidence. Anything broader is read off where the fleet settled, and where
    /// that names nothing the verdict says so rather than picking the nearest
    /// label.
    fn told(
        self,
        went: Went,
        settled: &Value,
        admissible: &[(usize, &Checkpoint)],
        at: usize,
    ) -> String {
        if let [only] = self.could_show() {
            return format!(
                ". Breaking the fleet this way can show nothing but {only}, so that is what broke"
            );
        }
        let could = spelled(self.could_show());
        match went {
            Went::Lost => ". It settled where fewer steps would have left it, so work was lost, which \
                           is durability"
                .to_owned(),
            Went::Twice => {
                ". It settled where the steps it took would have left it had one of them been taken \
                 twice, so work was done twice, which is idempotency"
                    .to_owned()
            }
            Went::OutOfOrder => {
                ". It settled where the steps it took would have left it had one of them arrived after \
                 the rest, so the order it was told things in decided the outcome, which is \
                 convergence"
                    .to_owned()
            }
            Went::Elsewhere => format!(
                "{}. It settled where losing a step, taking one twice and taking one out of order \
                 would all have left it somewhere else, so which of {could} broke cannot be read from \
                 where it settled",
                parted(settled, admissible, at)
            ),
            Went::Unreadable => format!(
                "{}. What taking a step twice would have left cannot be worked out from these checks, \
                 so which of {could} broke cannot be read from where it settled",
                parted(settled, admissible, at)
            ),
        }
    }
}

/// A list of invariants, as a verdict says them.
fn spelled(invariants: &[Invariant]) -> String {
    let spelled: Vec<String> = invariants.iter().map(ToString::to_string).collect();
    match spelled.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} or {last}", rest.join(", ")),
        None => "nothing".to_owned(),
    }
}

/// Which way the fleet's reading of `at` parted from every reading the run
/// admits, for a run where nothing else names what broke.
///
/// Empty where the reading has no order, and where the fleet landed between two
/// readings the run admits.
fn parted(settled: &Value, admissible: &[(usize, &Checkpoint)], at: usize) -> &'static str {
    // More or less than it owed is a quantity. A reading that only sorts, such
    // as a status, has an order without having an amount, and saying the fleet
    // held more of it would be nonsense.
    if !matches!(settled, Value::Int(_) | Value::Duration(_)) {
        return "";
    }
    let owed: Vec<&Value> = admissible
        .iter()
        .filter_map(|(_, expected)| expected.get(at).and_then(Option::as_ref))
        .collect();
    let every = |way: Ordering| {
        !owed.is_empty()
            && owed
                .iter()
                .all(|owed| super::order(settled, owed) == Some(way))
    };
    if every(Ordering::Greater) {
        ". It held more than it owed on any reading"
    } else if every(Ordering::Less) {
        ". It held less than it owed on any reading"
    } else {
        ""
    }
}

/// Where a moment in the run sits against the scenario's steps.
fn placement(windows: &[StepWindow], at_ns: u128) -> String {
    let Some(first) = windows.first() else {
        return "at an unknown point".into();
    };
    if at_ns < first.start_ns {
        return "before step 1".into();
    }
    for (i, window) in windows.iter().enumerate() {
        if at_ns <= window.end_ns {
            // Past the previous step's end but short of this one's start: the
            // fleet was between the two, with nothing of the scenario in flight.
            if at_ns < window.start_ns {
                return format!("between steps {i} and {}", i + 1);
            }
            return format!("during step {}", i + 1);
        }
    }
    format!("after step {}", windows.len())
}

/// `n` steps, spelled so a verdict does not have to say "step(s)".
fn steps(n: usize) -> String {
    if n == 1 {
        "1 step".into()
    } else {
        format!("{n} steps")
    }
}

/// The counts a run admits, as a verdict says them.
fn counts(landed: &[usize]) -> String {
    let spelled: Vec<String> = landed.iter().map(ToString::to_string).collect();
    spelled.join(" or ")
}

#[cfg(test)]
mod tests {
    use crucible_protocol::{At, FaultReport, FaultResult};

    use crate::schema::Moves;

    use super::*;
    use crate::{
        plan,
        verdict::{Ack, Observed, Outcome},
    };

    fn fired_fault() -> FaultReport {
        fired_fault_at(0)
    }

    /// A kill of `db` placed `at_ns` nanoseconds into the scenario.
    fn fired_fault_at(at_ns: u128) -> FaultReport {
        placed(Primitive::Kill, at_ns)
    }

    /// A fault that broke the fleet by `how`, placed `at_ns` nanoseconds into
    /// the scenario.
    fn placed(how: Primitive, at_ns: u128) -> FaultReport {
        FaultReport::fired(
            0,
            "db",
            how,
            At::Moment {
                direction: crucible_protocol::Direction::ClientToUpstream,
                mark: "publish:1:after".to_owned(),
                why: "a publish the broker has not confirmed".to_owned(),
                offset_ns: at_ns,
            },
            0,
        )
    }

    /// A kill of `db` that stood for the whole run.
    fn fired_throughout() -> FaultReport {
        FaultReport::fired(0, "db", Primitive::Kill, At::Throughout, 0)
    }

    fn outcome(ack: Ack) -> Outcome {
        Outcome { ack }
    }

    /// A reading of `writes.count`.
    fn reading(read: i64) -> Observed {
        Observed {
            check: plan::Check {
                service: "db".into(),
                observer: "mariadb".into(),
                observable: vec!["writes".into(), "count".into()],
                args: Vec::new(),
                filter: None,
                moves: crate::schema::Moves::Counts,
                clauses: std::collections::BTreeMap::new(),
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Int(read),
            },
            value: Some(plan::Value::Int(read)),
        }
    }

    /// One point of a run, where the scenario states a single check.
    fn checkpoint(value: i64) -> Checkpoint {
        vec![Some(plan::Value::Int(value))]
    }

    /// A run whose fault fired, judged against a fault-free run that stood at
    /// `fault_free` after each step, and that settled at `settled` once the
    /// target was back.
    fn judged(acks: &[Ack], fault_free: &[i64], settled: i64) -> Verdict {
        broken_by(fired_fault(), acks, fault_free, settled)
    }

    /// A run whose fleet was broken by `fault`, judged the same way.
    fn broken_by(fault: FaultReport, acks: &[Ack], fault_free: &[i64], settled: i64) -> Verdict {
        let mut obs = Observations::empty();
        obs.fault = Some(fault);
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![reading(settled)];
        obs.fault_free = fault_free.iter().copied().map(checkpoint).collect();
        obs.verdict()
    }

    /// A reading of `zone.address`, which a step sets outright rather than
    /// moving.
    fn address(read: &str) -> Observed {
        Observed {
            check: plan::Check {
                service: "pdns".into(),
                observer: "http".into(),
                observable: vec!["zone".into(), "address".into()],
                args: Vec::new(),
                filter: None,
                moves: crate::schema::Moves::Sets,
                clauses: std::collections::BTreeMap::new(),
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Str(read.to_owned()),
            },
            value: Some(plan::Value::Str(read.to_owned())),
        }
    }

    /// A run whose scenario states both a count every step moves and a reading
    /// every step sets. Together they say whether every step landed and which
    /// of them landed last, which is what an order can be read from.
    fn ordered(
        fault: FaultReport,
        acks: &[Ack],
        fault_free: &[(i64, &str)],
        settled: (i64, &str),
    ) -> Verdict {
        let mut obs = Observations::empty();
        obs.fault = Some(fault);
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![reading(settled.0), address(settled.1)];
        obs.fault_free = fault_free
            .iter()
            .map(|(count, address)| {
                vec![
                    Some(plan::Value::Int(*count)),
                    Some(plan::Value::Str((*address).to_owned())),
                ]
            })
            .collect();
        obs.verdict()
    }

    /// What a failing run says broke, and why.
    fn showed(verdict: Verdict) -> (Option<Invariant>, String) {
        match verdict {
            Verdict::Fail { invariant, reason } => (invariant, reason),
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    /// A fault can leave the fleet with no row to answer from. That is a
    /// reading of the fleet, and the run says what it could not read rather
    /// than holding the fleet to a value it never had.
    #[test]
    fn a_check_the_fleet_cannot_answer_is_not_a_failure() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_throughout());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![Observed {
            value: None,
            ..reading(3)
        }];
        obs.fault_free = vec![checkpoint(0), checkpoint(3)].into_iter().collect();
        assert!(
            matches!(obs.verdict(), Verdict::Inconclusive { .. }),
            "{:?}",
            obs.verdict()
        );
    }

    #[test]
    fn a_run_that_observed_nothing_is_inconclusive() {
        assert!(matches!(
            Observations::empty().verdict(),
            Verdict::Inconclusive { .. }
        ));
    }

    #[test]
    fn a_missed_fault_is_inconclusive() {
        let mut obs = Observations::empty();
        obs.fault = Some(FaultReport {
            schedule_id: 0,
            service: "db".into(),
            result: FaultResult::Missed(
                crucible_protocol::FaultMissReason::ScenarioEndedBeforeAnchor,
            ),
        });
        obs.checks = vec![reading(0)];
        assert!(matches!(obs.verdict(), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_with_nothing_to_check_is_inconclusive() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes.push(outcome(Ack::Acked));
        assert!(matches!(obs.verdict(), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_that_drove_nothing_is_not_a_test() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.checks = vec![reading(0)];
        assert!(matches!(obs.verdict(), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn settling_where_the_fault_free_run_ended_is_pass() {
        assert_eq!(
            judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 2),
            Verdict::Pass,
        );
    }

    #[test]
    fn settling_anywhere_else_is_fail() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 1),
            Verdict::Fail { .. },
        ));
    }

    /// The steps the fleet refused are steps it never owed anything for, so the
    /// run answers to the checkpoint it got as far as.
    #[test]
    fn a_run_that_took_fewer_steps_answers_to_an_earlier_checkpoint() {
        assert_eq!(
            judged(&[Ack::Acked, Ack::Rejected], &[0, 1, 2], 1),
            Verdict::Pass,
        );
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Rejected], &[0, 1, 2], 2),
            Verdict::Fail { .. },
        ));
    }

    #[test]
    fn a_step_refused_while_a_later_one_landed_is_inconclusive() {
        assert!(matches!(
            judged(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1),
            Verdict::Inconclusive { .. },
        ));
    }

    /// A lost ack leaves the fleet owing either what it would owe having taken
    /// the step or what it would owe having refused it, so either is a pass.
    #[test]
    fn a_run_whose_ack_was_lost_answers_to_either_checkpoint() {
        assert_eq!(
            judged(&[Ack::Acked, Ack::Unknown], &[0, 1, 2], 1),
            Verdict::Pass,
        );
        assert_eq!(
            judged(&[Ack::Acked, Ack::Unknown], &[0, 1, 2], 2),
            Verdict::Pass,
        );
    }

    #[test]
    fn a_run_whose_ack_was_lost_still_fails_where_neither_describes_it() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Unknown], &[0, 1, 2], 7),
            Verdict::Fail { .. },
        ));
    }

    /// A step that landed after the one in doubt settles it: the doubt cannot be
    /// resolved as "not landed" without leaving a gap no checkpoint describes.
    #[test]
    fn a_later_landed_step_settles_what_a_lost_ack_left_open() {
        assert_eq!(
            judged(&[Ack::Unknown, Ack::Acked], &[0, 1, 2], 2),
            Verdict::Pass,
        );
        assert!(matches!(
            judged(&[Ack::Unknown, Ack::Acked], &[0, 1, 2], 1),
            Verdict::Fail { .. },
        ));
    }

    /// Every step in doubt, so the fleet may have taken all of them or none.
    #[test]
    fn a_run_of_lost_acks_admits_every_checkpoint() {
        for settled in [0, 1, 2] {
            assert_eq!(
                judged(&[Ack::Unknown, Ack::Unknown], &[0, 1, 2], settled),
                Verdict::Pass,
                "settling at {settled}",
            );
        }
    }

    /// A refusal bounds the doubt: nothing after it can have landed, so a lost
    /// ack that follows one cannot be read as landed.
    #[test]
    fn a_refusal_bounds_what_a_later_lost_ack_admits() {
        assert_eq!(
            judged(&[Ack::Acked, Ack::Rejected, Ack::Unknown], &[0, 1, 2, 3], 1),
            Verdict::Pass,
        );
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Rejected, Ack::Unknown], &[0, 1, 2, 3], 2),
            Verdict::Fail { .. },
        ));
    }

    #[test]
    fn a_fault_free_run_too_short_to_say_is_inconclusive() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Acked], &[0, 1], 2),
            Verdict::Inconclusive { .. },
        ));
    }

    #[test]
    fn a_lost_ack_whose_outcomes_have_no_checkpoint_is_inconclusive() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Unknown], &[0], 1),
            Verdict::Inconclusive { .. },
        ));
    }

    /// The fault-free run could not read the state it is meant to be the
    /// authority on, so there is nothing to hold this run to.
    #[test]
    fn an_unread_observable_is_inconclusive() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = vec![checkpoint(0), vec![None]].into_iter().collect();
        assert!(matches!(obs.verdict(), Verdict::Inconclusive { .. }));
    }

    /// Falling behind under the fault and catching up afterwards is a fleet
    /// that recovered, so only where it settled counts.
    #[test]
    fn diverging_and_coming_back_is_pass() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(2)];
        obs.trajectory = vec![checkpoint(0), checkpoint(0), checkpoint(2)]
            .into_iter()
            .collect();
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)]
            .into_iter()
            .collect();
        assert_eq!(obs.verdict(), Verdict::Pass);
    }

    #[test]
    fn a_failure_points_at_the_step_the_run_first_differed_after() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(1)]
            .into_iter()
            .collect();
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)]
            .into_iter()
            .collect();
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("settling short of the fault-free run is a failure");
        };
        assert!(reason.contains("after step 2"), "reason: {reason}");
    }

    /// Parting after the step being judged says nothing about why the run
    /// failed, and reads as a contradiction beside the count of steps it took.
    #[test]
    fn a_failure_keeps_quiet_about_a_divergence_past_the_step_it_judged() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Rejected)];
        obs.checks = vec![reading(2)];
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(5)]
            .into_iter()
            .collect();
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)]
            .into_iter()
            .collect();
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("holding 2 where 1 step landed is a failure");
        };
        assert!(!reason.contains("first differed"), "reason: {reason}");
    }

    /// A fleet that settled where taking one of its steps twice would have left
    /// it did the work twice, which is idempotency. Nothing about killing a
    /// service says that: the run does.
    #[test]
    fn a_fleet_that_did_a_step_twice_broke_idempotency() {
        let (broke, reason) = showed(judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 3));
        assert_eq!(broke, Some(Invariant::Idempotent));
        assert!(reason.contains("work was done twice"), "{reason}");
    }

    /// A fault that fired and left the fleet where it should be is a pass.
    #[test]
    fn a_fleet_that_dropped_a_step_broke_durability() {
        let (broke, reason) = showed(judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 1));
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(reason.contains("work was lost"), "{reason}");
    }

    /// A lost ack leaves more than one reading admissible. Settling between two
    /// of them is neither losing a step nor taking one twice, and calling it
    /// either would be a claim the readings do not support.
    #[test]
    fn settling_where_nothing_describes_it_names_no_invariant() {
        let (broke, reason) = showed(judged(&[Ack::Unknown, Ack::Unknown], &[0, 5, 10], 7));
        assert_eq!(broke, None);
        assert!(
            reason.contains("cannot be read from where it settled"),
            "{reason}"
        );
    }

    /// An unattributed failure still says which invariants were in play, so a
    /// reader knows what was ruled out and what was not.
    #[test]
    fn an_unattributed_failure_says_what_it_could_have_been() {
        let (_, reason) = showed(judged(&[Ack::Unknown, Ack::Unknown], &[0, 5, 10], 7));
        assert!(
            reason.contains("which of durability, idempotency or convergence broke"),
            "{reason}"
        );
    }

    /// Making a message arrive twice asks one question, so what broke is that
    /// question, and the settled state has nothing to add.
    #[test]
    fn a_redelivery_can_show_nothing_but_idempotency() {
        let (broke, reason) = showed(broken_by(
            placed(Primitive::Redeliver, 0),
            &[Ack::Acked, Ack::Acked],
            &[0, 1, 2],
            7,
        ));
        assert_eq!(broke, Some(Invariant::Idempotent));
        assert!(
            reason.contains("can show nothing but idempotency"),
            "{reason}"
        );
    }

    /// Telling the fleet things out of order asks only whether the order
    /// mattered, whatever the settled state looks like.
    #[test]
    fn a_reorder_can_show_nothing_but_convergence() {
        let (broke, _) = showed(broken_by(
            placed(Primitive::Reorder, 0),
            &[Ack::Acked, Ack::Acked],
            &[0, 1, 2],
            1,
        ));
        assert_eq!(broke, Some(Invariant::Converges));
    }

    /// A kill can leave the fleet holding every step it was given in an order
    /// it was not given them in. The count says all three landed, so nothing
    /// was lost, and the reading each step sets says the second landed last.
    #[test]
    fn a_fleet_holding_every_step_in_the_wrong_order_shows_convergence() {
        let (broke, why) = showed(ordered(
            placed(Primitive::Kill, 0),
            &[Ack::Acked, Ack::Acked, Ack::Acked],
            &[(0, "none"), (1, "one"), (2, "two"), (3, "three")],
            (3, "two"),
        ));
        assert_eq!(broke, Some(Invariant::Converges));
        assert!(why.contains("arrived after the rest"), "{why}");
    }

    /// A count reaches the same total whichever order its steps arrive in, so a
    /// scenario stating nothing else has no reading an order could move, and
    /// the run says it cannot tell rather than naming convergence.
    #[test]
    fn counts_alone_cannot_show_an_order() {
        let (broke, why) = showed(judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 7));
        assert_eq!(broke, None);
        assert!(
            why.contains("cannot be read from where it settled"),
            "{why}"
        );
    }

    /// Where every step sets the same reading and none of them moves a count,
    /// losing the last step and holding it back leave the fleet in the same
    /// place. The run reads that as work lost, since it cannot see the
    /// difference.
    #[test]
    fn without_a_count_a_held_back_step_reads_as_a_lost_one() {
        let (broke, _) = showed(ordered(
            placed(Primitive::Kill, 0),
            &[Ack::Acked, Ack::Acked, Ack::Acked],
            &[(1, "none"), (1, "one"), (1, "two"), (1, "three")],
            (1, "two"),
        ));
        assert_eq!(broke, Some(Invariant::Durable));
    }

    /// A reading of `zone.sequence`: a number the fleet holds, which each step
    /// sets rather than adds to. What kind of Rust value it arrives as says
    /// nothing about that, so the observer declares it.
    fn sequence(read: i64) -> Observed {
        Observed {
            check: plan::Check {
                service: "pdns".into(),
                observer: "http".into(),
                observable: vec!["zone".into(), "sequence".into()],
                args: Vec::new(),
                filter: None,
                clauses: std::collections::BTreeMap::new(),
                moves: Moves::Sets,
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Int(read),
            },
            value: Some(plan::Value::Int(read)),
        }
    }

    /// A number a step sets is not a count, and reading it as one would take a
    /// sequence the fleet holds and do arithmetic on it. The observer says
    /// which it is, so the same digits are read either way.
    #[test]
    fn a_number_a_step_sets_is_not_read_as_a_count() {
        let mut obs = Observations::empty();
        obs.fault = Some(placed(Primitive::Kill, 0));
        obs.outcomes = vec![
            outcome(Ack::Acked),
            outcome(Ack::Acked),
            outcome(Ack::Acked),
        ];
        obs.checks = vec![reading(3), sequence(2)];
        obs.fault_free = (0..=3)
            .map(|n| vec![Some(plan::Value::Int(n)), Some(plan::Value::Int(n))])
            .collect();

        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Converges));
        assert!(why.contains("arrived after the rest"), "{why}");
    }

    /// A run held degraded from start to finish shows recovery. That is the
    /// shape of the run, so it is known before the run rather than read off it.
    #[test]
    fn a_run_degraded_throughout_shows_recovery() {
        let (broke, _) = showed(broken_by(
            fired_throughout(),
            &[Ack::Acked, Ack::Acked],
            &[0, 1, 2],
            1,
        ));
        assert_eq!(broke, Some(Invariant::Recovers));
    }

    /// Working out what a step taken twice would have left needs a step whose
    /// effect is arithmetic. A reading that is not a count cannot say, and the
    /// verdict says that rather than guessing.
    #[test]
    fn a_reading_that_is_not_a_count_cannot_say_what_twice_would_leave() {
        let named = |at: &str| vec![Some(plan::Value::Str(at.to_owned()))];
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![Observed {
            value: Some(plan::Value::Str("shipped".into())),
            ..reading(0)
        }];
        obs.fault_free = vec![named("new"), named("paid")].into_iter().collect();
        let (broke, reason) = showed(obs.verdict());
        assert_eq!(broke, None);
        assert!(
            reason.contains("cannot be worked out from these checks"),
            "{reason}"
        );
    }

    #[test]
    fn a_verdict_on_a_degraded_run_says_it_stood_throughout() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_throughout());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)]
            .into_iter()
            .collect();
        obs.windows = vec![window(0, 100), window(120, 200)];
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("settling short of the fault-free run is a failure");
        };
        assert!(
            reason.starts_with("`db` was killed for the whole run."),
            "{reason}"
        );
    }

    #[test]
    fn a_verdict_names_the_check_as_the_scenario_spells_it() {
        let mut filtered = reading(100);
        filtered.check.observable = vec!["stock".into(), "select".into()];
        filtered.check.args = vec![plan::Value::Ident("level".into())];
        filtered.check.filter = Some(("item".into(), plan::Value::Str("book".into())));
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![filtered];
        obs.fault_free = vec![checkpoint(100), checkpoint(96)].into_iter().collect();
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("settling somewhere the fault-free run never did is a failure");
        };
        assert!(
            reason.contains(r#"stock.select level where item = "book""#),
            "reason: {reason}"
        );
    }

    /// A verdict on its own says what moved, not what moved it, so it leads with
    /// the fault and where in the scenario it landed.
    #[test]
    fn a_verdict_names_the_fault_that_caused_it() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault_at(150));
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)]
            .into_iter()
            .collect();
        obs.windows = vec![window(0, 100), window(120, 200)];
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("settling short of the fault-free run is a failure");
        };
        assert!(
            reason.starts_with(
                "`db` was killed during step 2, on a publish the broker has not confirmed."
            ),
            "{reason}"
        );
    }

    fn window(start_ns: u128, end_ns: u128) -> StepWindow {
        StepWindow { start_ns, end_ns }
    }

    #[test]
    fn a_fault_is_placed_against_the_step_that_was_in_flight() {
        let windows = [window(10, 100), window(120, 200)];
        for (at_ns, placed) in [
            (5, "before step 1"),
            (10, "during step 1"),
            (100, "during step 1"),
            (110, "between steps 1 and 2"),
            (150, "during step 2"),
            (900, "after step 2"),
        ] {
            assert_eq!(placement(&windows, at_ns), placed, "at {at_ns}ns");
        }
    }

    #[test]
    fn a_fault_with_no_steps_to_place_it_against_says_so() {
        assert!(placement(&[], 150).contains("unknown"));
    }
}
