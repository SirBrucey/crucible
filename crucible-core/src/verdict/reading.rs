//! What a run's observations say the fleet did, and which invariant that
//! broke.
//!
//! Every invariant asks the same thing of the settled state, that the fleet
//! holds what it took responsibility for and no more.

use std::cmp::Ordering;

use crucible_protocol::{At, FaultReport, FaultResult};

use super::{
    Ack, Baseline, Checkpoint, Invariant, Judged, Readings, References, StepWindow, Trajectory,
};
use crate::{fault::Primitive, ipc::Verdict, plan::Value};

/// How many steps may be left in doubt before a run is no longer judged.
///
/// Each one doubles the states the fleet could be in.
const IN_DOUBT_LIMIT: usize = 8;

impl Readings {
    /// What this run's readings say the fleet did.
    ///
    /// Only where the fleet settled counts. A run that took on something other
    /// than a prefix of its steps says which reference runs it needs.
    #[must_use]
    pub fn judge(&self, references: &References) -> Judged {
        // No fault fired => nothing to test.
        let fault = match &self.fault {
            None => return unjudgeable("no fault was scheduled".to_owned()),
            Some(FaultReport {
                result: FaultResult::Missed(miss),
                ..
            }) => return unjudgeable(format!("fault did not fire: {miss:?}")),
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
            return unjudgeable(
                "the scenario drove nothing, so nothing was put at risk".to_owned(),
            );
        }
        if self.checks.is_empty() {
            return unjudgeable("the scenario states nothing to check after heal".to_owned());
        }

        // A step whose ack was lost leaves more than one set admissible, and the
        // fleet answers to any of them.
        let Some(sets) = self.landed_sets() else {
            return unjudgeable(format!(
                "more than {IN_DOUBT_LIMIT} steps were left in doubt, so what the fleet \
                 accepted cannot be enumerated"
            ));
        };
        let mut admissible: Vec<Admissible<'_>> = Vec::with_capacity(sets.len());
        let mut pending: Vec<Vec<usize>> = Vec::new();
        for set in sets {
            match ran(&self.fault_free, references, &set) {
                Some((settled, trail, settled_there)) => admissible.push(Admissible {
                    landed: set,
                    settled,
                    trail,
                    settled_there,
                }),
                // A prefix the fault-free run has no checkpoint for is one it
                // could not read. It drove every step, so no reference run
                // would answer differently.
                None if is_prefix(&set) => {}
                None => pending.push(set),
            }
        }
        let settled = self.settled();
        // Any outcome the run admits is enough. What is in doubt is what the
        // fleet accepted, and it is not held to the strictest reading of that.
        // One of the states already to hand settles it, and the rest cost a run
        // each, so this is asked before they are asked for.
        if admissible
            .iter()
            .any(|admits| matches!(differing(&settled, admits.settled), Ok(None)))
        {
            return Judged::Now(Verdict::Pass);
        }
        // Nothing the run has been held to fits, and ruling the rest out is
        // what saying it broke rests on.
        if !pending.is_empty() {
            return Judged::Pending(pending);
        }
        let Some(most) = admissible.last() else {
            return unjudgeable(format!(
                "the fault-free run left {} checkpoint(s), so it cannot say where the \
                 fleet's steps leave it",
                self.fault_free.trail.len(),
            ));
        };
        let (landed, expected) = (most.landed.clone(), most.settled);
        // Judged against the most it can have accepted, which is the most it can
        // owe.
        Judged::Now(match differing(&settled, expected) {
            Ok(None) => Verdict::Pass,
            Ok(Some(at)) => {
                let went = Went::of(
                    &points(&self.trajectory, &settled),
                    &points(&self.fault_free.trail, &self.fault_free.settled),
                    &self.outcomes.iter().map(|o| o.ack).collect::<Vec<_>>(),
                    matches!(fault.at, At::Throughout),
                );
                // The reading that says work was lost is the one to quote, and
                // it need not be the first the two runs disagree on. A fleet
                // can keep what it refused and lose what it accepted at once.
                let at = match went {
                    Went::Lost => short_count(&settled, &admissible).unwrap_or(at),
                    _ => at,
                };
                Verdict::Fail {
                    invariant: fault.broke(went),
                    reason: self.failure(fault, &settled, &admissible, &landed, at, went),
                }
            }
            Err(at) => Verdict::Inconclusive {
                reason: format!(
                    "`{}` could not be read in both runs",
                    self.observable_at(at)
                ),
            },
        })
    }

    /// What the run says, held to the reference runs there are rather than the
    /// ones it asked for.
    #[must_use]
    pub fn settle(&self, references: &References) -> Verdict {
        match self.judge(references) {
            Judged::Now(verdict) => verdict,
            Judged::Pending(sets) => Verdict::Inconclusive {
                reason: unreferenced(&sets),
            },
        }
    }

    /// What the run says with no reference runs at all.
    #[must_use]
    pub fn verdict(&self) -> Verdict {
        self.settle(&References::new())
    }

    /// Which steps the fleet may have taken responsibility for, fewest first.
    ///
    /// A step whose ack was lost may have landed or not. `None` where too many
    /// were left in doubt to enumerate.
    fn landed_sets(&self) -> Option<Vec<Vec<usize>>> {
        // Numbered from one, as the checkpoints are.
        let (acked, doubted): (Vec<usize>, Vec<usize>) = (
            self.settled_steps(Ack::Acked),
            self.settled_steps(Ack::Unknown),
        );
        if doubted.len() > IN_DOUBT_LIMIT {
            return None;
        }
        let mut sets: Vec<Vec<usize>> = (0..1u32 << doubted.len())
            .map(|mask| {
                let mut set = acked.clone();
                set.extend(
                    doubted
                        .iter()
                        .enumerate()
                        .filter(|(bit, _)| mask >> bit & 1 == 1)
                        .map(|(_, step)| *step),
                );
                set.sort_unstable();
                set
            })
            .collect();
        sets.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        Some(sets)
    }

    /// The steps, numbered from one, whose outcome was `ack`.
    fn settled_steps(&self, ack: Ack) -> Vec<usize> {
        self.outcomes
            .iter()
            .enumerate()
            .filter(|(_, outcome)| outcome.ack == ack)
            .map(|(i, _)| i + 1)
            .collect()
    }

    /// What the fault was, where it landed, what should have been true, and the step
    /// the fleet started getting it wrong at.
    fn failure(
        &self,
        fault: Placed<'_>,
        settled: &Checkpoint,
        admissible: &[Admissible<'_>],
        landed: &[usize],
        at: usize,
        went: Went,
    ) -> String {
        let observable = self.observable_at(at);
        let expected = admissible.last().map(|most| most.settled);
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
        // Only the steps the fleet took, which for a run that refused one and
        // served the next is not the first few.
        let diverged = match self.diverged_at().filter(|step| landed.contains(step)) {
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
            .zip(&self.fault_free.trail)
            .position(|(reached, expected)| reached != expected)
    }

    fn observable_at(&self, i: usize) -> String {
        self.checks.get(i).map_or_else(
            || format!("observable {i}"),
            |observed| observed.check.observable(),
        )
    }
}

/// What a step did to one reading, from the checkpoints either side of it.
///
/// `None` where either side went unread, which is not the same as a step that
/// changed nothing.
fn moved(was: Option<&Value>, now: Option<&Value>) -> Option<Delta> {
    let (was, now) = (was?, now?);
    Some(Delta {
        by: match (was, now) {
            (Value::Int(was), Value::Int(now)) => Some(now - was),
            (was, now) if was == now => Some(0),
            _ => None,
        },
        to: now.clone(),
    })
}

/// What a step did to a reading.
/// Whether a reading took the same steps in a different order.
///
/// The same changes in a different order is the fleet applying every step it
/// was given, just not when it was told to. Settling is left out: it is where
/// the run came to rest, not a step that could have arrived elsewhere.
fn rearranged(spans: &[Span], steps: usize) -> bool {
    let spans = spans.get(..steps.min(spans.len())).unwrap_or_default();
    // Steps that already line up are not a rearrangement of one another, and
    // whatever the run got wrong it was not the order.
    if spans.is_empty() || spans.iter().all(Span::matches) {
        return false;
    }
    // A reading the steps add to arrives at the same total whatever order they
    // came in, and one they overwrite ends on whichever landed last, so either
    // reading of the steps showing the same work is the order having moved.
    let moved: Option<(Vec<i64>, Vec<i64>)> = spans
        .iter()
        .map(|span| Some((span.drove.by?, span.learned.by?)))
        .collect::<Option<Vec<_>>>()
        .map(|both| both.into_iter().unzip());
    let held: (Vec<&Value>, Vec<&Value>) = spans
        .iter()
        .map(|span| (&span.drove.to, &span.learned.to))
        .unzip();
    matches!(moved, Some((ref drove, ref learned)) if same_work(drove, learned))
        || same_work(&held.0, &held.1)
}

/// One stretch of a reading that both runs could read either end of.
///
/// A step the observer read on its own, or a run of steps that went unread.
struct Span {
    /// What the faulted run did across the stretch.
    drove: Delta,
    /// What the fault-free run did across the same stretch.
    learned: Delta,
    /// Whether the fleet took on any of the work the stretch covers.
    acked: bool,
}

impl Span {
    /// Every stretch of one reading the faulted run could read the ends of.
    ///
    /// `None` where it read too little to say anything, or where the fault-free
    /// run cannot answer for the same stretch. Steps that went unread make one
    /// stretch rather than none: where the fleet got to across them can still
    /// be read.
    fn across(
        drove: &[Option<Value>],
        learned: &[Option<Value>],
        acks: &[Ack],
    ) -> Option<Vec<Span>> {
        let marks: Vec<usize> = drove
            .iter()
            .enumerate()
            .filter(|(_, at)| at.is_some())
            .map(|(at, _)| at)
            .collect();
        if marks.len() < 2 {
            return None;
        }
        marks
            .windows(2)
            .map(|pair| {
                let (from, to) = (pair[0], pair[1]);
                let covered = acks.get(from.min(acks.len())..to.min(acks.len()))?;
                Some(Span {
                    drove: moved(drove.get(from)?.as_ref(), drove.get(to)?.as_ref())?,
                    learned: moved(learned.get(from)?.as_ref(), learned.get(to)?.as_ref())?,
                    // A stretch past the last step is settling, which nobody
                    // acknowledged and the fleet owes regardless.
                    acked: covered.is_empty() || covered.contains(&Ack::Acked),
                })
            })
            .collect()
    }

    /// Whether both runs did the same thing across this stretch.
    fn matches(&self) -> bool {
        self.drove.matches(&self.learned)
    }
}

/// Whether two runs' steps hold the same work, in whatever order.
fn same_work<T: PartialEq>(drove: &[T], learned: &[T]) -> bool {
    if drove.len() != learned.len() {
        return false;
    }
    let mut learned: Vec<&T> = learned.iter().collect();
    for step in drove {
        let Some(at) = learned.iter().position(|learned| *learned == step) else {
            return false;
        };
        learned.swap_remove(at);
    }
    learned.is_empty()
}

/// What one step did to one reading.
///
/// A step both moves a reading and leaves it holding something. Nothing
/// declares which of those a reading is for, so both are kept and the run is
/// read whichever way accounts for it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Delta {
    /// How far the reading moved, where both ends are numbers.
    by: Option<i64>,
    /// What the reading came to hold.
    to: Value,
}

impl Delta {
    /// Whether this is `once` applied twice over.
    fn is_twice(&self, once: &Delta) -> bool {
        match (self.by, once.by) {
            (Some(twice), Some(once)) => once != 0 && twice == once * 2,
            _ => false,
        }
    }

    /// Whether the step left the reading where it found it.
    fn is_nothing(&self) -> bool {
        self.by == Some(0)
    }

    /// Whether the fleet moved a reading less far than `owed`, the way `owed`
    /// went.
    ///
    /// Steps taken while the observer was out of service are measured together,
    /// so the fleet can land some of that work and lose the rest. Moving none of
    /// what it owed and moving some of it are both work that did not land.
    fn is_short_of(&self, owed: &Delta) -> bool {
        let (Some(ours), Some(owed)) = (self.by, owed.by) else {
            return false;
        };
        owed != 0 && ours.signum() != -owed.signum() && ours.abs() < owed.abs()
    }

    /// Whether two runs' steps did the same thing.
    ///
    /// How far a reading moved, where that can be read, since the two runs sit
    /// at different places and it is the change that is comparable.
    fn matches(&self, other: &Delta) -> bool {
        match (self.by, other.by) {
            (Some(ours), Some(theirs)) => ours == theirs,
            _ => self.to == other.to,
        }
    }
}

/// Every reading's value at each checkpoint, and then where it settled.
///
/// A row per reading rather than per step: one reading goes unread while the
/// rest still answer.
fn points(trail: &Trajectory, settled: &Checkpoint) -> Vec<Vec<Option<Value>>> {
    let stops = || trail.iter().chain(std::iter::once(settled));
    let width = stops().map(Vec::len).max().unwrap_or(0);
    (0..width)
        .map(|reading| {
            stops()
                .map(|at| at.get(reading).cloned().flatten())
                .collect()
        })
        .collect()
}

/// What the fleet did that the fault-free run did not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Went {
    /// It holds less than every state the run admits.
    Lost,
    /// It settled where taking one of its steps twice would have left it.
    Twice,
    /// It settled where one of its steps arriving last would have left it.
    OutOfOrder,
    /// It holds more than every state the run admits, and no step taken twice
    /// puts it there, so it kept work it turned away.
    Kept,
    /// None of those describes where it ended up.
    Elsewhere,
    /// The readings cannot say.
    Unreadable,
}

impl Went {
    /// What the run's own trail says the fleet did with each step.
    ///
    /// The fault-free run recorded what every step does to every reading. This
    /// run recorded the same, so the two sequences of changes are read against
    /// each other a step at a time. Nothing is projected and nothing is
    /// declared: a step that changed nothing it was meant to, changed something
    /// it was refused for, or made the same change twice says so itself.
    fn of(
        drove: &[Vec<Option<Value>>],
        learned: &[Vec<Option<Value>>],
        acks: &[Ack],
        degraded: bool,
    ) -> Self {
        // A fleet held down for the whole run has a trail that says only that
        // it was down. It catches up after the last step, so what it holds once
        // it is back is the only evidence of whether it did.
        if degraded {
            return if acks.contains(&Ack::Acked) {
                Went::Lost
            } else {
                Went::Kept
            };
        }
        let mut readable = true;
        let mut went: Option<Went> = None;
        // A run that turned steps away still has to answer for them, even where
        // it moved exactly as the fault-free run did.
        let refused = acks.iter().any(|ack| *ack != Ack::Acked);
        // A run can lose one reading and double another, so each is read on its
        // own and the strongest thing said about any of them is what the run
        // showed.
        for (drove, learned) in drove.iter().zip(learned) {
            let Some(spans) = Span::across(drove, learned, acks) else {
                readable = false;
                continue;
            };
            if !refused && spans.iter().all(Span::matches) {
                continue;
            }
            // Order is only visible where every step was read on its own. A
            // stretch that went unread holds its steps in no particular order.
            let stepwise = spans.len() == drove.len() - 1;
            let this = if !refused && stepwise && rearranged(&spans, acks.len()) {
                Went::OutOfOrder
            } else {
                Went::of_spans(&spans)
            };
            went = Some(match went {
                None => this,
                Some(said) => said.or(this),
            });
        }
        match went {
            Some(went) => went,
            None if readable => Went::Elsewhere,
            None => Went::Unreadable,
        }
    }

    /// What one reading's stretches say, when they are not a rearrangement.
    fn of_spans(spans: &[Span]) -> Went {
        let mut went = Went::Elsewhere;
        for span in spans {
            // A stretch the fleet turned away owes nothing, whatever the
            // fault-free run did in its place. Moving at all is the fleet
            // keeping work it said it had not taken.
            if !span.acked {
                if !span.drove.is_nothing() {
                    went = went.or(Went::Kept);
                }
                continue;
            }
            if span.matches() {
                continue;
            }
            went = went.or(if span.drove.is_twice(&span.learned) {
                Went::Twice
            } else if span.drove.is_short_of(&span.learned) {
                Went::Lost
            } else {
                Went::Elsewhere
            });
        }
        went
    }

    /// Which of two readings of the same run to report.
    ///
    /// Work the fleet accepted and lost is the strongest thing to have shown,
    /// ahead of work it refused and held, then work it did twice, and all of
    /// them ahead of nothing describing it.
    fn or(self, other: Went) -> Went {
        let rank = |went: Went| match went {
            Went::Lost => 4,
            Went::Kept => 3,
            Went::Twice => 2,
            Went::OutOfOrder => 1,
            Went::Elsewhere | Went::Unreadable => 0,
        };
        if rank(other) > rank(self) {
            other
        } else {
            self
        }
    }

    /// Which invariant this breaks, or `None` where nothing about the settled
    /// state names one.
    fn shows(self, degraded: bool) -> Option<Invariant> {
        match self {
            // Work the fleet took on while it was down and does not hold once
            // it is back is work it never caught up on, which is recovery. The
            // same reading under a fault placed at a moment is work lost.
            Went::Lost if degraded => Some(Invariant::Recovers),
            Went::Lost | Went::Kept => Some(Invariant::Durable),
            Went::Twice => Some(Invariant::Idempotent),
            Went::OutOfOrder => Some(Invariant::Converges),
            Went::Elsewhere | Went::Unreadable => None,
        }
    }
}

/// A run nothing can be said about, which no reference run would change.
fn unjudgeable(reason: String) -> Judged {
    Judged::Now(Verdict::Inconclusive { reason })
}

/// Why a run went unjudged. It stood somewhere no run has been, and none was
/// taken there.
fn unreferenced(sets: &[Vec<usize>]) -> String {
    let spelled: Vec<String> = sets
        .iter()
        .map(|set| {
            let steps: Vec<String> = set.iter().map(ToString::to_string).collect();
            format!("({})", steps.join(", "))
        })
        .collect();
    format!(
        "the fleet may have accepted steps {}, which no run has driven on their own, so where \
         they leave it is unknown",
        spelled.join(" or ")
    )
}

/// One state the run's own acknowledgements admit, and the run that found it.
struct Admissible<'a> {
    /// The steps the fleet would have taken to be here, numbered from one.
    landed: Vec<usize>,
    /// Where that run settled, which is what this run is held to.
    settled: &'a Checkpoint,
    /// Where it stood after each of those steps on the way.
    trail: &'a Trajectory,
    /// Whether `settled` is where a run stopped and was read once the fleet
    /// went quiet, or a point it passed through.
    settled_there: bool,
}

/// Whether `landed` is the first steps of the scenario and nothing else.
fn is_prefix(landed: &[usize]) -> bool {
    landed.iter().copied().eq(1..=landed.len())
}

/// Where the run that drove exactly `landed` left the fleet, and where it
/// stood after each step.
///
/// The fault-free run answers for a prefix. Any other set needs a reference
/// run.
fn ran<'a>(
    fault_free: &'a Baseline,
    references: &'a References,
    landed: &[usize],
) -> Option<(&'a Checkpoint, &'a Trajectory, bool)> {
    if is_prefix(landed) {
        // Only the whole run stopped where it was read once the fleet went
        // quiet. Every shorter prefix is a point it passed through.
        let settled_there = landed.len() + 1 == fault_free.trail.len();
        return Some((
            fault_free.at(landed.len())?,
            &fault_free.trail,
            settled_there,
        ));
    }
    let reference = references.get(landed)?;
    Some((&reference.settled, &reference.trail, true))
}

impl Trajectory {
    /// Whether every step left this reading where it was or higher.
    ///
    /// Holding less of something the steps only ever added to is work missing.
    /// Holding less of something they took from is work done. The order its
    /// steps arrived in moves a reading they overwrite and leaves one they add
    /// to alone, so what a step did tells the two apart.
    fn climbs(&self, at: usize) -> bool {
        let mut moved = false;
        for (was, now) in self.iter().zip(self.iter().skip(1)) {
            let (Some(was), Some(now)) = (
                was.get(at).and_then(Option::as_ref),
                now.get(at).and_then(Option::as_ref),
            ) else {
                continue;
            };
            match super::order(was, now) {
                Some(Ordering::Less) => moved = true,
                Some(Ordering::Equal) => {}
                _ => return false,
            }
        }
        moved
    }
}

/// The first observable the two readings disagree on, `None` if they agree,
/// and the index of one only one of them could read.
///
/// Neither being able to read one is agreement.
fn differing(reached: &Checkpoint, expected: &Checkpoint) -> Result<Option<usize>, usize> {
    for (i, (reached, expected)) in reached.iter().zip(expected).enumerate() {
        match (reached, expected) {
            (None, None) => {}
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

    /// Which invariant the run showed broken, or `None` where the settled
    /// state names none of the ones this fault could have shown.
    fn broke(self, went: Went) -> Option<Invariant> {
        match self.could_show() {
            [only] => Some(*only),
            could => went
                .shows(self.degraded())
                .filter(|shown| could.contains(shown)),
        }
    }

    /// Whether the fleet was held down for the whole run rather than broken at
    /// a moment in it.
    fn degraded(self) -> bool {
        matches!(self.at, At::Throughout)
    }

    /// What the run says broke, and the evidence for saying it.
    ///
    /// A way of breaking the fleet that can show only one thing needs no
    /// evidence. Anything broader is read off where the fleet settled.
    fn told(self, went: Went, settled: &Value, admissible: &[Admissible<'_>], at: usize) -> String {
        if let [only] = self.could_show() {
            // The fault asks one question, so the answer is what broke. Where
            // the fleet settled is not what decides it, but a reader is owed
            // whether it agrees.
            return match went.shows(self.degraded()) {
                Some(shown) if shown == *only => format!(
                    ". Breaking the fleet this way can show nothing but {only}, and where it \
                     settled says the same, so that is what broke"
                ),
                _ => format!(
                    ". Breaking the fleet this way can show nothing but {only}, so that is what \
                     broke, though where it settled does not say so"
                ),
            };
        }
        let could = spelled(self.could_show());
        match went {
            Went::Lost if self.degraded() => {
                ". It took work on while it was down and does not hold it now it is back, so it \
                 never caught up, which is recovery"
                    .to_owned()
            }
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
            Went::Kept => ". It holds more than the steps it took responsibility for owed, and no \
                            step taken twice puts it there, so it kept work it turned away, which \
                            is durability"
                .to_owned(),
            Went::Elsewhere => format!(
                "{}. It settled where losing a step, taking one twice and taking one out of order \
                 would all have left it somewhere else, so which of {could} broke cannot be read from \
                 where it settled",
                parted(settled, admissible, at)
            ),
            Went::Unreadable => format!(
                "{}. What the fleet would hold had it lost, repeated or resequenced a step cannot be \
                 worked out from what this run has, so which of {could} broke cannot be read from \
                 where it settled",
                parted(settled, admissible, at)
            ),
        }
    }
}

/// The first count that reads lower than every state the run admits.
///
/// Not always the first reading the two runs disagree on.
fn short_count(settled: &Checkpoint, admissible: &[Admissible<'_>]) -> Option<usize> {
    if admissible.is_empty() {
        return None;
    }
    let trail = admissible.first()?.trail;
    (0..settled.len()).position(|at| {
        trail.climbs(at)
            && settled
                .get(at)
                .and_then(Option::as_ref)
                .filter(|settled| {
                    matches!(settled, Value::Int(_) | Value::Duration(_) | Value::List(_))
                })
                .is_some_and(|settled| {
                    admissible.iter().all(|admits| {
                        admits
                            .settled
                            .get(at)
                            .and_then(Option::as_ref)
                            .and_then(|owed| super::order(settled, owed))
                            == Some(Ordering::Less)
                    })
                })
    })
}

/// Whether `projected` describes where the fleet settled, or `None` where a
/// reading it rests on could not be projected.
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
/// admits.
///
/// Empty where the reading has no order.
fn parted(settled: &Value, admissible: &[Admissible<'_>], at: usize) -> &'static str {
    // More or less than it owed is a quantity. A reading that only sorts has an
    // order without having an amount.
    if !matches!(settled, Value::Int(_) | Value::Duration(_)) {
        return "";
    }
    let owed: Vec<&Value> = admissible
        .iter()
        .filter_map(|admits| admits.settled.get(at).and_then(Option::as_ref))
        .collect();
    let every = |way: Ordering| {
        !owed.is_empty()
            && owed
                .iter()
                .all(|owed| super::order(settled, owed) == Some(way))
    };
    // A run held to a point the fault-free run passed through had longer to
    // reach what it holds, so reading high says less than it appears to.
    let stopped_there = admissible.iter().all(|admits| admits.settled_there);
    match (
        every(Ordering::Greater),
        every(Ordering::Less),
        stopped_there,
    ) {
        (true, _, true) => ". It held more than it owed on any reading",
        (true, _, false) => {
            ". It held more than it owed on any reading, one of which is a point the \
             fault-free run passed through rather than where it stopped"
        }
        (_, true, _) => ". It held less than it owed on any reading",
        _ => "",
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
            // Past the previous step's end and short of this one's start, so
            // nothing of the scenario was in flight.
            if at_ns < window.start_ns {
                return format!("between steps {i} and {}", i + 1);
            }
            return format!("during step {}", i + 1);
        }
    }
    format!("after step {}", windows.len())
}

/// Which steps the fleet took, spelled so a verdict reads as a sentence.
///
/// The first few are a count. Anything else names them, since "2 steps" would
/// read as the first two.
fn steps(landed: &[usize]) -> String {
    if is_prefix(landed) {
        return match landed.len() {
            1 => "1 step".into(),
            n => format!("{n} steps"),
        };
    }
    let spelled: Vec<String> = landed.iter().map(ToString::to_string).collect();
    match spelled.split_last() {
        Some((last, [])) => format!("step {last}"),
        Some((last, rest)) => format!("steps {} and {last}", rest.join(", ")),
        None => "no steps".into(),
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::{At, FaultReport, FaultResult};

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
                direction: Some(crucible_protocol::Direction::ClientToUpstream),
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

    /// A run broken by `fault` that acknowledged `acks` and settled at
    /// `settled`, read against a fault-free run standing at `fault_free`.
    fn run_of(fault: FaultReport, acks: &[Ack], fault_free: &[i64], settled: i64) -> Readings {
        // The run drove what it acknowledged and nothing for what it refused,
        // which is the trail a fleet that did as it was told would keep.
        let mut stood = vec![fault_free[0]];
        for (step, pair) in fault_free.windows(2).enumerate() {
            let took = acks.get(step).is_none_or(|ack| *ack == Ack::Acked);
            let last = *stood.last().expect("the trail starts somewhere");
            stood.push(if took { last + pair[1] - pair[0] } else { last });
        }
        with_trail(fault, acks, fault_free, &stood, settled)
    }

    /// The same run, saying what its own trail was rather than letting it be
    /// taken for one that did as it was told.
    fn with_trail(
        fault: FaultReport,
        acks: &[Ack],
        fault_free: &[i64],
        stood: &[i64],
        settled: i64,
    ) -> Readings {
        let mut obs = Readings::empty();
        obs.fault = Some(fault);
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![reading(settled)];
        obs.trajectory = stood.iter().copied().map(checkpoint).collect();
        obs.fault_free = Baseline::unsettled(fault_free.iter().copied().map(checkpoint));
        obs
    }

    /// A reference run that settled at `at`, with no trail.
    fn settling(at: i64) -> Baseline {
        Baseline {
            trail: Trajectory::default(),
            settled: checkpoint(at),
        }
    }

    /// A run whose fault fired and whose own trail stood at each of `stood`.
    fn drove(acks: &[Ack], fault_free: &[i64], stood: &[i64], settled: i64) -> Verdict {
        with_trail(fired_fault(), acks, fault_free, stood, settled).verdict()
    }

    /// A run whose fault fired, judged with no reference runs to hand.
    fn judged(acks: &[Ack], fault_free: &[i64], settled: i64) -> Verdict {
        broken_by(fired_fault(), acks, fault_free, settled)
    }

    /// A run whose fleet was broken by `fault`, judged the same way.
    fn broken_by(fault: FaultReport, acks: &[Ack], fault_free: &[i64], settled: i64) -> Verdict {
        run_of(fault, acks, fault_free, settled).verdict()
    }

    /// The same run, judged with a reference run for each of `references`.
    fn judged_given(
        acks: &[Ack],
        fault_free: &[i64],
        settled: i64,
        references: &[(&[usize], i64)],
    ) -> Verdict {
        let references: References = references
            .iter()
            .map(|(landed, at)| (landed.to_vec(), settling(*at)))
            .collect();
        match run_of(fired_fault(), acks, fault_free, settled).judge(&references) {
            Judged::Now(verdict) => verdict,
            Judged::Pending(sets) => panic!("expected a verdict, wants references for {sets:?}"),
        }
    }

    /// The landed sets a run cannot be judged without.
    fn wants(acks: &[Ack], fault_free: &[i64], settled: i64) -> Vec<Vec<usize>> {
        match run_of(fired_fault(), acks, fault_free, settled).judge(&References::new()) {
            Judged::Pending(sets) => sets,
            Judged::Now(verdict) => panic!("expected pending, got {verdict:?}"),
        }
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
                clauses: std::collections::BTreeMap::new(),
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Str(read.to_owned()),
            },
            value: Some(plan::Value::Str(read.to_owned())),
        }
    }

    /// Two readings, where the run's own trail stood at each of `stood`.
    fn ordered_trail(
        fault: FaultReport,
        acks: &[Ack],
        fault_free: &[(i64, &str)],
        stood: &[(i64, &str)],
        settled: (i64, &str),
    ) -> Verdict {
        let pair = |(count, at): &(i64, &str)| {
            vec![
                Some(plan::Value::Int(*count)),
                Some(plan::Value::Str((*at).to_owned())),
            ]
        };
        let mut obs = Readings::empty();
        obs.fault = Some(fault);
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![reading(settled.0), address(settled.1)];
        obs.trajectory = stood.iter().map(pair).collect();
        obs.fault_free = Baseline::unsettled(fault_free.iter().map(pair));
        obs.verdict()
    }

    /// What a failing run says broke, and why.
    fn showed(verdict: Verdict) -> (Option<Invariant>, String) {
        match verdict {
            Verdict::Fail { invariant, reason } => (invariant, reason),
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    /// A fault can leave the fleet with no row to answer from.
    #[test]
    fn a_check_the_fleet_cannot_answer_is_not_a_failure() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_throughout());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![Observed {
            value: None,
            ..reading(3)
        }];
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), checkpoint(3)]);
        assert!(
            matches!(obs.verdict(), Verdict::Inconclusive { .. }),
            "{:?}",
            obs.verdict()
        );
    }

    #[test]
    fn a_run_that_observed_nothing_is_inconclusive() {
        assert!(matches!(
            Readings::empty().verdict(),
            Verdict::Inconclusive { .. }
        ));
    }

    #[test]
    fn a_missed_fault_is_inconclusive() {
        let mut obs = Readings::empty();
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
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes.push(outcome(Ack::Acked));
        assert!(matches!(obs.verdict(), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_that_drove_nothing_is_not_a_test() {
        let mut obs = Readings::empty();
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

    /// A refusal in the middle leaves the fleet somewhere the fault-free run
    /// never stood.
    #[test]
    fn a_step_refused_while_a_later_one_landed_needs_a_run_of_its_own() {
        assert_eq!(wants(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1), [[2]]);
    }

    #[test]
    fn a_step_refused_while_a_later_one_landed_answers_to_its_reference() {
        assert_eq!(
            judged_given(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1, &[(&[2], 1)]),
            Verdict::Pass,
        );
        assert!(matches!(
            judged_given(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 2, &[(&[2], 1)]),
            Verdict::Fail { .. },
        ));
    }

    /// A lost ack leaves the fleet owing either answer, so either is a pass.
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

    /// Either answer to the doubt describes a state the fleet could be in.
    #[test]
    fn a_later_landed_step_leaves_a_lost_ack_open() {
        for settled in [1, 2] {
            assert_eq!(
                judged_given(
                    &[Ack::Unknown, Ack::Acked],
                    &[0, 1, 2],
                    settled,
                    &[(&[2], 1)]
                ),
                Verdict::Pass,
                "settling at {settled}",
            );
        }
        assert!(matches!(
            judged_given(&[Ack::Unknown, Ack::Acked], &[0, 1, 2], 0, &[(&[2], 1)]),
            Verdict::Fail { .. },
        ));
    }

    /// Every step in doubt, so the fleet may have taken all of them or none.
    #[test]
    fn a_run_of_lost_acks_admits_every_checkpoint() {
        for settled in [0, 1, 2] {
            assert_eq!(
                judged_given(
                    &[Ack::Unknown, Ack::Unknown],
                    &[0, 1, 2],
                    settled,
                    &[(&[2], 1)]
                ),
                Verdict::Pass,
                "settling at {settled}",
            );
        }
    }

    /// The fleet turning one step away says nothing about whether it took the
    /// next.
    #[test]
    fn a_refusal_does_not_bound_what_a_later_lost_ack_admits() {
        let acks = [Ack::Acked, Ack::Rejected, Ack::Unknown];
        let reference: &[(&[usize], i64)] = &[(&[1, 3], 2)];
        for settled in [1, 2] {
            assert_eq!(
                judged_given(&acks, &[0, 1, 2, 3], settled, reference),
                Verdict::Pass,
                "settling at {settled}",
            );
        }
        assert!(matches!(
            judged_given(&acks, &[0, 1, 2, 3], 3, reference),
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

    /// The scenario asks after order 1 and the run refused every step, so
    /// neither the fleet nor its baseline holds one.
    #[test]
    fn a_check_neither_run_can_answer_is_agreement() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_throughout());
        obs.outcomes = vec![outcome(Ack::Rejected), outcome(Ack::Rejected)];
        obs.checks = vec![Observed {
            value: None,
            ..reading(0)
        }];
        obs.fault_free = Baseline::unsettled(vec![vec![None], checkpoint(1), checkpoint(2)]);
        assert_eq!(obs.verdict(), Verdict::Pass);
    }

    /// The fault-free run could not read the state it is the authority on.
    #[test]
    fn an_unread_observable_is_inconclusive() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), vec![None]]);
        assert!(matches!(obs.verdict(), Verdict::Inconclusive { .. }));
    }

    /// Falling behind under the fault and catching up afterwards is a fleet
    /// that recovered.
    #[test]
    fn diverging_and_coming_back_is_pass() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(2)];
        obs.trajectory = vec![checkpoint(0), checkpoint(0), checkpoint(2)]
            .into_iter()
            .collect();
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), checkpoint(1), checkpoint(2)]);
        assert_eq!(obs.verdict(), Verdict::Pass);
    }

    #[test]
    fn a_failure_points_at_the_step_the_run_first_differed_after() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(1)]
            .into_iter()
            .collect();
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), checkpoint(1), checkpoint(2)]);
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("settling short of the fault-free run is a failure");
        };
        assert!(reason.contains("after step 2"), "reason: {reason}");
    }

    /// Parting after the step being judged says nothing about why the run
    /// failed.
    #[test]
    fn a_failure_keeps_quiet_about_a_divergence_past_the_step_it_judged() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Rejected)];
        obs.checks = vec![reading(2)];
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(5)]
            .into_iter()
            .collect();
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), checkpoint(1), checkpoint(2)]);
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("holding 2 where 1 step landed is a failure");
        };
        assert!(!reason.contains("first differed"), "reason: {reason}");
    }

    /// Nothing about killing a service says the work was done twice. The run
    /// does.
    #[test]
    fn a_fleet_that_did_a_step_twice_broke_idempotency() {
        let (broke, reason) = showed(drove(&[Ack::Acked, Ack::Acked], &[0, 1, 2], &[0, 2, 3], 3));
        assert_eq!(broke, Some(Invariant::Idempotent));
        assert!(reason.contains("work was done twice"), "{reason}");
    }

    #[test]
    fn a_fleet_that_dropped_a_step_broke_durability() {
        let (broke, reason) = showed(drove(&[Ack::Acked, Ack::Acked], &[0, 1, 2], &[0, 1, 1], 1));
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(reason.contains("work was lost"), "{reason}");
    }

    /// A reading of `stock.level` for pens, which a step sets rather than adds
    /// to.
    fn pens(read: i64) -> Observed {
        Observed {
            check: plan::Check {
                observable: vec!["stock".into(), "select".into()],
                ..reading(read).check
            },
            value: Some(plan::Value::Int(read)),
        }
    }

    /// The fleet wrote down every order and acted on all but one. The count is
    /// short while the reading the lost step would have set is untouched.
    #[test]
    fn work_lost_from_the_middle_of_a_run_is_still_lost() {
        let mut obs = Readings::empty();
        obs.fault = Some(placed(Primitive::Kill, 0));
        obs.outcomes = [Ack::Acked; 5].into_iter().map(outcome).collect();
        // Five events owed, four applied, and the pens the lost one carried are
        // still on the shelf.
        obs.checks = vec![reading(4), pens(500)];
        obs.trajectory = [(0, 500), (1, 500), (1, 500), (2, 500), (3, 500), (4, 500)]
            .into_iter()
            .map(|(applied, pens)| {
                vec![
                    Some(plan::Value::Int(applied)),
                    Some(plan::Value::Int(pens)),
                ]
            })
            .collect();
        obs.fault_free = Baseline::unsettled(
            [(0, 500), (1, 500), (2, 490), (3, 490), (4, 490), (5, 490)]
                .into_iter()
                .map(|(applied, pens)| {
                    vec![
                        Some(plan::Value::Int(applied)),
                        Some(plan::Value::Int(pens)),
                    ]
                }),
        );

        // Not where any shorter run of the steps ends. Every one of those has
        // the pens gone.
        for n in 0..=4 {
            assert_ne!(
                obs.fault_free.at(n).map(Vec::as_slice),
                Some(&[Some(plan::Value::Int(4)), Some(plan::Value::Int(500))][..]),
                "checkpoint {n}",
            );
        }
        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(why.contains("work was lost"), "{why}");
    }

    /// A fleet can keep what it refused and lose what it accepted in the same
    /// run. The reading they first disagree on is then the surplus, not the
    /// shortfall.
    #[test]
    fn a_loss_is_quoted_from_the_reading_that_fell_short() {
        let mut obs = Readings::empty();
        obs.fault = Some(placed(Primitive::Kill, 0));
        // One step accepted, the rest refused.
        obs.outcomes = [Ack::Acked, Ack::Rejected, Ack::Rejected]
            .into_iter()
            .map(outcome)
            .collect();
        // Three orders written down where one was owed, and none of them acted
        // on where one was owed.
        let mut kept = reading(3);
        kept.check.observable = vec!["orders".into(), "count".into()];
        obs.checks = vec![kept, reading(0)];
        obs.trajectory = [(0, 0), (1, 0), (2, 0), (3, 0)]
            .into_iter()
            .map(|(orders, applied)| {
                vec![
                    Some(plan::Value::Int(orders)),
                    Some(plan::Value::Int(applied)),
                ]
            })
            .collect();
        obs.fault_free = Baseline::unsettled([(0, 0), (1, 1), (2, 2), (3, 3)].into_iter().map(
            |(orders, applied)| {
                vec![
                    Some(plan::Value::Int(orders)),
                    Some(plan::Value::Int(applied)),
                ]
            },
        ));

        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(why.contains("`writes.count` at `0`"), "{why}");
        assert!(!why.contains("orders.count"), "{why}");
    }

    /// Steps taken while the observer was out of service are measured together,
    /// so the fleet can land some of that work and lose the rest.
    #[test]
    fn work_partly_landed_while_the_observer_was_down_is_lost() {
        let mut obs = Readings::empty();
        obs.fault = Some(placed(Primitive::Kill, 3));
        obs.outcomes = [Ack::Acked; 5].into_iter().map(outcome).collect();
        obs.checks = vec![reading(4)];
        // Read for three steps, then out for the two the kill landed on. One of
        // those two landed and the other did not.
        obs.trajectory = [Some(0), Some(1), Some(2), Some(3), None, None]
            .into_iter()
            .map(|applied| vec![applied.map(plan::Value::Int)])
            .collect();
        obs.fault_free =
            Baseline::unsettled((0..=5).map(|applied| vec![Some(plan::Value::Int(applied))]));

        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(why.contains("work was lost"), "{why}");
    }

    /// A reading that went unread still has both ends of what it covered, and
    /// what the fleet did across them is read from those.
    #[test]
    fn work_lost_while_the_reading_was_out_is_still_lost() {
        let mut obs = Readings::empty();
        obs.fault = Some(placed(Primitive::Kill, 3));
        obs.outcomes = [Ack::Acked; 5].into_iter().map(outcome).collect();
        obs.checks = vec![reading(3)];
        // Read for the first three steps, then out for the two the kill landed
        // on, and readable again once the fleet was back.
        obs.trajectory = [Some(0), Some(1), Some(2), Some(3), None, None]
            .into_iter()
            .map(|applied| vec![applied.map(plan::Value::Int)])
            .collect();
        obs.fault_free =
            Baseline::unsettled((0..=5).map(|applied| vec![Some(plan::Value::Int(applied))]));

        // The fault-free run moved two across those steps and this one moved
        // none, so the two it took on never landed.
        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(why.contains("work was lost"), "{why}");
    }

    /// Both runs agreeing there is nothing there is a reading, so the other
    /// checks still answer.
    #[test]
    fn a_reading_no_run_could_take_leaves_the_others_answerable() {
        let absent = |applied: i64| vec![Some(plan::Value::Int(applied)), None];
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        // A count the fleet keeps, and a row no run in this world creates.
        obs.checks = vec![
            reading(3),
            Observed {
                check: plan::Check {
                    observable: vec!["orders".into(), "select".into()],
                    ..reading(0).check
                },
                value: None,
            },
        ];
        obs.trajectory = [0, 2, 3].into_iter().map(absent).collect();
        obs.fault_free = Baseline::unsettled([0, 1, 2].into_iter().map(absent));

        // Three where two were owed, which is one of them applied twice, and
        // the unread column no longer stops that being said.
        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Idempotent));
        assert!(why.contains("work was done twice"), "{why}");
    }

    /// The fault-free run cannot say, since it only ever took step 3 after
    /// step 2.
    #[test]
    fn without_one_the_same_run_names_nothing() {
        let obs = run_of(
            fired_fault(),
            &[Ack::Acked, Ack::Rejected, Ack::Acked],
            &[0, 1, 2, 3],
            5,
        );
        let (broke, _) = showed(
            match obs.judge(&References::from([(vec![1, 3], settling(3))])) {
                Judged::Now(verdict) => verdict,
                Judged::Pending(sets) => panic!("wants references for {sets:?}"),
            },
        );
        assert_eq!(broke, None);
    }

    /// A fault that asks one question answers it, and says whether where the
    /// fleet settled agrees. A reader who cannot see the corroboration cannot
    /// tell a fleet that failed the question from one that failed elsewhere.
    #[test]
    fn a_fault_that_asks_one_question_says_whether_the_state_agrees() {
        let doubled = with_trail(
            placed(Primitive::Redeliver, 0),
            &[Ack::Acked, Ack::Acked],
            &[0, 1, 2],
            &[0, 2, 3],
            3,
        )
        .verdict();
        let (broke, why) = showed(doubled);
        assert_eq!(broke, Some(Invariant::Idempotent));
        assert!(why.contains("where it settled says the same"), "{why}");

        // Short of what it owed, which a redelivery does not explain.
        let short = with_trail(
            placed(Primitive::Redeliver, 0),
            &[Ack::Acked, Ack::Acked],
            &[0, 1, 2],
            &[0, 1, 1],
            1,
        )
        .verdict();
        let (broke, why) = showed(short);
        assert_eq!(broke, Some(Invariant::Idempotent));
        assert!(why.contains("does not say so"), "{why}");
    }

    /// A fleet held down all run that accepted nothing had nothing to catch up
    /// on, so what it kept is durability rather than recovery.
    #[test]
    fn a_degraded_run_that_accepted_nothing_did_not_fail_to_recover() {
        let mut run = run_of(
            fired_fault(),
            &[Ack::Rejected, Ack::Rejected, Ack::Rejected],
            &[0, 1, 2, 3],
            3,
        );
        run.fault = Some(FaultReport::fired(
            0,
            "db",
            Primitive::Kill,
            At::Throughout,
            0,
        ));
        let (broke, why) = showed(run.verdict());
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(why.contains("kept work it turned away"), "{why}");
    }

    /// One held down all run that took work on and does not hold it once it is
    /// back never caught up, which is what recovery is.
    #[test]
    fn a_degraded_run_that_never_caught_up_shows_recovery() {
        let mut run = run_of(
            fired_fault(),
            &[Ack::Acked, Ack::Acked, Ack::Acked],
            &[0, 1, 2, 3],
            0,
        );
        run.fault = Some(FaultReport::fired(
            0,
            "db",
            Primitive::Kill,
            At::Throughout,
            0,
        ));
        let (broke, why) = showed(run.verdict());
        assert_eq!(broke, Some(Invariant::Recovers));
        assert!(why.contains("never caught up"), "{why}");
    }

    /// A fleet that wrote a row down and then failed the caller holds work for
    /// a step it turned away. Nothing was lost and nothing was done twice, so
    /// only what the fleet kept says what broke.
    #[test]
    fn keeping_what_it_refused_is_durability() {
        // One step acknowledged of three driven, and the fleet sits where all
        // three would have left it.
        let run = with_trail(
            fired_fault(),
            &[Ack::Acked, Ack::Rejected, Ack::Rejected],
            &[0, 1, 2, 3],
            &[0, 1, 2, 3],
            3,
        );
        let (broke, why) = showed(run.verdict());
        assert_eq!(broke, Some(Invariant::Durable));
        assert!(why.contains("kept work it turned away"), "{why}");
    }

    /// Holding more than it owed is only kept work where driving the steps it
    /// refused would have put it there. Anywhere else names nothing.
    #[test]
    fn holding_more_than_any_run_would_leave_is_not_kept_work() {
        let (broke, why) = showed(judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 7));
        assert_eq!(broke, None);
        assert!(
            why.contains("would all have left it somewhere else"),
            "{why}"
        );
    }

    /// The verdict for a way that was asked and ruled out is worded
    /// differently from one for a way that could not be asked.
    #[test]
    fn a_run_that_ruled_every_way_out_says_so() {
        // Every step acknowledged, so the only state the run admits is the
        // whole scenario, and every way of leaving it is a prefix away.
        let (broke, why) = showed(judged(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 7));
        assert_eq!(broke, None);
        assert!(
            why.contains("would all have left it somewhere else"),
            "{why}"
        );
    }

    /// Settling between two admissible readings is neither losing a step nor
    /// taking one twice.
    #[test]
    fn settling_where_nothing_describes_it_names_no_invariant() {
        let (broke, reason) = showed(judged_given(
            &[Ack::Unknown, Ack::Unknown],
            &[0, 5, 10],
            7,
            &[(&[2], 5)],
        ));
        assert_eq!(broke, None);
        assert!(
            reason.contains("cannot be read from where it settled"),
            "{reason}"
        );
    }

    #[test]
    fn an_unattributed_failure_says_what_it_could_have_been() {
        let (_, reason) = showed(judged_given(
            &[Ack::Unknown, Ack::Unknown],
            &[0, 5, 10],
            7,
            &[(&[2], 5)],
        ));
        assert!(
            reason.contains("which of durability, idempotency or convergence broke"),
            "{reason}"
        );
    }

    /// Making a message arrive twice asks one question, so the settled state
    /// has nothing to add.
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

    /// The count says all three landed, so nothing was lost, and the reading
    /// each step sets says the second landed last.
    #[test]
    fn a_fleet_holding_every_step_in_the_wrong_order_shows_convergence() {
        let (broke, why) = showed(ordered_trail(
            placed(Primitive::Kill, 0),
            &[Ack::Acked, Ack::Acked, Ack::Acked],
            &[(0, "none"), (1, "one"), (2, "two"), (3, "three")],
            &[(0, "none"), (1, "one"), (2, "three"), (3, "two")],
            (3, "two"),
        ));
        assert_eq!(broke, Some(Invariant::Converges));
        assert!(why.contains("arrived after the rest"), "{why}");
    }

    /// A count reaches the same total whichever order its steps arrive in.
    #[test]
    fn counts_alone_cannot_show_an_order() {
        assert_eq!(
            broken_by(
                placed(Primitive::Reorder, 0),
                &[Ack::Acked; 3],
                &[0, 1, 2, 3],
                3
            ),
            Verdict::Pass,
        );
    }

    /// Holding a run to the step boundary instead of the settled reading makes
    /// it look like it did the work twice.
    #[test]
    fn a_run_answers_to_where_the_fault_free_run_settled_not_where_it_stepped() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(2)];
        // The fault-free run was at 1 when its last step ended, and at 2 once
        // the fleet had finished with it.
        obs.fault_free = Baseline {
            trail: [0, 1, 1].into_iter().map(checkpoint).collect(),
            settled: checkpoint(2),
        };

        assert_eq!(obs.verdict(), Verdict::Pass);
    }

    /// A prefix is a run the fault-free run recorded the end of, so it is read
    /// off by index.
    #[test]
    fn a_prefix_is_read_off_and_anything_else_is_a_reference() {
        // A fault-free run whose settled reading waited longer than its last
        // trail point, which is what every run answers to.
        let fault_free = Baseline {
            trail: [0, 5, 9, 14].into_iter().map(checkpoint).collect(),
            settled: checkpoint(15),
        };
        let references = References::from([(vec![2, 3], settling(9))]);

        for n in 0..3 {
            let prefix: Vec<usize> = (1..=n).collect();
            assert_eq!(
                ran(&fault_free, &references, &prefix).map(|(settled, ..)| settled),
                fault_free.trail.at(n),
                "the first {n} steps",
            );
        }
        // Every step, so the settled reading rather than the trail point.
        assert_eq!(
            ran(&fault_free, &references, &[1, 2, 3]).map(|(settled, ..)| settled),
            Some(&checkpoint(15)),
        );
        assert_eq!(
            ran(&fault_free, &references, &[2, 3]).map(|(settled, ..)| settled),
            Some(&checkpoint(9)),
        );
        assert!(ran(&fault_free, &References::new(), &[2, 3]).is_none());
    }

    /// A reading the observer calls a value may be a count, and where the
    /// third step left it depends on having had the second.
    #[test]
    fn a_reading_worked_out_from_steps_that_did_not_land_is_not_taken() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = [Ack::Unknown, Ack::Acked, Ack::Acked, Ack::Acked, Ack::Acked]
            .into_iter()
            .map(outcome)
            .collect();
        // The API's own store read over HTTP, which the observer calls a value
        // because it cannot know the field is a count, and the consumer's rows,
        // which it knows are.
        obs.checks = vec![
            Observed {
                check: plan::Check { ..reading(3).check },
                value: Some(plan::Value::Int(3)),
            },
            reading(2),
        ];
        obs.fault_free = Baseline::unsettled(
            [0, 1, 2, 3, 3, 3]
                .into_iter()
                .map(|n| vec![Some(plan::Value::Int(n)), Some(plan::Value::Int(n))]),
        );

        // Steps 2 to 5 landed and step 1 is in doubt. Taking the fault-free
        // run's third checkpoint for the first reading would say 3, matching a
        // fleet whose two halves disagree, and the run would pass.
        assert_eq!(
            obs.judge(&References::new()),
            Judged::Pending(vec![vec![2, 3, 4, 5]])
        );

        // A run that drove only those four steps says the API should hold 2,
        // and holding 3 is the fleet keeping what it never acknowledged.
        let references = References::from([(
            vec![2, 3, 4, 5],
            Baseline {
                trail: Trajectory::default(),
                settled: vec![Some(plan::Value::Int(2)), Some(plan::Value::Int(2))],
            },
        )]);
        assert!(matches!(
            obs.judge(&references),
            Judged::Now(Verdict::Fail { .. })
        ));
    }

    /// A reading of `zone.sequence`, which each step sets rather than adds to.
    fn sequence(read: i64) -> Observed {
        Observed {
            check: plan::Check {
                service: "pdns".into(),
                observer: "http".into(),
                observable: vec!["zone".into(), "sequence".into()],
                args: Vec::new(),
                filter: None,
                clauses: std::collections::BTreeMap::new(),
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Int(read),
            },
            value: Some(plan::Value::Int(read)),
        }
    }

    /// A reading the steps overwrite ends where the last of them left it, so
    /// the order they arrived in decides it. One they add to reaches the same
    /// total either way. Both are integers; how each moved through the
    /// fault-free run is what tells them apart.
    #[test]
    fn a_number_a_step_sets_is_not_read_as_a_count() {
        let mut obs = Readings::empty();
        obs.fault = Some(placed(Primitive::Kill, 0));
        obs.outcomes = vec![
            outcome(Ack::Acked),
            outcome(Ack::Acked),
            outcome(Ack::Acked),
        ];
        // The count is where three steps leave it. The overwritten reading is
        // where step 2 left it, which is where step 3 arriving first would.
        obs.checks = vec![reading(3), sequence(2)];
        obs.trajectory = [(0, 0), (1, 6), (2, 9), (3, 2)]
            .into_iter()
            .map(|(count, seq)| vec![Some(plan::Value::Int(count)), Some(plan::Value::Int(seq))])
            .collect();
        let written = [0, 6, 2, 9];
        obs.fault_free = Baseline::unsettled((0..=3).map(|n| {
            vec![
                Some(plan::Value::Int(n)),
                Some(plan::Value::Int(
                    written[usize::try_from(n).expect("small")],
                )),
            ]
        }));

        let (broke, why) = showed(obs.verdict());
        assert_eq!(broke, Some(Invariant::Converges));
        assert!(why.contains("arrived after the rest"), "{why}");
    }

    /// That is the shape of the run, so it is known before the run rather than
    /// read off it.
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

    /// Working out what a step taken twice would have left needs arithmetic.
    #[test]
    fn a_verdict_on_a_degraded_run_says_it_stood_throughout() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_throughout());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), checkpoint(1), checkpoint(2)]);
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
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![filtered];
        obs.fault_free = Baseline::unsettled(vec![checkpoint(100), checkpoint(96)]);
        let Verdict::Fail { reason, .. } = obs.verdict() else {
            panic!("settling somewhere the fault-free run never did is a failure");
        };
        assert!(
            reason.contains(r#"stock.select level where item = "book""#),
            "reason: {reason}"
        );
    }

    /// A verdict leads with the fault and where in the scenario it landed.
    #[test]
    fn a_verdict_names_the_fault_that_caused_it() {
        let mut obs = Readings::empty();
        obs.fault = Some(fired_fault_at(150));
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = Baseline::unsettled(vec![checkpoint(0), checkpoint(1), checkpoint(2)]);
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
