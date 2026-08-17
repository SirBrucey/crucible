//! Verdict drivers: what a run's observations mean for the invariant it tested.

use crucible_protocol::{At, FaultReport, FaultResult};

use super::{Ack, Checkpoint, Driver, Observations, StepWindow};
use crate::fault::Primitive;
use crate::ipc::Verdict;

pub struct Durable;

/// Recovery asks the same question of the settled state as durability: the fleet
/// owes what it accepted, however it was degraded while accepting it. What
/// differs is the fault, not the reading.
pub struct Recovers;

impl Driver for Durable {
    fn drive(&mut self, observations: &Observations) -> Verdict {
        // No fault fired => nothing to test.
        let fault = match &observations.fault {
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

        if observations.outcomes.is_empty() {
            return Verdict::Inconclusive {
                reason: "the scenario drove nothing, so nothing was put at risk".into(),
            };
        }
        if observations.checks.is_empty() {
            return Verdict::Inconclusive {
                reason: "the scenario states nothing to check after heal".into(),
            };
        }

        // A checkpoint says where the fleet stands once the first N steps have
        // landed. If every step landed we hold the run to the last one, in
        // whatever order they got there. If a step in the middle did not land
        // and a later one did, no checkpoint says where that leaves things.
        let landed = match steps_landed(observations) {
            Ok(landed) => landed,
            Err(reason) => return Verdict::Inconclusive { reason },
        };
        let Some(expected) = observations.fault_free.get(landed) else {
            return Verdict::Inconclusive {
                reason: format!(
                    "the fault-free run left {} checkpoint(s), so it cannot say where {landed} step(s) leave the fleet",
                    observations.fault_free.len()
                ),
            };
        };

        // What the fleet settled on once the target was back, which is the only
        // state the verdict turns on. Diverging part way through and coming back
        // is a fleet that recovered, not a fleet that lost something.
        let settled: Checkpoint = observations
            .checks
            .iter()
            .map(|observed| Some(observed.value.clone()))
            .collect();
        match differing(&settled, expected) {
            Ok(None) => Verdict::Pass,
            Ok(Some(at)) => Verdict::Fail {
                reason: failure(observations, fault, &settled, expected, landed, at),
            },
            Err(at) => Verdict::Inconclusive {
                reason: format!(
                    "`{}` could not be read in both runs",
                    observable(observations, at)
                ),
            },
        }
    }
}

impl Driver for Recovers {
    fn drive(&mut self, observations: &Observations) -> Verdict {
        Durable.drive(observations)
    }
}

/// How many steps the fleet took responsibility for. Counts from the first, and
/// refuses a run that skipped one and carried on, since no checkpoint says where
/// that leaves the fleet.
fn steps_landed(observations: &Observations) -> Result<usize, String> {
    let landed: Vec<bool> = observations
        .outcomes
        .iter()
        .map(|outcome| outcome.ack == Ack::Acked)
        .collect();
    let counted = landed.iter().take_while(|landed| **landed).count();
    if landed[counted..].iter().any(|landed| *landed) {
        return Err(format!(
            "step {} did not land and a later one did, which no checkpoint describes",
            counted + 1
        ));
    }
    Ok(counted)
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
        }
    }

    /// When it was done, against the steps the scenario drove.
    fn when(self, windows: &[StepWindow]) -> String {
        match self.at {
            At::Packet { offset_ns, .. } => placement(windows, *offset_ns),
            At::Throughout => "for the whole run".into(),
        }
    }
}

/// What the fault was, where it landed, what should have been true, and the step
/// the fleet started getting it wrong at.
fn failure(
    observations: &Observations,
    fault: Placed<'_>,
    settled: &Checkpoint,
    expected: &Checkpoint,
    landed: usize,
    at: usize,
) -> String {
    let observable = observable(observations, at);
    let reason = match (&settled[at], &expected[at]) {
        (Some(settled), Some(expected)) => format!(
            "The fleet took {} which left `{observable}` at `{settled}`, expected value \
             `{expected}`",
            steps(landed)
        ),
        _ => format!("`{observable}` disagrees with the fault-free run"),
    };
    // Only worth saying when the state parted on the way to the step being
    // judged. Parting after that is downstream of the verdict, not evidence for
    // it, and reads as a contradiction next to the count of steps that landed.
    let diverged = match diverged_at(observations).filter(|step| (1..=landed).contains(step)) {
        Some(step) => format!(". It first differed after step {step}"),
        None => String::new(),
    };
    format!(
        "`{}` {} {}. {reason}{diverged}",
        fault.service,
        fault.done(),
        fault.when(&observations.windows)
    )
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

/// The step this run's state first parted from the fault-free run's.
fn diverged_at(observations: &Observations) -> Option<usize> {
    observations
        .trajectory
        .iter()
        .zip(&observations.fault_free)
        .position(|(reached, expected)| reached != expected)
}

fn observable(observations: &Observations, i: usize) -> String {
    observations.checks.get(i).map_or_else(
        || format!("observable {i}"),
        |observed| observed.check.observable(),
    )
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
        FaultReport::fired(
            0,
            "db",
            Primitive::Kill,
            At::Packet {
                direction: crucible_protocol::Direction::ClientToUpstream,
                k: 1,
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
        Outcome {
            operation: "write".into(),
            ack,
            request: Vec::new(),
            response: Vec::new(),
        }
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
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Int(read),
            },
            value: plan::Value::Int(read),
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
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![reading(settled)];
        obs.fault_free = fault_free.iter().copied().map(checkpoint).collect();
        Durable.drive(&obs)
    }

    #[test]
    fn a_run_that_observed_nothing_is_inconclusive() {
        assert!(matches!(
            Durable.drive(&Observations::empty()),
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
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_with_nothing_to_check_is_inconclusive() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes.push(outcome(Ack::Acked));
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_that_drove_nothing_is_not_a_test() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.checks = vec![reading(0)];
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
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

    #[test]
    fn a_fault_free_run_too_short_to_say_is_inconclusive() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Acked], &[0, 1], 2),
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
        obs.fault_free = vec![checkpoint(0), vec![None]];
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    /// Falling behind under the fault and catching up afterwards is a fleet
    /// that recovered, so only where it settled counts.
    #[test]
    fn diverging_and_coming_back_is_pass() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(2)];
        obs.trajectory = vec![checkpoint(0), checkpoint(0), checkpoint(2)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        assert_eq!(Durable.drive(&obs), Verdict::Pass);
    }

    #[test]
    fn a_failure_points_at_the_step_the_run_first_differed_after() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_fault());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(1)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        let Verdict::Fail { reason } = Durable.drive(&obs) else {
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
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(5)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        let Verdict::Fail { reason } = Durable.drive(&obs) else {
            panic!("holding 2 where 1 step landed is a failure");
        };
        assert!(!reason.contains("first differed"), "reason: {reason}");
    }

    #[test]
    fn a_verdict_on_a_degraded_run_says_it_stood_throughout() {
        let mut obs = Observations::empty();
        obs.fault = Some(fired_throughout());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        obs.windows = vec![window(0, 100), window(120, 200)];
        let Verdict::Fail { reason } = Durable.drive(&obs) else {
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
        obs.fault_free = vec![checkpoint(100), checkpoint(96)];
        let Verdict::Fail { reason } = Durable.drive(&obs) else {
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
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        obs.windows = vec![window(0, 100), window(120, 200)];
        let Verdict::Fail { reason } = Durable.drive(&obs) else {
            panic!("settling short of the fault-free run is a failure");
        };
        assert!(
            reason.starts_with("`db` was killed during step 2."),
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
