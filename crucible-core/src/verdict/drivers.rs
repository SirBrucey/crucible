//! Verdict drivers for the four invariants. Idempotent, Converges, and
//! Recovers remain stubs until their invariants land.

use super::{Ack, Checkpoint, Driver, Observations};
use crate::ipc::Verdict;

pub struct Idempotent;
pub struct Converges;
pub struct Durable;
pub struct Recovers;

impl Driver for Idempotent {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive {
            reason: "idempotency driver not yet implemented".into(),
        }
    }
}

impl Driver for Converges {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive {
            reason: "convergence driver not yet implemented".into(),
        }
    }
}

impl Driver for Durable {
    fn drive(&mut self, observations: &Observations) -> Verdict {
        // No fault fired => nothing to test.
        let Some(kill) = &observations.kill else {
            return Verdict::Inconclusive {
                reason: "no kill was scheduled".into(),
            };
        };
        if let crucible_protocol::KillResult::Missed(miss) = &kill.result {
            return Verdict::Inconclusive {
                reason: format!("fault did not fire: {miss:?}"),
            };
        }

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
                reason: failure(observations, &settled, expected, landed, at),
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

/// What went wrong, and the step the fleet started getting it wrong at.
fn failure(
    observations: &Observations,
    settled: &Checkpoint,
    expected: &Checkpoint,
    landed: usize,
    at: usize,
) -> String {
    let observable = observable(observations, at);
    let reason = match (&settled[at], &expected[at]) {
        (Some(settled), Some(expected)) => format!(
            "the fleet took {landed} step(s), which leaves `{observable}` at {expected}, and it \
             holds {settled}"
        ),
        _ => format!("`{observable}` disagrees with the fault-free run"),
    };
    match diverged_at(observations) {
        Some(0) | None => reason,
        Some(step) => format!("{reason}; it first differed after step {step}"),
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

impl Driver for Recovers {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive {
            reason: "recovery driver not yet implemented".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::{KillReport, KillResult};

    use super::*;
    use crate::{
        plan,
        verdict::{Ack, Invariant, Observed, Outcome, driver_for},
    };

    fn fired_kill() -> KillReport {
        KillReport {
            schedule_id: 0,
            service: "db".into(),
            result: KillResult::Fired {
                requested_direction: crucible_protocol::Direction::ClientToUpstream,
                requested_packet_index: 1,
                actual_offset_ns: 0,
                killed_at_ns: 0,
            },
        }
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
        obs.kill = Some(fired_kill());
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![reading(settled)];
        obs.fault_free = fault_free.iter().copied().map(checkpoint).collect();
        Durable.drive(&obs)
    }

    #[test]
    fn every_stub_yields_inconclusive_on_empty_observations() {
        let obs = Observations::empty();
        for invariant in [
            Invariant::Idempotent,
            Invariant::Converges,
            Invariant::Durable,
            Invariant::Recovers,
        ] {
            assert!(matches!(
                driver_for(invariant).drive(&obs),
                Verdict::Inconclusive { .. }
            ));
        }
    }

    #[test]
    fn missed_kill_is_inconclusive() {
        let mut obs = Observations::empty();
        obs.kill = Some(KillReport {
            schedule_id: 0,
            service: "db".into(),
            result: KillResult::Missed(
                crucible_protocol::KillMissReason::ScenarioEndedBeforeAnchor,
            ),
        });
        obs.checks = vec![reading(0)];
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_with_nothing_to_check_is_inconclusive() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes.push(outcome(Ack::Acked));
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_scenario_that_drove_nothing_is_not_a_test() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
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
        obs.kill = Some(fired_kill());
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
        obs.kill = Some(fired_kill());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(2)];
        obs.trajectory = vec![checkpoint(0), checkpoint(0), checkpoint(2)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        assert_eq!(Durable.drive(&obs), Verdict::Pass);
    }

    #[test]
    fn a_failure_points_at_the_step_the_run_first_differed_after() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        obs.checks = vec![reading(1)];
        obs.trajectory = vec![checkpoint(0), checkpoint(1), checkpoint(1)];
        obs.fault_free = vec![checkpoint(0), checkpoint(1), checkpoint(2)];
        let Verdict::Fail { reason } = Durable.drive(&obs) else {
            panic!("settling short of the fault-free run is a failure");
        };
        assert!(reason.contains("after step 2"), "reason: {reason}");
    }

    #[test]
    fn a_verdict_names_the_check_as_the_scenario_spells_it() {
        let mut filtered = reading(100);
        filtered.check.observable = vec!["stock".into(), "select".into()];
        filtered.check.args = vec![plan::Value::Ident("level".into())];
        filtered.check.filter = Some(("item".into(), plan::Value::Str("book".into())));
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
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
}
