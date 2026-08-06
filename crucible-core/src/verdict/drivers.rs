//! Verdict drivers for the four invariants. Idempotent, Converges, and
//! Recovers remain stubs until their invariants land.

use super::{Driver, Observations};
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

        // A step the caller was told had failed leaves the fleet somewhere the
        // scenario never describes: not where it started and not where it was
        // written to end. Saying where takes a checkpoint per step, so until
        // there is one this declines rather than holding a partial run to an
        // expectation written for a whole one.
        let undelivered = observations.undelivered();
        if undelivered > 0 {
            return Verdict::Inconclusive {
                reason: format!(
                    "{undelivered} of {} step(s) did not complete, and where that leaves the fleet takes a checkpoint per step to say",
                    observations.outcomes.len()
                ),
            };
        }

        // Every step completed, so the fleet was told to reach the state the
        // scenario describes and reported that it had. Anything else is state
        // lost or invented behind the caller's back.
        for observed in &observations.checks {
            let observable = observed.observable();
            match observed.holds() {
                Some(true) => {}
                Some(false) => {
                    return Verdict::Fail {
                        reason: format!(
                            "every step completed, but `{observable}` holds {} rather than {}",
                            observed.value, observed.check.value
                        ),
                    };
                }
                None => {
                    return Verdict::Inconclusive {
                        reason: format!(
                            "`{observable}` read as {}, which the scenario's {} cannot be compared against",
                            observed.value, observed.check.value
                        ),
                    };
                }
            }
        }
        Verdict::Pass
    }
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

    /// A check stating `writes.count == stated`, read as `read`.
    fn reading(stated: i64, read: i64) -> Observed {
        Observed {
            check: plan::Check {
                service: "db".into(),
                observer: "mariadb".into(),
                observable: vec!["writes".into(), "count".into()],
                args: Vec::new(),
                filter: None,
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Int(stated),
            },
            value: plan::Value::Int(read),
        }
    }

    /// A run whose fault fired and whose state was read, so what the scenario
    /// states is the only thing left to decide the verdict.
    fn judged(acks: &[Ack], readings: Vec<Observed>) -> Verdict {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = readings;
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
        obs.checks = vec![reading(0, 0)];
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
        obs.checks = vec![reading(0, 0)];
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn a_run_that_reached_what_the_scenario_states_is_pass() {
        assert_eq!(
            judged(&[Ack::Acked, Ack::Acked], vec![reading(2, 2)]),
            Verdict::Pass,
        );
    }

    #[test]
    fn every_check_has_to_hold() {
        assert_eq!(
            judged(
                &[Ack::Acked],
                vec![reading(1, 1), reading(1, 1), reading(1, 1)],
            ),
            Verdict::Pass,
        );
        assert!(matches!(
            judged(&[Ack::Acked], vec![reading(1, 1), reading(1, 0)]),
            Verdict::Fail { .. },
        ));
    }

    #[test]
    fn state_the_run_never_reached_is_fail() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Acked], vec![reading(2, 1)]),
            Verdict::Fail { .. },
        ));
    }

    #[test]
    fn state_the_run_never_asked_for_is_fail() {
        assert!(matches!(
            judged(&[Ack::Acked], vec![reading(1, 2)]),
            Verdict::Fail { .. },
        ));
    }

    /// A step the caller was told had failed leaves the fleet part way through
    /// the scenario, and the scenario only describes the end of it.
    #[test]
    fn a_step_that_did_not_complete_is_inconclusive() {
        for ack in [Ack::Rejected, Ack::Unknown] {
            assert!(matches!(
                judged(&[Ack::Acked, ack], vec![reading(2, 1)]),
                Verdict::Inconclusive { .. },
            ));
        }
    }

    #[test]
    fn a_reading_of_another_shape_is_not_a_comparison() {
        let mut mismatched = reading(1, 1);
        mismatched.value = plan::Value::Str("one".into());
        assert!(matches!(
            judged(&[Ack::Acked], vec![mismatched]),
            Verdict::Inconclusive { .. },
        ));
    }

    #[test]
    fn an_ordering_holds_as_well_as_an_equality() {
        let mut at_least = reading(2, 3);
        at_least.check.op = crate::schema::CmpOp::Ge;
        assert_eq!(judged(&[Ack::Acked], vec![at_least]), Verdict::Pass);

        let mut too_few = reading(2, 1);
        too_few.check.op = crate::schema::CmpOp::Ge;
        assert!(matches!(
            judged(&[Ack::Acked], vec![too_few]),
            Verdict::Fail { .. },
        ));
    }

    #[test]
    fn a_verdict_names_the_check_as_the_scenario_spells_it() {
        let mut filtered = reading(96, 100);
        filtered.check.observable = vec!["stock".into(), "select".into()];
        filtered.check.args = vec![plan::Value::Ident("level".into())];
        filtered.check.filter = Some(("item".into(), plan::Value::Str("book".into())));
        let Verdict::Fail { reason } = judged(&[Ack::Acked], vec![filtered]) else {
            panic!("a reading that misses what the scenario states is a failure");
        };
        assert!(
            reason.contains(r#"stock.select level where item = "book""#),
            "reason: {reason}"
        );
    }
}
