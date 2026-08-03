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

        // Weighing one tally of the whole run against one reading only says
        // anything when the reading covers the whole run too. Until a run
        // records what each step left behind, anything else is a question this
        // cannot answer, and answering it anyway would be a verdict about
        // something nobody asked.
        let observed = match observations.checks.as_slice() {
            [only] => only,
            [] => {
                return Verdict::Inconclusive {
                    reason: "the scenario states nothing to check after heal".into(),
                };
            }
            several => {
                return Verdict::Inconclusive {
                    reason: format!(
                        "durability weighs what the run wrote against one reading, and the scenario states {}",
                        several.len()
                    ),
                };
            }
        };
        if observed.check.filter.is_some() {
            return Verdict::Inconclusive {
                reason: "a filtered check reads part of what the run wrote, which the whole of it cannot be weighed against".into(),
            };
        }

        let name = observed.check.observable.join(".");
        let Some(reading) = observed.value.as_int() else {
            return Verdict::Inconclusive {
                reason: format!("`{name}` did not read as a number of writes"),
            };
        };
        let Ok(survived) = usize::try_from(reading) else {
            return Verdict::Inconclusive {
                reason: format!("`{name}` read as {reading}, which is not a count"),
            };
        };

        // The dual contract: everything acknowledged survived, and nothing
        // survived that was never acknowledged. An operation left in doubt may
        // legitimately land either way, so it only widens the upper bound. The
        // scenario's own expectation is the fault-free one, which a kill
        // legitimately changes, so the tally replaces it here.
        let acked = observations.acked();
        let unknown = observations.unknown();
        // Both bounds hold trivially when the fleet took nothing on, so a run
        // that never got a write in was not a durable one; it was not a test.
        if acked + unknown == 0 {
            return Verdict::Inconclusive {
                reason: "the fleet acknowledged nothing, so nothing was there to survive".into(),
            };
        }
        if survived < acked {
            return Verdict::Fail {
                reason: format!(
                    "{acked} acked write(s), but `{name}` holds only {survived} after heal"
                ),
            };
        }
        if survived > acked + unknown {
            return Verdict::Fail {
                reason: format!(
                    "`{name}` holds {survived}, but at most {} were acknowledged (zombie writes)",
                    acked + unknown
                ),
            };
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

    fn read(survived: i64) -> Observed {
        Observed {
            check: plan::Check {
                service: "db".into(),
                observer: "mariadb".into(),
                observable: vec!["writes".into(), "count".into()],
                filter: None,
                op: crate::schema::CmpOp::Eq,
                value: plan::Value::Int(survived),
            },
            value: plan::Value::Int(survived),
        }
    }

    /// A run whose fault fired and whose state was read, so the dual contract is
    /// the only thing left to decide the verdict.
    fn judged(acks: &[Ack], survived: i64) -> Verdict {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.checks = vec![read(survived)];
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
        obs.checks = vec![read(0)];
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
    fn a_run_the_fleet_took_nothing_on_is_not_a_test() {
        // Both bounds hold at zero, so a fleet that refused everything and holds
        // nothing would otherwise pass while having tested nothing at all.
        assert!(matches!(
            judged(&[Ack::Rejected, Ack::Rejected], 0),
            Verdict::Inconclusive { .. }
        ));
    }

    #[test]
    fn a_filtered_check_is_not_weighed_against_the_whole_run() {
        // The tally counts every step; the reading counts the rows the filter
        // matched. Comparing them fails whenever the filter matches less.
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes = vec![outcome(Ack::Acked), outcome(Ack::Acked)];
        let mut filtered = read(1);
        filtered.check.filter = Some(("item".into(), plan::Value::Str("book".into())));
        obs.checks = vec![filtered];
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn several_checks_are_several_questions() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes = vec![outcome(Ack::Acked)];
        obs.checks = vec![read(1), read(1)];
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn acked_writes_that_survive_are_pass() {
        assert_eq!(judged(&[Ack::Acked, Ack::Acked], 2), Verdict::Pass);
    }

    #[test]
    fn an_acked_write_that_did_not_survive_is_fail() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Acked], 1),
            Verdict::Fail { .. }
        ));
    }

    #[test]
    fn a_write_that_was_never_acked_is_a_zombie() {
        assert!(matches!(
            judged(&[Ack::Acked, Ack::Rejected], 2),
            Verdict::Fail { .. }
        ));
    }

    #[test]
    fn a_write_left_in_doubt_may_land_either_way() {
        assert_eq!(judged(&[Ack::Acked, Ack::Unknown], 1), Verdict::Pass);
        assert_eq!(judged(&[Ack::Acked, Ack::Unknown], 2), Verdict::Pass);
    }
}
