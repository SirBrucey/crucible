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

        // With nothing read after the heal we can't say either way.
        if observations.checks.is_empty() {
            return Verdict::Inconclusive {
                reason: "the scenario states nothing to check after heal".into(),
            };
        }

        // The dual contract: everything acknowledged survived, and nothing
        // survived that was never acknowledged. An operation left in doubt may
        // legitimately land either way, so it only widens the upper bound. The
        // scenario's own expectation is the fault-free one, which a kill
        // legitimately changes, so the tally replaces it here.
        let acked = observations.acked();
        let unknown = observations.unknown();
        for observed in &observations.checks {
            let name = observed.check.observable.join(".");
            let Some(survived) = observed.value.as_int() else {
                return Verdict::Inconclusive {
                    reason: format!("`{name}` did not read as a number of writes"),
                };
            };
            let Ok(survived) = usize::try_from(survived) else {
                return Verdict::Inconclusive {
                    reason: format!("`{name}` read as {survived}, which is not a count"),
                };
            };
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
