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

        // Without a persisted-state snapshot we can't say either way.
        let Some(db) = &observations.db_state else {
            return Verdict::Inconclusive {
                reason: "no persisted-state snapshot after heal".into(),
            };
        };

        // The dual contract: everything acknowledged survived, and nothing
        // survived that was never acknowledged. An operation left in doubt may
        // legitimately land either way, so it only widens the upper bound.
        let acked = observations.acked();
        let unknown = observations.unknown();
        let observed = db.orders.len();

        if observed < acked {
            return Verdict::Fail {
                reason: format!("{acked} acked write(s), but only {observed} survived the heal"),
            };
        }
        if observed > acked + unknown {
            return Verdict::Fail {
                reason: format!(
                    "{observed} write(s) persisted, but at most {} were acknowledged (zombie writes)",
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
    use crate::verdict::{Ack, DbState, Invariant, OrderRow, Outcome, driver_for};

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

    fn persisted(count: u64) -> DbState {
        DbState {
            orders: (0..count)
                .map(|id| OrderRow {
                    id,
                    item: "item".into(),
                    quantity: 1,
                })
                .collect(),
            stock: Vec::new(),
        }
    }

    /// A run whose fault fired and whose state was snapshotted, so the dual
    /// contract is the only thing left to decide the verdict.
    fn judged(acks: &[Ack], survived: u64) -> Verdict {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.outcomes = acks.iter().copied().map(outcome).collect();
        obs.db_state = Some(persisted(survived));
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
        obs.db_state = Some(DbState::default());
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn missing_db_state_is_inconclusive() {
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
