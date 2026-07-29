//! Verdict drivers for the four invariants. Idempotent, Converges, and
//! Recovers remain stubs until their invariants land.

use serde::Deserialize;

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

        // Parse every 2xx request/response into an AckedOrder.
        let mut acked: Vec<AckedOrder> = Vec::new();
        for outcome in &observations.http_outcomes {
            if !(200..300).contains(&outcome.status) {
                continue;
            }
            let Ok(req) = serde_json::from_slice::<OrderRequestBody>(&outcome.request_body) else {
                return Verdict::Inconclusive {
                    reason: "could not decode an acked order request".into(),
                };
            };
            let Ok(resp) = serde_json::from_slice::<OrderResponseBody>(&outcome.body) else {
                return Verdict::Inconclusive {
                    reason: "could not decode an acked order response".into(),
                };
            };
            acked.push(AckedOrder {
                order_id: resp.order_id,
                item: req.item,
                quantity: req.quantity,
            });
        }

        // Every acked write must be present in the DB with the same fields.
        for ack in &acked {
            let matched = db.orders.iter().any(|row| {
                row.id == ack.order_id && row.item == ack.item && row.quantity == ack.quantity
            });
            if !matched {
                return Verdict::Fail {
                    reason: format!(
                        "acked order {} ({} x{}) is absent from persisted state after heal",
                        ack.order_id, ack.item, ack.quantity
                    ),
                };
            }
        }

        // No un-acked writes should have made it to the DB (zombie state).
        if db.orders.len() > acked.len() {
            return Verdict::Fail {
                reason: format!(
                    "persisted {} orders but only {} were acked (zombie writes)",
                    db.orders.len(),
                    acked.len()
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

#[derive(Deserialize)]
struct OrderRequestBody {
    item: String,
    quantity: i32,
}

#[derive(Deserialize)]
struct OrderResponseBody {
    order_id: u64,
}

struct AckedOrder {
    order_id: u64,
    item: String,
    quantity: i32,
}

#[cfg(test)]
mod tests {
    use crucible_protocol::{KillReport, KillResult};

    use super::*;
    use crate::verdict::{DbState, HttpOutcome, Invariant, OrderRow, driver_for};

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

    fn ack(item: &str, quantity: i32, order_id: u64) -> HttpOutcome {
        HttpOutcome {
            method: "POST".into(),
            path: "/orders".into(),
            request_body: serde_json::to_vec(&serde_json::json!({
                "item": item,
                "quantity": quantity,
            }))
            .unwrap(),
            status: 200,
            body: serde_json::to_vec(&serde_json::json!({ "order_id": order_id })).unwrap(),
        }
    }

    fn err(item: &str, quantity: i32) -> HttpOutcome {
        HttpOutcome {
            method: "POST".into(),
            path: "/orders".into(),
            request_body: serde_json::to_vec(&serde_json::json!({
                "item": item,
                "quantity": quantity,
            }))
            .unwrap(),
            status: 0,
            body: Vec::new(),
        }
    }

    fn row(id: u64, item: &str, quantity: i32) -> OrderRow {
        OrderRow {
            id,
            item: item.into(),
            quantity,
        }
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
        obs.http_outcomes.push(ack("book", 4, 1));
        assert!(matches!(Durable.drive(&obs), Verdict::Inconclusive { .. }));
    }

    #[test]
    fn ack_matched_in_db_is_pass() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.http_outcomes.push(ack("book", 4, 1));
        obs.db_state = Some(DbState {
            orders: vec![row(1, "book", 4)],
            stock: vec![],
        });
        assert_eq!(Durable.drive(&obs), Verdict::Pass);
    }

    #[test]
    fn acked_but_not_persisted_is_fail() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.http_outcomes.push(ack("book", 4, 1));
        obs.db_state = Some(DbState {
            orders: vec![],
            stock: vec![],
        });
        assert!(matches!(Durable.drive(&obs), Verdict::Fail { .. }));
    }

    #[test]
    fn zombie_persisted_but_not_acked_is_fail() {
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.http_outcomes.push(ack("book", 4, 1));
        obs.http_outcomes.push(err("noodles", 10));
        obs.db_state = Some(DbState {
            orders: vec![row(1, "book", 4), row(2, "noodles", 10)],
            stock: vec![],
        });
        assert!(matches!(Durable.drive(&obs), Verdict::Fail { .. }));
    }

    #[test]
    fn acked_row_with_wrong_fields_is_fail() {
        // Ack for (book, 4) but DB persisted (book, 999).
        let mut obs = Observations::empty();
        obs.kill = Some(fired_kill());
        obs.http_outcomes.push(ack("book", 4, 1));
        obs.db_state = Some(DbState {
            orders: vec![row(1, "book", 999)],
            stock: vec![],
        });
        assert!(matches!(Durable.drive(&obs), Verdict::Fail { .. }));
    }
}
