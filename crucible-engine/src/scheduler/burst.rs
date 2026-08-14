//! Burst scheduler: enumerate the fault anchors the learn pass derived, one
//! schedule per `(service, direction, Kth-packet)` anchor. The learn pass does
//! the burst clustering (see
//! [`crucible_core::proxy_log::service_profiles_from_sessions`]); this just turns each
//! anchor into a schedule. Schedules are emitted round-robin
//! across `(service, direction)` so a budget-truncated campaign samples every
//! edge evenly rather than exhausting one service and never reaching another.

use crucible_protocol::Direction;

use crucible_core::{
    fault::{Anchor, Fault},
    learned::Learned,
    plan,
    schedule::Schedule,
    verdict::Invariant,
};

use super::Scheduler;

pub struct BurstScheduler {
    total: usize,
    schedules: std::vec::IntoIter<Schedule>,
}

impl BurstScheduler {
    /// Build one schedule per anchor the learn pass produced, round-robin across
    /// `(service, direction)` so a truncated campaign covers every edge. Each
    /// carries the work to run as well as the fault, so a worker needs nothing
    /// else to run it.
    #[must_use]
    pub fn new(
        fleet: &plan::Fleet,
        scenario: &plan::Scenario,
        learned: &Learned,
        testable: &[Invariant],
    ) -> Self {
        let anchored: Vec<(&str, Direction, &[u32])> = learned
            .profiles
            .iter()
            .flat_map(|p| {
                [
                    (
                        p.service.as_str(),
                        Direction::ClientToUpstream,
                        p.client_to_upstream.as_slice(),
                    ),
                    (
                        p.service.as_str(),
                        Direction::UpstreamToClient,
                        p.upstream_to_client.as_slice(),
                    ),
                ]
            })
            .filter(|(_, _, anchors)| !anchors.is_empty())
            .collect();

        let max_len = anchored.iter().map(|(_, _, a)| a.len()).max().unwrap_or(0);
        let mut schedules: Vec<Schedule> = Vec::new();
        let mut next_id: u32 = Schedule::LEARN_ID + 1;
        for i in 0..max_len {
            for (service, direction, anchors) in &anchored {
                let Some(&k) = anchors.get(i) else { continue };
                let anchor = Anchor {
                    service: (*service).to_string(),
                    direction: *direction,
                    k,
                };
                for fault in testable.iter().filter_map(|i| faults(*i, &anchor)) {
                    schedules.push(Schedule::faulted(
                        next_id,
                        fleet.clone(),
                        scenario.steps.clone(),
                        scenario.checks.clone(),
                        fault,
                        learned.trajectory.clone(),
                    ));
                    next_id += 1;
                }
            }
        }
        Self {
            total: schedules.len(),
            schedules: schedules.into_iter(),
        }
    }

    /// Total schedules generated, for coverage reporting against how many the
    /// runner actually dispatched within its wall-clock budget.
    #[must_use]
    pub fn total(&self) -> usize {
        self.total
    }
}

/// The fault that puts `invariant` under pressure at `anchor`, where a burst is
/// the right shape for it. Recovery degrades the fleet from the start rather
/// than part way through, so a burst has nowhere to put it.
fn faults(invariant: Invariant, anchor: &Anchor) -> Option<Fault> {
    match invariant {
        Invariant::Durable => Some(Fault::Durable(anchor.clone())),
        Invariant::Recovers | Invariant::Idempotent | Invariant::Converges => None,
    }
}

impl Scheduler for BurstScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crucible_core::fault::Primitive;
    use crucible_protocol::ServiceProfile;

    use super::*;

    fn profile(service: &str, c2u: Vec<u32>, u2c: Vec<u32>) -> ServiceProfile {
        ServiceProfile {
            service: service.into(),
            client_to_upstream: c2u,
            upstream_to_client: u2c,
        }
    }

    /// A campaign against a fleet that can be killed, so durability is on.
    fn scheduler(profiles: &[ServiceProfile]) -> BurstScheduler {
        let plan = plan::example();
        let learned = Learned {
            profiles: profiles.to_vec(),
            trajectory: Vec::new(),
            primitives: BTreeSet::from([Primitive::Kill]),
        };
        BurstScheduler::new(
            &plan.fleet,
            &plan.scenarios[0],
            &learned,
            &[Invariant::Durable],
        )
    }

    /// Where a burst schedule's fault lands.
    fn fault(schedule: &Schedule) -> &Anchor {
        schedule
            .fault
            .as_ref()
            .expect("a burst schedule always faults")
            .anchor()
    }

    /// A fleet whose loaded plugins cannot break anything cannot schedule any
    /// faults, so the campaign is a fault-free run only.
    #[test]
    fn nothing_testable_yields_nothing() {
        let plan = plan::example();
        let learned = Learned {
            profiles: vec![profile("db", vec![1, 2], vec![3])],
            trajectory: Vec::new(),
            primitives: BTreeSet::new(),
        };
        let mut s = BurstScheduler::new(&plan.fleet, &plan.scenarios[0], &learned, &[]);
        assert_eq!(s.total(), 0);
        assert!(s.next().is_none());
    }

    #[test]
    fn empty_profiles_yield_nothing() {
        let mut s = scheduler(&[]);
        assert_eq!(s.total(), 0);
        assert!(s.next().is_none());
    }

    #[test]
    fn one_schedule_per_anchor() {
        let s = scheduler(&[profile("db", vec![2, 3], vec![1])]);
        assert_eq!(s.total(), 3);
    }

    #[test]
    fn both_directions_are_scheduled() {
        let scheduler = scheduler(&[profile("db", vec![1], vec![2])]);
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert!(
            all.iter()
                .any(|s| fault(s).direction == Direction::ClientToUpstream)
        );
        assert!(
            all.iter()
                .any(|s| fault(s).direction == Direction::UpstreamToClient)
        );
    }

    #[test]
    fn emission_is_round_robin_across_services() {
        let scheduler = scheduler(&[
            profile("api", vec![1], vec![]),
            profile("db", vec![1], vec![]),
        ]);
        let first_two: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .take(2)
        .map(|s| fault(&s).service.clone())
        .collect();
        assert!(first_two.contains(&"api".to_string()));
        assert!(first_two.contains(&"db".to_string()));
    }
}
