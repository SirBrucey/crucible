//! Recovery scheduler: one schedule per way of degrading the fleet, held for the
//! whole scenario and put back afterwards, so what it accepted while down is
//! what it has to have caught up on.
//!
//! Losing a service degrades every edge it holds; losing one edge leaves both
//! its ends running, so a service reachable another way carries on.

use crucible_core::{
    fault::{Fault, Losing, Taking},
    learned::Learned,
    plan,
    schedule::Schedule,
    verdict::Invariant,
};

use super::Scheduler;

pub struct RecoveryScheduler {
    total: usize,
    schedules: std::vec::IntoIter<Schedule>,
}

impl RecoveryScheduler {
    /// Degrade the fleet every way it can be degraded. Nothing is anchored to a
    /// packet: the fault is in place before the scenario starts.
    #[must_use]
    pub fn new(
        fleet: &plan::Fleet,
        scenario: &plan::Scenario,
        learned: &Learned,
        ways: &[Losing],
        first_id: u32,
    ) -> Self {
        let mut next_id = first_id;
        let schedules: Vec<Schedule> = degradations(fleet, learned, ways)
            .map(|by| {
                let schedule = Schedule::faulted(
                    next_id,
                    fleet.clone(),
                    scenario.steps.clone(),
                    scenario.checks.clone(),
                    Fault::Recovers { by },
                    learned.trajectory.clone(),
                    scenario.consistent_within,
                );
                next_id += 1;
                schedule
            })
            .collect();
        Self {
            total: schedules.len(),
            schedules: schedules.into_iter(),
        }
    }

    /// How many schedules degrading this fleet takes, before building any. The
    /// campaign needs it to reserve their cost against its budget.
    #[must_use]
    pub fn count(fleet: &plan::Fleet, learned: &Learned, ways: &[Losing]) -> usize {
        degradations(fleet, learned, ways).count()
    }
}

impl Scheduler for RecoveryScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }

    fn total(&self) -> usize {
        self.total
    }
}

/// Every way this fleet can be held degraded: each service the author declared,
/// and each edge the fault-free run saw carry traffic.
fn degradations<'a>(
    fleet: &'a plan::Fleet,
    learned: &'a Learned,
    ways: &'a [Losing],
) -> impl Iterator<Item = Taking> + 'a {
    ways.iter()
        .flat_map(move |by| -> Box<dyn Iterator<Item = Taking> + 'a> {
            match by {
                Losing::Kill => Box::new(
                    fleet
                        .services
                        .iter()
                        .map(|service| Taking::Kill(service.name.clone())),
                ),
                Losing::Cut => Box::new(
                    learned
                        .profiles
                        .iter()
                        .map(|profile| Taking::Cut(profile.edge.clone())),
                ),
            }
        })
}

/// The ways this scheduler can degrade a fleet, out of what the campaign found
/// it could do.
#[must_use]
pub fn ways(testable: &[(Invariant, Vec<crucible_core::fault::Primitive>)]) -> Vec<Losing> {
    testable
        .iter()
        .filter(|(invariant, _)| *invariant == Invariant::Recovers)
        .flat_map(|(_, ways)| ways.iter().filter_map(|by| Losing::try_from(*by).ok()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crucible_core::fault::Primitive;
    use crucible_protocol::{Burst, Edge, EdgeProfile};

    use super::*;
    use crate::scheduler::fixture;

    fn carrying(packets: u32) -> Burst {
        Burst {
            start: 0,
            mid: packets.div_ceil(2),
            end: packets,
            packets,
        }
    }

    /// A fault-free run that saw traffic on the edges into `seen` and nothing
    /// else.
    fn learned(seen: &[&str]) -> Learned {
        Learned {
            profiles: seen
                .iter()
                .map(|upstream| EdgeProfile {
                    placements: Vec::new(),
                    edge: Edge {
                        client: None,
                        upstream: (*upstream).to_string(),
                    },
                    client_to_upstream: vec![carrying(1)],
                    upstream_to_client: vec![carrying(1)],
                })
                .collect(),
            trajectory: Vec::new(),
            primitives: BTreeSet::from([Primitive::Kill, Primitive::Cut]),
        }
    }

    fn schedules(ways: &[Losing], seen: &[&str]) -> Vec<Schedule> {
        let mut s = RecoveryScheduler::new(
            &fixture::fleet(),
            &fixture::scenario(),
            &learned(seen),
            ways,
            1,
        );
        std::iter::from_fn(move || s.next()).collect()
    }

    /// What each schedule degrades.
    fn degraded(ways: &[Losing], seen: &[&str]) -> Vec<String> {
        schedules(ways, seen)
            .iter()
            .map(|schedule| {
                schedule
                    .fault
                    .as_ref()
                    .expect("a recovery run faults")
                    .taking()
                    .target()
            })
            .collect()
    }

    #[test]
    fn one_schedule_per_service_to_kill() {
        let fleet = fixture::fleet().services.len();
        assert_eq!(schedules(&[Losing::Kill], &["db"]).len(), fleet);
    }

    /// A cut is per edge, and which edges exist is what the run observed, so a
    /// fleet whose services never spoke offers nothing to cut.
    #[test]
    fn one_schedule_per_observed_edge_to_cut() {
        assert_eq!(schedules(&[Losing::Cut], &["db", "api"]).len(), 2);
        assert!(schedules(&[Losing::Cut], &[]).is_empty());
    }

    #[test]
    fn a_service_the_run_saw_no_traffic_for_is_still_degraded() {
        let degraded = degraded(&[Losing::Kill], &[]);
        for service in &fixture::fleet().services {
            assert!(degraded.contains(&service.name), "{}", service.name);
        }
    }

    #[test]
    fn a_recovery_schedule_is_not_anchored_to_a_packet() {
        let schedules = schedules(&[Losing::Kill], &[]);
        let fault = schedules[0].fault.as_ref().expect("a recovery run faults");
        assert_eq!(fault.anchor(), None);
    }
}
