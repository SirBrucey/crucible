//! Recovery scheduler: one schedule per way of degrading the fleet, held for the
//! whole scenario and put back afterwards, so what it accepted while down is
//! what it has to have caught up on.
//!
//! Drive a service degrades every edge it holds; losing one edge leaves both
//! its ends running, so a service reachable another way carries on.

use std::collections::BTreeSet;

use crucible_core::{
    fault::{By, Drive, Fault},
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
        ways: &[Drive],
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
                    Fault::throughout(by),
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
    pub fn count(fleet: &plan::Fleet, learned: &Learned, ways: &[Drive]) -> usize {
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
/// and each link the fault-free run saw carry traffic.
fn degradations<'a>(
    fleet: &'a plan::Fleet,
    learned: &'a Learned,
    ways: &'a [Drive],
) -> impl Iterator<Item = By> + 'a {
    ways.iter()
        .flat_map(move |by| -> Box<dyn Iterator<Item = By> + 'a> {
            match by {
                Drive::Kill => Box::new(
                    fleet
                        .services
                        .iter()
                        .map(|service| By::Kill(service.name.clone())),
                ),
                Drive::Cut => Box::new(
                    learned
                        .profiles
                        .iter()
                        .filter(|profile| profile.edge.within_fleet())
                        .map(|profile| By::Cut(profile.edge.clone())),
                ),
                // Changing what crosses needs something to cross, and a fleet
                // held down from the start never sends anything.
                Drive::Repeat | Drive::Reorder | Drive::Drop => Box::new(std::iter::empty()),
            }
        })
}

/// The ways this scheduler can degrade a fleet, out of what the campaign found
/// it could do. Recovery is shown by holding the fleet down, so only what can
/// be held counts.
#[must_use]
pub fn ways(available: &BTreeSet<crucible_core::fault::Primitive>) -> Vec<Drive> {
    Invariant::Recovers
        .showable(available)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|by| Drive::try_from(by).ok())
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
    /// else, each dialled by the fleet's own `api`.
    fn learned(seen: &[&str]) -> Learned {
        Learned {
            profiles: seen
                .iter()
                .map(|upstream| profile(dialled(upstream)))
                .collect(),
            trajectory: crucible_core::verdict::Trajectory::default(),
            primitives: BTreeSet::from([Primitive::Kill, Primitive::Cut]),
        }
    }

    /// The edge `api` holds to `upstream`.
    fn dialled(upstream: &str) -> Edge {
        Edge {
            client: Some("api".to_string()),
            upstream: upstream.to_string(),
        }
    }

    fn profile(edge: Edge) -> EdgeProfile {
        EdgeProfile {
            placements: Vec::new(),
            edge,
            client_to_upstream: vec![carrying(1)],
            upstream_to_client: vec![carrying(1)],
        }
    }

    fn schedules(ways: &[Drive], seen: &[&str]) -> Vec<Schedule> {
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
    fn degraded(ways: &[Drive], seen: &[&str]) -> Vec<String> {
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
        assert_eq!(schedules(&[Drive::Kill], &["db"]).len(), fleet);
    }

    /// A cut is per edge, and which edges exist is what the run observed, so a
    /// fleet whose services never spoke offers nothing to cut.
    #[test]
    fn one_schedule_per_observed_edge_to_cut() {
        assert_eq!(schedules(&[Drive::Cut], &["db", "api"]).len(), 2);
        assert!(schedules(&[Drive::Cut], &[]).is_empty());
    }

    /// The framework reaches in over an edge of its own to drive the scenario.
    /// Holding that down runs the fleet against no traffic at all.
    #[test]
    fn the_edge_the_scenario_is_driven_over_is_not_degraded() {
        let mut learned = learned(&["db"]);
        learned.profiles.push(profile(Edge {
            client: None,
            upstream: "api".to_string(),
        }));
        let mut s = RecoveryScheduler::new(
            &fixture::fleet(),
            &fixture::scenario(),
            &learned,
            &[Drive::Cut],
            1,
        );
        let cut: Vec<String> = std::iter::from_fn(move || s.next())
            .map(|schedule| {
                schedule
                    .fault
                    .expect("a recovery run faults")
                    .taking()
                    .target()
            })
            .collect();
        assert_eq!(cut, ["api -> db"]);
    }

    #[test]
    fn a_service_the_run_saw_no_traffic_for_is_still_degraded() {
        let degraded = degraded(&[Drive::Kill], &[]);
        for service in &fixture::fleet().services {
            assert!(degraded.contains(&service.name), "{}", service.name);
        }
    }

    #[test]
    fn a_recovery_schedule_is_not_anchored_to_a_packet() {
        let schedules = schedules(&[Drive::Kill], &[]);
        let fault = schedules[0].fault.as_ref().expect("a recovery run faults");
        assert_eq!(fault.anchor(), None);
    }
}
