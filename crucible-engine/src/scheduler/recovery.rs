//! Recovery scheduler: one schedule per `(service, way of degrading it)`. The
//! fleet runs the whole scenario degraded and is put back afterwards, so what it
//! accepted while down is what it has to have caught up on.

use crucible_core::{
    fault::{Fault, Losing},
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
    /// Degrade each of the fleet's services, one schedule per way of degrading
    /// it. Nothing is anchored to a packet: the fault is in place before the
    /// scenario starts.
    #[must_use]
    pub fn new(
        fleet: &plan::Fleet,
        scenario: &plan::Scenario,
        learned: &Learned,
        ways: &[Losing],
        first_id: u32,
    ) -> Self {
        let mut schedules = Vec::new();
        let mut next_id = first_id;
        for service in &fleet.services {
            for by in ways {
                schedules.push(Schedule::faulted(
                    next_id,
                    fleet.clone(),
                    scenario.steps.clone(),
                    scenario.checks.clone(),
                    Fault::Recovers {
                        service: service.name.clone(),
                        by: *by,
                    },
                    learned.trajectory.clone(),
                    scenario.consistent_within,
                ));
                next_id += 1;
            }
        }
        Self {
            total: schedules.len(),
            schedules: schedules.into_iter(),
        }
    }

    /// How many schedules degrading this fleet takes, before building any. The
    /// campaign needs it to reserve their cost against its budget.
    #[must_use]
    pub fn count(fleet: &plan::Fleet, ways: &[Losing]) -> usize {
        fleet.services.len() * ways.len()
    }

    #[must_use]
    pub fn total(&self) -> usize {
        self.total
    }
}

impl Scheduler for RecoveryScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
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
    use crucible_protocol::ServiceProfile;

    use super::*;
    use crate::scheduler::fixture;

    fn carrying(packets: u32) -> crucible_protocol::Burst {
        crucible_protocol::Burst {
            start: 0,
            mid: packets.div_ceil(2),
            end: packets,
            packets,
        }
    }

    /// A fault-free run that observed traffic for `seen` and nothing else.
    fn learned(seen: &[&str]) -> Learned {
        Learned {
            profiles: seen
                .iter()
                .map(|service| ServiceProfile {
                    service: (*service).to_string(),
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

    #[test]
    fn one_schedule_per_service_per_way_of_degrading_it() {
        let fleet = fixture::fleet().services.len();
        assert_eq!(schedules(&[Losing::Kill], &[]).len(), fleet);
        assert_eq!(
            schedules(&[Losing::Kill, Losing::Cut], &[]).len(),
            fleet * 2
        );
    }

    #[test]
    fn a_service_the_run_saw_no_traffic_for_is_still_degraded() {
        let degraded: Vec<String> = schedules(&[Losing::Kill], &[])
            .iter()
            .map(|schedule| {
                schedule
                    .fault
                    .as_ref()
                    .expect("a recovery run faults")
                    .service()
                    .to_owned()
            })
            .collect();
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
