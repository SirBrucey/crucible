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
    /// Degrade each service the fault-free run saw traffic for, one schedule per
    /// way of degrading it. Nothing is anchored to a packet: the fault is in
    /// place before the scenario starts.
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
        for profile in &learned.profiles {
            for by in ways {
                schedules.push(Schedule::faulted(
                    next_id,
                    fleet.clone(),
                    scenario.steps.clone(),
                    scenario.checks.clone(),
                    Fault::Recovers {
                        service: profile.service.clone(),
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

    fn learned(services: &[&str]) -> Learned {
        Learned {
            profiles: services
                .iter()
                .map(|service| ServiceProfile {
                    service: (*service).to_string(),
                    client_to_upstream: vec![1],
                    upstream_to_client: vec![1],
                })
                .collect(),
            trajectory: Vec::new(),
            primitives: BTreeSet::from([Primitive::Kill, Primitive::Cut]),
        }
    }

    #[test]
    fn one_schedule_per_service_per_way_of_degrading_it() {
        let plan = plan::example();
        let s = RecoveryScheduler::new(
            &plan.fleet,
            &plan.scenarios[0],
            &learned(&["api", "db"]),
            &[Losing::Kill, Losing::Cut],
            1,
        );
        assert_eq!(s.total(), 4);
    }

    #[test]
    fn a_recovery_schedule_is_not_anchored_to_a_packet() {
        let plan = plan::example();
        let mut s = RecoveryScheduler::new(
            &plan.fleet,
            &plan.scenarios[0],
            &learned(&["db"]),
            &[Losing::Kill],
            1,
        );
        let fault = s.next().unwrap().fault.expect("a recovery run faults");
        assert_eq!(fault.anchor(), None);
        assert_eq!(fault.service(), "db");
    }
}
