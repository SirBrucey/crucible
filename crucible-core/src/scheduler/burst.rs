//! Burst scheduler: enumerate the fault anchors the learn pass derived, one
//! schedule per `(service, direction, Kth-packet)` anchor. The learn pass does
//! the burst clustering (see
//! [`crate::proxy_log::service_profiles_from_sessions`]); this just turns each
//! anchor into a schedule. Schedules are emitted round-robin
//! across `(service, direction)` so a budget-truncated campaign samples every
//! edge evenly rather than exhausting one service and never reaching another.

use crucible_protocol::{Direction, ServiceProfile};

use super::{Schedule, Scheduler};

pub struct BurstScheduler {
    total: usize,
    schedules: std::vec::IntoIter<Schedule>,
}

impl BurstScheduler {
    /// Build one schedule per anchor the learn pass produced, round-robin across
    /// `(service, direction)` so a truncated campaign covers every edge.
    #[must_use]
    pub fn new(profiles: &[ServiceProfile]) -> Self {
        let anchored: Vec<(&str, Direction, &[u32])> = profiles
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
        let mut next_id: u32 = 0;
        for i in 0..max_len {
            for (service, direction, anchors) in &anchored {
                if let Some(&k) = anchors.get(i) {
                    schedules.push(Schedule {
                        schedule_id: next_id,
                        service: (*service).to_string(),
                        direction: *direction,
                        fault_packet_index: k,
                        payload: Vec::new(),
                    });
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

impl Scheduler for BurstScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(service: &str, c2u: Vec<u32>, u2c: Vec<u32>) -> ServiceProfile {
        ServiceProfile {
            service: service.into(),
            client_to_upstream: c2u,
            upstream_to_client: u2c,
        }
    }

    #[test]
    fn empty_profiles_yield_nothing() {
        let mut s = BurstScheduler::new(&[]);
        assert_eq!(s.total(), 0);
        assert!(s.next().is_none());
    }

    #[test]
    fn one_schedule_per_anchor() {
        let s = BurstScheduler::new(&[profile("db", vec![2, 3], vec![1])]);
        assert_eq!(s.total(), 3);
    }

    #[test]
    fn both_directions_are_scheduled() {
        let scheduler = BurstScheduler::new(&[profile("db", vec![1], vec![2])]);
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert!(
            all.iter()
                .any(|s| s.direction == Direction::ClientToUpstream)
        );
        assert!(
            all.iter()
                .any(|s| s.direction == Direction::UpstreamToClient)
        );
    }

    #[test]
    fn emission_is_round_robin_across_services() {
        let scheduler = BurstScheduler::new(&[
            profile("api", vec![1], vec![]),
            profile("db", vec![1], vec![]),
        ]);
        let first_two: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .take(2)
        .map(|s| s.service)
        .collect();
        assert!(first_two.contains(&"api".to_string()));
        assert!(first_two.contains(&"db".to_string()));
    }
}
