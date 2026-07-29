//! Burst scheduler: for each burst of packets a service exchanged (per
//! direction), anchor kills at the burst's edges, just before its first
//! packet, at its midpoint, and just after its last packet. Anchors are packet
//! counts, so a kill fires relative to observed traffic rather than a wall
//! clock. Schedules are emitted round-robin across `(service, direction)` so a
//! budget-truncated campaign samples every edge evenly rather than exhausting
//! one service and never reaching another.

use std::collections::BTreeSet;

use crucible_protocol::{Direction, ServiceProfile};

use super::{Schedule, Scheduler};

/// Consecutive packets more than this far apart start a new burst.
const BURST_GAP_NS: u128 = 20_000_000; // 20 ms

pub struct BurstScheduler {
    total: usize,
    schedules: std::vec::IntoIter<Schedule>,
}

impl BurstScheduler {
    /// Build schedules from the per-service packet timestamps Learn observed.
    /// Each direction's packets are clustered into bursts; each burst yields a
    /// before / during / after anchor, deduped per direction.
    pub fn new(profiles: &[ServiceProfile]) -> Self {
        // (service, direction) -> sorted, deduped anchor packet counts.
        let mut anchored: Vec<(String, Direction, Vec<u32>)> = Vec::new();
        for profile in profiles {
            for (direction, packets) in [
                (Direction::ClientToUpstream, &profile.client_to_upstream),
                (Direction::UpstreamToClient, &profile.upstream_to_client),
            ] {
                let anchors = burst_anchors(packets);
                if !anchors.is_empty() {
                    anchored.push((profile.service.clone(), direction, anchors));
                }
            }
        }

        // Round-robin across (service, direction): emit each edge's Nth anchor
        // before moving to its N+1th, so a truncated campaign covers every edge.
        let max_len = anchored.iter().map(|(_, _, a)| a.len()).max().unwrap_or(0);
        let mut schedules: Vec<Schedule> = Vec::new();
        let mut next_id: u32 = 0;
        for i in 0..max_len {
            for (service, direction, anchors) in &anchored {
                if let Some(&k) = anchors.get(i) {
                    schedules.push(Schedule {
                        schedule_id: next_id,
                        service: service.clone(),
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
    pub fn total(&self) -> usize {
        self.total
    }
}

impl Scheduler for BurstScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
}

/// Cluster `packets` (sorted timestamps) into bursts by inter-packet gap and
/// return the before / during / after anchor packet counts, sorted and deduped.
/// A count `K` means "freeze once `K` packets have crossed": `K = first - 1`
/// lands just before a burst, `K = last` just after it.
fn burst_anchors(packets: &[u128]) -> Vec<u32> {
    let n = packets.len();
    if n == 0 {
        return Vec::new();
    }
    let mut anchors: BTreeSet<u32> = BTreeSet::new();
    let mut start = 0usize; // 0-based index of the current burst's first packet
    for j in 0..n {
        let ends_burst = j + 1 == n || packets[j + 1] - packets[j] > BURST_GAP_NS;
        if ends_burst {
            // 1-based packet counts; a single learn run should never approach u32.
            let first_count = u32::try_from(start + 1).expect("packet count fits in u32");
            let last_count = u32::try_from(j + 1).expect("packet count fits in u32");
            anchors.insert(first_count - 1); // before
            anchors.insert(u32::midpoint(first_count, last_count)); // during
            anchors.insert(last_count); // after
            start = j + 1;
        }
    }
    anchors.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(service: &str, c2u: Vec<u128>, u2c: Vec<u128>) -> ServiceProfile {
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
    fn single_packet_burst_anchors_before_and_after() {
        // One packet is a one-packet burst: before (K=0) and after (K=1); the
        // midpoint coincides with after and dedupes away.
        assert_eq!(burst_anchors(&[10_000_000]), vec![0, 1]);
    }

    #[test]
    fn a_gap_splits_bursts_and_shares_the_boundary_anchor() {
        // Two single-packet bursts 40ms apart: {0,1} from the first, {1,2} from
        // the second; K=1 (after A / before B) dedupes.
        assert_eq!(burst_anchors(&[10_000_000, 50_000_000]), vec![0, 1, 2]);
    }

    #[test]
    fn contiguous_packets_are_one_burst() {
        // Three packets 1ms apart: one burst [1..3] -> before 0, mid 2, after 3.
        assert_eq!(
            burst_anchors(&[10_000_000, 11_000_000, 12_000_000]),
            vec![0, 2, 3]
        );
    }

    #[test]
    fn both_directions_are_scheduled() {
        let scheduler = BurstScheduler::new(&[profile("db", vec![10_000_000], vec![20_000_000])]);
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
            profile("api", vec![10_000_000], vec![]),
            profile("db", vec![10_000_000], vec![]),
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
