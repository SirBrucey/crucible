//! Burst scheduler: N×T grid where N is a service's work burst and T is a
//! sample within it.

use std::{collections::BTreeMap, time::Duration};

use crucible_protocol::{HISTOGRAM_BIN_NS, ServiceProfile};

use super::{Schedule, Scheduler};

const BURST_BYTE_THRESHOLD: u64 = 64;
const MIN_SAMPLES_PER_BURST: usize = 3;

pub struct BurstScheduler {
    schedules: std::vec::IntoIter<Schedule>,
}

impl BurstScheduler {
    /// Build schedules from per-service byte-over-time profiles produced by
    /// Learn. Bins above `BURST_BYTE_THRESHOLD` are clustered into contiguous
    /// bursts; each burst gets at least `MIN_SAMPLES_PER_BURST` schedules,
    /// bounded by the budget/cost ratio.
    pub fn new(profiles: &[ServiceProfile], run_cost: Duration, total_budget: Duration) -> Self {
        if profiles.is_empty() {
            return Self {
                schedules: Vec::new().into_iter(),
            };
        }
        let bursts_per_service = detect_bursts(profiles);
        if bursts_per_service.is_empty() {
            return Self {
                schedules: Vec::new().into_iter(),
            };
        }
        let total_bursts: usize = bursts_per_service.values().map(Vec::len).sum();
        let budget_ns = total_budget.as_nanos();
        let cost_ns = run_cost.as_nanos().max(1);
        let total_schedules =
            usize::try_from((budget_ns / cost_ns).max(1)).expect("total schedules fits usize");
        let raw_per_burst = total_schedules.saturating_div(total_bursts.max(1));
        let t_per_burst = raw_per_burst.max(MIN_SAMPLES_PER_BURST);

        let mut schedules: Vec<Schedule> = Vec::new();
        let mut next_id: u32 = 0;
        for (service, bursts) in bursts_per_service {
            for burst in bursts {
                let duration = burst.end_ns.saturating_sub(burst.start_ns).max(1);
                for i in 0..t_per_burst {
                    let offset = burst.start_ns + duration * i as u128 / t_per_burst as u128;
                    schedules.push(Schedule {
                        schedule_id: next_id,
                        service: service.clone(),
                        fault_offset_ns: offset,
                        payload: Vec::new(),
                    });
                    next_id += 1;
                }
            }
        }
        Self {
            schedules: schedules.into_iter(),
        }
    }
}

impl Scheduler for BurstScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
}

#[derive(Debug, Clone, Copy)]
struct Burst {
    start_ns: u128,
    end_ns: u128,
}

/// Cluster contiguous "active" bins (bytes > threshold) into bursts. Returns
/// per-service list of bursts covering the half-open range `[start_ns, end_ns)`.
fn detect_bursts(profiles: &[ServiceProfile]) -> BTreeMap<String, Vec<Burst>> {
    let mut out: BTreeMap<String, Vec<Burst>> = BTreeMap::new();
    for profile in profiles {
        let mut bursts: Vec<Burst> = Vec::new();
        let mut current: Option<Burst> = None;
        let mut previous_bin: Option<u128> = None;
        for &(bin, bytes) in &profile.bins {
            if bytes < BURST_BYTE_THRESHOLD {
                continue;
            }
            let start_ns = bin * HISTOGRAM_BIN_NS;
            let end_ns = start_ns + HISTOGRAM_BIN_NS;
            match (&mut current, previous_bin) {
                (Some(active), Some(prev)) if prev + 1 == bin => {
                    active.end_ns = end_ns;
                }
                _ => {
                    if let Some(b) = current.take() {
                        bursts.push(b);
                    }
                    current = Some(Burst { start_ns, end_ns });
                }
            }
            previous_bin = Some(bin);
        }
        if let Some(b) = current.take() {
            bursts.push(b);
        }
        if !bursts.is_empty() {
            out.insert(profile.service.clone(), bursts);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(service: &str, bins: Vec<(u128, u64)>) -> ServiceProfile {
        ServiceProfile {
            service: service.into(),
            bins,
        }
    }

    #[test]
    fn empty_profiles_yield_nothing() {
        let mut s = BurstScheduler::new(&[], Duration::from_millis(10), Duration::from_millis(100));
        assert!(s.next().is_none());
    }

    #[test]
    fn single_burst_becomes_min_samples() {
        // One 10ms bin above threshold.
        let profiles = vec![profile("db", vec![(10, 500)])];
        let scheduler = BurstScheduler::new(
            &profiles,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert_eq!(all.len(), MIN_SAMPLES_PER_BURST);
        for schedule in &all {
            assert_eq!(schedule.service, "db");
        }
    }

    #[test]
    fn silent_bin_splits_bursts() {
        let profiles = vec![profile("db", vec![(10, 500), (20, 500)])];
        let count = std::iter::from_fn({
            let mut s = BurstScheduler::new(
                &profiles,
                Duration::from_millis(10),
                Duration::from_millis(60),
            );
            move || s.next()
        })
        .count();
        assert_eq!(count, MIN_SAMPLES_PER_BURST * 2);
    }

    #[test]
    fn below_threshold_bins_ignored() {
        let profiles = vec![profile("db", vec![(10, 5)])];
        let mut s = BurstScheduler::new(
            &profiles,
            Duration::from_millis(10),
            Duration::from_millis(30),
        );
        assert!(s.next().is_none());
    }
}
