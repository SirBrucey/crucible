//! Burst scheduler: for each work burst it observed, emit `SAMPLES_PER_BURST`
//! evenly-spaced kill offsets. The schedule count is decoupled from the run
//! budget (the runner caps wall-clock, see `crucible/src/main.rs`); schedules
//! are emitted round-robin across bursts so a budget-truncated campaign still
//! samples every burst evenly rather than exhausting one service and never
//! reaching another.

use std::collections::BTreeMap;

use crucible_protocol::{HISTOGRAM_BIN_NS, ServiceProfile};

use super::{Schedule, Scheduler};

const BURST_BYTE_THRESHOLD: u64 = 64;
const SAMPLES_PER_BURST: usize = 5;

pub struct BurstScheduler {
    total: usize,
    schedules: std::vec::IntoIter<Schedule>,
}

impl BurstScheduler {
    /// Build schedules from per-service byte-over-time profiles produced by
    /// Learn. Bins above `BURST_BYTE_THRESHOLD` are clustered into contiguous
    /// bursts; each burst gets `SAMPLES_PER_BURST` evenly-spaced kill offsets.
    pub fn new(profiles: &[ServiceProfile]) -> Self {
        // Flatten to (service, burst) in a stable order (BTreeMap sorts by
        // service) so round-robin emission is deterministic.
        let bursts: Vec<(String, Burst)> = detect_bursts(profiles)
            .into_iter()
            .flat_map(|(service, bursts)| bursts.into_iter().map(move |b| (service.clone(), b)))
            .collect();

        let mut schedules: Vec<Schedule> = Vec::with_capacity(bursts.len() * SAMPLES_PER_BURST);
        let mut next_id: u32 = 0;
        for sample in 0..SAMPLES_PER_BURST {
            for (service, burst) in &bursts {
                let duration = burst.end_ns.saturating_sub(burst.start_ns).max(1);
                let offset = burst.start_ns + duration * sample as u128 / SAMPLES_PER_BURST as u128;
                schedules.push(Schedule {
                    schedule_id: next_id,
                    service: service.clone(),
                    fault_offset_ns: offset,
                    payload: Vec::new(),
                });
                next_id += 1;
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
        let mut s = BurstScheduler::new(&[]);
        assert_eq!(s.total(), 0);
        assert!(s.next().is_none());
    }

    #[test]
    fn single_burst_gets_samples_per_burst() {
        // One 10ms bin above threshold.
        let scheduler = BurstScheduler::new(&[profile("db", vec![(10, 500)])]);
        assert_eq!(scheduler.total(), SAMPLES_PER_BURST);
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert_eq!(all.len(), SAMPLES_PER_BURST);
        assert!(all.iter().all(|s| s.service == "db"));
    }

    #[test]
    fn silent_bin_splits_bursts() {
        let scheduler = BurstScheduler::new(&[profile("db", vec![(10, 500), (20, 500)])]);
        assert_eq!(scheduler.total(), SAMPLES_PER_BURST * 2);
    }

    #[test]
    fn below_threshold_bins_ignored() {
        let mut s = BurstScheduler::new(&[profile("db", vec![(10, 5)])]);
        assert!(s.next().is_none());
    }

    #[test]
    fn emission_is_round_robin_across_bursts() {
        // Two services, one burst each: the first two schedules should cover
        // both services, so a truncated campaign samples both.
        let scheduler = BurstScheduler::new(&[
            profile("api", vec![(10, 500)]),
            profile("db", vec![(10, 500)]),
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
