//! Stub random scheduler.

use std::iter::Cycle;

use strum::IntoEnumIterator;

use super::{Schedule, Scheduler};
use crate::verdict::{Invariant, InvariantIter};

/// Yields a fixed number of empty schedules with incrementing ids, cycling through
/// the four invariants one per schedule.
pub struct RandomScheduler {
    remaining: u32,
    next_id: u32,
    invariants: Cycle<InvariantIter>,
}

impl RandomScheduler {
    pub fn new(count: u32) -> Self {
        Self {
            remaining: count,
            next_id: 0,
            invariants: Invariant::iter().cycle(),
        }
    }
}

impl Scheduler for RandomScheduler {
    fn next(&mut self) -> Option<Schedule> {
        if self.remaining == 0 {
            return None;
        }
        let schedule_id = self.next_id;
        let invariant = self.invariants.next().expect("cycle is infinite");
        self.next_id += 1;
        self.remaining -= 1;
        Some(Schedule {
            schedule_id,
            invariant,
            payload: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yields_count_then_none() {
        let mut scheduler = RandomScheduler::new(3);
        for expected_id in 0..3 {
            let schedule = scheduler.next().unwrap();
            assert_eq!(schedule.schedule_id, expected_id);
            assert!(schedule.payload.is_empty());
        }
        assert!(scheduler.next().is_none());
    }

    #[test]
    fn zero_count_is_empty() {
        assert!(RandomScheduler::new(0).next().is_none());
    }

    #[test]
    fn cycles_through_all_four_invariants() {
        let mut scheduler = RandomScheduler::new(8);
        let invariants: Vec<_> = std::iter::from_fn(|| scheduler.next())
            .map(|s| s.invariant)
            .collect();
        assert_eq!(
            invariants,
            vec![
                Invariant::Idempotent,
                Invariant::Converges,
                Invariant::Durable,
                Invariant::Recovers,
                Invariant::Idempotent,
                Invariant::Converges,
                Invariant::Durable,
                Invariant::Recovers,
            ]
        );
    }
}
