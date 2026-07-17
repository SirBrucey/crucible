//! Stub random scheduler.

use super::{Schedule, Scheduler};

/// Yields a fixed number of empty schedules with incrementing ids.
pub struct RandomScheduler {
    remaining: u32,
    next_id: u32,
}

impl RandomScheduler {
    pub fn new(count: u32) -> Self {
        Self {
            remaining: count,
            next_id: 0,
        }
    }
}

impl Scheduler for RandomScheduler {
    fn next(&mut self) -> Option<Schedule> {
        if self.remaining == 0 {
            return None;
        }
        let schedule_id = self.next_id;
        self.next_id += 1;
        self.remaining -= 1;
        Some(Schedule {
            schedule_id,
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
}
