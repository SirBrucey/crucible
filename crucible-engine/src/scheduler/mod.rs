//! Schedule generation.

pub mod burst;
pub mod recovery;

#[cfg(test)]
mod fixture;

pub use burst::BurstScheduler;
pub use recovery::RecoveryScheduler;

use std::time::Duration;

use crucible_core::schedule::Schedule;

/// What a campaign has left to spend, and what one schedule costs it.
///
/// A scenario stating no budget has none of this: it runs every schedule its
/// schedulers produce.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    pub left: Duration,
    /// Measured by the fault-free run, plus the settling every schedule waits.
    pub cost: Duration,
    /// Schedules running at once, so a round of this many costs `cost`.
    pub concurrency: usize,
}

impl Budget {
    /// Whether `n` more schedules fit.
    #[must_use]
    pub fn fits(&self, n: usize) -> bool {
        runtime(n, self.cost, self.concurrency) <= self.left
    }

    /// The most of `most` items that fit, where each costs `each` schedules.
    #[must_use]
    pub fn affords(&self, most: usize, each: usize) -> usize {
        (1..=most)
            .take_while(|n| self.fits(n.saturating_mul(each)))
            .count()
    }

    /// What is left after running `n` schedules.
    #[must_use]
    pub fn after(&self, n: usize) -> Self {
        Self {
            left: self
                .left
                .saturating_sub(runtime(n, self.cost, self.concurrency)),
            ..*self
        }
    }
}

/// How long `n` schedules costing `cost` each take, `concurrency` of them at a
/// time. A count too large to run in any real time saturates, which reads as
/// longer than any budget allows.
#[must_use]
pub fn runtime(n: usize, cost: Duration, concurrency: usize) -> Duration {
    let rounds = u32::try_from(n.div_ceil(concurrency.max(1))).unwrap_or(u32::MAX);
    cost.saturating_mul(rounds)
}

/// Produces schedules for the runner to dispatch.
pub trait Scheduler: Send + Sync {
    /// Return the next schedule, or `None` when the scheduler is exhausted.
    fn next(&mut self) -> Option<Schedule>;
}

/// Every schedule of the first, then every schedule of the second, so a campaign
/// draws from more than one way of picking faults.
pub struct Chain<A, B>(pub A, pub B);

impl<A: Scheduler, B: Scheduler> Scheduler for Chain<A, B> {
    fn next(&mut self) -> Option<Schedule> {
        self.0.next().or_else(|| self.1.next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minute to spend, on schedules costing ten seconds each.
    fn budget(concurrency: usize) -> Budget {
        Budget {
            left: Duration::from_mins(1),
            cost: Duration::from_secs(10),
            concurrency,
        }
    }

    #[test]
    fn schedules_run_at_once_share_a_round() {
        assert!(budget(1).fits(6));
        assert!(!budget(1).fits(7));
        assert!(budget(4).fits(24));
        assert!(!budget(4).fits(25));
    }

    #[test]
    fn a_part_filled_round_costs_a_whole_one() {
        assert!(budget(4).fits(21), "six rounds, the last of one schedule");
        assert!(!budget(4).fits(26), "a seventh round to run two schedules");
    }

    #[test]
    fn what_is_afforded_is_capped_by_what_there_is_to_run() {
        assert_eq!(budget(1).affords(10, 1), 6);
        assert_eq!(budget(1).affords(3, 1), 3);
        assert_eq!(budget(1).affords(10, 2), 3, "two schedules apiece");
    }

    #[test]
    fn spending_leaves_the_rest() {
        assert!(budget(1).after(4).fits(2));
        assert!(!budget(1).after(4).fits(3));
    }

    #[test]
    fn a_budget_spent_affords_nothing() {
        assert_eq!(budget(1).after(99).affords(10, 1), 0);
    }
}
