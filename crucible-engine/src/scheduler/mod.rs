//! Schedule generation.

pub mod burst;
pub mod recovery;

pub use burst::BurstScheduler;
pub use recovery::RecoveryScheduler;

use crucible_core::schedule::Schedule;

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
