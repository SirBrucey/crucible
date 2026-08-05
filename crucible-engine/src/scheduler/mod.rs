//! Schedule generation.

pub mod burst;

pub use burst::BurstScheduler;

use crucible_core::schedule::Schedule;

/// Produces schedules for the runner to dispatch.
pub trait Scheduler: Send + Sync {
    /// Return the next schedule, or `None` when the scheduler is exhausted.
    fn next(&mut self) -> Option<Schedule>;
}
