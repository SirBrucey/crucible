//! Schedule generation.

pub mod random;

pub use random::RandomScheduler;

use crate::{ipc::RunnerToWorker, verdict::Invariant};

/// A schedule the runner hands to a worker to execute.
pub struct Schedule {
    pub schedule_id: u32,
    pub invariant: Invariant,
    pub payload: Vec<u8>,
}

impl From<Schedule> for RunnerToWorker {
    fn from(schedule: Schedule) -> Self {
        RunnerToWorker::Schedule {
            schedule_id: schedule.schedule_id,
            invariant: schedule.invariant,
            payload: schedule.payload,
        }
    }
}

/// Produces schedules for the runner to dispatch.
pub trait Scheduler: Send + Sync {
    /// Return the next schedule, or `None` when the scheduler is exhausted.
    fn next(&mut self) -> Option<Schedule>;
}
