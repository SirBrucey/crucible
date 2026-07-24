//! Schedule generation.

pub mod burst;

pub use burst::BurstScheduler;

use crate::ipc::RunnerToWorker;

/// A schedule the runner hands to a worker to execute.
pub struct Schedule {
    pub schedule_id: u32,
    pub service: String,
    /// Nanoseconds from scenario start at which the fault should fire.
    pub fault_offset_ns: u128,
    pub payload: Vec<u8>,
}

impl From<Schedule> for RunnerToWorker {
    fn from(schedule: Schedule) -> Self {
        RunnerToWorker::Schedule {
            schedule_id: schedule.schedule_id,
            service: schedule.service,
            fault_offset_ns: schedule.fault_offset_ns,
            payload: schedule.payload,
        }
    }
}

/// Produces schedules for the runner to dispatch.
pub trait Scheduler: Send + Sync {
    /// Return the next schedule, or `None` when the scheduler is exhausted.
    fn next(&mut self) -> Option<Schedule>;
}
