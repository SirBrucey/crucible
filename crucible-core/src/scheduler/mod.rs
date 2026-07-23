//! Schedule generation.

pub mod session_derived;

pub use session_derived::SessionDerivedScheduler;

use crucible_protocol::SessionRef;

use crate::ipc::RunnerToWorker;

/// A schedule the runner hands to a worker to execute.
pub struct Schedule {
    pub schedule_id: u32,
    pub session: SessionRef,
    pub fault_offset_ns: u128,
    pub payload: Vec<u8>,
}

impl From<Schedule> for RunnerToWorker {
    fn from(schedule: Schedule) -> Self {
        RunnerToWorker::Schedule {
            schedule_id: schedule.schedule_id,
            session: schedule.session,
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
