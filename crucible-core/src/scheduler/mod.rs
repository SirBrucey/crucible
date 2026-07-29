//! Schedule generation.

pub mod burst;

pub use burst::BurstScheduler;

use crucible_protocol::Direction;

use crate::ipc::RunnerToWorker;

/// A schedule the runner hands to a worker to execute. The fault fires once the
/// target service's proxy has observed `fault_packet_index` packets on
/// `direction`, so it lands relative to observed traffic rather than a clock.
#[derive(Clone)]
pub struct Schedule {
    pub schedule_id: u32,
    pub service: String,
    /// Direction whose packets the anchor counts.
    pub direction: Direction,
    /// Freeze and kill once this many packets have crossed on `direction`.
    pub fault_packet_index: u32,
    pub payload: Vec<u8>,
}

impl From<Schedule> for RunnerToWorker {
    fn from(schedule: Schedule) -> Self {
        RunnerToWorker::Schedule {
            schedule_id: schedule.schedule_id,
            service: schedule.service,
            direction: schedule.direction,
            fault_packet_index: schedule.fault_packet_index,
            payload: schedule.payload,
        }
    }
}

/// Produces schedules for the runner to dispatch.
pub trait Scheduler: Send + Sync {
    /// Return the next schedule, or `None` when the scheduler is exhausted.
    fn next(&mut self) -> Option<Schedule>;
}
