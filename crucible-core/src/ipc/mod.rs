pub mod codec;
mod message;

use std::time::Duration;

pub use message::{EdgeProfile, RunnerToWorker, Session, Verdict, WorkerEvent, WorkerToRunner};

/// How often a busy worker emits a [`WorkerToRunner::Heartbeat`], so the runner
/// can tell a slow-but-alive worker from a hung or dead one.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
/// How long the runner waits for any frame (a heartbeat or a real message)
/// before deeming the worker unresponsive. Several intervals, to tolerate
/// scheduling and IPC jitter.
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
