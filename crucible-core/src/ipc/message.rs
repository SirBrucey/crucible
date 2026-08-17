pub use crucible_protocol::{FaultReport, ServiceProfile, Session};

use crate::{learned::Learned, schedule::Schedule};

/// Messages passed from one of the worker processes to the main runner.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WorkerToRunner {
    /// Handshake: Workers initiate a connection by sending a HELLO to register to the runner.
    Hello {
        /// Version of the worker
        worker_version: String,
        /// Workers unique identifier
        worker_id: u32,
    },
    /// Worker signals it is ready for the runner to send work.
    Ready,
    /// Worker reports the outcome of executing a schedule.
    RunResult {
        /// Correlation id, matching the [`Schedule`] this result is for.
        schedule_id: u32,
        /// Outcome of the run, carrying the driver's explanation.
        verdict: Verdict,
    },
    /// Worker returns what the fault-free run found out about the fleet.
    SessionCatalogue(Learned),
    /// Worker emits an observational event for the runner to record.
    Event(WorkerEvent),
    /// Periodic liveness ping the worker sends while it works, so the runner's
    /// watchdog can tell a slow-but-alive worker from a hung or dead one.
    Heartbeat,
}

/// Observational events streamed from a worker to the runner.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WorkerEvent {
    /// Free-form log line from the worker.
    Log(String),
    /// What a schedule's fault did to the fleet.
    Fault(FaultReport),
}

/// Outcome of running a schedule. Non-`Pass` verdicts carry the reason the
/// driver reached them, for the journal and report.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Verdict {
    /// The invariant held.
    Pass,
    /// The invariant was violated.
    Fail { reason: String },
    /// Neither passed nor failed within the run budget.
    Inconclusive { reason: String },
}

/// Messages passed from the main runner to one of the worker processes.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RunnerToWorker {
    /// Handshake: Runner acknowledges the HELLO sent from a worker and the connection is
    /// established.
    HelloAck {
        /// Version of the runner
        runner_version: String,
    },
    /// Runner sends a schedule for the worker to run. A schedule with no faults
    /// is the fault-free run, which answers with a `SessionCatalogue`; any other
    /// answers with a `RunResult`.
    Run(Schedule),
}
