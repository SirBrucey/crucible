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
    /// Runner sends a schedule for the worker to execute.
    Schedule {
        /// Correlation id, referenced by the matching `RunResult`.
        schedule_id: u32,
        /// Serialized schedule spec.
        payload: Vec<u8>,
    },
}
