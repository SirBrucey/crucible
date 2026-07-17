/// Messages passed from the one of the worker processes to the main runner.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum WorkerToRunner {
    /// Handshake: Workers initiate a connection by sending a HELLO to register to the runner.
    Hello {
        /// Version of the worker
        version: String,
        /// Workers unique identifier
        worker_id: u32,
    },
}

/// Messages passed from the main runner to one of the worker processes.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum RunnerToWorker {
    /// Handshake: Runner acknowledges the HELLO sent from a worker and the connection is
    /// established.
    HelloAck {
        /// Version of the runner
        version: String,
    },
}
