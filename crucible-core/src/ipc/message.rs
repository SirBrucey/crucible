use crate::verdict::Invariant;

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
        /// Correlation id, matching the `Schedule` this result is for.
        schedule_id: u32,
        /// Outcome of the run.
        verdict: Verdict,
    },
    /// Worker returns the sessions observed by the sidecars during a `Learn` run.
    SessionCatalogue { sessions: Vec<Session> },
    /// Worker emits an observational event for the runner to record.
    Event(WorkerEvent),
}

/// One TCP session observed by a sidecar proxy during a scenario run.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Session {
    /// Fleet service the sidecar fronts.
    pub service: String,
    /// Proxy-local connection id.
    pub conn_id: u64,
    /// Peer address as reported by the sidecar.
    pub peer: String,
    /// Wall-clock nanoseconds since the Unix epoch when the sidecar accepted the connection.
    pub opened_ns: u128,
    /// Wall-clock nanoseconds since the Unix epoch when the sidecar closed the connection.
    pub closed_ns: u128,
    /// Bytes forwarded from the client to the upstream over the session lifetime.
    pub bytes_client_to_upstream: u64,
    /// Bytes forwarded from the upstream to the client over the session lifetime.
    pub bytes_upstream_to_client: u64,
}

/// Observational events streamed from a worker to the runner.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WorkerEvent {
    /// Free-form log line from the worker.
    Log(String),
}

/// Outcome of running a schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Verdict {
    /// All invariants held.
    Pass,
    /// An invariant was violated.
    Fail,
    /// Neither passed nor failed within the run budget.
    Inconclusive,
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
    /// Runner asks the worker to run the scenario once with faults disabled and
    /// return the sessions the sidecars observed as a `SessionCatalogue`.
    Learn,
    /// Runner sends a schedule for the worker to execute.
    Schedule {
        /// Correlation id, referenced by the matching `RunResult`.
        schedule_id: u32,
        /// Which invariant this schedule targets.
        invariant: Invariant,
        /// Serialized schedule spec.
        #[serde(with = "base64_bytes")]
        payload: Vec<u8>,
    },
    /// Runner tells the worker there is no more work and to exit cleanly.
    Shutdown,
}

mod base64_bytes {
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        BASE64_STANDARD.encode(bytes).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        BASE64_STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}
