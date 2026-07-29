pub use crucible_protocol::{Direction, KillReport, ServiceProfile, Session};

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
        /// Outcome of the run, carrying the driver's explanation.
        verdict: Verdict,
    },
    /// Worker returns per-service byte-over-time histograms derived from the
    /// Learn run's observed sidecar traffic, already scenario-relative.
    SessionCatalogue { services: Vec<ServiceProfile> },
    /// Worker emits an observational event for the runner to record.
    Event(WorkerEvent),
}

/// Observational events streamed from a worker to the runner.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WorkerEvent {
    /// Free-form log line from the worker.
    Log(String),
    /// Kill primitive outcome for a schedule.
    Kill(KillReport),
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
    /// Runner asks the worker to run the scenario once with faults disabled and
    /// return the sessions the sidecars observed as a `SessionCatalogue`.
    Learn,
    /// Runner sends a schedule for the worker to execute.
    Schedule {
        /// Correlation id, referenced by the matching `RunResult`.
        schedule_id: u32,
        /// Target service for the kill fault.
        service: String,
        /// Direction whose packets the anchor counts.
        direction: Direction,
        /// Freeze and kill once this many packets have crossed on `direction`.
        fault_packet_index: u32,
        /// Serialized schedule spec.
        #[serde(with = "base64_bytes")]
        payload: Vec<u8>,
    },
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
