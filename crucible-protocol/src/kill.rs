use serde::{Deserialize, Serialize};

use crate::SessionRef;

/// Outcome of the kill primitive for one schedule.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct KillReport {
    pub schedule_id: u32,
    pub session: SessionRef,
    pub result: KillResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum KillResult {
    /// Kill fired.
    Fired {
        /// Sidecar-stamped timestamp of the target session's `Opened` event.
        opened_at_ns: u128,
        /// Wall-clock timestamp of the moment bollard's kill returned.
        killed_at_ns: u128,
        /// The offset the scheduler asked for, in nanoseconds after `Opened`.
        requested_offset_ns: u128,
        /// The offset actually achieved, `killed_at_ns - opened_at_ns`.
        actual_offset_ns: u128,
    },
    /// Kill did not fire.
    Missed(KillMissReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum KillMissReason {
    /// Scenario completed before the target session opened.
    ScenarioEndedBeforeTargetOpened,
    /// Observer's log stream closed before we saw the target.
    ObserverStreamClosed,
    /// bollard's `kill_container` returned an error.
    KillFailed(String),
}
