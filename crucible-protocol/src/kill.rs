use serde::{Deserialize, Serialize};

use crate::Direction;

/// Outcome of the kill primitive for one schedule.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct KillReport {
    pub schedule_id: u32,
    pub service: String,
    pub result: KillResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum KillResult {
    /// Kill fired.
    Fired {
        /// Direction whose packets the anchor counted.
        requested_direction: Direction,
        /// The packet count on that direction the kill was anchored to.
        requested_packet_index: u32,
        /// Nanoseconds from scenario start when the kill actually returned.
        actual_offset_ns: u128,
        /// Wall-clock nanoseconds when bollard's kill returned.
        killed_at_ns: u128,
    },
    /// Kill did not fire.
    Missed(KillMissReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum KillMissReason {
    /// Scenario completed before the target reached the anchored packet count.
    ScenarioEndedBeforeAnchor,
    /// bollard's `kill_container` returned an error.
    KillFailed(String),
}
