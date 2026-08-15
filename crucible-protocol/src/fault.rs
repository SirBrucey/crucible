use serde::{Deserialize, Serialize};

use crate::{Direction, Primitive};

/// What a schedule's fault did to the fleet.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FaultReport {
    pub schedule_id: u32,
    pub service: String,
    pub result: FaultResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum FaultResult {
    /// The fault was placed.
    Fired {
        /// What was done to the fleet.
        by: Primitive,
        /// Direction whose packets the anchor counted.
        requested_direction: Direction,
        /// The packet count on that direction the fault was anchored to.
        requested_packet_index: u32,
        /// Nanoseconds from scenario start when the fault was placed.
        actual_offset_ns: u128,
        /// Wall-clock nanoseconds when it landed.
        placed_at_ns: u128,
    },
    /// The fault was never placed.
    Missed(FaultMissReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum FaultMissReason {
    /// Scenario completed before the target reached the anchored packet count.
    ScenarioEndedBeforeAnchor,
    /// The deployment could not place it.
    Failed(String),
}
