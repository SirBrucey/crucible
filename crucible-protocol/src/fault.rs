use serde::{Deserialize, Serialize};

use crate::{Direction, Primitive};

/// What a schedule's fault did to the fleet.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FaultReport {
    pub schedule_id: u32,
    pub service: String,
    pub result: FaultResult,
}

impl FaultReport {
    #[must_use]
    pub fn fired(
        schedule_id: u32,
        service: impl Into<String>,
        by: Primitive,
        at: At,
        placed_at_ns: u128,
    ) -> Self {
        Self {
            schedule_id,
            service: service.into(),
            result: FaultResult::Fired {
                by,
                at,
                placed_at_ns,
            },
        }
    }

    #[must_use]
    pub fn missed(schedule_id: u32, service: impl Into<String>, reason: FaultMissReason) -> Self {
        Self {
            schedule_id,
            service: service.into(),
            result: FaultResult::Missed(reason),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum FaultResult {
    /// The fault was placed.
    Fired {
        /// What was done to the fleet.
        by: Primitive,
        /// Where in the run it was placed.
        at: At,
        /// Wall-clock nanoseconds when it landed.
        placed_at_ns: u128,
    },
    /// The fault was never placed.
    Missed(FaultMissReason),
}

/// Where in the run a fault was placed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum At {
    /// On one moment in the traffic, so it caught whatever was in flight.
    Moment {
        /// Which way the traffic it was placed on runs.
        direction: Direction,
        /// What the moment was, in the terms of whatever read that edge.
        mark: String,
        /// What faulting there catches.
        why: String,
        /// Nanoseconds from scenario start when it was placed.
        offset_ns: u128,
    },
    /// Imposed before the scenario and lifted after it, so the whole run met a
    /// degraded fleet.
    Throughout,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum FaultMissReason {
    /// Scenario completed before the target reached the anchored packet count.
    ScenarioEndedBeforeAnchor,
    /// The deployment could not place it.
    Failed(String),
}
