//! Shared wire types used across Crucible's components.

mod kill;
mod proxy;
mod session;

use std::time::{SystemTime, UNIX_EPOCH};

pub use kill::{KillMissReason, KillReport, KillResult};
pub use proxy::{ConnEvent, ConnEventKind, ConnId, Direction};
pub use session::{Session, WriteRecord};

use serde::{Deserialize, Serialize};

/// Per-service packet timestamps observed during a Learn run, split by
/// direction. Each entry is a write's nanoseconds-from-scenario-start, in
/// order. The scheduler clusters these into bursts and anchors kills on the
/// Kth packet of a direction, so faults land relative to observed traffic
/// rather than a wall clock.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ServiceProfile {
    pub service: String,
    /// Client-to-upstream write timestamps (requests reaching the service).
    pub client_to_upstream: Vec<u128>,
    /// Upstream-to-client write timestamps (responses leaving the service).
    pub upstream_to_client: Vec<u128>,
}

pub fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_nanos()
}
