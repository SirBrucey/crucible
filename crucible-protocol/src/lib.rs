//! Shared wire types used across Crucible's components.

mod kill;
mod proxy;
mod session;

use std::time::{SystemTime, UNIX_EPOCH};

pub use kill::{KillMissReason, KillReport, KillResult};
pub use proxy::{ConnEvent, ConnEventKind, ConnId, Direction};
pub use session::{Session, WriteRecord};

/// Nanoseconds per histogram bin in `ServiceProfile`.
pub const HISTOGRAM_BIN_NS: u128 = 10_000_000; // 10 ms

use serde::{Deserialize, Serialize};

/// Per-service byte-over-time histogram derived from a Learn run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ServiceProfile {
    pub service: String,
    /// Sparse: only bins with bytes > 0. Each entry is
    /// `(bin_offset_ns_from_scenario_start, total_bytes_in_bin)`.
    pub bins: Vec<(u128, u64)>,
}

pub fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_nanos()
}
