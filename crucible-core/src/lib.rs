//! The types Crucible's components share: the schema a plugin advertises, the
//! plan a scenario lowers to, the fleet spec, what a run observes, the verdicts
//! drawn from it, and the runner/worker message protocol.

pub mod fleet;
pub mod ipc;
pub mod observer;
pub mod plan;
pub mod proxy_log;
pub mod scenario;
pub mod schema;
pub mod verdict;

use std::time::Duration;

/// How long a fleet is given to settle after a fault before its state is read.
/// A scenario's `consistent_within` supersedes this once plans drive the run.
pub const HEAL_BUDGET: Duration = Duration::from_secs(15);
