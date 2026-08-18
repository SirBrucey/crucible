//! The types Crucible's components share: the schema a plugin advertises, the
//! plan a scenario lowers to, what a run observes, the verdicts drawn from it,
//! and the runner/worker message protocol.

pub mod fault;
pub mod ipc;
pub mod learned;
pub mod observer;
pub mod plan;
pub mod proxy_log;
pub mod schedule;
pub mod schema;
pub mod verdict;

use std::time::Duration;

/// The longest a scenario may give its fleet to settle. Every schedule waits it
/// out, so it bounds what a campaign costs.
pub const MAX_CONSISTENT_WITHIN: Duration = Duration::from_secs(30);
