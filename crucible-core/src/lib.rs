//! The types Crucible's components share: the schema a plugin advertises, the
//! plan a scenario lowers to, the fleet spec, what a run observes, the verdicts
//! drawn from it, and the runner/worker message protocol.

pub mod deployment;
pub mod fleet;
pub mod ipc;
pub mod observer;
pub mod plan;
pub mod proxy_log;
pub mod scenario;
pub mod schema;
pub mod verdict;
