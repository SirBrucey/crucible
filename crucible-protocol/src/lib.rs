//! Shared wire types used across Crucible's components.

mod kill;
mod proxy;
mod session;

use std::time::{SystemTime, UNIX_EPOCH};

pub use kill::{KillMissReason, KillReport, KillResult};
pub use proxy::{ConnEvent, ConnEventKind, ConnId};
pub use session::{Session, SessionRef};

pub fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_nanos()
}
