//! Shared wire types used across Crucible's components.

mod proxy;
mod session;

pub use proxy::{ConnEvent, ConnEventKind, ConnId};
pub use session::Session;
