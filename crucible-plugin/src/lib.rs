//! The plugin contract: the per-role plugin traits and the in-process registry
//! of the first-party plugins.

pub mod builtin;
pub mod registry;
pub mod role;

pub use registry::Registry;
pub use role::{Deployment, Driver, Observer};
