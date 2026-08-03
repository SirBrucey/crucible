//! The plugin contract: the per-role plugin traits and the in-process registry
//! of the first-party plugins.

pub mod builtin;
pub mod error;
pub mod registry;
pub mod role;

pub use error::Error;
pub use registry::Registry;
pub use role::{
    Action, Deployment, DeploymentRuntime, Driver, DriverRuntime, FaultPrimitives, Observer,
    ObserverRuntime, Query, Targeted,
};
