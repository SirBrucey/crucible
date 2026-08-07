//! The plugin contract: the per-role plugin traits, the registry that resolves
//! a name to a plugin, and the protocol a plugin speaks when it runs as its own
//! process rather than being compiled in.

pub mod builtin;
pub mod discovery;
pub mod error;
pub mod external;
pub mod protocol;
pub mod registry;
pub mod role;
pub mod serve;

pub use error::Error;
pub use registry::Registry;
pub use role::{
    Action, Deployment, DeploymentRuntime, Driver, DriverRuntime, FaultPrimitives, Observer,
    ObserverRuntime, Query, Targeted,
};
pub use serve::serve_observer;
