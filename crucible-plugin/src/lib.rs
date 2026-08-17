//! The plugin contract: the per-role plugin traits, the protocol a plugin
//! speaks when it runs as its own process, and the loop that answers it.
//!
//! # Publishing a plugin
//!
//! A plugin depends on this crate and nothing else of crucible's:
//!
//! ```toml
//! [dependencies]
//! crucible-plugin = "0.1"
//! ```
//!
//! It implements the trait for the part it plays and hands itself to the loop:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     crucible_plugin::serve_observer::<Dns>().await?;
//!     Ok(())
//! }
//! ```
//!
//! That builds one binary. Installing it is putting that binary in a plugin
//! directory, and crucible finds it by asking what it is.
//!
//! The `framework` feature adds what crucible needs of plugins.

pub mod error;
pub mod protocol;
pub mod role;
pub mod serve;

#[cfg(feature = "framework")]
pub mod builtin;
#[cfg(feature = "framework")]
pub mod discovery;
#[cfg(feature = "framework")]
pub mod external;
#[cfg(feature = "framework")]
pub mod registry;

/// The pieces of a plan a plugin is handed, and the vocabulary it declares
/// itself in. Re-exported so writing a plugin means depending on this crate and
/// nothing else of crucible's.
pub use crucible_core::{plan, schema};
pub use error::Error;
#[cfg(feature = "framework")]
pub use registry::Registry;
pub use role::{
    Action, BoxFuture, Deployment, DeploymentRuntime, Driver, DriverRuntime, Faults, Kill,
    Observer, ObserverRuntime, Query, Substrate, Targeted,
};
pub use serve::serve_observer;
