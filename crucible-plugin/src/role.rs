//! The per-role plugin traits. A plugin crate implements one or more, and each
//! advertises the schema for its role and supplies the code that runs it.
//!
//! Every role is a pair. The static trait carries what is known without an
//! instance: the plugin's name, its schema, and how a piece of a plan binds to
//! the plugin's own types. The runtime trait is object-safe, so the framework
//! can hold plugins whose types it cannot name.

use std::{future::Future, net::SocketAddr, pin::Pin};

use crucible_core::{
    observer::SessionObserver,
    plan,
    schema::{AttrSchema, OpSig},
    verdict::Outcome,
};

use crate::error::Error;

/// A future returned across the erased plugin boundary.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A deployment plugin: it advertises the attributes a `service { ... }` body of
/// its kind accepts, and binds them to its own configuration.
pub trait Deployment {
    /// Stable identifier used to select this plugin.
    const NAME: &'static str;
    /// This plugin's configuration for one service.
    type Config;
    type Error: std::error::Error + Send + Sync + 'static;

    fn attr_schema() -> AttrSchema;

    /// Bind a service's validated attributes to this plugin's configuration.
    ///
    /// # Errors
    /// Errors if the attributes satisfy the schema but the plugin still cannot
    /// use them, such as a port outside the range it can bind.
    fn bind(service: &plan::Service) -> Result<Self::Config, Self::Error>;
}

/// The faults a schedule drives against a live replica. Separate from the
/// replica's lifecycle, because a schedule only ever needs these.
pub trait FaultPrimitives: Send + Sync {
    /// Arm the fault anchor at scenario start, so the replica freezes once the
    /// target reaches the anchored point.
    fn arm_anchor(&self) -> BoxFuture<'_, Result<(), Error>>;
    /// Release the freeze.
    fn resume(&self) -> BoxFuture<'_, Result<(), Error>>;
    /// Kill the named service, reporting the wall-clock nanoseconds when it died.
    fn kill(&self, service: &str) -> BoxFuture<'_, Result<u128, Error>>;
    /// Bring the named service back, reporting when it became ready.
    fn restart(&self, service: &str) -> BoxFuture<'_, Result<u128, Error>>;
}

/// A live fleet replica: bring it up, probe it, fault it, and remove it.
pub trait DeploymentRuntime: FaultPrimitives {
    fn setup(&mut self) -> BoxFuture<'_, Result<(), Error>>;
    fn wait_ready(&self) -> BoxFuture<'_, Result<(), Error>>;
    fn teardown(&mut self) -> BoxFuture<'_, Result<(), Error>>;

    /// Where the named service can be reached, once the replica is up.
    fn endpoint(&self, service: &str) -> Option<SocketAddr>;

    /// Start streaming what the replica's substrate sees on the wire. The
    /// deployment runs that substrate, so only it can open the stream. The
    /// observer runs until shut down, so dropping one strands its tasks.
    #[must_use]
    fn start_session_observer(&self) -> SessionObserver;
}

/// A driver plugin: it advertises the action operations a `do { ... }` step can
/// invoke, and binds a step to its own action type.
pub trait Driver {
    /// Stable identifier used to select this plugin.
    const NAME: &'static str;
    /// One step of a plan, in this plugin's own terms.
    type Action;
    type Error: std::error::Error + Send + Sync + 'static;

    fn signatures() -> Vec<OpSig>;

    /// Bind a step to an action this driver can run.
    ///
    /// # Errors
    /// Errors if the step names an operation this driver does not run, or
    /// carries arguments it cannot use.
    fn bind(step: &plan::Step) -> Result<Self::Action, Self::Error>;
}

/// A driver, ready to run steps.
pub trait DriverRuntime: Send + Sync {
    /// Bind a step to a runnable action, without a live fleet.
    ///
    /// # Errors
    /// Errors if the step does not bind to an operation this driver runs.
    fn prepare(&self, step: &plan::Step) -> Result<Box<dyn Action>, Error>;
}

/// One bound step, runnable against the service it names.
pub trait Action: Send + Sync {
    /// The service this action runs against.
    fn target(&self) -> &str;

    /// Run the action, reporting whether the system took responsibility for it.
    fn run(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<Outcome, Error>>;
}

/// An observer plugin: it advertises the observables an `expect { ... }`
/// predicate can read.
pub trait Observer {
    /// Stable identifier used to select this plugin.
    const NAME: &'static str;

    fn signatures() -> Vec<OpSig>;
}
