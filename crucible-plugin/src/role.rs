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

/// Hold the fleet still at the anchored moment and let it go again. Every
/// deployment provides this: it is how a fault is placed precisely, not a fault
/// in itself.
pub trait Freeze: Send + Sync {
    /// Arm the fault anchor at scenario start, so the replica freezes once the
    /// target reaches the anchored point.
    fn arm_anchor(&self) -> BoxFuture<'_, Result<(), Error>>;
    /// Release the freeze.
    fn resume(&self) -> BoxFuture<'_, Result<(), Error>>;
}

/// What a plugin can do to a fleet beyond driving it. Each accessor answers with
/// a provider when the plugin can do that thing, so nothing can claim a
/// primitive it has not written.
pub trait Faults: Send + Sync {
    fn kills(&self) -> Option<&dyn Kill> {
        None
    }
}

/// Take a service out of the fleet and put it back. Both halves together: a
/// service that cannot come back leaves a dead fleet rather than a fault.
pub trait Kill: Send + Sync {
    /// Kill the named service, reporting the wall-clock nanoseconds when it died.
    fn kill(&self, service: &str) -> BoxFuture<'_, Result<u128, Error>>;
    /// Bring the named service back, reporting when it became ready.
    fn restart(&self, service: &str) -> BoxFuture<'_, Result<u128, Error>>;
}

/// A live fleet replica: bring it up, probe it, fault it, and remove it.
pub trait DeploymentRuntime: Freeze + Faults {
    fn setup(&mut self) -> BoxFuture<'_, Result<(), Error>>;
    fn wait_ready(&self) -> BoxFuture<'_, Result<(), Error>>;
    fn teardown(&mut self) -> BoxFuture<'_, Result<(), Error>>;

    /// The data plane address for a service's kind, where the test traffic runs.
    fn endpoint(&self, service: &str, kind: &str) -> Option<SocketAddr>;

    /// The control plane address for a service's kind.
    fn control_endpoint(&self, service: &str, kind: &str) -> Option<SocketAddr>;

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

    /// What a service speaking this plugin declares beyond its bring-up: a
    /// plugin that needs nothing of the author declares nothing.
    #[must_use]
    fn attr_schema() -> AttrSchema {
        AttrSchema::new(Vec::new())
    }

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

/// Something bound to one service of the fleet, speaking one of the kinds that
/// service declares. A service may answer several kinds on several ports, so
/// which one this speaks is what says where to reach it.
pub trait Targeted {
    /// The service this is bound to.
    fn target(&self) -> &str;
    /// The kind this speaks to it.
    fn kind(&self) -> &str;
}

/// One bound step, runnable against the service it names.
pub trait Action: Targeted + Send + Sync {
    /// Run the action, reporting whether the system took responsibility for it.
    fn run(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<Outcome, Error>>;
}

/// An observer plugin: it advertises the observables an `expect { ... }`
/// predicate can read.
pub trait Observer: ObserverRuntime + Sized {
    /// Stable identifier used to select this plugin.
    const NAME: &'static str;
    /// One check of a plan, in this plugin's own terms.
    type Query;
    type Error: std::error::Error + Send + Sync + 'static;

    fn signatures() -> Vec<OpSig>;

    /// Construct an observer for `service`, configured from the attributes that
    /// service declares for this plugin.
    fn runtime(service: &plan::Service) -> Self;

    /// Bind a check to a query this observer can answer.
    ///
    /// # Errors
    /// Errors if the check names an observable this observer does not read, or
    /// filters it in a way it cannot express.
    fn bind(check: &plan::Check) -> Result<Self::Query, Self::Error>;

    /// What a service speaking this plugin declares beyond its bring-up: a
    /// plugin that needs nothing of the author declares nothing.
    #[must_use]
    fn attr_schema() -> AttrSchema {
        AttrSchema::new(Vec::new())
    }
}

/// An observer, ready to read settled state.
pub trait ObserverRuntime: Send + Sync {
    /// Bind a check to a runnable query, without a live fleet. Asking an
    /// observer that runs as its own process is a round trip, so this is where
    /// a check it cannot answer is refused, before a replica is spent on it.
    ///
    /// # Errors
    /// Errors if the check does not bind to an observable this observer reads.
    fn prepare<'a>(
        &'a self,
        check: &'a plan::Check,
    ) -> BoxFuture<'a, Result<Box<dyn Query>, Error>>;
}

/// One bound check, readable against the service it names. It yields what was
/// observed; the framework compares that against what the check expects.
pub trait Query: Targeted + Send + Sync {
    fn read(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<plan::Value, Error>>;
}
