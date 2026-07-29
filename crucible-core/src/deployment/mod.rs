//! Deployment plugin abstraction: bring a fleet replica up, wait for readiness, tear it down.

pub mod docker;

use std::{future::Future, net::SocketAddr};

pub use docker::{Docker, ProxyAnchor};

/// A per-worker fleet replica the orchestrator can bring up, probe, drive faults
/// against, and remove.
pub trait Deployment {
    type Error;

    fn setup(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn wait_ready(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn teardown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn endpoint(&self, name: &str) -> Option<SocketAddr>;

    /// Arm the proxy's fault anchor at scenario start, so it counts scenario
    /// traffic and freezes the fleet once the target reaches the anchored packet.
    fn arm_anchor(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Release the freeze the proxy is holding on the fleet.
    fn resume_proxy(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Kill the named service. Returns the wall-clock nanoseconds when the kill
    /// returned.
    fn kill_service(&self, name: &str) -> impl Future<Output = Result<u128, Self::Error>> + Send;
    /// Bring the named service back and wait for it to report ready. Returns the
    /// wall-clock nanoseconds when it became ready.
    fn restart_service(&self, name: &str)
    -> impl Future<Output = Result<u128, Self::Error>> + Send;
}
