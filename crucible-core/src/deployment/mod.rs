//! Deployment plugin abstraction: bring a fleet replica up, wait for readiness, tear it down.

pub mod docker;

use std::{future::Future, net::SocketAddr};

pub use docker::Docker;

/// A per-worker fleet replica the orchestrator can bring up, probe, and remove.
pub trait Deployment {
    type Error;

    fn setup(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn wait_ready(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn teardown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn endpoint(&self, name: &str) -> Option<SocketAddr>;
}
