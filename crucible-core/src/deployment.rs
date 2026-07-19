//! Docker deployment: brings up the fleet, tears it down.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use bollard::{
    Docker,
    models::{ContainerCreateBody, HostConfig, NetworkCreateRequest},
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, StartContainerOptions,
    },
};
use futures_util::TryStreamExt;
use tokio::{net::TcpStream, time::timeout};

use crate::fleet::{Fleet, Service};

const READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Docker(#[from] bollard::errors::Error),
    #[error("service `{name}` did not publish port {port}")]
    MissingPort { name: String, port: u16 },
    #[error("service `{name}` at {addr} did not become ready within {timeout:?}")]
    ReadinessTimeout {
        name: String,
        addr: SocketAddr,
        timeout: Duration,
    },
    #[error("teardown incomplete: {0}")]
    TeardownIncomplete(TeardownFailures),
}

/// Items teardown could not remove, paired with the daemon's reason.
#[derive(Debug, Default)]
pub struct TeardownFailures {
    containers: Vec<(String, String)>,
    network: Option<String>,
}

impl TeardownFailures {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append_container(&mut self, name: impl Into<String>, reason: impl Into<String>) {
        self.containers.push((name.into(), reason.into()));
    }

    pub fn set_network(&mut self, reason: impl Into<String>) {
        self.network = Some(reason.into());
    }

    pub fn is_empty(&self) -> bool {
        self.containers.is_empty() && self.network.is_none()
    }

    pub fn containers(&self) -> &[(String, String)] {
        &self.containers
    }

    pub fn network(&self) -> Option<&str> {
        self.network.as_deref()
    }
}

impl std::fmt::Display for TeardownFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (name, reason) in &self.containers {
            if !first {
                f.write_str("; ")?;
            }
            write!(f, "container `{name}`: {reason}")?;
            first = false;
        }
        if let Some(reason) = &self.network {
            if !first {
                f.write_str("; ")?;
            }
            write!(f, "network: {reason}")?;
        }
        Ok(())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Deployment {
    docker: Docker,
    network: String,
    containers: Vec<String>,
    endpoints: HashMap<String, SocketAddr>,
}

impl Deployment {
    pub fn new(worker_id: u32) -> Result<Self> {
        let docker = Docker::connect_with_socket_defaults()?;
        Ok(Self {
            docker,
            network: format!("crucible-{worker_id}"),
            containers: Vec::new(),
            endpoints: HashMap::new(),
        })
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    pub fn endpoint(&self, name: &str) -> Option<SocketAddr> {
        self.endpoints.get(name).copied()
    }

    /// Create the per-worker network, pull each image, start every service on the network,
    /// and record the host port the daemon published for each service's exposed port.
    pub async fn setup(&mut self, fleet: &Fleet) -> Result<()> {
        self.docker
            .create_network(NetworkCreateRequest {
                name: self.network.clone(),
                ..Default::default()
            })
            .await?;

        for service in fleet.services {
            self.start_service(service).await?;
        }
        Ok(())
    }

    /// Wait until every service accepts a TCP connection on its published port,
    /// with per-service exponential backoff and a shared overall deadline.
    pub async fn wait_ready(&self) -> Result<()> {
        let probes = self.endpoints.iter().map(|(name, addr)| async move {
            let start = tokio::time::Instant::now();
            let mut backoff = INITIAL_BACKOFF;
            loop {
                if start.elapsed() >= READINESS_TIMEOUT {
                    return Err(Error::ReadinessTimeout {
                        name: name.clone(),
                        addr: *addr,
                        timeout: READINESS_TIMEOUT,
                    });
                }
                if let Ok(Ok(_)) = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                    return Ok(());
                }
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
            }
        });
        futures_util::future::try_join_all(probes).await?;
        Ok(())
    }

    async fn start_service(&mut self, service: &Service) -> Result<()> {
        pull_image(&self.docker, service.image).await?;

        let container_name = format!("{}-{}", self.network, service.name);
        let exposed_port = format!("{}/tcp", service.port);

        let config = ContainerCreateBody {
            image: Some(service.image.to_string()),
            exposed_ports: Some(vec![exposed_port.clone()]),
            env: (!service.env.is_empty())
                .then(|| service.env.iter().map(|e| (*e).to_string()).collect()),
            host_config: Some(HostConfig {
                network_mode: Some(self.network.clone()),
                publish_all_ports: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };

        self.docker
            .create_container(
                Some(
                    CreateContainerOptionsBuilder::default()
                        .name(&container_name)
                        .build(),
                ),
                config,
            )
            .await?;
        self.containers.push(container_name.clone());

        self.docker
            .start_container(&container_name, None::<StartContainerOptions>)
            .await?;

        let host_port = published_port(&self.docker, &container_name, service.port).await?;
        self.endpoints.insert(
            service.name.to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), host_port),
        );
        Ok(())
    }
}

async fn pull_image(docker: &Docker, image: &str) -> Result<()> {
    docker
        .create_image(
            Some(
                CreateImageOptionsBuilder::default()
                    .from_image(image)
                    .build(),
            ),
            None,
            None,
        )
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn published_port(docker: &Docker, container: &str, container_port: u16) -> Result<u16> {
    let inspect = docker.inspect_container(container, None).await?;
    let ports = inspect
        .network_settings
        .and_then(|ns| ns.ports)
        .ok_or_else(|| Error::MissingPort {
            name: container.to_string(),
            port: container_port,
        })?;
    let key = format!("{container_port}/tcp");
    let bindings = ports
        .get(&key)
        .and_then(Option::as_ref)
        .and_then(|v| v.first());
    let host_port = bindings
        .and_then(|pb| pb.host_port.as_ref())
        .ok_or_else(|| Error::MissingPort {
            name: container.to_string(),
            port: container_port,
        })?;
    host_port.parse::<u16>().map_err(|_| Error::MissingPort {
        name: container.to_string(),
        port: container_port,
    })
}

#[cfg(test)]
mod tests {
    use bollard::query_parameters::RemoveContainerOptionsBuilder;

    use super::*;
    use crate::fleet;

    #[test]
    fn teardown_failures_is_empty_by_default() {
        assert!(TeardownFailures::new().is_empty());
    }

    #[test]
    fn teardown_failures_appended_items_show_up_in_display() {
        let mut failures = TeardownFailures::new();
        failures.append_container("api", "boom");
        failures.append_container("db", "gone");
        failures.set_network("network still has endpoints");
        assert!(!failures.is_empty());
        assert_eq!(
            failures.to_string(),
            "container `api`: boom; container `db`: gone; network: network still has endpoints"
        );
    }

    #[test]
    fn teardown_failures_display_with_only_network() {
        let mut failures = TeardownFailures::new();
        failures.set_network("still in use");
        assert_eq!(failures.to_string(), "network: still in use");
    }

    #[test]
    fn teardown_failures_display_with_only_containers() {
        let mut failures = TeardownFailures::new();
        failures.append_container("api", "boom");
        assert_eq!(failures.to_string(), "container `api`: boom");
    }

    /// Best-effort teardown for the setup-only test until `Deployment::teardown` lands.
    async fn manual_cleanup(deployment: &Deployment) {
        for name in &deployment.containers {
            let _ = deployment
                .docker
                .remove_container(
                    name,
                    Some(RemoveContainerOptionsBuilder::default().force(true).build()),
                )
                .await;
        }
        let _ = deployment.docker.remove_network(&deployment.network).await;
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    async fn setup_and_wait_ready_bring_up_every_service() {
        let worker_id = std::process::id();
        let mut deployment = Deployment::new(worker_id).expect("connect to docker");

        let setup_outcome = deployment.setup(&fleet::EXAMPLE).await;
        for service in fleet::EXAMPLE.services {
            let endpoint = deployment.endpoint(service.name);
            assert!(
                endpoint.is_some(),
                "expected endpoint for `{}`",
                service.name
            );
        }
        assert_eq!(deployment.containers.len(), fleet::EXAMPLE.services.len());

        let wait_outcome = deployment.wait_ready().await;

        manual_cleanup(&deployment).await;
        setup_outcome.expect("setup should succeed");
        wait_outcome.expect("every service should become ready");
    }
}
