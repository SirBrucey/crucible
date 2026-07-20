//! Docker-backed [`Deployment`] implementation.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use bollard::{
    Docker as DockerClient,
    models::{
        ContainerCreateBody, EndpointSettings, HostConfig, NetworkCreateRequest, NetworkingConfig,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
        StartContainerOptions,
    },
};
use futures_util::TryStreamExt;
use tokio::{net::TcpStream, time::timeout};

use super::Deployment;
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

pub struct Docker {
    client: DockerClient,
    network: String,
    fleet: &'static Fleet,
    endpoints: HashMap<String, SocketAddr>,
}

impl Docker {
    pub fn new(worker_id: u32, fleet: &'static Fleet) -> Result<Self> {
        let client = DockerClient::connect_with_socket_defaults()?;
        Ok(Self {
            client,
            network: format!("crucible-{worker_id}"),
            fleet,
            endpoints: HashMap::new(),
        })
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    async fn start_service(&mut self, service: &Service) -> Result<()> {
        ensure_image(&self.client, service.image).await?;

        let container_name = format!("{}-{}", self.network, service.name);
        let exposed_port = format!("{}/tcp", service.port);

        let mut endpoints_config = HashMap::new();
        endpoints_config.insert(
            self.network.clone(),
            EndpointSettings {
                aliases: Some(vec![service.name.to_string()]),
                ..Default::default()
            },
        );

        let config = ContainerCreateBody {
            image: Some(service.image.to_string()),
            exposed_ports: Some(vec![exposed_port.clone()]),
            env: (!service.env.is_empty())
                .then(|| service.env.iter().map(|e| (*e).to_string()).collect()),
            host_config: Some(HostConfig {
                publish_all_ports: Some(true),
                ..Default::default()
            }),
            networking_config: Some(NetworkingConfig {
                endpoints_config: Some(endpoints_config),
            }),
            ..Default::default()
        };

        self.client
            .create_container(
                Some(
                    CreateContainerOptionsBuilder::default()
                        .name(&container_name)
                        .build(),
                ),
                config,
            )
            .await?;

        self.client
            .start_container(&container_name, None::<StartContainerOptions>)
            .await?;

        let host_port = published_port(&self.client, &container_name, service.port).await?;
        self.endpoints.insert(
            service.name.to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), host_port),
        );
        Ok(())
    }
}

impl Deployment for Docker {
    type Error = Error;

    async fn setup(&mut self) -> Result<()> {
        self.teardown().await?;

        self.client
            .create_network(NetworkCreateRequest {
                name: self.network.clone(),
                ..Default::default()
            })
            .await?;

        for service in self.fleet.services {
            self.start_service(service).await?;
        }
        Ok(())
    }

    async fn wait_ready(&self) -> Result<()> {
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

    async fn teardown(&mut self) -> Result<()> {
        let opts = RemoveContainerOptionsBuilder::default().force(true).build();
        let mut failures = TeardownFailures::new();
        for service in self.fleet.services {
            let name = format!("{}-{}", self.network, service.name);
            match self
                .client
                .remove_container(&name, Some(opts.clone()))
                .await
            {
                Ok(()) => {}
                Err(e) if is_not_found(&e) => {}
                Err(e) => failures.append_container(name, e.to_string()),
            }
        }
        self.endpoints.clear();
        match self.client.remove_network(&self.network).await {
            Ok(()) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => failures.set_network(e.to_string()),
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::TeardownIncomplete(failures))
        }
    }

    fn endpoint(&self, name: &str) -> Option<SocketAddr> {
        self.endpoints.get(name).copied()
    }
}

fn is_not_found(e: &bollard::errors::Error) -> bool {
    matches!(
        e,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

async fn ensure_image(docker: &DockerClient, image: &str) -> Result<()> {
    if docker.inspect_image(image).await.is_ok() {
        return Ok(());
    }
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

async fn published_port(
    docker: &DockerClient,
    container: &str,
    container_port: u16,
) -> Result<u16> {
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
    use super::*;

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

    const LIFECYCLE_TEST_FLEET: Fleet = Fleet {
        services: &[
            Service {
                name: "web-a",
                image: "nginx:alpine",
                port: 80,
                env: &[],
            },
            Service {
                name: "web-b",
                image: "nginx:alpine",
                port: 80,
                env: &[],
            },
        ],
    };

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    async fn deployment_lifecycle_brings_up_and_tears_down_every_service() {
        let worker_id = std::process::id();
        let mut docker = Docker::new(worker_id, &LIFECYCLE_TEST_FLEET).expect("connect to docker");

        let setup_outcome = docker.setup().await;
        for service in LIFECYCLE_TEST_FLEET.services {
            assert!(
                docker.endpoint(service.name).is_some(),
                "expected endpoint for `{}`",
                service.name
            );
        }

        let wait_outcome = docker.wait_ready().await;

        let teardown_outcome = docker.teardown().await;

        setup_outcome.expect("setup should succeed");
        wait_outcome.expect("every service should become ready");
        teardown_outcome.expect("teardown should succeed");

        for service in LIFECYCLE_TEST_FLEET.services {
            assert!(
                docker.endpoint(service.name).is_none(),
                "endpoint for `{}` should be cleared after teardown",
                service.name
            );
        }
    }

    const ORPHAN_TEST_FLEET: Fleet = Fleet {
        services: &[Service {
            name: "orphan",
            image: "nginx:alpine",
            port: 80,
            env: &[],
        }],
    };

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    async fn setup_sweeps_an_orphan_container_from_a_prior_run() {
        let worker_id = std::process::id().wrapping_add(1);
        let orphan_name = format!("crucible-{worker_id}-orphan");

        let client = DockerClient::connect_with_socket_defaults().expect("connect to docker");
        ensure_image(&client, "nginx:alpine")
            .await
            .expect("pull nginx");
        client
            .create_container(
                Some(
                    CreateContainerOptionsBuilder::default()
                        .name(&orphan_name)
                        .build(),
                ),
                ContainerCreateBody {
                    image: Some("nginx:alpine".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("plant orphan");

        let mut docker = Docker::new(worker_id, &ORPHAN_TEST_FLEET).expect("connect to docker");
        let setup_outcome = docker.setup().await;
        let teardown_outcome = docker.teardown().await;

        setup_outcome.expect("setup should sweep the orphan and succeed");
        teardown_outcome.expect("teardown should succeed");

        let inspect = client.inspect_container(&orphan_name, None).await;
        assert!(
            inspect.is_err(),
            "orphan container should have been removed"
        );
    }
}
