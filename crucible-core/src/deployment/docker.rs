//! Docker-backed [`Deployment`] implementation.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use bollard::{
    Docker as DockerClient,
    models::{
        ContainerCreateBody, EndpointSettings, HealthConfig, HealthStatusEnum, HostConfig,
        NetworkCreateRequest, NetworkingConfig,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
        StartContainerOptions,
    },
};
use futures_util::TryStreamExt;
use tokio::time::sleep;

use super::Deployment;
use crate::fleet::{Fleet, Service};

const READINESS_TIMEOUT: Duration = Duration::from_mins(1);
const READINESS_POLL: Duration = Duration::from_millis(500);
const HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(1);
const HEALTHCHECK_START_PERIOD: Duration = Duration::from_secs(30);
const PROXY_IMAGE: &str = "crucible-proxy:0.1";
const PROXY_SUFFIX: &str = "proxy";
const BACKING_SUFFIX: &str = "actual";

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

    fn backing_container_name(&self, service: &Service) -> String {
        format!("{}-{}-{}", self.network, service.name, BACKING_SUFFIX)
    }

    fn proxy_container_name(&self, service: &Service) -> String {
        format!("{}-{}-{}", self.network, service.name, PROXY_SUFFIX)
    }

    fn backing_alias(service: &Service) -> String {
        format!("{}-{}", service.name, BACKING_SUFFIX)
    }

    async fn start_service(&mut self, service: &Service) -> Result<()> {
        ensure_image(&self.client, service.image).await?;

        let container_name = self.backing_container_name(service);
        let exposed_port = format!("{}/tcp", service.port);

        let mut endpoints_config = HashMap::new();
        endpoints_config.insert(
            self.network.clone(),
            EndpointSettings {
                aliases: Some(vec![Self::backing_alias(service)]),
                ..Default::default()
            },
        );

        let healthcheck = (!service.healthcheck.is_empty()).then(|| HealthConfig {
            test: Some(
                service
                    .healthcheck
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
            ),
            interval: Some(nanos(HEALTHCHECK_INTERVAL)),
            start_period: Some(nanos(HEALTHCHECK_START_PERIOD)),
            ..Default::default()
        });

        let config = ContainerCreateBody {
            image: Some(service.image.to_string()),
            exposed_ports: Some(vec![exposed_port]),
            env: (!service.env.is_empty())
                .then(|| service.env.iter().map(|e| (*e).to_string()).collect()),
            healthcheck,
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
        Ok(())
    }

    async fn start_proxy_for(&mut self, service: &Service) -> Result<()> {
        ensure_image(&self.client, PROXY_IMAGE).await?;

        let container_name = self.proxy_container_name(service);
        let cmd = vec![
            "--pair".to_string(),
            format!(
                "0.0.0.0:{port}={upstream}:{port}",
                port = service.port,
                upstream = Self::backing_alias(service)
            ),
        ];
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
            image: Some(PROXY_IMAGE.to_string()),
            cmd: Some(cmd),
            exposed_ports: Some(vec![exposed_port]),
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
            self.start_proxy_for(service).await?;
        }
        Ok(())
    }

    async fn wait_ready(&self) -> Result<()> {
        let probes = self
            .fleet
            .services
            .iter()
            .flat_map(|s| [self.backing_container_name(s), self.proxy_container_name(s)])
            .map(|container_name| async move {
                let start = tokio::time::Instant::now();
                loop {
                    if start.elapsed() >= READINESS_TIMEOUT {
                        return Err(Error::ReadinessTimeout {
                            name: container_name,
                            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                            timeout: READINESS_TIMEOUT,
                        });
                    }
                    if container_ready(&self.client, &container_name).await? {
                        return Ok(());
                    }
                    sleep(READINESS_POLL).await;
                }
            });
        futures_util::future::try_join_all(probes).await?;
        Ok(())
    }

    async fn teardown(&mut self) -> Result<()> {
        let opts = RemoveContainerOptionsBuilder::default().force(true).build();
        let mut failures = TeardownFailures::new();
        let names = self
            .fleet
            .services
            .iter()
            .flat_map(|s| [self.backing_container_name(s), self.proxy_container_name(s)]);
        for name in names {
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

fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).expect("healthcheck duration fits in i64")
}

async fn container_ready(docker: &DockerClient, container: &str) -> Result<bool> {
    let inspect = docker.inspect_container(container, None).await?;
    let state = inspect.state;
    let status = state
        .as_ref()
        .and_then(|s| s.health.as_ref())
        .and_then(|h| h.status);
    match status {
        Some(HealthStatusEnum::HEALTHY) => Ok(true),
        Some(_) => Ok(false),
        None => Ok(state.and_then(|s| s.running).unwrap_or(false)),
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
                healthcheck: &[],
            },
            Service {
                name: "web-b",
                image: "nginx:alpine",
                port: 80,
                env: &[],
                healthcheck: &[],
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
            healthcheck: &[],
        }],
    };

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    async fn setup_sweeps_an_orphan_container_from_a_prior_run() {
        let worker_id = std::process::id().wrapping_add(1);
        let orphan_name = format!("crucible-{worker_id}-orphan-actual");

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
