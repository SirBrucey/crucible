//! Docker deployment: brings up the fleet, tears it down.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use bollard::{
    Docker,
    models::{ContainerCreateBody, HostConfig, NetworkCreateRequest},
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, StartContainerOptions,
    },
};
use futures_util::TryStreamExt;

use crate::fleet::{Fleet, Service};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Docker(#[from] bollard::errors::Error),
    #[error("service `{name}` did not publish port {port}")]
    MissingPort { name: String, port: u16 },
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

    async fn start_service(&mut self, service: &Service) -> Result<()> {
        pull_image(&self.docker, service.image).await?;

        let container_name = format!("{}-{}", self.network, service.name);
        let exposed_port = format!("{}/tcp", service.port);

        let config = ContainerCreateBody {
            image: Some(service.image.to_string()),
            exposed_ports: Some(vec![exposed_port.clone()]),
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
    async fn setup_starts_every_service_and_records_a_published_port() {
        let worker_id = std::process::id();
        let mut deployment = Deployment::new(worker_id).expect("connect to docker");

        let outcome = deployment.setup(&fleet::EXAMPLE).await;

        for service in fleet::EXAMPLE.services {
            let endpoint = deployment.endpoint(service.name);
            assert!(
                endpoint.is_some(),
                "expected endpoint for `{}`",
                service.name
            );
        }
        assert_eq!(deployment.containers.len(), fleet::EXAMPLE.services.len());

        manual_cleanup(&deployment).await;
        outcome.expect("setup should succeed");
    }
}
