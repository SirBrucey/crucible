//! The Docker deployment plugin: it advertises what a container needs,
//! binds a service to that, and runs the replica.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, KillContainerOptionsBuilder,
        LogsOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptions,
    },
};
use crucible_protocol::{Direction, now_ns};
use futures_util::{StreamExt, TryStreamExt};
use tokio::time::sleep;

use crucible_core::{
    fault::Fault,
    observer::SessionObserver,
    plan,
    schema::{AttrDecl, AttrSchema, ValueType},
};

use crate::{
    error::Error as PluginError,
    role::{BoxFuture, Cut, Deployment, DeploymentRuntime, Faults, Kill, Substrate},
};

const READINESS_TIMEOUT: Duration = Duration::from_mins(1);
const READINESS_POLL: Duration = Duration::from_millis(500);
const HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(1);
const HEALTHCHECK_START_PERIOD: Duration = Duration::from_secs(30);
const PROXY_IMAGE: &str = "crucible-proxy:0.1";
/// Control-plane listeners start here, above anything a fleet would use.
const CONTROL_BASE: u16 = 40000;
const PROXY_SUFFIX: &str = "proxy";
const BACKING_SUFFIX: &str = "actual";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Docker(#[from] bollard::errors::Error),
    #[error("service `{name}` did not publish port {port}")]
    MissingPort { name: String, port: u16 },
    #[error("service `{name}` did not become ready within {timeout:?}")]
    ReadinessTimeout { name: String, timeout: Duration },
    #[error("teardown incomplete: {0}")]
    TeardownIncomplete(TeardownFailures),
    #[error("unknown service `{0}`")]
    UnknownService(String),
    #[error(
        "two services share port {0}; the single-container proxy would need to remap it and \
         rewrite the consumer's endpoint, which is not wired yet"
    )]
    PortCollision(u16),
    #[error("service `{service}` depends on `{missing}`, which the fleet does not describe")]
    UnknownDependency { service: String, missing: String },
    #[error("these services depend on each other, so none of them can start first: {0}")]
    DependencyCycle(String),
}

/// Items teardown could not remove, paired with the daemon's reason.
#[derive(Debug, Default)]
pub struct TeardownFailures {
    containers: Vec<(String, String)>,
    network: Option<String>,
}

impl TeardownFailures {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append_container(&mut self, name: impl Into<String>, reason: impl Into<String>) {
        self.containers.push((name.into(), reason.into()));
    }

    pub fn set_network(&mut self, reason: impl Into<String>) {
        self.network = Some(reason.into());
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.containers.is_empty() && self.network.is_none()
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

#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error("service `{service}` has no usable `{attr}`")]
    Attr { service: String, attr: &'static str },
    #[error("service `{service}`: {port} is not a port number for `{kind}`")]
    Port {
        service: String,
        kind: String,
        port: i64,
    },
    #[error("service `{service}` states a port for `{kind}`, which it does not speak")]
    UnspokenKind { service: String, kind: String },
    #[error("service `{service}` states no port for `{kind}`, which it speaks")]
    MissingKind { service: String, kind: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What Docker needs to run one service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceConfig {
    pub name: String,
    pub image: String,
    /// The port each kind the service speaks answers on.
    pub ports: BTreeMap<String, u16>,
    /// Environment variables, one per entry in `KEY=value` form.
    pub env: Vec<String>,
    /// Command to run inside the container as a healthcheck; empty means the
    /// image's own healthcheck.
    pub healthcheck: Vec<String>,
    /// Services that must be healthy before this one starts.
    pub depends_on: Vec<String>,
}

/// Brings services up as Docker containers.
pub struct Docker {
    client: DockerClient,
    network: String,
    services: Vec<ServiceConfig>,
    endpoints: Endpoints,
    fault: Option<Fault>,
    proxy: Proxy,
}

/// Where each service answers, keyed by service and kind. The proxy fronts every
/// service twice, once per plane.
#[derive(Default)]
struct Endpoints {
    /// The test traffic: watched, and frozen when a fault fires.
    data: HashMap<(String, String), SocketAddr>,
    /// The side channel, for faulting and observation.
    control: HashMap<(String, String), SocketAddr>,
}

/// Where a service's control plane listens inside the proxy. Far enough above
/// the fleet's own ports that it cannot land on one of them.
fn control_port(port: u16) -> u16 {
    CONTROL_BASE + port % CONTROL_BASE
}

fn local(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

impl Docker {
    /// Connect to the local Docker daemon and prepare a per-worker deployment
    /// handle (nothing is created until [`Docker::setup`]).
    ///
    /// # Errors
    /// Errors if connecting to the Docker daemon socket fails.
    pub fn new(worker_id: u32, services: Vec<ServiceConfig>, fault: Option<Fault>) -> Result<Self> {
        let client = DockerClient::connect_with_socket_defaults()?;
        let network = format!("crucible-{worker_id}");
        Ok(Self {
            // Both act on the daemon, and the handle is shared rather than a
            // second connection.
            proxy: Proxy {
                client: client.clone(),
                container: format!("{network}-{PROXY_SUFFIX}"),
            },
            client,
            network,
            services,
            endpoints: Endpoints::default(),
            fault,
        })
    }

    fn service_by_name(&self, name: &str) -> Result<&ServiceConfig> {
        self.services
            .iter()
            .find(|service| service.name == name)
            .ok_or_else(|| Error::UnknownService(name.to_string()))
    }

    /// Network-scoped name of a service's backing container: its in-network
    /// alias prefixed with the per-worker network.
    fn backing_container_name(&self, service: &ServiceConfig) -> String {
        format!("{}-{}", self.network, Self::backing_alias(service))
    }

    /// The single proxy container that fronts every service in the fleet.
    fn proxy_container_name(&self) -> String {
        format!("{}-{}", self.network, PROXY_SUFFIX)
    }

    /// The names of every container in a replica: each backing service plus the
    /// fronting proxy.
    fn container_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .services
            .iter()
            .map(|s| self.backing_container_name(s))
            .collect();
        names.push(self.proxy_container_name());
        names
    }

    /// In-network alias a backing container is reached by (via the proxy).
    fn backing_alias(service: &ServiceConfig) -> String {
        format!("{}-{}", service.name, BACKING_SUFFIX)
    }

    /// Start a service's backing container. It is reached only through the proxy
    /// (via its `service-actual` network alias), so it publishes no host port.
    async fn start_service(&self, service: &ServiceConfig) -> Result<()> {
        ensure_image(&self.client, &service.image).await?;

        let container_name = self.backing_container_name(service);
        let exposed: Vec<String> = service
            .ports
            .values()
            .map(|port| format!("{port}/tcp"))
            .collect();

        let mut endpoints_config = HashMap::new();
        endpoints_config.insert(
            self.network.clone(),
            EndpointSettings {
                aliases: Some(vec![Self::backing_alias(service)]),
                ..Default::default()
            },
        );

        let healthcheck = (!service.healthcheck.is_empty()).then(|| HealthConfig {
            test: Some(service.healthcheck.clone()),
            interval: Some(nanos(HEALTHCHECK_INTERVAL)),
            start_period: Some(nanos(HEALTHCHECK_START_PERIOD)),
            ..Default::default()
        });

        let config = ContainerCreateBody {
            image: Some(service.image.clone()),
            exposed_ports: Some(exposed),
            env: (!service.env.is_empty()).then(|| service.env.clone()),
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

    /// Start the single proxy container fronting the whole fleet: one pair per
    /// service (`service=0.0.0.0:port=service-actual:port`), every service name
    /// as a network alias, and all ports published to the host. Returns each
    /// service's published host endpoint. Every pair shares one process-wide
    /// pause gate, so a single signal freezes the whole fleet atomically.
    async fn start_proxy(&self) -> Result<Endpoints> {
        ensure_image(&self.client, PROXY_IMAGE).await?;

        // The proxy binds one listener per service port. Distinct ports map
        // through unchanged; a shared port cannot be disambiguated on the single
        // container IP, so reject it rather than misroute.
        let mut seen = HashSet::new();
        for service in &self.services {
            for port in service.ports.values() {
                if !seen.insert(*port) {
                    return Err(Error::PortCollision(*port));
                }
            }
        }

        let container_name = self.proxy_container_name();

        let mut cmd = Vec::new();
        for service in &self.services {
            // A pair per port, each tagged with the service rather than the
            // kind: what a fault is anchored to is the service, however many
            // ports its traffic arrives on.
            for port in service.ports.values() {
                cmd.push("--pair".to_string());
                cmd.push(format!(
                    "{name}=0.0.0.0:{port}={upstream}:{port}",
                    name = service.name,
                    upstream = Self::backing_alias(service),
                ));
                cmd.push("--control".to_string());
                cmd.push(format!(
                    "{name}=0.0.0.0:{listen}={upstream}:{port}",
                    name = service.name,
                    listen = control_port(*port),
                    upstream = Self::backing_alias(service),
                ));
            }
        }
        if let Some(fault) = &self.fault {
            let anchor = fault.anchor();
            let direction = match anchor.direction {
                Direction::ClientToUpstream => "c2u",
                Direction::UpstreamToClient => "u2c",
            };
            cmd.push("--fault-at".to_string());
            cmd.push(format!("{}={}={}", anchor.service, direction, anchor.k));
            cmd.push("--fault".to_string());
            cmd.push(fault.primitive().to_string());
        }

        let exposed_ports: Vec<String> = self
            .services
            .iter()
            .flat_map(|s| s.ports.values())
            .flat_map(|port| {
                [
                    format!("{port}/tcp"),
                    format!("{}/tcp", control_port(*port)),
                ]
            })
            .collect();

        let aliases: Vec<String> = self.services.iter().map(|s| s.name.clone()).collect();
        let mut endpoints_config = HashMap::new();
        endpoints_config.insert(
            self.network.clone(),
            EndpointSettings {
                aliases: Some(aliases),
                ..Default::default()
            },
        );

        let config = ContainerCreateBody {
            image: Some(PROXY_IMAGE.to_string()),
            cmd: Some(cmd),
            exposed_ports: Some(exposed_ports),
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

        let mut endpoints = Endpoints::default();
        for service in &self.services {
            for (kind, port) in &service.ports {
                let key = (service.name.clone(), kind.clone());
                let observed = published_port(&self.client, &container_name, *port).await?;
                let control =
                    published_port(&self.client, &container_name, control_port(*port)).await?;
                endpoints.data.insert(key.clone(), local(observed));
                endpoints.control.insert(key, local(control));
            }
        }
        Ok(endpoints)
    }

    /// SIGKILL the service's backing container. Returns the wall-clock
    /// nanoseconds since the Unix epoch of the moment the kill returned.
    async fn kill_backing(&self, name: &str) -> Result<u128> {
        let service = self.service_by_name(name)?;
        let container = self.backing_container_name(service);
        let options = KillContainerOptionsBuilder::default()
            .signal("SIGKILL")
            .build();
        self.client
            .kill_container(&container, Some(options))
            .await?;
        Ok(now_ns())
    }

    /// Start the previously-killed backing container and wait for its
    /// healthcheck to report healthy. The caller waits for fleet quiescence
    /// (see `SessionObserver::wait_for_quiescence`) before collecting the
    /// verdict. Returns wall-clock nanoseconds when the service was ready.
    async fn restart_backing(&self, name: &str) -> Result<u128> {
        let service = self.service_by_name(name)?;
        let container = self.backing_container_name(service);
        self.client
            .start_container(&container, None::<StartContainerOptions>)
            .await?;
        wait_container_ready(&self.client, &container).await?;
        Ok(now_ns())
    }

    async fn create_replica(&mut self) -> Result<()> {
        self.destroy_replica().await?;

        self.client
            .create_network(NetworkCreateRequest {
                name: self.network.clone(),
                ..Default::default()
            })
            .await?;

        // The proxy first: every service reaches its peers by a name the proxy
        // answers for, so until it is up a service that talks to another on
        // startup has nothing to talk to. It resolves its own upstreams per
        // connection, so it does not need them yet.
        self.endpoints = self.start_proxy().await?;
        self.start_services().await?;
        Ok(())
    }

    /// Start each service once everything it depends on is healthy, and those
    /// that depend on nothing together.
    async fn start_services(&self) -> Result<()> {
        for service in &self.services {
            for needed in &service.depends_on {
                if !self.services.iter().any(|s| &s.name == needed) {
                    return Err(Error::UnknownDependency {
                        service: service.name.clone(),
                        missing: needed.clone(),
                    });
                }
            }
        }

        let mut healthy: HashSet<&str> = HashSet::new();
        let mut waiting: Vec<&ServiceConfig> = self.services.iter().collect();
        while !waiting.is_empty() {
            let (ready, blocked): (Vec<_>, Vec<_>) = waiting.into_iter().partition(|service| {
                service
                    .depends_on
                    .iter()
                    .all(|needed| healthy.contains(needed.as_str()))
            });
            if ready.is_empty() {
                let names: Vec<&str> = blocked.iter().map(|s| s.name.as_str()).collect();
                return Err(Error::DependencyCycle(names.join(", ")));
            }
            futures_util::future::try_join_all(ready.iter().map(|s| self.start_service(s))).await?;
            futures_util::future::try_join_all(ready.iter().map(|service| async move {
                wait_container_ready(&self.client, &self.backing_container_name(service)).await
            }))
            .await?;
            healthy.extend(ready.iter().map(|service| service.name.as_str()));
            waiting = blocked;
        }
        Ok(())
    }

    async fn await_healthy(&self) -> Result<()> {
        let probes = self
            .container_names()
            .into_iter()
            .map(|name| async move { wait_container_ready(&self.client, &name).await });
        futures_util::future::try_join_all(probes).await?;
        Ok(())
    }

    async fn destroy_replica(&mut self) -> Result<()> {
        // `v` takes the anonymous volumes with it. A replica is thrown away
        // after every schedule, and a database image makes one of these per
        // container, so without it a campaign can leave hundreds behind.
        let opts = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();
        let mut failures = TeardownFailures::new();
        let removals =
            futures_util::future::join_all(self.container_names().into_iter().map(|name| {
                let client = &self.client;
                let opts = opts.clone();
                async move {
                    let result = client.remove_container(&name, Some(opts)).await;
                    (name, result)
                }
            }))
            .await;
        for (name, result) in removals {
            match result {
                Ok(()) => {}
                Err(e) if is_not_found(&e) => {}
                Err(e) => failures.append_container(name, e.to_string()),
            }
        }
        self.endpoints = Endpoints::default();
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
}

fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).expect("healthcheck duration fits in i64")
}

/// Poll `container` until it reports ready, or return [`Error::ReadinessTimeout`]
/// once [`READINESS_TIMEOUT`] elapses.
async fn wait_container_ready(client: &DockerClient, container: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + READINESS_TIMEOUT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::ReadinessTimeout {
                name: container.to_string(),
                timeout: READINESS_TIMEOUT,
            });
        }
        if container_ready(client, container).await? {
            return Ok(());
        }
        sleep(READINESS_POLL).await;
    }
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
    // Docker binds a port on IPv4 and IPv6 separately, and gives each its own
    // host port. Endpoints here are IPv4, and taking whichever came first would
    // sometimes hand back the v6 one, which is some other service's v4 port.
    let bindings = ports.get(&key).and_then(Option::as_ref).and_then(|v| {
        v.iter().find(|binding| {
            binding
                .host_ip
                .as_deref()
                .is_some_and(|ip| !ip.contains(':'))
        })
    });
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

impl Deployment for Docker {
    const NAME: &'static str = "docker";
    type Config = ServiceConfig;
    type Error = BindError;

    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![
            AttrDecl::required("image", ValueType::Str),
            AttrDecl::required("ports", ValueType::MapOf(Box::new(ValueType::Int))),
            AttrDecl::optional("env", ValueType::List(Box::new(ValueType::Str))),
            AttrDecl::optional("healthcheck", ValueType::List(Box::new(ValueType::Str))),
            AttrDecl::optional(
                "depends_on",
                ValueType::List(Box::new(ValueType::ServiceRef)),
            ),
        ])
    }

    fn bind(service: &plan::Service) -> Result<Self::Config, Self::Error> {
        let missing = |attr| BindError::Attr {
            service: service.name.clone(),
            attr,
        };

        let image = service
            .attr("image")
            .and_then(plan::Value::as_str)
            .ok_or_else(|| missing("image"))?;
        let stated = service
            .attr("ports")
            .and_then(plan::Value::as_map)
            .ok_or_else(|| missing("ports"))?;

        let mut ports = BTreeMap::new();
        for (kind, port) in stated {
            if !service.kinds.contains(kind) {
                return Err(BindError::UnspokenKind {
                    service: service.name.clone(),
                    kind: kind.clone(),
                });
            }
            let port = port.as_int().ok_or_else(|| BindError::Port {
                service: service.name.clone(),
                kind: kind.clone(),
                port: 0,
            })?;
            let port = u16::try_from(port).map_err(|_| BindError::Port {
                service: service.name.clone(),
                kind: kind.clone(),
                port,
            })?;
            ports.insert(kind.clone(), port);
        }
        // A kind with no port could never be reached, so the service says one
        // for each rather than failing when something tries.
        if let Some(kind) = service.kinds.iter().find(|kind| !ports.contains_key(*kind)) {
            return Err(BindError::MissingKind {
                service: service.name.clone(),
                kind: kind.clone(),
            });
        }

        Ok(ServiceConfig {
            name: service.name.clone(),
            image: image.to_owned(),
            ports,
            env: owned_strs(service, "env")?,
            healthcheck: owned_strs(service, "healthcheck")?,
            depends_on: owned_idents(service, "depends_on")?,
        })
    }
}

/// An optional list-of-strings attribute, empty when the service omits it.
fn owned_strs(service: &plan::Service, attr: &'static str) -> Result<Vec<String>, BindError> {
    let Some(value) = service.attr(attr) else {
        return Ok(Vec::new());
    };
    let strs = value.as_strs().ok_or(BindError::Attr {
        service: service.name.clone(),
        attr,
    })?;
    Ok(strs.into_iter().map(str::to_owned).collect())
}

/// An optional list-of-names attribute, empty when the service omits it.
fn owned_idents(service: &plan::Service, attr: &'static str) -> Result<Vec<String>, BindError> {
    let Some(plan::Value::List(items)) = service.attr(attr) else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .map(|item| match item {
            plan::Value::Ident(name) => Ok(name.clone()),
            _ => Err(BindError::Attr {
                service: service.name.clone(),
                attr,
            }),
        })
        .collect()
}

/// The container Docker runs in front of the fleet.
pub struct Proxy {
    client: DockerClient,
    container: String,
}

impl Substrate for Proxy {
    /// SIGUSR1 the proxy: it resets its packet counter and begins counting, so
    /// the anchor lands relative to scenario traffic rather than the fleet's
    /// bring-up. When it reaches the scheduled packet the proxy holds the fleet
    /// itself.
    fn arm_anchor(&self) -> BoxFuture<'_, Result<(), PluginError>> {
        Box::pin(async move { self.signal("SIGUSR1").await.map_err(PluginError::from) })
    }

    /// SIGUSR2 the proxy to let the fleet go, so the held bytes flow again.
    fn resume(&self) -> BoxFuture<'_, Result<(), PluginError>> {
        Box::pin(async move { self.signal("SIGUSR2").await.map_err(PluginError::from) })
    }
}

/// The proxy holds both sides of every edge, so it can let one go.
impl Faults for Proxy {
    fn cuts(&self) -> Option<&dyn Cut> {
        Some(self)
    }
}

impl Cut for Proxy {}

impl Proxy {
    async fn signal(&self, signal: &str) -> Result<()> {
        let options = KillContainerOptionsBuilder::default()
            .signal(signal)
            .build();
        self.client
            .kill_container(&self.container, Some(options))
            .await?;
        Ok(())
    }
}

/// Docker owns the containers behind the fleet, so it can take one away.
impl Faults for Docker {
    fn kills(&self) -> Option<&dyn Kill> {
        Some(self)
    }
}

impl Kill for Docker {
    fn kill(&self, service: &str) -> BoxFuture<'_, Result<u128, PluginError>> {
        let service = service.to_owned();
        Box::pin(async move { self.kill_backing(&service).await.map_err(PluginError::from) })
    }

    fn restart(&self, service: &str) -> BoxFuture<'_, Result<u128, PluginError>> {
        let service = service.to_owned();
        Box::pin(async move {
            self.restart_backing(&service)
                .await
                .map_err(PluginError::from)
        })
    }
}

impl DeploymentRuntime for Docker {
    fn substrate(&self) -> &dyn Substrate {
        &self.proxy
    }

    fn setup(&mut self) -> BoxFuture<'_, Result<(), PluginError>> {
        Box::pin(async move { self.create_replica().await.map_err(PluginError::from) })
    }

    fn wait_ready(&self) -> BoxFuture<'_, Result<(), PluginError>> {
        Box::pin(async move { self.await_healthy().await.map_err(PluginError::from) })
    }

    fn teardown(&mut self) -> BoxFuture<'_, Result<(), PluginError>> {
        Box::pin(async move { self.destroy_replica().await.map_err(PluginError::from) })
    }

    fn endpoint(&self, service: &str, kind: &str) -> Option<SocketAddr> {
        self.endpoints
            .data
            .get(&(service.to_owned(), kind.to_owned()))
            .copied()
    }

    fn control_endpoint(&self, service: &str, kind: &str) -> Option<SocketAddr> {
        self.endpoints
            .control
            .get(&(service.to_owned(), kind.to_owned()))
            .copied()
    }

    /// The proxy writes its event lines to its container's stdout, so following
    /// that container's log is how this deployment hands the observer its feed.
    fn start_session_observer(&self) -> SessionObserver {
        let container = self.proxy_container_name();
        let options = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(false)
            .follow(true)
            .tail("all")
            .build();
        let chunks = self
            .client
            .logs(&container, Some(options))
            .scan((), move |(), chunk| {
                // A stream that ends is how a failed read reaches the observer,
                // which has no way to ask docker what went wrong.
                std::future::ready(match chunk {
                    Ok(bytes) => Some(bytes.as_ref().to_vec()),
                    Err(e) => {
                        tracing::warn!(target: "session_observer", %container, error = %e, "log stream error");
                        None
                    }
                })
            });
        SessionObserver::start(chunks)
    }
}

impl From<Error> for PluginError {
    fn from(e: Error) -> Self {
        Self::new(Docker::NAME, e)
    }
}

impl From<BindError> for PluginError {
    fn from(e: BindError) -> Self {
        Self::new(Docker::NAME, e)
    }
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

    // Distinct ports: the single proxy container binds one listener per service
    // port, so two services must not share one (see `Error::PortCollision`).
    fn lifecycle_test_fleet() -> Vec<ServiceConfig> {
        vec![
            test_service("web-a", "nginx:alpine", "http", 80),
            test_service("cache-b", "redis:alpine", "redis", 6379),
        ]
    }

    fn test_service(name: &str, image: &str, kind: &str, port: u16) -> ServiceConfig {
        ServiceConfig {
            name: name.to_owned(),
            image: image.to_owned(),
            ports: BTreeMap::from([(kind.to_owned(), port)]),
            env: Vec::new(),
            healthcheck: Vec::new(),
            depends_on: Vec::new(),
        }
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    async fn deployment_lifecycle_brings_up_and_tears_down_every_service() {
        let worker_id = std::process::id();
        let fleet = lifecycle_test_fleet();
        // Driven through the trait, which is how the framework reaches a
        // deployment, so a delegation wired to the wrong method fails here.
        let mut deployment: Box<dyn DeploymentRuntime> =
            Box::new(Docker::new(worker_id, fleet.clone(), None).expect("connect to docker"));

        let setup_outcome = deployment.setup().await;
        for service in &fleet {
            for kind in service.ports.keys() {
                assert!(
                    deployment.endpoint(&service.name, kind).is_some(),
                    "expected endpoint for `{}` speaking `{kind}`",
                    service.name
                );
            }
        }

        let wait_outcome = deployment.wait_ready().await;

        let teardown_outcome = deployment.teardown().await;

        setup_outcome.expect("setup should succeed");
        wait_outcome.expect("every service should become ready");
        teardown_outcome.expect("teardown should succeed");

        for service in &fleet {
            for kind in service.ports.keys() {
                assert!(
                    deployment.endpoint(&service.name, kind).is_none(),
                    "endpoint for `{}` speaking `{kind}` should be cleared after teardown",
                    service.name
                );
            }
        }
    }

    fn orphan_test_fleet() -> Vec<ServiceConfig> {
        vec![test_service("orphan", "nginx:alpine", "http", 80)]
    }

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

        let mut docker =
            Docker::new(worker_id, orphan_test_fleet(), None).expect("connect to docker");
        let setup_outcome = docker.create_replica().await;
        let teardown_outcome = docker.destroy_replica().await;

        setup_outcome.expect("setup should sweep the orphan and succeed");
        teardown_outcome.expect("teardown should succeed");

        let inspect = client.inspect_container(&orphan_name, None).await;
        assert!(
            inspect.is_err(),
            "orphan container should have been removed"
        );
    }

    fn service(attrs: Vec<(&str, plan::Value)>) -> plan::Service {
        plan::Service {
            name: "api".into(),
            kinds: vec!["http".into()],
            attrs: attrs
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        }
    }

    #[test]
    fn image_and_ports_are_required_env_is_optional() {
        let schema = Docker::attr_schema();

        let image = schema.attr("image").expect("image is declared");
        assert!(image.required);
        assert_eq!(image.ty, ValueType::Str);

        let ports = schema.attr("ports").expect("ports is declared");
        assert!(ports.required);
        assert_eq!(ports.ty, ValueType::MapOf(Box::new(ValueType::Int)));

        assert!(!schema.attr("env").expect("env is declared").required);
    }

    #[test]
    fn a_service_binds_to_a_container_config() {
        let bound = Docker::bind(&service(vec![
            ("image", plan::Value::Str("example/api:1".into())),
            (
                "ports",
                plan::Value::Map(vec![("http".into(), plan::Value::Int(8080))]),
            ),
            (
                "env",
                plan::Value::List(vec![plan::Value::Str("A=1".into())]),
            ),
        ]))
        .expect("binds");

        assert_eq!(
            bound,
            ServiceConfig {
                name: "api".into(),
                image: "example/api:1".into(),
                ports: BTreeMap::from([("http".to_owned(), 8080)]),
                env: vec!["A=1".into()],
                healthcheck: Vec::new(),
                depends_on: Vec::new(),
            },
        );
    }

    #[test]
    fn every_service_of_the_example_fleet_binds() {
        // The example is built at runtime rather than declared as a constant, so
        // a mistyped attribute would otherwise surface only at bring-up.
        for service in &crucible_core::plan::example().fleet.services {
            Docker::bind(service)
                .unwrap_or_else(|e| panic!("service `{}` should bind: {e}", service.name));
        }
    }

    #[test]
    fn a_port_outside_the_port_range_is_rejected() {
        let bound = Docker::bind(&service(vec![
            ("image", plan::Value::Str("example/api:1".into())),
            (
                "ports",
                plan::Value::Map(vec![("http".into(), plan::Value::Int(70_000))]),
            ),
        ]));
        assert!(matches!(bound, Err(BindError::Port { .. })));
    }

    #[test]
    fn a_port_for_a_kind_the_service_does_not_speak_is_rejected() {
        let bound = Docker::bind(&service(vec![
            ("image", plan::Value::Str("example/api:1".into())),
            (
                "ports",
                plan::Value::Map(vec![
                    ("http".into(), plan::Value::Int(80)),
                    ("dns".into(), plan::Value::Int(53)),
                ]),
            ),
        ]));
        assert!(matches!(bound, Err(BindError::UnspokenKind { .. })));
    }

    /// A kind with no port could never be reached, so it is refused where the
    /// service is described rather than when something tries to reach it.
    #[test]
    fn a_kind_the_service_speaks_needs_a_port() {
        let bound = Docker::bind(&service(vec![
            ("image", plan::Value::Str("example/api:1".into())),
            ("ports", plan::Value::Map(Vec::new())),
        ]));
        assert!(matches!(bound, Err(BindError::MissingKind { .. })));
    }
}
