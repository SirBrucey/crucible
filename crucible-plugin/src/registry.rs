//! The in-process registry of the compiled-in plugins, resolved to their schemas.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use crucible_core::{
    fault::Fault,
    plan,
    schema::{AttrDecl, AttrSchema, OpSig},
};

use crate::{
    builtin::{Docker, Http, HttpObserver, Mariadb},
    discovery,
    error::Error,
    external::Plugin,
    protocol::{self, Role},
    role::{
        Action, Deployment, DeploymentRuntime, Driver, DriverRuntime, Observer, ObserverRuntime,
        Query,
    },
};

/// The available plugins, keyed by name and resolved to their schemas.
#[derive(Default)]
pub struct Registry {
    deployments: HashMap<String, AttrSchema>,
    drivers: HashMap<String, RegisteredDriver>,
    observers: HashMap<String, RegisteredObserver>,
}

impl Registry {
    /// The registry of first-party plugins: docker, http as both a driver and an
    /// observer, and mariadb.
    #[must_use]
    pub fn builtins() -> Self {
        let mut registry = Self::default();
        registry.add_deployment::<Docker>();
        registry.add_driver::<Http>();
        registry.add_observer::<Mariadb>();
        registry.add_observer::<HttpObserver>();
        registry
    }

    /// The first-party plugins, plus every plugin installed on this machine.
    ///
    /// A plugin is an executable the framework runs, so what may install one is
    /// the trust boundary: the search directories are the package manager's,
    /// and `CRUCIBLE_PLUGIN_PATH` is someone choosing otherwise.
    pub async fn load() -> Self {
        let mut registry = Self::builtins();
        let mut claims: HashMap<(String, &'static str), Vec<discovery::Found>> = HashMap::new();
        for found in discovery::discover().await {
            let claim = (
                found.description.name.clone(),
                found.description.role.part(),
            );
            claims.entry(claim).or_default().push(found);
        }
        for ((name, part), claimants) in claims {
            match <[discovery::Found; 1]>::try_from(claimants) {
                Ok([only]) => registry.add_installed(only),
                // Picking one would be arbitrary and invisible to whoever wrote
                // the scenario, so the name is left unclaimed and anything that
                // asks for it says so.
                Err(several) => {
                    let paths: Vec<String> = several
                        .iter()
                        .map(|found| found.path.display().to_string())
                        .collect();
                    tracing::warn!(
                        plugin = %name,
                        paths = %paths.join(", "),
                        "several {part} plugins claim this name, so none of them is loaded",
                    );
                }
            }
        }
        registry
    }

    fn add_installed(&mut self, found: discovery::Found) {
        let discovery::Found { description, path } = found;
        let at = path.display().to_string();
        if description.protocol != protocol::VERSION {
            tracing::warn!(
                path = %at,
                plugin = %description.name,
                theirs = description.protocol,
                ours = protocol::VERSION,
                "ignoring plugin built against another protocol",
            );
            return;
        }
        let plugin = Arc::new(Plugin::new(description.name.clone(), path));
        let Role::Observer { signatures } = description.role;
        if self.observers.contains_key(&description.name) {
            tracing::warn!(
                path = %at,
                plugin = %description.name,
                "ignoring plugin claiming the name of an observer built in",
            );
            return;
        }
        tracing::info!(path = %at, plugin = %description.name, "loaded observer plugin");
        self.observers.insert(
            description.name,
            RegisteredObserver {
                signatures,
                attrs: description.attrs,
                source: Source::Process(plugin),
            },
        );
    }

    fn add_deployment<D: Deployment>(&mut self) {
        self.deployments
            .insert(D::NAME.to_owned(), D::attr_schema());
    }

    fn add_driver<D: Driver>(&mut self) {
        self.drivers.insert(
            D::NAME.to_owned(),
            RegisteredDriver {
                signatures: D::signatures(),
                attrs: D::attr_schema(),
            },
        );
    }

    fn add_observer<O: Observer + 'static>(&mut self) {
        self.observers.insert(
            O::NAME.to_owned(),
            RegisteredObserver {
                signatures: O::signatures(),
                attrs: O::attr_schema(),
                source: Source::Builtin(|service| Box::new(O::runtime(service))),
            },
        );
    }

    /// The attribute schema of the named deployment plugin.
    #[must_use]
    pub fn deployment(&self, name: &str) -> Option<&AttrSchema> {
        self.deployments.get(name)
    }

    /// The action signatures of the named driver plugin.
    #[must_use]
    pub fn driver(&self, name: &str) -> Option<&[OpSig]> {
        self.drivers
            .get(name)
            .map(|driver| driver.signatures.as_slice())
    }

    /// The observable signatures of the named observer plugin.
    #[must_use]
    pub fn observer(&self, name: &str) -> Option<&[OpSig]> {
        self.observers
            .get(name)
            .map(|observer| observer.signatures.as_slice())
    }

    /// The names of the registered deployment plugins.
    #[must_use]
    pub fn deployment_names(&self) -> Vec<String> {
        self.deployments.keys().cloned().collect()
    }

    /// The names of the registered driver plugins.
    #[must_use]
    pub fn driver_names(&self) -> Vec<String> {
        self.drivers.keys().cloned().collect()
    }

    /// Build the replica the planned fleet describes, ready to be brought up by
    /// the deployment plugin it names. The plugin must be registered, so a plan
    /// that passed `check` against this registry resolves here too.
    ///
    /// # Errors
    /// Errors if the plan names a deployment plugin this registry does not hold,
    /// if a service does not bind, or if the plugin cannot be reached.
    pub fn deployment_for(
        &self,
        planned: &plan::Fleet,
        worker_id: u32,
        fault: Option<Fault>,
    ) -> Result<Box<dyn DeploymentRuntime>, Error> {
        match planned.deployment.as_str() {
            Docker::NAME if self.deployments.contains_key(Docker::NAME) => {
                let services = bind_services::<Docker>(planned)?;
                Ok(Box::new(Docker::new(worker_id, services, fault)?))
            }
            other => Err(Error::new(
                "registry",
                format!("no deployment plugin named `{other}`"),
            )),
        }
    }

    /// What a service brought up by `deployment` and speaking `kinds` may
    /// declare.
    ///
    /// # Errors
    /// Errors if the deployment or one of the kinds is not registered, or if two
    /// of them read one attribute as different things.
    pub fn service_schema(
        &self,
        deployment: &str,
        kinds: &[String],
    ) -> Result<ServiceSchema, Error> {
        let (name, attrs) = self.deployments.get_key_value(deployment).ok_or_else(|| {
            Error::new(
                "registry",
                format!("no deployment plugin named `{deployment}`"),
            )
        })?;
        let mut schema = ServiceSchema { attrs: Vec::new() };
        schema.extend(name, attrs.clone())?;
        for kind in kinds {
            for (name, attrs) in self.kind_attrs(kind) {
                schema.extend(&name, attrs)?;
            }
        }
        Ok(schema)
    }

    /// What every plugin registered under `kind` reads of a service.
    ///
    /// One name can be both a driver and an observer, since a fleet is driven
    /// over the same protocol it is read over, and a service speaking it
    /// declares what each of them needs.
    fn kind_attrs(&self, kind: &str) -> Vec<(String, AttrSchema)> {
        let driver = self
            .drivers
            .get_key_value(kind)
            .map(|(name, driver)| (name.clone(), driver.attrs.clone()));
        let observer = self
            .observers
            .get_key_value(kind)
            .map(|(name, observer)| (name.clone(), observer.attrs.clone()));
        driver.into_iter().chain(observer).collect()
    }

    /// Prepare each step, bound to the driver it names, in the order given.
    ///
    /// # Errors
    /// Errors if a step names a driver this registry does not hold, or does not
    /// bind to an operation that driver runs.
    pub fn actions_for(&self, steps: &[plan::Step]) -> Result<Vec<Box<dyn Action>>, Error> {
        let mut drivers: HashMap<&str, Box<dyn DriverRuntime>> = HashMap::new();
        let mut actions = Vec::with_capacity(steps.len());
        for step in steps {
            let driver = match drivers.entry(step.driver.as_str()) {
                Entry::Occupied(driver) => driver.into_mut(),
                Entry::Vacant(slot) => slot.insert(self.driver_runtime(&step.driver)?),
            };
            actions.push(driver.prepare(step)?);
        }
        Ok(actions)
    }

    /// The guard keeps this in step with what the registry reports: a plugin it
    /// says is absent falls through to the catch-all rather than being built.
    fn driver_runtime(&self, name: &str) -> Result<Box<dyn DriverRuntime>, Error> {
        match name {
            Http::NAME if self.drivers.contains_key(name) => Ok(Box::new(Http::new()?)),
            other => Err(Error::new(
                "registry",
                format!("no driver plugin named `{other}`"),
            )),
        }
    }

    /// Prepare each check, bound to the observer that answers it, reading as the
    /// service it names is configured.
    ///
    /// # Errors
    /// Errors if a check names an observer this registry does not hold or a
    /// service the fleet does not describe, or does not bind to an observable
    /// that observer reads.
    pub async fn queries_for(
        &self,
        fleet: &plan::Fleet,
        checks: &[plan::Check],
    ) -> Result<Vec<PreparedCheck>, Error> {
        let mut prepared = Vec::with_capacity(checks.len());
        for check in checks {
            let service = fleet
                .services
                .iter()
                .find(|service| service.name == check.service)
                .ok_or_else(|| {
                    Error::new(
                        "registry",
                        format!("the fleet has no service named `{}`", check.service),
                    )
                })?;
            let observer = self.observer_runtime(&check.observer, service)?;
            prepared.push((check.clone(), observer.prepare(check).await?));
        }
        Ok(prepared)
    }

    fn observer_runtime(
        &self,
        name: &str,
        service: &plan::Service,
    ) -> Result<Box<dyn ObserverRuntime>, Error> {
        let observer = self
            .observers
            .get(name)
            .ok_or_else(|| Error::new("registry", format!("no observer plugin named `{name}`")))?;
        Ok(observer.source.reading(service))
    }
}

/// A registered driver: the operations it runs, and what it reads of a service
/// that speaks it.
struct RegisteredDriver {
    signatures: Vec<OpSig>,
    attrs: AttrSchema,
}

/// A registered observer: the observables it answers, what it reads of a
/// service that speaks it, and where the code that answers lives.
struct RegisteredObserver {
    signatures: Vec<OpSig>,
    attrs: AttrSchema,
    source: Source,
}

/// Where an observer's answers come from.
enum Source {
    /// Compiled in, and called.
    Builtin(fn(&plan::Service) -> Box<dyn ObserverRuntime>),
    /// Installed on the machine, and asked.
    Process(Arc<Plugin>),
}

impl Source {
    fn reading(&self, service: &plan::Service) -> Box<dyn ObserverRuntime> {
        match self {
            Source::Builtin(new) => new(service),
            Source::Process(plugin) => plugin.observing(service),
        }
    }
}

/// A check and the query bound to answer it.
pub type PreparedCheck = (plan::Check, Box<dyn Query>);

/// What one service may declare: the attributes its deployment reads, plus
/// those read by each plugin it speaks, and which plugin reads each.
pub struct ServiceSchema {
    attrs: Vec<(AttrDecl, String)>,
}

impl ServiceSchema {
    /// The declaration for `name`, if any plugin reads it.
    #[must_use]
    pub fn attr(&self, name: &str) -> Option<&AttrDecl> {
        self.attrs
            .iter()
            .find(|(decl, _)| decl.name == name)
            .map(|(decl, _)| decl)
    }

    /// The plugin that reads `name`.
    #[must_use]
    pub fn reader(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(decl, _)| decl.name == name)
            .map(|(_, plugin)| plugin.as_str())
    }

    /// Every attribute a service may declare.
    pub fn attrs(&self) -> impl Iterator<Item = &AttrDecl> {
        self.attrs.iter().map(|(decl, _)| decl)
    }

    /// Take on `schema`, whose attributes `plugin` reads. Two plugins may read
    /// the same attribute, which is how a service states a fact once for both,
    /// but only if they agree on what it holds.
    fn extend(&mut self, plugin: &str, schema: AttrSchema) -> Result<(), Error> {
        for decl in schema.attrs {
            match self.attrs.iter().find(|(held, _)| held.name == decl.name) {
                Some((held, other)) if held.ty != decl.ty => {
                    return Err(Error::new(
                        "registry",
                        format!(
                            "`{}` and `{other}` disagree on what `{}` holds",
                            plugin, decl.name
                        ),
                    ));
                }
                Some(_) => {}
                None => self.attrs.push((decl, plugin.to_owned())),
            }
        }
        Ok(())
    }
}

/// Bind every service of a planned fleet through one deployment plugin.
fn bind_services<D: Deployment>(planned: &plan::Fleet) -> Result<Vec<D::Config>, Error> {
    planned
        .services
        .iter()
        .map(|service| D::bind(service).map_err(|e| Error::new(D::NAME, e)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::Registry;

    use crucible_core::plan;

    fn service(name: &str, kind: &str) -> plan::Service {
        plan::Service {
            name: name.to_owned(),
            kinds: vec![kind.to_owned()],
            attrs: Vec::new(),
        }
    }

    fn fleet() -> plan::Fleet {
        plan::Fleet {
            name: "f".into(),
            deployment: "docker".into(),
            services: vec![service("api", "http"), service("db", "mariadb")],
        }
    }

    fn steps() -> Vec<plan::Step> {
        vec![plan::Step {
            driver: "http".into(),
            operation: "POST".into(),
            args: vec![
                plan::Value::Ident("api".into()),
                plan::Value::Str("/orders".into()),
            ],
            blocks: std::collections::BTreeMap::new(),
            expect: None,
        }]
    }

    fn checks() -> Vec<plan::Check> {
        vec![plan::Check {
            moves: crucible_core::schema::Moves::Counts,
            service: "db".into(),
            observer: "mariadb".into(),
            observable: vec!["orders".into(), "orders".into(), "count".into()],
            args: Vec::new(),
            filter: None,
            clauses: std::collections::BTreeMap::new(),
            op: crucible_core::schema::CmpOp::Eq,
            value: plan::Value::Int(3),
        }]
    }

    #[test]
    fn builtins_resolve_each_role_by_name() {
        let registry = Registry::builtins();
        assert!(registry.deployment("docker").is_some());
        assert!(registry.driver("http").is_some());
        assert!(registry.observer("mariadb").is_some());
    }

    #[test]
    fn an_unregistered_plugin_is_absent() {
        let registry = Registry::builtins();
        assert!(registry.deployment("podman").is_none());
        assert!(registry.driver("grpc").is_none());
        assert!(registry.observer("postgres").is_none());
    }

    #[test]
    fn a_step_prepares_against_the_service_it_names() {
        let actions = Registry::builtins()
            .actions_for(&steps())
            .expect("every step binds to its driver");
        let targets: Vec<&str> = actions.iter().map(|action| action.target()).collect();
        assert_eq!(targets, ["api"]);
    }

    #[tokio::test]
    async fn a_check_prepares_against_the_service_it_names() {
        let queries = Registry::builtins()
            .queries_for(&fleet(), &checks())
            .await
            .expect("every check binds to its observer");
        let targets: Vec<&str> = queries.iter().map(|(_, query)| query.target()).collect();
        assert_eq!(targets, ["db"]);
    }

    #[tokio::test]
    async fn a_check_naming_a_service_the_fleet_lacks_is_rejected() {
        let mut checks = checks();
        checks[0].service = "ledger".into();
        assert!(
            Registry::builtins()
                .queries_for(&fleet(), &checks)
                .await
                .is_err()
        );
    }

    #[test]
    fn a_step_naming_an_unregistered_driver_is_rejected() {
        let mut steps = steps();
        steps[0].driver = "grpc".into();
        assert!(Registry::builtins().actions_for(&steps).is_err());
    }

    #[test]
    fn a_registry_will_not_build_a_deployment_it_does_not_hold() {
        // A plan is checked against the registry before it runs, so a registry
        // that reports a plugin absent must not then hand one out.
        let registry = Registry::default();
        assert!(registry.deployment("docker").is_none());
        assert!(registry.deployment_for(&fleet(), 0, None).is_err());
    }
}
