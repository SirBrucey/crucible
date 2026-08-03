//! The in-process registry of the compiled-in plugins, resolved to their schemas.

use std::collections::{HashMap, hash_map::Entry};

use crucible_core::{
    fault::Anchor,
    plan,
    schema::{AttrSchema, OpSig},
};

use crate::{
    builtin::{Docker, Http, Mariadb},
    error::Error,
    role::{Action, Deployment, DeploymentRuntime, Driver, DriverRuntime, Observer},
};

/// The available plugins, keyed by name and resolved to their schemas.
#[derive(Default)]
pub struct Registry {
    deployments: HashMap<&'static str, AttrSchema>,
    drivers: HashMap<&'static str, Vec<OpSig>>,
    observers: HashMap<&'static str, Vec<OpSig>>,
}

impl Registry {
    /// The registry of first-party plugins: docker, http, mariadb.
    #[must_use]
    pub fn builtins() -> Self {
        let mut registry = Self::default();
        registry.add_deployment::<Docker>();
        registry.add_driver::<Http>();
        registry.add_observer::<Mariadb>();
        registry
    }

    fn add_deployment<D: Deployment>(&mut self) {
        self.deployments.insert(D::NAME, D::attr_schema());
    }

    fn add_driver<D: Driver>(&mut self) {
        self.drivers.insert(D::NAME, D::signatures());
    }

    fn add_observer<O: Observer>(&mut self) {
        self.observers.insert(O::NAME, O::signatures());
    }

    /// The attribute schema of the named deployment plugin.
    #[must_use]
    pub fn deployment(&self, name: &str) -> Option<&AttrSchema> {
        self.deployments.get(name)
    }

    /// The action signatures of the named driver plugin.
    #[must_use]
    pub fn driver(&self, name: &str) -> Option<&[OpSig]> {
        self.drivers.get(name).map(Vec::as_slice)
    }

    /// The observable signatures of the named observer plugin.
    #[must_use]
    pub fn observer(&self, name: &str) -> Option<&[OpSig]> {
        self.observers.get(name).map(Vec::as_slice)
    }

    /// The names of the registered deployment plugins.
    #[must_use]
    pub fn deployment_names(&self) -> Vec<&'static str> {
        self.deployments.keys().copied().collect()
    }

    /// The names of the registered driver plugins.
    #[must_use]
    pub fn driver_names(&self) -> Vec<&'static str> {
        self.drivers.keys().copied().collect()
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
        anchor: Option<Anchor>,
    ) -> Result<Box<dyn DeploymentRuntime>, Error> {
        match planned.deployment.as_str() {
            Docker::NAME if self.deployments.contains_key(Docker::NAME) => {
                let services = bind_services::<Docker>(planned)?;
                Ok(Box::new(Docker::new(worker_id, services, anchor)?))
            }
            other => Err(Error::new(
                "registry",
                format!("no deployment plugin named `{other}`"),
            )),
        }
    }

    /// Prepare every step of a scenario, each bound to the driver it names, in
    /// the order the scenario runs them.
    ///
    /// # Errors
    /// Errors if a step names a driver this registry does not hold, or does not
    /// bind to an operation that driver runs.
    pub fn actions_for(&self, scenario: &plan::Scenario) -> Result<Vec<Box<dyn Action>>, Error> {
        let mut drivers: HashMap<&str, Box<dyn DriverRuntime>> = HashMap::new();
        let mut actions = Vec::with_capacity(scenario.steps.len());
        for step in &scenario.steps {
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
    fn every_step_of_the_example_scenario_prepares() {
        let plan = crucible_core::plan::example();
        let actions = Registry::builtins()
            .actions_for(&plan.scenarios[0])
            .expect("every step binds to its driver");
        let targets: Vec<&str> = actions.iter().map(|action| action.target()).collect();
        assert_eq!(targets, ["api", "api", "api"]);
    }

    #[test]
    fn a_step_naming_an_unregistered_driver_is_rejected() {
        let mut scenario = crucible_core::plan::example().scenarios.remove(0);
        scenario.steps[0].driver = "grpc".into();
        assert!(Registry::builtins().actions_for(&scenario).is_err());
    }

    #[test]
    fn a_registry_will_not_build_a_deployment_it_does_not_hold() {
        // A plan is checked against the registry before it runs, so a registry
        // that reports a plugin absent must not then hand one out.
        let registry = Registry::default();
        assert!(registry.deployment("docker").is_none());
        assert!(
            registry
                .deployment_for(&crucible_core::plan::example().fleet, 0, None)
                .is_err()
        );
    }
}
