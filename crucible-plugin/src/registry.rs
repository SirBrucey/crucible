//! The in-process registry of the compiled-in plugins, resolved to their schemas.

use std::collections::HashMap;

use crucible_core::{
    fault::Anchor,
    plan,
    schema::{AttrSchema, OpSig},
};

use crate::{
    builtin::{Docker, Http, Mariadb},
    error::Error,
    role::{Deployment, DeploymentRuntime, Driver, Observer},
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
        let name = planned.deployment.as_str();
        if !self.deployments.contains_key(name) {
            return Err(unknown_deployment(name));
        }
        match name {
            Docker::NAME => {
                let services = bind_services::<Docker>(planned)?;
                let docker = Docker::new(worker_id, services, anchor).map_err(Error::from)?;
                Ok(Box::new(docker))
            }
            other => Err(unknown_deployment(other)),
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

fn unknown_deployment(name: &str) -> Error {
    Error::new("registry", format!("no deployment plugin named `{name}`"))
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
    fn a_registry_will_not_build_a_deployment_it_does_not_hold() {
        // A plan is checked against the registry before it runs, so a registry
        // that reports a plugin absent must not then hand one out.
        let registry = Registry::default();
        assert!(registry.deployment("docker").is_none());
        assert!(
            registry
                .deployment_for(&crucible_core::plan::example(), 0, None)
                .is_err()
        );
    }
}
