//! The in-process registry of the compiled-in plugins, resolved to their schemas.

use std::collections::HashMap;

use crucible_core::{
    fleet, plan,
    schema::{AttrSchema, OpSig},
};

use crate::{
    builtin::{Docker, Http, Mariadb, deployment::docker::ProxyAnchor},
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

    /// Bind every service of a planned fleet through the deployment plugin it
    /// names, giving the spec that plugin needs to bring the fleet up.
    ///
    /// # Errors
    /// Errors if the plan names a deployment plugin that is not registered, or
    /// if a service does not bind.
    pub fn bind_fleet(&self, planned: &plan::Fleet) -> Result<fleet::Fleet, Error> {
        match planned.deployment.as_str() {
            Docker::NAME => planned
                .services
                .iter()
                .map(|service| Docker::bind(service).map_err(|e| Error::new(Docker::NAME, e)))
                .collect::<Result<Vec<_>, _>>()
                .map(fleet::Fleet::new),
            other => Err(unknown_deployment(other)),
        }
    }

    /// Build the replica the planned fleet describes, ready to be brought up by
    /// the deployment plugin it names.
    ///
    /// # Errors
    /// Errors if the plan names a deployment plugin that is not registered, if a
    /// service does not bind, or if the plugin cannot be reached.
    pub fn deployment_for(
        &self,
        planned: &plan::Fleet,
        worker_id: u32,
        anchor: Option<ProxyAnchor>,
    ) -> Result<Box<dyn DeploymentRuntime>, Error> {
        let fleet = self.bind_fleet(planned)?;
        match planned.deployment.as_str() {
            Docker::NAME => {
                let docker = Docker::new(worker_id, fleet, anchor)
                    .map_err(|e| Error::new(Docker::NAME, e))?;
                Ok(Box::new(docker))
            }
            other => Err(unknown_deployment(other)),
        }
    }
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
}
