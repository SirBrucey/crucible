//! The in-process registry of the compiled-in plugins, resolved to their schemas.

use std::collections::HashMap;

use crate::{
    builtin::{Docker, Http, Mariadb},
    role::{Deployment, Driver, Observer},
    schema::{AttrSchema, OpSig},
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
