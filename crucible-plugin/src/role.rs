//! The per-role plugin traits. A plugin crate implements one or more, and each
//! advertises the schema for its role.

use crate::schema::{AttrSchema, OpSig};

/// A deployment plugin: it advertises the attributes a `service { ... }` body of
/// its kind accepts.
pub trait Deployment {
    /// Stable identifier used to select this plugin, e.g. `docker`.
    const NAME: &'static str;

    fn attr_schema() -> AttrSchema;
}

/// A driver plugin: it advertises the action operations a `do { ... }` step can
/// invoke, e.g. `http` with `POST` / `GET` / `DELETE`.
pub trait Driver {
    /// Stable identifier used to select this plugin, e.g. `http`.
    const NAME: &'static str;

    fn signatures() -> Vec<OpSig>;
}

/// An observer plugin: it advertises the observables an `expect { ... }`
/// predicate can read, e.g. `mariadb` with `<table>.count`.
pub trait Observer {
    /// Stable identifier used to select this plugin, e.g. `mariadb`.
    const NAME: &'static str;

    fn signatures() -> Vec<OpSig>;
}
