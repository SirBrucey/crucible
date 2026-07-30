//! The parsed form of a `.cru` file.

use std::time::Duration;

use crate::span::Spanned;

/// A parsed `.cru` file: one fleet and its scenarios.
#[derive(Clone, Debug, PartialEq)]
pub struct File {
    pub fleet: Spanned<Fleet>,
    pub scenarios: Vec<Spanned<Scenario>>,
}

/// A fleet: its name and the services it brings up.
#[derive(Clone, Debug, PartialEq)]
pub struct Fleet {
    pub name: Spanned<String>,
    pub services: Vec<Spanned<Service>>,
}

/// A service: its name and its bring-up attributes, a map the deployment plugin
/// interprets.
#[derive(Clone, Debug, PartialEq)]
pub struct Service {
    pub name: Spanned<String>,
    pub attrs: Spanned<Value>,
}

/// A scenario, identified by name.
#[derive(Clone, Debug, PartialEq)]
pub struct Scenario {
    pub name: Spanned<String>,
}

/// A literal or composite value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Bool(bool),
    Duration(Duration),
    Ident(String),
    List(Vec<Spanned<Value>>),
    Map(Vec<(Spanned<String>, Spanned<Value>)>),
}
