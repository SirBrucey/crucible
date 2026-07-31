//! The lowered form of a `.cru` file: the runtime's input.

use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

use crate::schema::CmpOp;

/// A validated `.cru` file lowered to a fleet and its scenarios, with every
/// service, action, and observable carrying the plugin that serves it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Plan {
    pub fleet: Fleet,
    pub scenarios: Vec<Scenario>,
}

impl Plan {
    /// A content hash over the lowered plan, for the recording-cache drift key.
    #[must_use]
    pub fn spec_hash(&self) -> SpecHash {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        SpecHash(hasher.finish())
    }
}

/// A content hash of a [`Plan`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SpecHash(pub u64);

/// A fleet: the deployment plugin that brings it up and the services it runs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Fleet {
    pub name: String,
    pub deployment: String,
    pub services: Vec<Service>,
}

/// A service: the plugins it speaks and its bring-up attributes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Service {
    pub name: String,
    pub kinds: Vec<String>,
    pub attrs: Vec<(String, Value)>,
}

/// A scenario: its heal-phase deadline, driver steps, and settled-state checks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scenario {
    pub name: String,
    pub consistent_within: Duration,
    pub steps: Vec<Step>,
    pub checks: Vec<Check>,
}

/// A driver action, carrying the driver that runs it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Step {
    pub driver: String,
    pub operation: String,
    pub args: Vec<Value>,
    pub body: Option<Vec<(String, Value)>>,
}

/// A settled-state check, carrying the service and observer that answer it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Check {
    pub service: String,
    pub observer: String,
    pub observable: Vec<String>,
    pub filter: Option<(String, Value)>,
    pub op: CmpOp,
    pub value: Value,
}

/// A literal value carried by a plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Str(String),
    Int(i64),
    Bool(bool),
    Duration(Duration),
    Ident(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}
