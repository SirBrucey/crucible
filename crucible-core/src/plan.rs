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

impl Service {
    /// The named bring-up attribute, if the service declares it.
    #[must_use]
    pub fn attr(&self, name: &str) -> Option<&Value> {
        lookup(&self.attrs, name)
    }
}

/// The named entry of an attribute or body list.
#[must_use]
pub fn lookup<'a>(entries: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    entries
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
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

/// Read a value a plugin expects to be of a particular shape. The check pass has
/// already validated it against the plugin's schema, so a `None` here means the
/// plugin asked for something it never declared.
impl Value {
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) | Value::Ident(s) => Some(s),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            Value::Duration(d) => Some(*d),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(items) => Some(items),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(entries) => Some(entries),
            _ => None,
        }
    }

    /// The strings in a list value, or `None` if any entry is not a string.
    #[must_use]
    pub fn as_strs(&self) -> Option<Vec<&str>> {
        self.as_list()?.iter().map(Value::as_str).collect()
    }
}
