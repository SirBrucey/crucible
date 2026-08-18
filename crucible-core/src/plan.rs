//! The lowered form of a `.cru` file: the runtime's input.

use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

use crate::schema::CmpOp;

/// A validated `.cru` file lowered to a fleet and its scenarios, with every
/// service, action, and observable carrying the plugin that serves it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct SpecHash(pub u64);

/// A fleet: the deployment plugin that brings it up and the services it runs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Fleet {
    pub name: String,
    pub deployment: String,
    pub services: Vec<Service>,
}

/// A service: the plugins it speaks and its bring-up attributes.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
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
fn lookup<'a>(entries: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    entries
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

/// A scenario: its heal-phase deadline, driver steps, and settled-state checks.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Scenario {
    pub name: String,
    /// How long the campaign may run. `None` is unbounded, so every schedule is
    /// run.
    pub budget: Option<Duration>,
    pub consistent_within: Duration,
    pub steps: Vec<Step>,
    pub checks: Vec<Check>,
}

/// A driver action, carrying the driver that runs it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Step {
    pub driver: String,
    pub operation: String,
    pub args: Vec<Value>,
    pub body: Option<Vec<(String, Value)>>,
    /// What the author says this produces, in the driver's own terms. None
    /// leaves the driver's default for the operation standing.
    pub expect: Option<Value>,
}

/// A settled-state check, carrying the service and observer that answer it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Check {
    pub service: String,
    pub observer: String,
    pub observable: Vec<String>,
    /// The observable's positional arguments, as its plugin declared them.
    pub args: Vec<Value>,
    pub filter: Option<(String, Value)>,
    pub op: CmpOp,
    pub value: Value,
}

impl Check {
    /// The observable as the scenario spells it, for a verdict to point at.
    #[must_use]
    pub fn observable(&self) -> String {
        let mut spelling = vec![self.observable.join(".")];
        spelling.extend(self.args.iter().map(ToString::to_string));
        if let Some((column, value)) = &self.filter {
            spelling.push(format!("where {column} = {value}"));
        }
        spelling.join(" ")
    }

    /// The whole clause as the scenario spells it, observable and all.
    #[must_use]
    pub fn stated(&self) -> String {
        format!("{} {} {}", self.observable(), self.op, self.value)
    }
}

/// A literal value carried by a plan.
///
/// The `as_*` accessors mirror [`crate::schema::ValueType`], one per shape a
/// plugin can declare. The check pass has already validated a value against the
/// schema, so a `None` means the plugin asked for a shape it never declared.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Value {
    /// Stated, and stated to be absent.
    Null,
    Str(String),
    Int(i64),
    Bool(bool),
    Duration(Duration),
    Ident(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}

/// As an author would have written it, so a verdict can quote a reading back
/// in the terms of the scenario it came from.
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Str(s) => write!(f, "{s:?}"),
            Value::Int(n) => write!(f, "{n}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Duration(d) => write!(f, "{d:?}"),
            Value::Ident(name) => write!(f, "{name}"),
            Value::List(items) => {
                let items: Vec<String> = items.iter().map(ToString::to_string).collect();
                write!(f, "[{}]", items.join(", "))
            }
            Value::Map(entries) => {
                let entries: Vec<String> = entries
                    .iter()
                    .map(|(key, value)| format!("{key}: {value}"))
                    .collect();
                write!(f, "{{ {} }}", entries.join(", "))
            }
        }
    }
}

impl Value {
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// The service this value names, for a value declared as a service
    /// reference.
    #[must_use]
    pub fn as_service_ref(&self) -> Option<&str> {
        match self {
            Value::Ident(s) => Some(s),
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
