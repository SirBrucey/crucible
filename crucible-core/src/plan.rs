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

/// The `orders` example: a fleet of an HTTP API that accepts orders, a rabbitmq
/// broker, a mariadb database, and an inventory consumer that decrements stock
/// in response to `order.created` events, and a scenario that places three
/// orders and expects all three to survive.
#[must_use]
pub fn example() -> Plan {
    Plan {
        fleet: example_fleet(),
        scenarios: vec![example_scenario()],
    }
}

fn example_scenario() -> Scenario {
    let order = |item: &str, quantity: i64| Step {
        driver: "http".into(),
        operation: "POST".into(),
        args: vec![Value::Ident("api".into()), Value::Str("/orders".into())],
        body: Some(vec![
            ("item".into(), Value::Str(item.into())),
            ("quantity".into(), Value::Int(quantity)),
        ]),
    };
    let level = |item: &str, level: i64| Check {
        service: "db".into(),
        observer: "mariadb".into(),
        observable: vec!["stock".into(), "select".into()],
        args: vec![Value::Ident("level".into())],
        filter: Some(("item".into(), Value::Str(item.into()))),
        op: CmpOp::Eq,
        value: Value::Int(level),
    };
    Scenario {
        name: "orders_durability".into(),
        consistent_within: Duration::from_secs(15),
        steps: vec![order("book", 4), order("pen", 10), order("mug", 1)],
        checks: vec![
            Check {
                service: "db".into(),
                observer: "mariadb".into(),
                observable: vec!["orders".into(), "count".into()],
                args: Vec::new(),
                filter: None,
                op: CmpOp::Eq,
                value: Value::Int(3),
            },
            level("book", 96),
            level("pen", 490),
            level("mug", 249),
        ],
    }
}

fn example_fleet() -> Fleet {
    Fleet {
        name: "orders".into(),
        deployment: "docker".into(),
        services: vec![
            service(
                "api",
                &["http"],
                "crucible-example/orders-api:0.1",
                8080,
                &[
                    "DATABASE_URL=mysql://root@db:3306/orders",
                    "BROKER_URL=amqp://broker:5672",
                    "RUST_LOG=info",
                ],
                &["CMD", "curl", "-fsS", "http://127.0.0.1:8080/healthz"],
            ),
            service(
                "broker",
                &["amqp"],
                "rabbitmq:3.13-management",
                5672,
                &[],
                // Probe the AMQP port rather than run `rabbitmq-diagnostics`. That
                // command starts an Erlang node, and Docker runs healthchecks as the
                // image's default user (root here), so under a slow concurrent boot a
                // probe can create a root-owned /var/lib/rabbitmq/.erlang.cookie before
                // the broker (running as rabbitmq) writes its own; the broker then
                // cannot read the cookie and dies with `.erlang.cookie: eacces`. A bare
                // TCP connect touches no cookie and still gates on the listener.
                &["CMD", "bash", "-c", ": < /dev/tcp/127.0.0.1/5672"],
            ),
            service(
                "db",
                &["mariadb"],
                "mariadb:11.4",
                3306,
                &[
                    "MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=yes",
                    "MARIADB_DATABASE=orders",
                ],
                &["CMD", "mariadb-admin", "ping", "-h", "127.0.0.1"],
            ),
            service(
                "inventory",
                &["amqp"],
                "crucible-example/orders-inventory:0.1",
                8081,
                &[
                    "DATABASE_URL=mysql://root@db:3306/orders",
                    "BROKER_URL=amqp://broker:5672",
                    "RUST_LOG=info",
                ],
                &["CMD", "curl", "-fsS", "http://127.0.0.1:8081/healthz"],
            ),
        ],
    }
}

fn service(
    name: &str,
    kinds: &[&str],
    image: &str,
    port: i64,
    env: &[&str],
    healthcheck: &[&str],
) -> Service {
    // An attribute an author would leave out is left out here too, so this and
    // the `.cru` describing the same fleet stay comparable.
    let mut attrs = vec![
        ("image".to_owned(), Value::Str(image.to_owned())),
        ("port".to_owned(), Value::Int(port)),
    ];
    if !env.is_empty() {
        attrs.push(("env".to_owned(), strs(env)));
    }
    if !healthcheck.is_empty() {
        attrs.push(("healthcheck".to_owned(), strs(healthcheck)));
    }
    Service {
        name: name.to_owned(),
        kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
        attrs,
    }
}

fn strs(values: &[&str]) -> Value {
    Value::List(
        values
            .iter()
            .map(|value| Value::Str((*value).to_owned()))
            .collect(),
    )
}
