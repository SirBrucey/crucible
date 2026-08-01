//! Fleet spec: services the framework brings up, faults, and observes.

use crate::plan;

/// One service of a fleet replica, as a deployment plugin needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    pub name: String,
    pub image: String,
    pub port: u16,
    /// Environment variables passed to the container, one per entry in `KEY=value` form.
    pub env: Vec<String>,
    /// Command to run inside the container as a HEALTHCHECK; empty means the image's
    /// built-in HEALTHCHECK is used.
    pub healthcheck: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fleet {
    pub services: Vec<Service>,
}

impl Fleet {
    #[must_use]
    pub fn new(services: Vec<Service>) -> Self {
        Self { services }
    }

    #[must_use]
    pub fn service(&self, name: &str) -> Option<&Service> {
        self.services.iter().find(|service| service.name == name)
    }
}

/// The `orders` example fleet: an HTTP API that accepts orders, a rabbitmq
/// broker, a mariadb database, and an inventory consumer that decrements
/// stock in response to `order.created` events.
#[must_use]
pub fn example() -> plan::Fleet {
    plan::Fleet {
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
) -> plan::Service {
    plan::Service {
        name: name.to_owned(),
        kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
        attrs: vec![
            ("image".to_owned(), plan::Value::Str(image.to_owned())),
            ("port".to_owned(), plan::Value::Int(port)),
            ("env".to_owned(), strs(env)),
            ("healthcheck".to_owned(), strs(healthcheck)),
        ],
    }
}

fn strs(values: &[&str]) -> plan::Value {
    plan::Value::List(
        values
            .iter()
            .map(|value| plan::Value::Str((*value).to_owned()))
            .collect(),
    )
}
