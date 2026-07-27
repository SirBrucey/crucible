//! Fleet spec: services the framework brings up, faults, and observes.

pub struct Service {
    pub name: &'static str,
    pub image: &'static str,
    pub port: u16,
    /// Environment variables passed to the container, one per entry in `KEY=value` form.
    pub env: &'static [&'static str],
    /// Command to run inside the container as a HEALTHCHECK; empty means the image's
    /// built-in HEALTHCHECK is used.
    pub healthcheck: &'static [&'static str],
}

pub struct Fleet {
    pub services: &'static [Service],
}

/// The `orders` example fleet: an HTTP API that accepts orders, a rabbitmq
/// broker, a mariadb database, and an inventory consumer that decrements
/// stock in response to `order.created` events.
pub const EXAMPLE: Fleet = Fleet {
    services: &[
        Service {
            name: "api",
            image: "crucible-example/orders-api:0.1",
            port: 8080,
            env: &[
                "DATABASE_URL=mysql://root@db:3306/orders",
                "BROKER_URL=amqp://broker:5672",
                "RUST_LOG=info",
            ],
            healthcheck: &["CMD", "curl", "-fsS", "http://127.0.0.1:8080/healthz"],
        },
        Service {
            name: "broker",
            image: "rabbitmq:3.13-management",
            port: 5672,
            env: &[],
            // Probe the AMQP port rather than run `rabbitmq-diagnostics`. That
            // command starts an Erlang node, and Docker runs healthchecks as the
            // image's default user (root here), so under a slow concurrent boot a
            // probe can create a root-owned /var/lib/rabbitmq/.erlang.cookie before
            // the broker (running as rabbitmq) writes its own; the broker then
            // cannot read the cookie and dies with `.erlang.cookie: eacces`. A bare
            // TCP connect touches no cookie and still gates on the listener.
            healthcheck: &["CMD", "bash", "-c", ": < /dev/tcp/127.0.0.1/5672"],
        },
        Service {
            name: "db",
            image: "mariadb:11.4",
            port: 3306,
            env: &[
                "MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=yes",
                "MARIADB_DATABASE=orders",
            ],
            healthcheck: &["CMD", "mariadb-admin", "ping", "-h", "127.0.0.1"],
        },
        Service {
            name: "inventory",
            image: "crucible-example/orders-inventory:0.1",
            port: 8081,
            env: &[
                "DATABASE_URL=mysql://root@db:3306/orders",
                "BROKER_URL=amqp://broker:5672",
                "RUST_LOG=info",
            ],
            healthcheck: &["CMD", "curl", "-fsS", "http://127.0.0.1:8081/healthz"],
        },
    ],
};
