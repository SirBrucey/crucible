//! Fleet spec: services the framework brings up, faults, and observes.

pub struct Service {
    pub name: &'static str,
    pub image: &'static str,
    pub port: u16,
    /// Environment variables passed to the container, one per entry in `KEY=value` form.
    pub env: &'static [&'static str],
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
        },
        Service {
            name: "broker",
            image: "rabbitmq:3.13-management",
            port: 5672,
            env: &[],
        },
        Service {
            name: "db",
            image: "mariadb:11.4",
            port: 3306,
            env: &[
                "MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=yes",
                "MARIADB_DATABASE=orders",
            ],
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
        },
    ],
};
