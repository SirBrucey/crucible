//! Fleet spec: services the framework brings up, faults, and observes.

pub struct Service {
    pub name: &'static str,
    pub image: &'static str,
    pub port: u16,
}

pub struct Fleet {
    pub services: &'static [Service],
}

/// Hardcoded example fleet: an API, a broker, a database, and a downstream consumer.
// FIXME(#76): swap the placeholder public images for a real modelled fleet.
pub const EXAMPLE: Fleet = Fleet {
    services: &[
        Service {
            name: "api",
            image: "nginx:alpine",
            port: 80,
        },
        Service {
            name: "broker",
            image: "rabbitmq:3.13-management",
            port: 5672,
        },
        Service {
            name: "db",
            image: "mariadb:11.4",
            port: 3306,
        },
        Service {
            name: "downstream",
            image: "nginx:alpine",
            port: 80,
        },
    ],
};
