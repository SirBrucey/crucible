//! What a scheduler reads: a fleet to break and a scenario to run against it.

use std::time::Duration;

use crucible_core::{plan, schema::CmpOp};

fn service(name: &str, kind: &str) -> plan::Service {
    plan::Service {
        name: name.to_owned(),
        kinds: vec![kind.to_owned()],
        attrs: Vec::new(),
    }
}

pub(super) fn fleet() -> plan::Fleet {
    plan::Fleet {
        name: "f".into(),
        deployment: "docker".into(),
        services: vec![service("api", "http"), service("db", "mariadb")],
    }
}

pub(super) fn scenario() -> plan::Scenario {
    plan::Scenario {
        name: "s".into(),
        budget: None,
        consistent_within: Duration::from_secs(15),
        steps: vec![plan::Step {
            driver: "http".into(),
            operation: "POST".into(),
            args: vec![
                plan::Value::Ident("api".into()),
                plan::Value::Str("/orders".into()),
            ],
            blocks: std::collections::BTreeMap::new(),
            expect: None,
        }],
        checks: vec![plan::Check {
            service: "db".into(),
            observer: "mariadb".into(),
            observable: vec!["orders".into(), "count".into()],
            args: Vec::new(),
            filter: None,
            clauses: std::collections::BTreeMap::new(),
            op: CmpOp::Eq,
            value: plan::Value::Int(1),
        }],
    }
}
