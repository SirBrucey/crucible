//! Lowering a parsed file to a [`plan::Plan`], resolving plugins and validating
//! service attributes and operation signatures as it goes.

use std::collections::HashMap;

use crucible_core::{
    plan,
    schema::{CmpOp, HeadPattern, OpSig, Param, ParamType, ValueType},
};
use crucible_plugin::{Registry, registry::ServiceSchema};

use crate::{
    ast::{self, Clause, Fleet, OpCall, Predicate, Scenario, Value},
    diagnostics::Diag,
    span::{Span, Spanned},
};

/// Lower `file` to a [`plan::Plan`], or the diagnostics that prevent it.
///
/// # Errors
/// Returns one diagnostic per semantic error when the file does not lower.
pub fn lower(file: &ast::File, registry: &Registry) -> Result<plan::Plan, Vec<Diag>> {
    let mut lowerer = Lowerer {
        registry,
        diags: Vec::new(),
    };
    let (fleet, services) = lowerer.fleet(&file.fleet.node);
    let mut scenarios = Vec::new();
    for scenario in &file.scenarios {
        scenarios.push(lowerer.scenario(&scenario.node, &services));
    }
    if lowerer.diags.is_empty() {
        Ok(plan::Plan { fleet, scenarios })
    } else {
        Err(lowerer.diags)
    }
}

/// Check `file` against `registry`, returning one diagnostic per semantic error.
#[must_use]
pub fn validate(file: &ast::File, registry: &Registry) -> Vec<Diag> {
    lower(file, registry).err().unwrap_or_default()
}

/// The plugins a service speaks.
struct ServiceModel {
    kinds: Vec<String>,
}

struct Lowerer<'a> {
    registry: &'a Registry,
    diags: Vec<Diag>,
}

impl<'a> Lowerer<'a> {
    fn error(&mut self, span: Span, message: impl Into<String>) {
        self.diags.push(Diag::new(span, message));
    }

    /// Report an error, and if there are alternatives, list them as a help note.
    fn error_suggesting(
        &mut self,
        span: Span,
        message: impl Into<String>,
        lead: impl Into<String>,
        suggestions: Vec<String>,
    ) {
        let diag = Diag::new(span, message);
        let diag = if suggestions.is_empty() {
            diag
        } else {
            diag.with_help(lead, suggestions)
        };
        self.diags.push(diag);
    }

    /// Report an error listing `options` in green, or, when there are none, the
    /// `empty_note` as prose.
    fn error_options(
        &mut self,
        span: Span,
        message: impl Into<String>,
        lead: impl Into<String>,
        options: Vec<String>,
        empty_note: impl Into<String>,
    ) {
        let diag = if options.is_empty() {
            Diag::new(span, message).with_help(empty_note, Vec::new())
        } else {
            Diag::new(span, message).with_help(lead, options)
        };
        self.diags.push(diag);
    }

    /// Resolve the deployment plugin, lower each service, and return the plan
    /// fleet alongside the services by name.
    fn fleet(&mut self, fleet: &Fleet) -> (plan::Fleet, HashMap<String, ServiceModel>) {
        let schema = self.registry.deployment(&fleet.deployment.node);
        if schema.is_none() {
            let known = sorted(self.registry.deployment_names());
            self.error_suggesting(
                fleet.deployment.span,
                format!("unknown deployment plugin `{}`", fleet.deployment.node),
                "known deployments:",
                known,
            );
        }

        let mut services = HashMap::new();
        let mut plan_services = Vec::new();
        for service in &fleet.services {
            let kinds = self.service_kinds(&service.node);
            // Each plugin the service speaks reads attributes of its own, so a
            // service is checked against all of them together.
            match self.registry.service_schema(&fleet.deployment.node, &kinds) {
                Ok(schema) => self.service_attrs(&service.node, &schema),
                // The unknown deployment is already reported above; anything
                // else is the plugins disagreeing with each other.
                Err(e) if schema.is_some() => self.error(service.node.name.span, e.to_string()),
                Err(_) => {}
            }
            plan_services.push(plan::Service {
                name: service.node.name.node.clone(),
                kinds: kinds.clone(),
                attrs: lower_attrs(&service.node),
            });
            services.insert(service.node.name.node.clone(), ServiceModel { kinds });
        }

        let plan_fleet = plan::Fleet {
            name: fleet.name.node.clone(),
            deployment: fleet.deployment.node.clone(),
            services: plan_services,
        };
        (plan_fleet, services)
    }

    /// The plugin names in a service's `kinds` list, reporting a malformed one.
    fn service_kinds(&mut self, service: &ast::Service) -> Vec<String> {
        let Value::Map(entries) = &service.attrs.node else {
            return Vec::new();
        };
        let Some((_, value)) = entries.iter().find(|(key, _)| key.node == "kinds") else {
            return Vec::new();
        };
        let Value::List(items) = &value.node else {
            self.error(value.span, "`kinds` must be a list of plugin names");
            return Vec::new();
        };
        items
            .iter()
            .filter_map(|item| {
                if let Value::Ident(name) = &item.node {
                    Some(name.clone())
                } else {
                    self.error(item.span, "a kind must be a plugin name");
                    None
                }
            })
            .collect()
    }

    /// Validate a service's attributes against everything its plugins read,
    /// skipping the reserved `kinds`.
    fn service_attrs(&mut self, service: &ast::Service, schema: &ServiceSchema) {
        let Value::Map(entries) = &service.attrs.node else {
            return;
        };
        for (key, value) in entries {
            if key.node == "kinds" {
                continue;
            }
            if let Some(decl) = schema.attr(&key.node) {
                self.check_type(value, &decl.ty);
            } else {
                let known = sorted(schema.attrs().map(|decl| decl.name.clone()));
                self.error_suggesting(
                    key.span,
                    format!("unknown attribute `{}`", key.node),
                    "known attributes:",
                    known,
                );
            }
        }
        for decl in schema.attrs() {
            if decl.required && !entries.iter().any(|(key, _)| key.node == decl.name) {
                let reader = schema.reader(&decl.name).unwrap_or("a plugin");
                self.error(
                    service.name.span,
                    format!(
                        "service `{}` is missing `{}`, which `{reader}` reads",
                        service.name.node, decl.name
                    ),
                );
            }
        }
    }

    fn scenario(
        &mut self,
        scenario: &Scenario,
        services: &HashMap<String, ServiceModel>,
    ) -> plan::Scenario {
        let mut steps = Vec::new();
        for step in &scenario.steps {
            if let Some(step) = self.do_step(step, services) {
                steps.push(step);
            }
        }
        let mut checks = Vec::new();
        for predicate in &scenario.expect {
            if let Some(check) = self.predicate(predicate, services) {
                checks.push(check);
            }
        }
        // Every schedule waits this out, so a scenario that asks for longer than
        // a campaign can afford is refused rather than silently shortened.
        if scenario.consistent_within.node > crucible_core::MAX_CONSISTENT_WITHIN {
            self.error(
                scenario.consistent_within.span,
                format!(
                    "a fleet may be given at most {:?} to settle",
                    crucible_core::MAX_CONSISTENT_WITHIN
                ),
            );
        }
        plan::Scenario {
            name: scenario.name.node.clone(),
            consistent_within: scenario.consistent_within.node,
            steps,
            checks,
        }
    }

    /// Lower a `do` action against its driver's signature.
    fn do_step(
        &mut self,
        step: &Spanned<ast::Step>,
        services: &HashMap<String, ServiceModel>,
    ) -> Option<plan::Step> {
        let action = &step.node.action;
        let op = &action.node;
        let [driver, operation] = op.head.as_slice() else {
            self.error(step.span, "a `do` action names a driver and an operation");
            return None;
        };
        let Some(signatures) = self.registry.driver(&driver.node) else {
            let known = sorted(self.registry.driver_names());
            self.error_suggesting(
                driver.span,
                format!("unknown driver `{}`", driver.node),
                "known drivers:",
                known,
            );
            return None;
        };
        let Some(sig) = signatures
            .iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Exact(name) if *name == operation.node))
        else {
            let ops = sorted(signatures.iter().map(|sig| head_label(&sig.head)));
            self.error_suggesting(
                operation.span,
                format!("`{}` has no operation `{}`", driver.node, operation.node),
                "operations:",
                ops,
            );
            return None;
        };
        self.check_args(action, sig, &driver.node.clone(), driver, services);
        self.check_clauses(op, sig);
        if let (Some(stated), Some(outcome)) = (&step.node.expect, &sig.result) {
            self.check_type(stated, outcome);
        }
        Some(plan::Step {
            driver: driver.node.clone(),
            operation: operation.node.clone(),
            args: op.args.iter().map(|arg| lower_value(&arg.node)).collect(),
            body: body_of(op),
            expect: step
                .node
                .expect
                .as_ref()
                .map(|stated| lower_value(&stated.node)),
        })
    }

    /// Validate an operation's positional arguments against its parameters.
    /// `label` is what the author wrote, for the message; `kind` is the plugin
    /// answering it, which a service reference is checked against.
    fn check_args(
        &mut self,
        call: &Spanned<OpCall>,
        sig: &OpSig,
        label: &str,
        kind: &Spanned<String>,
        services: &HashMap<String, ServiceModel>,
    ) {
        let op = &call.node;
        let required = sig.params.iter().filter(|param| param.required).count();
        if op.args.len() < required || op.args.len() > sig.params.len() {
            self.error(
                call.span,
                format!(
                    "`{label}` takes {} argument(s), got {}",
                    sig.params.len(),
                    op.args.len()
                ),
            );
            return;
        }
        for (arg, param) in op.args.iter().zip(&sig.params) {
            self.check_param(arg, param, kind, services);
        }
    }

    fn check_param(
        &mut self,
        arg: &Spanned<Value>,
        param: &Param,
        driver: &Spanned<String>,
        services: &HashMap<String, ServiceModel>,
    ) {
        match param.ty {
            ParamType::ServiceRef => {
                let Value::Ident(name) = &arg.node else {
                    self.error(arg.span, "expected a service name");
                    return;
                };
                let Some(service) = services.get(name) else {
                    self.unknown_service(arg.span, name, services);
                    return;
                };
                if !service.kinds.contains(&driver.node) {
                    let drivers: Vec<String> = service
                        .kinds
                        .iter()
                        .filter(|kind| self.registry.driver(kind).is_some())
                        .cloned()
                        .collect();
                    self.error_options(
                        arg.span,
                        format!("service `{name}` does not speak `{}`", driver.node),
                        "drivers:",
                        sorted(drivers),
                        format!("no available drivers for `{name}`"),
                    );
                }
            }
            ParamType::Path | ParamType::Str => {
                if !matches!(arg.node, Value::Str(_)) {
                    self.error(arg.span, "expected a string");
                }
            }
            ParamType::Int => {
                if !matches!(arg.node, Value::Int(_)) {
                    self.error(arg.span, "expected an integer");
                }
            }
            ParamType::Ident => {
                if !matches!(arg.node, Value::Ident(_)) {
                    self.error(arg.span, "expected an identifier");
                }
            }
            ParamType::Matcher => {}
        }
    }

    /// Lower an `expect` predicate: resolve the observable, check the comparison,
    /// and bind the observer that answers it.
    fn predicate(
        &mut self,
        predicate: &Spanned<Predicate>,
        services: &HashMap<String, ServiceModel>,
    ) -> Option<plan::Check> {
        let observable = &predicate.node.left.node;
        let Some((service_ref, path)) = observable.head.split_first() else {
            self.error(predicate.node.left.span, "an observable names a service");
            return None;
        };
        let Some(service) = services.get(&service_ref.node) else {
            self.unknown_service(service_ref.span, &service_ref.node, services);
            return None;
        };
        let Some((observer, sig)) = self.match_observable(service, path) else {
            let name = path
                .iter()
                .map(|segment| segment.node.as_str())
                .collect::<Vec<_>>()
                .join(".");
            let span = path_span(path).unwrap_or(predicate.node.left.span);
            let known = sorted(self.observable_labels(service));
            self.error_options(
                span,
                format!("no observable `{name}` on service `{}`", service_ref.node),
                "observables:",
                known,
                format!("no available observables for `{}`", service_ref.node),
            );
            return None;
        };
        let path_names: Vec<String> = path.iter().map(|segment| segment.node.clone()).collect();
        let kind = Spanned::new(
            observer.clone(),
            path_span(path).unwrap_or(predicate.node.left.span),
        );
        self.check_args(
            &predicate.node.left,
            sig,
            &path_names.join("."),
            &kind,
            services,
        );
        self.check_clauses(observable, sig);
        self.check_comparison(&predicate.node, sig);
        Some(plan::Check {
            service: service_ref.node.clone(),
            observer,
            observable: path_names,
            args: observable
                .args
                .iter()
                .map(|a| lower_value(&a.node))
                .collect(),
            filter: where_of(observable),
            op: plugin_cmp(predicate.node.op.node),
            value: lower_value(&predicate.node.right.node),
        })
    }

    /// The observer kind on the service and the signature whose head matches
    /// `path`.
    fn match_observable(
        &self,
        service: &ServiceModel,
        path: &[Spanned<String>],
    ) -> Option<(String, &'a OpSig)> {
        let registry = self.registry;
        service.kinds.iter().find_map(|kind| {
            let sig = registry
                .observer(kind)?
                .iter()
                .find(|sig| head_matches(&sig.head, path))?;
            Some((kind.clone(), sig))
        })
    }

    fn observable_labels(&self, service: &ServiceModel) -> Vec<String> {
        let registry = self.registry;
        service
            .kinds
            .iter()
            .filter_map(|kind| registry.observer(kind))
            .flat_map(<[OpSig]>::iter)
            .map(|sig| head_label(&sig.head))
            .collect()
    }

    /// Report a reference to an undefined service, suggesting the defined ones.
    fn unknown_service(
        &mut self,
        span: Span,
        name: &str,
        services: &HashMap<String, ServiceModel>,
    ) {
        self.error_suggesting(
            span,
            format!("unknown service `{name}`"),
            "defined services:",
            sorted(services.keys().cloned()),
        );
    }

    fn check_comparison(&mut self, predicate: &Predicate, sig: &OpSig) {
        let Some(result) = &sig.result else {
            self.error(
                predicate.left.span,
                "this observable has no value to compare",
            );
            return;
        };
        if !sig.cmp_ops.contains(&plugin_cmp(predicate.op.node)) {
            self.error(predicate.op.span, "this comparison is not allowed here");
        }
        self.check_type(&predicate.right, result);
    }

    /// Validate the clauses on an operation against the ones its signature
    /// allows, by the keyword the author wrote rather than its shape: two
    /// clauses can share a shape and mean different things.
    fn check_clauses(&mut self, op: &OpCall, sig: &OpSig) {
        for clause in &op.clauses {
            let keyword = match &clause.node {
                Clause::Body(_) => "body",
                Clause::Where(_) => "where",
            };
            if !sig.clauses.iter().any(|decl| decl.keyword == keyword) {
                self.error(clause.span, format!("this operation takes no `{keyword}`"));
            }
        }
    }

    fn check_type(&mut self, value: &Spanned<Value>, ty: &ValueType) {
        if let ValueType::List(inner) = ty {
            let Value::List(items) = &value.node else {
                self.error(value.span, "expected a list");
                return;
            };
            for item in items {
                self.check_type(item, inner);
            }
            return;
        }
        if let ValueType::MapOf(inner) = ty {
            let Value::Map(entries) = &value.node else {
                self.error(value.span, "expected a map");
                return;
            };
            for (_, entry) in entries {
                self.check_type(entry, inner);
            }
            return;
        }
        let matches = matches!(
            (ty, &value.node),
            (ValueType::Str, Value::Str(_))
                | (ValueType::Int, Value::Int(_))
                | (ValueType::Bool, Value::Bool(_))
                | (ValueType::Duration, Value::Duration(_))
                | (ValueType::Map, Value::Map(_))
                | (ValueType::ServiceRef, Value::Ident(_))
        );
        if !matches {
            self.error(value.span, format!("expected {}", type_name(ty)));
        }
    }
}

fn lower_value(value: &Value) -> plan::Value {
    match value {
        Value::Null => plan::Value::Null,
        Value::Str(s) => plan::Value::Str(s.clone()),
        Value::Int(n) => plan::Value::Int(*n),
        Value::Bool(b) => plan::Value::Bool(*b),
        Value::Duration(d) => plan::Value::Duration(*d),
        Value::Ident(s) => plan::Value::Ident(s.clone()),
        Value::List(items) => {
            plan::Value::List(items.iter().map(|item| lower_value(&item.node)).collect())
        }
        Value::Map(entries) => plan::Value::Map(lower_pairs(entries)),
    }
}

fn lower_pairs(entries: &[(Spanned<String>, Spanned<Value>)]) -> Vec<(String, plan::Value)> {
    entries
        .iter()
        .map(|(key, value)| (key.node.clone(), lower_value(&value.node)))
        .collect()
}

/// A service's bring-up attributes, without the reserved `kinds`.
fn lower_attrs(service: &ast::Service) -> Vec<(String, plan::Value)> {
    let Value::Map(entries) = &service.attrs.node else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|(key, _)| key.node != "kinds")
        .map(|(key, value)| (key.node.clone(), lower_value(&value.node)))
        .collect()
}

fn body_of(op: &OpCall) -> Option<Vec<(String, plan::Value)>> {
    op.clauses.iter().find_map(|clause| match &clause.node {
        Clause::Body(value) => Some(match &value.node {
            Value::Map(entries) => lower_pairs(entries),
            _ => Vec::new(),
        }),
        Clause::Where(_) => None,
    })
}

fn where_of(op: &OpCall) -> Option<(String, plan::Value)> {
    op.clauses.iter().find_map(|clause| match &clause.node {
        Clause::Where(filter) => {
            Some((filter.column.node.clone(), lower_value(&filter.value.node)))
        }
        Clause::Body(_) => None,
    })
}

fn sorted(names: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut names: Vec<String> = names.into_iter().collect();
    names.sort_unstable();
    names
}

/// A readable label for an operation head, e.g. `POST` or `<table>.count`.
fn head_label(head: &HeadPattern) -> String {
    match head {
        HeadPattern::Exact(name) => name.clone(),
        HeadPattern::Wildcard { segment, tail } => format!("<{segment}>.{tail}"),
    }
}

/// The span covering a dotted path's segments, or `None` if it is empty.
fn path_span(path: &[Spanned<String>]) -> Option<Span> {
    match (path.first(), path.last()) {
        (Some(first), Some(last)) => Some(Span::new(first.span.start, last.span.end)),
        _ => None,
    }
}

fn head_matches(pattern: &HeadPattern, path: &[Spanned<String>]) -> bool {
    match pattern {
        HeadPattern::Exact(name) => path.len() == 1 && path[0].node == *name,
        HeadPattern::Wildcard { tail, .. } => path.len() == 2 && path[1].node == *tail,
    }
}

fn plugin_cmp(op: ast::CmpOp) -> CmpOp {
    match op {
        ast::CmpOp::Eq => CmpOp::Eq,
        ast::CmpOp::Ne => CmpOp::Ne,
        ast::CmpOp::Lt => CmpOp::Lt,
        ast::CmpOp::Le => CmpOp::Le,
        ast::CmpOp::Gt => CmpOp::Gt,
        ast::CmpOp::Ge => CmpOp::Ge,
    }
}

fn type_name(ty: &ValueType) -> &'static str {
    match ty {
        ValueType::Str => "a string",
        ValueType::Int => "an integer",
        ValueType::Bool => "a boolean",
        ValueType::Duration => "a duration",
        ValueType::List(_) => "a list",
        ValueType::MapOf(_) | ValueType::Map => "a map",
        ValueType::ServiceRef => "a service name",
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{lower, validate};
    use crate::{ast, diagnostics::Diag, lexer::lex, parser::parse};
    use crucible_core::plan;
    use crucible_plugin::Registry;

    fn parse_file(src: &str) -> ast::File {
        let (tokens, lex_errors) = lex(src);
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");
        parse(tokens).expect("parses")
    }

    fn diagnose(src: &str) -> Vec<Diag> {
        validate(&parse_file(src), &Registry::builtins())
    }

    fn lower_ok(src: &str) -> plan::Plan {
        lower(&parse_file(src), &Registry::builtins()).expect("lowers")
    }

    fn find<'a>(diags: &'a [Diag], needle: &str) -> &'a Diag {
        diags
            .iter()
            .find(|diag| diag.message.contains(needle))
            .unwrap_or_else(|| panic!("no diagnostic matching {needle:?} in {diags:?}"))
    }

    const VALID: &str = r#"
        fleet "orders" {
          deployment: docker;
          service api { kinds: [http], image: "api:1", ports: { http: 80 } };
          service db  { kinds: [mariadb], image: "mariadb:11", ports: { mariadb: 3306 } };
        }
        scenario "s" {
          consistent_within: 10s;
          do { http POST api "/orders" body { item: "book", quantity: 1 } };
          expect { db.orders.count where item = "book" == 1; }
        }
    "#;

    #[test]
    fn the_example_shape_validates_clean() {
        assert!(diagnose(VALID).is_empty(), "{:?}", diagnose(VALID));
    }

    #[test]
    fn a_body_may_state_a_value_is_absent() {
        let plan = lower_ok(&VALID.replace(r#"item: "book", quantity: 1"#, "item: null"));
        let body = plan.scenarios[0].steps[0]
            .body
            .as_ref()
            .expect("the step carries a body");
        assert_eq!(body[0].1, plan::Value::Null);
    }

    #[test]
    fn an_attribute_of_a_stated_type_may_not_be_absent() {
        let diags = diagnose(&VALID.replace("http: 80", "http: null"));
        find(&diags, "expected an integer");
    }

    #[test]
    fn the_example_lowers_to_a_plan() {
        let plan = lower_ok(VALID);
        assert_eq!(plan.fleet.name, "orders");
        assert_eq!(plan.fleet.deployment, "docker");
        assert_eq!(plan.fleet.services.len(), 2);

        let scenario = &plan.scenarios[0];
        assert_eq!(scenario.consistent_within, Duration::from_secs(10));

        let step = &scenario.steps[0];
        assert_eq!(step.driver, "http");
        assert_eq!(step.operation, "POST");
        assert!(step.body.is_some());

        // The check binds `db`'s kind to the mariadb observer, and keeps the filter.
        let check = &scenario.checks[0];
        assert_eq!(check.service, "db");
        assert_eq!(check.observer, "mariadb");
        assert_eq!(check.observable, ["orders", "count"]);
        assert_eq!(
            check.filter,
            Some(("item".to_string(), plan::Value::Str("book".to_string()))),
        );
    }

    #[test]
    fn lowering_fails_on_a_semantic_error() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [http], image: "x", ports: { http: 80 } } }
                     scenario "s" { consistent_within: 1s; expect { db.orders.count == 1; } }"#;
        let error = lower(&parse_file(src), &Registry::builtins()).unwrap_err();
        assert!(
            error
                .iter()
                .any(|diag| diag.message.contains("unknown service"))
        );
    }

    #[test]
    fn consistent_within_must_be_within_bound() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [http], image: "x", ports: { http: 80 } } }
                     scenario "s" { consistent_within: 10m; }"#;
        let diags = diagnose(src);
        find(&diags, "may be given at most");
    }

    #[test]
    fn the_spec_hash_is_stable_and_content_sensitive() {
        assert_eq!(lower_ok(VALID).spec_hash(), lower_ok(VALID).spec_hash());
        let renamed = lower_ok(&VALID.replace(r#"scenario "s""#, r#"scenario "t""#));
        assert_ne!(lower_ok(VALID).spec_hash(), renamed.spec_hash());
    }

    #[test]
    fn an_unknown_deployment_suggests_the_known_ones() {
        let src = r#"fleet "f" { deployment: podman; service api { image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "deployment plugin `podman`");
        assert_eq!(diag.help.as_ref().unwrap().suggestions, ["docker"]);
    }

    #[test]
    fn an_unknown_attribute_suggests_the_schema() {
        let src = r#"fleet "f" { deployment: docker; service api { image: "x", port: 80, colour: "red" } }
                     scenario "s" { consistent_within: 1s; }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "unknown attribute `colour`");
        assert!(
            diag.help
                .as_ref()
                .unwrap()
                .suggestions
                .contains(&"image".to_string())
        );
    }

    #[test]
    fn a_missing_required_attribute_is_reported() {
        let src = r#"fleet "f" { deployment: docker; service api { image: "x" } }
                     scenario "s" { consistent_within: 1s; }"#;
        // Naming the plugin that reads it says where the requirement came from,
        // which matters once several plugins read a service's attributes.
        let diags = diagnose(src);
        let diag = find(&diags, "is missing `ports`");
        assert!(diag.message.contains("docker"), "{}", diag.message);
    }

    #[test]
    fn an_unknown_service_lists_the_defined_ones() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [http], image: "x", ports: { http: 80 } } }
                     scenario "s" { consistent_within: 1s; do { http POST gateway "/x" }; }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "unknown service `gateway`");
        assert_eq!(diag.help.as_ref().unwrap().suggestions, ["api"]);
    }

    #[test]
    fn a_service_that_does_not_speak_the_driver_has_no_driver_to_suggest() {
        // api's only kind, mariadb, is an observer, so there is no driver to suggest.
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [mariadb], image: "x", ports: { mariadb: 80 } } }
                     scenario "s" { consistent_within: 1s; do { http POST api "/x" }; }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "does not speak `http`");
        let help = diag.help.as_ref().unwrap();
        assert!(help.suggestions.is_empty());
        assert!(help.lead.contains("no available drivers"));
    }

    #[test]
    fn an_unknown_observable_suggests_the_observables() {
        let src = r#"fleet "f" { deployment: docker; service db { kinds: [mariadb], image: "x", ports: { mariadb: 80 } } }
                     scenario "s" { consistent_within: 1s; expect { db.orders.rows == 1; } }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "no observable `orders.rows`");
        assert_eq!(
            diag.help.as_ref().unwrap().suggestions,
            ["<table>.count", "<table>.select"],
        );
        // The span covers the observable path, not the valid `db` service.
        assert_eq!(&src[diag.span.start..diag.span.end], "orders.rows");
    }
}
