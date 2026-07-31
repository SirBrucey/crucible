//! Semantic checks on a parsed file: plugin resolution, service attributes, and
//! operation signatures.

use std::collections::HashMap;

use crucible_plugin::{
    AttrSchema, ClauseShape, CmpOp, HeadPattern, OpSig, Param, ParamType, Registry, ValueType,
};

use crate::{
    ast::{self, Clause, Fleet, OpCall, Predicate, Scenario, Value},
    diagnostics::Diag,
    span::{Span, Spanned},
};

/// Check `file` against `registry`, returning one diagnostic per semantic error.
#[must_use]
pub fn validate(file: &ast::File, registry: &Registry) -> Vec<Diag> {
    let mut validator = Validator {
        registry,
        diags: Vec::new(),
    };
    let services = validator.fleet(&file.fleet.node);
    for scenario in &file.scenarios {
        validator.scenario(&scenario.node, &services);
    }
    validator.diags
}

/// The plugins a service speaks.
struct ServiceModel {
    kinds: Vec<String>,
}

struct Validator<'a> {
    registry: &'a Registry,
    diags: Vec<Diag>,
}

impl<'a> Validator<'a> {
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

    /// Resolve the deployment plugin and validate each service's attributes,
    /// returning the services by name.
    fn fleet(&mut self, fleet: &Fleet) -> HashMap<String, ServiceModel> {
        let schema = self.registry.deployment(&fleet.deployment.node);
        if schema.is_none() {
            let known = sorted(
                self.registry
                    .deployment_names()
                    .into_iter()
                    .map(String::from),
            );
            self.error_suggesting(
                fleet.deployment.span,
                format!("unknown deployment plugin `{}`", fleet.deployment.node),
                "known deployments:",
                known,
            );
        }

        let mut services = HashMap::new();
        for service in &fleet.services {
            let kinds = self.service_kinds(&service.node);
            if let Some(schema) = schema {
                self.service_attrs(&service.node, schema);
            }
            services.insert(service.node.name.node.clone(), ServiceModel { kinds });
        }
        services
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

    /// Validate a service's attributes against the deployment schema, skipping the
    /// reserved `kinds`.
    fn service_attrs(&mut self, service: &ast::Service, schema: &AttrSchema) {
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
                let known = sorted(schema.attrs.iter().map(|decl| decl.name.clone()));
                self.error_suggesting(
                    key.span,
                    format!("unknown attribute `{}`", key.node),
                    "known attributes:",
                    known,
                );
            }
        }
        for decl in &schema.attrs {
            if decl.required && !entries.iter().any(|(key, _)| key.node == decl.name) {
                self.error(
                    service.name.span,
                    format!(
                        "service `{}` is missing required attribute `{}`",
                        service.name.node, decl.name
                    ),
                );
            }
        }
    }

    fn scenario(&mut self, scenario: &Scenario, services: &HashMap<String, ServiceModel>) {
        for step in &scenario.steps {
            self.do_step(step, services);
        }
        for predicate in &scenario.expect {
            self.predicate(predicate, services);
        }
    }

    /// Validate a `do` action against its driver's signature.
    fn do_step(&mut self, step: &Spanned<OpCall>, services: &HashMap<String, ServiceModel>) {
        let op = &step.node;
        let [driver, operation] = op.head.as_slice() else {
            self.error(step.span, "a `do` action names a driver and an operation");
            return;
        };
        let Some(signatures) = self.registry.driver(&driver.node) else {
            let known = sorted(self.registry.driver_names().into_iter().map(String::from));
            self.error_suggesting(
                driver.span,
                format!("unknown driver `{}`", driver.node),
                "known drivers:",
                known,
            );
            return;
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
            return;
        };
        self.check_args(step, sig, driver, services);
        self.check_clauses(op, sig);
    }

    /// Validate a `do` action's positional arguments against the operation's
    /// parameters.
    fn check_args(
        &mut self,
        step: &Spanned<OpCall>,
        sig: &OpSig,
        driver: &Spanned<String>,
        services: &HashMap<String, ServiceModel>,
    ) {
        let op = &step.node;
        let required = sig.params.iter().filter(|param| param.required).count();
        if op.args.len() < required || op.args.len() > sig.params.len() {
            self.error(
                step.span,
                format!(
                    "`{}` takes {} argument(s), got {}",
                    driver.node,
                    sig.params.len(),
                    op.args.len()
                ),
            );
            return;
        }
        for (arg, param) in op.args.iter().zip(&sig.params) {
            self.check_param(arg, param, driver, services);
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

    /// Validate an `expect` predicate: resolve the observable, then check the
    /// comparison against its result.
    fn predicate(
        &mut self,
        predicate: &Spanned<Predicate>,
        services: &HashMap<String, ServiceModel>,
    ) {
        let observable = &predicate.node.left.node;
        let Some((service_ref, path)) = observable.head.split_first() else {
            self.error(predicate.node.left.span, "an observable names a service");
            return;
        };
        let Some(service) = services.get(&service_ref.node) else {
            self.unknown_service(service_ref.span, &service_ref.node, services);
            return;
        };
        let Some(sig) = self.match_observable(service, path) else {
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
            return;
        };
        self.check_clauses(observable, sig);
        self.check_comparison(&predicate.node, sig);
    }

    /// The observer signature on one of the service's kinds whose head matches
    /// `path`.
    fn match_observable(
        &self,
        service: &ServiceModel,
        path: &[Spanned<String>],
    ) -> Option<&'a OpSig> {
        self.observables(service)
            .find(|sig| head_matches(&sig.head, path))
    }

    /// Every observable on the service's observer kinds.
    fn observables(&self, service: &ServiceModel) -> impl Iterator<Item = &'a OpSig> {
        let registry = self.registry;
        service
            .kinds
            .iter()
            .filter_map(move |kind| registry.observer(kind))
            .flat_map(<[OpSig]>::iter)
    }

    fn observable_labels(&self, service: &ServiceModel) -> Vec<String> {
        self.observables(service)
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

    /// Validate the clauses on an operation against the ones its signature allows.
    fn check_clauses(&mut self, op: &OpCall, sig: &OpSig) {
        for clause in &op.clauses {
            let (shape, what) = match &clause.node {
                Clause::Body(_) => (ClauseShape::Block, "body"),
                Clause::Where(_) => (ClauseShape::Filter, "where"),
            };
            if !sig.clauses.iter().any(|decl| decl.shape == shape) {
                self.error(clause.span, format!("this operation takes no `{what}`"));
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
        ValueType::Map => "a map",
        ValueType::ServiceRef => "a service name",
    }
}

#[cfg(test)]
mod tests {
    use super::validate;
    use crate::{diagnostics::Diag, lexer::lex, parser::parse};
    use crucible_plugin::Registry;

    fn diagnose(src: &str) -> Vec<Diag> {
        let (tokens, lex_errors) = lex(src);
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");
        let file = parse(tokens).expect("parses");
        validate(&file, &Registry::builtins())
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
          service api { kinds: [http], image: "api:1", port: 80 };
          service db  { kinds: [mariadb], image: "mariadb:11", port: 3306 };
        }
        scenario "s" {
          consistent_within: 10s;
          do { http POST api "/orders" body { item: "book", quantity: 1 } };
          expect { db.orders.count == 1; }
        }
    "#;

    #[test]
    fn the_example_shape_validates_clean() {
        assert!(diagnose(VALID).is_empty(), "{:?}", diagnose(VALID));
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
        assert!(!diagnose(src).is_empty());
        find(&diagnose(src), "missing required attribute `port`");
    }

    #[test]
    fn an_unknown_service_lists_the_defined_ones() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [http], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; do { http POST gateway "/x" }; }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "unknown service `gateway`");
        assert_eq!(diag.help.as_ref().unwrap().suggestions, ["api"]);
    }

    #[test]
    fn a_service_that_does_not_speak_the_driver_has_no_driver_to_suggest() {
        // api's only kind, mariadb, is an observer, so there is no driver to suggest.
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [mariadb], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; do { http POST api "/x" }; }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "does not speak `http`");
        let help = diag.help.as_ref().unwrap();
        assert!(help.suggestions.is_empty());
        assert!(help.lead.contains("no available drivers"));
    }

    #[test]
    fn an_unknown_observable_suggests_the_observables() {
        let src = r#"fleet "f" { deployment: docker; service db { kinds: [mariadb], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; expect { db.orders.rows == 1; } }"#;
        let diags = diagnose(src);
        let diag = find(&diags, "no observable `orders.rows`");
        assert_eq!(diag.help.as_ref().unwrap().suggestions, ["<table>.count"]);
        // The span covers the observable path, not the valid `db` service.
        assert_eq!(&src[diag.span.start..diag.span.end], "orders.rows");
    }
}
