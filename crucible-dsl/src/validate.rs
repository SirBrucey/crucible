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

    /// Report a reference to an undefined service, suggesting the defined ones.
    fn unknown_service(
        &mut self,
        span: Span,
        name: &str,
        services: &HashMap<String, ServiceModel>,
    ) {
        let mut diag = Diag::new(span, format!("unknown service `{name}`"));
        if !services.is_empty() {
            diag = diag.with_help("defined services:", service_names(services));
        }
        self.diags.push(diag);
    }

    /// Resolve the deployment plugin and validate each service's attributes,
    /// returning the services by name.
    fn fleet(&mut self, fleet: &Fleet) -> HashMap<String, ServiceModel> {
        let schema = self.registry.deployment(&fleet.deployment.node);
        if schema.is_none() {
            self.error(
                fleet.deployment.span,
                format!("unknown deployment plugin `{}`", fleet.deployment.node),
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
            match schema.attr(&key.node) {
                Some(decl) => self.check_type(value, &decl.ty),
                None => self.error(key.span, format!("unknown attribute `{}`", key.node)),
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
            self.error(driver.span, format!("unknown driver `{}`", driver.node));
            return;
        };
        let Some(sig) = signatures
            .iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Exact(name) if *name == operation.node))
        else {
            self.error(
                operation.span,
                format!("`{}` has no operation `{}`", driver.node, operation.node),
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
                    self.error(
                        arg.span,
                        format!("service `{name}` does not speak `{}`", driver.node),
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
            self.error(
                predicate.node.left.span,
                format!("no observable `{name}` on service `{}`", service_ref.node),
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
        let registry = self.registry;
        service
            .kinds
            .iter()
            .filter_map(|kind| registry.observer(kind))
            .flat_map(<[OpSig]>::iter)
            .find(|sig| head_matches(&sig.head, path))
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

fn service_names(services: &HashMap<String, ServiceModel>) -> Vec<String> {
    let mut names: Vec<String> = services.keys().cloned().collect();
    names.sort_unstable();
    names
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
    use crate::{lexer::lex, parser::parse};
    use crucible_plugin::Registry;

    fn diags(src: &str) -> Vec<String> {
        let (tokens, lex_errors) = lex(src);
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");
        let file = parse(tokens).expect("parses");
        validate(&file, &Registry::builtins())
            .into_iter()
            .map(|diag| diag.message)
            .collect()
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
        assert!(diags(VALID).is_empty(), "{:?}", diags(VALID));
    }

    #[test]
    fn an_unknown_deployment_is_reported() {
        let src = r#"fleet "f" { deployment: podman; service api { image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; }"#;
        assert!(
            diags(src)
                .iter()
                .any(|d| d.contains("deployment plugin `podman`"))
        );
    }

    #[test]
    fn an_unknown_attribute_is_reported() {
        let src = r#"fleet "f" { deployment: docker; service api { image: "x", port: 80, colour: "red" } }
                     scenario "s" { consistent_within: 1s; }"#;
        assert!(
            diags(src)
                .iter()
                .any(|d| d.contains("unknown attribute `colour`"))
        );
    }

    #[test]
    fn a_missing_required_attribute_is_reported() {
        let src = r#"fleet "f" { deployment: docker; service api { image: "x" } }
                     scenario "s" { consistent_within: 1s; }"#;
        assert!(
            diags(src)
                .iter()
                .any(|d| d.contains("missing required attribute `port`"))
        );
    }

    #[test]
    fn a_do_on_an_unknown_service_is_reported() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [http], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; do { http POST gateway "/x" }; }"#;
        assert!(
            diags(src)
                .iter()
                .any(|d| d.contains("unknown service `gateway`"))
        );
    }

    #[test]
    fn an_unknown_service_lists_the_defined_ones() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [http], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; do { http POST gateway "/x" }; }"#;
        let (tokens, _) = lex(src);
        let file = parse(tokens).expect("parses");
        let diags = validate(&file, &Registry::builtins());
        let unknown = diags
            .iter()
            .find(|diag| diag.message.contains("unknown service"))
            .expect("unknown service diagnostic");
        let help = unknown.help.as_ref().expect("help note");
        assert_eq!(help.suggestions, ["api"]);
    }

    #[test]
    fn a_service_that_does_not_speak_the_driver_is_reported() {
        let src = r#"fleet "f" { deployment: docker; service api { kinds: [mariadb], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; do { http POST api "/x" }; }"#;
        assert!(
            diags(src)
                .iter()
                .any(|d| d.contains("does not speak `http`"))
        );
    }

    #[test]
    fn an_unknown_observable_is_reported() {
        let src = r#"fleet "f" { deployment: docker; service db { kinds: [mariadb], image: "x", port: 80 } }
                     scenario "s" { consistent_within: 1s; expect { db.orders.rows == 1; } }"#;
        assert!(
            diags(src)
                .iter()
                .any(|d| d.contains("no observable `orders.rows`"))
        );
    }
}
