//! Recursive-descent parser: builds the file AST from a token stream, recovering
//! at statement and block boundaries so one error does not abort the parse.

use crate::{
    ast::{Clause, CmpOp, File, Filter, Fleet, OpCall, Predicate, Scenario, Service, Value},
    diagnostics::Diag,
    lexer::{Token, TokenKind},
    span::{Span, Spanned},
};

/// Parse `tokens` (from a clean lex) into a [`File`], or the diagnostics that
/// prevented it.
///
/// # Errors
/// Returns the collected diagnostics if the file does not parse.
pub fn parse(tokens: Vec<Token>) -> Result<File, Vec<Diag>> {
    let mut parser = Parser::new(tokens);
    match parser.file() {
        Some(file) if parser.errors.is_empty() => Ok(file),
        _ => Err(parser.errors),
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    errors: Vec<Diag>,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            errors: Vec::new(),
        }
    }

    // Token cursor. `tokens` always ends in `Eof`, and `advance` stops there, so
    // `pos` is always in bounds.

    fn peek(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    fn peek_span(&self) -> Span {
        self.tokens[self.pos].span
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), TokenKind::Eof)
    }

    fn advance(&mut self) {
        if !self.at_eof() {
            self.pos += 1;
        }
    }

    fn at(&self, kind: &TokenKind) -> bool {
        self.peek() == kind
    }

    fn at_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), TokenKind::Ident(s) if s == kw)
    }

    fn consume(&mut self, kind: &TokenKind) -> bool {
        let hit = self.at(kind);
        if hit {
            self.advance();
        }
        hit
    }

    fn consume_kw(&mut self, kw: &str) -> bool {
        let hit = self.at_kw(kw);
        if hit {
            self.advance();
        }
        hit
    }

    // Diagnostics and recovery.

    fn error(&mut self, span: Span, message: impl Into<String>) {
        self.errors.push(Diag::new(span, message));
    }

    fn error_here(&mut self, message: impl Into<String>) {
        let span = self.peek_span();
        self.error(span, message);
    }

    /// Skip tokens until `sync`, a closing `}`, or end of input, so the next
    /// well-formed construct can be parsed after an error.
    fn recover_to(&mut self, sync: &TokenKind) {
        while !self.at_eof() && !self.at(&TokenKind::RBrace) && !self.at(sync) {
            self.advance();
        }
    }

    fn expect(&mut self, kind: &TokenKind, what: &str) -> bool {
        let hit = self.consume(kind);
        if !hit {
            self.error_here(format!("expected {what}"));
        }
        hit
    }

    fn expect_ident(&mut self, what: &str) -> Option<Spanned<String>> {
        let span = self.peek_span();
        if let TokenKind::Ident(s) = self.peek() {
            let s = s.clone();
            self.advance();
            Some(Spanned::new(s, span))
        } else {
            self.error_here(format!("expected {what}"));
            None
        }
    }

    fn expect_str(&mut self, what: &str) -> Option<Spanned<String>> {
        let span = self.peek_span();
        if let TokenKind::Str(s) = self.peek() {
            let s = s.clone();
            self.advance();
            Some(Spanned::new(s, span))
        } else {
            self.error_here(format!("expected {what}"));
            None
        }
    }

    // Grammar.

    fn file(&mut self) -> Option<File> {
        let fleet = self.fleet()?;
        let mut scenarios = Vec::new();
        while !self.at_eof() {
            if self.at_kw("scenario") {
                match self.scenario() {
                    Some(scenario) => scenarios.push(scenario),
                    None => break,
                }
            } else {
                self.error_here("expected `scenario` or end of file");
                break;
            }
        }
        // A file with nothing to run is not something a campaign can be asked
        // to run, so say so here rather than at the point of running it. Only
        // when nothing else went wrong: a scenario that failed to parse has
        // already said why it is missing.
        if scenarios.is_empty() && self.errors.is_empty() {
            self.error_here("expected at least one `scenario`");
        }
        Some(File { fleet, scenarios })
    }

    fn fleet(&mut self) -> Option<Spanned<Fleet>> {
        let start = self.peek_span();
        if !self.consume_kw("fleet") {
            self.error(start, "expected `fleet`");
            return None;
        }
        let name = self.expect_str("a fleet name")?;
        self.expect(&TokenKind::LBrace, "`{`");
        let mut deployment = None;
        let mut services = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBrace) {
            if self.at_kw("deployment") {
                if let Some(plugin) = self.deployment_stmt() {
                    deployment = Some(plugin);
                }
            } else if self.at_kw("service") {
                if let Some(service) = self.service() {
                    services.push(service);
                } else {
                    self.recover_to(&TokenKind::Semi);
                    self.consume(&TokenKind::Semi);
                }
            } else {
                self.error_here("expected `deployment` or `service`");
                self.recover_to(&TokenKind::Semi);
                self.consume(&TokenKind::Semi);
            }
        }
        let end = self.peek_span();
        self.expect(&TokenKind::RBrace, "`}`");
        let Some(deployment) = deployment else {
            self.error(name.span, "fleet is missing `deployment`");
            return None;
        };
        Some(Spanned::new(
            Fleet {
                name,
                deployment,
                services,
            },
            Span::new(start.start, end.end),
        ))
    }

    /// Parse `deployment: <plugin>;`, the plugin that brings the fleet up.
    fn deployment_stmt(&mut self) -> Option<Spanned<String>> {
        self.consume_kw("deployment");
        self.expect(&TokenKind::Colon, "`:`");
        let plugin = self.expect_ident("a deployment plugin name")?;
        self.consume(&TokenKind::Semi);
        Some(plugin)
    }

    fn service(&mut self) -> Option<Spanned<Service>> {
        let start = self.peek_span();
        if !self.consume_kw("service") {
            self.error(start, "expected `service`");
            return None;
        }
        let name = self.expect_ident("a service name")?;
        let attrs = self.map();
        let span = Span::new(start.start, attrs.span.end);
        self.consume(&TokenKind::Semi);
        Some(Spanned::new(Service { name, attrs }, span))
    }

    fn scenario(&mut self) -> Option<Spanned<Scenario>> {
        let start = self.peek_span();
        self.consume_kw("scenario");
        let name = self.expect_str("a scenario name")?;
        self.expect(&TokenKind::LBrace, "`{`");
        let mut consistent_within = None;
        let mut steps = Vec::new();
        let mut expect = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBrace) {
            if self.at_kw("consistent_within") {
                if let Some(deadline) = self.consistent_within_stmt() {
                    consistent_within = Some(deadline);
                }
            } else if self.at_kw("do") {
                if let Some(step) = self.do_step() {
                    steps.push(step);
                } else {
                    self.recover_to(&TokenKind::Semi);
                    self.consume(&TokenKind::Semi);
                }
            } else if self.at_kw("expect") {
                expect.append(&mut self.expect_block());
            } else {
                self.error_here("expected `consistent_within`, `do`, or `expect`");
                self.recover_to(&TokenKind::Semi);
                self.consume(&TokenKind::Semi);
            }
        }
        let end = self.peek_span();
        self.expect(&TokenKind::RBrace, "`}`");
        let Some(consistent_within) = consistent_within else {
            self.error(name.span, "scenario is missing `consistent_within`");
            return None;
        };
        Some(Spanned::new(
            Scenario {
                name,
                consistent_within,
                steps,
                expect,
            },
            Span::new(start.start, end.end),
        ))
    }

    /// Parse `consistent_within: <duration>;`, the scenario's heal-phase deadline.
    fn consistent_within_stmt(&mut self) -> Option<Spanned<std::time::Duration>> {
        self.consume_kw("consistent_within");
        self.expect(&TokenKind::Colon, "`:`");
        let value = self.value()?;
        self.consume(&TokenKind::Semi);
        if let Value::Duration(d) = value.node {
            Some(Spanned::new(d, value.span))
        } else {
            self.error(
                value.span,
                "`consistent_within` expects a duration like `30s`",
            );
            None
        }
    }

    /// Parse `do { <operation> };`, one driver step.
    fn do_step(&mut self) -> Option<Spanned<OpCall>> {
        let start = self.peek_span();
        self.consume_kw("do");
        self.expect(&TokenKind::LBrace, "`{`");
        let op = self.op_call()?;
        let end = self.peek_span();
        self.expect(&TokenKind::RBrace, "`}`");
        self.consume(&TokenKind::Semi);
        Some(Spanned::new(op.node, Span::new(start.start, end.end)))
    }

    /// Parse an action operation: a `driver op` head, positional arguments, and
    /// clauses (`body { ... }`).
    fn op_call(&mut self) -> Option<Spanned<OpCall>> {
        let driver = self.expect_ident("a driver name")?;
        let op = self.expect_ident("an operation")?;
        let start = driver.span.start;
        let mut end = op.span.end;
        let head = vec![driver, op];
        let mut args = Vec::new();
        let mut clauses = Vec::new();
        loop {
            if self.at_kw("body") {
                let clause = self.body_clause();
                end = clause.span.end;
                clauses.push(clause);
            } else if self.is_value_start() {
                let Some(value) = self.value() else { break };
                end = value.span.end;
                args.push(value);
            } else {
                break;
            }
        }
        Some(Spanned::new(
            OpCall {
                head,
                args,
                clauses,
            },
            Span::new(start, end),
        ))
    }

    fn body_clause(&mut self) -> Spanned<Clause> {
        let start = self.peek_span();
        self.consume_kw("body");
        let map = self.map();
        let span = Span::new(start.start, map.span.end);
        Spanned::new(Clause::Body(map), span)
    }

    /// Whether the next token can begin a value (and so a positional argument).
    fn is_value_start(&self) -> bool {
        matches!(
            self.peek(),
            TokenKind::Str(_)
                | TokenKind::Int(_)
                | TokenKind::Duration(_)
                | TokenKind::Ident(_)
                | TokenKind::LBracket
                | TokenKind::LBrace
        )
    }

    /// Parse `expect { <predicate>; ... }`, the settled-state expectation.
    fn expect_block(&mut self) -> Vec<Spanned<Predicate>> {
        self.consume_kw("expect");
        self.expect(&TokenKind::LBrace, "`{`");
        let mut predicates = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBrace) {
            if let Some(predicate) = self.predicate() {
                predicates.push(predicate);
                self.consume(&TokenKind::Semi);
            } else {
                self.recover_to(&TokenKind::Semi);
                self.consume(&TokenKind::Semi);
            }
        }
        self.expect(&TokenKind::RBrace, "`}`");
        self.consume(&TokenKind::Semi);
        predicates
    }

    /// Parse `<observable> <cmp> <value>`, e.g. `db.orders.count == 3`.
    fn predicate(&mut self) -> Option<Spanned<Predicate>> {
        let left = self.observable()?;
        let op = self.cmp_op()?;
        let right = self.value()?;
        let span = Span::new(left.span.start, right.span.end);
        Some(Spanned::new(Predicate { left, op, right }, span))
    }

    /// Parse a dotted observable path, its arguments, and an optional `where`
    /// filter, e.g. `db.orders.count where name = "www"`.
    fn observable(&mut self) -> Option<Spanned<OpCall>> {
        let first = self.expect_ident("an observable")?;
        let start = first.span.start;
        let mut end = first.span.end;
        let mut head = vec![first];
        while self.consume(&TokenKind::Dot) {
            let segment = self.expect_ident("an observable path segment")?;
            end = segment.span.end;
            head.push(segment);
        }
        let mut args = Vec::new();
        // `where` is an identifier like any other, so it is excluded by name or
        // it reads as an argument.
        while !self.at_kw("where") && self.is_value_start() {
            let Some(value) = self.value() else { break };
            end = value.span.end;
            args.push(value);
        }
        let mut clauses = Vec::new();
        if self.at_kw("where")
            && let Some(clause) = self.where_clause()
        {
            end = clause.span.end;
            clauses.push(clause);
        }
        Some(Spanned::new(
            OpCall {
                head,
                args,
                clauses,
            },
            Span::new(start, end),
        ))
    }

    fn where_clause(&mut self) -> Option<Spanned<Clause>> {
        let start = self.peek_span();
        self.consume_kw("where");
        let column = self.expect_ident("a column name")?;
        self.expect(&TokenKind::Eq, "`=`");
        let value = self.value()?;
        let span = Span::new(start.start, value.span.end);
        Some(Spanned::new(Clause::Where(Filter { column, value }), span))
    }

    fn cmp_op(&mut self) -> Option<Spanned<CmpOp>> {
        let span = self.peek_span();
        let op = match self.peek() {
            TokenKind::EqEq => CmpOp::Eq,
            TokenKind::Ne => CmpOp::Ne,
            TokenKind::Lt => CmpOp::Lt,
            TokenKind::Le => CmpOp::Le,
            TokenKind::Gt => CmpOp::Gt,
            TokenKind::Ge => CmpOp::Ge,
            _ => {
                self.error_here("expected a comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`)");
                return None;
            }
        };
        self.advance();
        Some(Spanned::new(op, span))
    }

    fn map(&mut self) -> Spanned<Value> {
        let start = self.peek_span();
        self.expect(&TokenKind::LBrace, "`{`");
        let mut entries = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBrace) {
            let Some(key) = self.expect_ident("an attribute name") else {
                self.recover_to(&TokenKind::Comma);
                self.consume(&TokenKind::Comma);
                continue;
            };
            self.expect(&TokenKind::Colon, "`:`");
            let Some(value) = self.value() else {
                self.recover_to(&TokenKind::Comma);
                self.consume(&TokenKind::Comma);
                continue;
            };
            entries.push((key, value));
            if !self.consume(&TokenKind::Comma) {
                break;
            }
        }
        let end = self.peek_span();
        self.expect(&TokenKind::RBrace, "`}`");
        Spanned::new(Value::Map(entries), Span::new(start.start, end.end))
    }

    fn list(&mut self) -> Spanned<Value> {
        let start = self.peek_span();
        self.expect(&TokenKind::LBracket, "`[`");
        let mut items = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBracket) {
            let Some(value) = self.value() else { break };
            items.push(value);
            if !self.consume(&TokenKind::Comma) {
                break;
            }
        }
        let end = self.peek_span();
        self.expect(&TokenKind::RBracket, "`]`");
        Spanned::new(Value::List(items), Span::new(start.start, end.end))
    }

    fn value(&mut self) -> Option<Spanned<Value>> {
        let span = self.peek_span();
        let value = match self.peek() {
            TokenKind::Str(s) => Value::Str(s.clone()),
            TokenKind::Int(n) => Value::Int(*n),
            TokenKind::Duration(d) => Value::Duration(*d),
            TokenKind::Ident(s) => match s.as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                "null" => Value::Null,
                _ => Value::Ident(s.clone()),
            },
            TokenKind::LBracket => return Some(self.list()),
            TokenKind::LBrace => return Some(self.map()),
            _ => {
                self.error_here("expected a value");
                return None;
            }
        };
        self.advance();
        Some(Spanned::new(value, span))
    }
}

#[cfg(test)]
mod tests {
    use super::parse;
    use crate::{
        ast::{Clause, CmpOp, Value},
        lexer::lex,
    };

    fn parse_src(src: &str) -> Result<super::File, Vec<super::Diag>> {
        let (tokens, lex_errors) = lex(src);
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");
        parse(tokens)
    }

    /// Parse a fleet block, completing the file with the least a scenario can
    /// say, so a test about the fleet need not restate one.
    fn parse_fleet(src: &str) -> Result<super::File, Vec<super::Diag>> {
        parse_src(&format!(
            r#"{src} scenario "s" {{ consistent_within: 1s; do {{ http GET api "/" }}; expect {{ db.t.count == 0; }} }}"#
        ))
    }

    #[test]
    fn a_fleet_with_services_parses() {
        let file = parse_fleet(
            r#"fleet "orders" { deployment: docker; service api { kind: http, ports: { http: 8080 } }; service db { kind: sql, ports: { sql: 3306 } }; }"#,
        )
        .expect("parses");
        assert_eq!(file.fleet.node.name.node, "orders");
        assert_eq!(file.fleet.node.deployment.node, "docker");
        assert_eq!(file.fleet.node.services.len(), 2);
        assert_eq!(file.fleet.node.services[0].node.name.node, "api");
        assert_eq!(file.fleet.node.services[1].node.name.node, "db");
    }

    #[test]
    fn a_file_without_a_scenario_is_an_error() {
        let errors = parse_src(r#"fleet "f" { deployment: docker; }"#).unwrap_err();
        assert!(errors.iter().any(|d| d.message.contains("scenario")));
    }

    #[test]
    fn a_fleet_without_a_deployment_is_an_error() {
        let errors = parse_src(r#"fleet "f" { service api { port: 80 } }"#).unwrap_err();
        assert!(errors.iter().any(|d| d.message.contains("deployment")));
    }

    #[test]
    fn service_attrs_parse_as_a_map() {
        let file = parse_fleet(
            r#"fleet "f" { deployment: docker; service api { kind: http, ports: { http: 8080 } } }"#,
        )
        .expect("parses");
        let Value::Map(entries) = &file.fleet.node.services[0].node.attrs.node else {
            panic!("expected a map");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0.node, "kind");
        assert_eq!(entries[0].1.node, Value::Ident("http".to_string()));
        assert_eq!(entries[1].0.node, "ports");
        let Value::Map(ports) = &entries[1].1.node else {
            panic!("expected a map of ports");
        };
        assert_eq!(ports[0].0.node, "http");
        assert_eq!(ports[0].1.node, Value::Int(8080));
    }

    #[test]
    fn a_list_attribute_parses() {
        let file =
            parse_fleet(r#"fleet "f" { deployment: docker; service api { env: ["A=1", "B=2"] } }"#)
                .expect("parses");
        let Value::Map(entries) = &file.fleet.node.services[0].node.attrs.node else {
            panic!("expected a map");
        };
        let Value::List(items) = &entries[0].1.node else {
            panic!("expected a list");
        };
        let values: Vec<&Value> = items.iter().map(|i| &i.node).collect();
        assert_eq!(
            values,
            vec![
                &Value::Str("A=1".to_string()),
                &Value::Str("B=2".to_string()),
            ],
        );
    }

    #[test]
    fn a_scenario_body_parses() {
        let file = parse_src(
            r#"fleet "f" { deployment: docker; }
               scenario "s" {
                 consistent_within: 30s;
                 do { http POST api "/orders" body { item: "book", quantity: 4 } };
                 expect {
                   db.orders.count == 2;
                   db.orders.count where item = "book" == 1;
                 }
               }"#,
        )
        .expect("parses");

        let scenario = &file.scenarios[0].node;
        assert_eq!(scenario.name.node, "s");
        assert_eq!(
            scenario.consistent_within.node,
            std::time::Duration::from_secs(30),
        );

        let step = &scenario.steps[0].node;
        let head: Vec<&str> = step.head.iter().map(|h| h.node.as_str()).collect();
        assert_eq!(head, ["http", "POST"]);
        let args: Vec<&Value> = step.args.iter().map(|a| &a.node).collect();
        assert_eq!(
            args,
            [
                &Value::Ident("api".to_string()),
                &Value::Str("/orders".to_string())
            ],
        );
        assert!(matches!(step.clauses[0].node, Clause::Body(_)));

        assert_eq!(scenario.expect.len(), 2);
        let first = &scenario.expect[0].node;
        let observable: Vec<&str> = first
            .left
            .node
            .head
            .iter()
            .map(|h| h.node.as_str())
            .collect();
        assert_eq!(observable, ["db", "orders", "count"]);
        assert_eq!(first.op.node, CmpOp::Eq);
        assert_eq!(first.right.node, Value::Int(2));
        assert!(matches!(
            scenario.expect[1].node.left.node.clauses[0].node,
            Clause::Where(_),
        ));
    }

    #[test]
    fn a_scenario_without_consistent_within_is_an_error() {
        let errors =
            parse_src(r#"fleet "f" { deployment: docker; } scenario "s" { }"#).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|d| d.message.contains("consistent_within"))
        );
    }

    #[test]
    fn a_missing_fleet_is_an_error() {
        let errors = parse_src("service api { }").unwrap_err();
        assert!(errors.iter().any(|d| d.message.contains("fleet")));
    }

    #[test]
    fn recovery_reports_an_error_from_each_malformed_service() {
        // Two services each missing a value; both surface in one pass.
        let errors =
            parse_src(r#"fleet "f" { deployment: docker; service a { x: }; service b { y: }; }"#)
                .unwrap_err();
        assert_eq!(errors.len(), 2);
    }
}
