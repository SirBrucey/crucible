//! Recursive-descent parser: builds the file AST from a token stream, recovering
//! at statement and block boundaries so one error does not abort the parse.

use crate::{
    ast::{File, Fleet, Scenario, Service, Value},
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
        let mut services = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBrace) {
            if let Some(service) = self.service() {
                services.push(service);
            } else {
                self.recover_to(&TokenKind::Semi);
                self.consume(&TokenKind::Semi);
            }
        }
        let end = self.peek_span();
        self.expect(&TokenKind::RBrace, "`}`");
        Some(Spanned::new(
            Fleet { name, services },
            Span::new(start.start, end.end),
        ))
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
        // The scenario body grammar lands in a later slice; recognise the block
        // and skip it so the rest of the file still parses.
        let end = self.skip_braced_block();
        Some(Spanned::new(
            Scenario { name },
            Span::new(start.start, end.end),
        ))
    }

    /// Consume a `{ ... }` block, matching nested braces, and return the closing
    /// brace's span (or the opening one if the block is unterminated).
    fn skip_braced_block(&mut self) -> Span {
        let open = self.peek_span();
        if !self.expect(&TokenKind::LBrace, "`{`") {
            return open;
        }
        let mut depth = 1u32;
        let mut last = open;
        while depth > 0 && !self.at_eof() {
            last = self.peek_span();
            match self.peek() {
                TokenKind::LBrace => depth += 1,
                TokenKind::RBrace => depth -= 1,
                _ => {}
            }
            self.advance();
        }
        last
    }

    fn map(&mut self) -> Spanned<Value> {
        let start = self.peek_span();
        self.expect(&TokenKind::LBrace, "`{`");
        let mut entries = Vec::new();
        while !self.at_eof() && !self.at(&TokenKind::RBrace) {
            let Some(key) = self.expect_ident("an attribute name") else {
                self.recover_to(&TokenKind::Semi);
                self.consume(&TokenKind::Semi);
                continue;
            };
            self.expect(&TokenKind::Colon, "`:`");
            let Some(value) = self.value() else {
                self.recover_to(&TokenKind::Semi);
                self.consume(&TokenKind::Semi);
                continue;
            };
            entries.push((key, value));
            if !self.consume(&TokenKind::Semi) {
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
    use crate::{ast::Value, lexer::lex};

    fn parse_src(src: &str) -> Result<super::File, Vec<super::Diag>> {
        let (tokens, lex_errors) = lex(src);
        assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");
        parse(tokens)
    }

    #[test]
    fn a_fleet_with_services_parses() {
        let file = parse_src(
            r#"fleet "orders" { service api { kind: http; port: 8080 }; service db { kind: sql; port: 3306 }; }"#,
        )
        .expect("parses");
        assert_eq!(file.fleet.node.name.node, "orders");
        assert_eq!(file.fleet.node.services.len(), 2);
        assert_eq!(file.fleet.node.services[0].node.name.node, "api");
        assert_eq!(file.fleet.node.services[1].node.name.node, "db");
    }

    #[test]
    fn service_attrs_parse_as_a_map() {
        let file =
            parse_src(r#"fleet "f" { service api { kind: http; port: 8080 } }"#).expect("parses");
        let Value::Map(entries) = &file.fleet.node.services[0].node.attrs.node else {
            panic!("expected a map");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0.node, "kind");
        assert_eq!(entries[0].1.node, Value::Ident("http".to_string()));
        assert_eq!(entries[1].0.node, "port");
        assert_eq!(entries[1].1.node, Value::Int(8080));
    }

    #[test]
    fn a_list_attribute_parses() {
        let file =
            parse_src(r#"fleet "f" { service api { env: ["A=1", "B=2"] } }"#).expect("parses");
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
    fn a_scenario_header_is_recognised_and_its_body_skipped() {
        let file =
            parse_src(r#"fleet "f" { } scenario "s" { do { anything } ; nested { braces } }"#)
                .expect("parses");
        assert_eq!(file.scenarios.len(), 1);
        assert_eq!(file.scenarios[0].node.name.node, "s");
    }

    #[test]
    fn a_missing_fleet_is_an_error() {
        let errors = parse_src("service api { }").unwrap_err();
        assert!(errors.iter().any(|d| d.message.contains("fleet")));
    }

    #[test]
    fn recovery_reports_an_error_from_each_malformed_service() {
        // Two services each missing a value; both surface in one pass.
        let errors = parse_src(r#"fleet "f" { service a { x: }; service b { y: }; }"#).unwrap_err();
        assert_eq!(errors.len(), 2);
    }
}
