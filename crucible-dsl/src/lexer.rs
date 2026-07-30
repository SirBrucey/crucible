//! Hand-rolled lexer for the `.cru` grammar.
//!
//! Structural keywords (`fleet`, `scenario`, `do`, ...) are lexed as [`TokenKind::Ident`]
//! and recognised by the parser, so the lexer stays agnostic to the grammar's
//! vocabulary. Statements are `;`-terminated; newlines are whitespace.

use std::{iter::Peekable, str::CharIndices, time::Duration};

use crate::span::Span;

/// A lexical token: its kind and the source span it covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

/// The kinds of token the lexer produces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenKind {
    /// A bare word: `fleet`, `http`, `POST`, `image`, `true`.
    Ident(String),
    /// A double-quoted string literal, with escapes resolved.
    Str(String),
    /// A non-negative integer literal.
    Int(i64),
    /// A duration literal: `30s`, `500ms`.
    Duration(Duration),
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Colon,
    Semi,
    Comma,
    Dot,
    Eq,
    EqEq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// End of input.
    Eof,
}

/// A lexing failure anchored to a source span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LexError {
    pub span: Span,
    pub message: String,
}

/// Tokenise `src`, returning the tokens (always ending in [`TokenKind::Eof`]) and
/// any errors. Lexing is best-effort: an unexpected character is reported and
/// skipped so later tokens are still produced.
#[must_use]
pub fn lex(src: &str) -> (Vec<Token>, Vec<LexError>) {
    Lexer::new(src).run()
}

struct Lexer<'a> {
    src: &'a str,
    chars: Peekable<CharIndices<'a>>,
    tokens: Vec<Token>,
    errors: Vec<LexError>,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            chars: src.char_indices().peekable(),
            tokens: Vec::new(),
            errors: Vec::new(),
        }
    }

    fn run(mut self) -> (Vec<Token>, Vec<LexError>) {
        while let Some(&(start, c)) = self.chars.peek() {
            match c {
                c if c.is_whitespace() => {
                    self.chars.next();
                }
                '/' => self.slash(start),
                '{' => self.punct(start, TokenKind::LBrace),
                '}' => self.punct(start, TokenKind::RBrace),
                '[' => self.punct(start, TokenKind::LBracket),
                ']' => self.punct(start, TokenKind::RBracket),
                ':' => self.punct(start, TokenKind::Colon),
                ';' => self.punct(start, TokenKind::Semi),
                ',' => self.punct(start, TokenKind::Comma),
                '.' => self.punct(start, TokenKind::Dot),
                '=' => self.one_or_two(start, '=', TokenKind::EqEq, TokenKind::Eq),
                '<' => self.one_or_two(start, '=', TokenKind::Le, TokenKind::Lt),
                '>' => self.one_or_two(start, '=', TokenKind::Ge, TokenKind::Gt),
                '!' => self.bang(start),
                '"' => self.string(start),
                c if c.is_ascii_digit() => self.number(start),
                c if is_ident_start(c) => self.ident(start),
                other => {
                    self.chars.next();
                    let span = Span::new(start, self.pos());
                    self.errors.push(LexError {
                        span,
                        message: format!("unexpected character `{other}`"),
                    });
                }
            }
        }
        self.tokens.push(Token {
            kind: TokenKind::Eof,
            span: Span::new(self.src.len(), self.src.len()),
        });
        (self.tokens, self.errors)
    }

    /// Byte offset just past the last consumed char (the next char's start, or
    /// end of input).
    fn pos(&mut self) -> usize {
        self.chars.peek().map_or(self.src.len(), |&(i, _)| i)
    }

    fn emit(&mut self, start: usize, kind: TokenKind) {
        let span = Span::new(start, self.pos());
        self.tokens.push(Token { kind, span });
    }

    fn punct(&mut self, start: usize, kind: TokenKind) {
        self.chars.next();
        self.emit(start, kind);
    }

    /// A one-char token, or a two-char token when the next char is `second`.
    fn one_or_two(&mut self, start: usize, second: char, two: TokenKind, one: TokenKind) {
        self.chars.next();
        if self.chars.peek().is_some_and(|&(_, c)| c == second) {
            self.chars.next();
            self.emit(start, two);
        } else {
            self.emit(start, one);
        }
    }

    fn bang(&mut self, start: usize) {
        self.chars.next();
        if self.chars.peek().is_some_and(|&(_, c)| c == '=') {
            self.chars.next();
            self.emit(start, TokenKind::Ne);
        } else {
            let span = Span::new(start, self.pos());
            self.errors.push(LexError {
                span,
                message: "unexpected `!` (did you mean `!=`?)".to_string(),
            });
        }
    }

    fn slash(&mut self, start: usize) {
        self.chars.next();
        if self.chars.peek().is_some_and(|&(_, c)| c == '/') {
            while let Some(&(_, c)) = self.chars.peek() {
                if c == '\n' {
                    break;
                }
                self.chars.next();
            }
        } else {
            let span = Span::new(start, self.pos());
            self.errors.push(LexError {
                span,
                message: "unexpected `/` (line comments start with `//`)".to_string(),
            });
        }
    }

    fn string(&mut self, start: usize) {
        self.chars.next();
        let mut value = String::new();
        while let Some((_, c)) = self.chars.next() {
            match c {
                '"' => {
                    self.emit(start, TokenKind::Str(value));
                    return;
                }
                '\\' => match self.chars.next() {
                    Some((_, '"')) => value.push('"'),
                    Some((_, '\\')) => value.push('\\'),
                    Some((_, 'n')) => value.push('\n'),
                    Some((_, 't')) => value.push('\t'),
                    Some((esc, other)) => {
                        let span = Span::new(esc, self.pos());
                        self.errors.push(LexError {
                            span,
                            message: format!("unknown escape `\\{other}`"),
                        });
                        value.push(other);
                    }
                    None => break,
                },
                c => value.push(c),
            }
        }
        self.errors.push(LexError {
            span: Span::new(start, self.src.len()),
            message: "unterminated string".to_string(),
        });
    }

    fn ident(&mut self, start: usize) {
        while self
            .chars
            .peek()
            .is_some_and(|&(_, c)| is_ident_continue(c))
        {
            self.chars.next();
        }
        let end = self.pos();
        let text = self.src[start..end].to_string();
        self.tokens.push(Token {
            kind: TokenKind::Ident(text),
            span: Span::new(start, end),
        });
    }

    fn number(&mut self, start: usize) {
        while self.chars.peek().is_some_and(|&(_, c)| c.is_ascii_digit()) {
            self.chars.next();
        }
        let digits_end = self.pos();
        while self
            .chars
            .peek()
            .is_some_and(|&(_, c)| c.is_ascii_alphabetic())
        {
            self.chars.next();
        }
        let end = self.pos();
        let digits = &self.src[start..digits_end];
        let unit = &self.src[digits_end..end];
        let span = Span::new(start, end);
        if unit.is_empty() {
            match digits.parse::<i64>() {
                Ok(n) => self.tokens.push(Token {
                    kind: TokenKind::Int(n),
                    span,
                }),
                Err(_) => self.errors.push(LexError {
                    span,
                    message: format!("integer `{digits}` is out of range"),
                }),
            }
        } else if let Some(duration) = duration_from(digits, unit) {
            self.tokens.push(Token {
                kind: TokenKind::Duration(duration),
                span,
            });
        } else {
            self.errors.push(LexError {
                span,
                message: format!("`{digits}{unit}` is not a valid integer or duration"),
            });
        }
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn duration_from(digits: &str, unit: &str) -> Option<Duration> {
    let n: u64 = digits.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(n)),
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(n.saturating_mul(60))),
        "h" => Some(Duration::from_secs(n.saturating_mul(3600))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Span, TokenKind as T, lex};
    use std::time::Duration;

    fn kinds(src: &str) -> Vec<T> {
        let (tokens, errors) = lex(src);
        assert!(errors.is_empty(), "unexpected lex errors: {errors:?}");
        tokens
            .into_iter()
            .map(|t| t.kind)
            .filter(|k| *k != T::Eof)
            .collect()
    }

    #[test]
    fn punctuation_and_comparison_operators() {
        assert_eq!(
            kinds("{ } [ ] : ; , . = == != < <= > >="),
            vec![
                T::LBrace,
                T::RBrace,
                T::LBracket,
                T::RBracket,
                T::Colon,
                T::Semi,
                T::Comma,
                T::Dot,
                T::Eq,
                T::EqEq,
                T::Ne,
                T::Lt,
                T::Le,
                T::Gt,
                T::Ge,
            ],
        );
    }

    #[test]
    fn identifiers_strings_ints_and_durations() {
        assert_eq!(
            kinds(r#"service api "a/b:0.1" 8080 30s 500ms"#),
            vec![
                T::Ident("service".to_string()),
                T::Ident("api".to_string()),
                T::Str("a/b:0.1".to_string()),
                T::Int(8080),
                T::Duration(Duration::from_secs(30)),
                T::Duration(Duration::from_millis(500)),
            ],
        );
    }

    #[test]
    fn a_service_line_lexes() {
        assert_eq!(
            kinds("service api { kind: http; port: 8080 }"),
            vec![
                T::Ident("service".to_string()),
                T::Ident("api".to_string()),
                T::LBrace,
                T::Ident("kind".to_string()),
                T::Colon,
                T::Ident("http".to_string()),
                T::Semi,
                T::Ident("port".to_string()),
                T::Colon,
                T::Int(8080),
                T::RBrace,
            ],
        );
    }

    #[test]
    fn line_comments_are_skipped() {
        assert_eq!(
            kinds("api // this is the api service\nbroker"),
            vec![T::Ident("api".to_string()), T::Ident("broker".to_string())],
        );
    }

    #[test]
    fn string_escapes_resolve() {
        assert_eq!(kinds(r#""a\"b\\c""#), vec![T::Str("a\"b\\c".to_string())],);
    }

    #[test]
    fn an_unexpected_character_is_reported_and_skipped() {
        let (tokens, errors) = lex("api ? broker");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].span, Span::new(4, 5));
        // The `?` is skipped, so both identifiers still lex.
        let idents: Vec<_> = tokens
            .into_iter()
            .filter_map(|t| match t.kind {
                T::Ident(s) => Some(s),
                _ => None,
            })
            .collect();
        assert_eq!(idents, vec!["api".to_string(), "broker".to_string()]);
    }

    #[test]
    fn an_unterminated_string_is_reported() {
        let (_, errors) = lex(r#""no closing quote"#);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("unterminated"));
    }
}
