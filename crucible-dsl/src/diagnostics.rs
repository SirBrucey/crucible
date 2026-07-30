//! Rendering diagnostics with source context and colour.

use ariadne::{Color, Label, Report, ReportKind, Source};

use crate::span::Span;

/// A source-anchored diagnostic. For now every diagnostic is an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diag {
    pub span: Span,
    pub message: String,
}

impl Diag {
    #[must_use]
    pub fn new(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            message: message.into(),
        }
    }
}

/// Render `diags` for the source `src` (shown as `name`) to stderr, with source
/// context and colour.
///
/// # Errors
/// Returns an [`std::io::Error`] if a report cannot be written to stderr.
pub fn emit_to_stderr(name: &str, src: &str, diags: &[Diag]) -> std::io::Result<()> {
    let mut cache = (name, Source::from(src));
    for diag in diags {
        Report::build(ReportKind::Error, (name, diag.span.range()))
            .with_message(&diag.message)
            .with_label(Label::new((name, diag.span.range())).with_color(Color::Red))
            .finish()
            .eprint(&mut cache)?;
    }
    Ok(())
}
