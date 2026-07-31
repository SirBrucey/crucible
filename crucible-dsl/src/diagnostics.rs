//! Rendering diagnostics with source context and colour.

use ariadne::{Color, Fmt, Label, Report, ReportKind, Source};

use crate::span::Span;

/// A source-anchored diagnostic, with an optional help note. For now every
/// diagnostic is an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diag {
    pub span: Span,
    pub message: String,
    pub help: Option<Help>,
}

/// A help note: a lead-in and a list of suggested names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Help {
    pub lead: String,
    pub suggestions: Vec<String>,
}

impl Diag {
    #[must_use]
    pub fn new(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            message: message.into(),
            help: None,
        }
    }

    #[must_use]
    pub fn with_help(mut self, lead: impl Into<String>, suggestions: Vec<String>) -> Self {
        self.help = Some(Help {
            lead: lead.into(),
            suggestions,
        });
        self
    }
}

/// Render `diags` for the source `src` (shown as `name`) to stderr, with source
/// context and colour.
///
/// # Errors
/// Returns an [`std::io::Error`] if a report cannot be written to stderr.
pub fn emit_to_stderr(name: &str, src: &str, diags: &[Diag]) -> std::io::Result<()> {
    let mut cache = (name, Source::from(src));
    for (index, diag) in diags.iter().enumerate() {
        if index > 0 {
            eprintln!();
        }
        let mut report = Report::build(ReportKind::Error, (name, diag.span.range()))
            .with_message(&diag.message)
            .with_label(Label::new((name, diag.span.range())).with_color(Color::Red));
        if let Some(help) = &diag.help {
            let note = if help.suggestions.is_empty() {
                help.lead.clone()
            } else {
                let suggestions = help
                    .suggestions
                    .iter()
                    .map(|name| name.fg(Color::Green).to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} {suggestions}", help.lead)
            };
            report = report.with_help(note);
        }
        report.finish().eprint(&mut cache)?;
    }
    Ok(())
}
