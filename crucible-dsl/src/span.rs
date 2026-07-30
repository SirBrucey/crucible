//! Source spans: byte ranges into a `.cru` file, carried on tokens and AST
//! nodes so diagnostics can point at the offending text.

use std::ops::Range;

/// A byte range `start..end` in the source (end-exclusive).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    #[must_use]
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// The span as a `start..end` byte range.
    #[must_use]
    pub fn range(self) -> Range<usize> {
        self.start..self.end
    }
}

/// A value paired with its source span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    #[must_use]
    pub fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }
}
