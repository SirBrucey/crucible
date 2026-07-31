//! Front end for `.cru` scenario files: lexer, parser, AST, source spans, and
//! diagnostic rendering.

pub mod ast;
pub mod diagnostics;
pub mod lexer;
pub mod parser;
pub mod span;
pub mod validate;
