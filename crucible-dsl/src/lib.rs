//! Front end for `.cru` scenario files: lexer, parser, AST, source spans, and
//! diagnostic rendering.

pub mod ast;
pub mod diagnostics;
pub mod lexer;
pub mod lower;
pub mod parser;
pub mod span;

use crucible_core::plan::Plan;
use crucible_plugin::Registry;

use crate::diagnostics::Diag;

/// Turn `.cru` source into the plan a campaign runs, checking it against the
/// plugins in `registry` on the way.
///
/// # Errors
/// Returns everything wrong with the source, from whichever stage first found
/// something wrong with it. Rendering them is the caller's to decide (see
/// [`diagnostics::emit_to_stderr`]).
pub fn compile(src: &str, registry: &Registry) -> Result<Plan, Vec<Diag>> {
    let (tokens, lex_errors) = lexer::lex(src);
    if !lex_errors.is_empty() {
        return Err(lex_errors
            .iter()
            .map(|e| Diag::new(e.span, e.message.clone()))
            .collect());
    }
    let ast = parser::parse(tokens)?;
    lower::lower(&ast, registry)
}
