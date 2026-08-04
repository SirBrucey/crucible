//! End-to-end tests for the `crucible check` subcommand, driving the built
//! binary so the CLI wiring, exit codes, and the example's lexability are all
//! covered.

use std::process::{Command, Output};

fn check(path: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_crucible"))
        .args(["check", path])
        .output()
        .expect("run crucible check")
}

#[test]
fn the_example_scenario_checks_cleanly() {
    let out = check(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/orders/orders.cru"
    ));
    assert!(
        out.status.success(),
        "expected exit 0, got {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn the_errors_example_reports_diagnostics() {
    let out = check(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/errors/errors.cru"
    ));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr.contains("known drivers"), "stderr: {stderr}");
    assert!(stderr.contains("known attributes"), "stderr: {stderr}");
    assert!(stderr.contains("errors"), "stderr: {stderr}");
}

#[test]
fn a_lexing_error_exits_one_with_a_diagnostic() {
    let out = check(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/lex_error.cru"
    ));
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unexpected character"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn a_parse_error_exits_one_with_a_diagnostic() {
    let out = check(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/parse_error.cru"
    ));
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("fleet name"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn a_semantic_error_exits_one_with_a_diagnostic() {
    let out = check(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/semantic_error.cru"
    ));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr.contains("unknown service"), "stderr: {stderr}");
    assert!(stderr.contains("defined services"), "stderr: {stderr}");
}

#[test]
fn a_non_cru_extension_is_rejected() {
    let out = check(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn the_bundled_example_lowers_to_the_plan_the_tests_build() {
    // The same fleet is described twice: once for an author to read, once for
    // tests that cannot depend on the DSL. They have drifted before.
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/orders/orders.cru"
    ))
    .expect("read the example");
    let (tokens, lex_errors) = crucible_dsl::lexer::lex(&src);
    assert!(lex_errors.is_empty(), "lex errors: {lex_errors:?}");
    let ast = crucible_dsl::parser::parse(tokens).expect("parses");
    let lowered =
        crucible_dsl::lower::lower(&ast, &crucible_plugin::Registry::builtins()).expect("lowers");
    assert_eq!(lowered, crucible_core::plan::example());
}
