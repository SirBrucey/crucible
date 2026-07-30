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
fn the_example_scenario_lexes_cleanly() {
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
fn a_non_cru_extension_is_rejected() {
    let out = check(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
    assert_eq!(out.status.code(), Some(2));
}
