//! The bundled example fleet is described twice: once in `.cru` for an author
//! to read, once as a built plan for tests that cannot depend on the DSL. They
//! have drifted before, so this holds them to each other.

#[test]
fn the_bundled_example_lowers_to_the_plan_the_tests_build() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/orders/orders.cru"
    ))
    .expect("read the example");
    let lowered = crucible_dsl::compile(&src, &crucible_plugin::Registry::builtins())
        .expect("the bundled example compiles");
    assert_eq!(lowered, crucible_core::plan::example());
}
