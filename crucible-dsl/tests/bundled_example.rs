//! The example shipped in the repository has to compile against the builtins.

#[test]
fn the_bundled_example_compiles() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/orders/orders.cru"
    ))
    .expect("read the example");
    crucible_dsl::compile(&src, &crucible_plugin::Registry::builtins())
        .expect("the bundled example compiles");
}
