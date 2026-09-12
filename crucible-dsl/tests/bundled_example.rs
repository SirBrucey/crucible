//! The examples shipped in the repository have to compile against the builtins.

use rstest::rstest;

#[rstest]
#[case("orders/1_base/orders.cru")]
#[case("orders/2_outbox/orders.cru")]
#[case("orders/3_local_first/orders.cru")]
fn a_bundled_example_compiles(#[case] scenario: &str) {
    let src = std::fs::read_to_string(format!(
        "{}/../examples/{scenario}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("read the example");
    crucible_dsl::compile(&src, &crucible_plugin::Registry::builtins())
        .expect("the bundled example compiles");
}
