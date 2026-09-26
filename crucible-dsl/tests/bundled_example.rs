//! The examples shipped in the repository have to compile against the builtins.

use rstest::rstest;

#[rstest]
#[case("orders/1_base/orders.cru")]
#[case("orders/2_outbox/orders.cru")]
#[case("orders/3_inbox/orders.cru")]
#[case("orders/4_reconnect/orders.cru")]
#[case("orders/5_ack_on_success/orders.cru")]
#[case("orders/local_first/orders.cru")]
fn a_bundled_example_compiles(#[case] scenario: &str) {
    let src = std::fs::read_to_string(format!(
        "{}/../examples/{scenario}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("read the example");
    crucible_dsl::compile(&src, &crucible_plugin::Registry::builtins())
        .expect("the bundled example compiles");
}
