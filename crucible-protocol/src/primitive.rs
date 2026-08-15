//! What can be done to a running fleet to break it.

/// Something that can be done to a running fleet to put an invariant under
/// pressure. A plugin offers one by implementing it, so this is the vocabulary a
/// campaign uses to say what it could and could not reach.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Deserialize,
    serde::Serialize,
    strum::EnumIter,
    strum::Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum Primitive {
    /// Take a service out of the fleet and put it back.
    Kill,
    /// Sever an edge, leaving the services either side of it running.
    Cut,
    /// Deliver a message the fleet has already handled a second time.
    Redeliver,
    /// Hold a message back until a later one has passed it.
    Reorder,
}
