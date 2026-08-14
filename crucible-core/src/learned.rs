//! What the fault-free run found out about a fleet.

use std::collections::BTreeSet;

use crucible_protocol::ServiceProfile;

use crate::{fault::Primitive, verdict::Checkpoint};

/// Everything we know about the fleet after the fault-free run. This is all the
/// information the scheduler has to go on.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Learned {
    /// Per-service traffic profiles, which the fault anchors come from.
    pub profiles: Vec<ServiceProfile>,
    /// The state each step left behind, which every faulted run is judged
    /// against.
    pub trajectory: Vec<Checkpoint>,
    /// What can be done to this fleet.
    pub primitives: BTreeSet<Primitive>,
}
