//! What the fault-free run found out about a fleet.

use std::collections::BTreeSet;

use crucible_protocol::{EdgeProfile, Reached};

use crate::{fault::Primitive, verdict::Baseline};

/// Everything we know about the fleet after the fault-free run. This is all the
/// information the scheduler has to go on.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Learned {
    /// Per-edge traffic profiles, which the fault anchors come from.
    pub profiles: Vec<EdgeProfile>,
    /// The state each step left behind and where the run settled, which every
    /// faulted run is judged against.
    pub fault_free: Baseline,
    /// The moments services offered from inside themselves.
    /// Empty unless a service is instrumented.
    /// A fleet that offers none is still scheduled from its edges.
    pub inside: Vec<Reached>,
    /// What can be done to this fleet.
    pub primitives: BTreeSet<Primitive>,
}
