//! Invariants, observations, and the one reading that turns them into a
//! verdict.

mod reading;

use std::{cmp::Ordering, collections::BTreeSet};

use serde::{Deserialize, Serialize};
use strum::{EnumIter, IntoEnumIterator};

use crate::{fault::Primitive, schema::CmpOp};

/// The four canonical event-driven invariants Crucible checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, EnumIter)]
pub enum Invariant {
    Idempotent,
    Converges,
    Durable,
    Recovers,
}

/// Named as the thing a verdict says broke, so a report reads as a sentence.
impl std::fmt::Display for Invariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Invariant::Idempotent => f.write_str("idempotency"),
            Invariant::Converges => f.write_str("convergence"),
            Invariant::Durable => f.write_str("durability"),
            Invariant::Recovers => f.write_str("recovery"),
        }
    }
}

impl Invariant {
    /// What a run held degraded from start to finish.
    /// Nothing in the run derives this as it is settled from the start.
    pub const DEGRADED: &'static [Invariant] = &[Invariant::Recovers];

    /// What breaking the fleet this way could show.
    #[must_use]
    pub fn could_show(throughout: bool, by: Primitive) -> &'static [Invariant] {
        if throughout {
            Invariant::DEGRADED
        } else {
            Invariant::shown_by(by)
        }
    }

    /// Making a message arrive twice can only ask whether handling it twice
    /// leaves what handling it once would. Making messages arrive out of order
    /// can only ask whether the order mattered. Taking something away asks
    /// nothing so narrow: the fleet is left in doubt, and what it does about
    /// the doubt is what decides which invariant it broke.
    fn shown_by(primitive: Primitive) -> &'static [Invariant] {
        match primitive {
            Primitive::Redeliver => &[Invariant::Idempotent],
            Primitive::Reorder => &[Invariant::Converges],
            Primitive::Kill | Primitive::Cut | Primitive::Drop => &[
                Invariant::Durable,
                Invariant::Idempotent,
                Invariant::Converges,
            ],
        }
    }

    /// Anything that could show this invariant broken.
    ///
    /// The inverse of what shows an invariant, except for recovery, which needs
    /// a run held degraded throughout.
    #[must_use]
    pub fn shown_by_any(self) -> Vec<Primitive> {
        match self {
            Invariant::Recovers => vec![Primitive::Kill, Primitive::Cut],
            _ => Primitive::iter()
                .filter(|by| Invariant::shown_by(*by).contains(&self))
                .collect(),
        }
    }

    /// What a campaign against this fleet could show this invariant broken by,
    /// given what the loaded plugins turned out to be able to do.
    ///
    /// This is what the campaign could show, not what it did. Which invariant
    /// any one run showed is that run's verdict to say.
    ///
    /// # Errors
    /// Errors if nothing can be shown.
    pub fn showable(self, available: &BTreeSet<Primitive>) -> Result<Vec<Primitive>, Unreachable> {
        let showable: Vec<Primitive> = self
            .shown_by_any()
            .into_iter()
            .filter(|by| available.contains(by))
            .collect();
        if showable.is_empty() {
            return Err(Unreachable(self.shown_by_any()));
        }
        Ok(showable)
    }
}

/// Nothing the campaign loaded can prove an invariant
/// wrong, so the campaign cannot claim to have tested it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unreachable(pub Vec<Primitive>);

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let any_of: Vec<String> = self.0.iter().map(ToString::to_string).collect();
        write!(f, "nothing loaded can {}", any_of.join(" or "))
    }
}

/// Observations captured during schedule execution, which
/// [`Observations::verdict`] turns into a verdict.
#[derive(Debug, Default)]
pub struct Observations {
    pub outcomes: Vec<Outcome>,
    /// What the scenario's checks read once the fleet settled, in the order the
    /// scenario states them.
    pub checks: Vec<Observed>,
    /// What those checks read at each point of the run: the baseline before the
    /// first step, then one per step. A step's effects are what separates its
    /// checkpoint from the one before it.
    pub trajectory: Trajectory,
    /// The fault-free run's trajectory, which this run is judged against. Empty
    /// in the fault-free run itself.
    pub fault_free: Trajectory,
    /// When each step ran, in the order the scenario states them.
    pub windows: Vec<StepWindow>,
    pub sessions: Vec<crucible_protocol::Session>,
    pub fault: Option<crucible_protocol::FaultReport>,
}

/// When a step ran, as nanoseconds from scenario start. The fault records the
/// same origin, so a verdict can say which steps it landed among.
#[derive(Clone, Copy, Debug)]
pub struct StepWindow {
    pub start_ns: u128,
    pub end_ns: u128,
}

/// What every check the scenario states read at one point in a run, in the
/// order the scenario states them. `None` where the reading could not be taken,
/// which under a fault is most of the point: a service that is down cannot be
/// asked, and that is a thing to record rather than to fail on.
pub type Checkpoint = Vec<Option<crate::plan::Value>>;

/// Where the fleet stood at each point of a run: the baseline before the first
/// step, then one per step.
///
/// A run is judged by reading this against what the fleet's own steps would
/// have left, so it is the substrate every verdict rests on.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Trajectory(Vec<Checkpoint>);

impl Trajectory {
    /// Where the fleet stood once `n` steps had landed.
    #[must_use]
    pub fn at(&self, n: usize) -> Option<&Checkpoint> {
        self.0.get(n)
    }

    /// How many points it holds, which is one more than the steps driven.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Checkpoint> {
        self.0.iter()
    }

    /// Where the fleet stood once every step had landed.
    #[must_use]
    pub fn settled(&self) -> Option<&Checkpoint> {
        self.0.last()
    }

    /// Record where the fleet stands now, which is one more step taken.
    pub fn push(&mut self, point: Checkpoint) {
        self.0.push(point);
    }
}

impl FromIterator<Checkpoint> for Trajectory {
    fn from_iter<I: IntoIterator<Item = Checkpoint>>(points: I) -> Self {
        Self(points.into_iter().collect())
    }
}

impl<'a> IntoIterator for &'a Trajectory {
    type Item = &'a Checkpoint;
    type IntoIter = std::slice::Iter<'a, Checkpoint>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A check and what the fleet was actually holding when it was read.
#[derive(Debug)]
pub struct Observed {
    pub check: crate::plan::Check,
    /// What the fleet was holding, or `None` where there was nothing to read.
    /// A fault can leave a fleet with no row to answer from, which is a
    /// reading of the fleet rather than a failure to take one.
    pub value: Option<crate::plan::Value>,
}

impl Observed {
    /// Whether the reading satisfies the check it answers, or `None` when the
    /// two cannot be compared: a reading of a different shape from the one the
    /// check states, or an ordering asked of values that have none.
    #[must_use]
    pub fn holds(&self) -> Option<bool> {
        let (reading, stated) = (self.value.as_ref()?, &self.check.value);
        // The check pass held the author's value to the shape the observable
        // declares, so a reading of another shape is the plugin answering with
        // something it never said it would.
        if std::mem::discriminant(reading) != std::mem::discriminant(stated) {
            return None;
        }
        match self.check.op {
            CmpOp::Eq => Some(reading == stated),
            CmpOp::Ne => Some(reading != stated),
            CmpOp::Lt => order(reading, stated).map(Ordering::is_lt),
            CmpOp::Le => order(reading, stated).map(Ordering::is_le),
            CmpOp::Gt => order(reading, stated).map(Ordering::is_gt),
            CmpOp::Ge => order(reading, stated).map(Ordering::is_ge),
        }
    }
}

/// Why a fault-free run missed what the scenario stated.
///
/// If the fault-free run cannot satisfy its predicate then it is mis-authored.
/// We cannot judge a faulted run against a non-deterministic result.
#[must_use]
pub fn unmet(
    checks: &[crate::plan::Check],
    settled: &[Option<crate::plan::Value>],
) -> Option<String> {
    for (check, reading) in checks.iter().zip(settled) {
        let Some(value) = reading else {
            return Some(format!("`{}` could not be read", check.observable()));
        };
        let observed = Observed {
            check: check.clone(),
            value: Some(value.clone()),
        };
        if observed.holds() != Some(true) {
            return Some(format!("`{}` reads {value}", check.stated()));
        }
    }
    checks
        .get(settled.len())
        .map(|check| format!("`{}` was never read", check.observable()))
}

/// How two readings of the same shape order, for the shapes that have an order.
fn order(a: &crate::plan::Value, b: &crate::plan::Value) -> Option<Ordering> {
    use crate::plan::Value::{Duration, Int, Str};
    match (a, b) {
        (Int(a), Int(b)) => Some(a.cmp(b)),
        (Str(a), Str(b)) => Some(a.cmp(b)),
        (Duration(a), Duration(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

impl Observations {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}

/// Whether the system took responsibility for a driven operation. The driver
/// that ran the operation decides, by the rules of the protocol it speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    /// Acknowledged: the system accepted responsibility for the write.
    Acked,
    /// Refused: the system definitively did not accept it.
    Rejected,
    /// In doubt: the caller cannot tell whether it was accepted.
    Unknown,
}

/// The result of one operation a driver ran. The payloads are opaque; only the
/// driver that produced them knows how to read them.
#[derive(Debug)]
pub struct Outcome {
    pub ack: Ack,
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;
    use crate::plan::{Check, Value};

    #[test]
    fn an_invariant_nothing_can_show_is_out_of_reach() {
        let nothing = BTreeSet::new();
        for invariant in Invariant::iter() {
            assert_eq!(
                invariant.showable(&nothing),
                Err(Unreachable(invariant.shown_by_any())),
                "{invariant:?}"
            );
        }
    }

    /// A campaign shows an invariant broken by whichever ways of breaking the
    /// fleet it loaded and that invariant answers to. Losing a write is losing
    /// a write, however the edge was broken, but no redelivery loses one.
    #[test]
    fn only_the_loaded_ways_that_could_show_it_are_offered() {
        assert_eq!(
            Invariant::Durable.showable(&BTreeSet::from([Primitive::Kill, Primitive::Redeliver])),
            Ok(vec![Primitive::Kill]),
            "a redelivery cannot lose a write"
        );
        assert_eq!(
            Invariant::Durable.showable(&BTreeSet::from([Primitive::Kill, Primitive::Cut])),
            Ok(vec![Primitive::Kill, Primitive::Cut])
        );
    }

    /// Taking something away leaves the fleet in doubt, and what it does about
    /// the doubt could break any of them, so a fleet that can only be killed is
    /// still a fleet idempotency can be shown broken on.
    #[test]
    fn taking_something_away_could_show_any_of_them() {
        assert!(
            Invariant::Idempotent
                .showable(&BTreeSet::from([Primitive::Kill]))
                .is_ok()
        );
    }

    /// A scenario stating `orders.count >= 2`.
    fn check() -> Check {
        Check {
            service: "db".into(),
            observer: "mariadb".into(),
            observable: vec!["orders".into(), "count".into()],
            args: Vec::new(),
            filter: None,
            moves: crate::schema::Moves::Counts,
            clauses: std::collections::BTreeMap::new(),
            op: CmpOp::Ge,
            value: Value::Int(2),
        }
    }

    #[test]
    fn a_fault_free_run_that_satisfies_the_scenario_has_nothing_unmet() {
        assert_eq!(unmet(&[check()], &[Some(Value::Int(3))]), None);
    }

    #[test]
    fn a_reading_the_scenario_rules_out_is_quoted_back_as_written() {
        assert_eq!(
            unmet(&[check()], &[Some(Value::Int(1))]),
            Some("`orders.count >= 2` reads 1".into())
        );
    }

    #[test]
    fn a_reading_of_another_shape_is_not_a_comparison() {
        assert!(unmet(&[check()], &[Some(Value::Str("two".into()))]).is_some());
    }

    #[test]
    fn a_check_the_run_could_not_read_is_unmet() {
        assert!(unmet(&[check()], &[None]).is_some());
    }

    #[test]
    fn a_check_the_run_never_read_is_unmet() {
        assert!(unmet(&[check()], &[]).is_some());
    }
}
