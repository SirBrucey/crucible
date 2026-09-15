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
    /// What a run held degraded from start to finish can show.
    ///
    /// Recovery is what a fleet that accepted work while down and never caught
    /// up has broken. A fleet that accepted nothing had nothing to catch up on,
    /// so what it kept, repeated or resequenced is still read off the run.
    pub const DEGRADED: &'static [Invariant] = &[
        Invariant::Recovers,
        Invariant::Durable,
        Invariant::Idempotent,
        Invariant::Converges,
    ];

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
    /// given what the loaded plugins can do.
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

/// Nothing the campaign loaded can prove this invariant wrong.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unreachable(pub Vec<Primitive>);

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let any_of: Vec<String> = self.0.iter().map(ToString::to_string).collect();
        write!(f, "nothing loaded can {}", any_of.join(" or "))
    }
}

/// Where a run of one set of steps left the fleet, and where it stood after
/// each step on the way.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Baseline {
    /// Where the fleet stood after each step, starting from before the first.
    pub trail: Trajectory,
    /// Where the fleet settled once it went quiet, after a longer wait than any
    /// point of the trail.
    pub settled: Checkpoint,
}

impl Baseline {
    /// A run that was not read again after its last step.
    #[must_use]
    pub fn unsettled<I: IntoIterator<Item = Checkpoint>>(points: I) -> Self {
        let trail: Trajectory = points.into_iter().collect();
        let settled = trail.settled().cloned().unwrap_or_default();
        Self { trail, settled }
    }

    /// Where the fleet stood having taken the first `n` of these steps.
    ///
    /// At the end of the run this is the settled reading, not the trail point.
    #[must_use]
    pub fn at(&self, n: usize) -> Option<&Checkpoint> {
        if n + 1 == self.trail.len() {
            return Some(&self.settled);
        }
        self.trail.at(n)
    }
}

/// What a run of each set of steps found, keyed on the steps that landed.
///
/// A prefix needs no entry. The fault-free run recorded the end of those.
pub type References = std::collections::BTreeMap<Vec<usize>, Baseline>;

/// What reading a run came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Judged {
    /// The run can be judged, and this is what it says.
    Now(crate::ipc::Verdict),
    /// Not until each of these landed sets has a reference run of its own.
    Pending(Vec<Vec<usize>>),
}

/// What a run read, which is everything a verdict is made of.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Readings {
    pub outcomes: Vec<Outcome>,
    /// What the scenario's checks read once the fleet settled.
    pub checks: Vec<Observed>,
    /// What those checks read at each point of the run, starting from before
    /// the first step.
    pub trajectory: Trajectory,
    /// What the fault-free run found, which this run is judged against.
    pub fault_free: Baseline,
    /// When each step ran, in the order the scenario states them.
    pub windows: Vec<StepWindow>,
    pub fault: Option<crucible_protocol::FaultReport>,
}

/// Everything a worker gathered while running a schedule. What a verdict is
/// made of, and what only the worker itself needs.
#[derive(Debug, Default)]
pub struct Observations {
    pub readings: Readings,
    pub sessions: Vec<crucible_protocol::Session>,
    /// The moments services reported from inside themselves.
    pub inside: Vec<crucible_protocol::Reached>,
}

/// When a step ran, as nanoseconds from scenario start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct StepWindow {
    pub start_ns: u128,
    pub end_ns: u128,
}

/// What every check the scenario states read at one point in a run.
///
/// `None` where the reading could not be taken.
pub type Checkpoint = Vec<Option<crate::plan::Value>>;

/// Where the fleet stood at each point of a run, starting from before the
/// first step.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Trajectory(Vec<Checkpoint>);

impl Trajectory {
    /// Where the fleet stood once `n` steps had landed.
    #[must_use]
    pub fn at(&self, n: usize) -> Option<&Checkpoint> {
        self.0.get(n)
    }

    /// How many points it holds, one more than the steps driven.
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Observed {
    pub check: crate::plan::Check,
    /// What the fleet was holding, or `None` where there was nothing to read.
    pub value: Option<crate::plan::Value>,
}

impl Observed {
    /// Whether the reading satisfies the check it answers, or `None` when the
    /// two cannot be compared.
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
/// A fault-free run that cannot satisfy its own predicate is mis-authored.
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

impl Readings {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// What this run found, for the runs it answers for.
    #[must_use]
    pub fn baseline(&self) -> Baseline {
        Baseline {
            trail: self.trajectory.clone(),
            settled: self.settled(),
        }
    }

    /// Where the fleet settled, as one checkpoint.
    #[must_use]
    pub fn settled(&self) -> Checkpoint {
        self.checks
            .iter()
            .map(|observed| observed.value.clone())
            .collect()
    }
}

/// Whether the system took responsibility for a driven operation. The driver
/// that ran the operation decides, by the rules of the protocol it speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
