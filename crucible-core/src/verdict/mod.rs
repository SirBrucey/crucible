//! Invariants, observations, and drivers that produce verdicts.

pub mod drivers;

use std::cmp::Ordering;

pub use drivers::{Converges, Durable, Idempotent, Recovers};
use serde::{Deserialize, Serialize};
use strum::EnumIter;

use crate::{ipc::Verdict, schema::CmpOp};

/// The four canonical event-driven invariants Crucible checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, EnumIter)]
pub enum Invariant {
    Idempotent,
    Converges,
    Durable,
    Recovers,
}

/// Observations captured during schedule execution and fed to a [`Driver`].
#[derive(Debug, Default)]
pub struct Observations {
    pub outcomes: Vec<Outcome>,
    /// What the scenario's checks read once the fleet settled, in the order the
    /// scenario states them.
    pub checks: Vec<Observed>,
    /// What those checks read at each point of the run: the baseline before the
    /// first step, then one per step. A step's effects are what separates its
    /// checkpoint from the one before it.
    pub trajectory: Vec<Checkpoint>,
    /// The fault-free run's trajectory, which this run is judged against. Empty
    /// in the fault-free run itself.
    pub fault_free: Vec<Checkpoint>,
    pub sessions: Vec<crucible_protocol::Session>,
    pub kill: Option<crucible_protocol::KillReport>,
}

/// What every check the scenario states read at one point in a run, in the
/// order the scenario states them. `None` where the reading could not be taken,
/// which under a fault is most of the point: a service that is down cannot be
/// asked, and that is a thing to record rather than to fail on.
pub type Checkpoint = Vec<Option<crate::plan::Value>>;

/// A check and what the fleet was actually holding when it was read.
#[derive(Debug)]
pub struct Observed {
    pub check: crate::plan::Check,
    pub value: crate::plan::Value,
}

impl Observed {
    /// Whether the reading satisfies the check it answers, or `None` when the
    /// two cannot be compared: a reading of a different shape from the one the
    /// check states, or an ordering asked of values that have none.
    #[must_use]
    pub fn holds(&self) -> Option<bool> {
        let (reading, stated) = (&self.value, &self.check.value);
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
            value: value.clone(),
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

    /// How many driven operations the system did not take responsibility for,
    /// whether by refusing them or by leaving the caller in doubt.
    #[must_use]
    pub fn undelivered(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.ack != Ack::Acked)
            .count()
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
    /// What was run, for reporting.
    pub operation: String,
    pub ack: Ack,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
}

/// Produces a [`Verdict`] from a set of observations for one invariant.
pub trait Driver {
    fn drive(&mut self, observations: &Observations) -> Verdict;
}

/// Return the stub driver for the given invariant.
#[must_use]
pub fn driver_for(invariant: Invariant) -> Box<dyn Driver> {
    match invariant {
        Invariant::Idempotent => Box::new(Idempotent),
        Invariant::Converges => Box::new(Converges),
        Invariant::Durable => Box::new(Durable),
        Invariant::Recovers => Box::new(Recovers),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Check, Value};

    /// A scenario stating `orders.count >= 2`.
    fn check() -> Check {
        Check {
            service: "db".into(),
            observer: "mariadb".into(),
            observable: vec!["orders".into(), "count".into()],
            args: Vec::new(),
            filter: None,
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
