//! Stub verdict drivers for the four invariants.

use super::{Driver, Observations};
use crate::ipc::Verdict;

pub struct Idempotent;
pub struct Converges;
pub struct Durable;
pub struct Recovers;

impl Driver for Idempotent {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive
    }
}

impl Driver for Converges {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive
    }
}

impl Driver for Durable {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive
    }
}

impl Driver for Recovers {
    fn drive(&mut self, _observations: &Observations) -> Verdict {
        Verdict::Inconclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verdict::{Invariant, driver_for};

    #[test]
    fn every_invariant_yields_inconclusive_on_empty_observations() {
        let obs = Observations::empty();
        for invariant in [
            Invariant::Idempotent,
            Invariant::Converges,
            Invariant::Durable,
            Invariant::Recovers,
        ] {
            assert_eq!(driver_for(invariant).drive(&obs), Verdict::Inconclusive);
        }
    }
}
