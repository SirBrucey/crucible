//! Judging what a run read, and going to get the reference runs it needs.
//!
//! A reference run drives the same steps on a clean fleet with nothing broken.
//! Nothing about one reaches the campaign's tally.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Display,
};

use crucible_core::{
    ipc::Verdict,
    plan,
    schedule::Schedule,
    verdict::{Ack, Judged, Readings, References},
};

/// How many reference runs one set of steps is worth.
///
/// What stopped one may not stop the next, so a failure does not spend the
/// set.
const ASKS_PER_SET: u32 = 2;

/// How many reference runs one run's verdict may cost.
///
/// Every step left in doubt doubles the states the fleet could be in. A run
/// that wants more than this says the state is unknown instead.
const REFERENCES_PER_RUN: usize = 6;

/// How much more work a campaign will take on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Taking {
    /// Whatever it is given.
    More,
    /// Nothing new. A reference run a verdict is already waiting on is still
    /// worth finishing.
    Finishing,
    /// Nothing at all. The fleet has failed too often for another run against
    /// it to mean anything.
    Nothing,
}

/// What has become of the campaign's asking after a set of steps.
///
/// [`Bench::ask_for`] sends one out, [`Bench::failed`] brings it back short of
/// an answer, and [`Bench::arrived`] closes it.
#[derive(Clone, Copy, Debug)]
enum Ask {
    /// A run is out for it. The count is how many the set has already cost
    /// without answering.
    Out(u32),
    /// Answered, and `references` holds where those steps leave the fleet.
    ///
    /// Apart from `Spent` so a log tells a set that answered from one the
    /// campaign gave up on. Both refuse any further asking.
    Answered,
    /// Nothing out for it, having cost this many runs, and worth one more.
    Failed(u32),
    /// Cost what the set is worth. Nothing asks after it again.
    Spent,
}

/// A run held back until a reference run says where its steps leave the fleet.
struct Parked {
    schedule_id: u32,
    readings: Readings,
}

/// What the bench made of a run.
pub enum Decided {
    Now((u32, Verdict)),
    Waiting(Vec<Vec<usize>>),
}

/// What a run read, judged against where the fleet stands having landed the
/// steps that run accepted.
pub struct Bench<'a> {
    fleet: &'a plan::Fleet,
    /// The scenario, which a reference run drives a subset of.
    scenario: &'a plan::Scenario,
    /// Where landing a set of steps leaves the fleet, learned by running them.
    references: References,
    /// What the campaign has already asked after, so runs wanting the same set
    /// share one run of it.
    asks: BTreeMap<Vec<usize>, Ask>,
    parked: Vec<Parked>,
    /// Reference runs waiting for a worker.
    queued: VecDeque<Schedule>,
    /// How many more reference runs may go out once the campaign has stopped
    /// taking work on.
    chase: usize,
    /// Reference runs are numbered down from the top, so their ids never
    /// collide with the scheduler's.
    next_id: u32,
}

impl<'a> Bench<'a> {
    pub fn new(fleet: &'a plan::Fleet, scenario: &'a plan::Scenario, chase: usize) -> Self {
        Self {
            fleet,
            scenario,
            references: References::new(),
            asks: BTreeMap::new(),
            parked: Vec::new(),
            queued: VecDeque::new(),
            chase,
            next_id: u32::MAX,
        }
    }

    /// What a run read says of the fleet, or nothing while it waits on a
    /// reference run to say where its steps leave it.
    pub fn judge(&mut self, schedule_id: u32, readings: Readings) -> Decided {
        match readings.judge(&self.references) {
            Judged::Now(verdict) => Decided::Now((schedule_id, verdict)),
            Judged::Pending(sets) if sets.len() > REFERENCES_PER_RUN => {
                tracing::warn!(
                    schedule_id,
                    wants = sets.len(),
                    limit = REFERENCES_PER_RUN,
                    "too many states left open to run them all; leaving the run unjudged"
                );
                Decided::Now((schedule_id, readings.settle(&self.references)))
            }
            Judged::Pending(sets) => {
                for set in &sets {
                    self.ask_for(set);
                }
                tracing::info!(schedule_id, wants = ?sets, "waiting on a reference run");
                self.parked.push(Parked {
                    schedule_id,
                    readings,
                });
                Decided::Waiting(sets)
            }
        }
    }

    /// Take in where a reference run left the fleet, and every verdict that was
    /// waiting on it.
    pub fn arrived(&mut self, landed: &[usize], readings: &Readings) -> Vec<(u32, Verdict)> {
        let accepted: Vec<usize> = readings
            .outcomes
            .iter()
            .enumerate()
            .filter(|(_, outcome)| outcome.ack == Ack::Acked)
            .filter_map(|(i, _)| landed.get(i).copied())
            .collect();
        // A run that was told these steps and turned some of them away has not
        // been where landing all of them leaves the fleet. It answers a
        // different question, and asking again would answer the same different
        // question, so the set is spent.
        if accepted != landed {
            tracing::warn!(
                drove = ?landed,
                ?accepted,
                "a clean fleet turned away steps this reference drove, so it cannot say where they leave the fleet"
            );
            self.asks.insert(landed.to_vec(), Ask::Spent);
            return Vec::new();
        }
        tracing::info!(?landed, "reference run settled");
        self.asks.insert(landed.to_vec(), Ask::Answered);
        self.references.insert(landed.to_vec(), readings.baseline());
        self.rejudge()
    }

    /// Take in a reference run that did not answer.
    ///
    /// The set is worth [`ASKS_PER_SET`] runs between every run that wants it.
    pub fn failed(&mut self, landed: &[usize], why: &dyn Display) {
        let cost = match self.asks.get(landed) {
            // Answered or given up on, so nothing is out for it to have failed.
            Some(Ask::Answered | Ask::Spent) => return,
            Some(Ask::Out(cost)) => *cost,
            // One run goes out per set, so these two mean two did.
            Some(Ask::Failed(cost)) => {
                tracing::warn!(?landed, "a second run failed for a set already back");
                *cost
            }
            None => {
                tracing::warn!(?landed, "a run failed for a set nothing asked after");
                0
            }
        };
        let asks = cost + 1;
        let ask = if asks < ASKS_PER_SET {
            Ask::Failed(asks)
        } else {
            Ask::Spent
        };
        self.asks.insert(landed.to_vec(), ask);
        tracing::warn!(?landed, %why, asks, ?ask, "a reference run did not answer");
    }

    /// The next reference run to send out, given how much more work the
    /// campaign will take on.
    pub fn wanted(&mut self, taking: Taking) -> Option<Schedule> {
        match taking {
            Taking::More => self.queued.pop_front(),
            // Chased far enough, or not worth chasing at all. What is still
            // waiting answers with what it has.
            Taking::Finishing if self.chase > 0 => {
                let reference = self.queued.pop_front()?;
                self.chase -= 1;
                Some(reference)
            }
            Taking::Finishing | Taking::Nothing => {
                self.queued.clear();
                None
            }
        }
    }

    /// What every run still waiting on a reference run settles for.
    pub fn settle(&mut self) -> Vec<(u32, Verdict)> {
        std::mem::take(&mut self.parked)
            .into_iter()
            .map(|parked| (parked.schedule_id, parked.readings.settle(&self.references)))
            .collect()
    }

    /// Judge everything held back, now that there is one more state to hold it
    /// to.
    fn rejudge(&mut self) -> Vec<(u32, Verdict)> {
        let mut judged = Vec::new();
        for parked in std::mem::take(&mut self.parked) {
            if let Decided::Now(verdict) = self.judge(parked.schedule_id, parked.readings) {
                judged.push(verdict);
            }
        }
        judged
    }

    /// Queue a run of `landed` and nothing else, unless one is already out, has
    /// already answered, or has cost the campaign what the set is worth.
    fn ask_for(&mut self, landed: &[usize]) {
        let cost = match self.asks.get(landed) {
            Some(Ask::Out(_) | Ask::Answered | Ask::Spent) => return,
            Some(Ask::Failed(cost)) => *cost,
            None => 0,
        };
        self.asks.insert(landed.to_vec(), Ask::Out(cost));
        // The steps are numbered from one, as the checkpoints are.
        let steps = landed
            .iter()
            .filter_map(|step| self.scenario.steps.get(step - 1))
            .cloned()
            .collect();
        self.next_id -= 1;
        self.queued.push_back(Schedule::reference(
            self.next_id,
            self.fleet.clone(),
            steps,
            self.scenario.checks.clone(),
            landed.to_vec(),
            self.scenario.consistent_within,
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crucible_core::{
        fault::Primitive,
        plan::{Check, Fleet, Scenario, Step, Value},
        verdict::{Baseline, Observed, Outcome},
    };
    use crucible_protocol::{At, FaultReport};

    use super::*;

    fn fleet() -> Fleet {
        Fleet {
            name: "orders".into(),
            deployment: "docker".into(),
            services: Vec::new(),
        }
    }

    fn step(n: i64) -> Step {
        Step {
            driver: "http".into(),
            operation: "POST".into(),
            args: vec![Value::Ident("api".into()), Value::Str(format!("/{n}"))],
            blocks: std::collections::BTreeMap::new(),
            expect: None,
        }
    }

    fn check() -> Check {
        Check {
            service: "db".into(),
            observer: "mariadb".into(),
            observable: vec!["writes".into(), "count".into()],
            args: Vec::new(),
            filter: None,
            clauses: std::collections::BTreeMap::new(),
            op: crucible_core::schema::CmpOp::Eq,
            value: Value::Int(2),
        }
    }

    fn scenario() -> Scenario {
        Scenario {
            name: "orders".into(),
            budget: None,
            consistent_within: Duration::from_secs(15),
            steps: vec![step(1), step(2), step(3)],
            checks: vec![check()],
        }
    }

    /// A run whose fault fired, that acknowledged `acks`, and that settled at
    /// `settled`.
    fn run(acks: &[Ack], fault_free: &[i64], settled: i64) -> Readings {
        let mut readings = Readings::empty();
        readings.fault = Some(FaultReport::fired(
            0,
            "db",
            Primitive::Kill,
            At::Throughout,
            0,
        ));
        readings.outcomes = acks.iter().map(|ack| Outcome { ack: *ack }).collect();
        readings.checks = vec![Observed {
            check: check(),
            value: Some(Value::Int(settled)),
        }];
        readings.fault_free =
            Baseline::unsettled(fault_free.iter().map(|n| vec![Some(Value::Int(*n))]));
        readings
    }

    /// What a reference run of `landed` settled at, as one would come back.
    fn answer(landed: &[usize], settled: i64) -> Readings {
        let mut readings = Readings::empty();
        readings.outcomes = landed.iter().map(|_| Outcome { ack: Ack::Acked }).collect();
        readings.checks = vec![Observed {
            check: check(),
            value: Some(Value::Int(settled)),
        }];
        readings
    }

    /// Each step left in doubt doubles the states the fleet could be in, and
    /// each is a run of the fleet to find out.
    #[test]
    fn a_run_wanting_more_runs_than_it_is_worth_is_left_unjudged() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        let doubted = [Ack::Unknown; 5];
        let readings = run(&doubted, &[0, 1, 2, 3, 4, 5], 99);
        let Decided::Now((id, verdict)) = bench.judge(1, readings) else {
            panic!("not worth waiting on")
        };
        assert_eq!(id, 1);
        assert!(matches!(verdict, Verdict::Inconclusive { .. }));
        assert!(
            bench.wanted(Taking::More).is_none(),
            "nothing was asked for"
        );
    }

    #[test]
    fn a_run_answering_to_a_checkpoint_is_judged_at_once() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        let judged = bench.judge(1, run(&[Ack::Acked, Ack::Acked], &[0, 1, 2], 2));
        assert!(matches!(judged, Decided::Now((1, Verdict::Pass))));
        assert!(bench.wanted(Taking::More).is_none());
    }

    #[test]
    fn a_run_answering_to_no_checkpoint_waits_for_a_run_of_its_own() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(1, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));

        let reference = bench
            .wanted(Taking::More)
            .expect("a reference was asked for");
        assert_eq!(reference.landed(), Some(&[2][..]));
        assert_eq!(reference.steps, vec![step(2)]);

        let judged = bench.arrived(&[2], &answer(&[2], 1));
        assert_eq!(judged, vec![(1, Verdict::Pass)]);
    }

    /// The reference is the campaign's, not the run's, so the second run to
    /// want it takes the first one's answer.
    #[test]
    fn runs_wanting_the_same_steps_share_one_reference() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        for id in [1, 2] {
            assert!(matches!(
                bench.judge(id, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
                Decided::Waiting(_)
            ));
        }
        assert!(bench.wanted(Taking::More).is_some());
        assert!(bench.wanted(Taking::More).is_none(), "one run, not two");

        let judged = bench.arrived(&[2], &answer(&[2], 1));
        assert_eq!(judged, vec![(1, Verdict::Pass), (2, Verdict::Pass)]);
    }

    #[test]
    fn a_set_the_fleet_keeps_failing_to_produce_stops_being_asked_for() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        for ask in 1..=ASKS_PER_SET {
            assert!(matches!(
                bench.judge(ask, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
                Decided::Waiting(_)
            ));
            assert!(
                bench.wanted(Taking::More).is_some(),
                "ask {ask} should have gone out"
            );
            bench.failed(&[2], &"docker said no");
        }
        assert!(matches!(
            bench.judge(99, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::More).is_none(), "the set is spent");
    }

    /// A run gets its states one at a time, and stays held back until the last
    /// lands.
    #[test]
    fn a_run_wanting_two_states_waits_for_both() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        // Step 1 refused, step 2 taken, step 3 in doubt, so the fleet may have
        // landed steps 2 and 3, or step 2 alone.
        let readings = run(&[Ack::Rejected, Ack::Acked, Ack::Unknown], &[0, 1, 2, 3], 2);
        assert!(matches!(bench.judge(1, readings), Decided::Waiting(_)));
        assert!(bench.wanted(Taking::More).is_some());
        assert!(bench.wanted(Taking::More).is_some(), "one run per state");

        assert!(
            bench.arrived(&[2], &answer(&[2], 1)).is_empty(),
            "still owed the other state"
        );
        assert_eq!(
            bench.arrived(&[2, 3], &answer(&[2, 3], 2)),
            vec![(1, Verdict::Pass)]
        );
        assert!(bench.settle().is_empty(), "nothing left waiting");
    }

    /// One failure is not the fleet saying it cannot produce a state.
    #[test]
    fn a_set_that_failed_once_is_asked_for_again_and_answers() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(1, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::More).is_some());
        bench.failed(&[2], &"the replica would not come up");

        // The next run to want it asks again, and this time it answers.
        assert!(matches!(
            bench.judge(2, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::More).is_some(), "asked a second time");
        assert_eq!(
            bench.arrived(&[2], &answer(&[2], 1)),
            vec![(1, Verdict::Pass), (2, Verdict::Pass)]
        );
    }

    /// A clean fleet that will not take the steps has not been where taking
    /// them leaves it.
    #[test]
    fn a_reference_that_turned_steps_away_answers_for_nothing() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(1, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::More).is_some(), "the run went out");
        let mut refused = answer(&[2], 0);
        refused.outcomes = vec![Outcome { ack: Ack::Rejected }];

        assert!(bench.arrived(&[2], &refused).is_empty());
        // Spent, not merely unanswered. Even a failure does not buy it another.
        bench.failed(&[2], &"the replica would not come up");
        assert!(matches!(
            bench.judge(2, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::More).is_none(), "the set is spent");

        let settled = bench.settle();
        assert_eq!(settled.len(), 2);
        assert!(
            settled
                .iter()
                .all(|(_, verdict)| matches!(verdict, Verdict::Inconclusive { .. }))
        );
    }

    /// A late report cannot take away what an answered set found.
    #[test]
    fn what_a_reference_found_survives_a_later_failure() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(1, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::More).is_some());
        assert_eq!(
            bench.arrived(&[2], &answer(&[2], 1)),
            vec![(1, Verdict::Pass)]
        );

        bench.failed(&[2], &"a straggler that answers for nothing");
        // Still answered. The next run to want those steps is judged, not held.
        assert!(matches!(
            bench.judge(2, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Now((2, Verdict::Pass))
        ));
        assert!(bench.wanted(Taking::More).is_none(), "nothing asked again");
    }

    #[test]
    fn a_campaign_that_gave_up_chases_nothing() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(1, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        assert!(bench.wanted(Taking::Nothing).is_none());
        assert!(
            bench.wanted(Taking::More).is_none(),
            "what it gave up on stays given up on"
        );
    }

    /// The chase is what a stopped campaign spends finishing what it started,
    /// and it is spent once.
    #[test]
    fn a_stopped_campaign_chases_one_worker_deep() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 2);
        // Three runs, each having accepted different steps, so each wants a
        // reference run of its own rather than sharing one.
        for (id, acks) in [
            (1, [Ack::Rejected, Ack::Acked, Ack::Rejected]),
            (2, [Ack::Rejected, Ack::Rejected, Ack::Acked]),
            (3, [Ack::Rejected, Ack::Acked, Ack::Acked]),
        ] {
            assert!(
                matches!(
                    bench.judge(id, run(&acks, &[0, 1, 2, 3], 1)),
                    Decided::Waiting(_)
                ),
                "run {id} should be waiting"
            );
        }
        assert!(bench.wanted(Taking::Finishing).is_some());
        assert!(bench.wanted(Taking::Finishing).is_some());
        assert!(
            bench.wanted(Taking::Finishing).is_none(),
            "two workers deep is the whole chase"
        );
    }

    #[test]
    fn what_settles_for_want_of_a_reference_settles_once() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(7, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        let settled = bench.settle();
        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].0, 7);
        assert!(matches!(settled[0].1, Verdict::Inconclusive { .. }));
        assert!(bench.settle().is_empty(), "settled once");
    }

    #[test]
    fn a_reference_run_carries_the_scenario_it_answers_for() {
        let (fleet, scenario) = (fleet(), scenario());
        let mut bench = Bench::new(&fleet, &scenario, 3);
        assert!(matches!(
            bench.judge(1, run(&[Ack::Rejected, Ack::Acked], &[0, 1, 2], 1)),
            Decided::Waiting(_)
        ));
        let reference = bench.wanted(Taking::More).expect("asked for");
        assert_eq!(reference.checks, scenario.checks);
        assert_eq!(reference.consistent_within, scenario.consistent_within);
        assert_eq!(reference.fault_free, Baseline::default());
        assert!(reference.fault().is_none());
    }
}
