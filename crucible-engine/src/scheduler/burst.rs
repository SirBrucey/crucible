//! Burst scheduler: faults placed against the bursts of traffic the learn pass
//! saw (it does the clustering, see
//! [`crucible_core::proxy_log::edge_profiles_from_sessions`]).
//!
//! Every burst on every edge is faulted before any burst is faulted at more than
//! one point, so a short budget costs resolution within a burst rather than
//! whole edges. Where it will not stretch to one point per burst, the busiest
//! bursts take what there is.

use crucible_protocol::{Burst, Direction, Edge};

use crucible_core::{
    fault::{Anchor, Fault, Losing, Primitive, Taking},
    learned::Learned,
    plan,
    schedule::Schedule,
    verdict::Invariant,
};

use super::{Budget, Scheduler};

/// The points a burst offers a fault: its middle and its two boundaries.
const POINTS_PER_BURST: usize = 3;

pub struct BurstScheduler {
    coverage: Coverage,
    schedules: std::vec::IntoIter<Schedule>,
}

/// What the campaign reaches, and what the budget cost it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Coverage {
    /// Bursts the learn run saw, across every edge.
    pub bursts: usize,
    /// Bursts a fault is placed in.
    pub probed: usize,
    /// Points placed in each of them, one to three.
    pub points: usize,
    pub schedules: usize,
}

impl std::fmt::Display for Coverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Coverage {
            bursts,
            probed,
            points,
            schedules,
        } = self;
        write!(
            f,
            "{schedules} schedules, faulting {probed} of {bursts} bursts at {points} point(s) each"
        )
    }
}

impl BurstScheduler {
    /// Fit the bursts the learn pass found into `budget`. Every burst on every
    /// edge is faulted before any is faulted harder, so what a short budget
    /// costs is resolution within a burst rather than whole edges.
    ///
    /// No budget places every point in every burst. Each schedule carries the
    /// work to run as well as the fault, so a worker needs nothing else to run
    /// it.
    #[must_use]
    pub fn new(
        fleet: &plan::Fleet,
        scenario: &plan::Scenario,
        learned: &Learned,
        testable: &[(Invariant, Vec<Primitive>)],
        budget: Option<Budget>,
    ) -> Self {
        let mut edges: Vec<(&Edge, Direction, &[Burst])> = learned
            .profiles
            .iter()
            .flat_map(|p| {
                [
                    (
                        &p.edge,
                        Direction::ClientToUpstream,
                        p.client_to_upstream.as_slice(),
                    ),
                    (
                        &p.edge,
                        Direction::UpstreamToClient,
                        p.upstream_to_client.as_slice(),
                    ),
                ]
            })
            .filter(|(_, _, bursts)| !bursts.is_empty())
            .collect();
        edges.sort_by_key(|(edge, _, _)| *edge);

        // One point in one burst is faulted every way the fleet can be broken
        // there, and covering those comes before resolution, so they multiply
        // what a point costs. A way no burst can place must not be costed.
        let ways: Vec<(Invariant, Losing)> = testable
            .iter()
            .flat_map(|(invariant, ways)| ways.iter().map(|by| (*invariant, *by)))
            .filter_map(|(invariant, by)| Some((invariant, placeable(invariant, by)?)))
            .collect();
        let seen: usize = edges.iter().map(|(_, _, bursts)| bursts.len()).sum();

        // Interleaved, so an edge's second burst comes after every edge's first.
        let mut probed: Vec<(&Edge, Direction, Burst)> = Vec::new();
        for nth in 0..edges.iter().map(|(_, _, b)| b.len()).max().unwrap_or(0) {
            for (edge, direction, bursts) in &edges {
                if let Some(&burst) = bursts.get(nth) {
                    probed.push((edge, *direction, burst));
                }
            }
        }

        // What one point in one burst costs, which is not the same everywhere:
        // an edge dialled from outside the fleet has only one end to kill.
        let cost =
            |edge: &Edge| -> usize { ways.iter().map(|(_, by)| targets(*by, edge).len()).sum() };
        let at_one_point: usize = probed.iter().map(|(edge, ..)| cost(edge)).sum();
        let fitted = Fitted::new(budget, at_one_point);
        if let Fitted::Busiest(budget) = fitted {
            // The busiest bursts take what there is. Costs differ per burst, so
            // how many fit is only knowable in the order they will be kept.
            probed.sort_by_key(|(_, _, burst)| std::cmp::Reverse(burst.packets));
            let mut spent = 0;
            probed.retain(|(edge, ..)| {
                let affordable = budget.fits(spent + cost(edge));
                if affordable {
                    spent += cost(edge);
                }
                affordable
            });
        }

        let mut schedules: Vec<Schedule> = Vec::new();
        let mut next_id: u32 = Schedule::LEARN_ID + 1;
        for (edge, direction, burst) in &probed {
            for k in fitted.points(*burst, *direction) {
                for (invariant, by) in &ways {
                    for taking in targets(*by, edge) {
                        let anchor = Anchor {
                            edge: (*edge).clone(),
                            direction: *direction,
                            k,
                        };
                        let fault = match invariant {
                            Invariant::Durable => Fault::Durable { anchor, by: taking },
                            // `placeable` kept these out of `ways`.
                            Invariant::Recovers | Invariant::Idempotent | Invariant::Converges => {
                                continue;
                            }
                        };
                        schedules.push(Schedule::faulted(
                            next_id,
                            fleet.clone(),
                            scenario.steps.clone(),
                            scenario.checks.clone(),
                            fault,
                            learned.trajectory.clone(),
                            scenario.consistent_within,
                        ));
                        next_id += 1;
                    }
                }
            }
        }

        Self {
            coverage: Coverage {
                bursts: seen,
                probed: probed.len(),
                points: fitted.points_per_burst(),
                schedules: schedules.len(),
            },
            schedules: schedules.into_iter(),
        }
    }

    /// Total schedules this hands out.
    #[must_use]
    pub fn total(&self) -> usize {
        self.coverage.schedules
    }

    /// What the campaign reaches, for the runner to report.
    #[must_use]
    pub fn coverage(&self) -> Coverage {
        self.coverage
    }
}

/// How much of each burst a budget affords.
#[derive(Clone, Copy, Debug)]
enum Fitted {
    /// Every burst, at this many points each.
    Every(usize),
    /// One point each, in as many of the busiest bursts as this affords. What is
    /// left when the budget does not stretch to every burst, so it carries the
    /// budget that could not.
    Busiest(Budget),
}

impl Fitted {
    /// What `budget` buys, given that faulting every burst at one point costs
    /// `at_one_point` schedules.
    fn new(budget: Option<Budget>, at_one_point: usize) -> Self {
        let Some(budget) = budget else {
            return Fitted::Every(POINTS_PER_BURST);
        };
        for points in (1..=POINTS_PER_BURST).rev() {
            if budget.fits(at_one_point * points) {
                return Fitted::Every(points);
            }
        }
        Fitted::Busiest(budget)
    }

    fn points_per_burst(self) -> usize {
        match self {
            Fitted::Every(points) => points,
            Fitted::Busiest(_) => 1,
        }
    }

    /// Where in `burst` to place a fault, from the middle outwards.
    ///
    /// A second point goes to the boundary the service is on: it is the source of
    /// an outbound burst, so the end is where it has emitted everything and is
    /// about to write it down; it is the target of an inbound one, so the start
    /// is where it has taken delivery and not yet acted. A `start` of zero is not
    /// placeable, since freezing there kills the service before the scenario has
    /// driven anything across the edge.
    fn points(self, burst: Burst, direction: Direction) -> Vec<u32> {
        let outer = match direction {
            Direction::UpstreamToClient => [burst.end, burst.start],
            Direction::ClientToUpstream => [burst.start, burst.end],
        };
        let mut points = vec![burst.mid];
        points.extend(outer.into_iter().take(self.points_per_burst() - 1));
        points.retain(|&k| k > 0);
        points.sort_unstable();
        points.dedup();
        points
    }
}

/// What breaking `edge` by `losing` can take: either end of it if the fleet is
/// to lose a service, the edge itself if it is to lose the link. An edge dialled
/// from outside the fleet has only the one end we can reach.
fn targets(losing: Losing, edge: &Edge) -> Vec<Taking> {
    match losing {
        Losing::Kill => edge
            .client
            .iter()
            .chain(std::iter::once(&edge.upstream))
            .map(|service| Taking::Kill(service.clone()))
            .collect(),
        Losing::Cut => vec![Taking::Cut(edge.clone())],
    }
}

/// How a burst breaks the fleet to test `invariant`, if it can. Recovery
/// degrades the fleet from the start rather than part way through, so a burst
/// cannot test it.
fn placeable(invariant: Invariant, by: Primitive) -> Option<Losing> {
    match invariant {
        Invariant::Durable => Losing::try_from(by).ok(),
        Invariant::Recovers | Invariant::Idempotent | Invariant::Converges => None,
    }
}

impl Scheduler for BurstScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use crucible_protocol::EdgeProfile;

    use super::*;
    use crate::scheduler::fixture;

    /// A burst of `packets` packets, the `nth` on its edge.
    fn burst(nth: u32, packets: u32) -> Burst {
        let first = nth * packets + 1;
        let last = first + packets - 1;
        Burst {
            start: first - 1,
            mid: u32::midpoint(first, last),
            end: last,
            packets,
        }
    }

    /// An edge to `upstream`, dialled from outside the fleet so it has one end
    /// to kill and a test says how many schedules it expects.
    fn profile(upstream: &str, c2u: Vec<Burst>, u2c: Vec<Burst>) -> EdgeProfile {
        from(None, upstream, c2u, u2c)
    }

    fn from(client: Option<&str>, upstream: &str, c2u: Vec<Burst>, u2c: Vec<Burst>) -> EdgeProfile {
        EdgeProfile {
            edge: Edge {
                client: client.map(ToOwned::to_owned),
                upstream: upstream.into(),
            },
            client_to_upstream: c2u,
            upstream_to_client: u2c,
        }
    }

    /// An unbounded campaign against a fleet that can be killed, so durability
    /// is on.
    fn scheduler(profiles: &[EdgeProfile]) -> BurstScheduler {
        driven(profiles, vec![Primitive::Kill], None)
    }

    /// A budget that affords exactly `schedules` runs.
    fn affording(schedules: u32) -> Budget {
        Budget {
            left: Duration::from_secs(1) * schedules,
            cost: Duration::from_secs(1),
            concurrency: 1,
        }
    }

    /// A campaign testing durability by every way of breaking the fleet in
    /// `ways`, fitted to a budget affording `capacity` schedules.
    fn driven(
        profiles: &[EdgeProfile],
        ways: Vec<Primitive>,
        capacity: Option<u32>,
    ) -> BurstScheduler {
        let learned = Learned {
            profiles: profiles.to_vec(),
            trajectory: Vec::new(),
            primitives: ways.iter().copied().collect(),
        };
        BurstScheduler::new(
            &fixture::fleet(),
            &fixture::scenario(),
            &learned,
            &[(Invariant::Durable, ways)],
            capacity.map(affording),
        )
    }

    /// Where a burst schedule's fault lands.
    fn fault(schedule: &Schedule) -> &Anchor {
        schedule
            .fault
            .as_ref()
            .expect("a burst schedule always faults")
            .anchor()
            .expect("a burst schedule always anchors")
    }

    /// A fleet whose loaded plugins cannot break anything cannot schedule any
    /// faults, so the campaign is a fault-free run only.
    #[test]
    fn nothing_testable_yields_nothing() {
        let learned = Learned {
            profiles: vec![profile("db", vec![burst(0, 4)], vec![])],
            trajectory: Vec::new(),
            primitives: BTreeSet::new(),
        };
        let mut s =
            BurstScheduler::new(&fixture::fleet(), &fixture::scenario(), &learned, &[], None);
        assert_eq!(s.total(), 0);
        assert!(s.next().is_none());
    }

    #[test]
    fn empty_profiles_yield_nothing() {
        let mut s = scheduler(&[]);
        assert_eq!(s.total(), 0);
        assert!(s.next().is_none());
    }

    #[test]
    fn an_unbounded_campaign_faults_every_point_of_every_burst() {
        let s = scheduler(&[profile("db", vec![burst(1, 4), burst(2, 4)], vec![])]);
        assert_eq!(s.total(), 6);
        assert_eq!(s.coverage().probed, 2);
        assert_eq!(s.coverage().points, 3);
    }

    /// Freezing before the first packet kills the service before the scenario
    /// has driven anything across the edge, so that point is not placeable.
    #[test]
    fn the_first_burst_on_an_edge_offers_no_start() {
        let s = scheduler(&[profile("db", vec![burst(0, 4)], vec![])]);
        assert_eq!(s.total(), 2);
    }

    #[test]
    fn every_way_of_breaking_the_fleet_gets_its_own_schedule() {
        let s = driven(
            &[profile("db", vec![burst(1, 4)], vec![])],
            vec![Primitive::Kill, Primitive::Cut],
            None,
        );
        assert_eq!(s.total(), 6);
    }

    #[test]
    fn both_directions_are_scheduled() {
        let scheduler = scheduler(&[profile("db", vec![burst(0, 1)], vec![burst(0, 1)])]);
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert!(
            all.iter()
                .any(|s| fault(s).direction == Direction::ClientToUpstream)
        );
        assert!(
            all.iter()
                .any(|s| fault(s).direction == Direction::UpstreamToClient)
        );
    }

    #[test]
    fn emission_is_round_robin_across_edges() {
        let scheduler = scheduler(&[
            profile("api", vec![burst(0, 1)], vec![]),
            profile("db", vec![burst(0, 1)], vec![]),
        ]);
        let first_two: Vec<String> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .take(2)
        .map(|s| fault(&s).edge.upstream.clone())
        .collect();
        assert!(first_two.contains(&"api".to_string()));
        assert!(first_two.contains(&"db".to_string()));
    }

    /// What each schedule takes away.
    fn taken(s: BurstScheduler) -> Vec<String> {
        std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .map(|s| {
            s.fault
                .as_ref()
                .expect("a burst schedule always faults")
                .taking()
                .target()
        })
        .collect()
    }

    #[test]
    fn a_kill_can_take_either_end_of_an_edge() {
        let taken = taken(driven(
            &[from(Some("inventory"), "broker", vec![burst(1, 4)], vec![])],
            vec![Primitive::Kill],
            None,
        ));
        assert!(taken.contains(&"inventory".to_string()));
        assert!(taken.contains(&"broker".to_string()));
    }

    #[test]
    fn an_edge_from_outside_the_fleet_offers_only_its_upstream() {
        let taken = taken(driven(
            &[profile("api", vec![burst(1, 4)], vec![])],
            vec![Primitive::Kill],
            None,
        ));
        assert!(taken.iter().all(|target| target == "api"), "{taken:?}");
    }

    #[test]
    fn a_cut_takes_the_edge_itself() {
        let taken = taken(driven(
            &[from(Some("api"), "db", vec![burst(1, 4)], vec![])],
            vec![Primitive::Cut],
            None,
        ));
        assert!(
            taken.iter().all(|target| target == "api -> db"),
            "{taken:?}"
        );
    }

    #[test]
    fn a_budget_that_affords_one_point_takes_the_middle_of_each_burst() {
        let s = driven(
            &[profile("db", vec![burst(0, 4), burst(1, 4)], vec![])],
            vec![Primitive::Kill],
            Some(2),
        );
        assert_eq!(s.coverage().points, 1);
        let at: Vec<u32> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .map(|s| fault(&s).k)
        .collect();
        assert_eq!(at, [burst(0, 4).mid, burst(1, 4).mid]);
    }

    #[test]
    fn a_second_point_lands_on_the_boundary_the_service_is_on() {
        let s = driven(
            &[profile("db", vec![burst(1, 4)], vec![burst(1, 4)])],
            vec![Primitive::Kill],
            Some(4),
        );
        assert_eq!(s.coverage().points, 2);
        let placed: Vec<(Direction, u32)> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .map(|s| (fault(&s).direction, fault(&s).k))
        .collect();
        let burst = burst(1, 4);
        // Receiving, so the start: it has taken delivery and not yet acted.
        assert!(placed.contains(&(Direction::ClientToUpstream, burst.start)));
        // Sending, so the end: it has emitted everything and owes a write.
        assert!(placed.contains(&(Direction::UpstreamToClient, burst.end)));
    }

    #[test]
    fn a_budget_below_one_point_per_burst_keeps_the_busiest() {
        let s = driven(
            &[profile("db", vec![burst(0, 1), burst(1, 9)], vec![])],
            vec![Primitive::Kill],
            Some(1),
        );
        assert_eq!(s.coverage().probed, 1);
        let mut s = s;
        let only = s.next().expect("the budget affords one");
        assert_eq!(fault(&only).k, burst(1, 9).mid);
    }

    #[test]
    fn an_invariant_no_burst_can_place_costs_nothing() {
        let learned = Learned {
            profiles: vec![profile("db", vec![burst(1, 4), burst(2, 4)], vec![])],
            trajectory: Vec::new(),
            primitives: BTreeSet::from([Primitive::Kill]),
        };
        let s = BurstScheduler::new(
            &fixture::fleet(),
            &fixture::scenario(),
            &learned,
            &[
                (Invariant::Durable, vec![Primitive::Kill]),
                (Invariant::Recovers, vec![Primitive::Kill]),
            ],
            Some(affording(6)),
        );
        assert_eq!(
            s.coverage().points,
            3,
            "recovery took a share of the budget"
        );
        assert_eq!(s.total(), 6);
    }

    #[test]
    fn covering_both_ways_of_breaking_the_fleet_comes_before_resolution() {
        let s = driven(
            &[profile("db", vec![burst(0, 4)], vec![])],
            vec![Primitive::Kill, Primitive::Cut],
            Some(2),
        );
        assert_eq!(s.coverage().points, 1);
        assert_eq!(s.total(), 2);
    }
}
