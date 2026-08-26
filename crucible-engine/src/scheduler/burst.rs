//! Faults placed at the points the fault-free run turned up.
//!
//! Where a plugin read an edge, it said what its moments are and what each
//! catches. Where nothing could, all that is known is the traffic that crossed
//! it, clustered into bursts (see
//! [`crucible_core::proxy_log::edge_profiles_from_sessions`]).
//!
//! Every edge is faulted at one point before any edge is faulted at two, so a
//! short budget costs depth rather than whole edges.

use crucible_protocol::{Burst, Direction, Edge, EdgeProfile};

use crucible_core::{
    fault::{Anchor, Fault, Losing, Primitive, Taking},
    learned::Learned,
    plan,
    schedule::Schedule,
    verdict::Invariant,
};

use super::{Budget, Scheduler};

/// Somewhere on an edge a fault can go.
///
/// An edge a plugin can read says where its own moments are and what they
/// catch. One nothing can read offers only counts of what crossed it, which is
/// what the bursts are for.
#[derive(Clone, Debug)]
struct Point<'a> {
    edge: &'a Edge,
    direction: Direction,
    mark: String,
    why: String,
    /// What faulting here is good for. A moment a plugin named is only a moment
    /// for the property it named it for.
    exercises: Invariant,
    /// How much traffic was around it, so a short budget keeps the busiest.
    weight: u32,
}

/// Where a fault can go on `profile`, in the order the points should be spent.
///
/// A plugin that read the edge has already said what its moments are and what
/// each catches. Otherwise all that is known is what crossed it, so a fault goes
/// by the middle of a burst first and its edges after.
fn points_on(profile: &EdgeProfile) -> Vec<Point<'_>> {
    if !profile.placements.is_empty() {
        return profile
            .placements
            .iter()
            .map(|placement| Point {
                edge: &profile.edge,
                direction: placement.direction,
                mark: placement.mark.clone(),
                why: placement.why.clone(),
                exercises: placement.exercises.into(),
                weight: 1,
            })
            .collect();
    }

    // Per burst, in preference order, then transposed so every burst gets its
    // middle before any gets an edge.
    let mut rounds: Vec<Vec<Point<'_>>> = Vec::new();
    for (direction, bursts) in [
        (Direction::ClientToUpstream, &profile.client_to_upstream),
        (Direction::UpstreamToClient, &profile.upstream_to_client),
    ] {
        for burst in bursts {
            for (round, k) in points_in(*burst, direction).into_iter().enumerate() {
                if rounds.len() == round {
                    rounds.push(Vec::new());
                }
                rounds[round].push(Point {
                    edge: &profile.edge,
                    direction,
                    mark: k.to_string(),
                    why: format!("{k} reads into what this edge carried"),
                    // Nothing read this edge, so all a moment on it can say is
                    // that the fleet was part way through something.
                    exercises: Invariant::Durable,
                    weight: burst.packets,
                });
            }
        }
    }
    // Where a budget runs out part way through, the busiest bursts are the ones
    // worth keeping.
    for round in &mut rounds {
        round.sort_by_key(|point| std::cmp::Reverse(point.weight));
    }
    rounds.concat()
}

/// Where in `burst` a fault can go, from the middle outwards.
///
/// The outer point is the boundary the service is on: it is the source of an
/// outbound burst, so the end is where it has emitted everything and is about
/// to write it down; it is the target of an inbound one, so the start is where
/// it has taken delivery and not yet acted. A `start` of zero is not placeable,
/// since freezing there kills the service before the scenario has driven
/// anything across the edge.
fn points_in(burst: Burst, direction: Direction) -> Vec<u32> {
    let outer = match direction {
        Direction::UpstreamToClient => [burst.end, burst.start],
        Direction::ClientToUpstream => [burst.start, burst.end],
    };
    let mut points = vec![burst.mid];
    points.extend(outer);
    points.retain(|&k| k > 0);
    points.dedup();
    points
}

pub struct BurstScheduler {
    coverage: Coverage,
    schedules: std::vec::IntoIter<Schedule>,
}

/// What the campaign reaches, and what the budget cost it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Coverage {
    /// Points the fault-free run turned up, across every edge.
    pub found: usize,
    /// Points a fault is placed at.
    pub taken: usize,
    pub schedules: usize,
}

impl std::fmt::Display for Coverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Coverage {
            found,
            taken,
            schedules,
        } = self;
        write!(
            f,
            "{schedules} schedules, faulting {taken} of {found} points"
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
        // One point is faulted every way the fleet can be broken there, and
        // covering those comes before covering more points, so they multiply
        // what a point costs. A way no point can place must not be costed.
        let ways: Vec<(Invariant, Losing)> = testable
            .iter()
            .flat_map(|(invariant, ways)| ways.iter().map(|by| (*invariant, *by)))
            .filter_map(|(invariant, by)| Some((invariant, placeable(invariant, by)?)))
            .collect();

        // Interleaved, so an edge's second point comes after every edge's
        // first.
        let on_edge: Vec<Vec<Point<'_>>> = learned.profiles.iter().map(points_on).collect();
        let found: usize = on_edge.iter().map(Vec::len).sum();
        let mut points: Vec<Point<'_>> = Vec::with_capacity(found);
        for round in 0..on_edge.iter().map(Vec::len).max().unwrap_or(0) {
            let mut taken: Vec<Point<'_>> = on_edge
                .iter()
                .filter_map(|edge| edge.get(round))
                .cloned()
                .collect();
            // Where a budget runs out part way through a round, the busiest
            // points are the ones worth keeping.
            taken.sort_by_key(|point| std::cmp::Reverse(point.weight));
            points.extend(taken);
        }

        // What one point costs, which is not the same everywhere: what an edge
        // has to break depends on the edge, and a moment is only spent on what
        // it is good for.
        let cost = |point: &Point<'_>| -> usize {
            ways.iter()
                .filter(|(invariant, _)| *invariant == point.exercises)
                .map(|(_, by)| targets(*by, point.edge).len())
                .sum()
        };
        // A moment nothing can be placed on is not a moment this campaign has.
        points.retain(|point| cost(point) > 0);
        if let Some(budget) = budget {
            let mut spent = 0;
            points.retain(|point| {
                let affordable = budget.fits(spent + cost(point));
                if affordable {
                    spent += cost(point);
                }
                affordable
            });
        }

        let mut schedules: Vec<Schedule> = Vec::new();
        let mut next_id: u32 = Schedule::LEARN_ID + 1;
        for point in &points {
            for (invariant, by) in ways.iter().filter(|(it, _)| *it == point.exercises) {
                for taking in targets(*by, point.edge) {
                    let anchor = Anchor {
                        edge: point.edge.clone(),
                        direction: point.direction,
                        mark: point.mark.clone(),
                        why: point.why.clone(),
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

        Self {
            coverage: Coverage {
                found,
                taken: points.len(),
                schedules: schedules.len(),
            },
            schedules: schedules.into_iter(),
        }
    }

    /// What the campaign reaches, for the runner to report.
    #[must_use]
    pub fn coverage(&self) -> Coverage {
        self.coverage
    }
}

/// What breaking `edge` by `losing` takes from the fleet: the services at its
/// ends, or the link between them.
fn targets(losing: Losing, edge: &Edge) -> Vec<Taking> {
    match losing {
        Losing::Kill => edge
            .client
            .iter()
            .chain(std::iter::once(&edge.upstream))
            .map(|service| Taking::Kill(service.clone()))
            .collect(),
        Losing::Cut => edge
            .within_fleet()
            .then(|| Taking::Cut(edge.clone()))
            .into_iter()
            .collect(),
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

    fn total(&self) -> usize {
        self.coverage.schedules
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
            placements: Vec::new(),
            edge: Edge {
                client: client.map(ToOwned::to_owned),
                upstream: upstream.into(),
            },
            client_to_upstream: c2u,
            upstream_to_client: u2c,
        }
    }

    /// An edge a plugin read, which named `moments` on it.
    fn named(moments: Vec<(&str, crucible_protocol::Property)>) -> EdgeProfile {
        EdgeProfile {
            placements: moments
                .into_iter()
                .map(|(mark, exercises)| crucible_protocol::Placement {
                    direction: Direction::ClientToUpstream,
                    mark: mark.to_owned(),
                    why: format!("what {mark} catches"),
                    exercises,
                })
                .collect(),
            ..from(Some("api"), "broker", vec![], vec![])
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
        assert_eq!(s.coverage().taken, 6, "every point of both bursts");
    }

    /// Freezing before the first packet kills the service before the scenario
    /// has driven anything across the edge, so that point is not placeable.
    #[test]
    fn the_first_burst_on_an_edge_offers_no_start() {
        let s = scheduler(&[profile("db", vec![burst(0, 4)], vec![])]);
        assert_eq!(s.total(), 2);
    }

    /// Both ends and the link between them, at each of the burst's points.
    #[test]
    fn every_way_of_breaking_the_fleet_gets_its_own_schedule() {
        let s = driven(
            &[from(Some("api"), "db", vec![burst(1, 4)], vec![])],
            vec![Primitive::Kill, Primitive::Cut],
            None,
        );
        assert_eq!(s.total(), 9);
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

    /// The framework reaches in over an edge of its own to drive the scenario.
    /// Cutting that stops the steps rather than the fleet, so there is no link
    /// there to take.
    #[test]
    fn the_edge_the_scenario_is_driven_over_is_not_cut() {
        let s = driven(
            &[profile("api", vec![burst(1, 4)], vec![])],
            vec![Primitive::Cut],
            None,
        );
        assert_eq!(s.total(), 0);
    }

    /// A plugin names a moment for one property. Faulting there for another
    /// asks it to hold the fleet somewhere it was never watching, and the run
    /// comes back having tested nothing.
    #[test]
    fn a_moment_is_only_spent_on_what_it_was_named_for() {
        let s = driven(
            &[named(vec![
                ("ack:1:before", crucible_protocol::Property::Durable),
                ("redeliver:1", crucible_protocol::Property::Idempotent),
            ])],
            vec![Primitive::Kill],
            None,
        );
        let marks: Vec<String> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .filter_map(|schedule| schedule.fault?.anchor().map(|at| at.mark.clone()))
        .collect();
        assert!(marks.iter().all(|mark| mark == "ack:1:before"), "{marks:?}");
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
        assert_eq!(s.coverage().taken, 2, "one point in each burst");
        let at: Vec<String> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .map(|s| fault(&s).mark.clone())
        .collect();
        assert_eq!(
            at,
            [burst(0, 4).mid.to_string(), burst(1, 4).mid.to_string()]
        );
    }

    #[test]
    fn a_second_point_lands_on_the_boundary_the_service_is_on() {
        let s = driven(
            &[profile("db", vec![burst(1, 4)], vec![burst(1, 4)])],
            vec![Primitive::Kill],
            Some(4),
        );
        assert_eq!(s.coverage().taken, 4, "both points each way");
        let placed: Vec<(Direction, String)> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .map(|s| (fault(&s).direction, fault(&s).mark.clone()))
        .collect();
        let burst = burst(1, 4);
        // Receiving, so the start: it has taken delivery and not yet acted.
        assert!(placed.contains(&(Direction::ClientToUpstream, burst.start.to_string())));
        // Sending, so the end: it has emitted everything and owes a write.
        assert!(placed.contains(&(Direction::UpstreamToClient, burst.end.to_string())));
    }

    #[test]
    fn a_budget_below_one_point_per_burst_keeps_the_busiest() {
        let s = driven(
            &[profile("db", vec![burst(0, 1), burst(1, 9)], vec![])],
            vec![Primitive::Kill],
            Some(1),
        );
        assert_eq!(s.coverage().taken, 1);
        let mut s = s;
        let only = s.next().expect("the budget affords one");
        assert_eq!(fault(&only).mark, burst(1, 9).mid.to_string());
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
        assert_eq!(s.coverage().taken, 6, "recovery took a share of the budget");
        assert_eq!(s.total(), 6);
    }

    #[test]
    fn covering_both_ways_of_breaking_the_fleet_comes_before_more_points() {
        let s = driven(
            &[from(Some("api"), "db", vec![burst(0, 4)], vec![])],
            vec![Primitive::Kill, Primitive::Cut],
            Some(3),
        );
        assert_eq!(s.coverage().taken, 1, "one point, broken every way");
        assert_eq!(s.total(), 3);
    }
}
