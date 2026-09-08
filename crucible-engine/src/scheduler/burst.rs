//! Faults placed at the points the fault-free run turned up.
//!
//! Where a plugin read an edge, it said what its moments are and what breaking
//! the fleet at each of them is. Where nothing could, all that is known is the
//! traffic that crossed it, clustered into bursts (see
//! [`crucible_core::proxy_log::edge_profiles_from_sessions`]).
//!
//! A schedule is a way of breaking the fleet at a moment. Which invariant that
//! broke is read off the run afterwards.
//!
//! Every edge is faulted at one point before any edge is faulted at two, so a
//! short budget costs depth rather than whole edges.

use std::collections::BTreeSet;

use crucible_protocol::{Burst, Direction, Doing, Edge, EdgeProfile, Reached, Side};

use crucible_core::{
    fault::{Anchor, By, Drive, Fault},
    learned::Learned,
    plan,
    schedule::Schedule,
};

use super::{Budget, Scheduler};

/// Somewhere a fault can go.
///
/// An edge a plugin can read says where its own moments are and what they
/// catch. One nothing can read offers only counts of what crossed it, which is
/// what the bursts are for. A service that reports its own moments offers those
/// too, and they are not on an edge.
#[derive(Clone, Debug)]
struct Point<'a> {
    /// What reaches this moment, which is what can be broken.
    at: Where<'a>,
    mark: String,
    why: String,
    /// What breaking the fleet here is. This is also what decides which
    /// invariants a fault here could show, so it is the whole of what a moment
    /// says about its own worth.
    doing: Doing,
    /// How many packets were in the burst this came from, so a short budget
    /// keeps the busiest. A moment a plugin named came from no burst.
    packets: Option<u32>,
}

/// What reaches a point, and so what a fault placed there is anchored to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Where<'a> {
    /// Traffic crossing an edge.
    Crossing {
        edge: &'a Edge,
        direction: Direction,
    },
    /// A service reaching a point inside itself.
    Inside { service: &'a str },
}

impl Where<'_> {
    /// The anchor a schedule carries for a fault placed here.
    fn anchoring(self, mark: String, why: String) -> Anchor {
        match self {
            Where::Crossing { edge, direction } => {
                Anchor::crossing(edge.clone(), direction, mark, why)
            }
            Where::Inside { service } => Anchor::inside(service.to_owned(), mark, why),
        }
    }
}

impl Point<'_> {
    /// The group this point takes its turn in, so the budget is spread across
    /// the ways of breaking the fleet and neither the bursts nor the edges
    /// crowd out the moments inside services.
    ///
    /// Where a moment is counts as much as how it breaks the fleet: one inside
    /// a service reaches a gap nothing on the wire marks, so it is not
    /// interchangeable with an edge moment that breaks the fleet the same way.
    fn bucket(&self) -> (bool, bool, Doing) {
        (
            self.packets.is_none(),
            matches!(self.at, Where::Inside { .. }),
            self.doing,
        )
    }
}

/// Where a fault can go on `profile`, in the order the points should be spent.
///
/// A plugin that read the edge has already said what its moments are and how
/// each breaks the fleet. Otherwise all that is known is what crossed it, so a
/// fault goes by the middle of a burst first and its edges after.
fn points_on(profile: &EdgeProfile) -> Vec<Point<'_>> {
    if !profile.placements.is_empty() {
        return profile
            .placements
            .iter()
            .map(|placement| Point {
                at: Where::Crossing {
                    edge: &profile.edge,
                    direction: placement.direction,
                },
                mark: placement.mark.clone(),
                why: placement.why.clone(),
                doing: placement.doing,
                packets: None,
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
                    at: Where::Crossing {
                        edge: &profile.edge,
                        direction,
                    },
                    mark: k.to_string(),
                    why: format!("{k} reads into what this edge carried"),
                    // Nothing read this edge, so all a moment on it can offer
                    // is a place to hold the fleet part way through something.
                    doing: Doing::Holding,
                    packets: Some(burst.packets),
                });
            }
        }
    }
    // Where a budget runs out part way through, the busiest bursts are the ones
    // worth keeping.
    for round in &mut rounds {
        round.sort_by_key(|point| std::cmp::Reverse(point.packets));
    }
    rounds.concat()
}

/// Where inside its services a fault can go, one point per moment reported.
///
/// Empty unless a service is instrumented, a fleet that reports nothing is
/// still scheduled from its edges.
fn points_inside(reached: &[Reached]) -> Vec<Point<'_>> {
    let mut seen: BTreeSet<(&str, String)> = BTreeSet::new();
    let mut offered: Vec<&Reached> = reached
        .iter()
        .filter(|reached| seen.insert((reached.service.as_str(), reached.mark())))
        .collect();
    // Endings before starts, because an ending is work finished with the next
    // not begun, which is the gap a fault wants. The first start is only the
    // request arriving, which the edges already offer.
    offered.sort_by_key(|reached| reached.boundary.side == Side::Started);
    // One of every kind of moment the fleet offers before any second of one.
    in_turn(offered.into_iter(), |reached| {
        (
            reached.service.as_str(),
            reached.boundary.span.as_str(),
            reached.boundary.side,
        )
    })
    .into_iter()
    .map(|reached| {
        let mark = reached.mark();
        Point {
            at: Where::Inside {
                service: &reached.service,
            },
            why: format!(
                "{mark} holds {} part way through its own work",
                reached.service
            ),
            mark,

            doing: Doing::Holding,
            packets: None,
        }
    })
    .collect()
}

/// `items` reordered so one of every group comes before any group's second,
/// keeping the order within each group and the order the groups first appear.
fn in_turn<T, K: PartialEq>(items: impl Iterator<Item = T>, group: impl Fn(&T) -> K) -> Vec<T> {
    let mut groups: Vec<(K, std::collections::VecDeque<T>)> = Vec::new();
    for item in items {
        let key = group(&item);
        match groups.iter_mut().find(|(seen, _)| *seen == key) {
            Some((_, members)) => members.push_back(item),
            None => groups.push((key, [item].into())),
        }
    }
    let mut taken = Vec::with_capacity(groups.iter().map(|(_, members)| members.len()).sum());
    while groups.iter().any(|(_, members)| !members.is_empty()) {
        taken.extend(
            groups
                .iter_mut()
                .filter_map(|(_, members)| members.pop_front()),
        );
    }
    taken
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
        budget: Option<Budget>,
    ) -> Self {
        // One point is faulted every way the fleet can be broken there, and
        // covering those comes before covering more points, so they multiply
        // what a point costs.
        let ways: Vec<Drive> = learned
            .primitives
            .iter()
            .filter_map(|by| Drive::try_from(*by).ok())
            .collect();

        // Interleaved, so an edge's second point comes after every edge's
        // first.
        let on_edge: Vec<Vec<Point<'_>>> = learned.profiles.iter().map(points_on).collect();
        let inside = points_inside(&learned.inside);
        let found: usize = on_edge.iter().map(Vec::len).sum::<usize>() + inside.len();
        let mut by_edge: Vec<Point<'_>> = Vec::with_capacity(found);
        for round in 0..on_edge.iter().map(Vec::len).max().unwrap_or(0) {
            by_edge.extend(on_edge.iter().filter_map(|edge| edge.get(round)).cloned());
        }
        by_edge.extend(inside);
        // Then by turn, so a way of breaking the fleet with few moments is
        // covered before one with many, and the bursts take their share rather
        // than all of it.
        let mut points = in_turn(by_edge.into_iter(), Point::bucket);

        // What one point costs, which is not the same everywhere: what an edge
        // has to break depends on the edge, and a moment can only be broken the
        // way it says.
        let cost = |point: &Point<'_>| -> usize {
            ways_at(&ways, point)
                .into_iter()
                .map(|by| targets(by, point.at).len())
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
            for by in ways_at(&ways, point) {
                for taking in targets(by, point.at) {
                    let anchor = point.at.anchoring(point.mark.clone(), point.why.clone());
                    let fault = Fault::at(anchor, taking);
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
    pub fn coverage(&self) -> &Coverage {
        &self.coverage
    }
}

/// The ways this campaign can break the fleet at `point`.
///
/// A moment a plugin named says what breaking the fleet there is, and there is
/// one way to do that. A burst is a count of packets, so all it can offer is a
/// place to hold the fleet while something is taken away from outside it:
/// changing what crosses needs something that can read what crossed.
fn ways_at(ways: &[Drive], point: &Point<'_>) -> Vec<Drive> {
    ways.iter()
        .filter(|by| match point.doing {
            Doing::Holding => matches!(by, Drive::Kill | Drive::Cut),
            Doing::Rewriting(primitive) => Drive::try_from(primitive).is_ok_and(|own| own == **by),
        })
        .copied()
        .collect()
}

/// What breaking the fleet at `at` by `losing` takes from the fleet.
/// For an edge, the services at its ends or the link between them.
/// For a moment inside a service, that service, held where it reported reaching.
fn targets(losing: Drive, at: Where<'_>) -> Vec<By> {
    let edge = match at {
        Where::Crossing { edge, .. } => edge,
        Where::Inside { service } => {
            return match losing {
                Drive::Kill => vec![By::Kill(service.to_owned())],
                // Everything else names an edge, and this moment is not on one.
                Drive::Cut | Drive::Repeat | Drive::Reorder | Drive::Drop => Vec::new(),
            };
        }
    };
    match losing {
        Drive::Kill => edge
            .client
            .iter()
            .chain(std::iter::once(&edge.upstream))
            .map(|service| By::Kill(service.clone()))
            .collect(),
        Drive::Cut => edge
            .within_fleet()
            .then(|| By::Cut(edge.clone()))
            .into_iter()
            .collect(),
        Drive::Repeat => vec![By::Repeat(edge.clone())],
        Drive::Reorder => vec![By::Reorder(edge.clone())],
        Drive::Drop => vec![By::Drop(edge.clone())],
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

    use crucible_core::fault::Primitive;
    use crucible_core::fault::Reaches;
    use crucible_protocol::{Boundary, EdgeProfile};

    use super::*;
    use crate::scheduler::fixture;

    /// A burst of `packets` packets, the `nth` on its edge.
    /// A moment `service` reported reaching, as a learn run collects them.
    fn reached(service: &str, span: &str, side: Side, nth: u32) -> Reached {
        Reached {
            service: service.to_owned(),
            boundary: Boundary {
                span: span.to_owned(),
                side,
                nth,
            },
        }
    }

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

    /// An edge a plugin read, which named `moments` on it, each a place to hold
    /// the fleet.
    fn named(moments: Vec<&str>) -> EdgeProfile {
        marked(
            moments
                .into_iter()
                .map(|mark| (mark, Doing::Holding))
                .collect(),
        )
    }

    /// An edge a plugin read, which named `moments` on it and how each breaks
    /// the fleet.
    fn marked(moments: Vec<(&str, Doing)>) -> EdgeProfile {
        EdgeProfile {
            placements: moments
                .into_iter()
                .map(|(mark, doing)| crucible_protocol::Placement {
                    direction: Direction::ClientToUpstream,
                    mark: mark.to_owned(),
                    why: format!("what {mark} catches"),
                    doing,
                })
                .collect(),
            ..from(Some("api"), "broker", vec![], vec![])
        }
    }

    /// An unbounded campaign against a fleet that can only be killed.
    fn scheduler(profiles: &[EdgeProfile]) -> BurstScheduler {
        driven(profiles, &[Primitive::Kill], None)
    }

    /// A budget that affords exactly `schedules` runs.
    fn affording(schedules: u32) -> Budget {
        Budget {
            left: Duration::from_secs(1) * schedules,
            cost: Duration::from_secs(1),
            concurrency: 1,
        }
    }

    /// A campaign against a fleet that can be broken in `ways`, fitted to a
    /// budget affording `capacity` schedules.
    fn driven(
        profiles: &[EdgeProfile],
        ways: &[Primitive],
        capacity: Option<u32>,
    ) -> BurstScheduler {
        let learned = Learned {
            profiles: profiles.to_vec(),
            trajectory: crucible_core::verdict::Trajectory::default(),
            inside: Vec::new(),
            primitives: ways.iter().copied().collect(),
        };
        BurstScheduler::new(
            &fixture::fleet(),
            &fixture::scenario(),
            &learned,
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

    /// The kill takes the service that reported the moment: it is held where it
    /// said it got to, and dies there.
    #[test]
    fn a_service_that_reports_its_moments_is_faulted_at_them() {
        let learned = Learned {
            profiles: Vec::new(),
            trajectory: crucible_core::verdict::Trajectory::default(),
            inside: vec![
                reached("api", "write", Side::Started, 1),
                reached("api", "publish", Side::Started, 1),
            ],
            primitives: BTreeSet::from([Primitive::Kill, Primitive::Cut, Primitive::Reorder]),
        };
        let mut s = BurstScheduler::new(&fixture::fleet(), &fixture::scenario(), &learned, None);

        let placed: Vec<_> = std::iter::from_fn(|| s.next())
            .map(|schedule| {
                let fault = schedule.fault.expect("a moment is faulted");
                let anchor = fault.anchor().expect("an inside moment anchors").clone();
                (anchor.reaches, anchor.mark, fault.taking().target().clone())
            })
            .collect();

        assert_eq!(
            placed,
            vec![
                (
                    Reaches::Inside {
                        service: "api".into()
                    },
                    "write:1:start".into(),
                    "api".to_owned()
                ),
                (
                    Reaches::Inside {
                        service: "api".into()
                    },
                    "publish:1:start".into(),
                    "api".to_owned()
                ),
            ],
            "the moment is not on an edge, so only a kill reaches it"
        );
    }

    #[test]
    fn every_kind_of_moment_comes_before_any_second_of_one() {
        let reported = [
            reached("api", "write", Side::Started, 1),
            reached("api", "write", Side::Ended, 1),
            reached("api", "publish", Side::Started, 1),
            reached("api", "publish", Side::Ended, 1),
            reached("api", "write", Side::Started, 2),
            reached("api", "publish", Side::Started, 2),
        ];
        let marks: Vec<_> = points_inside(&reported)
            .into_iter()
            .map(|point| point.mark)
            .collect();

        let second = marks
            .iter()
            .position(|mark| mark == "write:2:start")
            .expect("the second write is offered");
        let first_publish = marks
            .iter()
            .position(|mark| mark == "publish:1:start")
            .expect("the first publish is offered");
        assert!(
            first_publish < second,
            "the second write came before the first publish: {marks:?}"
        );
    }

    #[test]
    fn a_span_ending_is_spent_before_a_span_starting() {
        let reported = [
            reached("api", "record", Side::Started, 1),
            reached("api", "record", Side::Ended, 1),
            reached("api", "queue", Side::Started, 1),
            reached("api", "queue", Side::Ended, 1),
        ];

        let marks: Vec<_> = points_inside(&reported)
            .into_iter()
            .map(|point| point.mark)
            .collect();

        assert_eq!(marks[0], "record:1:end", "spent first: {marks:?}");
        assert!(
            marks.iter().position(|m| m == "queue:1:end")
                < marks.iter().position(|m| m == "record:1:start"),
            "a starting boundary outranked an ending one: {marks:?}"
        );
    }

    #[test]
    fn a_short_budget_reaches_the_moments_inside_services() {
        let learned = Learned {
            profiles: vec![profile("db", vec![burst(0, 4), burst(1, 4)], vec![])],
            trajectory: crucible_core::verdict::Trajectory::default(),
            inside: vec![reached("api", "publish", Side::Started, 1)],
            primitives: BTreeSet::from([Primitive::Kill]),
        };
        let mut s = BurstScheduler::new(
            &fixture::fleet(),
            &fixture::scenario(),
            &learned,
            Some(affording(3)),
        );

        let marks: Vec<_> = std::iter::from_fn(|| s.next())
            .map(|schedule| fault(&schedule).mark.clone())
            .collect();
        assert!(
            marks.contains(&"publish:1:start".to_owned()),
            "the budget was spent on edges alone: {marks:?}"
        );
    }

    #[test]
    fn a_fleet_that_reports_nothing_is_still_scheduled_from_its_edges() {
        let learned = Learned {
            profiles: vec![profile("db", vec![burst(0, 4)], vec![])],
            trajectory: crucible_core::verdict::Trajectory::default(),
            inside: Vec::new(),
            primitives: BTreeSet::from([Primitive::Kill]),
        };
        let s = BurstScheduler::new(&fixture::fleet(), &fixture::scenario(), &learned, None);
        assert!(s.total() > 0);
    }

    /// A fleet whose loaded plugins cannot break anything cannot schedule any
    /// faults, so the campaign is a fault-free run only.
    #[test]
    fn nothing_testable_yields_nothing() {
        let learned = Learned {
            profiles: vec![profile("db", vec![burst(0, 4)], vec![])],
            trajectory: crucible_core::verdict::Trajectory::default(),
            inside: Vec::new(),
            primitives: BTreeSet::new(),
        };
        let mut s = BurstScheduler::new(&fixture::fleet(), &fixture::scenario(), &learned, None);
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
            &[Primitive::Kill, Primitive::Cut],
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
                .any(|s| fault(s).reaches.direction() == Some(Direction::ClientToUpstream))
        );
        assert!(
            all.iter()
                .any(|s| fault(s).reaches.direction() == Some(Direction::UpstreamToClient))
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
        .filter_map(|s| fault(&s).reaches.edge().map(|e| e.upstream.clone()))
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
            &[Primitive::Kill],
            None,
        ));
        assert!(taken.contains(&"inventory".to_string()));
        assert!(taken.contains(&"broker".to_string()));
    }

    #[test]
    fn an_edge_from_outside_the_fleet_offers_only_its_upstream() {
        let taken = taken(driven(
            &[profile("api", vec![burst(1, 4)], vec![])],
            &[Primitive::Kill],
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
            &[Primitive::Cut],
            None,
        );
        assert_eq!(s.total(), 0);
    }

    /// A way of breaking the fleet with fewer moments than another must not sit
    /// behind every one of them, or a budget spends itself before reaching it.
    #[test]
    fn a_budget_reaches_every_way_before_any_of_them_twice() {
        let repeating = Doing::Rewriting(Primitive::Redeliver);
        let profiles = [marked(vec![
            ("publish:1", Doing::Holding),
            ("publish:2", Doing::Holding),
            ("redeliver:1", repeating),
            ("redeliver:2", repeating),
            ("redeliver:3", repeating),
        ])];
        let mut scheduler = driven(&profiles, &[Primitive::Kill, Primitive::Redeliver], None);

        let mut spent = Vec::new();
        while let Some(schedule) = scheduler.next() {
            spent.push(fault(&schedule).mark.clone());
        }
        spent.dedup();
        assert_eq!(
            spent,
            [
                "publish:1",
                "redeliver:1",
                "publish:2",
                "redeliver:2",
                "redeliver:3"
            ]
        );
    }

    /// Bursts on a busy edge must not spend the budget before the moments a
    /// plugin named on a quiet one are reached.
    #[test]
    fn a_busy_edge_does_not_spend_the_budget_before_a_named_moment() {
        let profiles = [
            profile("db", vec![burst(1, 900), burst(2, 800)], vec![]),
            named(vec!["publish:1"]),
        ];
        let mut scheduler = driven(&profiles, &[Primitive::Kill], None);

        let mut spent = Vec::new();
        while let Some(schedule) = scheduler.next() {
            spent.push(fault(&schedule).mark.clone());
        }
        spent.dedup();
        let named = spent.iter().position(|mark| mark == "publish:1");
        assert_eq!(named, Some(1), "{spent:?}");
    }

    /// A moment says how it can be broken, and only that way is scheduled.
    /// This one is a confirm the plugin can drop, so the campaign spends it on
    /// dropping even though it could also kill and cut: a kill there would
    /// freeze the fleet and take a service away, which is not what the moment
    /// offered.
    #[test]
    fn a_moment_is_broken_the_way_it_says_it_is() {
        let s = driven(
            &[marked(vec![(
                "confirm:1",
                Doing::Rewriting(Primitive::Drop),
            )])],
            &[Primitive::Kill, Primitive::Cut, Primitive::Drop],
            None,
        );
        assert_eq!(s.total(), 1, "one way, not three");
        let mut s = s;
        let only = s.next().expect("the moment is placeable");
        assert_eq!(
            only.fault.as_ref().map(Fault::primitive),
            Some(Primitive::Drop)
        );
    }

    /// A burst is a count of packets. Nothing read the edge, so nothing there
    /// can change what crosses it, and a way that needs to must not be costed.
    #[test]
    fn a_burst_is_only_a_place_to_hold_the_fleet() {
        let s = driven(
            &[profile("db", vec![burst(1, 4)], vec![])],
            &[Primitive::Kill, Primitive::Drop],
            None,
        );
        let ways: Vec<Primitive> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .filter_map(|schedule| schedule.fault.as_ref().map(Fault::primitive))
        .collect();
        assert!(ways.iter().all(|by| *by == Primitive::Kill), "{ways:?}");
    }

    /// A moment broken one way must not queue behind the moments of another
    /// way, or a busy edge spends the budget before the fleet meets it.
    #[test]
    fn a_way_of_breaking_the_fleet_takes_its_turn_against_the_others() {
        let profiles = [marked(vec![
            ("publish:1:after", Doing::Holding),
            ("publish:2:after", Doing::Holding),
            ("confirm:1", Doing::Rewriting(Primitive::Drop)),
        ])];
        let mut scheduler = driven(&profiles, &[Primitive::Kill, Primitive::Drop], None);
        let mut spent = Vec::new();
        while let Some(schedule) = scheduler.next() {
            spent.push(fault(&schedule).mark.clone());
        }
        spent.dedup();
        assert_eq!(spent, ["publish:1:after", "confirm:1", "publish:2:after"]);
    }

    #[test]
    fn a_cut_takes_the_edge_itself() {
        let taken = taken(driven(
            &[from(Some("api"), "db", vec![burst(1, 4)], vec![])],
            &[Primitive::Cut],
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
            &[Primitive::Kill],
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
            &[Primitive::Kill],
            Some(4),
        );
        assert_eq!(s.coverage().taken, 4, "both points each way");
        let placed: Vec<(Option<Direction>, String)> = std::iter::from_fn({
            let mut s = s;
            move || s.next()
        })
        .map(|s| (fault(&s).reaches.direction(), fault(&s).mark.clone()))
        .collect();
        let burst = burst(1, 4);
        // Receiving, so the start: it has taken delivery and not yet acted.
        assert!(placed.contains(&(Some(Direction::ClientToUpstream), burst.start.to_string())));
        // Sending, so the end: it has emitted everything and owes a write.
        assert!(placed.contains(&(Some(Direction::UpstreamToClient), burst.end.to_string())));
    }

    #[test]
    fn a_budget_below_one_point_per_burst_keeps_the_busiest() {
        let s = driven(
            &[profile("db", vec![burst(0, 1), burst(1, 9)], vec![])],
            &[Primitive::Kill],
            Some(1),
        );
        assert_eq!(s.coverage().taken, 1);
        let mut s = s;
        let only = s.next().expect("the budget affords one");
        assert_eq!(fault(&only).mark, burst(1, 9).mid.to_string());
    }

    /// A degraded run is scheduled elsewhere and paid for off the top, so
    /// nothing here reserves anything for it.
    #[test]
    fn recovery_takes_no_share_of_what_the_bursts_are_fitted_to() {
        let s = driven(
            &[profile("db", vec![burst(1, 4), burst(2, 4)], vec![])],
            &[Primitive::Kill],
            Some(6),
        );
        assert_eq!(s.coverage().taken, 6);
        assert_eq!(s.total(), 6);
    }

    #[test]
    fn covering_both_ways_of_breaking_the_fleet_comes_before_more_points() {
        let s = driven(
            &[from(Some("api"), "db", vec![burst(0, 4)], vec![])],
            &[Primitive::Kill, Primitive::Cut],
            Some(3),
        );
        assert_eq!(s.coverage().taken, 1, "one point, broken every way");
        assert_eq!(s.total(), 3);
    }
}
