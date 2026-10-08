//! Reconstruct `Session` records from sidecar proxy log lines.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::{IpAddr, SocketAddr},
};

use crucible_protocol::{
    Burst, ConnEvent, ConnEventKind, ConnId, Direction, Edge, EdgeProfile, Found, Placement,
    Reached, Session, WriteRecord,
};

struct Pending {
    opened_ns: u128,
    peer: String,
    writes: Vec<WriteRecord>,
    placements: Vec<Found>,
}

/// Sessions observed across a Learn run.
#[derive(Default)]
pub struct Sessions {
    opened: HashMap<(String, ConnId), Pending>,
    finished: Vec<Session>,
    /// Moments inside services.
    inside: Vec<Reached>,
}

impl Sessions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The moments services reported from inside themselves.
    #[must_use]
    pub fn inside(&self) -> &[Reached] {
        &self.inside
    }

    pub fn accept_event(&mut self, service: &str, event: ConnEvent) {
        let ConnEvent { id, ts_ns, kind } = event;
        match kind {
            ConnEventKind::Opened { peer } => {
                self.opened.insert(
                    (service.to_string(), id),
                    Pending {
                        opened_ns: ts_ns,
                        peer: peer.to_string(),
                        writes: Vec::new(),
                        placements: Vec::new(),
                    },
                );
            }
            ConnEventKind::Wrote { direction, bytes } => {
                if let Some(pending) = self.opened.get_mut(&(service.to_string(), id)) {
                    pending.writes.push(WriteRecord {
                        ts_ns,
                        direction,
                        bytes,
                    });
                }
            }
            ConnEventKind::Closed { .. } => {
                if let Some(pending) = self.opened.remove(&(service.to_string(), id)) {
                    self.finished.push(Session {
                        service: service.to_string(),
                        conn_id: id,
                        peer: pending.peer,
                        opened_ns: pending.opened_ns,
                        closed_ns: Some(ts_ns),
                        writes: pending.writes,
                        placements: pending.placements,
                        unreadable: None,
                    });
                }
            }
            ConnEventKind::Failed { .. } => {
                self.opened.remove(&(service.to_string(), id));
            }
            ConnEventKind::Reached { boundary } => self.inside.push(Reached {
                service: service.to_string(),
                boundary,
                at_ns: ts_ns,
                edges: Vec::new(),
            }),
            ConnEventKind::Placeable { placement } => {
                if let Some(pending) = self.opened.get_mut(&(service.to_string(), id)) {
                    pending.placements.push(Found { ts_ns, placement });
                }
            }
            // What the fault did, not part of a session's byte accounting;
            // whoever is waiting on the fault consumes these elsewhere.
            ConnEventKind::Froze { .. } | ConnEventKind::Did { .. } => {}
        }
    }
}

/// Consecutive packets more than this far apart start a new burst.
const BURST_GAP_NS: u128 = 20_000_000; // 20 ms

/// The most bursts kept per edge and direction. A chatty edge repeats the same
/// work, so its busiest few are representative and the rest tell nothing new.
const MAX_BURSTS: usize = 3;

/// The `MAX_BURSTS` heaviest bursts.
fn heaviest(mut bursts: Vec<Burst>) -> Vec<Burst> {
    bursts.sort_unstable_by_key(|burst| std::cmp::Reverse(burst.packets));
    bursts.truncate(MAX_BURSTS);
    bursts
}

/// The bursts busier than the edge's idle floor, the largest burst it carried
/// before the scenario drove anything. Below that an edge is idling on
/// keepalives and health checks, which is not interesting to break.
fn above_floor(bursts: Vec<Burst>, floor: u32) -> Vec<Burst> {
    bursts
        .into_iter()
        .filter(|burst| burst.packets > floor)
        .collect()
}

/// When each phase of a learn run began.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// When the healthy fleet began idling before the scenario.
    pub rest_start_ns: u128,
    /// When the scenario began driving the fleet.
    pub scenario_start_ns: u128,
    /// When the fleet fell quiet, after which only the checks read it back.
    pub steps_settled_ns: u128,
}

/// Derive per-edge bursts from a session catalogue, split by direction and made
/// relative to when the scenario began.
///
/// `addresses` names the service behind a peer. One it does not name came from
/// outside the fleet.
///
/// Only the traffic the scenario caused counts. What crossed before it began is
/// the fleet resting, and what crossed after it settled is the checks reading it
/// back. A write inside a span window of its edge's client belongs to that span,
/// a burst no busier than the edge's idle floor is left out and a plugin-read
/// edge keeps only its placements.
#[must_use]
pub fn edge_profiles_from_sessions<S: std::hash::BuildHasher>(
    sessions: &[Session],
    timing: Timing,
    addresses: &HashMap<IpAddr, String, S>,
    covered: &[SpanWindow],
) -> Vec<EdgeProfile> {
    let Timing {
        rest_start_ns,
        scenario_start_ns,
        steps_settled_ns,
    } = timing;
    let in_span = |edge: &Edge, at: u128| {
        edge.client.as_deref().is_some_and(|client| {
            covered
                .iter()
                .any(|window| window.host == client && window.covers(at))
        })
    };

    // Per edge, the packets each direction carried while the fleet idled before
    // the scenario.
    let mut idle: BTreeMap<Edge, (Vec<Packet>, Vec<Packet>)> = BTreeMap::new();
    let mut live: BTreeMap<Edge, (Vec<Packet>, Vec<Packet>)> = BTreeMap::new();
    // Where the plugins reading each edge said a fault could go.
    let mut placeable: BTreeMap<Edge, Vec<Placement>> = BTreeMap::new();
    for session in sessions {
        let edge = Edge {
            client: client_of(session, addresses),
            upstream: session.service.clone(),
        };
        // Only what the scenario caused.
        // A moment that passed before the first step is one no fault can be
        // placed at.
        let drove: Vec<Placement> = session
            .placements
            .iter()
            .filter(|found| (scenario_start_ns..steps_settled_ns).contains(&found.ts_ns))
            .map(|found| found.placement.clone())
            .collect();
        if !drove.is_empty() {
            placeable.entry(edge.clone()).or_default().extend(drove);
        }
        for write in &session.writes {
            let scenario = scenario_start_ns..steps_settled_ns;
            let (into, at) = if (rest_start_ns..scenario_start_ns).contains(&write.ts_ns) {
                (&mut idle, write.ts_ns)
            } else if !scenario.contains(&write.ts_ns) || in_span(&edge, write.ts_ns) {
                continue;
            } else {
                (&mut live, write.ts_ns - scenario_start_ns)
            };
            let entry = into.entry(edge.clone()).or_default();
            match write.direction {
                Direction::ClientToUpstream => entry.0.push(Packet { at }),
                Direction::UpstreamToClient => entry.1.push(Packet { at }),
            }
        }
    }

    // The busiest a direction idled at, so a scenario burst no busier than that
    // is the edge idling rather than working.
    let floor = |packets: &[Packet]| bursts(packets).iter().map(|b| b.packets).max().unwrap_or(0);

    // Every edge the scenario drove, and every edge a plugin read, so an edge
    // whose whole scenario the plugin or a span accounted for still reports its
    // placements rather than dropping out.
    let edges: BTreeSet<Edge> = live.keys().chain(placeable.keys()).cloned().collect();
    edges
        .into_iter()
        .map(|edge| {
            let placements = placeable.get(&edge).cloned().unwrap_or_default();
            // A plugin reading the edge drives more accurate faults than a
            // packet count, so its bursts are then unnecessary.
            let (client_to_upstream, upstream_to_client) = if placements.is_empty() {
                let (c2u, u2c) = live.get(&edge).cloned().unwrap_or_default();
                let (idle_c2u, idle_u2c) = idle.get(&edge).cloned().unwrap_or_default();
                (
                    heaviest(above_floor(bursts(&c2u), floor(&idle_c2u))),
                    heaviest(above_floor(bursts(&u2c), floor(&idle_u2c))),
                )
            } else {
                (Vec::new(), Vec::new())
            };
            EdgeProfile {
                client_to_upstream,
                upstream_to_client,
                placements,
                edge,
            }
        })
        .collect()
}

/// A window a span held its host open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanWindow {
    /// The service the span ran in.
    pub host: String,
    pub start_ns: u128,
    pub end_ns: u128,
}

impl SpanWindow {
    /// How long the window is open.
    #[must_use]
    fn width(&self) -> u128 {
        self.end_ns - self.start_ns
    }

    /// Whether `at_ns` falls within the window.
    #[must_use]
    fn covers(&self, at_ns: u128) -> bool {
        (self.start_ns..=self.end_ns).contains(&at_ns)
    }
}

/// Record on each span the edges its host dials during it, and return the
/// windows the spans held their hosts open.
///
/// A write on an edge is the doing of that edge's client, so it belongs to the
/// tightest span of that client whose window holds it: an inner `write` span
/// over the outer `handle` that contains it. A plugin-read edge is left to its
/// own precise faults, and a write no span holds is left to its bursts.
pub fn cover_edges<S: std::hash::BuildHasher>(
    sessions: &[Session],
    inside: &mut [Reached],
    addresses: &HashMap<IpAddr, String, S>,
) -> Vec<SpanWindow> {
    // A span instance: the service, the span's name, and which time it was
    // reached, so its two ends line up under one key.
    type Span = (String, String, u32);
    fn key(reached: &Reached) -> Span {
        (
            reached.service.clone(),
            reached.boundary.span.clone(),
            reached.boundary.nth,
        )
    }

    // The window each span instance held open, from its two ends.
    let mut ends: BTreeMap<Span, (Option<u128>, Option<u128>)> = BTreeMap::new();
    for reached in inside.iter() {
        let slot = ends.entry(key(reached)).or_default();
        match reached.boundary.side {
            crucible_protocol::Side::Started => slot.0 = Some(reached.at_ns),
            crucible_protocol::Side::Ended => slot.1 = Some(reached.at_ns),
        }
    }
    // Only a span with both ends is a window; one still open at teardown is not.
    let windows: BTreeMap<Span, SpanWindow> = ends
        .into_iter()
        .filter_map(|(key, (start, end))| {
            Some((
                key.clone(),
                SpanWindow {
                    host: key.0,
                    start_ns: start?,
                    end_ns: end?,
                },
            ))
        })
        .collect();

    // Edges a plugin read, whose own faults are more precise than a span's, so
    // no span names them.
    let by_a_plugin: BTreeSet<Edge> = sessions
        .iter()
        .filter(|session| !session.placements.is_empty())
        .map(|session| Edge {
            client: client_of(session, addresses),
            upstream: session.service.clone(),
        })
        .collect();

    // Each span, and the edges it is the tightest holder of a write on.
    let mut named: BTreeMap<&Span, BTreeSet<Edge>> = BTreeMap::new();
    for session in sessions {
        let edge = Edge {
            client: client_of(session, addresses),
            upstream: session.service.clone(),
        };
        let Some(host) = edge.client.clone() else {
            continue;
        };
        if by_a_plugin.contains(&edge) {
            continue;
        }
        for write in &session.writes {
            let tightest = windows
                .iter()
                .filter(|(key, window)| key.0 == host && window.covers(write.ts_ns))
                .min_by_key(|(_, window)| window.width());
            if let Some((key, _)) = tightest {
                named.entry(key).or_default().insert(edge.clone());
            }
        }
    }

    for reached in inside.iter_mut() {
        if let Some(edges) = named.get(&key(reached)) {
            reached.edges = edges.iter().cloned().collect();
        }
    }

    windows.into_values().collect()
}

/// The service that dialled, or `None` for a caller from outside the fleet.
fn client_of<S: std::hash::BuildHasher>(
    session: &Session,
    addresses: &HashMap<IpAddr, String, S>,
) -> Option<String> {
    // The peer is `address:port`; the port is the caller's ephemeral one, so
    // only the address identifies it.
    let address: SocketAddr = session.peer.parse().ok()?;
    addresses.get(&address.ip()).cloned()
}

/// One packet the proxy forwarded, at its time relative to scenario start. The
/// order of these is what an anchor's index counts.
#[derive(Clone, Copy, Debug)]
struct Packet {
    at: u128,
}

/// Cluster `packets` into bursts by inter-packet gap, each given as the three
/// points a fault can be placed against. A count `K` means "freeze once `K`
/// packets have crossed", so `start` is `first - 1` and `end` is `last`.
fn bursts(packets: &[Packet]) -> Vec<Burst> {
    let mut packets = packets.to_vec();
    packets.sort_unstable_by_key(|packet| packet.at);
    let packets = &packets[..];
    let mut bursts = Vec::new();
    let mut start = 0usize; // 0-based index of the current burst's first packet
    for j in 0..packets.len() {
        let ends_burst = j + 1 == packets.len() || packets[j + 1].at - packets[j].at > BURST_GAP_NS;
        if ends_burst {
            // 1-based packet counts; a single learn run should never approach u32.
            let first = u32::try_from(start + 1).expect("packet count fits in u32");
            let last = u32::try_from(j + 1).expect("packet count fits in u32");
            bursts.push(Burst {
                start: first - 1,
                mid: u32::midpoint(first, last),
                end: last,
                packets: last - first + 1,
            });
            start = j + 1;
        }
    }
    bursts
}

impl Extend<(String, ConnEvent)> for Sessions {
    fn extend<I: IntoIterator<Item = (String, ConnEvent)>>(&mut self, iter: I) {
        for (service, event) in iter {
            self.accept_event(&service, event);
        }
    }
}

impl FromIterator<(String, ConnEvent)> for Sessions {
    fn from_iter<I: IntoIterator<Item = (String, ConnEvent)>>(iter: I) -> Self {
        let mut sessions = Self::new();
        sessions.extend(iter);
        sessions
    }
}

impl IntoIterator for Sessions {
    type Item = Session;
    type IntoIter = std::vec::IntoIter<Session>;

    fn into_iter(mut self) -> Self::IntoIter {
        for ((service, conn_id), pending) in self.opened.drain() {
            self.finished.push(Session {
                service,
                conn_id,
                peer: pending.peer,
                opened_ns: pending.opened_ns,
                closed_ns: None,
                writes: pending.writes,
                placements: pending.placements,
                unreadable: None,
            });
        }
        self.finished
            .sort_by_key(|s| (s.opened_ns, s.service.clone(), s.conn_id));
        self.finished.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::Direction;
    use proptest::prelude::*;

    use super::*;
    use crate::ipc::{WorkerToRunner, codec::MAX_FRAME_SIZE};

    fn a_write() -> impl Strategy<Value = WriteRecord> {
        (any::<bool>(), any::<u128>()).prop_map(|(c2u, ts_ns)| WriteRecord {
            ts_ns,
            direction: if c2u {
                Direction::ClientToUpstream
            } else {
                Direction::UpstreamToClient
            },
            bytes: 1,
        })
    }

    fn a_session() -> impl Strategy<Value = Session> {
        ("[a-z_]{1,24}", prop::collection::vec(a_write(), 0..3000)).prop_map(|(service, writes)| {
            Session {
                service,
                conn_id: 0,
                peer: "127.0.0.1:1".to_string(),
                opened_ns: 0,
                closed_ns: None,
                writes,
                placements: Vec::new(),
                unreadable: None,
            }
        })
    }

    fn calling(caller_ip: &str, upstream: &str, writes_at: &[u128]) -> Session {
        Session {
            service: upstream.to_owned(),
            conn_id: 0,
            peer: format!("{caller_ip}:40000"),
            opened_ns: 0,
            closed_ns: None,
            writes: writes_at
                .iter()
                .map(|at| WriteRecord {
                    ts_ns: *at,
                    direction: Direction::ClientToUpstream,
                    bytes: 1,
                })
                .collect(),
            placements: Vec::new(),
            unreadable: None,
        }
    }

    fn span(service: &str, name: &str, nth: u32, start: u128, end: u128) -> Vec<Reached> {
        [
            (crucible_protocol::Side::Started, start),
            (crucible_protocol::Side::Ended, end),
        ]
        .into_iter()
        .map(|(side, at_ns)| Reached {
            service: service.to_owned(),
            boundary: crucible_protocol::Boundary {
                span: name.to_owned(),
                side,
                nth,
            },
            at_ns,
            edges: Vec::new(),
        })
        .collect()
    }

    fn covered_edges(inside: &[Reached], service: &str, span: &str) -> Vec<Edge> {
        inside
            .iter()
            .find(|reached| reached.service == service && reached.boundary.span == span)
            .map(|reached| reached.edges.clone())
            .unwrap_or_default()
    }

    fn to_db() -> Edge {
        Edge {
            client: Some("api".to_owned()),
            upstream: "db".to_owned(),
        }
    }

    /// A run with no resting window and no settling boundary, so every write
    /// after `scenario_start` is the scenario's to burst.
    /// Somewhere a plugin said a fault could go, found at `ts_ns`.
    fn found(ts_ns: u128) -> Found {
        Found {
            ts_ns,
            placement: Placement {
                direction: Direction::ClientToUpstream,
                mark: "ack:1:after".into(),
                why: "an ack the broker has taken".into(),
                doing: crucible_protocol::Doing::Holding,
            },
        }
    }

    fn timing(scenario_start: u128) -> Timing {
        Timing {
            rest_start_ns: scenario_start,
            scenario_start_ns: scenario_start,
            steps_settled_ns: u128::MAX,
        }
    }

    #[test]
    fn a_span_covers_the_edge_its_host_talks_on() {
        let addresses = HashMap::from([("10.0.0.1".parse().unwrap(), "api".to_owned())]);
        let sessions = [calling("10.0.0.1", "db", &[100, 200])];
        let mut inside = span("api", "query", 1, 50, 250);

        let windows = cover_edges(&sessions, &mut inside, &addresses);

        assert_eq!(covered_edges(&inside, "api", "query"), vec![to_db()]);
        assert!(windows.iter().any(|window| window.host == "api"));
    }

    #[test]
    fn a_covered_edge_keeps_no_bursts_in_the_window() {
        let addresses = HashMap::from([("10.0.0.1".parse().unwrap(), "api".to_owned())]);
        let sessions = [calling("10.0.0.1", "db", &[100, 200])];
        let mut inside = span("api", "query", 1, 50, 250);

        let windows = cover_edges(&sessions, &mut inside, &addresses);
        let profiles = edge_profiles_from_sessions(&sessions, timing(0), &addresses, &windows);

        assert!(
            profiles.iter().all(|profile| profile.edge.upstream != "db"),
            "every packet was the span's, so the edge has no bursts to profile"
        );
    }

    #[test]
    fn packets_outside_a_window_keep_their_bursts() {
        let addresses = HashMap::from([("10.0.0.1".parse().unwrap(), "api".to_owned())]);
        // One packet in the span's window, one long after it: only the first is
        // the span's, so the edge keeps a burst for the second.
        let sessions = [calling("10.0.0.1", "db", &[100, 500])];
        let mut inside = span("api", "query", 1, 50, 150);

        let windows = cover_edges(&sessions, &mut inside, &addresses);
        let profiles = edge_profiles_from_sessions(&sessions, timing(0), &addresses, &windows);

        let db = profiles
            .iter()
            .find(|profile| profile.edge.upstream == "db")
            .expect("the edge still bursts what fell outside the window");
        assert_eq!(db.client_to_upstream.len(), 1, "one burst, the late packet");
    }

    #[test]
    fn the_tightest_of_nested_spans_names_the_edge() {
        let addresses = HashMap::from([("10.0.0.1".parse().unwrap(), "api".to_owned())]);
        let sessions = [calling("10.0.0.1", "db", &[100, 200])];
        let mut inside = span("api", "handle", 1, 0, 1000);
        inside.extend(span("api", "query", 1, 90, 210));

        cover_edges(&sessions, &mut inside, &addresses);

        assert_eq!(
            covered_edges(&inside, "api", "query"),
            vec![to_db()],
            "the inner span holds the call most tightly, so it names the edge"
        );
        assert_eq!(
            covered_edges(&inside, "api", "handle"),
            Vec::new(),
            "the outer span leaves the call to the inner one"
        );
    }

    #[test]
    fn a_span_talking_on_two_edges_covers_both() {
        let addresses = HashMap::from([("10.0.0.1".parse().unwrap(), "api".to_owned())]);
        let sessions = [
            calling("10.0.0.1", "db", &[100]),
            calling("10.0.0.1", "broker", &[150]),
        ];
        // One span in `api` is open while it talks on both edges.
        let mut inside = span("api", "handle", 1, 0, 1000);

        cover_edges(&sessions, &mut inside, &addresses);

        assert_eq!(
            covered_edges(&inside, "api", "handle"),
            vec![
                Edge {
                    client: Some("api".to_owned()),
                    upstream: "broker".to_owned(),
                },
                to_db(),
            ],
        );
    }

    #[test]
    fn a_window_with_no_traffic_covers_nothing() {
        let addresses = HashMap::from([("10.0.0.1".parse().unwrap(), "api".to_owned())]);
        let sessions = [calling("10.0.0.1", "db", &[100, 200])];
        // The span opens and closes before any of the edge's packets crossed.
        let mut inside = span("api", "query", 1, 10, 40);

        let windows = cover_edges(&sessions, &mut inside, &addresses);
        let profiles = edge_profiles_from_sessions(&sessions, timing(0), &addresses, &windows);

        assert!(covered_edges(&inside, "api", "query").is_empty());
        let db = profiles
            .iter()
            .find(|profile| profile.edge.upstream == "db")
            .expect("no span was open for it, so it bursts");
        assert_eq!(db.client_to_upstream.len(), 1, "both packets, one burst");
    }

    proptest! {
        /// Nothing trims the catalogue to fit the frame, so the frame has to
        /// be wide enough for a run far busier than a real one.
        #[test]
        fn a_busy_runs_catalogue_fits_the_frame(
            sessions in prop::collection::vec(a_session(), 0..64),
        ) {
            let profiles = edge_profiles_from_sessions(&sessions, timing(0), &HashMap::new(), &[]);
            let catalogue = WorkerToRunner::SessionCatalogue(crate::learned::Learned {
                profiles,
                fault_free: crate::verdict::Baseline::default(),
                inside: Vec::new(),
                primitives: std::collections::BTreeSet::new(),
            });
            let mut buf = vec![0u8; 2_000_000];
            let encoded = postcard::to_slice(&catalogue, &mut buf)
                .expect("catalogue fits the oversized test buffer");
            prop_assert!(
                encoded.len() <= MAX_FRAME_SIZE,
                "catalogue is {} bytes, exceeds frame {}",
                encoded.len(),
                MAX_FRAME_SIZE,
            );
        }
    }

    fn driven(at: &[u128]) -> Vec<Packet> {
        at.iter().map(|&at| Packet { at }).collect()
    }

    #[test]
    fn a_single_packet_collapses_to_one_placeable_point() {
        let [burst] = bursts(&driven(&[1_000])).try_into().expect("one burst");
        assert_eq!((burst.start, burst.mid, burst.end), (0, 1, 1));
        assert_eq!(burst.packets, 1);
    }

    #[test]
    fn contiguous_packets_are_one_burst() {
        let [burst] = bursts(&driven(&[1_000, 2_000, 3_000, 4_000]))
            .try_into()
            .expect("one burst");
        assert_eq!((burst.start, burst.mid, burst.end), (0, 2, 4));
        assert_eq!(burst.packets, 4);
    }

    #[test]
    fn a_gap_splits_bursts() {
        let packets = driven(&[1_000, 2_000, 100_000_000, 101_000_000]);
        let [first, second] = bursts(&packets).try_into().expect("two bursts");
        assert_eq!((first.start, first.mid, first.end), (0, 1, 2));
        assert_eq!((second.start, second.mid, second.end), (2, 3, 4));
    }

    #[test]
    fn packets_reported_out_of_order_burst_the_same_as_in_order() {
        let ordered = bursts(&driven(&[1_000, 2_000, 100_000_000, 101_000_000]));
        let jumbled = bursts(&driven(&[100_000_000, 1_000, 101_000_000, 2_000]));
        assert_eq!(ordered, jumbled);
    }

    #[test]
    fn edge_profiles_split_by_direction_and_skip_pre_scenario_writes() {
        let session = Session {
            service: "db".into(),
            conn_id: 0,
            peer: "127.0.0.1:1".to_string(),
            opened_ns: 0,
            closed_ns: None,
            placements: Vec::new(),
            unreadable: None,
            writes: vec![
                // Before scenario start (50): ignored.
                WriteRecord {
                    ts_ns: 10,
                    direction: Direction::ClientToUpstream,
                    bytes: 1,
                },
                // One client-to-upstream packet after start.
                WriteRecord {
                    ts_ns: 100,
                    direction: Direction::ClientToUpstream,
                    bytes: 1,
                },
                // One upstream-to-client packet after start.
                WriteRecord {
                    ts_ns: 120,
                    direction: Direction::UpstreamToClient,
                    bytes: 1,
                },
            ],
        };
        let addresses = HashMap::from([("127.0.0.1".parse().unwrap(), "api".to_string())]);
        let profiles = edge_profiles_from_sessions(&[session], timing(50), &addresses, &[]);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].edge.client.as_deref(), Some("api"));
        assert_eq!(profiles[0].edge.upstream, "db");
        // One post-start packet each direction, so one burst carrying it.
        assert_eq!(profiles[0].client_to_upstream.len(), 1);
        assert_eq!(profiles[0].client_to_upstream[0].packets, 1);
        assert_eq!(profiles[0].upstream_to_client.len(), 1);
        assert_eq!(profiles[0].upstream_to_client[0].packets, 1);
    }

    /// One packet on `at`, from `peer`, to `service`.
    fn dialled(service: &str, peer: &str, at: u128) -> Session {
        Session {
            service: service.into(),
            conn_id: 0,
            peer: format!("{peer}:1"),
            opened_ns: 0,
            closed_ns: None,
            placements: Vec::new(),
            unreadable: None,
            writes: vec![WriteRecord {
                ts_ns: at,
                direction: Direction::ClientToUpstream,
                bytes: 1,
            }],
        }
    }

    /// Two services dialling one upstream are two edges. Sharing a profile
    /// would make `k` count another client's packets, so a fault would land
    /// where the schedule never named.
    #[test]
    fn one_upstream_dialled_by_two_services_is_two_edges() {
        let addresses = HashMap::from([
            ("10.0.0.1".parse().unwrap(), "api".to_string()),
            ("10.0.0.2".parse().unwrap(), "inventory".to_string()),
        ]);
        let sessions = [
            dialled("db", "10.0.0.1", 100),
            dialled("db", "10.0.0.2", 200),
        ];
        let profiles = edge_profiles_from_sessions(&sessions, timing(0), &addresses, &[]);
        let clients: Vec<Option<&str>> = profiles
            .iter()
            .map(|profile| profile.edge.client.as_deref())
            .collect();
        assert_eq!(clients, [Some("api"), Some("inventory")]);
    }

    /// A protocol-aware plugin drives more accurate faults, so bursts are
    /// unnecessary on an edge it read.
    #[test]
    fn an_edge_a_plugin_read_carries_its_placements_and_no_bursts() {
        let mut session = dialled("broker", "10.0.0.1", 100);
        session.placements = vec![found(100)];
        let profiles = edge_profiles_from_sessions(&[session], timing(0), &HashMap::new(), &[]);

        let profile = profiles.first().expect("the edge was seen");
        assert_eq!(profile.placements.len(), 1);
        assert!(profile.client_to_upstream.is_empty());
        assert!(profile.upstream_to_client.is_empty());
    }

    #[test]
    fn a_moment_the_scenario_did_not_cause_is_left_out() {
        let mut session = dialled("broker", "10.0.0.1", 500);
        session.placements = vec![found(100), found(500)];
        let profiles = edge_profiles_from_sessions(&[session], timing(400), &HashMap::new(), &[]);

        let profile = profiles.first().expect("the edge was seen");
        assert_eq!(profile.placements.len(), 1);
    }

    #[test]
    fn a_peer_the_fleet_does_not_hold_dialled_from_outside_it() {
        let sessions = [dialled("api", "192.168.1.5", 100)];
        let profiles = edge_profiles_from_sessions(&sessions, timing(0), &HashMap::new(), &[]);
        assert_eq!(profiles[0].edge.client, None);
    }

    #[test]
    fn writes_are_folded_into_session() {
        let mut sessions = Sessions::new();
        sessions.accept_event(
            "db",
            ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
        );
        sessions.accept_event(
            "db",
            ConnEvent::wrote_at(0, 150, Direction::ClientToUpstream, 32),
        );
        sessions.accept_event(
            "db",
            ConnEvent::wrote_at(0, 180, Direction::UpstreamToClient, 64),
        );
        sessions.accept_event("db", ConnEvent::closed_at(0, 200, 0, 0));
        let out: Vec<_> = sessions.into_iter().collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].writes.len(), 2);
        assert_eq!(out[0].writes[0].ts_ns, 150);
        assert_eq!(out[0].writes[0].direction, Direction::ClientToUpstream);
        assert_eq!(out[0].writes[0].bytes, 32);
    }

    #[test]
    fn open_without_close_keeps_writes() {
        let out: Vec<_> = Sessions::from_iter([
            (
                "api".into(),
                ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
            ),
            (
                "api".into(),
                ConnEvent::wrote_at(0, 120, Direction::ClientToUpstream, 16),
            ),
        ])
        .into_iter()
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].closed_ns, None);
        assert_eq!(out[0].writes.len(), 1);
    }

    #[test]
    fn failed_drops_pending() {
        let out: Vec<_> = Sessions::from_iter([
            (
                "api".into(),
                ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
            ),
            (
                "api".into(),
                ConnEvent::failed_at(0, 150, "upstream refused"),
            ),
        ])
        .into_iter()
        .collect();
        assert!(out.is_empty());
    }
}
