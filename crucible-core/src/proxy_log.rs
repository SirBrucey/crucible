//! Reconstruct `Session` records from sidecar proxy log lines.

use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
};

use crucible_protocol::{
    Burst, ConnEvent, ConnEventKind, ConnId, Direction, Edge, EdgeProfile, Session, WriteRecord,
};

struct Pending {
    opened_ns: u128,
    peer: String,
    writes: Vec<WriteRecord>,
}

/// Sessions observed across a Learn run.
#[derive(Default)]
pub struct Sessions {
    opened: HashMap<(String, ConnId), Pending>,
    finished: Vec<Session>,
}

impl Sessions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
                    });
                }
            }
            ConnEventKind::Failed { .. } => {
                self.opened.remove(&(service.to_string(), id));
            }
            // A control signal about the fault anchor, not part of a session's
            // byte accounting; the freeze waiter consumes it elsewhere.
            ConnEventKind::Froze { .. } => {}
        }
    }
}

/// Consecutive packets more than this far apart start a new burst.
const BURST_GAP_NS: u128 = 20_000_000; // 20 ms

/// Cap on the total bursts in one catalogue so it always fits the IPC frame. A
/// learn run with more bursts than this keeps the busiest.
const MAX_TOTAL_BURSTS: usize = 100;

/// Derive per-edge bursts from a session catalogue, split by direction and made
/// scenario-relative to `scenario_start_ns` (writes before scenario start are
/// ignored). Each direction's packets are clustered into bursts, then the whole
/// catalogue is trimmed if it would not fit the IPC frame.
///
/// `addresses` names the service behind a peer, so an edge carries both the
/// service that dialled and the one it reached. A peer it does not name came
/// from outside the fleet.
#[must_use]
pub fn edge_profiles_from_sessions<S: std::hash::BuildHasher>(
    sessions: &[Session],
    scenario_start_ns: u128,
    addresses: &HashMap<IpAddr, String, S>,
) -> Vec<EdgeProfile> {
    // Per edge: (client-to-upstream packets, upstream-to-client packets).
    let mut by_edge: BTreeMap<Edge, (Vec<Packet>, Vec<Packet>)> = BTreeMap::new();
    for session in sessions {
        let edge = Edge {
            client: client_of(session, addresses),
            upstream: session.service.clone(),
        };
        for write in &session.writes {
            if write.ts_ns < scenario_start_ns {
                continue;
            }
            let packet = Packet {
                at: write.ts_ns - scenario_start_ns,
            };
            let entry = by_edge.entry(edge.clone()).or_default();
            match write.direction {
                Direction::ClientToUpstream => entry.0.push(packet),
                Direction::UpstreamToClient => entry.1.push(packet),
            }
        }
    }

    let mut profiles: Vec<EdgeProfile> = by_edge
        .into_iter()
        .map(|(edge, (mut c2u, mut u2c))| {
            c2u.sort_unstable_by_key(|packet| packet.at);
            u2c.sort_unstable_by_key(|packet| packet.at);
            EdgeProfile {
                edge,
                client_to_upstream: bursts(&c2u),
                upstream_to_client: bursts(&u2c),
            }
        })
        .collect();
    sample_to_frame(&mut profiles);
    profiles
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

/// Cluster `packets` (sorted timestamps) into bursts by inter-packet gap, each
/// given as the three points a fault can be placed against. A count `K` means
/// "freeze once `K` packets have crossed", so `start` is `first - 1` and `end` is
/// `last`.
fn bursts(packets: &[Packet]) -> Vec<Burst> {
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

/// Trim the catalogue to `MAX_TOTAL_BURSTS` if it holds more, so the serialized
/// catalogue always fits the IPC frame. A no-op for a normal-sized run; only a
/// pathologically busy learn is trimmed, and the drop is logged.
///
/// The busiest bursts are kept, which is the same preference the scheduler
/// applies when it cannot afford them all.
fn sample_to_frame(profiles: &mut [EdgeProfile]) {
    let total: usize = profiles
        .iter()
        .map(|p| p.client_to_upstream.len() + p.upstream_to_client.len())
        .sum();
    if total <= MAX_TOTAL_BURSTS {
        return;
    }
    let mut flat: Vec<(usize, bool, Burst)> = Vec::with_capacity(total);
    for (i, profile) in profiles.iter().enumerate() {
        flat.extend(profile.client_to_upstream.iter().map(|&b| (i, true, b)));
        flat.extend(profile.upstream_to_client.iter().map(|&b| (i, false, b)));
    }
    flat.sort_unstable_by_key(|(_, _, burst)| std::cmp::Reverse(burst.packets));
    flat.truncate(MAX_TOTAL_BURSTS);
    for profile in profiles.iter_mut() {
        profile.client_to_upstream.clear();
        profile.upstream_to_client.clear();
    }
    for (i, is_c2u, burst) in flat {
        if is_c2u {
            profiles[i].client_to_upstream.push(burst);
        } else {
            profiles[i].upstream_to_client.push(burst);
        }
    }
    // Picking the busiest left each edge in size order, and a burst's position on
    // its edge is what makes it the first or the third. `mid` rises across an
    // edge's bursts, so it puts them back in the order they happened.
    for profile in profiles.iter_mut() {
        profile.client_to_upstream.sort_unstable_by_key(|b| b.mid);
        profile.upstream_to_client.sort_unstable_by_key(|b| b.mid);
    }
    tracing::warn!(
        total,
        cap = MAX_TOTAL_BURSTS,
        "learn produced more bursts than fit the catalogue; kept the busiest"
    );
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
        ("[a-z]{1,12}", prop::collection::vec(a_write(), 0..3000)).prop_map(|(service, writes)| {
            Session {
                service,
                conn_id: 0,
                peer: "127.0.0.1:1".to_string(),
                opened_ns: 0,
                closed_ns: None,
                writes,
            }
        })
    }

    proptest! {
        /// The session catalogue must always fit the IPC frame, however much
        /// traffic a learn run observed. Grounds the bound on the wire type.
        #[test]
        fn catalogue_always_fits_the_frame(
            sessions in prop::collection::vec(a_session(), 0..8),
        ) {
            let profiles = edge_profiles_from_sessions(&sessions, 0, &HashMap::new());
            let catalogue = WorkerToRunner::SessionCatalogue(crate::learned::Learned {
                profiles,
                trajectory: Vec::new(),
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
    fn edge_profiles_split_by_direction_and_skip_pre_scenario_writes() {
        let session = Session {
            service: "db".into(),
            conn_id: 0,
            peer: "127.0.0.1:1".to_string(),
            opened_ns: 0,
            closed_ns: None,
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
        let profiles = edge_profiles_from_sessions(&[session], 50, &addresses);
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
        let profiles = edge_profiles_from_sessions(&sessions, 0, &addresses);
        let clients: Vec<Option<&str>> = profiles
            .iter()
            .map(|profile| profile.edge.client.as_deref())
            .collect();
        assert_eq!(clients, [Some("api"), Some("inventory")]);
    }

    #[test]
    fn a_peer_the_fleet_does_not_hold_dialled_from_outside_it() {
        let sessions = [dialled("api", "192.168.1.5", 100)];
        let profiles = edge_profiles_from_sessions(&sessions, 0, &HashMap::new());
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
