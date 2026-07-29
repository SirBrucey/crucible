//! Reconstruct `Session` records from sidecar proxy log lines.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crucible_protocol::{
    ConnEvent, ConnEventKind, ConnId, Direction, ServiceProfile, Session, WriteRecord,
};
use rand::seq::SliceRandom;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("parse conn event: {source} in line: {line}")]
    Parse {
        source: serde_json::Error,
        line: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

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
        }
    }

    pub fn accept_line(&mut self, service: &str, line: &str) -> Result<()> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }
        let event: ConnEvent = serde_json::from_str(line).map_err(|source| Error::Parse {
            source,
            line: line.to_string(),
        })?;
        self.accept_event(service, event);
        Ok(())
    }
}

/// Consecutive packets more than this far apart start a new burst.
const BURST_GAP_NS: u128 = 20_000_000; // 20 ms

/// Cap on the total anchors in one catalogue so it always fits the IPC frame. A
/// learn run with more anchors than this is randomly sampled down (seeded
/// sampling for cross-run reproducibility is a future refinement).
const MAX_TOTAL_ANCHORS: usize = 400;

/// Derive per-service fault anchors from a session catalogue, split by direction
/// and made scenario-relative to `scenario_start_ns` (writes before scenario
/// start are ignored). Each direction's packets are clustered into bursts and
/// reduced to their before/during/after anchor packet-counts, then the whole
/// catalogue is sampled down if it would not fit the IPC frame.
pub fn service_profiles_from_sessions(
    sessions: &[Session],
    scenario_start_ns: u128,
) -> Vec<ServiceProfile> {
    // Per service: (client-to-upstream timestamps, upstream-to-client timestamps).
    let mut by_service: BTreeMap<String, (Vec<u128>, Vec<u128>)> = BTreeMap::new();
    for session in sessions {
        for write in &session.writes {
            if write.ts_ns < scenario_start_ns {
                continue;
            }
            let rel = write.ts_ns - scenario_start_ns;
            let entry = by_service.entry(session.service.clone()).or_default();
            match write.direction {
                Direction::ClientToUpstream => entry.0.push(rel),
                Direction::UpstreamToClient => entry.1.push(rel),
            }
        }
    }

    let mut profiles: Vec<ServiceProfile> = by_service
        .into_iter()
        .map(|(service, (mut c2u, mut u2c))| {
            c2u.sort_unstable();
            u2c.sort_unstable();
            ServiceProfile {
                service,
                client_to_upstream: burst_anchors(&c2u),
                upstream_to_client: burst_anchors(&u2c),
            }
        })
        .collect();
    sample_to_frame(&mut profiles);
    profiles
}

/// Cluster `packets` (sorted timestamps) into bursts by inter-packet gap and
/// return the before / during / after anchor packet-counts, sorted and deduped.
/// A count `K` means "freeze once `K` packets have crossed": `K = first - 1`
/// lands just before a burst, `K = last` just after it.
///
/// `K = 0` (before the very first packet) is dropped. Armed on the proxy's
/// command line it freezes at boot, which holds the fleet's own bring-up traffic
/// (the apps connecting to their dependencies through the proxy), so the fleet
/// never becomes healthy. Doing "kill before the first packet" properly needs
/// the freeze deferred until after bring-up, which boot-time arming cannot do.
fn burst_anchors(packets: &[u128]) -> Vec<u32> {
    let n = packets.len();
    if n == 0 {
        return Vec::new();
    }
    let mut anchors: BTreeSet<u32> = BTreeSet::new();
    let mut start = 0usize; // 0-based index of the current burst's first packet
    for j in 0..n {
        let ends_burst = j + 1 == n || packets[j + 1] - packets[j] > BURST_GAP_NS;
        if ends_burst {
            // 1-based packet counts; a single learn run should never approach u32.
            let first_count = u32::try_from(start + 1).expect("packet count fits in u32");
            let last_count = u32::try_from(j + 1).expect("packet count fits in u32");
            anchors.insert(first_count - 1); // before
            anchors.insert(u32::midpoint(first_count, last_count)); // during
            anchors.insert(last_count); // after
            start = j + 1;
        }
    }
    anchors.into_iter().filter(|&k| k > 0).collect()
}

/// Randomly sample the catalogue's anchors down to `MAX_TOTAL_ANCHORS` if it has
/// more, so the serialized catalogue always fits the IPC frame. A no-op for a
/// normal-sized run; only a pathologically busy learn (very many bursts) is
/// trimmed, and the drop is logged.
fn sample_to_frame(profiles: &mut [ServiceProfile]) {
    let total: usize = profiles
        .iter()
        .map(|p| p.client_to_upstream.len() + p.upstream_to_client.len())
        .sum();
    if total <= MAX_TOTAL_ANCHORS {
        return;
    }
    // Flatten to (profile index, is-client-to-upstream, K), keep a random subset,
    // then rebuild each profile's per-direction vectors.
    let mut flat: Vec<(usize, bool, u32)> = Vec::with_capacity(total);
    for (i, profile) in profiles.iter().enumerate() {
        flat.extend(profile.client_to_upstream.iter().map(|&k| (i, true, k)));
        flat.extend(profile.upstream_to_client.iter().map(|&k| (i, false, k)));
    }
    flat.shuffle(&mut rand::rng());
    flat.truncate(MAX_TOTAL_ANCHORS);
    for profile in profiles.iter_mut() {
        profile.client_to_upstream.clear();
        profile.upstream_to_client.clear();
    }
    for (i, is_c2u, k) in flat {
        if is_c2u {
            profiles[i].client_to_upstream.push(k);
        } else {
            profiles[i].upstream_to_client.push(k);
        }
    }
    for profile in profiles.iter_mut() {
        profile.client_to_upstream.sort_unstable();
        profile.upstream_to_client.sort_unstable();
    }
    tracing::warn!(
        total,
        cap = MAX_TOTAL_ANCHORS,
        "learn produced more anchors than fit the catalogue; sampled down"
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
            let profiles = service_profiles_from_sessions(&sessions, 0);
            let catalogue = WorkerToRunner::SessionCatalogue { services: profiles };
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

    #[test]
    fn single_packet_yields_only_the_after_anchor() {
        // before is K=0 (dropped); during and after both collapse to K=1.
        assert_eq!(burst_anchors(&[1_000]), vec![1]);
    }

    #[test]
    fn k_zero_is_never_an_anchor() {
        assert!(!burst_anchors(&[1_000]).contains(&0));
        assert!(!burst_anchors(&[1_000, 2_000, 3_000]).contains(&0));
    }

    #[test]
    fn contiguous_packets_are_one_burst() {
        // Four packets inside the gap: one burst, during=midpoint(1,4)=2, after=4.
        assert_eq!(burst_anchors(&[1_000, 2_000, 3_000, 4_000]), vec![2, 4]);
    }

    #[test]
    fn a_gap_splits_bursts_and_shares_the_boundary_anchor() {
        // Packets 1-2 form the first burst, 3-4 the second (gap > BURST_GAP_NS).
        // Burst 1 -> {during 1, after 2}; burst 2 -> {before 2, during 3, after 4}.
        // The shared boundary count 2 is deduped away by the anchor set.
        let packets = [1_000, 2_000, 100_000_000, 101_000_000];
        assert_eq!(burst_anchors(&packets), vec![1, 2, 3, 4]);
    }

    #[test]
    fn service_profiles_split_by_direction_and_skip_pre_scenario_writes() {
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
        let profiles = service_profiles_from_sessions(&[session], 50);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].service, "db");
        // A single post-start packet each direction -> the after anchor (K=1).
        assert_eq!(profiles[0].client_to_upstream, vec![1]);
        assert_eq!(profiles[0].upstream_to_client, vec![1]);
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
