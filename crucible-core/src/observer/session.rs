//! `SessionObserver`: streams sidecar proxy events into an authoritative log.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crucible_protocol::{ConnEvent, ConnEventKind, Did, Direction, Session, now_ns};
use futures_util::{Stream, StreamExt};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{proxy_log::Sessions, verdict::Observations};

const QUIESCENCE_POLL: Duration = Duration::from_millis(200);
/// How often the kill waiter re-checks for the proxy's freeze signal.
const ANCHOR_POLL: Duration = Duration::from_millis(5);

/// Per-service running packet tallies, one counter per direction.
#[derive(Default, Clone, Copy)]
struct DirectionCounts {
    client_to_upstream: u32,
    upstream_to_client: u32,
}

impl DirectionCounts {
    fn get(self, direction: Direction) -> u32 {
        match direction {
            Direction::ClientToUpstream => self.client_to_upstream,
            Direction::UpstreamToClient => self.upstream_to_client,
        }
    }

    fn increment(&mut self, direction: Direction) {
        match direction {
            Direction::ClientToUpstream => self.client_to_upstream += 1,
            Direction::UpstreamToClient => self.upstream_to_client += 1,
        }
    }
}

/// In-memory index of the proxy events the observer has recorded. Keeps the raw
/// event log (for session correlation) alongside incremental aggregates so the
/// hot-path lookups stay O(1) as the log grows: the anchor waiter re-reads
/// [`EventIndex::packet_count`] every few milliseconds and the quiescence waiter
/// re-reads [`EventIndex::last_event_ns`] several times a second, so neither can
/// afford to rescan the whole log each time.
#[derive(Default)]
pub struct EventIndex {
    events: Vec<(String, ConnEvent)>,
    packet_counts: HashMap<String, DirectionCounts>,
    freezes: u32,
    /// When the proxy last held the fleet, which is also when the fault landed.
    froze_at: Option<u128>,
    /// What the fault has said of itself, since a fault nothing can be seen to
    /// have met is one the run cannot be judged on.
    placed: Vec<Reported>,
    last_ts: Option<u128>,
}

/// Something the fault said of itself, and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    pub did: Did,
    pub at_ns: u128,
}

impl EventIndex {
    /// Record one observed event, folding it into the aggregates before storing
    /// it. `Wrote` events count as packets and `Froze` events as freezes; every
    /// event advances the last seen timestamp.
    pub fn record(&mut self, service: String, event: ConnEvent) {
        self.last_ts = Some(match self.last_ts {
            Some(prev) => prev.max(event.ts_ns),
            None => event.ts_ns,
        });
        if let ConnEventKind::Wrote { direction, .. } = event.kind {
            self.packet_counts
                .entry(service.clone())
                .or_default()
                .increment(direction);
        }
        if matches!(event.kind, ConnEventKind::Froze { .. }) {
            self.freezes += 1;
            self.froze_at = Some(event.ts_ns);
        }
        if let ConnEventKind::Did { did } = &event.kind {
            self.placed.push(Reported {
                did: did.clone(),
                at_ns: event.ts_ns,
            });
        }
        self.events.push((service, event));
    }

    /// What the fault said of itself, in the order it said it.
    #[must_use]
    pub fn placed(&self) -> &[Reported] {
        &self.placed
    }

    /// How many packets `service` has written on `direction` so far. O(1).
    #[must_use]
    pub fn packet_count(&self, service: &str, direction: Direction) -> u32 {
        self.packet_counts
            .get(service)
            .map_or(0, |counts| counts.get(direction))
    }

    /// How many times the proxy has reported the fleet freezing (the fault
    /// anchor tripping) so far. O(1).
    #[must_use]
    pub fn freeze_count(&self) -> u32 {
        self.freezes
    }

    /// Wall-clock nanoseconds of the last freeze, or `None` if the fleet has not
    /// been held. O(1).
    #[must_use]
    pub fn froze_at_ns(&self) -> Option<u128> {
        self.froze_at
    }

    /// Wall-clock nanoseconds of the most recent event, or `None` if nothing has
    /// been recorded yet. O(1).
    #[must_use]
    pub fn last_event_ns(&self) -> Option<u128> {
        self.last_ts
    }

    /// Correlate every recorded event into `Session` records.
    #[must_use]
    pub fn sessions(&self) -> Vec<Session> {
        let mut sessions = Sessions::new();
        for (service, event) in &self.events {
            sessions.accept_event(service, event.clone());
        }
        sessions.into_iter().collect()
    }
}

pub struct SessionObserver {
    index: Arc<Mutex<EventIndex>>,
    tasks: Vec<JoinHandle<()>>,
}

impl SessionObserver {
    /// Read the fleet's proxy log from `chunks`, which the deployment opens
    /// because only it knows where its proxy writes. Every pair in that proxy
    /// tags its event lines with its service, so the interleaved stream stays
    /// attributable per service.
    #[must_use]
    pub fn start<S>(chunks: S) -> Self
    where
        S: Stream<Item = Vec<u8>> + Send + 'static,
    {
        let (mpsc_tx, mut mpsc_rx) = mpsc::unbounded_channel::<(String, ConnEvent)>();
        let index: Arc<Mutex<EventIndex>> = Arc::new(Mutex::new(EventIndex::default()));

        let agg_index = index.clone();
        let aggregator = tokio::spawn(async move {
            while let Some((service, event)) = mpsc_rx.recv().await {
                agg_index
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .record(service, event);
            }
        });

        let stream = tokio::spawn(async move {
            read_events(chunks, mpsc_tx).await;
        });

        Self {
            index,
            tasks: vec![aggregator, stream],
        }
    }

    /// Snapshot every event the observer has recorded so far, correlate them
    /// into `Session` records, and place the result in `observations.sessions`.
    pub fn observe(&self, observations: &mut Observations) {
        observations.sessions = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions();
    }

    /// Block until the proxy reports the fleet has frozen (the fault anchor
    /// tripped) since `baseline`, or until `timeout` elapses; returns whether
    /// the freeze was observed.
    ///
    /// The caller takes `baseline` from [`Self::freeze_count`] when it arms the
    /// anchor, and passes the same one to every wait. Taking a fresh one per
    /// call would hide a freeze that had already arrived, and the run would go
    /// looking for a second that is never coming.
    ///
    /// A freeze that the proxy itself releases may already be over by the time
    /// it is seen here, so this says the fault was placed, not that the fleet is
    /// still held. Use [`Self::froze_at_ns`] for when it landed.
    pub async fn wait_for_freeze(&self, baseline: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.freeze_count() > baseline {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(ANCHOR_POLL).await;
        }
    }

    /// Block until the fault is seen to have been placed, or until `timeout`
    /// elapses. Returns what it last said of itself, which is `None` if it said
    /// nothing at all.
    ///
    /// A fault that asks the fleet for something cannot be known to have landed
    /// when it is asked, only when the fleet does it. Waiting for that is what
    /// separates a run the fleet met from one it never did.
    pub async fn wait_for_placed(&self, timeout: Duration) -> Option<Reported> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let last = self.placed().pop();
            if matches!(
                &last,
                Some(Reported {
                    did: Did::Placed(_),
                    ..
                })
            ) || tokio::time::Instant::now() >= deadline
            {
                return last;
            }
            tokio::time::sleep(ANCHOR_POLL).await;
        }
    }

    /// What the fault has said of itself so far.
    #[must_use]
    pub fn placed(&self) -> Vec<Reported> {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .placed()
            .to_vec()
    }

    /// Freezes seen so far, which a caller takes as the baseline for its waits.
    #[must_use]
    pub fn freeze_count(&self) -> u32 {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .freeze_count()
    }

    /// When the proxy last held the fleet, by its own clock. What reads this
    /// stream sees the freeze later than that, so a fault is timed by the proxy
    /// rather than by whoever noticed.
    #[must_use]
    pub fn froze_at_ns(&self) -> Option<u128> {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .froze_at_ns()
    }

    /// Wall-clock nanoseconds of the most recent event the observer has
    /// recorded, or `None` if nothing has been observed yet.
    fn last_event_ns(&self) -> Option<u128> {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_event_ns()
    }

    /// Block until no sidecar has forwarded traffic for `idle`, or until
    /// `ceiling` elapses. Waits `min_settle` first so post-restart recovery
    /// traffic has a chance to start before the fleet can be judged quiescent.
    /// Network idle across every sidecar implies persisted-state writes have
    /// landed too, since those flow through the db sidecar.
    pub async fn wait_for_quiescence(
        &self,
        min_settle: Duration,
        idle: Duration,
        ceiling: Duration,
    ) {
        let deadline = tokio::time::Instant::now() + ceiling;
        tokio::time::sleep(min_settle.min(ceiling)).await;
        let idle_ns = idle.as_nanos();
        loop {
            let quiet_for = match self.last_event_ns() {
                Some(ts) => now_ns().saturating_sub(ts),
                None => idle_ns,
            };
            if quiet_for >= idle_ns || tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(QUIESCENCE_POLL).await;
        }
    }

    pub async fn shutdown(self) {
        for handle in self.tasks {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// Assemble `chunks` into lines and record the events they carry. Each line is
/// `service\tjson`: the proxy tags every event with the pair's service so one
/// interleaved stream stays attributable.
async fn read_events<S>(chunks: S, tx: mpsc::UnboundedSender<(String, ConnEvent)>)
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    let mut chunks = std::pin::pin!(chunks);
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(chunk) = chunks.next().await {
        buffer.extend_from_slice(&chunk);
        while let Some(nl) = buffer.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buffer.drain(..=nl).collect();
            let text = String::from_utf8_lossy(&line[..line.len() - 1]);
            let trimmed = text.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Some((service, json)) = trimmed.split_once('\t') else {
                tracing::warn!(target: "session_observer", line = %trimmed, "conn event line missing service tag");
                continue;
            };
            match serde_json::from_str::<ConnEvent>(json) {
                Ok(event) => {
                    if tx.send((service.to_string(), event)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "session_observer", error = %e, line = %json, "parse conn event");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_count_tracks_service_and_direction() {
        let mut index = EventIndex::default();
        for (svc, ts, dir) in [
            ("db", 10, Direction::ClientToUpstream),
            ("db", 20, Direction::ClientToUpstream),
            ("db", 30, Direction::UpstreamToClient),
            ("api", 40, Direction::ClientToUpstream),
        ] {
            index.record(svc.into(), ConnEvent::wrote_at(0, ts, dir, 1));
        }
        assert_eq!(index.packet_count("db", Direction::ClientToUpstream), 2);
        assert_eq!(index.packet_count("db", Direction::UpstreamToClient), 1);
        assert_eq!(index.packet_count("api", Direction::ClientToUpstream), 1);
        assert_eq!(index.packet_count("api", Direction::UpstreamToClient), 0);
        assert_eq!(
            index.packet_count("missing", Direction::ClientToUpstream),
            0
        );
    }

    #[test]
    fn only_wrote_events_count_as_packets() {
        let mut index = EventIndex::default();
        index.record(
            "db".into(),
            ConnEvent::opened_at(0, 5, "127.0.0.1:1".parse().unwrap()),
        );
        index.record("db".into(), ConnEvent::closed_at(0, 15, 0, 0));
        assert_eq!(index.packet_count("db", Direction::ClientToUpstream), 0);
        assert_eq!(index.packet_count("db", Direction::UpstreamToClient), 0);
    }

    #[test]
    fn freeze_count_tracks_only_froze_events() {
        let mut index = EventIndex::default();
        assert_eq!(index.freeze_count(), 0);
        // Traffic is not a freeze.
        index.record(
            "db".into(),
            ConnEvent::wrote_at(0, 10, Direction::ClientToUpstream, 1),
        );
        assert_eq!(index.freeze_count(), 0);
        index.record(
            "db".into(),
            ConnEvent::froze_at(0, 20, "publish:1:after".to_owned()),
        );
        assert_eq!(index.freeze_count(), 1);
        index.record(
            "db".into(),
            ConnEvent::froze_at(0, 30, "publish:2:after".to_owned()),
        );
        assert_eq!(index.freeze_count(), 2);
    }

    /// We ask twice whether the fault fired, once while the scenario runs and
    /// again once it ends. If the second ask started counting from scratch it
    /// would miss a freeze that had already arrived, and we would report a fault
    /// that fired as one that never did.
    #[tokio::test]
    async fn a_freeze_stays_seen_by_every_wait_that_shares_a_baseline() {
        let line = format!(
            "db\t{}\n",
            serde_json::to_string(&ConnEvent::froze_at(0, 20, "publish:1:after".to_owned()))
                .expect("an event serializes")
        );
        let observer = SessionObserver::start(
            futures_util::stream::iter([line.into_bytes()])
                .chain(futures_util::stream::pending::<Vec<u8>>()),
        );
        let baseline = 0;
        let brief = Duration::from_secs(1);
        assert!(observer.wait_for_freeze(baseline, brief).await);
        assert!(
            observer.wait_for_freeze(baseline, brief).await,
            "the same freeze answers a later wait"
        );
    }

    #[test]
    fn last_event_ns_is_the_max_timestamp() {
        let mut index = EventIndex::default();
        assert_eq!(index.last_event_ns(), None);
        // Record out of order; the latest timestamp must win regardless.
        index.record(
            "db".into(),
            ConnEvent::wrote_at(0, 30, Direction::ClientToUpstream, 1),
        );
        index.record(
            "db".into(),
            ConnEvent::wrote_at(0, 10, Direction::UpstreamToClient, 1),
        );
        index.record(
            "db".into(),
            ConnEvent::wrote_at(0, 20, Direction::ClientToUpstream, 1),
        );
        assert_eq!(index.last_event_ns(), Some(30));
    }

    #[test]
    fn sessions_correlates_recorded_events() {
        let mut index = EventIndex::default();
        index.record(
            "db".into(),
            ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
        );
        index.record(
            "db".into(),
            ConnEvent::wrote_at(0, 120, Direction::ClientToUpstream, 8),
        );
        index.record("db".into(), ConnEvent::closed_at(0, 140, 0, 0));
        let sessions = index.sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].service, "db");
        assert_eq!(sessions[0].writes.len(), 1);
    }
}
