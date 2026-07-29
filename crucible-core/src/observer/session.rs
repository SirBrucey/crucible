//! `SessionObserver`: streams sidecar proxy events into an authoritative log.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bollard::{Docker as DockerClient, query_parameters::LogsOptionsBuilder};
use crucible_protocol::{ConnEvent, ConnEventKind, Direction, now_ns};
use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{proxy_log::Sessions, verdict::Observations};

const QUIESCENCE_POLL: Duration = Duration::from_millis(200);
/// How often the fault anchor re-checks the observed packet count.
const ANCHOR_POLL: Duration = Duration::from_millis(5);

pub struct SessionObserver {
    buffer: Arc<Mutex<Vec<(String, ConnEvent)>>>,
    tasks: Vec<JoinHandle<()>>,
}

impl SessionObserver {
    /// Stream the fleet's single proxy container. Every pair in that proxy tags
    /// its event lines with its service, so the interleaved stream stays
    /// attributable per service.
    pub fn start(client: &DockerClient, proxy_container: String) -> Self {
        let (mpsc_tx, mut mpsc_rx) = mpsc::unbounded_channel::<(String, ConnEvent)>();
        let buffer: Arc<Mutex<Vec<(String, ConnEvent)>>> = Arc::new(Mutex::new(Vec::new()));

        let agg_buffer = buffer.clone();
        let aggregator = tokio::spawn(async move {
            while let Some(pair) = mpsc_rx.recv().await {
                agg_buffer
                    .lock()
                    .expect("session observer buffer mutex")
                    .push(pair);
            }
        });

        let stream = {
            let client = client.clone();
            tokio::spawn(async move {
                stream_sidecar(client, proxy_container, mpsc_tx).await;
            })
        };

        Self {
            buffer,
            tasks: vec![aggregator, stream],
        }
    }

    /// Snapshot every event the observer has recorded so far, correlate them
    /// into `Session` records, and place the result in `observations.sessions`.
    pub fn observe(&self, observations: &mut Observations) {
        let events = self
            .buffer
            .lock()
            .expect("session observer buffer mutex")
            .clone();
        let mut sessions = Sessions::new();
        for (service, event) in events {
            sessions.accept_event(&service, event);
        }
        observations.sessions = sessions.into_iter().collect();
    }

    /// Block until `service` has written `count` more packets on `direction`
    /// than it had when this call began, or until `timeout` elapses; returns
    /// whether the count was reached. The baseline is captured at call time,
    /// which the caller aligns with scenario start (the same moment it arms the
    /// proxy), so both count scenario traffic from the same origin. This detects
    /// the proxy's own freeze; `count == 0` returns immediately.
    pub async fn wait_for_packet(
        &self,
        service: &str,
        direction: Direction,
        count: u32,
        timeout: Duration,
    ) -> bool {
        let baseline = self.packet_count(service, direction);
        if count == 0 {
            return true;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self
                .packet_count(service, direction)
                .saturating_sub(baseline)
                >= count
            {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(ANCHOR_POLL).await;
        }
    }

    fn packet_count(&self, service: &str, direction: Direction) -> u32 {
        let count = self
            .buffer
            .lock()
            .expect("session observer buffer mutex")
            .iter()
            .filter(|(svc, event)| {
                svc == service
                    && matches!(event.kind, ConnEventKind::Wrote { direction: d, .. } if d == direction)
            })
            .count();
        u32::try_from(count).expect("observed packet count fits in u32")
    }

    /// Wall-clock nanoseconds of the most recent event the observer has
    /// recorded, or `None` if nothing has been observed yet.
    pub fn last_event_ns(&self) -> Option<u128> {
        self.buffer
            .lock()
            .expect("session observer buffer mutex")
            .iter()
            .map(|(_, event)| event.ts_ns)
            .max()
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

async fn stream_sidecar(
    client: DockerClient,
    container: String,
    tx: mpsc::UnboundedSender<(String, ConnEvent)>,
) {
    let options = LogsOptionsBuilder::default()
        .stdout(true)
        .stderr(false)
        .follow(true)
        .tail("all")
        .build();
    let mut stream = client.logs(&container, Some(options));
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(target: "session_observer", %container, error = %e, "log stream error");
                return;
            }
        };
        buffer.extend_from_slice(chunk.as_ref());
        while let Some(nl) = buffer.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buffer.drain(..=nl).collect();
            let text = String::from_utf8_lossy(&line[..line.len() - 1]);
            let trimmed = text.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Each line is `service\tjson`: the proxy tags every event with the
            // pair's service so one interleaved stream stays attributable.
            let Some((service, json)) = trimmed.split_once('\t') else {
                tracing::warn!(target: "session_observer", %container, line = %trimmed, "conn event line missing service tag");
                continue;
            };
            match serde_json::from_str::<ConnEvent>(json) {
                Ok(event) => {
                    if tx.send((service.to_string(), event)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "session_observer", %container, error = %e, line = %json, "parse conn event");
                }
            }
        }
    }
}
