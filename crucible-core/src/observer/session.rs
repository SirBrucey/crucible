//! `SessionObserver`: streams sidecar proxy events into an authoritative log.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bollard::{Docker as DockerClient, query_parameters::LogsOptionsBuilder};
use crucible_protocol::{ConnEvent, now_ns};
use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{proxy_log::Sessions, verdict::Observations};

const QUIESCENCE_POLL: Duration = Duration::from_millis(200);

pub struct SessionObserver {
    buffer: Arc<Mutex<Vec<(String, ConnEvent)>>>,
    tasks: Vec<JoinHandle<()>>,
}

impl SessionObserver {
    pub fn start(client: &DockerClient, sidecars: Vec<(String, String)>) -> Self {
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

        let mut tasks = vec![aggregator];
        for (service, container) in sidecars {
            let client = client.clone();
            let tx = mpsc_tx.clone();
            tasks.push(tokio::spawn(async move {
                stream_sidecar(client, service, container, tx).await;
            }));
        }

        Self { buffer, tasks }
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
    service: String,
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
                tracing::warn!(target: "session_observer", %service, %container, error = %e, "log stream error");
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
            match serde_json::from_str::<ConnEvent>(trimmed) {
                Ok(event) => {
                    if tx.send((service.clone(), event)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "session_observer", %service, %container, error = %e, line = %trimmed, "parse conn event");
                }
            }
        }
    }
}
