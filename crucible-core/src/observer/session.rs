//! `SessionObserver`: streams sidecar proxy events into an observation buffer.

use bollard::{Docker as DockerClient, query_parameters::LogsOptionsBuilder};
use crucible_protocol::{ConnEvent, ConnEventKind, ConnId};
use futures_util::StreamExt;
use tokio::{sync::broadcast, task::JoinHandle};

use crate::{proxy_log::Sessions, verdict::Observations};

const BROADCAST_CAPACITY: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("session observer stream closed")]
    Closed,
}

pub struct SessionObserver {
    events_tx: broadcast::Sender<(String, ConnEvent)>,
    collector_rx: broadcast::Receiver<(String, ConnEvent)>,
    tasks: Vec<JoinHandle<()>>,
}

impl SessionObserver {
    pub fn start(client: &DockerClient, sidecars: Vec<(String, String)>) -> Self {
        let (events_tx, collector_rx) = broadcast::channel(BROADCAST_CAPACITY);
        let mut tasks = Vec::with_capacity(sidecars.len());
        for (service, container) in sidecars {
            let client = client.clone();
            let tx = events_tx.clone();
            tasks.push(tokio::spawn(async move {
                stream_sidecar(client, service, container, tx).await;
            }));
        }
        Self {
            events_tx,
            collector_rx,
            tasks,
        }
    }

    /// Fresh subscriber; sees events sent after the moment it subscribes.
    pub fn subscribe(&self) -> broadcast::Receiver<(String, ConnEvent)> {
        self.events_tx.subscribe()
    }

    /// Drain the observer's persistent subscriber, correlate events into
    /// `Session` records, and place the result in `observations.sessions`.
    pub fn observe(&mut self, observations: &mut Observations) {
        let mut sessions = Sessions::new();
        loop {
            match self.collector_rx.try_recv() {
                Ok((service, event)) => sessions.accept_event(&service, event),
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => break,
                Err(broadcast::error::TryRecvError::Lagged(dropped)) => {
                    tracing::warn!(target: "session_observer", dropped, "collector lagged");
                }
            }
        }
        observations.sessions = sessions.into_iter().collect();
    }

    /// Wait for an `Opened` event matching `(service, conn_id)` and return its
    /// sidecar-stamped wall-clock timestamp.
    pub async fn wait_for(&self, service: &str, conn_id: ConnId) -> Result<u128, WaitError> {
        let mut rx = self.subscribe();
        loop {
            match rx.recv().await {
                Ok((svc, event))
                    if svc == service
                        && event.id == conn_id
                        && matches!(event.kind, ConnEventKind::Opened { .. }) =>
                {
                    return Ok(event.ts_ns);
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    tracing::warn!(target: "session_observer", dropped, "wait_for lagged");
                }
                Err(broadcast::error::RecvError::Closed) => return Err(WaitError::Closed),
            }
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
    tx: broadcast::Sender<(String, ConnEvent)>,
) {
    let options = LogsOptionsBuilder::default()
        .stdout(true)
        .stderr(false)
        .follow(true)
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
