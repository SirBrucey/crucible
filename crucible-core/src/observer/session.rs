//! `SessionObserver`: streams sidecar proxy events into an observation buffer.

use bollard::{Docker as DockerClient, query_parameters::LogsOptionsBuilder};
use crucible_protocol::ConnEvent;
use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{proxy_log::Sessions, verdict::Observations};

pub struct SessionObserver {
    events_rx: mpsc::UnboundedReceiver<(String, ConnEvent)>,
    tasks: Vec<JoinHandle<()>>,
}

impl SessionObserver {
    pub fn start(client: &DockerClient, sidecars: Vec<(String, String)>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut tasks = Vec::with_capacity(sidecars.len());
        for (service, container) in sidecars {
            let client = client.clone();
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                stream_sidecar(client, service, container, tx).await;
            }));
        }
        Self {
            events_rx: rx,
            tasks,
        }
    }

    /// Drain every event the streaming tasks have delivered so far, correlate
    /// the raw events into `Session` records, and place the result in
    /// `observations.sessions`.
    pub fn observe(&mut self, observations: &mut Observations) {
        let mut sessions = Sessions::new();
        while let Ok((service, event)) = self.events_rx.try_recv() {
            sessions.accept_event(&service, event);
        }
        observations.sessions = sessions.into_iter().collect();
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
