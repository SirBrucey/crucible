//! `SessionObserver`: streams sidecar proxy events into an authoritative log.

use std::sync::{Arc, Mutex};

use bollard::{Docker as DockerClient, query_parameters::LogsOptionsBuilder};
use crucible_protocol::{ConnEvent, ConnEventKind, ConnId};
use futures_util::StreamExt;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

use crate::{proxy_log::Sessions, verdict::Observations};

const BROADCAST_CAPACITY: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("session observer stream closed")]
    Closed,
}

pub struct SessionObserver {
    buffer: Arc<Mutex<Vec<(String, ConnEvent)>>>,
    events_tx: broadcast::Sender<(String, ConnEvent)>,
    tasks: Vec<JoinHandle<()>>,
}

impl SessionObserver {
    pub fn start(client: &DockerClient, sidecars: Vec<(String, String)>) -> Self {
        let (events_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (mpsc_tx, mut mpsc_rx) = mpsc::unbounded_channel::<(String, ConnEvent)>();
        let buffer: Arc<Mutex<Vec<(String, ConnEvent)>>> = Arc::new(Mutex::new(Vec::new()));

        let agg_buffer = buffer.clone();
        let agg_events_tx = events_tx.clone();
        let aggregator = tokio::spawn(async move {
            while let Some(pair) = mpsc_rx.recv().await {
                agg_buffer
                    .lock()
                    .expect("session observer buffer mutex")
                    .push(pair.clone());
                let _ = agg_events_tx.send(pair);
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

        Self {
            buffer,
            events_tx,
            tasks,
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

    /// Return the sidecar-stamped timestamp of the `Opened` event matching
    /// `(service, conn_id)`. Subscribes first, then scans the buffer, so an
    /// event that arrives between the two paths is caught by either.
    pub async fn wait_for(&self, service: &str, conn_id: ConnId) -> Result<u128, WaitError> {
        let mut rx = self.events_tx.subscribe();
        {
            let buffer = self.buffer.lock().expect("session observer buffer mutex");
            for (svc, event) in buffer.iter() {
                if svc == service
                    && event.id == conn_id
                    && matches!(event.kind, ConnEventKind::Opened { .. })
                {
                    return Ok(event.ts_ns);
                }
            }
        }
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
