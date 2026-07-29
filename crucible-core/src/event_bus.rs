//! In-runner event bus.
//!
//! Publishers call [`EventBus::publish`] to deliver events to the journal
//! (mpsc, back-pressured) and to live observers (broadcast, lag-drops).

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use crate::ipc::{RunnerToWorker, WorkerToRunner};

/// Capacity of the mpsc journal channel.
const MPSC_CAPACITY: usize = 1024;

/// Capacity of the broadcast observer channel.
const BROADCAST_CAPACITY: usize = 256;

/// Events published on the runner's event bus.
#[derive(Debug, serde::Serialize)]
pub enum RunnerEvent {
    /// A message received from a worker over IPC.
    WorkerMessage {
        worker_id: u32,
        message: WorkerToRunner,
    },
    /// A message the runner sent to a worker over IPC.
    RunnerMessage {
        worker_id: u32,
        message: RunnerToWorker,
    },
}

/// Error returned when [`EventBus::publish`] cannot deliver to the journal
/// (mpsc consumer dropped).
pub type PublishError = mpsc::error::SendError<Arc<RunnerEvent>>;

/// In-runner event bus.
#[derive(Clone)]
pub struct EventBus {
    mpsc_tx: mpsc::Sender<Arc<RunnerEvent>>,
    broadcast_tx: broadcast::Sender<Arc<RunnerEvent>>,
}

impl EventBus {
    /// Create the channels and return the bus plus the journal's mpsc receiver.
    ///
    /// The caller hands the receiver to the journal task (or a stand-in) so
    /// the mpsc drains. Without a consumer, publishers eventually block on
    /// [`EventBus::publish`].
    #[must_use]
    pub fn new() -> (Self, mpsc::Receiver<Arc<RunnerEvent>>) {
        let (mpsc_tx, mpsc_rx) = mpsc::channel(MPSC_CAPACITY);
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        (
            Self {
                mpsc_tx,
                broadcast_tx,
            },
            mpsc_rx,
        )
    }

    /// Publish an event. Awaits mpsc capacity for the journal; fire-and-forget
    /// on the observer broadcast.
    ///
    /// # Errors
    /// Errors if the journal's mpsc consumer has been dropped.
    pub async fn publish(&self, event: RunnerEvent) -> Result<(), PublishError> {
        let event = Arc::new(event);
        self.mpsc_tx.send(event.clone()).await?;
        let _ = self.broadcast_tx.send(event);
        Ok(())
    }

    /// Subscribe a live observer to the broadcast channel.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<RunnerEvent>> {
        self.broadcast_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::WorkerToRunner;

    #[tokio::test]
    async fn journal_receives_published_event() {
        let (bus, mut journal_rx) = EventBus::new();
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: 7,
            message: WorkerToRunner::Ready,
        })
        .await
        .unwrap();
        let event = journal_rx.recv().await.unwrap();
        match &*event {
            RunnerEvent::WorkerMessage {
                worker_id,
                message: WorkerToRunner::Ready,
            } => {
                assert_eq!(*worker_id, 7);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscriber_receives_published_event() {
        let (bus, mut journal_rx) = EventBus::new();
        let mut observer = bus.subscribe();
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: 3,
            message: WorkerToRunner::Ready,
        })
        .await
        .unwrap();
        // Drain the journal side so subsequent publishes don't block.
        let _ = journal_rx.recv().await.unwrap();
        let event = observer.recv().await.unwrap();
        match &*event {
            RunnerEvent::WorkerMessage {
                worker_id,
                message: WorkerToRunner::Ready,
            } => {
                assert_eq!(*worker_id, 3);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiple_subscribers_each_receive_events() {
        let (bus, mut journal_rx) = EventBus::new();
        let mut obs1 = bus.subscribe();
        let mut obs2 = bus.subscribe();
        bus.publish(RunnerEvent::WorkerMessage {
            worker_id: 1,
            message: WorkerToRunner::Ready,
        })
        .await
        .unwrap();
        let _ = journal_rx.recv().await.unwrap();
        for observer in [&mut obs1, &mut obs2] {
            match &*observer.recv().await.unwrap() {
                RunnerEvent::WorkerMessage {
                    worker_id,
                    message: WorkerToRunner::Ready,
                } => {
                    assert_eq!(*worker_id, 1);
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn dropping_bus_closes_channels() {
        let (bus, mut journal_rx) = EventBus::new();
        let mut observer = bus.subscribe();
        drop(bus);
        assert!(journal_rx.recv().await.is_none());
        assert!(matches!(
            observer.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
    }
}
