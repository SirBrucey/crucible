//! In-runner event bus.
//!
//! Publishers call [`EventBus::publish`] to deliver events to the journal
//! (mpsc, back-pressured) and to live observers (broadcast, lag-drops).

use std::{sync::Arc, time::Duration};

use crucible_core::{
    ipc::{RunnerToWorker, WorkerEvent, WorkerToRunner},
    schedule::{Progress, Purpose},
};
use tokio::sync::{broadcast, mpsc};

/// Capacity of the mpsc journal channel.
const MPSC_CAPACITY: usize = 1024;

/// Capacity of the broadcast observer channel.
const BROADCAST_CAPACITY: usize = 256;

/// Events published on the runner's event bus.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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
    /// The campaign the scheduler fitted, what each schedule is for and how
    /// long it is expected to take.
    Fitted {
        schedules: Vec<(u32, Purpose)>,
        eta: Duration,
        /// How many schedules the campaign can run at once.
        workers: usize,
    },
    /// A schedule reached a new state.
    Moved { schedule: u32, to: Progress },
}

impl RunnerEvent {
    /// Whether this is a reference run for one of the step sets in `wanted`.
    #[must_use]
    pub fn answers(&self, wanted: &[&Vec<usize>]) -> bool {
        let RunnerEvent::RunnerMessage {
            message: RunnerToWorker::Run(schedule),
            ..
        } = self
        else {
            return false;
        };
        matches!(&schedule.purpose, Purpose::Reference { landed } if wanted.contains(&landed))
    }

    /// Which schedule this event is about.
    #[must_use]
    pub fn about(&self) -> Option<u32> {
        match self {
            RunnerEvent::Moved { schedule, .. } => Some(*schedule),
            RunnerEvent::RunnerMessage {
                message: RunnerToWorker::Run(schedule),
                ..
            } => Some(schedule.id),
            RunnerEvent::WorkerMessage { message, .. } => match message {
                WorkerToRunner::RunResult { schedule_id, .. } => Some(*schedule_id),
                WorkerToRunner::Event(WorkerEvent::Fault(report)) => Some(report.schedule_id),
                _ => None,
            },
            _ => None,
        }
    }
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
    use crucible_core::ipc::WorkerToRunner;

    use super::*;

    #[rstest::rstest]
    #[case::fitted(RunnerEvent::Fitted {
        schedules: vec![(1, Purpose::Learn), (2, Purpose::Reference { landed: vec![1, 2] })],
        eta: Duration::from_secs(90),
        workers: 3,
    })]
    #[case::moved(RunnerEvent::Moved { schedule: 4, to: Progress::Running { worker: 2 } })]
    #[case::parked(RunnerEvent::Moved {
        schedule: 4,
        to: Progress::CounterExample { wants: vec![vec![1], vec![1, 2]] },
    })]
    #[case::errored(RunnerEvent::Moved { schedule: 4, to: Progress::Errored })]
    #[case::worker(RunnerEvent::WorkerMessage { worker_id: 7, message: WorkerToRunner::Ready })]
    fn journal_roundtrip(#[case] event: RunnerEvent) {
        // A variant that will not read back shows up as an empty journal.
        let line = serde_json::to_string(&event).expect("an event serialises");

        let read: RunnerEvent = serde_json::from_str(&line).expect("an event reads back");

        assert_eq!(read, event);
    }

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
