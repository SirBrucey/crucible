//! Length-prefixed postcard framing for IPC messages.

use std::io;

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum size of a single frame. Anything larger is refused as malformed.
///
/// A bound on what a peer may claim to be sending, not on what a message may
/// say.
pub(crate) const MAX_FRAME_SIZE: usize = 16 << 20;

// The frame header stores the payload length as a big-endian u32, so the cap
// must fit in a u32. Pinned at compile time, which lets `write_frame` convert
// the length with no run-time panic path.
const _: () = assert!(MAX_FRAME_SIZE <= u32::MAX as usize);

/// Errors returned by the codec.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Postcard(#[from] postcard::Error),
    #[error("frame too large: {size} bytes exceeds {max}")]
    TooLarge { size: usize, max: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Serialize `message` with postcard and write it as a length-prefixed frame.
///
/// # Errors
/// Returns `Error::TooLarge` if the encoded message exceeds `MAX_FRAME_SIZE`,
/// `Error::Postcard` for any other serialization failure, and `Error::Io` if
/// writing to `writer` fails.
pub async fn write_frame<T, W>(writer: &mut W, message: &T) -> Result<()>
where
    T: Serialize,
    W: AsyncWriteExt + Unpin,
{
    let bytes: Vec<u8> = postcard::to_extend(message, Vec::new())?;
    if bytes.len() > MAX_FRAME_SIZE {
        return Err(Error::TooLarge {
            size: bytes.len(),
            max: MAX_FRAME_SIZE,
        });
    }
    // Safe by construction: the check above holds `bytes` at or under
    // MAX_FRAME_SIZE, which the compile-time assertion pins within u32 range.
    #[allow(clippy::cast_possible_truncation)]
    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

/// Read one length-prefixed frame and deserialize it with postcard.
///
/// # Errors
/// Returns `Error::Io` if reading the length header or the frame body fails,
/// `Error::TooLarge` if the declared length exceeds `MAX_FRAME_SIZE`, and
/// `Error::Postcard` if the payload fails to decode into `T`.
pub async fn read_frame<T, R>(reader: &mut R) -> Result<T>
where
    T: DeserializeOwned,
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(Error::TooLarge {
            size: len,
            max: MAX_FRAME_SIZE,
        });
    }
    // The length decides the allocation.
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    let message = postcard::from_bytes(&bytes)?;
    Ok(message)
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crucible_protocol::Direction;

    use crate::ipc::{RunnerToWorker, WorkerEvent, WorkerToRunner};

    /// Encode `msg`, decode it, and assert the decoded value equals the original.
    async fn roundtrip<T>(msg: T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let (mut tx, mut rx) = tokio::io::duplex(MAX_FRAME_SIZE);
        write_frame(&mut tx, &msg).await.unwrap();
        assert_eq!(read_frame::<T, _>(&mut rx).await.unwrap(), msg);
    }

    #[tokio::test]
    async fn roundtrips_hello() {
        roundtrip(WorkerToRunner::Hello {
            worker_version: "0.1.0".to_string(),
            worker_id: 42,
        })
        .await;
    }

    #[tokio::test]
    async fn roundtrips_ready() {
        roundtrip(WorkerToRunner::Ready).await;
    }

    #[tokio::test]
    async fn roundtrips_hello_ack() {
        roundtrip(RunnerToWorker::HelloAck {
            runner_version: "0.1.0".to_string(),
        })
        .await;
    }

    fn fleet() -> crate::plan::Fleet {
        crate::plan::Fleet {
            name: "orders".into(),
            deployment: "docker".into(),
            services: vec![crate::plan::Service {
                name: "db".into(),
                kinds: vec!["mariadb".into()],
                attrs: vec![("image".into(), crate::plan::Value::Str("mariadb:11".into()))],
            }],
        }
    }

    fn step() -> crate::plan::Step {
        crate::plan::Step {
            driver: "http".into(),
            operation: "POST".into(),
            args: vec![
                crate::plan::Value::Ident("api".into()),
                crate::plan::Value::Str("/orders".into()),
            ],
            blocks: std::collections::BTreeMap::new(),
            expect: None,
        }
    }

    fn check() -> crate::plan::Check {
        crate::plan::Check {
            service: "db".into(),
            observer: "mariadb".into(),
            observable: vec!["orders".into(), "count".into()],
            args: Vec::new(),
            filter: None,
            clauses: std::collections::BTreeMap::new(),
            op: crate::schema::CmpOp::Eq,
            value: crate::plan::Value::Int(3),
        }
    }

    #[tokio::test]
    async fn roundtrips_a_schedule_carrying_its_work() {
        roundtrip(RunnerToWorker::Run(Box::new(
            crate::schedule::Schedule::faulted(
                7,
                fleet(),
                vec![step()],
                vec![check()],
                crate::fault::Fault::at(
                    crate::fault::Anchor::crossing(
                        crucible_protocol::Edge {
                            client: Some("api".into()),
                            upstream: "db".into(),
                        },
                        Direction::ClientToUpstream,
                        "ack:7:before".into(),
                        "an ack the consumer has sent and the broker has not seen".into(),
                    ),
                    crate::fault::By::Kill("db".into()),
                ),
                crate::verdict::Baseline::unsettled([
                    vec![Some(crate::plan::Value::Int(0))],
                    vec![Some(crate::plan::Value::Int(3))],
                ]),
                std::time::Duration::from_secs(15),
            ),
        )))
        .await;
    }

    #[tokio::test]
    async fn roundtrips_run_result() {
        roundtrip(WorkerToRunner::RunResult {
            schedule_id: 7,
            readings: Box::default(),
        })
        .await;
    }

    #[tokio::test]
    async fn roundtrips_run_result_with_readings() {
        let mut readings = crate::verdict::Readings::empty();
        readings.outcomes = vec![crate::verdict::Outcome {
            ack: crate::verdict::Ack::Acked,
        }];
        readings.windows = vec![crate::verdict::StepWindow {
            start_ns: 1_788_943_890_682_389_587,
            end_ns: 1_788_943_890_682_389_999,
        }];
        readings.fault_free =
            crate::verdict::Baseline::unsettled([vec![Some(crate::plan::Value::Int(3))]]);
        roundtrip(WorkerToRunner::RunResult {
            schedule_id: 7,
            readings: Box::new(readings),
        })
        .await;
    }

    #[tokio::test]
    async fn roundtrips_event_log() {
        roundtrip(WorkerToRunner::Event(WorkerEvent::Log("hello".into()))).await;
    }

    #[tokio::test]
    async fn write_frame_rejects_oversized_payload() {
        let (mut tx, _rx) = tokio::io::duplex(MAX_FRAME_SIZE);
        let big = "x".repeat(MAX_FRAME_SIZE + 1);
        let msg = WorkerToRunner::Hello {
            worker_version: big,
            worker_id: 0,
        };
        let result = write_frame(&mut tx, &msg).await;
        assert!(matches!(result, Err(Error::TooLarge { .. })));
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_declared_length() {
        let bad_len = u32::try_from(MAX_FRAME_SIZE + 1).unwrap().to_be_bytes();
        let (mut tx, mut rx) = tokio::io::duplex(MAX_FRAME_SIZE);
        tx.write_all(&bad_len).await.unwrap();
        drop(tx);
        let result: Result<WorkerToRunner> = read_frame(&mut rx).await;
        assert!(matches!(result, Err(Error::TooLarge { .. })));
    }
}
