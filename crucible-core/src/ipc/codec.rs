//! Length-prefixed postcard framing for IPC messages.

use std::io;

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum size of a single frame. Anything larger is refused as malformed.
///
/// Kept small on purpose. The handshake is tens of bytes; the biggest current
/// message is [`WorkerToRunner::SessionCatalogue`](crate::ipc::WorkerToRunner::SessionCatalogue),
/// whose per-service anchor lists are sampled down to a fixed cap on the learn
/// side so it always fits (see
/// [`crate::proxy_log::service_profiles_from_sessions`]).
pub(crate) const MAX_FRAME_SIZE: usize = 4096;

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
/// The frame is a 4-byte big-endian length header followed by the postcard payload.
/// Encoding uses a stack-allocated buffer of `MAX_FRAME_SIZE` bytes; larger
/// messages are rejected as `Error::TooLarge`.
///
/// # Errors
/// Returns `Error::TooLarge` if the encoded message overflows the
/// `MAX_FRAME_SIZE` buffer, `Error::Postcard` for any other serialization
/// failure, and `Error::Io` if writing the length header or payload to `writer`
/// fails.
pub async fn write_frame<T, W>(writer: &mut W, message: &T) -> Result<()>
where
    T: Serialize,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = [0u8; MAX_FRAME_SIZE];
    let bytes = match postcard::to_slice(message, &mut buf) {
        Ok(bytes) => bytes,
        Err(postcard::Error::SerializeBufferFull) => {
            return Err(Error::TooLarge {
                size: MAX_FRAME_SIZE + 1,
                max: MAX_FRAME_SIZE,
            });
        }
        Err(e) => return Err(e.into()),
    };
    // Safe by construction: `bytes` never exceeds MAX_FRAME_SIZE, which the
    // compile-time assertion above pins within u32 range.
    #[allow(clippy::cast_possible_truncation)]
    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
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
    let mut bytes = [0u8; MAX_FRAME_SIZE];
    reader.read_exact(&mut bytes[..len]).await?;
    let message = postcard::from_bytes(&bytes[..len])?;
    Ok(message)
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crucible_protocol::Direction;

    use crate::ipc::{RunnerToWorker, Verdict, WorkerEvent, WorkerToRunner};

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

    #[tokio::test]
    async fn roundtrips_a_schedule_carrying_its_work() {
        let plan = crate::plan::example();
        roundtrip(RunnerToWorker::Run(crate::schedule::Schedule::faulted(
            7,
            plan.fleet,
            plan.scenarios[0].steps.clone(),
            plan.scenarios[0].checks.clone(),
            crate::fault::Fault::Durable(crate::fault::Anchor {
                service: "db".into(),
                direction: Direction::ClientToUpstream,
                k: 3,
            }),
            vec![
                vec![Some(crate::plan::Value::Int(0))],
                vec![Some(crate::plan::Value::Int(3))],
            ],
        )))
        .await;
    }

    #[tokio::test]
    async fn roundtrips_run_result() {
        roundtrip(WorkerToRunner::RunResult {
            schedule_id: 7,
            verdict: Verdict::Pass,
        })
        .await;
    }

    #[tokio::test]
    async fn roundtrips_run_result_with_fail_reason() {
        roundtrip(WorkerToRunner::RunResult {
            schedule_id: 7,
            verdict: Verdict::Fail {
                reason: "acked order 3 (book x4) is absent from persisted state after heal".into(),
            },
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
