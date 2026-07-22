//! Length-prefixed postcard framing for IPC messages.

use std::io;

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum size of a single frame. Anything larger is refused as malformed.
///
/// Kept small on purpose. The handshake is tens of bytes; other IPC messages
/// currently fit comfortably in this budget. Bump deliberately if a real
/// message needs more headroom.
const MAX_FRAME_SIZE: usize = 1024;

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
/// Encoding happens into a stack-allocated buffer of `MAX_FRAME_SIZE` bytes;
/// larger messages are rejected as `Error::TooLarge`.
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
    let len = u32::try_from(bytes.len()).expect("size <= MAX_FRAME_SIZE fits in u32");
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
    Ok(())
}

/// Read one length-prefixed frame and deserialize it with postcard.
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
    async fn roundtrips_schedule() {
        roundtrip(RunnerToWorker::Schedule {
            schedule_id: 7,
            invariant: crate::verdict::Invariant::Durable,
            payload: vec![0u8; 128],
        })
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
