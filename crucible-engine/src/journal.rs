//! Single-writer append-only journal.
//!
//! Serializes [`RunnerEvent`]s as JSON Lines.

// JSON was chosen for human greppability; write throughput is not a bottleneck
// at journal cadence.
//
// Each line is flushed to the OS as it is written, so an abnormal exit loses at
// most the line in flight rather than a whole in-process buffer. No `fsync` is
// called, so a power loss can still drop lines sitting in the OS page cache;
// that degree of durability is beyond the journal's scope.

use std::{io, path::PathBuf, sync::Arc};

use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};

use crate::event_bus::RunnerEvent;

/// Drain the receiver, appending each event to `path` as JSON.
///
/// # Errors
/// Returns an [`io::Error`] if the parent directory cannot be created, `path`
/// cannot be opened for append, an event cannot be serialized to JSON, or a line
/// cannot be written or flushed.
pub async fn run(mut rx: mpsc::Receiver<Arc<RunnerEvent>>, path: PathBuf) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    let mut writer = BufWriter::new(file);
    while let Some(event) = rx.recv().await {
        let line = serde_json::to_string(&*event).map_err(io::Error::other)?;
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

/// Default journal path under `$XDG_STATE_HOME/crucible/logs/{pid}/journal.ndjson`,
/// falling back to `$HOME/.local/state/...`, then the system temp directory, when
/// neither environment variable is set.
#[must_use]
pub fn default_path(pid: u32) -> PathBuf {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .unwrap_or_else(std::env::temp_dir);
    base.join("crucible")
        .join("logs")
        .join(pid.to_string())
        .join("journal.ndjson")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crucible_core::ipc::WorkerToRunner;

    fn temp_journal_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "crucible-journal-test-{}-{}-{name}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        path
    }

    #[tokio::test]
    async fn writes_events_as_json_lines() {
        let path = temp_journal_path("writes");
        let (tx, rx) = mpsc::channel(4);
        tx.send(Arc::new(RunnerEvent::WorkerMessage {
            worker_id: 0,
            message: WorkerToRunner::Ready,
        }))
        .await
        .unwrap();
        tx.send(Arc::new(RunnerEvent::WorkerMessage {
            worker_id: 1,
            message: WorkerToRunner::Ready,
        }))
        .await
        .unwrap();
        drop(tx);

        run(rx, path.clone()).await.unwrap();

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"worker_id\":0"));
        assert!(lines[1].contains("\"worker_id\":1"));

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn creates_parent_directory() {
        let mut path = temp_journal_path("mkdirs");
        path.push("nested/journal.ndjson");
        let (tx, rx) = mpsc::channel(1);
        tx.send(Arc::new(RunnerEvent::WorkerMessage {
            worker_id: 42,
            message: WorkerToRunner::Ready,
        }))
        .await
        .unwrap();
        drop(tx);

        run(rx, path.clone()).await.unwrap();

        assert!(path.exists());
        if let Some(parent) = path.parent() {
            tokio::fs::remove_dir_all(parent).await.ok();
        }
    }
}
