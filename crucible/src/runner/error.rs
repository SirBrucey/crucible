use std::{process::ExitStatus, time::Duration};

use crucible_core::ipc::codec;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] codec::Error),
    #[error("runner exe has no parent directory")]
    RunnerExeParentless,
    #[error("no worker binary at {} or {}", beside.display(), installed.display())]
    WorkerBinMissing {
        beside: std::path::PathBuf,
        installed: std::path::PathBuf,
    },
    #[error("expected a `.cru` file, got `{0}`")]
    NotAScenarioFile(String),
    #[error("cannot read {path}: {source}")]
    ScenarioUnreadable {
        path: String,
        source: std::io::Error,
    },
    #[error("{0} does not describe a runnable plan")]
    ScenarioRejected(String),
    #[error("spawned child has no pid")]
    ChildPidMissing,
    #[error("handshake timed out")]
    HandshakeTimeout,
    #[error("worker exited with non-zero status: {0}")]
    WorkerExitedNonZero(ExitStatus),
    #[error("worker did not exit within {0:?}")]
    WorkerTimeout(Duration),
    #[error("worker sent no heartbeat within {0:?}")]
    WorkerUnresponsive(Duration),
    #[error("version mismatch: runner is `{ours}` but worker is `{theirs}`")]
    VersionMismatch { ours: String, theirs: String },
    #[error("runner session in state `{state}` expected `{expected}`, got `{got}`")]
    UnexpectedMessage {
        state: &'static str,
        expected: &'static str,
        got: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
