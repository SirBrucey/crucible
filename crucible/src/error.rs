use std::process::ExitStatus;

use crucible_core::ipc::codec;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] codec::Error),
    #[error("runner exe has no parent directory")]
    RunnerExeParentless,
    #[error("spawned child has no pid")]
    ChildPidMissing,
    #[error("handshake timed out")]
    HandshakeTimeout,
    #[error("worker exited with non-zero status: {0}")]
    WorkerExitedNonZero(ExitStatus),
}

pub type Result<T> = std::result::Result<T, Error>;
