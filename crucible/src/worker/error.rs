use std::io;

use crucible_core::ipc::codec;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Codec(#[from] codec::Error),
    #[error(transparent)]
    Plugin(#[from] crucible_plugin::Error),
    #[error("worker in state `{state}` expected `{expected}`, got `{got}`")]
    UnexpectedMessage {
        state: &'static str,
        expected: &'static str,
        got: String,
    },
    #[error("version mismatch: worker is `{ours}` but runner is `{theirs}`")]
    VersionMismatch { ours: String, theirs: String },
}

pub type Result<T> = std::result::Result<T, Error>;
