use std::io;

use crucible_core::{deployment::docker, ipc::codec, orchestrator, scenario};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Codec(#[from] codec::Error),
    #[error(transparent)]
    Deployment(#[from] docker::Error),
    #[error(transparent)]
    Scenario(#[from] scenario::Error),
    #[error(transparent)]
    Execute(#[from] orchestrator::Error),
    #[error("worker in state `{state}` expected `{expected}`, got `{got}`")]
    UnexpectedMessage {
        state: &'static str,
        expected: &'static str,
        got: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
