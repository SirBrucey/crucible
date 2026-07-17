mod cli;

use clap::Parser;
use crucible_core::ipc::{
    RunnerToWorker, WorkerToRunner,
    codec::{self, read_frame, write_frame},
};
use tokio::net::UnixStream;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> codec::Result<()> {
    let args = Cli::parse();

    eprintln!("worker {} connecting to {}", args.worker_id, args.socket);
    let mut stream = UnixStream::connect(&args.socket).await?;

    eprintln!("worker {} sending HELLO to {}", args.worker_id, args.socket);
    write_frame(
        &mut stream,
        &WorkerToRunner::Hello {
            worker_version: env!("CARGO_PKG_VERSION").to_string(),
            worker_id: args.worker_id,
        },
    )
    .await?;

    match read_frame::<RunnerToWorker, _>(&mut stream).await? {
        RunnerToWorker::HelloAck { runner_version } => {
            eprintln!(
                "worker {} handshake ok, runner version {runner_version}",
                args.worker_id
            );
        }
        other => panic!("expected HelloAck during handshake, got {other:?}"),
    }

    Ok(())
}
