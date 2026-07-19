mod cli;
mod error;
mod state;

use clap::Parser;
use tokio::net::UnixStream;

use crate::{
    cli::Cli,
    error::Result,
    state::{IdleNext, Worker},
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    eprintln!("[worker {}] connecting to {}", args.worker_id, args.socket);
    let stream = UnixStream::connect(&args.socket).await?;

    let mut worker = Worker::new(
        stream,
        args.worker_id,
        env!("CARGO_PKG_VERSION").to_string(),
    )
    .handshake()
    .await?;

    loop {
        worker = match worker.await_work().await? {
            IdleNext::Work(worker) => worker.execute_and_report().await?,
            IdleNext::Shutdown(worker) => {
                worker.teardown().await?;
                break;
            }
        };
    }

    Ok(())
}
