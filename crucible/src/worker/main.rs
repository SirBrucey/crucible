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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    if let Err(e) = run().await {
        tracing::error!(error = %e, "worker exiting with error");
        return Err(e);
    }
    Ok(())
}

async fn run() -> Result<()> {
    let args = Cli::parse();

    tracing::info!(worker_id = args.worker_id, socket = %args.socket, "connecting");
    let stream = UnixStream::connect(&args.socket).await?;

    let worker = Worker::new(
        stream,
        args.worker_id,
        env!("CARGO_PKG_VERSION").to_string(),
    )
    .handshake()
    .await?;

    let shutting_down = match worker.await_work().await? {
        IdleNext::Learn(worker) => worker.execute_learn().await?,
        IdleNext::Work(worker) => worker.execute_and_report().await?,
    };
    shutting_down.teardown().await?;

    Ok(())
}
