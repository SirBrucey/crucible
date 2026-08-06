/// Crucible worker process CLI.
#[derive(Debug, clap::Parser)]
#[command(version)]
pub(crate) struct Cli {
    /// Unix socket to connect back to the runner.
    #[arg(long, env = "CRUCIBLE_SOCKET")]
    pub socket: String,

    /// This worker's identifier within the runner's pool.
    #[arg(long, env = "CRUCIBLE_WORKER_ID")]
    pub worker_id: u32,
}
