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

    /// The runner invocation this worker belongs to.
    #[arg(long, env = "CRUCIBLE_RUN_ID")]
    pub run_id: u32,
}
