/// Crucible worker process CLI.
#[derive(Debug, clap::Parser)]
#[command(version)]
pub(crate) struct Cli {
    /// Unix socket to connect back to the runner.
    ///
    /// Accepts a filesystem path (`/tmp/crucible.sock`) or an abstract-namespace
    /// address prefixed with `@` (`@crucible.<invocation>.<id>`).
    #[arg(long, env = "CRUCIBLE_SOCKET")]
    pub socket: String,

    /// This worker's identifier within the runner's pool.
    #[arg(long, env = "CRUCIBLE_WORKER_ID")]
    pub worker_id: u32,
}
