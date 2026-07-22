mod error;
mod session;

use std::{path::PathBuf, process::Stdio, time::Duration};

use crucible_core::{
    event_bus::EventBus,
    journal,
    scheduler::{RandomScheduler, Scheduler},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixListener,
    process::Command,
    time::timeout,
};

use crate::{
    error::{Error, Result},
    session::{DispatchNext, Session},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

fn worker_bin_path() -> Result<PathBuf> {
    let runner = std::env::current_exe()?;
    let mut path = runner
        .parent()
        .ok_or(Error::RunnerExeParentless)?
        .to_path_buf();
    path.push("crucible-worker");
    Ok(path)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let result = run().await;
    if let Err(e) = &result {
        tracing::error!(error = %e, "runner exiting with error");
    }
    result
}

async fn run() -> Result<()> {
    let socket_path = PathBuf::from(format!("/tmp/crucible-{}.sock", std::process::id()));
    let _ = tokio::fs::remove_file(&socket_path).await;

    let (bus, journal_rx) = EventBus::new();

    let journal_path = journal::default_path(std::process::id());
    tracing::info!(path = %journal_path.display(), "journal ready");
    let journal_task = tokio::spawn(journal::run(journal_rx, journal_path));

    let mut observer_rx = bus.subscribe();
    let observer_task = tokio::spawn(async move {
        while let Ok(event) = observer_rx.recv().await {
            tracing::info!(target: "observer", event = ?event, "");
        }
    });

    let listener = UnixListener::bind(&socket_path)?;
    tracing::info!(socket = %socket_path.display(), "listening");

    let mut command = Command::new(worker_bin_path()?);
    command
        .arg("--socket")
        .arg(&socket_path)
        .arg("--worker-id")
        .arg("0")
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped());
    // SAFETY: `pre_exec` runs after `fork()` and before `exec()` in the child.
    // `prctl(PR_SET_PDEATHSIG, SIGKILL)` sets a per-process flag with no aliasing
    // or shared-state concerns, and its side effect (kill worker if runner dies)
    // is exactly the intent.
    unsafe {
        command.pre_exec(|| {
            let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            if ret == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    let mut child = command.spawn()?;
    let child_pid = child.id().ok_or(Error::ChildPidMissing)?;
    tracing::info!(pid = child_pid, "spawned worker");

    let worker_stderr = child
        .stderr
        .take()
        .expect("stderr set to piped so child has one");
    let stderr_relay = tokio::spawn(async move {
        let mut lines = BufReader::new(worker_stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("{line}");
        }
    });

    let workload = drive(&listener, &bus).await;

    let status = child.wait().await?;
    tracing::info!(?status, "worker exited");
    let _ = tokio::fs::remove_file(&socket_path).await;

    drop(bus);
    journal_task.await.expect("journal task should not panic")?;
    let _ = observer_task.await;
    let _ = stderr_relay.await;

    workload?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::WorkerExitedNonZero(status))
    }
}

async fn drive(listener: &UnixListener, bus: &EventBus) -> Result<()> {
    let (stream, _addr) = timeout(HANDSHAKE_TIMEOUT, listener.accept())
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    tracing::info!("accepted worker connection");

    let session = timeout(
        HANDSHAKE_TIMEOUT,
        Session::new(stream, env!("CARGO_PKG_VERSION").to_string()).handshake(bus),
    )
    .await
    .map_err(|_| Error::HandshakeTimeout)??;

    let (mut session, catalogue) = session.learn(bus).await?;
    tracing::info!(count = catalogue.len(), "session catalogue received");

    let mut scheduler = RandomScheduler::new(3);
    loop {
        session = match session.dispatch(bus, scheduler.next()).await? {
            DispatchNext::More(session) => session.await_result(bus).await?,
            DispatchNext::Done => break,
        };
    }
    Ok(())
}
