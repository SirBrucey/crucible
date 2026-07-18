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
    let socket_path = PathBuf::from(format!("/tmp/crucible-{}.sock", std::process::id()));
    let _ = tokio::fs::remove_file(&socket_path).await;

    let (bus, journal_rx) = EventBus::new();

    let journal_path = journal::default_path(std::process::id());
    eprintln!("[runner] journal at {}", journal_path.display());
    let journal = tokio::spawn(journal::run(journal_rx, journal_path));

    // Demo observer on the broadcast side.
    let mut observer_rx = bus.subscribe();
    let observer = tokio::spawn(async move {
        while let Ok(event) = observer_rx.recv().await {
            eprintln!("[observer] {event:?}");
        }
    });

    let listener = UnixListener::bind(&socket_path)?;
    eprintln!("[runner] listening on {}", socket_path.display());

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
    eprintln!("[runner] spawned worker pid {child_pid}");

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

    let (stream, _addr) = timeout(HANDSHAKE_TIMEOUT, listener.accept())
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    eprintln!("[runner] accepted connection");

    let mut session = timeout(
        HANDSHAKE_TIMEOUT,
        Session::new(stream, env!("CARGO_PKG_VERSION").to_string()).handshake(&bus),
    )
    .await
    .map_err(|_| Error::HandshakeTimeout)??;

    let mut scheduler = RandomScheduler::new(3);
    loop {
        session = match session.dispatch(&bus, scheduler.next()).await? {
            DispatchNext::More(session) => session.await_result(&bus).await?,
            DispatchNext::Done => break,
        };
    }

    let status = child.wait().await?;
    eprintln!("[runner] worker exited: {status}");
    let _ = tokio::fs::remove_file(&socket_path).await;

    // Shutdown bus: drop it, then wait for journal + observer to drain and exit.
    drop(bus);
    journal.await.expect("journal task should not panic")?;
    let _ = observer.await;
    let _ = stderr_relay.await;

    if status.success() {
        Ok(())
    } else {
        Err(Error::WorkerExitedNonZero(status))
    }
}
