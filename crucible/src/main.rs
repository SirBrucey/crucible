mod error;

use std::{path::PathBuf, process::Stdio, time::Duration};

use crucible_core::ipc::{
    RunnerToWorker, WorkerToRunner,
    codec::{read_frame, write_frame},
};
use tokio::{net::UnixListener, process::Command, time::timeout};

use crate::error::{Error, Result};

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

    let listener = UnixListener::bind(&socket_path)?;
    eprintln!("runner listening on {}", socket_path.display());

    let mut command = Command::new(worker_bin_path()?);
    command
        .arg("--socket")
        .arg(&socket_path)
        .arg("--worker-id")
        .arg("0")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
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
    eprintln!("runner spawned worker pid {child_pid}");

    let (mut stream, _addr) = timeout(HANDSHAKE_TIMEOUT, listener.accept())
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    eprintln!("runner accepted connection");

    let hello: WorkerToRunner = timeout(HANDSHAKE_TIMEOUT, read_frame(&mut stream))
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    match hello {
        WorkerToRunner::Hello {
            worker_version,
            worker_id,
        } => {
            eprintln!("runner received HELLO from worker {worker_id} version {worker_version}");
        }
        other => panic!("expected Hello during handshake, got {other:?}"),
    }

    write_frame(
        &mut stream,
        &RunnerToWorker::HelloAck {
            runner_version: env!("CARGO_PKG_VERSION").to_string(),
        },
    )
    .await?;
    eprintln!("runner sent HELLO_ACK");

    let status = child.wait().await?;
    eprintln!("worker exited: {status}");
    let _ = tokio::fs::remove_file(&socket_path).await;

    if status.success() {
        Ok(())
    } else {
        Err(Error::WorkerExitedNonZero(status))
    }
}
