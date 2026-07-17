mod error;

use std::{path::PathBuf, process::Stdio, time::Duration};

use crucible_core::{
    event_bus::{EventBus, RunnerEvent},
    ipc::{
        RunnerToWorker, WorkerToRunner,
        codec::{read_frame, write_frame},
    },
    journal,
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
    eprintln!("[runner] spawned worker pid {child_pid}");

    let (mut stream, _addr) = timeout(HANDSHAKE_TIMEOUT, listener.accept())
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    eprintln!("[runner] accepted connection");

    let hello: WorkerToRunner = timeout(HANDSHAKE_TIMEOUT, read_frame(&mut stream))
        .await
        .map_err(|_| Error::HandshakeTimeout)??;
    let worker_id = match &hello {
        WorkerToRunner::Hello { worker_id, .. } => *worker_id,
        other => panic!("expected Hello during handshake, got {other:?}"),
    };
    bus.publish(RunnerEvent::WorkerMessage {
        worker_id,
        message: hello,
    })
    .await
    .expect("journal receiver alive");

    let ack = RunnerToWorker::HelloAck {
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    write_frame(&mut stream, &ack).await?;
    bus.publish(RunnerEvent::RunnerMessage {
        worker_id,
        message: ack,
    })
    .await
    .expect("journal receiver alive");

    let ready = read_frame::<WorkerToRunner, _>(&mut stream).await?;
    if !matches!(&ready, WorkerToRunner::Ready) {
        panic!("expected Ready, got {ready:?}");
    }
    bus.publish(RunnerEvent::WorkerMessage {
        worker_id,
        message: ready,
    })
    .await
    .expect("journal receiver alive");

    let schedule = RunnerToWorker::Schedule {
        schedule_id: 0,
        payload: Vec::new(),
    };
    write_frame(&mut stream, &schedule).await?;
    bus.publish(RunnerEvent::RunnerMessage {
        worker_id,
        message: schedule,
    })
    .await
    .expect("journal receiver alive");

    let run_result = read_frame::<WorkerToRunner, _>(&mut stream).await?;
    if !matches!(&run_result, WorkerToRunner::RunResult { .. }) {
        panic!("expected RunResult, got {run_result:?}");
    }
    bus.publish(RunnerEvent::WorkerMessage {
        worker_id,
        message: run_result,
    })
    .await
    .expect("journal receiver alive");

    let status = child.wait().await?;
    eprintln!("[runner] worker exited: {status}");
    let _ = tokio::fs::remove_file(&socket_path).await;

    // Shutdown bus: drop it, then wait for journal + observer to drain and exit.
    drop(bus);
    journal.await.expect("journal task should not panic")?;
    let _ = observer.await;

    if status.success() {
        Ok(())
    } else {
        Err(Error::WorkerExitedNonZero(status))
    }
}
