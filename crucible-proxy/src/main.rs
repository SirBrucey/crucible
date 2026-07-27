mod error;
mod proxy;

use std::{io::Write, net::SocketAddr};

use clap::Parser;
use tokio::{
    net::lookup_host,
    signal::unix::{SignalKind, signal},
    sync::watch,
};

use crate::{
    error::{Error, Result},
    proxy::Proxy,
};

#[derive(Parser)]
#[command(about = "Multi-listener bytes-through proxy for a crucible fleet")]
struct Cli {
    /// Listen and upstream pair in the form `LISTEN=UPSTREAM`
    /// (e.g. `0.0.0.0:3306=db-actual:3306`).
    #[arg(long = "pair", required = true)]
    pairs: Vec<String>,
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

    let cli = Cli::parse();

    // One pause gate shared by every pair in this sidecar. The control plane is
    // signals: SIGUSR1 holds all forwarding (freeze the flow so a fault can be
    // injected against a fixed state), SIGUSR2 releases it. Both are latency
    // tolerant because nothing crosses the proxy while paused.
    let (pause_tx, pause_rx) = watch::channel(false);
    spawn_pause_control(pause_tx);

    for spec in cli.pairs {
        let (listen_str, upstream_str) = spec
            .split_once('=')
            .ok_or_else(|| Error::MalformedPair { pair: spec.clone() })?;
        let listen: SocketAddr = listen_str.parse().map_err(|source| Error::ParseListen {
            addr: listen_str.to_string(),
            source,
        })?;
        let upstream =
            lookup_host(upstream_str)
                .await?
                .next()
                .ok_or_else(|| Error::UpstreamUnresolved {
                    upstream: upstream_str.to_string(),
                })?;
        let (proxy, _local_addr, mut events) =
            Proxy::bind(listen, upstream, pause_rx.clone()).await?;
        tracing::info!(%listen, %upstream, "proxy pair up");

        tokio::spawn(async move {
            if let Err(e) = proxy.run().await {
                tracing::error!(?e, "proxy accept loop ended");
            }
        });
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match serde_json::to_string(&event) {
                    Ok(line) => {
                        println!("{line}");
                        let _ = std::io::stdout().flush();
                    }
                    Err(e) => tracing::error!(?e, "serialize conn event"),
                }
            }
        });
    }

    std::future::pending::<()>().await;
    Ok(())
}

/// Listen for SIGUSR1 (pause) and SIGUSR2 (resume) for the lifetime of the
/// process, driving the shared pause gate. Holds the sole `watch::Sender`, so it
/// must outlive the proxies; it never returns.
fn spawn_pause_control(pause_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        let mut pause = match signal(SignalKind::user_defined1()) {
            Ok(sig) => sig,
            Err(e) => {
                tracing::error!(?e, "failed to install SIGUSR1 handler");
                return;
            }
        };
        let mut resume = match signal(SignalKind::user_defined2()) {
            Ok(sig) => sig,
            Err(e) => {
                tracing::error!(?e, "failed to install SIGUSR2 handler");
                return;
            }
        };
        loop {
            tokio::select! {
                _ = pause.recv() => {
                    let _ = pause_tx.send(true);
                    tracing::info!("forwarding paused (SIGUSR1)");
                }
                _ = resume.recv() => {
                    let _ = pause_tx.send(false);
                    tracing::info!("forwarding resumed (SIGUSR2)");
                }
            }
        }
    });
}
