mod error;
mod proxy;

use std::{io::Write, net::SocketAddr};

use clap::Parser;
use crucible_protocol::Direction;
use tokio::{
    net::lookup_host,
    signal::unix::{SignalKind, signal},
    sync::watch,
};

use crate::{
    error::{Error, Result},
    proxy::{Anchor, Proxy},
};

#[derive(Parser)]
#[command(about = "Multi-listener bytes-through proxy for a crucible fleet")]
struct Cli {
    /// Listen and upstream pair in the form `SERVICE=LISTEN=UPSTREAM`
    /// (e.g. `db=0.0.0.0:3306=db-actual:3306`).
    #[arg(long = "pair", required = true)]
    pairs: Vec<String>,
    /// Fault anchor `SERVICE=DIRECTION=K` (DIRECTION is `c2u` or `u2c`): freeze
    /// the fleet once `SERVICE` has forwarded `K` packets on that direction.
    #[arg(long = "freeze-at")]
    freeze_at: Option<String>,
}

/// A parsed `--freeze-at` anchor: freeze once `service` forwards `k` packets on
/// `direction`.
struct FreezeAt {
    service: String,
    direction: Direction,
    k: u32,
}

fn parse_freeze_at(spec: &str) -> Result<FreezeAt> {
    let mut parts = spec.splitn(3, '=');
    let (Some(service), Some(dir), Some(k)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(Error::MalformedFreezeAt { spec: spec.into() });
    };
    let direction = match dir {
        "c2u" => Direction::ClientToUpstream,
        "u2c" => Direction::UpstreamToClient,
        _ => return Err(Error::MalformedFreezeAt { spec: spec.into() }),
    };
    let k = k
        .parse()
        .map_err(|_| Error::MalformedFreezeAt { spec: spec.into() })?;
    Ok(FreezeAt {
        service: service.into(),
        direction,
        k,
    })
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

    // One pause gate shared by every pair in this sidecar. Control is by signal:
    // SIGUSR1 arms the fault anchor at scenario start (so it counts only scenario
    // traffic, not bring-up), and SIGUSR2 releases the freeze after the kill.
    let (pause_tx, pause_rx) = watch::channel(false);

    // Build the fault anchor (if any) once, so the same handle both counts on the
    // matching pair and is armed by the SIGUSR1 handler.
    let freeze = cli.freeze_at.as_deref().map(parse_freeze_at).transpose()?;
    let anchor = freeze
        .as_ref()
        .map(|f| Anchor::new(f.direction, f.k, pause_tx.clone()));
    spawn_pause_control(pause_tx.clone(), anchor.clone());

    for spec in cli.pairs {
        // `SERVICE=LISTEN=UPSTREAM`. One process fronts the whole fleet, so every
        // pair tags its events with its service; the runner attributes the
        // interleaved stream by that tag.
        let mut parts = spec.splitn(3, '=');
        let (Some(service), Some(listen_str), Some(upstream_str)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(Error::MalformedPair { pair: spec.clone() });
        };
        let service = service.to_string();
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
        // Only the pair fronting the anchored service counts toward the freeze.
        let pair_anchor = if freeze.as_ref().is_some_and(|f| f.service == service) {
            anchor.clone()
        } else {
            None
        };
        let (proxy, _local_addr, mut events) =
            Proxy::bind(listen, upstream, pause_rx.clone(), pair_anchor).await?;
        tracing::info!(%service, %listen, %upstream, "proxy pair up");

        tokio::spawn(async move {
            if let Err(e) = proxy.run().await {
                tracing::error!(?e, "proxy accept loop ended");
            }
        });
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match serde_json::to_string(&event) {
                    Ok(line) => {
                        println!("{service}\t{line}");
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

/// Listen for SIGUSR1 (arm the anchor at scenario start) and SIGUSR2 (release
/// the freeze) for the lifetime of the process. Holds a `watch::Sender`, so it
/// must outlive the proxies; it never returns.
fn spawn_pause_control(pause_tx: watch::Sender<bool>, anchor: Option<Anchor>) {
    tokio::spawn(async move {
        let mut arm = match signal(SignalKind::user_defined1()) {
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
                _ = arm.recv() => {
                    if let Some(anchor) = &anchor {
                        anchor.arm();
                        tracing::info!("anchor armed at scenario start (SIGUSR1)");
                    } else {
                        tracing::debug!("SIGUSR1 with no anchor; ignored");
                    }
                }
                _ = resume.recv() => {
                    let _ = pause_tx.send(false);
                    tracing::info!("forwarding resumed (SIGUSR2)");
                }
            }
        }
    });
}
