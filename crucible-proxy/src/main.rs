mod error;
mod proxy;

use std::net::SocketAddr;

use clap::Parser;
use tokio::net::lookup_host;

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
        .init();

    let cli = Cli::parse();

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
        let (proxy, _local_addr, mut events) = Proxy::bind(listen, upstream).await?;
        tracing::info!(%listen, %upstream, "proxy pair up");

        tokio::spawn(async move {
            if let Err(e) = proxy.run().await {
                tracing::error!(?e, "proxy accept loop ended");
            }
        });
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                tracing::info!(?event, "conn event");
            }
        });
    }

    std::future::pending::<()>().await;
    Ok(())
}
