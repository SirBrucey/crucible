mod error;
mod proxy;

use std::{io::Write, net::SocketAddr};

use clap::Parser;
use crucible_protocol::{Direction, Primitive};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::watch,
};

use crate::{
    error::{Error, Result},
    proxy::{Anchor, Gate, Proxy, Relay},
};

#[derive(Parser)]
#[command(about = "Multi-listener bytes-through proxy for a crucible fleet")]
struct Cli {
    /// Listen and upstream pair in the form `SERVICE=LISTEN=UPSTREAM`
    /// (e.g. `db=0.0.0.0:3306=db-actual:3306`). A pair connects a service to
    /// the proxy, and a service-to-service edge is two of them. Test traffic is
    /// driven and monitored here.
    #[arg(long = "pair", required = true)]
    pairs: Vec<String>,
    /// A side channel to the same services, in the same form, for faulting and
    /// observation. Nothing here is reported or frozen, so the test traffic is
    /// not infected by the framework's own.
    #[arg(long = "control")]
    control: Vec<String>,
    /// Fault anchor `SERVICE=DIRECTION=K` (DIRECTION is `c2u` or `u2c`): place
    /// the fault once `SERVICE` has forwarded `K` packets in that direction.
    #[arg(long = "fault-at")]
    fault_at: Option<String>,
    /// What to do to the fleet at the anchored packet. `kill` freezes and waits
    /// for the service to be killed and brought back from outside; `cut` severs
    /// the connection between the proxy and the two services.
    #[arg(long = "fault", default_value = "kill")]
    fault: Primitive,
}

/// What the proxy does to the fleet once the anchored packet is reached. The
/// network is frozen before the fault is placed and released immediately after,
/// to ensure the fault is placed deterministically.
///
/// Narrower than [`Primitive`], because this holds connections and nothing else.
/// A primitive it cannot place is refused as the proxy starts rather than when
/// the packet arrives.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Wait to be told the service has been killed and brought back. That
    /// happens outside the proxy, so the freeze is what holds the fleet for it.
    Kill,
    /// Sever the connection between the proxy and the two services, so both see
    /// the edge go away mid-request while their processes and state survive.
    Cut,
}

impl TryFrom<Primitive> for Fault {
    type Error = Error;

    fn try_from(primitive: Primitive) -> Result<Self> {
        match primitive {
            Primitive::Kill => Ok(Self::Kill),
            Primitive::Cut => Ok(Self::Cut),
            Primitive::Redeliver | Primitive::Reorder => Err(Error::UnplaceableFault { primitive }),
        }
    }
}

/// A parsed `--fault-at` anchor: place the fault once `service` forwards `k`
/// packets in `direction`.
struct FaultAt {
    service: String,
    direction: Direction,
    k: u32,
}

/// Whether some pair spec fronts `service` (the part before its first `=`).
fn is_fronted(service: &str, pairs: &[String]) -> bool {
    pairs
        .iter()
        .any(|spec| spec.split('=').next() == Some(service))
}

fn parse_fault_at(spec: &str) -> Result<FaultAt> {
    let mut parts = spec.splitn(3, '=');
    let (Some(service), Some(dir), Some(k)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(Error::MalformedFaultAt { spec: spec.into() });
    };
    let direction = match dir {
        "c2u" => Direction::ClientToUpstream,
        "u2c" => Direction::UpstreamToClient,
        _ => return Err(Error::MalformedFaultAt { spec: spec.into() }),
    };
    let k = k
        .parse()
        .map_err(|_| Error::MalformedFaultAt { spec: spec.into() })?;
    Ok(FaultAt {
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
    let fault = Fault::try_from(cli.fault)?;

    // The network every pair in this sidecar forwards on, frozen while a fault
    // is placed. SIGUSR1 arms the anchor at scenario start, so it counts only
    // scenario traffic and not bring-up.
    let (pause_tx, pause_rx) = watch::channel(false);
    // Fired at the anchored packet, so the fault runs at the point the schedule
    // named rather than wherever the fleet has got to since.
    let (trip_tx, trip_rx) = watch::channel(false);

    // Build the fault anchor (if any) once, so the same handle both counts on the
    // matching pair and is armed by the SIGUSR1 handler.
    let at = cli.fault_at.as_deref().map(parse_fault_at).transpose()?;
    // A fault-at must name a service some pair fronts. Otherwise it would
    // silently never count (k>0, no pair increments it) or, on arm, freeze the
    // whole fleet for a target that is not even fronted (k=0).
    if let Some(at) = &at
        && !is_fronted(&at.service, &cli.pairs)
    {
        return Err(Error::UnknownFaultService {
            service: at.service.clone(),
        });
    }
    // Bumped to sever the anchored pairs. A service may be fronted on several
    // ports, and the anchor counts across all of them.
    let (sever_tx, sever_rx) = watch::channel(0u64);
    let anchor = at
        .as_ref()
        .map(|at| Anchor::new(at.direction, at.k, pause_tx.clone(), trip_tx));

    for spec in cli.pairs {
        let Pair {
            service,
            listen,
            upstream,
        } = Pair::parse(&spec)?;
        // Only the pairs fronting the anchored service count toward the fault,
        // and only they can be severed by it.
        let anchored = at.as_ref().is_some_and(|at| at.service == service);
        let severings = if anchored {
            sever_rx.clone()
        } else {
            watch::channel(0u64).1
        };
        let gate = Gate::new(pause_rx.clone(), severings);
        let pair_anchor = anchored.then(|| anchor.clone()).flatten();
        let (proxy, _local_addr, mut events) =
            Proxy::bind(listen, upstream.clone(), gate, pair_anchor).await?;
        tracing::info!(%service, %listen, %upstream, "proxy pair up");

        tokio::spawn(async move {
            if let Err(e) = proxy.run().await {
                tracing::error!(?e, "proxy accept loop ended");
            }
        });
        // One process fronts the whole fleet, so every pair tags its events with
        // its service; the runner attributes the interleaved stream by that tag.
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

    spawn_fault_control(fault, anchor, trip_rx, pause_tx, sever_tx);

    for spec in cli.control {
        let Pair {
            service,
            listen,
            upstream,
        } = Pair::parse(&spec)?;
        let relay = Relay::bind(listen, upstream.clone()).await?;
        tracing::info!(%service, %listen, %upstream, "control pair up");
        tokio::spawn(async move {
            if let Err(e) = relay.run().await {
                tracing::error!(?e, "control accept loop ended");
            }
        });
    }

    std::future::pending::<()>().await;
    Ok(())
}

/// One listener and where it forwards to, as `SERVICE=LISTEN=UPSTREAM`.
struct Pair {
    service: String,
    listen: SocketAddr,
    upstream: String,
}

impl Pair {
    fn parse(spec: &str) -> Result<Self> {
        let mut parts = spec.splitn(3, '=');
        let (Some(service), Some(listen), Some(upstream)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(Error::MalformedPair {
                pair: spec.to_string(),
            });
        };
        Ok(Self {
            service: service.to_string(),
            listen: listen.parse().map_err(|source| Error::ParseListen {
                addr: listen.to_string(),
                source,
            })?,
            upstream: upstream.to_string(),
        })
    }
}

/// Drive the fault for the lifetime of the process: arm the anchor when the
/// scenario starts (SIGUSR1), and once the anchored packet passes, freeze the
/// network, place the fault, and release it.
///
/// Holds a `watch::Sender`, so it must outlive the proxies; it never returns.
fn spawn_fault_control(
    fault: Fault,
    anchor: Option<Anchor>,
    mut trip: watch::Receiver<bool>,
    pause: watch::Sender<bool>,
    sever: watch::Sender<u64>,
) {
    tokio::spawn(async move {
        let (mut arm, mut proceed, mut abandon) = match (
            signal(SignalKind::user_defined1()),
            signal(SignalKind::user_defined2()),
            signal(SignalKind::hangup()),
        ) {
            (Ok(arm), Ok(proceed), Ok(abandon)) => (arm, proceed, abandon),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                tracing::error!(?e, "failed to install the fault-control signal handlers");
                return;
            }
        };
        loop {
            tokio::select! {
                _ = arm.recv() => if let Some(anchor) = &anchor {
                    anchor.arm();
                    tracing::info!("anchor armed at scenario start (SIGUSR1)");
                } else {
                    tracing::debug!("SIGUSR1 with no anchor; ignored");
                },
                Ok(()) = trip.changed() => {
                    if !*trip.borrow() {
                        continue;
                    }
                    match fault {
                        // The service is killed and brought back from outside,
                        // which says so by signalling; until then the fleet
                        // waits, so nothing runs against a half-dead service.
                        // Abandoning says the kill never landed, and releases
                        // just the same.
                        Fault::Kill => tokio::select! {
                            _ = proceed.recv() => tracing::info!("the kill has landed (SIGUSR2)"),
                            _ = abandon.recv() => tracing::info!("the kill was abandoned (SIGHUP)"),
                        },
                        Fault::Cut => {
                            // FIXME: propagate once this loop can report
                            // failure. Every connection on the pair holds a
                            // receiver, so this cannot fail.
                            sever
                                .send(*sever.borrow() + 1)
                                .expect("the sever watch has live receivers");
                            tracing::info!("severed the anchored pairs");
                        }
                    }
                    release(anchor.as_ref(), &pause);
                }
                // Nothing was placed: the scenario ended before the anchored
                // packet, or the wait for it timed out.
                _ = abandon.recv() => release(anchor.as_ref(), &pause),
                _ = proceed.recv() => tracing::debug!("SIGUSR2 with nothing held; ignored"),
            }
        }
    });
}

/// Let the fleet go, and stop the anchor placing a second fault behind this one.
// FIXME: propagate once the fault-control loop can report failure. main holds a
// pause receiver for the process lifetime, so this cannot fail; a silent failure
// would leave the fleet frozen with nothing left to release it.
fn release(anchor: Option<&Anchor>, pause: &watch::Sender<bool>) {
    if let Some(anchor) = anchor {
        anchor.disarm();
    }
    pause.send(false).expect("pause watch has a live receiver");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_client_to_upstream_anchor() {
        let at = parse_fault_at("db=c2u=3").expect("valid spec");
        assert_eq!(at.service, "db");
        assert_eq!(at.direction, Direction::ClientToUpstream);
        assert_eq!(at.k, 3);
    }

    #[test]
    fn parses_an_upstream_to_client_anchor() {
        let at = parse_fault_at("api=u2c=0").expect("valid spec");
        assert_eq!(at.service, "api");
        assert_eq!(at.direction, Direction::UpstreamToClient);
        assert_eq!(at.k, 0);
    }

    #[test]
    fn rejects_an_unknown_direction() {
        assert!(parse_fault_at("db=sideways=3").is_err());
    }

    #[test]
    fn rejects_a_non_numeric_k() {
        assert!(parse_fault_at("db=c2u=three").is_err());
    }

    #[test]
    fn rejects_a_missing_field() {
        assert!(parse_fault_at("db=c2u").is_err());
    }

    #[test]
    fn a_fault_service_must_front_a_pair() {
        let pairs = vec!["db=0.0.0.0:3306=db-actual:3306".to_string()];
        assert!(is_fronted("db", &pairs));
        assert!(!is_fronted("api", &pairs));
    }
}
