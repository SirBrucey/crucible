mod error;
mod proxy;

use std::{io::Write, net::SocketAddr};

use clap::Parser;
use crucible_protocol::Direction;
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
    fault: Fault,
}

/// What the proxy does to the fleet once the anchored packet is reached. The
/// network is frozen before the fault is placed and released immediately after,
/// to ensure the fault is placed deterministically.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Fault {
    /// Wait to be told the service has been killed and brought back. That
    /// happens outside the proxy, so the freeze is what holds the fleet for it.
    Kill,
    /// Sever the connection between the proxy and the two services, so both see
    /// the edge go away mid-request while their processes and state survive.
    Cut,
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
    let anchor = at
        .as_ref()
        .map(|at| Anchor::new(at.direction, at.k, trip_tx));
    let mut severs = None;

    for spec in cli.pairs {
        let Pair {
            service,
            listen,
            upstream,
        } = Pair::parse(&spec)?;
        // Only the pair fronting the anchored service counts toward the fault.
        let (cut_tx, cut_rx) = watch::channel(false);
        let pair_anchor = if at.as_ref().is_some_and(|at| at.service == service) {
            severs = Some(cut_tx);
            anchor.clone()
        } else {
            None
        };
        let gate = Gate::new(pause_rx.clone(), cut_rx);
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

    spawn_fault_control(cli.fault, anchor, trip_rx, pause_tx, severs);

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
    severs: Option<watch::Sender<bool>>,
) {
    tokio::spawn(async move {
        let (mut arm, mut killed) = match (
            signal(SignalKind::user_defined1()),
            signal(SignalKind::user_defined2()),
        ) {
            (Ok(arm), Ok(killed)) => (arm, killed),
            (Err(e), _) | (_, Err(e)) => {
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
                    freeze(&pause, true);
                    match fault {
                        // The service is killed and brought back from outside,
                        // which says so by signalling; until then the fleet
                        // waits, so nothing runs against a half-dead service.
                        Fault::Kill => {
                            killed.recv().await;
                            tracing::info!("the service was killed and is back (SIGUSR2)");
                        }
                        Fault::Cut => if let Some(cut) = &severs {
                            // FIXME: propagate once this loop can report
                            // failure. Every connection on the pair holds a
                            // receiver, so this cannot fail.
                            cut.send(true).expect("the cut watch has live receivers");
                            tracing::info!("severed the anchored pair");
                        } else {
                            tracing::warn!("nothing to sever; the fault did nothing");
                        },
                    }
                    // The anchor is one-shot, so disarm before releasing: a late
                    // packet cannot place a second fault behind this one.
                    if let Some(anchor) = &anchor {
                        anchor.disarm();
                    }
                    freeze(&pause, false);
                }
                // No fault was placed, so this is a give-up path: the scenario
                // ended before the anchored packet, or the wait timed out.
                _ = killed.recv() => {
                    if let Some(anchor) = &anchor {
                        anchor.disarm();
                    }
                    freeze(&pause, false);
                }
            }
        }
    });
}

/// Hold the network still, or let it go.
// FIXME: propagate once the fault-control loop can report failure. main holds a
// pause receiver for the process lifetime, so this cannot fail; a silent failure
// would leave the fleet frozen with nothing left to release it.
fn freeze(pause: &watch::Sender<bool>, held: bool) {
    pause.send(held).expect("pause watch has a live receiver");
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
