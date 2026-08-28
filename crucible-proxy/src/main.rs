mod error;
mod proxy;

use std::{
    collections::HashSet,
    io::Write,
    net::SocketAddr,
    sync::{Arc, OnceLock},
};

use clap::Parser;
use crucible_protocol::{Direction, Primitive};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::watch,
};

use crate::{
    error::{Error, Result},
    proxy::{Anchor, Gate, OnEdge, Proxy, Relay, Upstream},
};

#[derive(Parser)]
#[command(about = "Multi-listener bytes-through proxy for a crucible fleet")]
struct Cli {
    /// Listen and upstream pair in the form `SERVICE=KIND=LISTEN=UPSTREAM`
    /// (e.g. `db=mariadb=0.0.0.0:3306=db-actual:3306`). A pair connects a
    /// service to the proxy, and a service-to-service edge is two of them. Test
    /// traffic is driven and monitored here. KIND is what the service speaks,
    /// which decides what the proxy can make of the bytes crossing it.
    #[arg(long = "pair", required = true)]
    pairs: Vec<String>,
    /// A side channel to the same services, in the same form, for faulting and
    /// observation. Nothing here is reported or frozen, so the test traffic is
    /// not infected by the framework's own.
    #[arg(long = "control")]
    control: Vec<String>,
    /// Fault anchor `CLIENT>UPSTREAM=DIRECTION=MARK` (DIRECTION is `c2u` or
    /// `u2c`): place the fault when the kind plugin reading that direction sees
    /// the moment MARK names. An empty CLIENT (`>db=c2u=publish:1:after`) is the
    /// edge dialled from outside the fleet.
    #[arg(long = "fault-at")]
    fault_at: Option<String>,
    /// What to do to the fleet at the anchored packet. `kill` freezes and waits
    /// for the service to be killed and brought back from outside; `cut` severs
    /// the connection between the proxy and the two services.
    #[arg(long = "fault", default_value = "kill")]
    fault: Primitive,
    /// Hold the edge `CLIENT>UPSTREAM` down from scenario start until released,
    /// so the fleet runs degraded rather than meeting one instantaneous fault.
    #[arg(long = "degrade")]
    degrade: Option<String>,
}

/// What the proxy does to the fleet at the moment a schedule names.
///
/// Two sorts, and the difference is where the fault happens. Taking something
/// away happens outside the byte stream, so the network is frozen first and
/// released after, and the fault lands on the moment however long it takes.
/// Changing what crosses happens in the stream itself, as the bytes pass, so
/// there is nothing to hold.
///
/// Narrower than [`Primitive`], which names what is done rather than where.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Wait to be told the service has been killed and brought back. That
    /// happens outside the proxy, so the freeze is what holds the fleet for it.
    Kill,
    /// Sever the connection between the proxy and the two services, so both see
    /// the edge go away mid-request while their processes and state survive.
    Cut,
    /// Leave it to the plugin reading the edge, which changes what crosses as
    /// it passes.
    Rewritten,
}

impl From<Primitive> for Fault {
    fn from(primitive: Primitive) -> Self {
        match primitive {
            Primitive::Kill => Self::Kill,
            Primitive::Cut => Self::Cut,
            Primitive::Redeliver | Primitive::Reorder => Self::Rewritten,
        }
    }
}

/// A parsed `--fault-at` anchor: place the fault when the edge from `client` to
/// `upstream` reaches the moment `mark` names, on `direction`.
struct FaultAt {
    client: Option<String>,
    upstream: String,
    direction: Direction,
    mark: String,
}

/// How many of the moments an edge offers must pass before the one `at` names.
///
/// A fault-at must name a service some pair fronts, or it would silently never
/// reach its moment, or freeze the whole fleet for a target that is not even
/// fronted. What that pair speaks is what its mark means.
fn nth_named(at: Option<&FaultAt>, pairs: &[Pair]) -> Result<u32> {
    let Some(at) = at else {
        return Ok(1);
    };
    let pair = fronting(&at.upstream, pairs).ok_or_else(|| Error::UnknownFaultService {
        service: at.upstream.clone(),
    })?;
    Ok(crucible_kind::nth(&pair.kind, &at.mark))
}

/// The pair fronting `service`, if any pair does.
fn fronting<'a>(service: &str, pairs: &'a [Pair]) -> Option<&'a Pair> {
    pairs.iter().find(|pair| pair.service == service)
}

/// The service each pair fronts, and the host it forwards to, which is where
/// that service answers.
fn fronted(pairs: &[Pair]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|pair| {
            // The upstream is `host:port`; only the host names the container.
            let host = pair
                .upstream
                .rsplit_once(':')
                .map_or(pair.upstream.as_str(), |(host, _)| host);
            (pair.service.clone(), host.to_owned())
        })
        .collect()
}

/// An edge as `CLIENT>UPSTREAM`, where an empty CLIENT was dialled from outside
/// the fleet.
fn parse_edge(spec: &str) -> Result<(Option<String>, String)> {
    let (client, upstream) = spec
        .split_once('>')
        .ok_or_else(|| Error::MalformedFaultAt { spec: spec.into() })?;
    Ok((
        (!client.is_empty()).then(|| client.to_owned()),
        upstream.to_owned(),
    ))
}

fn parse_fault_at(spec: &str) -> Result<FaultAt> {
    let malformed = || Error::MalformedFaultAt { spec: spec.into() };
    let mut parts = spec.splitn(3, '=');
    let (Some(edge), Some(dir), Some(mark)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(malformed());
    };
    let (client, upstream) = parse_edge(edge)?;
    let direction = match dir {
        "c2u" => Direction::ClientToUpstream,
        "u2c" => Direction::UpstreamToClient,
        _ => return Err(malformed()),
    };
    Ok(FaultAt {
        client,
        upstream,
        direction,
        mark: mark.to_owned(),
    })
}

/// Every address the fleet's services answer at, and those of `client` alone.
///
/// Read as the scenario starts, when the fleet is up and its names resolve.
/// Nothing is looked up while bytes are moving.
async fn resolve(hosts: &[(String, String)], client: Option<&str>) -> OnEdge {
    let mut fleet = HashSet::new();
    let mut theirs = HashSet::new();
    for (service, host) in hosts {
        // A host with no port does not resolve, and which port it answers on
        // does not change which container it is.
        let Ok(addrs) = tokio::net::lookup_host((host.as_str(), 0)).await else {
            tracing::warn!(%service, %host, "did not resolve, so its traffic is not recognised");
            continue;
        };
        for addr in addrs {
            fleet.insert(addr.ip());
            if client == Some(service.as_str()) {
                theirs.insert(addr.ip());
            }
        }
    }
    // What the proxy decided the edge is, which every later attribution rests
    // on.
    tracing::debug!(
        client = client.unwrap_or("outside the fleet"),
        theirs = ?theirs,
        fleet = ?fleet,
        "resolved the anchored edge"
    );
    OnEdge::new(theirs, fleet, client.is_none())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        // Nothing reads this but the run's log, which relays it. Colour codes
        // would arrive as escape sequences in the middle of it.
        .with_ansi(false)
        .init();

    let cli = Cli::parse();
    let fault = Fault::from(cli.fault);

    // The network every pair in this sidecar forwards on, frozen while a fault
    // is placed. SIGUSR1 arms the anchor at scenario start, so it counts only
    // scenario traffic and not bring-up.
    let (pause_tx, pause_rx) = watch::channel(false);
    // Fired at the anchored packet, so the fault runs at the point the schedule
    // named rather than wherever the fleet has got to since.
    let (trip_tx, trip_rx) = watch::channel(false);

    // Everything the proxy fronts, read before anything binds so a malformed
    // pair is refused rather than met halfway through bring-up.
    let pairs = cli
        .pairs
        .iter()
        .map(|spec| Pair::parse(spec))
        .collect::<Result<Vec<_>>>()?;

    // Build the fault anchor (if any) once, so the same handle both counts on the
    // matching pair and is armed by the SIGUSR1 handler.
    let at = cli.fault_at.as_deref().map(parse_fault_at).transpose()?;
    let nth = nth_named(at.as_ref(), &pairs)?;
    // Bumped to sever the anchored pairs. A service may be fronted on several
    // ports, and the anchor counts across all of them.
    let (sever_tx, sever_rx) = watch::channel(0u64);
    // Held down for the whole scenario, rather than severed on one packet.
    let (down_tx, down_rx) = watch::channel(false);
    let degrade = cli.degrade.as_deref().map(parse_edge).transpose()?;
    if let Some((_, upstream)) = &degrade
        && fronting(upstream, &pairs).is_none()
    {
        return Err(Error::UnknownFaultService {
            service: upstream.clone(),
        });
    }
    // Where each service answers, which is what names the far end of a
    // connection. Taken before the pairs are consumed below.
    let hosts = fronted(&pairs);
    // Which connections the fault applies to. A pair carries every client that
    // dials its upstream, and a fault is placed on one edge.
    let edge: Arc<OnceLock<OnEdge>> = Arc::new(OnceLock::new());
    let job = match &at {
        Some(at) => Job::Anchored {
            anchor: Anchor::new(
                at.direction,
                at.mark.clone(),
                nth,
                pause_tx.clone(),
                trip_tx,
                Arc::clone(&edge),
            ),
            fault,
        },
        None if degrade.is_some() => Job::Degrade {
            down: down_tx.clone(),
        },
        None => Job::Observe,
    };
    let anchor = match &job {
        Job::Anchored { anchor, .. } => Some(anchor.clone()),
        Job::Observe | Job::Degrade { .. } => None,
    };

    for pair in pairs {
        // Only the pairs fronting the anchored service count toward the fault,
        // and only they can be severed by it.
        let anchored = at.as_ref().is_some_and(|at| at.upstream == pair.service);
        let degraded = degrade
            .as_ref()
            .is_some_and(|(_, upstream)| *upstream == pair.service);
        let gate = Gate::new(
            pause_rx.clone(),
            if anchored {
                sever_rx.clone()
            } else {
                watch::channel(0u64).1
            },
            if degraded {
                down_rx.clone()
            } else {
                watch::channel(false).1
            },
            Arc::clone(&edge),
        );
        serve(pair, gate, anchored.then(|| anchor.clone()).flatten()).await?;
    }

    // Whichever of the two placed a fault named the edge it applies to.
    let client = at
        .as_ref()
        .map(|at| at.client.clone())
        .or_else(|| degrade.as_ref().map(|(client, _)| client.clone()))
        .flatten();
    spawn_fault_control(job, trip_rx, pause_tx, sever_tx, edge, hosts, client);

    for spec in cli.control {
        // A control pair carries the framework's own traffic, which is not
        // reported and so is not read as anything.
        let Pair {
            service,
            listen,
            upstream,
            ..
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
    /// What the service speaks here, which decides what the proxy can make of
    /// the bytes crossing it.
    kind: String,
    listen: SocketAddr,
    upstream: String,
}

impl Pair {
    fn parse(spec: &str) -> Result<Self> {
        let mut parts = spec.splitn(4, '=');
        let (Some(service), Some(kind), Some(listen), Some(upstream)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(Error::MalformedPair {
                pair: spec.to_string(),
            });
        };
        Ok(Self {
            service: service.to_string(),
            kind: kind.to_string(),
            listen: listen.parse().map_err(|source| Error::ParseListen {
                addr: listen.to_string(),
                source,
            })?,
            upstream: upstream.to_string(),
        })
    }
}

/// Bring one pair up and report what crosses it, for the lifetime of the
/// process.
async fn serve(pair: Pair, gate: Gate, anchor: Option<Anchor>) -> Result<()> {
    let Pair {
        service,
        kind,
        listen,
        upstream,
    } = pair;
    // Only the direction a schedule named watches for its moment.
    let watching = anchor
        .as_ref()
        .map(|anchor| (anchor.direction(), anchor.mark().to_owned()));
    let (proxy, _local_addr, mut events) = Proxy::bind(
        listen,
        Upstream {
            host: upstream.clone(),
            kinds: crucible_kind::Kinds::new(&kind, watching),
        },
        gate,
        anchor,
    )
    .await?;
    tracing::info!(
        %service, %kind, %listen, %upstream,
        // Whether a fault here lands on an operation or on a read off the wire.
        read = crucible_kind::is_read(&kind),
        "proxy pair up"
    );

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
    Ok(())
}

/// Do this proxy's job for the lifetime of the process: impose it when the
/// scenario starts (SIGUSR1), and lift it when told the run is over (SIGUSR2)
/// or that nothing was placed (SIGHUP).
///
/// Holds a `watch::Sender`, so it must outlive the proxies; it never returns.
fn spawn_fault_control(
    job: Job,
    mut trip: watch::Receiver<bool>,
    pause: watch::Sender<bool>,
    sever: watch::Sender<u64>,
    edge: Arc<OnceLock<OnEdge>>,
    hosts: Vec<(String, String)>,
    client: Option<String>,
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
                _ = arm.recv() => {
                    // The fleet answers to its names only once it is up, so the
                    // edge is resolved here rather than at bind.
                    if edge.get().is_none()
                        && edge.set(resolve(&hosts, client.as_deref()).await).is_err()
                    {
                        tracing::warn!("the edge was resolved twice, so the first stands");
                    }
                    job.arm();
                }
                Ok(()) = trip.changed() => {
                    if !*trip.borrow() {
                        continue;
                    }
                    if let Job::Anchored { fault, .. } = &job {
                        match fault {
                            // The service is killed and brought back from
                            // outside, which says so by signalling; until then
                            // the fleet waits, so nothing runs against a
                            // half-dead service. Abandoning says the kill never
                            // landed, and releases just the same.
                            Fault::Kill => tokio::select! {
                                _ = proceed.recv() => tracing::info!("the kill has landed (SIGUSR2)"),
                                _ = abandon.recv() => tracing::info!("the kill was abandoned (SIGHUP)"),
                            },
                            Fault::Cut => {
                                sever_once(&sever);
                                tracing::info!("severed the anchored pairs");
                            }
                            // The plugin changed what crossed as it passed, so
                            // the fleet was never held and there is nothing
                            // here to put back.
                            Fault::Rewritten => {}
                        }
                    }
                    job.release(&pause);
                }
                // The run is over: either the scenario ended before the anchored
                // packet, or it was degraded throughout and is done.
                _ = abandon.recv() => job.release(&pause),
                _ = proceed.recv() => job.release(&pause),
            }
        }
    });
}

/// What this proxy was launched to do.
enum Job {
    /// Watch the fleet and interfere with nothing.
    Observe,
    /// Place `fault` on the anchored packet.
    Anchored { anchor: Anchor, fault: Fault },
    /// Hold a service's pairs down from scenario start until released.
    Degrade { down: watch::Sender<bool> },
}

impl Job {
    /// The scenario is starting.
    fn arm(&self) {
        match self {
            Job::Observe => {}
            Job::Anchored { anchor, .. } => {
                anchor.arm();
                tracing::info!("anchor armed at scenario start (SIGUSR1)");
            }
            // A degraded pair is down for the whole scenario, so it goes down
            // here rather than at any packet.
            Job::Degrade { down } => {
                send(down, true);
                tracing::info!("held the degraded pairs down (SIGUSR1)");
            }
        }
    }

    /// Leave the fleet able to carry traffic, and unable to be faulted again.
    fn release(&self, pause: &watch::Sender<bool>) {
        match self {
            Job::Observe => {}
            Job::Anchored { anchor, .. } => anchor.disarm(),
            Job::Degrade { down } => send(down, false),
        }
        send(pause, false);
        tracing::info!("let the fleet go");
    }
}

// FIXME: propagate once the fault-control loop can report failure. main holds a
// receiver for the process lifetime, so this cannot fail; a silent failure would
// leave the fleet frozen with nothing left to release it.
fn send(watch: &watch::Sender<bool>, value: bool) {
    watch.send(value).expect("the watch has a live receiver");
}

/// Sever the anchored pairs once more.
///
/// The count is advanced in place. Reading it to send one more would hold the
/// watch open while asking to write it, and the sever would wait on itself.
fn sever_once(severings: &watch::Sender<u64>) {
    severings.send_modify(|count| *count += 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_client_to_upstream_anchor() {
        let at = parse_fault_at("api>db=c2u=ack:7:before").expect("valid spec");
        assert_eq!(at.client.as_deref(), Some("api"));
        assert_eq!(at.upstream, "db");
        assert_eq!(at.direction, Direction::ClientToUpstream);
        assert_eq!(at.mark, "ack:7:before");
    }

    #[test]
    fn parses_an_upstream_to_client_anchor() {
        let at = parse_fault_at("inventory>api=u2c=publish:1:after").expect("valid spec");
        assert_eq!(at.client.as_deref(), Some("inventory"));
        assert_eq!(at.upstream, "api");
        assert_eq!(at.direction, Direction::UpstreamToClient);
        assert_eq!(at.mark, "publish:1:after");
    }

    /// The framework driving a step is not one of the fleet's services, so the
    /// edge it dials has no near end to name.
    #[test]
    fn parses_an_anchor_on_an_edge_from_outside_the_fleet() {
        let at = parse_fault_at(">api=c2u=1").expect("valid spec");
        assert_eq!(at.client, None);
        assert_eq!(at.upstream, "api");
    }

    #[test]
    fn rejects_an_anchor_naming_no_edge() {
        assert!(parse_fault_at("db=c2u=3").is_err());
    }

    #[test]
    fn rejects_an_unknown_direction() {
        assert!(parse_fault_at("api>db=sideways=3").is_err());
    }

    /// Only the plugin reading the edge knows what its marks mean, so the proxy
    /// carries whatever it is given rather than judging it.
    #[test]
    fn a_mark_is_whatever_the_plugin_reading_it_says() {
        for mark in ["three", "ack:7:before", "publish:1:after"] {
            let at = parse_fault_at(&format!("api>db=c2u={mark}")).expect("valid spec");
            assert_eq!(at.mark, mark);
        }
    }

    #[test]
    fn rejects_a_missing_field() {
        assert!(parse_fault_at("db=c2u").is_err());
    }

    /// Every cut tells the pairs to let go of what they hold, so a second one
    /// has to say something a connection accepted after the first can tell
    /// apart from it.
    #[test]
    fn severing_advances_what_the_pairs_are_holding() {
        let (severings, holding) = watch::channel(0u64);
        sever_once(&severings);
        let first = *holding.borrow();
        sever_once(&severings);
        assert_ne!(*holding.borrow(), first);
    }

    #[test]
    fn a_fault_service_must_front_a_pair() {
        let pairs = vec![Pair::parse("db=mariadb=0.0.0.0:3306=db-actual:3306").unwrap()];
        assert!(fronting("db", &pairs).is_some());
        assert!(fronting("api", &pairs).is_none());
    }

    /// What a mark means is the fronted pair's business: a kind with a plugin
    /// names its own moment, and one without counts the moments offered.
    #[test]
    fn what_a_mark_means_comes_from_the_kind_fronting_it() {
        assert_eq!(crucible_kind::nth("mariadb", "3"), 3);
        assert_eq!(crucible_kind::nth("amqp", "ack:7:before"), 1);
    }
}
