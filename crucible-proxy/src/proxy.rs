//! Bytes-through TCP forwarder that emits connection events.

use std::{
    collections::HashSet,
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use crucible_protocol::{ConnEvent, ConnId, Direction, Kind};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, watch},
};

/// Where a fault lands on a pair: at the moment `mark` names, which the kind
/// plugin recognises on `direction`.
///
/// Dormant until [`arm`](Self::arm), so a fault cannot be placed in the fleet's
/// own bring-up. A run that never reaches the moment says so, rather than
/// faulting whatever happened to be in that position.
#[derive(Clone)]
pub struct Anchor {
    direction: Direction,
    mark: String,
    /// How many of the moments offered to let pass before the one the mark
    /// names, which is one unless the count is the framework's own.
    nth: u32,
    /// Moments that have passed on this edge since the scenario started. The
    /// pair carries every client that dials its upstream, and a count taken
    /// from one edge means nothing on another.
    seen: Arc<AtomicU32>,
    active: Arc<AtomicBool>,
    /// Held the moment the anchored packet passes, inside the task that carried
    /// it, so nothing slips through while the fault is being decided.
    pause: watch::Sender<bool>,
    /// Says the anchored packet has passed.
    trip: watch::Sender<bool>,
    /// The edge the mark is watched on, so another client's traffic to the same
    /// upstream cannot fire a fault the schedule never named.
    edge: Arc<OnceLock<OnEdge>>,
}

/// Which connections a fault applies to: those carrying the edge it names.
///
/// The proxy fronts an upstream, so its pair carries every client that dials
/// that upstream. A fault named for one edge must not count or sever another
/// client's traffic to the same service.
///
/// Resolved once, when the anchor is armed: the fleet is up by then, so its
/// names answer, and nothing is looked up while bytes are moving.
pub struct OnEdge {
    /// Addresses the client that dialled holds.
    client: HashSet<IpAddr>,
    /// Every address the fleet holds, so a caller from outside it is one that
    /// is none of these.
    fleet: HashSet<IpAddr>,
    /// Whether the edge was dialled from outside the fleet.
    outside: bool,
}

impl OnEdge {
    #[must_use]
    pub fn new(client: HashSet<IpAddr>, fleet: HashSet<IpAddr>, outside: bool) -> Self {
        Self {
            client,
            fleet,
            outside,
        }
    }

    /// Whether a connection from `peer` is the edge this was built for.
    #[must_use]
    fn holds(&self, peer: IpAddr) -> bool {
        if self.outside {
            !self.fleet.contains(&peer)
        } else {
            self.client.contains(&peer)
        }
    }
}

/// Whether `peer` is on the edge, once it is known. An unresolved edge holds
/// nothing: it is resolved as the scenario starts, and nothing is counted or
/// severed before that.
fn on_edge(edge: &OnceLock<OnEdge>, peer: IpAddr) -> bool {
    edge.get().is_some_and(|edge| edge.holds(peer))
}

/// One connection the proxy carries, and who dialled it.
#[derive(Clone, Copy)]
struct Conn {
    id: ConnId,
    peer: IpAddr,
}

/// Where a pair forwards to, and what reads what crosses it.
#[derive(Clone)]
pub struct Upstream {
    /// The host it dials, resolved per connection rather than once: it is a
    /// container that may not exist when the pair binds, and may be a different
    /// one after a kill and restart.
    pub host: String,
    /// Makes a reader per connection, holding whatever they share.
    pub kinds: crucible_kind::Kinds,
}

/// One direction of one connection: where its bytes come from, where they go,
/// and which way that is.
struct Half {
    read: OwnedReadHalf,
    write: OwnedWriteHalf,
    direction: Direction,
}

/// What a connection is subject to while the fleet runs.
#[derive(Clone)]
pub struct Gate {
    /// Stop delivering packets by holding the bytes while this holds `true`.
    /// The network is frozen for as long as the fault takes to run, so it lands
    /// on the anchored packet however long it needs.
    pause: watch::Receiver<bool>,
    /// Severings of this pair. A connection lets go when it moves past what it
    /// read at accept, so a peer that reconnects afterwards gets a working edge
    /// and the fleet is left to recover.
    severings: watch::Receiver<u64>,
    /// What `severings` read when this connection was accepted.
    opened_at: u64,
    /// While `true` the pair carries nothing, so the peer meets a partition
    /// rather than a blip.
    down: watch::Receiver<bool>,
    /// The edge being cut, which only some of this pair's connections carry.
    edge: Arc<OnceLock<OnEdge>>,
    /// Who dialled, so this connection can tell whether it is that edge.
    peer: Option<IpAddr>,
}

impl Gate {
    #[must_use]
    pub fn new(
        pause: watch::Receiver<bool>,
        severings: watch::Receiver<u64>,
        down: watch::Receiver<bool>,
        edge: Arc<OnceLock<OnEdge>>,
    ) -> Self {
        let opened_at = *severings.borrow();
        Self {
            pause,
            severings,
            opened_at,
            down,
            edge,
            peer: None,
        }
    }

    /// The gate a connection from `peer` is subject to.
    fn accept(&self, peer: IpAddr) -> Self {
        Self {
            opened_at: *self.severings.borrow(),
            peer: Some(peer),
            pause: self.pause.clone(),
            severings: self.severings.clone(),
            down: self.down.clone(),
            edge: Arc::clone(&self.edge),
        }
    }

    /// Hold here while the network is frozen. Returns whether the connection
    /// came through it: a fault that runs while it waits may have severed it.
    async fn passable(&mut self) -> bool {
        while *self.pause.borrow() {
            if self.pause.changed().await.is_err() {
                // Control has gone; forwarding must not wedge behind it.
                break;
            }
        }
        !self.severed()
    }

    fn severed(&self) -> bool {
        let cut = *self.severings.borrow() != self.opened_at || *self.down.borrow();
        cut && self.peer.is_some_and(|peer| on_edge(&self.edge, peer))
    }

    /// Resolves once this connection is severed, so it can race something that
    /// would only end when a packet arrives.
    async fn severing(&mut self) {
        while !self.severed() {
            tokio::select! {
                () = changed(&mut self.severings) => {}
                () = changed(&mut self.down) => {}
            }
        }
    }
}

/// Resolves when `watch` changes, and never once nothing can change it, so one
/// dead signal cannot stop another being waited on.
async fn changed<T>(watch: &mut watch::Receiver<T>) {
    if watch.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

impl Anchor {
    pub fn new(
        direction: Direction,
        mark: String,
        nth: u32,
        pause: watch::Sender<bool>,
        trip: watch::Sender<bool>,
        edge: Arc<OnceLock<OnEdge>>,
    ) -> Self {
        Self {
            direction,
            mark,
            nth,
            seen: Arc::new(AtomicU32::new(0)),
            active: Arc::new(AtomicBool::new(false)),
            pause,
            trip,
            edge,
        }
    }

    /// Which way the traffic carrying its moment runs.
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// The moment it waits for.
    #[must_use]
    pub fn mark(&self) -> &str {
        &self.mark
    }

    /// Hold the fleet. Holding it here rather than leaving it to whoever reads
    /// the trip keeps the fault on the moment the mark named.
    fn fire(&self) {
        // FIXME: propagate once arm and record can report failure. Both watches
        // keep a receiver for the whole process, so this cannot fail; dropping
        // it silently would leave the fault unplaced while the caller is told
        // the anchor tripped.
        self.pause
            .send(true)
            .expect("the pause watch has a live receiver");
        self.trip
            .send(true)
            .expect("the trip watch has a live receiver");
        tracing::info!(mark = %self.mark, "held the fleet on the moment its mark names");
    }

    /// Arm at scenario start, so what the fleet does from here can place the
    /// fault.
    pub fn arm(&self) {
        self.seen.store(0, Ordering::SeqCst);
        self.active.store(true, Ordering::SeqCst);
    }

    /// Whether a fault may be placed on this connection now: the scenario has
    /// started and this is the edge the schedule named.
    #[must_use]
    pub fn placing(&self, peer: IpAddr) -> bool {
        self.active.load(Ordering::SeqCst) && on_edge(&self.edge, peer)
    }

    /// Whether a moment offered on this connection is the one the schedule
    /// named, and if it is, hold the fleet.
    ///
    /// A no-op until armed, so bring-up traffic cannot place a fault, and a
    /// no-op for a connection that is not the anchored edge.
    fn reached(&self, peer: IpAddr) -> bool {
        if !self.active.load(Ordering::SeqCst) {
            return false;
        }
        if !on_edge(&self.edge, peer) {
            // Whose traffic is being passed over, which is what separates an
            // edge that carried nothing from one we failed to recognise.
            tracing::debug!(%peer, direction = ?self.direction, "not the anchored edge");
            return false;
        }
        if self.seen.fetch_add(1, Ordering::SeqCst) + 1 != self.nth {
            return false;
        }
        self.fire();
        true
    }

    /// Disarm: stop counting so the anchor cannot fire again. Paired with every
    /// release, so a late packet cannot place a second fault once the fleet has
    /// been let go with no one left to let it go again.
    pub fn disarm(&self) {
        self.active.store(false, Ordering::SeqCst);
        // How far the anchor got, which is what tells a run that reported no
        // fault whether its packet never came or was never noticed.
        tracing::debug!(
            mark = %self.mark,
            reached = self.seen.load(Ordering::SeqCst),
            direction = ?self.direction,
            resolved = self.edge.get().is_some(),
            "anchor disarmed"
        );
    }
}

pub struct Proxy {
    listener: TcpListener,
    upstream: Upstream,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    next_id: Arc<AtomicU64>,
    gate: Gate,
    anchor: Option<Anchor>,
}

impl Proxy {
    /// Bind to `listen` and prepare to forward every accepted connection to
    /// `upstream`, subject to `gate`, reading what crosses it as `kind`.
    /// `anchor`, if set, trips once this pair has carried `k` operations on its
    /// direction. Returns the proxy, its bound local address, and the receiver
    /// for connection events.
    pub async fn bind(
        listen: SocketAddr,
        upstream: Upstream,
        gate: Gate,
        anchor: Option<Anchor>,
    ) -> io::Result<(Self, SocketAddr, mpsc::UnboundedReceiver<ConnEvent>)> {
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let proxy = Self {
            listener,
            upstream,
            events_tx,
            next_id: Arc::new(AtomicU64::new(0)),
            gate,
            anchor,
        };
        Ok((proxy, local_addr, events_rx))
    }

    /// Accept-forward loop; returns only if `accept()` errors.
    pub async fn run(self) -> io::Result<()> {
        loop {
            let (client, peer) = self.listener.accept().await?;
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(forward(
                id,
                client,
                peer,
                self.upstream.clone(),
                self.events_tx.clone(),
                self.gate.accept(peer.ip()),
                self.anchor.clone(),
            ));
        }
    }
}

/// Fronts one service on the side channel: forwards bytes and nothing else.
/// What crosses it is not reported, cannot be held, and cannot trip an anchor,
/// so the framework can reach a service without that reaching the results.
pub struct Relay {
    listener: TcpListener,
    upstream: String,
}

impl Relay {
    /// Bind to `listen` and prepare to forward every accepted connection to
    /// `upstream`.
    ///
    /// # Errors
    /// Errors if `listen` cannot be bound.
    pub async fn bind(listen: SocketAddr, upstream: String) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(listen).await?,
            upstream,
        })
    }

    /// Accept-forward loop; returns only if `accept()` errors.
    ///
    /// # Errors
    /// Errors if accepting a connection fails.
    pub async fn run(self) -> io::Result<()> {
        loop {
            let (mut client, peer) = self.listener.accept().await?;
            let upstream = self.upstream.clone();
            tokio::spawn(async move {
                match TcpStream::connect(&upstream).await {
                    Ok(mut server) => {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                    Err(e) => {
                        tracing::warn!(%peer, %upstream, ?e, "side channel could not reach upstream");
                    }
                }
            });
        }
    }
}

/// Send a connection event to the drain task in `main`, which holds the receiver
/// for as long as any forward task (or the accept loop) holds a sender.
// FIXME: propagate this for real recovery once the forward tasks report failures
// upward; a send failure means the drain task died unexpectedly, so until then
// assert that invariant rather than drop the event silently.
fn emit(events: &mpsc::UnboundedSender<ConnEvent>, event: ConnEvent) {
    events
        .send(event)
        .expect("event drain outlives the forward tasks");
}

async fn forward(
    id: ConnId,
    client: TcpStream,
    peer: SocketAddr,
    upstream: Upstream,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    gate: Gate,
    anchor: Option<Anchor>,
) {
    let upstream_conn = match TcpStream::connect(&upstream.host).await {
        Ok(s) => s,
        Err(e) => {
            emit(
                &events_tx,
                ConnEvent::failed(id, format!("dial upstream {}: {e}", upstream.host)),
            );
            return;
        }
    };

    emit(&events_tx, ConnEvent::opened(id, peer));

    let (client_r, client_w) = client.into_split();
    let (upstream_r, upstream_w) = upstream_conn.into_split();

    let anchor_c2u = anchor
        .clone()
        .filter(|a| a.direction == Direction::ClientToUpstream);
    let anchor_u2c = anchor.filter(|a| a.direction == Direction::UpstreamToClient);

    let conn = Conn {
        id,
        peer: peer.ip(),
    };
    // A reader each way: what a client sends and what it gets back are two
    // streams, and neither says anything about where the other has got to.
    let c2u = tokio::spawn(forward_bytes(
        conn,
        Half {
            read: client_r,
            write: upstream_w,
            direction: Direction::ClientToUpstream,
        },
        // Only the anchored direction watches for the moment; the other way
        // reads its traffic and forwards it.
        upstream.kinds.reader(Direction::ClientToUpstream),
        events_tx.clone(),
        gate.clone(),
        anchor_c2u,
    ));
    let u2c = tokio::spawn(forward_bytes(
        conn,
        Half {
            read: upstream_r,
            write: client_w,
            direction: Direction::UpstreamToClient,
        },
        upstream.kinds.reader(Direction::UpstreamToClient),
        events_tx.clone(),
        gate,
        anchor_u2c,
    ));

    let (c2u_res, u2c_res) = tokio::join!(c2u, u2c);
    let event = match (
        c2u_res.unwrap_or(Err("c2u task panicked".into())),
        u2c_res.unwrap_or(Err("u2c task panicked".into())),
    ) {
        (Ok(up), Ok(down)) => ConnEvent::closed(id, up, down),
        (Err(e), _) | (_, Err(e)) => ConnEvent::failed(id, format!("forwarding: {e}")),
    };
    emit(&events_tx, event);
}

/// Forward the byte stream flowing one way (`direction`) of a connection: read a
/// chunk from `read`, hold it while the fleet is paused, write it to `write`, and
/// count each chunk against `anchor`. Returns the total bytes forwarded, or the
/// error that ended it.
async fn forward_bytes(
    conn: Conn,
    half: Half,
    mut operations: Box<dyn Kind>,
    events: mpsc::UnboundedSender<ConnEvent>,
    mut gate: Gate,
    anchor: Option<Anchor>,
) -> Result<u64, String> {
    let Half {
        mut read,
        mut write,
        direction,
    } = half;
    let (read_label, write_label) = match direction {
        Direction::ClientToUpstream => ("client_read", "upstream_write"),
        Direction::UpstreamToClient => ("upstream_read", "client_write"),
    };
    let mut bytes_total: u64 = 0;
    let mut buf = vec![0u8; 4096];
    loop {
        // A cut severs an idle connection too, so it races the read rather than
        // waiting for a packet that is never coming.
        let read = tokio::select! {
            read = read.read(&mut buf) => read,
            () = gate.severing() => {
                tracing::debug!(%conn.peer, ?direction, "severed while waiting for traffic");
                break Ok(bytes_total);
            }
        };
        let n = match read {
            Ok(0) => break Ok(bytes_total),
            Ok(n) => n,
            Err(e) => break Err(format!("{read_label}: {e}")),
        };
        if !gate.passable().await {
            tracing::debug!(%conn.peer, ?direction, "severed with traffic in hand");
            break Ok(bytes_total);
        }
        // What goes on the wire, which is what arrived unless the plugin had
        // reason to change it. Anything still arriving stays with the plugin
        // until the rest of it turns up.
        let placing = anchor
            .as_ref()
            .is_some_and(|anchor| anchor.placing(conn.peer));
        let carried = operations.carry(&buf[..n], placing);
        for placement in carried.found {
            emit(&events, ConnEvent::placeable(conn.id, placement));
        }
        if let Some(did) = carried.did {
            tracing::info!(%conn.peer, ?direction, ?did, "the plugin changed what crossed");
            emit(&events, ConnEvent::did(conn.id, did));
        }
        // The plugin says which of these the moment falls after, so what comes
        // before it goes out first and the fleet is held on the moment itself.
        let held_after = carried.freeze_after.unwrap_or(carried.forward.len());
        let mut written: u64 = 0;
        let mut severed = false;
        for (sent, frame) in carried.forward.iter().enumerate() {
            if let Err(e) = write.write_all(frame).await {
                return Err(format!("{write_label}: {e}"));
            }
            written += frame.len() as u64;
            if sent + 1 != held_after {
                continue;
            }
            if carried.freeze_after.is_some()
                && let Some(anchor) = &anchor
                && anchor.reached(conn.peer)
            {
                emit(&events, ConnEvent::froze(conn.id, anchor.mark.clone()));
                // Hold here, in the task carrying the moment, so nothing else
                // crosses while the fault is placed.
                if !gate.passable().await {
                    severed = true;
                    break;
                }
            }
        }
        // Only once it is through: what the gate held back and then severed was
        // never forwarded, and the learn pass counts these as traffic.
        emit(&events, ConnEvent::wrote(conn.id, direction, written));
        bytes_total += written;
        if severed {
            break Ok(bytes_total);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, time::Duration};

    use crucible_protocol::ConnEventKind;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::timeout,
    };

    use super::*;

    /// Who the connections under test come from.
    const PEER: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));

    /// An edge that everything is on, so a test says what it is about rather
    /// than which end dialled.
    fn every_peer() -> Arc<OnceLock<OnEdge>> {
        let edge = OnceLock::new();
        // An edge dialled from outside an empty fleet, which every peer is.
        let held = edge.set(OnEdge::new(HashSet::new(), HashSet::new(), true));
        assert!(held.is_ok(), "a fresh cell is empty");
        Arc::new(edge)
    }

    /// `host` spoken as a kind with no plugin to read it, so a read off the
    /// wire is one operation and these tests count what they send.
    fn unread(host: SocketAddr) -> Upstream {
        Upstream {
            host: host.to_string(),
            kinds: crucible_kind::Kinds::new("bytes", None),
        }
    }

    /// The same, offering the moments `anchor` counts, so a fault can land.
    fn watching(host: SocketAddr, anchor: &Anchor) -> Upstream {
        Upstream {
            host: host.to_string(),
            kinds: crucible_kind::Kinds::new(
                "bytes",
                Some((anchor.direction(), anchor.mark().to_owned())),
            ),
        }
    }

    /// A gate that never closes.
    fn open() -> Gate {
        Gate::new(
            watch::channel(false).1,
            watch::channel(0).1,
            watch::channel(false).1,
            every_peer(),
        )
    }

    /// A gate held by `pause`.
    fn held(pause: watch::Receiver<bool>) -> Gate {
        Gate::new(
            pause,
            watch::channel(0).1,
            watch::channel(false).1,
            every_peer(),
        )
    }

    /// A moment for an anchor to watch for.
    const MARK: &str = "publish:1:after";

    /// An anchor watching for `mark`, with the watches it fires. The kind is
    /// unread, so the mark counts the moments offered.
    fn anchor(mark: &str) -> (Anchor, watch::Receiver<bool>, watch::Receiver<bool>) {
        let (pause_tx, pause_rx) = watch::channel(false);
        let (trip_tx, trip_rx) = watch::channel(false);
        (
            Anchor::new(
                Direction::ClientToUpstream,
                mark.to_owned(),
                crucible_kind::nth("bytes", mark),
                pause_tx,
                trip_tx,
                every_peer(),
            ),
            pause_rx,
            trip_rx,
        )
    }

    async fn spawn_echo() -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 || sock.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    async fn recv(events: &mut mpsc::UnboundedReceiver<ConnEvent>) -> ConnEvent {
        timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event within 2s")
            .expect("channel not closed")
    }

    #[tokio::test]
    async fn two_concurrent_connections_round_trip_and_emit_distinct_events() {
        let echo = spawn_echo().await;
        let (proxy, proxy_addr, mut events) =
            Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), unread(echo), open(), None)
                .await
                .unwrap();
        tokio::spawn(proxy.run());

        let mut a = TcpStream::connect(proxy_addr).await.unwrap();
        let mut b = TcpStream::connect(proxy_addr).await.unwrap();

        a.write_all(b"foo").await.unwrap();
        b.write_all(b"abcdef").await.unwrap();

        let mut buf_a = [0u8; 3];
        let mut buf_b = [0u8; 6];
        a.read_exact(&mut buf_a).await.unwrap();
        b.read_exact(&mut buf_b).await.unwrap();
        assert_eq!(&buf_a, b"foo");
        assert_eq!(&buf_b, b"abcdef");

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();

        let mut opened = std::collections::HashSet::new();
        let mut closed = std::collections::HashMap::new();
        let mut wrote_counts: std::collections::HashMap<ConnId, u64> =
            std::collections::HashMap::default();
        // Two opens + two closes + variable number of wrote events (at least 4: one c2u and one u2c per conn).
        while closed.len() < 2 {
            let event = recv(&mut events).await;
            match event.kind {
                ConnEventKind::Opened { .. } => {
                    opened.insert(event.id);
                }
                ConnEventKind::Wrote { bytes, .. } => {
                    *wrote_counts.entry(event.id).or_default() += bytes;
                }
                // Nothing reads these bytes, so it can neither say where a
                // fault would go nor change what crosses.
                ConnEventKind::Placeable { placement } => {
                    panic!("unexpected placement from an unread kind: {placement:?}")
                }
                ConnEventKind::Did { did } => {
                    panic!("an unread kind changed nothing: {did:?}")
                }
                ConnEventKind::Closed {
                    bytes_client_to_upstream,
                    bytes_upstream_to_client,
                } => {
                    closed.insert(
                        event.id,
                        (bytes_client_to_upstream, bytes_upstream_to_client),
                    );
                }
                ConnEventKind::Failed { reason } => panic!("unexpected Failed: {reason}"),
                ConnEventKind::Froze { mark } => {
                    panic!("unexpected Froze without an anchor: {mark}")
                }
            }
        }

        assert_eq!(opened.len(), 2);
        assert_eq!(closed.len(), 2);
        assert!(closed.values().any(|&counts| counts == (3, 3)));
        assert!(closed.values().any(|&counts| counts == (6, 6)));
    }

    #[tokio::test]
    async fn emits_failed_event_when_upstream_unreachable() {
        let unused = {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            listener.local_addr().unwrap()
        };
        let (proxy, proxy_addr, mut events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            unread(unused),
            open(),
            None,
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());

        let _client = TcpStream::connect(proxy_addr).await.unwrap();

        let event = recv(&mut events).await;
        assert!(matches!(event.kind, ConnEventKind::Failed { .. }));
    }

    #[tokio::test]
    async fn paused_proxy_holds_bytes_until_resumed() {
        let echo = spawn_echo().await;
        let (pause_tx, pause_rx) = watch::channel(false);
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            unread(echo),
            held(pause_rx),
            None,
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();

        // Pause, then send: the proxy reads the bytes but must hold them, so the
        // echo upstream never sees them and nothing comes back.
        pause_tx.send(true).unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        let held = timeout(Duration::from_millis(300), client.read_exact(&mut buf)).await;
        assert!(held.is_err(), "bytes should be held while paused");

        // Resume: the held bytes flow, echo replies, and the round trip completes.
        pause_tx.send(false).unwrap();
        timeout(Duration::from_secs(2), client.read_exact(&mut buf))
            .await
            .expect("echo within 2s after resume")
            .expect("read succeeds");
        assert_eq!(&buf, b"ping");
    }

    #[tokio::test]
    async fn a_pair_held_down_refuses_reconnects_until_it_is_lifted() {
        let echo = spawn_echo().await;
        let (down_tx, down_rx) = watch::channel(false);
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            unread(echo),
            Gate::new(
                watch::channel(false).1,
                watch::channel(0).1,
                down_rx,
                every_peer(),
            ),
            None,
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());

        down_tx.send(true).unwrap();
        let mut refused = TcpStream::connect(proxy_addr).await.unwrap();
        refused.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        let read = timeout(Duration::from_secs(2), refused.read(&mut buf))
            .await
            .expect("a connection opened while down goes, rather than hanging");
        assert!(matches!(read, Ok(0) | Err(_)), "the edge is not carrying");

        // Lifted, the edge carries again and the fleet can catch up.
        down_tx.send(false).unwrap();
        let mut restored = TcpStream::connect(proxy_addr).await.unwrap();
        restored.write_all(b"pong").await.unwrap();
        timeout(Duration::from_secs(2), restored.read_exact(&mut buf))
            .await
            .expect("the edge carries once lifted")
            .expect("read succeeds");
        assert_eq!(&buf, b"pong");
    }

    #[tokio::test]
    async fn a_cut_takes_the_connection_out_from_under_an_idle_peer() {
        let echo = spawn_echo().await;
        let (sever_tx, sever_rx) = watch::channel(0);
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            unread(echo),
            Gate::new(
                watch::channel(false).1,
                sever_rx,
                watch::channel(false).1,
                every_peer(),
            ),
            None,
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();

        // Nothing more is coming, so a cut that waited for the next packet would
        // leave this connection sitting here.
        sever_tx.send(1).unwrap();
        let read = timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("the connection goes within 2s of the cut");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "the peer sees the edge go away"
        );

        // The edge is severed, not removed, so whatever recovery the peer has
        // gets to run.
        let mut reconnected = TcpStream::connect(proxy_addr).await.unwrap();
        reconnected.write_all(b"pong").await.unwrap();
        timeout(Duration::from_secs(2), reconnected.read_exact(&mut buf))
            .await
            .expect("a connection opened after the cut still works")
            .expect("read succeeds");
        assert_eq!(&buf, b"pong");
    }

    #[test]
    fn a_dormant_anchor_does_not_fire() {
        // Before arm the moment cannot be reached, so the fleet's own bring-up
        // never places a fault.
        let (anchor, _pause, rx) = anchor(MARK);
        assert!(!anchor.reached(PEER), "an unarmed anchor must not fire");
        assert!(!*rx.borrow(), "gate stays open while dormant");
        anchor.arm();
        assert!(anchor.reached(PEER), "an armed anchor fires at the moment");
        assert!(*rx.borrow(), "gate tripped once armed");
    }

    #[test]
    fn a_disarmed_anchor_does_not_refreeze() {
        // On a give-up path the runner releases the gate but the anchor stays
        // armed; a moment reached afterwards must not trip it again, since
        // there would be no one left to resume. Disarming makes arm a one-shot.
        let (anchor, _pause, rx) = anchor(MARK);
        anchor.arm();
        anchor.disarm();
        assert!(!anchor.reached(PEER), "a disarmed anchor must not fire");
        assert!(!*rx.borrow(), "a disarmed anchor must not re-trip the gate");
    }

    #[tokio::test]
    async fn an_anchor_announces_the_moment_it_stopped_on() {
        let echo = spawn_echo().await;
        let (anchor, pause_rx, _trip) = anchor("2");
        let (proxy, proxy_addr, mut events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            watching(echo, &anchor),
            held(pause_rx),
            Some(anchor.clone()),
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());
        anchor.arm();

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        // The first read is before the moment; forwarded and echoed normally.
        client.write_all(b"a").await.unwrap();
        let mut buf = [0u8; 1];
        client.read_exact(&mut buf).await.unwrap();
        // The second is the moment the mark named.
        client.write_all(b"b").await.unwrap();

        // The proxy must announce the freeze, naming what it stopped on.
        let stopped_on = loop {
            let event = recv(&mut events).await;
            if let ConnEventKind::Froze { mark } = event.kind {
                break mark;
            }
        };
        assert_eq!(stopped_on, "2");
    }

    #[tokio::test]
    async fn an_anchor_holds_the_fleet_at_the_moment_it_names() {
        let echo = spawn_echo().await;
        let (anchor, mut pause_rx, _trip) = anchor("2");
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            watching(echo, &anchor),
            held(pause_rx.clone()),
            Some(anchor.clone()),
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());
        // Arm at "scenario start"; before this the anchor is dormant.
        anchor.arm();

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();

        // First client->upstream packet: below the anchor, still forwarding.
        client.write_all(b"a").await.unwrap();
        let mut buf = [0u8; 1];
        timeout(Duration::from_secs(2), client.read_exact(&mut buf))
            .await
            .expect("first packet echoes back")
            .expect("read succeeds");
        assert!(
            !*pause_rx.borrow_and_update(),
            "gate not tripped after one packet"
        );

        // Second client->upstream packet reaches K=2 and trips the shared gate.
        client.write_all(b"b").await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if *pause_rx.borrow_and_update() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "anchor should have tripped the gate by K=2"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A cut takes the connection it lands on, not the service. What the caller
    /// dials next has to be answered, or every step after the fault reports a
    /// fleet that stopped talking rather than the one write that was lost.
    #[tokio::test]
    async fn a_severed_connection_leaves_the_next_one_working() {
        let echo = spawn_echo().await;
        let (pause_tx, pause_rx) = watch::channel(false);
        let (trip_tx, mut trip_rx) = watch::channel(false);
        let (sever_tx, sever_rx) = watch::channel(0u64);
        let edge = every_peer();
        let anchor = Anchor::new(
            Direction::ClientToUpstream,
            "1".to_owned(),
            1,
            pause_tx.clone(),
            trip_tx,
            Arc::clone(&edge),
        );
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            watching(echo, &anchor),
            Gate::new(pause_rx, sever_rx, watch::channel(false).1, edge),
            Some(anchor.clone()),
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());
        anchor.arm();

        // What the fault control does for a cut: sever the pair, then let the
        // fleet go, leaving it to carry on with one connection lost.
        let severing = anchor.clone();
        tokio::spawn(async move {
            while !*trip_rx.borrow_and_update() {
                if trip_rx.changed().await.is_err() {
                    return;
                }
            }
            sever_tx.send(1).expect("the sever watch has receivers");
            severing.disarm();
            pause_tx.send(false).expect("the pause watch has receivers");
        });

        // The connection the fault lands on: it goes, one way or another.
        let mut cut = TcpStream::connect(proxy_addr).await.unwrap();
        cut.write_all(b"a").await.unwrap();
        let mut buf = [0u8; 1];
        let read = timeout(Duration::from_secs(2), cut.read(&mut buf)).await;
        assert!(
            matches!(read, Ok(Ok(0) | Err(_))),
            "the severed connection ends rather than hanging: {read:?}"
        );

        // The one dialled after it is a working edge.
        let mut next = TcpStream::connect(proxy_addr).await.unwrap();
        next.write_all(b"b").await.unwrap();
        timeout(Duration::from_secs(2), next.read_exact(&mut buf))
            .await
            .expect("the next connection is answered")
            .expect("read succeeds");
        assert_eq!(&buf, b"b");
    }
}
