//! Bytes-through TCP forwarder that emits connection events.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use crucible_protocol::{ConnEvent, ConnId, Direction};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, watch},
};

/// Where a fault lands on a pair: once `k` packets have been forwarded on
/// `direction`, counted across all this pair's connections.
///
/// Dormant until [`arm`](Self::arm), so it counts only scenario traffic, not the
/// fleet's own bring-up handshakes. The runner arms it once the scenario starts,
/// which resets the count to zero; that shared origin is what makes the count
/// match the scenario-relative one the learn pass measured.
///
/// The count is shared across the pair's connections. With a single connection
/// on `direction` (the common case) the fault lands exactly on the k-th packet.
/// With several connections forwarding at once, one that has already passed the
/// gate can write a packet or two past `k` before it next observes it, so under
/// contention the fault lands slightly late, never early.
#[derive(Clone)]
pub struct Anchor {
    direction: Direction,
    k: u32,
    count: Arc<AtomicU32>,
    active: Arc<AtomicBool>,
    /// Fired once the anchored packet passes. What that sets off is the [`Gate`]
    /// the connections were given.
    trip: watch::Sender<bool>,
}

/// What a connection is subject to while the fleet runs.
#[derive(Clone)]
pub struct Gate {
    /// Stop delivering packets by holding the bytes while this holds `true`.
    /// The network is frozen for as long as the fault takes to run, so it lands
    /// on the anchored packet however long it needs.
    pause: watch::Receiver<bool>,
    /// Drop the peer's connection when this fires.
    cut: watch::Receiver<bool>,
}

impl Gate {
    #[must_use]
    pub fn new(pause: watch::Receiver<bool>, cut: watch::Receiver<bool>) -> Self {
        Self { pause, cut }
    }

    /// Hold here while the network is frozen. Returns whether the connection
    /// came through it: a fault that runs while it waits may have cut it.
    async fn passable(&mut self) -> bool {
        while *self.pause.borrow() {
            if self.pause.changed().await.is_err() {
                // Control has gone; forwarding must not wedge behind it.
                break;
            }
        }
        !*self.cut.borrow()
    }
}

/// Resolves once this connection is cut, and never otherwise, so it can race
/// something that would only end when a packet arrives.
async fn cut(cut: &mut watch::Receiver<bool>) {
    while !*cut.borrow() {
        if cut.changed().await.is_err() {
            // Nothing left to cut us; wait forever rather than report one.
            std::future::pending::<()>().await;
        }
    }
}

impl Anchor {
    pub fn new(direction: Direction, k: u32, trip: watch::Sender<bool>) -> Self {
        Self {
            direction,
            k,
            count: Arc::new(AtomicU32::new(0)),
            active: Arc::new(AtomicBool::new(false)),
            trip,
        }
    }

    /// Tell the connections waiting on this that the anchored packet has passed.
    fn fire(&self) {
        // FIXME: propagate once arm and record can report failure. The watch
        // keeps a receiver for the whole process, so this cannot fail; dropping
        // it silently would leave the fault unplaced while the caller is told
        // the anchor tripped.
        self.trip
            .send(true)
            .expect("the anchor's watch has a live receiver");
    }

    /// Arm at scenario start: reset the count and begin counting. `k == 0`
    /// (freeze before the first scenario packet) freezes right away, now that
    /// the fleet is up rather than mid-bring-up.
    pub fn arm(&self) {
        self.count.store(0, Ordering::SeqCst);
        self.active.store(true, Ordering::SeqCst);
        if self.k == 0 {
            self.fire();
        }
    }

    /// Count one forwarded packet; freeze the fleet on the one that reaches `k`.
    /// A no-op until armed, so bring-up traffic is not counted. Returns whether
    /// this call tripped the freeze, so the caller can announce it.
    fn record(&self) -> bool {
        if !self.active.load(Ordering::SeqCst) {
            return false;
        }
        let crossed = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if crossed == self.k {
            self.fire();
            return true;
        }
        false
    }

    /// Disarm: stop counting so the anchor cannot trip again. Paired with resume
    /// on the give-up paths (the scenario ended, or the anchor timed out, before
    /// `k` was reached), so a late packet cannot re-trip the gate after the flow
    /// has been released with no one left to release it again.
    pub fn disarm(&self) {
        self.active.store(false, Ordering::SeqCst);
    }
}

pub struct Proxy {
    listener: TcpListener,
    /// Resolved per connection rather than once: the upstream is a container
    /// that may not exist when this binds, and may be a different one after a
    /// kill and restart.
    upstream: String,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    next_id: Arc<AtomicU64>,
    gate: Gate,
    anchor: Option<Anchor>,
}

impl Proxy {
    /// Bind to `listen` and prepare to forward every accepted connection to
    /// `upstream`, subject to `gate`. `anchor`, if set, trips once this pair has
    /// forwarded `k` packets on its direction. Returns the proxy, its bound
    /// local address, and the receiver for connection events.
    pub async fn bind(
        listen: SocketAddr,
        upstream: String,
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
                self.gate.clone(),
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
    upstream: String,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    gate: Gate,
    anchor: Option<Anchor>,
) {
    let upstream_conn = match TcpStream::connect(&upstream).await {
        Ok(s) => s,
        Err(e) => {
            emit(
                &events_tx,
                ConnEvent::failed(id, format!("dial upstream {upstream}: {e}")),
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

    let c2u = tokio::spawn(forward_bytes(
        id,
        client_r,
        upstream_w,
        Direction::ClientToUpstream,
        events_tx.clone(),
        gate.clone(),
        anchor_c2u,
    ));
    let u2c = tokio::spawn(forward_bytes(
        id,
        upstream_r,
        client_w,
        Direction::UpstreamToClient,
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
    id: ConnId,
    mut read: OwnedReadHalf,
    mut write: OwnedWriteHalf,
    direction: Direction,
    events: mpsc::UnboundedSender<ConnEvent>,
    mut gate: Gate,
    anchor: Option<Anchor>,
) -> Result<u64, String> {
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
            () = cut(&mut gate.cut) => break Ok(bytes_total),
        };
        let n = match read {
            Ok(0) => break Ok(bytes_total),
            Ok(n) => n,
            Err(e) => break Err(format!("{read_label}: {e}")),
        };
        emit(&events, ConnEvent::wrote(id, direction, n as u64));
        if !gate.passable().await {
            break Ok(bytes_total);
        }
        if let Err(e) = write.write_all(&buf[..n]).await {
            break Err(format!("{write_label}: {e}"));
        }
        bytes_total += n as u64;
        if let Some(anchor) = &anchor
            && anchor.record()
        {
            emit(&events, ConnEvent::froze(id, anchor.k));
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

    /// A gate that never closes.
    fn open() -> Gate {
        Gate::new(watch::channel(false).1, watch::channel(false).1)
    }

    /// A gate held by `pause`.
    fn held(pause: watch::Receiver<bool>) -> Gate {
        Gate::new(pause, watch::channel(false).1)
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
        let (proxy, proxy_addr, mut events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            echo.to_string(),
            open(),
            None,
        )
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
                ConnEventKind::Froze { k } => panic!("unexpected Froze without an anchor: k={k}"),
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
            unused.to_string(),
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
            echo.to_string(),
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
    async fn a_cut_takes_the_connection_out_from_under_an_idle_peer() {
        let echo = spawn_echo().await;
        let (cut_tx, cut_rx) = watch::channel(false);
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            echo.to_string(),
            Gate::new(watch::channel(false).1, cut_rx),
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
        cut_tx.send(true).unwrap();
        let read = timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("the connection goes within 2s of the cut");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "the peer sees the edge go away"
        );
    }

    #[test]
    fn a_dormant_anchor_does_not_count() {
        // Before arm, record is a no-op, so the fleet's bring-up traffic never
        // moves the counter or trips the gate.
        let (tx, rx) = watch::channel(false);
        let anchor = Anchor::new(Direction::ClientToUpstream, 1, tx);
        assert!(!anchor.record(), "an unarmed record must not trip");
        assert!(!*rx.borrow(), "gate stays open while dormant");
        anchor.arm();
        assert!(
            anchor.record(),
            "the first armed packet reaches k=1 and trips"
        );
        assert!(*rx.borrow(), "gate tripped once armed");
    }

    #[test]
    fn a_zero_k_anchor_trips_on_arm() {
        // k=0 means freeze before the first scenario packet, so arming (after
        // bring-up) trips the gate immediately.
        let (tx, rx) = watch::channel(false);
        let anchor = Anchor::new(Direction::ClientToUpstream, 0, tx);
        assert!(!*rx.borrow(), "not tripped before arm");
        anchor.arm();
        assert!(*rx.borrow(), "k=0 trips the gate on arm");
    }

    #[test]
    fn a_disarmed_anchor_does_not_refreeze() {
        // On a give-up path the runner releases the gate but the anchor stays
        // armed; a trailing packet that reaches k must not trip it again (there
        // would be no one left to resume). Disarming makes each arm a one-shot.
        let (tx, rx) = watch::channel(false);
        let anchor = Anchor::new(Direction::ClientToUpstream, 2, tx);
        anchor.arm();
        anchor.record(); // count 1, below k
        assert!(!*rx.borrow(), "must not trip below k");
        anchor.disarm();
        anchor.record(); // would be count 2 == k, but disarmed
        anchor.record();
        assert!(!*rx.borrow(), "a disarmed anchor must not re-trip the gate");
    }

    #[tokio::test]
    async fn anchor_emits_a_froze_event_when_it_trips() {
        let echo = spawn_echo().await;
        let (pause_tx, pause_rx) = watch::channel(false);
        let anchor = Anchor::new(Direction::ClientToUpstream, 2, pause_tx);
        let (proxy, proxy_addr, mut events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            echo.to_string(),
            held(pause_rx),
            Some(anchor.clone()),
        )
        .await
        .unwrap();
        tokio::spawn(proxy.run());
        anchor.arm();

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        // First packet is below K; forwarded and echoed normally.
        client.write_all(b"a").await.unwrap();
        let mut buf = [0u8; 1];
        client.read_exact(&mut buf).await.unwrap();
        // Second packet reaches K=2 and trips the anchor.
        client.write_all(b"b").await.unwrap();

        // The proxy must announce the freeze with a Froze event carrying k.
        let k = loop {
            let event = recv(&mut events).await;
            if let ConnEventKind::Froze { k } = event.kind {
                break k;
            }
        };
        assert_eq!(k, 2);
    }

    #[tokio::test]
    async fn anchor_trips_the_gate_after_k_packets() {
        let echo = spawn_echo().await;
        let (pause_tx, mut pause_rx) = watch::channel(false);
        let anchor = Anchor::new(Direction::ClientToUpstream, 2, pause_tx);
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            echo.to_string(),
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
}
