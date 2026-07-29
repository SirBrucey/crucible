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
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
};

/// The fault anchor for a pair: once `k` packets have been forwarded on
/// `direction` (counted across all this pair's connections), trip the shared
/// pause gate so the whole fleet freezes at exactly that packet.
///
/// Dormant until [`arm`](Self::arm), so it counts only scenario traffic, not the
/// fleet's own bring-up handshakes. The runner arms it once the scenario starts,
/// which resets the count to zero; that shared origin is what makes the count
/// match the scenario-relative one the learn pass measured.
#[derive(Clone)]
pub struct Anchor {
    pub direction: Direction,
    pub k: u32,
    count: Arc<AtomicU32>,
    active: Arc<AtomicBool>,
    tripwire: watch::Sender<bool>,
}

impl Anchor {
    pub fn new(direction: Direction, k: u32, tripwire: watch::Sender<bool>) -> Self {
        Self {
            direction,
            k,
            count: Arc::new(AtomicU32::new(0)),
            active: Arc::new(AtomicBool::new(false)),
            tripwire,
        }
    }

    /// Arm at scenario start: reset the count and begin counting. `k == 0`
    /// (freeze before the first scenario packet) freezes right away, now that
    /// the fleet is up rather than mid-bring-up.
    pub fn arm(&self) {
        self.count.store(0, Ordering::SeqCst);
        self.active.store(true, Ordering::SeqCst);
        if self.k == 0 {
            let _ = self.tripwire.send(true);
        }
    }

    /// Count one forwarded packet; freeze the fleet on the one that reaches `k`.
    /// A no-op until armed, so bring-up traffic is not counted.
    fn record(&self) {
        if !self.active.load(Ordering::SeqCst) {
            return;
        }
        let crossed = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if crossed == self.k {
            let _ = self.tripwire.send(true);
        }
    }
}

pub struct Proxy {
    listener: TcpListener,
    upstream: SocketAddr,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    next_id: Arc<AtomicU64>,
    pause: watch::Receiver<bool>,
    anchor: Option<Anchor>,
}

impl Proxy {
    /// Bind to `listen` and prepare to forward every accepted connection to `upstream`.
    /// `pause` gates forwarding: while it holds `true`, every connection holds its
    /// bytes (delivering none) until it flips back to `false`. `anchor`, if set,
    /// freezes the fleet once this pair has forwarded `k` packets on its direction.
    /// Returns the proxy, its bound local address, and the receiver for connection events.
    pub async fn bind(
        listen: SocketAddr,
        upstream: SocketAddr,
        pause: watch::Receiver<bool>,
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
            pause,
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
                self.upstream,
                self.events_tx.clone(),
                self.pause.clone(),
                self.anchor.clone(),
            ));
        }
    }
}

/// Block while the pause gate holds `true`, returning as soon as it is `false`
/// (or the sender is dropped, so forwarding never wedges if control goes away).
async fn wait_while_paused(pause: &mut watch::Receiver<bool>) {
    while *pause.borrow() {
        if pause.changed().await.is_err() {
            return;
        }
    }
}

async fn forward(
    id: ConnId,
    client: TcpStream,
    peer: SocketAddr,
    upstream: SocketAddr,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    pause: watch::Receiver<bool>,
    anchor: Option<Anchor>,
) {
    let upstream_conn = match TcpStream::connect(upstream).await {
        Ok(s) => s,
        Err(e) => {
            let _ = events_tx.send(ConnEvent::failed(
                id,
                format!("dial upstream {upstream}: {e}"),
            ));
            return;
        }
    };

    let _ = events_tx.send(ConnEvent::opened(id, peer));

    let (mut client_r, mut client_w) = client.into_split();
    let (mut upstream_r, mut upstream_w) = upstream_conn.into_split();

    let anchor_c2u = anchor
        .clone()
        .filter(|a| a.direction == Direction::ClientToUpstream);
    let anchor_u2c = anchor.filter(|a| a.direction == Direction::UpstreamToClient);

    let events_tx_c2u = events_tx.clone();
    let mut pause_c2u = pause.clone();
    let c2u = tokio::spawn(async move {
        let mut bytes_total: u64 = 0;
        let mut buf = vec![0u8; 4096];
        loop {
            let n = match client_r.read(&mut buf).await {
                Ok(0) => break Ok(bytes_total),
                Ok(n) => n,
                Err(e) => break Err(format!("client_read: {e}")),
            };
            let _ = events_tx_c2u.send(ConnEvent::wrote(id, Direction::ClientToUpstream, n as u64));
            wait_while_paused(&mut pause_c2u).await;
            if let Err(e) = upstream_w.write_all(&buf[..n]).await {
                break Err(format!("upstream_write: {e}"));
            }
            bytes_total += n as u64;
            if let Some(anchor) = &anchor_c2u {
                anchor.record();
            }
        }
    });

    let events_tx_u2c = events_tx.clone();
    let mut pause_u2c = pause;
    let u2c = tokio::spawn(async move {
        let mut bytes_total: u64 = 0;
        let mut buf = vec![0u8; 4096];
        loop {
            let n = match upstream_r.read(&mut buf).await {
                Ok(0) => break Ok(bytes_total),
                Ok(n) => n,
                Err(e) => break Err(format!("upstream_read: {e}")),
            };
            let _ = events_tx_u2c.send(ConnEvent::wrote(id, Direction::UpstreamToClient, n as u64));
            wait_while_paused(&mut pause_u2c).await;
            if let Err(e) = client_w.write_all(&buf[..n]).await {
                break Err(format!("client_write: {e}"));
            }
            bytes_total += n as u64;
            if let Some(anchor) = &anchor_u2c {
                anchor.record();
            }
        }
    });

    let (c2u_res, u2c_res) = tokio::join!(c2u, u2c);
    let event = match (
        c2u_res.unwrap_or(Err("c2u task panicked".into())),
        u2c_res.unwrap_or(Err("u2c task panicked".into())),
    ) {
        (Ok(up), Ok(down)) => ConnEvent::closed(id, up, down),
        (Err(e), _) | (_, Err(e)) => ConnEvent::failed(id, format!("forwarding: {e}")),
    };
    let _ = events_tx.send(event);
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

    /// A pause receiver that never pauses, for tests that do not exercise the gate.
    fn never_paused() -> watch::Receiver<bool> {
        watch::channel(false).1
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
            Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), echo, never_paused(), None)
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
            unused,
            never_paused(),
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
        let (proxy, proxy_addr, _events) =
            Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), echo, pause_rx, None)
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
    async fn anchor_trips_the_gate_after_k_packets() {
        let echo = spawn_echo().await;
        let (pause_tx, mut pause_rx) = watch::channel(false);
        let anchor = Anchor::new(Direction::ClientToUpstream, 2, pause_tx);
        let (proxy, proxy_addr, _events) = Proxy::bind(
            (Ipv4Addr::LOCALHOST, 0).into(),
            echo,
            pause_rx.clone(),
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
