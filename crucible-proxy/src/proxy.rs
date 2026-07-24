//! Bytes-through TCP forwarder that emits connection events.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crucible_protocol::{ConnEvent, ConnId, Direction};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

pub struct Proxy {
    listener: TcpListener,
    upstream: SocketAddr,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    next_id: Arc<AtomicU64>,
}

impl Proxy {
    /// Bind to `listen` and prepare to forward every accepted connection to `upstream`.
    /// Returns the proxy, its bound local address, and the receiver for connection events.
    pub async fn bind(
        listen: SocketAddr,
        upstream: SocketAddr,
    ) -> io::Result<(Self, SocketAddr, mpsc::UnboundedReceiver<ConnEvent>)> {
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let proxy = Self {
            listener,
            upstream,
            events_tx,
            next_id: Arc::new(AtomicU64::new(0)),
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
            ));
        }
    }
}

async fn forward(
    id: ConnId,
    client: TcpStream,
    peer: SocketAddr,
    upstream: SocketAddr,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
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

    let events_tx_c2u = events_tx.clone();
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
            if let Err(e) = upstream_w.write_all(&buf[..n]).await {
                break Err(format!("upstream_write: {e}"));
            }
            bytes_total += n as u64;
        }
    });

    let events_tx_u2c = events_tx.clone();
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
            if let Err(e) = client_w.write_all(&buf[..n]).await {
                break Err(format!("client_write: {e}"));
            }
            bytes_total += n as u64;
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
        let (proxy, proxy_addr, mut events) = Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), echo)
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
        // Two opens + two closes + variable number of wrote events (at least 4 — one c2u and one u2c per conn).
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
        let (proxy, proxy_addr, mut events) = Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), unused)
            .await
            .unwrap();
        tokio::spawn(proxy.run());

        let _client = TcpStream::connect(proxy_addr).await.unwrap();

        let event = recv(&mut events).await;
        assert!(matches!(event.kind, ConnEventKind::Failed { .. }));
    }
}
