//! L1 session proxy: bytes-through TCP forwarder with connection tracking.

use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

pub type ConnId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnEvent {
    pub id: ConnId,
    pub kind: ConnEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEventKind {
    Opened {
        peer: SocketAddr,
    },
    Closed {
        bytes_client_to_upstream: u64,
        bytes_upstream_to_client: u64,
    },
    /// Forwarding failed before or during byte transfer.
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct ConnMeta {
    pub id: ConnId,
    pub peer: SocketAddr,
    pub opened_at: Instant,
}

pub struct Proxy {
    listener: TcpListener,
    upstream: SocketAddr,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
    conns: Arc<Mutex<HashMap<ConnId, ConnMeta>>>,
    next_id: Arc<AtomicU64>,
}

/// Read-only view of a running proxy's local address and open connections.
#[derive(Clone)]
pub struct ProxyHandle {
    local_addr: SocketAddr,
    conns: Arc<Mutex<HashMap<ConnId, ConnMeta>>>,
}

impl ProxyHandle {
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// A snapshot of the currently-open connections.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ConnMeta> {
        self.conns
            .lock()
            .expect("proxy conns mutex poisoned")
            .values()
            .cloned()
            .collect()
    }
}

impl Proxy {
    /// Bind to `listen` and prepare to forward every accepted connection to `upstream`.
    pub async fn bind(
        listen: SocketAddr,
        upstream: SocketAddr,
    ) -> io::Result<(Self, ProxyHandle, mpsc::UnboundedReceiver<ConnEvent>)> {
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let conns = Arc::new(Mutex::new(HashMap::new()));
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let next_id = Arc::new(AtomicU64::new(0));
        let handle = ProxyHandle {
            local_addr,
            conns: conns.clone(),
        };
        let proxy = Self {
            listener,
            upstream,
            events_tx,
            conns,
            next_id,
        };
        Ok((proxy, handle, events_rx))
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
                self.conns.clone(),
                self.events_tx.clone(),
            ));
        }
    }
}

async fn forward(
    id: ConnId,
    mut client: TcpStream,
    peer: SocketAddr,
    upstream: SocketAddr,
    conns: Arc<Mutex<HashMap<ConnId, ConnMeta>>>,
    events_tx: mpsc::UnboundedSender<ConnEvent>,
) {
    let mut upstream_conn = match TcpStream::connect(upstream).await {
        Ok(s) => s,
        Err(e) => {
            let _ = events_tx.send(ConnEvent {
                id,
                kind: ConnEventKind::Failed {
                    reason: format!("dial upstream {upstream}: {e}"),
                },
            });
            return;
        }
    };

    let meta = ConnMeta {
        id,
        peer,
        opened_at: Instant::now(),
    };
    conns
        .lock()
        .expect("proxy conns mutex poisoned")
        .insert(id, meta);
    let _ = events_tx.send(ConnEvent {
        id,
        kind: ConnEventKind::Opened { peer },
    });

    let event = match tokio::io::copy_bidirectional(&mut client, &mut upstream_conn).await {
        Ok((up, down)) => ConnEventKind::Closed {
            bytes_client_to_upstream: up,
            bytes_upstream_to_client: down,
        },
        Err(e) => ConnEventKind::Failed {
            reason: format!("forwarding: {e}"),
        },
    };

    conns
        .lock()
        .expect("proxy conns mutex poisoned")
        .remove(&id);
    let _ = events_tx.send(ConnEvent { id, kind: event });
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, time::Duration};

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
    async fn round_trips_two_concurrent_connections_with_distinct_tracking() {
        let echo = spawn_echo().await;
        let (proxy, handle, mut events) = Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), echo)
            .await
            .unwrap();
        let proxy_addr = handle.local_addr();
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

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.len(), 2);
        let ids: std::collections::HashSet<_> = snapshot.iter().map(|m| m.id).collect();
        assert_eq!(ids, [0, 1].into_iter().collect());
        let peers: std::collections::HashSet<_> = snapshot.iter().map(|m| m.peer).collect();
        assert_eq!(peers.len(), 2);

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();

        let mut opened = std::collections::HashMap::new();
        let mut closed = std::collections::HashMap::new();
        for _ in 0..4 {
            let event = recv(&mut events).await;
            match event.kind {
                ConnEventKind::Opened { peer } => {
                    opened.insert(event.id, peer);
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

        assert!(handle.snapshot().is_empty());
    }

    #[tokio::test]
    async fn emits_failed_event_when_upstream_unreachable() {
        let unused = {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            listener.local_addr().unwrap()
        };
        let (proxy, handle, mut events) = Proxy::bind((Ipv4Addr::LOCALHOST, 0).into(), unused)
            .await
            .unwrap();
        let proxy_addr = handle.local_addr();
        tokio::spawn(proxy.run());

        let _client = TcpStream::connect(proxy_addr).await.unwrap();

        let event = recv(&mut events).await;
        assert!(matches!(event.kind, ConnEventKind::Failed { .. }));
        assert!(handle.snapshot().is_empty());
    }
}
