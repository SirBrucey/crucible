use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::now_ns;

pub type ConnId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConnEvent {
    pub id: ConnId,
    /// Wall-clock nanoseconds since the Unix epoch. Read from the host kernel
    /// clock so events from different sidecars sort together faithfully.
    pub ts_ns: u128,
    #[serde(flatten)]
    pub kind: ConnEventKind,
}

impl ConnEvent {
    #[must_use]
    pub fn opened(id: ConnId, peer: SocketAddr) -> Self {
        Self::opened_at(id, now_ns(), peer)
    }

    #[must_use]
    pub fn opened_at(id: ConnId, ts_ns: u128, peer: SocketAddr) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Opened { peer },
        }
    }

    #[must_use]
    pub fn closed(
        id: ConnId,
        bytes_client_to_upstream: u64,
        bytes_upstream_to_client: u64,
    ) -> Self {
        Self::closed_at(
            id,
            now_ns(),
            bytes_client_to_upstream,
            bytes_upstream_to_client,
        )
    }

    #[must_use]
    pub fn closed_at(
        id: ConnId,
        ts_ns: u128,
        bytes_client_to_upstream: u64,
        bytes_upstream_to_client: u64,
    ) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Closed {
                bytes_client_to_upstream,
                bytes_upstream_to_client,
            },
        }
    }

    pub fn failed(id: ConnId, reason: impl Into<String>) -> Self {
        Self::failed_at(id, now_ns(), reason)
    }

    pub fn failed_at(id: ConnId, ts_ns: u128, reason: impl Into<String>) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Failed {
                reason: reason.into(),
            },
        }
    }

    #[must_use]
    pub fn wrote(id: ConnId, direction: Direction, bytes: u64) -> Self {
        Self::wrote_at(id, now_ns(), direction, bytes)
    }

    #[must_use]
    pub fn wrote_at(id: ConnId, ts_ns: u128, direction: Direction, bytes: u64) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Wrote { direction, bytes },
        }
    }

    #[must_use]
    pub fn froze(id: ConnId, k: u32) -> Self {
        Self::froze_at(id, now_ns(), k)
    }

    #[must_use]
    pub fn froze_at(id: ConnId, ts_ns: u128, k: u32) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Froze { k },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub enum ConnEventKind {
    Opened {
        peer: SocketAddr,
    },
    /// A non-empty chunk was forwarded on the connection.
    Wrote {
        direction: Direction,
        bytes: u64,
    },
    Closed {
        bytes_client_to_upstream: u64,
        bytes_upstream_to_client: u64,
    },
    Failed {
        reason: String,
    },
    /// The fault anchor tripped: this pair has forwarded `k` packets on its
    /// direction and the proxy has frozen the fleet. Emitted once per arm, at the
    /// trip, so a consumer can gate an action on the freeze actually being in
    /// place rather than on its own independent packet count.
    Froze {
        k: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Direction {
    ClientToUpstream,
    UpstreamToClient,
}
