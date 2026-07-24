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
    pub fn opened(id: ConnId, peer: SocketAddr) -> Self {
        Self::opened_at(id, now_ns(), peer)
    }

    pub fn opened_at(id: ConnId, ts_ns: u128, peer: SocketAddr) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Opened { peer },
        }
    }

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
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub enum ConnEventKind {
    Opened {
        peer: SocketAddr,
    },
    Closed {
        bytes_client_to_upstream: u64,
        bytes_upstream_to_client: u64,
    },
    Failed {
        reason: String,
    },
}
