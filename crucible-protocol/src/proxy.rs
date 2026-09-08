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
    pub fn placeable(id: ConnId, placement: crate::Placement) -> Self {
        Self {
            id,
            ts_ns: now_ns(),
            kind: ConnEventKind::Placeable { placement },
        }
    }

    #[must_use]
    pub fn reached(id: ConnId, boundary: crate::Boundary) -> Self {
        Self {
            id,
            ts_ns: now_ns(),
            kind: ConnEventKind::Reached { boundary },
        }
    }

    #[must_use]
    pub fn did(id: ConnId, did: crate::Did) -> Self {
        Self {
            id,
            ts_ns: now_ns(),
            kind: ConnEventKind::Did { did },
        }
    }

    #[must_use]
    pub fn froze(id: ConnId, mark: String) -> Self {
        Self::froze_at(id, now_ns(), mark)
    }

    #[must_use]
    pub fn froze_at(id: ConnId, ts_ns: u128, mark: String) -> Self {
        Self {
            id,
            ts_ns,
            kind: ConnEventKind::Froze { mark },
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
    /// The moment a schedule named has come, and the fleet is held on it.
    /// Emitted once per arm, so a consumer can gate an action on the freeze
    /// being in place rather than on a count of its own.
    Froze {
        mark: String,
    },
    /// Somewhere the plugin reading this connection says a fault could go.
    Placeable {
        placement: crate::Placement,
    },
    /// What the plugin reading this connection made of the fault it was asked
    /// to place.
    Did {
        did: crate::Did,
    },
    /// A moment inside a service, reported by the service itself.
    Reached {
        boundary: crate::Boundary,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Direction {
    ClientToUpstream,
    UpstreamToClient,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn boundary() -> crate::Boundary {
        crate::Boundary {
            span: "publish".to_owned(),
            side: crate::Side::Started,
            nth: 1,
        }
    }

    #[rstest]
    #[case(ConnEvent::opened(1, "10.0.0.4:5672".parse().expect("an address")))]
    #[case(ConnEvent::wrote(1, Direction::ClientToUpstream, 512))]
    #[case(ConnEvent::froze(1, "publish:1:start".to_owned()))]
    #[case(ConnEvent::reached(0, boundary()))]
    fn event_serialisation_roundtrip(#[case] event: ConnEvent) {
        let written = serde_json::to_string(&event).expect("written");
        let read: ConnEvent = serde_json::from_str(&written)
            .unwrap_or_else(|e| panic!("{written} could not be read back: {e}"));

        assert_eq!(read, event);
    }
}
