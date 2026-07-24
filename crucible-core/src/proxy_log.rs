//! Reconstruct `Session` records from sidecar proxy log lines.

use std::collections::{BTreeMap, HashMap};

use crucible_protocol::{
    ConnEvent, ConnEventKind, ConnId, HISTOGRAM_BIN_NS, ServiceProfile, Session, WriteRecord,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("parse conn event: {source} in line: {line}")]
    Parse {
        source: serde_json::Error,
        line: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

struct Pending {
    opened_ns: u128,
    peer: String,
    writes: Vec<WriteRecord>,
}

/// Sessions observed across a Learn run.
#[derive(Default)]
pub struct Sessions {
    opened: HashMap<(String, ConnId), Pending>,
    finished: Vec<Session>,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accept_event(&mut self, service: &str, event: ConnEvent) {
        let ConnEvent { id, ts_ns, kind } = event;
        match kind {
            ConnEventKind::Opened { peer } => {
                self.opened.insert(
                    (service.to_string(), id),
                    Pending {
                        opened_ns: ts_ns,
                        peer: peer.to_string(),
                        writes: Vec::new(),
                    },
                );
            }
            ConnEventKind::Wrote { direction, bytes } => {
                if let Some(pending) = self.opened.get_mut(&(service.to_string(), id)) {
                    pending.writes.push(WriteRecord {
                        ts_ns,
                        direction,
                        bytes,
                    });
                }
            }
            ConnEventKind::Closed { .. } => {
                if let Some(pending) = self.opened.remove(&(service.to_string(), id)) {
                    self.finished.push(Session {
                        service: service.to_string(),
                        conn_id: id,
                        peer: pending.peer,
                        opened_ns: pending.opened_ns,
                        closed_ns: Some(ts_ns),
                        writes: pending.writes,
                    });
                }
            }
            ConnEventKind::Failed { .. } => {
                self.opened.remove(&(service.to_string(), id));
            }
        }
    }

    pub fn accept_line(&mut self, service: &str, line: &str) -> Result<()> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }
        let event: ConnEvent = serde_json::from_str(line).map_err(|source| Error::Parse {
            source,
            line: line.to_string(),
        })?;
        self.accept_event(service, event);
        Ok(())
    }
}

/// Bin the write events across a session catalogue into per-service
/// `ServiceProfile`s scenario-relative to `scenario_start_ns`. Writes before
/// scenario start are ignored.
pub fn service_profiles_from_sessions(
    sessions: &[Session],
    scenario_start_ns: u128,
) -> Vec<ServiceProfile> {
    let mut by_service: BTreeMap<String, BTreeMap<u128, u64>> = BTreeMap::new();
    for session in sessions {
        for write in &session.writes {
            if write.ts_ns < scenario_start_ns {
                continue;
            }
            let bin = (write.ts_ns - scenario_start_ns) / HISTOGRAM_BIN_NS;
            let entry = by_service
                .entry(session.service.clone())
                .or_default()
                .entry(bin)
                .or_insert(0);
            *entry = entry.saturating_add(write.bytes);
        }
    }
    by_service
        .into_iter()
        .map(|(service, bins)| ServiceProfile {
            service,
            bins: bins.into_iter().collect(),
        })
        .collect()
}

impl Extend<(String, ConnEvent)> for Sessions {
    fn extend<I: IntoIterator<Item = (String, ConnEvent)>>(&mut self, iter: I) {
        for (service, event) in iter {
            self.accept_event(&service, event);
        }
    }
}

impl FromIterator<(String, ConnEvent)> for Sessions {
    fn from_iter<I: IntoIterator<Item = (String, ConnEvent)>>(iter: I) -> Self {
        let mut sessions = Self::new();
        sessions.extend(iter);
        sessions
    }
}

impl IntoIterator for Sessions {
    type Item = Session;
    type IntoIter = std::vec::IntoIter<Session>;

    fn into_iter(mut self) -> Self::IntoIter {
        for ((service, conn_id), pending) in self.opened.drain() {
            self.finished.push(Session {
                service,
                conn_id,
                peer: pending.peer,
                opened_ns: pending.opened_ns,
                closed_ns: None,
                writes: pending.writes,
            });
        }
        self.finished
            .sort_by_key(|s| (s.opened_ns, s.service.clone(), s.conn_id));
        self.finished.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::Direction;

    use super::*;

    #[test]
    fn writes_are_folded_into_session() {
        let mut sessions = Sessions::new();
        sessions.accept_event(
            "db",
            ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
        );
        sessions.accept_event(
            "db",
            ConnEvent::wrote_at(0, 150, Direction::ClientToUpstream, 32),
        );
        sessions.accept_event(
            "db",
            ConnEvent::wrote_at(0, 180, Direction::UpstreamToClient, 64),
        );
        sessions.accept_event("db", ConnEvent::closed_at(0, 200, 0, 0));
        let out: Vec<_> = sessions.into_iter().collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].writes.len(), 2);
        assert_eq!(out[0].writes[0].ts_ns, 150);
        assert_eq!(out[0].writes[0].direction, Direction::ClientToUpstream);
        assert_eq!(out[0].writes[0].bytes, 32);
    }

    #[test]
    fn open_without_close_keeps_writes() {
        let out: Vec<_> = Sessions::from_iter([
            (
                "api".into(),
                ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
            ),
            (
                "api".into(),
                ConnEvent::wrote_at(0, 120, Direction::ClientToUpstream, 16),
            ),
        ])
        .into_iter()
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].closed_ns, None);
        assert_eq!(out[0].writes.len(), 1);
    }

    #[test]
    fn failed_drops_pending() {
        let out: Vec<_> = Sessions::from_iter([
            (
                "api".into(),
                ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
            ),
            (
                "api".into(),
                ConnEvent::failed_at(0, 150, "upstream refused"),
            ),
        ])
        .into_iter()
        .collect();
        assert!(out.is_empty());
    }
}
