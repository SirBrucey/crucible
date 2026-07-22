//! Reconstruct `Session` records from sidecar proxy log lines.

use std::collections::HashMap;

use crucible_protocol::{ConnEvent, ConnEventKind, ConnId, Session};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("parse conn event: {source} in line: {line}")]
    Parse {
        source: serde_json::Error,
        line: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Sessions observed across a Learn run.
#[derive(Default)]
pub struct Sessions {
    opened: HashMap<(String, ConnId), (u128, String)>,
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
                self.opened
                    .insert((service.to_string(), id), (ts_ns, peer.to_string()));
            }
            ConnEventKind::Closed {
                bytes_client_to_upstream,
                bytes_upstream_to_client,
            } => {
                if let Some((opened_ns, peer)) = self.opened.remove(&(service.to_string(), id)) {
                    self.finished.push(Session {
                        service: service.to_string(),
                        conn_id: id,
                        peer,
                        opened_ns,
                        closed_ns: Some(ts_ns),
                        bytes_client_to_upstream: Some(bytes_client_to_upstream),
                        bytes_upstream_to_client: Some(bytes_upstream_to_client),
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
        for ((service, conn_id), (opened_ns, peer)) in self.opened.drain() {
            self.finished.push(Session {
                service,
                conn_id,
                peer,
                opened_ns,
                closed_ns: None,
                bytes_client_to_upstream: None,
                bytes_upstream_to_client: None,
            });
        }
        self.finished
            .sort_by_key(|s| (s.opened_ns, s.service.clone(), s.conn_id));
        self.finished.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_opens_and_closes_across_services() {
        let out: Vec<Session> = [
            (
                "db".into(),
                ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
            ),
            (
                "api".into(),
                ConnEvent::opened_at(0, 105, "127.0.0.1:2".parse().unwrap()),
            ),
            ("db".into(), ConnEvent::closed_at(0, 200, 3, 4)),
            ("api".into(), ConnEvent::closed_at(0, 210, 5, 6)),
        ]
        .into_iter()
        .collect::<Sessions>()
        .into_iter()
        .collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].service, "db");
        assert_eq!(out[0].opened_ns, 100);
        assert_eq!(out[0].closed_ns, Some(200));
        assert_eq!(out[0].bytes_client_to_upstream, Some(3));
        assert_eq!(out[1].service, "api");
        assert_eq!(out[1].opened_ns, 105);
    }

    #[test]
    fn same_conn_id_across_services_does_not_collide() {
        let out: Vec<Session> = Sessions::from_iter([
            (
                "db".into(),
                ConnEvent::opened_at(7, 100, "127.0.0.1:1".parse().unwrap()),
            ),
            (
                "api".into(),
                ConnEvent::opened_at(7, 110, "127.0.0.1:2".parse().unwrap()),
            ),
            ("db".into(), ConnEvent::closed_at(7, 200, 3, 4)),
            ("api".into(), ConnEvent::closed_at(7, 210, 5, 6)),
        ])
        .into_iter()
        .collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().any(|s| s.service == "db"));
        assert!(out.iter().any(|s| s.service == "api"));
    }

    #[test]
    fn open_without_close_is_kept_as_still_open() {
        let out: Vec<Session> = Sessions::from_iter([(
            "api".into(),
            ConnEvent::opened_at(0, 100, "127.0.0.1:1".parse().unwrap()),
        )])
        .into_iter()
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].opened_ns, 100);
        assert_eq!(out[0].closed_ns, None);
        assert_eq!(out[0].bytes_client_to_upstream, None);
        assert_eq!(out[0].bytes_upstream_to_client, None);
    }

    #[test]
    fn failed_event_drops_pending_open() {
        let out: Vec<Session> = Sessions::from_iter([
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

    #[test]
    fn accept_line_errors_on_malformed_json() {
        let mut sessions = Sessions::new();
        assert!(matches!(
            sessions.accept_line("api", "not-json"),
            Err(Error::Parse { .. })
        ));
    }

    #[test]
    fn accept_line_skips_empty_and_parses_valid() {
        let mut sessions = Sessions::new();
        sessions.accept_line("api", "").unwrap();
        sessions
            .accept_line(
                "api",
                r#"{"id":0,"ts_ns":100,"kind":"Opened","peer":"127.0.0.1:1"}"#,
            )
            .unwrap();
        sessions.accept_line("api", "  ").unwrap();
        sessions
            .accept_line(
                "api",
                r#"{"id":0,"ts_ns":200,"kind":"Closed","bytes_client_to_upstream":1,"bytes_upstream_to_client":2}"#,
            )
            .unwrap();
        let out: Vec<Session> = sessions.into_iter().collect();
        assert_eq!(out.len(), 1);
    }
}
