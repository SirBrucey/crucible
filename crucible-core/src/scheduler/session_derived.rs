//! Session-derived scheduler: N×T grid of kill schedules over the learn catalogue.

use std::time::Duration;

use crucible_protocol::{Session, SessionRef};

use super::{Schedule, Scheduler};

pub struct SessionDerivedScheduler {
    schedules: std::vec::IntoIter<Schedule>,
}

impl SessionDerivedScheduler {
    /// Build one schedule per (session × time-sample) pair. Total count is
    /// `total_budget / run_cost`, split evenly across the catalogue's sessions;
    /// each session's fault offsets are evenly spaced across its observed
    /// lifetime.
    pub fn new(catalogue: &[Session], run_cost: Duration, total_budget: Duration) -> Self {
        if catalogue.is_empty() {
            return Self {
                schedules: Vec::new().into_iter(),
            };
        }
        let budget_ns = total_budget.as_nanos();
        let cost_ns = run_cost.as_nanos().max(1);
        let n_sessions = catalogue.len() as u128;
        let total_schedules = (budget_ns / cost_ns).max(1);
        let t_per_session = usize::try_from((total_schedules / n_sessions).max(1))
            .expect("schedules per session fits usize");

        let session_end_ns = catalogue
            .iter()
            .filter_map(|s| s.closed_ns.or(Some(s.opened_ns + cost_ns)))
            .max()
            .expect("catalogue is non-empty");

        let mut schedules = Vec::with_capacity(catalogue.len() * t_per_session);
        let mut next_id: u32 = 0;
        for session in catalogue {
            let end = session.closed_ns.unwrap_or(session_end_ns);
            let duration = end.saturating_sub(session.opened_ns);
            for i in 0..t_per_session {
                let offset = duration * i as u128 / t_per_session as u128;
                schedules.push(Schedule {
                    schedule_id: next_id,
                    session: SessionRef::from(session),
                    fault_offset_ns: offset,
                    payload: Vec::new(),
                });
                next_id += 1;
            }
        }
        Self {
            schedules: schedules.into_iter(),
        }
    }
}

impl Scheduler for SessionDerivedScheduler {
    fn next(&mut self) -> Option<Schedule> {
        self.schedules.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(service: &str, conn_id: u64, opened_ns: u128, closed_ns: Option<u128>) -> Session {
        Session {
            service: service.into(),
            conn_id,
            peer: "127.0.0.1:1".into(),
            opened_ns,
            closed_ns,
            bytes_client_to_upstream: None,
            bytes_upstream_to_client: None,
        }
    }

    #[test]
    fn splits_budget_evenly_across_sessions_and_time() {
        let catalogue = vec![
            session("db", 0, 0, Some(1_000)),
            session("broker", 0, 0, Some(1_000)),
        ];
        // 100ms budget, 10ms run cost => 10 schedules total, 5 per session
        let scheduler = SessionDerivedScheduler::new(
            &catalogue,
            Duration::from_millis(10),
            Duration::from_millis(100),
        );
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert_eq!(all.len(), 10);
        let db: Vec<_> = all.iter().filter(|s| s.session.service == "db").collect();
        assert_eq!(db.len(), 5);
        let offsets: Vec<_> = db.iter().map(|s| s.fault_offset_ns).collect();
        assert_eq!(offsets, vec![0, 200, 400, 600, 800]);
    }

    #[test]
    fn empty_catalogue_yields_nothing() {
        let mut scheduler =
            SessionDerivedScheduler::new(&[], Duration::from_secs(1), Duration::from_secs(10));
        assert!(scheduler.next().is_none());
    }

    #[test]
    fn still_open_session_uses_max_observed_time_as_ceiling() {
        let catalogue = vec![session("db", 0, 0, Some(500)), session("api", 0, 100, None)];
        let scheduler = SessionDerivedScheduler::new(
            &catalogue,
            Duration::from_millis(10),
            Duration::from_millis(20),
        );
        let all: Vec<_> = std::iter::from_fn({
            let mut s = scheduler;
            move || s.next()
        })
        .collect();
        assert_eq!(all.len(), 2);
        let api = all.iter().find(|s| s.session.service == "api").unwrap();
        assert_eq!(api.fault_offset_ns, 0);
    }

    #[test]
    fn schedule_ids_are_dense_and_unique() {
        let catalogue = vec![
            session("db", 0, 0, Some(100)),
            session("broker", 0, 0, Some(100)),
            session("api", 0, 0, Some(100)),
        ];
        let mut scheduler = SessionDerivedScheduler::new(
            &catalogue,
            Duration::from_millis(10),
            Duration::from_millis(60),
        );
        let mut ids = Vec::new();
        while let Some(s) = scheduler.next() {
            ids.push(s.schedule_id);
        }
        let expected: Vec<u32> = (0..u32::try_from(ids.len()).unwrap()).collect();
        assert_eq!(ids, expected);
    }
}
