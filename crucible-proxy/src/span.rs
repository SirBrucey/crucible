//! Moments inside a service, held the way the proxy holds moments on the wire.
//!
//! An instrumented service reports its own span boundaries here, and the one a
//! schedule named does not answer until the framework has placed the fault and
//! let the fleet go. Services reach this through the same proxy they already send
//! everything through.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    routing::{get, post},
};
use crucible_protocol::{Boundary, ConnEvent, ConnId, Released, Watching, now_ns};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};

use crate::{fleet::Named, proxy::Anchor};

/// The connection id boundaries are reported under, since they arrive over a
/// request of their own rather than on a fleet connection.
const INSIDE: ConnId = 0;

/// What the proxy does with the moments a service reports.
pub struct Spans {
    /// The moment this run holds at.
    watching: Watching,
    /// Fired when that moment is reached, which stops the fleet.
    anchor: Option<Anchor>,
    /// Lifted once the framework has placed the fault.
    pause: watch::Receiver<bool>,
    /// Where a boundary goes, tagged with the service that reported it.
    events: mpsc::UnboundedSender<(String, ConnEvent)>,
    /// Who is at each address, for attributing a report to a service.
    named: Arc<Named>,
}

impl Spans {
    #[must_use]
    pub fn new(
        watching: Watching,
        anchor: Option<Anchor>,
        pause: watch::Receiver<bool>,
        events: mpsc::UnboundedSender<(String, ConnEvent)>,
        named: Arc<Named>,
    ) -> Self {
        Self {
            watching,
            anchor,
            pause,
            events,
            named,
        }
    }

    /// A service has reached `boundary`. Answers when it may carry on.
    async fn reached(&self, peer: IpAddr, boundary: Boundary) -> Released {
        let mark = boundary.mark();
        // A service is not told its own name, the proxy recognises it by where
        // it reported from. A service the fleet cannot name is one where no fault can be
        // placed, so the moment is passed over rather than reported under an
        // address.
        let Some(service) = self.named.at(peer).map(ToOwned::to_owned) else {
            tracing::warn!(%peer, %mark, "a moment was reported by nothing the fleet names");
            return Released { at_ns: now_ns() };
        };
        tracing::debug!(%service, %mark, "a service reached a moment inside itself");
        let _ = self
            .events
            .send((service.clone(), ConnEvent::reached(INSIDE, boundary)));

        let armed = self
            .anchor
            .as_ref()
            .is_some_and(|anchor| self.watching.holds(&mark) && anchor.reached_inside(&service));
        if !armed {
            return Released { at_ns: now_ns() };
        }
        // The framework waits on one signal whether the moment was on the wire or inside a service.
        let _ = self.events.send((service, ConnEvent::froze(INSIDE, mark)));

        // The fleet is stopped and the framework is placing the fault. The
        // service waits here, until the fleet is let go.
        let mut pause = self.pause.clone();
        while *pause.borrow() {
            if pause.changed().await.is_err() {
                break;
            }
        }
        Released { at_ns: now_ns() }
    }
}

async fn watching(State(spans): State<Arc<Spans>>) -> Json<Watching> {
    Json(spans.watching.clone())
}

async fn reached(
    State(spans): State<Arc<Spans>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(boundary): Json<Boundary>,
) -> Json<Released> {
    Json(spans.reached(peer.ip(), boundary).await)
}

/// Everything an instrumented service needs to talk to.
fn routes(spans: Arc<Spans>) -> Router {
    Router::new()
        .route("/watching", get(watching))
        .route("/boundary", post(reached))
        .with_state(spans)
}

/// Listen for the moments services report, and say where.
///
/// # Errors
/// Errors if the address cannot be bound.
pub async fn listen(spans: Arc<Spans>, addr: &str) -> std::io::Result<std::net::SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let routes = routes(spans).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, routes).await {
            tracing::error!(%e, "stopped listening for the moments inside services");
        }
    });
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crucible_protocol::{ConnEventKind, Side};

    use super::*;

    /// Where the test's service reports from.
    const REPORTER: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    /// Another service the fleet names, which reaches the same marks.
    const OTHER: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 4));

    fn boundary(span: &str, nth: u32) -> Boundary {
        Boundary {
            span: span.to_owned(),
            side: Side::Started,
            nth,
        }
    }

    /// A proxy under test.
    struct Under {
        spans: Arc<Spans>,
        /// The gate the framework lifts once it has placed the fault.
        pause: watch::Sender<bool>,
        reported: mpsc::UnboundedReceiver<(String, ConnEvent)>,
        /// Held only so firing the anchor has somewhere to say so.
        _trip: watch::Receiver<bool>,
    }

    fn proxy(at: Option<&str>) -> Under {
        let (pause_tx, pause_rx) = watch::channel(false);
        let (trip_tx, trip_rx) = watch::channel(false);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let watching = at.map_or(Watching::Reporting, |mark| Watching::Holding {
            at: vec![mark.to_owned()],
        });
        let anchor = at.map(|mark| {
            let anchor =
                Anchor::inside("api".to_owned(), mark.to_owned(), pause_tx.clone(), trip_tx);
            anchor.arm();
            anchor
        });
        let named = Arc::new(Named::known(std::collections::HashMap::from([
            (REPORTER, "api".to_owned()),
            (OTHER, "worker".to_owned()),
        ])));
        Under {
            spans: Arc::new(Spans::new(watching, anchor, pause_rx, events_tx, named)),
            pause: pause_tx,
            reported: events_rx,
            _trip: trip_rx,
        }
    }

    #[tokio::test]
    async fn a_moment_the_fleet_cannot_name_is_passed_over() {
        let mut under = proxy(None);
        let stranger = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 9));

        let answered = tokio::time::timeout(
            Duration::from_millis(200),
            under.spans.reached(stranger, boundary("publish", 1)),
        )
        .await;

        assert!(answered.is_ok(), "the service was left waiting");
        assert!(
            under.reported.try_recv().is_err(),
            "a moment nothing names was reported anyway"
        );
    }

    #[tokio::test]
    async fn every_boundary_is_reported() {
        let mut under = proxy(None);
        under.spans.reached(REPORTER, boundary("publish", 1)).await;

        let Some((service, event)) = under.reported.recv().await else {
            panic!("nothing was reported");
        };
        assert_eq!(service, "api", "attributed to whoever reported it");
        assert!(matches!(event.kind, ConnEventKind::Reached { .. }));
    }

    #[tokio::test]
    async fn a_moment_the_run_did_not_name_is_answered_at_once() {
        let under = proxy(Some("publish:1:start"));
        let answered = tokio::time::timeout(
            Duration::from_millis(200),
            under.spans.reached(REPORTER, boundary("handle", 1)),
        )
        .await;
        assert!(answered.is_ok(), "a moment nothing named should not wait");
    }

    /// A mark is unique only within a service, so two of them can each reach
    /// their own first publish.
    #[tokio::test]
    async fn another_service_reaching_the_same_mark_is_not_held() {
        let under = proxy(Some("publish:1:start"));
        let answered = tokio::time::timeout(
            Duration::from_millis(200),
            under.spans.reached(OTHER, boundary("publish", 1)),
        )
        .await;
        assert!(answered.is_ok(), "another service's moment should not wait");
        assert!(
            !*under.pause.borrow(),
            "the fleet was stopped by the wrong service"
        );
    }

    /// Holding the service where it said it got to is what puts the fault
    /// inside it.
    #[tokio::test]
    async fn the_named_moment_holds_until_the_fleet_is_let_go() {
        let mut under = proxy(Some("publish:1:start"));

        let held = tokio::spawn({
            let spans = Arc::clone(&under.spans);
            async move { spans.reached(REPORTER, boundary("publish", 1)).await }
        });

        // Reaching it stops the fleet, and the service is still waiting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(*under.pause.borrow(), "the fleet was not stopped");
        assert!(!held.is_finished(), "the service was let go too early");

        // The framework waits on one signal whether the moment was on the wire
        // or inside a service, so a held moment says the fleet froze.
        let said: Vec<_> = std::iter::from_fn(|| under.reported.try_recv().ok()).collect();
        assert!(
            said.iter()
                .any(|(_, event)| matches!(&event.kind, ConnEventKind::Froze { mark } if mark == "publish:1:start")),
            "the freeze was never reported, so nothing would place the fault"
        );

        under.pause.send(false).expect("the fleet is let go");
        let released = tokio::time::timeout(Duration::from_secs(2), held)
            .await
            .expect("the service carried on")
            .expect("answered");
        assert!(released.at_ns > 0);
    }
}
