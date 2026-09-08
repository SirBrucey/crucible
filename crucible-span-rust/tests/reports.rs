//! The adapter against a framework that answers over HTTP.
//!
//! A learn run finds the moments a service offers by being told all of them, so
//! what matters is that every report arrives.

use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    routing::{get, post},
};
use crucible_protocol::{Boundary, Released, Watching};
use crucible_span_rust::Boundaries;
use tracing::Instrument;
use tracing_subscriber::{Layer, layer::SubscriberExt};

/// A framework that takes every boundary and answers at once.
async fn framework() -> (String, Arc<Mutex<Vec<String>>>) {
    let taken: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&taken);
    let routes = Router::new()
        .route("/watching", get(async || Json(Watching::Reporting)))
        .route(
            "/boundary",
            post(async move |Json(boundary): Json<Boundary>| {
                seen.lock().expect("nothing panicked").push(boundary.mark());
                Json(Released { at_ns: 0 })
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bound");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move {
        axum::serve(listener, routes).await.expect("served");
    });
    (format!("http://{at}"), taken)
}

/// A run that hears only a service's first few moments would schedule against a
/// fleet it has half seen.
#[tokio::test(flavor = "multi_thread")]
async fn every_boundary_a_busy_service_reaches_is_reported() {
    let (at, taken) = framework().await;
    // Joining does a request of its own, so it runs where blocking is allowed.
    let boundaries = tokio::task::spawn_blocking(move || Boundaries::joined(&at).expect("joined"))
        .await
        .expect("joined");
    let subscriber = tracing_subscriber::registry().with(boundaries.with_filter(
        tracing_subscriber::filter::filter_fn(|span| span.name() == "publish"),
    ));
    let _guard = tracing::subscriber::set_default(subscriber);

    for _ in 0..30 {
        let _span = tracing::info_span!("publish").entered();
    }

    let marks = tokio::task::spawn_blocking(move || settled(taken, 60))
        .await
        .expect("waited");

    assert_eq!(
        marks.len(),
        60,
        "30 spans have 60 boundaries, and the run only got {}",
        marks.len()
    );
    assert!(marks.contains(&"publish:30:start".to_owned()), "{marks:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_spanning_two_halves_of_its_work_reports_both() {
    let (at, taken) = framework().await;
    let boundaries = tokio::task::spawn_blocking(move || Boundaries::joined(&at).expect("joined"))
        .await
        .expect("joined");
    let subscriber = tracing_subscriber::registry().with(boundaries.with_filter(
        tracing_subscriber::filter::filter_fn(|span| span.target().starts_with("reports")),
    ));
    let _guard = tracing::subscriber::set_default(subscriber);

    for _ in 0..3 {
        async {}.instrument(tracing::info_span!("write")).await;
        async {}.instrument(tracing::info_span!("publish")).await;
    }
    for _ in 0..2 {
        async {}.instrument(tracing::info_span!("publish")).await;
    }

    let mut marks = settled(taken, 16);
    marks.sort();

    assert_eq!(
        marks,
        [
            "publish:1:end",
            "publish:1:start",
            "publish:2:end",
            "publish:2:start",
            "publish:3:end",
            "publish:3:start",
            "publish:4:end",
            "publish:4:start",
            "publish:5:end",
            "publish:5:start",
            "write:1:end",
            "write:1:start",
            "write:2:end",
            "write:2:start",
            "write:3:end",
            "write:3:start",
        ]
    );
}

/// What the framework was told.
fn settled(taken: Arc<Mutex<Vec<String>>>, want: usize) -> Vec<String> {
    for _ in 0..200 {
        if taken.lock().expect("nothing panicked").len() >= want {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    taken.lock().expect("nothing panicked").clone()
}
