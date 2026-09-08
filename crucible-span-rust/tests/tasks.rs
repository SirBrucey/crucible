//! The adapter in a service, spans its work inside a task per request.

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

#[tokio::test(flavor = "multi_thread")]
async fn a_service_spanning_a_task_per_request_reports_every_one() {
    let (at, taken) = framework().await;
    let boundaries = tokio::task::spawn_blocking(move || Boundaries::joined(&at).expect("joined"))
        .await
        .expect("joined");
    // A service logs as well as reporting, and both layers carry a filter of
    // their own. What a run holds the service at must not depend on the other
    // layer's filter.
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(tracing_subscriber::EnvFilter::new("info")),
            )
            .with(
                boundaries.with_filter(tracing_subscriber::filter::filter_fn(|span| {
                    span.target().starts_with("tasks")
                })),
            ),
    )
    .expect("this is the only subscriber");

    for _ in 0..3 {
        tokio::spawn(async {
            async {}.instrument(tracing::info_span!("write")).await;
            async {}.instrument(tracing::info_span!("publish")).await;
        })
        .await
        .expect("served");
    }
    for _ in 0..2 {
        tokio::spawn(async {
            async {}.instrument(tracing::info_span!("publish")).await;
        })
        .await
        .expect("served");
    }

    let marks = tokio::task::spawn_blocking(move || {
        for _ in 0..200 {
            if taken.lock().expect("nothing panicked").len() >= 16 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        taken.lock().expect("nothing panicked").clone()
    })
    .await
    .expect("waited");

    assert_eq!(
        marks.iter().filter(|mark| mark.ends_with(":start")).count(),
        8,
        "3 writes and 5 publishes started, and the run heard {marks:?}"
    );
}
