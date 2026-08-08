//! Writes DNS records into the database the nameserver reads.

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool, Row};
use tokio::net::TcpListener;

const RETRY_ATTEMPTS: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(1);
const TTL: i32 = 60;

#[derive(Deserialize)]
struct RecordRequest {
    #[serde(rename = "type")]
    record_type: String,
    name: String,
    value: String,
}

#[derive(Serialize)]
struct RecordResponse {
    record_id: u64,
}

struct AppState {
    db: Pool<MySql>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let db = connect_db(&database_url).await?;

    let state = Arc::new(AppState { db });
    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/records", post(create_record))
        .with_state(state);

    let listen_addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;
    tracing::info!(addr = listen_addr, "listening");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

/// The database comes up alongside this service rather than before it, so a
/// refused connection is something to wait through rather than fail on.
async fn connect_db(url: &str) -> anyhow::Result<Pool<MySql>> {
    for attempt in 1..=RETRY_ATTEMPTS {
        match Pool::<MySql>::connect(url).await {
            Ok(pool) => return Ok(pool),
            Err(e) if attempt < RETRY_ATTEMPTS => {
                tracing::warn!(attempt, ?e, "database not ready yet");
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(e) => return Err(e).context("connect to the database"),
        }
    }
    anyhow::bail!("database never became reachable after {RETRY_ATTEMPTS} attempts");
}

async fn create_record(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RecordRequest>,
) -> Result<Json<RecordResponse>, StatusCode> {
    // The fleet serves one zone, so which zone a record belongs to is the only
    // one there is rather than something the caller states.
    let domain_id: i32 = sqlx::query("SELECT id FROM domains LIMIT 1")
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            tracing::warn!(?e, "no zone to write into");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .get("id");

    let result =
        sqlx::query("INSERT INTO records (domain_id, name, type, content, ttl) VALUES (?, ?, ?, ?, ?)")
            .bind(domain_id)
            .bind(&req.name)
            .bind(&req.record_type)
            .bind(&req.value)
            .bind(TTL)
            .execute(&state.db)
            .await
            .map_err(|e| {
                tracing::warn!(?e, "insert failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

    tracing::info!(name = %req.name, record_type = %req.record_type, "record created");
    Ok(Json(RecordResponse {
        record_id: result.last_insert_id(),
    }))
}
