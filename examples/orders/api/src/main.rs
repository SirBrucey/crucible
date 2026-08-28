use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool};
use tokio::net::TcpListener;

const EXCHANGE: &str = "orders";
const ROUTING_KEY: &str = "order.created";
const MODIFY_KEY: &str = "order.modified";
const RETRY_ATTEMPTS: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// The caller names the order, which is what lets it be referred to again and
/// what a fleet would use to recognise a create it has already seen.
#[derive(Deserialize)]
struct OrderRequest {
    id: u64,
    item: String,
    quantity: i32,
}

#[derive(Deserialize)]
struct ModifyRequest {
    quantity: i32,
}

#[derive(Serialize)]
struct OrderResponse {
    order_id: u64,
}

#[derive(Serialize)]
struct OrderCreated {
    id: u64,
    item: String,
    quantity: i32,
}

/// A customer changing their mind. What the order ends up for is whichever of
/// these the fleet was told last, so the order they arrive in is the answer.
#[derive(Serialize)]
struct OrderModified {
    id: u64,
    quantity: i32,
}

struct AppState {
    db: Pool<MySql>,
    channel: Channel,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let broker_url = std::env::var("BROKER_URL").context("BROKER_URL not set")?;
    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    let db = connect_db(&database_url).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            id BIGINT UNSIGNED PRIMARY KEY,
            item VARCHAR(255) NOT NULL,
            quantity INT NOT NULL
        )",
    )
    .execute(&db)
    .await
    .context("create orders table")?;

    let channel = connect_broker(&broker_url).await?;
    channel
        .exchange_declare(
            EXCHANGE,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("declare exchange")?;

    let state = Arc::new(AppState { db, channel });
    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/orders", post(create_order))
        .route("/orders/{id}", put(modify_order))
        .with_state(state);

    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;
    tracing::info!(addr = %listen_addr, "listening");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

async fn connect_db(url: &str) -> anyhow::Result<Pool<MySql>> {
    for attempt in 1..=RETRY_ATTEMPTS {
        match Pool::<MySql>::connect(url).await {
            Ok(pool) => return Ok(pool),
            Err(e) => {
                tracing::warn!(?e, attempt, "db not ready");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    anyhow::bail!("db never became reachable after {RETRY_ATTEMPTS} attempts");
}

async fn connect_broker(url: &str) -> anyhow::Result<Channel> {
    for attempt in 1..=RETRY_ATTEMPTS {
        match Connection::connect(url, ConnectionProperties::default()).await {
            Ok(conn) => return conn.create_channel().await.context("open channel"),
            Err(e) => {
                tracing::warn!(?e, attempt, "broker not ready");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    anyhow::bail!("broker never became reachable after {RETRY_ATTEMPTS} attempts");
}

async fn create_order(
    State(state): State<Arc<AppState>>,
    Json(req): Json<OrderRequest>,
) -> Result<Json<OrderResponse>, StatusCode> {
    sqlx::query("INSERT INTO orders (id, item, quantity) VALUES (?, ?, ?)")
        .bind(req.id)
        .bind(&req.item)
        .bind(req.quantity)
        .execute(&state.db)
        .await
        .map_err(|e| {
            tracing::warn!(?e, "insert failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let order_id = req.id;

    let event = OrderCreated {
        id: order_id,
        item: req.item,
        quantity: req.quantity,
    };
    let payload = serde_json::to_vec(&event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .channel
        .basic_publish(
            EXCHANGE,
            ROUTING_KEY,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await
        .map_err(|e| {
            tracing::warn!(?e, "publish failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(OrderResponse { order_id }))
}

/// Change what an order is for. Nothing here reads what it was, so two of these
/// applied the other way round leave the order at the earlier amount.
async fn modify_order(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    Json(req): Json<ModifyRequest>,
) -> Result<StatusCode, StatusCode> {
    let event = OrderModified {
        id,
        quantity: req.quantity,
    };
    let payload = serde_json::to_vec(&event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .channel
        .basic_publish(
            EXCHANGE,
            MODIFY_KEY,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await
        .map_err(|e| {
            tracing::warn!(?e, "publish failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::ACCEPTED)
}
