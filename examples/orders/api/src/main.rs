use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
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
const RETRY_ATTEMPTS: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Deserialize)]
struct OrderRequest {
    item: String,
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
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
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
    let result = sqlx::query("INSERT INTO orders (item, quantity) VALUES (?, ?)")
        .bind(&req.item)
        .bind(req.quantity)
        .execute(&state.db)
        .await
        .map_err(|e| {
            tracing::warn!(?e, "insert failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let order_id = result.last_insert_id();

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
