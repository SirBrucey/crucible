use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
};
use crucible_span_rust::Boundaries;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio::{net::TcpListener, sync::Notify};
use tracing::Instrument;
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt};

const EXCHANGE: &str = "orders";
const ROUTING_KEY: &str = "order.created";
const MODIFY_KEY: &str = "order.modified";
const RETRY_ATTEMPTS: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(1);
/// How long the relay sleeps before looking of its own accord, for rows a
/// restart left behind.
const RELAY_BACKSTOP: Duration = Duration::from_secs(2);

/// The caller names the order, which is what lets it be referred to again and
/// what a fleet would use to recognise a create it has already seen.
#[derive(Deserialize)]
struct OrderRequest {
    id: i64,
    item: String,
    quantity: i64,
}

#[derive(Deserialize)]
struct ModifyRequest {
    quantity: i64,
}

#[derive(Serialize)]
struct OrderResponse {
    order_id: i64,
}

#[derive(Serialize)]
struct OrderCreated {
    id: i64,
    item: String,
    quantity: i64,
}

/// A customer changing their mind. What the order ends up for is whichever of
/// these the fleet was told last, so the order they arrive in is the answer.
#[derive(Serialize)]
struct OrderModified {
    id: i64,
    quantity: i64,
}

/// What this service holds, which is now two files of its own rather than a
/// database across the network.
///
/// Two files means two connections, and two connections cannot share a
/// transaction. Writing an order and queueing its announcement are therefore
/// two separate commits, with a moment in between where only the first has
/// happened.
struct AppState {
    orders: SqlitePool,
    outbox: SqlitePool,
    broker_url: String,
    /// Rung when something is put in the outbox, so the relay answers a write
    /// rather than asking after one.
    queued: Notify,
}

/// What the campaign reads, since these tables are inside the service now.
#[derive(Serialize)]
struct Stats {
    orders: i64,
    outbox: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The filter sits on the logging layer alone. What a run holds this service
    // at must not depend on how talkative its logs are.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::from_default_env()),
        )
        .with(
            Boundaries::joining()
                .context("join the run this service is in")?
                // This service's own spans, not its libraries'.
                .map(|boundaries| {
                    boundaries
                        .with_filter(filter_fn(|span| span.target().starts_with("orders_local_api")))
                }),
        )
        .init();

    let broker_url = std::env::var("BROKER_URL").context("BROKER_URL not set")?;
    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());

    let orders = open(&format!("{data}/state.db")).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            item TEXT NOT NULL,
            quantity INTEGER NOT NULL
        )",
    )
    .execute(&orders)
    .await
    .context("create orders table")?;

    let outbox = open(&format!("{data}/outbox.db")).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS outbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            routing_key TEXT NOT NULL,
            payload BLOB NOT NULL
        )",
    )
    .execute(&outbox)
    .await
    .context("create outbox table")?;

    declare_exchange(&broker_url).await?;

    let state = Arc::new(AppState {
        orders,
        outbox,
        broker_url,
        queued: Notify::new(),
    });
    tokio::spawn(relay(Arc::clone(&state)));

    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/stats", get(stats))
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

/// A store of this service's own, created if it is not there yet.
async fn open(path: &str) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    SqlitePool::connect_with(options)
        .await
        .with_context(|| format!("open {path}"))
}

async fn declare_exchange(url: &str) -> anyhow::Result<()> {
    let channel = connect_broker(url).await?;
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
    Ok(())
}

/// Announce what the outbox holds, oldest first, and take each row away once it
/// has gone.
async fn relay(state: Arc<AppState>) {
    let mut channel: Option<Channel> = None;
    loop {
        if !channel.as_ref().is_some_and(|c| c.status().connected()) {
            channel = match Connection::connect(&state.broker_url, ConnectionProperties::default())
                .await
            {
                Ok(conn) => conn.create_channel().await.ok(),
                Err(e) => {
                    tracing::warn!(?e, "broker unreachable, will try again");
                    None
                }
            };
        }
        if let Some(channel) = &channel {
            match pending(&state.outbox).await {
                Ok(rows) => {
                    for (seq, key, payload) in rows {
                        if let Err(e) = announce(&state.outbox, channel, seq, &key, &payload).await {
                            tracing::warn!(?e, seq, "could not announce, will try again");
                            break;
                        }
                    }
                }
                Err(e) => tracing::warn!(?e, "could not read the outbox"),
            }
        }
        let _ = tokio::time::timeout(RELAY_BACKSTOP, state.queued.notified()).await;
    }
}

async fn pending(outbox: &SqlitePool) -> anyhow::Result<Vec<(i64, String, Vec<u8>)>> {
    sqlx::query_as("SELECT seq, routing_key, payload FROM outbox ORDER BY seq")
        .fetch_all(outbox)
        .await
        .context("read outbox")
}

async fn announce(
    outbox: &SqlitePool,
    channel: &Channel,
    seq: i64,
    key: &str,
    payload: &[u8],
) -> anyhow::Result<()> {
    channel
        .basic_publish(
            EXCHANGE,
            key,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await
        .context("publish")?
        .await
        .context("confirm")?;
    sqlx::query("DELETE FROM outbox WHERE seq = ?")
        .bind(seq)
        .execute(outbox)
        .await
        .context("settle outbox row")?;
    Ok(())
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

async fn stats(State(state): State<Arc<AppState>>) -> Result<Json<Stats>, StatusCode> {
    let orders: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
        .fetch_one(&state.orders)
        .await
        .map_err(|e| failed(&e))?;
    let outbox: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox")
        .fetch_one(&state.outbox)
        .await
        .map_err(|e| failed(&e))?;
    Ok(Json(Stats {
        orders: orders.0,
        outbox: outbox.0,
    }))
}

async fn create_order(
    State(state): State<Arc<AppState>>,
    Json(req): Json<OrderRequest>,
) -> Result<Json<OrderResponse>, StatusCode> {
    let order_id = req.id;
    let event = OrderCreated {
        id: order_id,
        item: req.item.clone(),
        quantity: req.quantity,
    };
    let payload = serde_json::to_vec(&event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Two stores, so two commits. Between them the order is on the books and
    // nothing has been queued to say so, and neither write crosses a network.
    sqlx::query("INSERT INTO orders (id, item, quantity) VALUES (?, ?, ?)")
        .bind(req.id)
        .bind(&req.item)
        .bind(req.quantity)
        .execute(&state.orders)
        .instrument(tracing::info_span!("record"))
        .await
        .map_err(|e| failed(&e))?;
    queue(&state, ROUTING_KEY, &payload).await?;

    Ok(Json(OrderResponse { order_id }))
}

fn failed(e: &sqlx::Error) -> StatusCode {
    tracing::warn!(?e, "write failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

/// Put an event in the outbox, for the relay to announce.
async fn queue(state: &AppState, key: &str, payload: &[u8]) -> Result<(), StatusCode> {
    sqlx::query("INSERT INTO outbox (routing_key, payload) VALUES (?, ?)")
        .bind(key)
        .bind(payload)
        .execute(&state.outbox)
        .instrument(tracing::info_span!("queue"))
        .await
        .map_err(|e| failed(&e))?;
    state.queued.notify_one();
    Ok(())
}

/// Change what an order is for. Nothing here reads what it was, so two of these
/// applied the other way round leave the order at the earlier amount.
async fn modify_order(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(req): Json<ModifyRequest>,
) -> Result<StatusCode, StatusCode> {
    let event = OrderModified {
        id,
        quantity: req.quantity,
    };
    let payload = serde_json::to_vec(&event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    queue(&state, MODIFY_KEY, &payload).await?;

    Ok(StatusCode::ACCEPTED)
}
