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
use tokio::{net::TcpListener, sync::Notify};

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
    /// The relay holds the broker connection, since nothing on the request path
    /// talks to the broker any more.
    broker_url: String,
    /// Rung when something is put in the outbox, so the relay answers a write
    /// rather than asking after one. It still wakes on its own eventually, for
    /// anything left behind by a restart.
    queued: Notify,
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
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS outbox (
            seq BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            routing_key VARCHAR(255) NOT NULL,
            payload BLOB NOT NULL
        )",
    )
    .execute(&db)
    .await
    .context("create outbox table")?;

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

    let state = Arc::new(AppState {
        db,
        broker_url,
        queued: Notify::new(),
    });
    tokio::spawn(relay(Arc::clone(&state)));

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

/// Announce what the outbox holds, oldest first, and take each row away once it
/// has gone.
///
/// Publishing and deleting are two steps, so a row that has been announced and
/// not yet deleted is announced again when this comes back. That is what makes
/// delivery at-least-once, and it is the consumer's job to take it twice.
async fn relay(state: Arc<AppState>) {
    // Its own connection, rebuilt whenever the broker goes away. An outbox that
    // gives up on its first failure keeps the event and never sends it.
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
            match pending(&state.db).await {
                Ok(rows) => {
                    for (seq, key, payload) in rows {
                        if let Err(e) = announce(&state.db, channel, seq, &key, &payload).await {
                            tracing::warn!(?e, seq, "could not announce, will try again");
                            break;
                        }
                    }
                }
                Err(e) => tracing::warn!(?e, "could not read the outbox"),
            }
        }
        // Woken by a write, or on its own if none comes.
        let _ = tokio::time::timeout(RELAY_BACKSTOP, state.queued.notified()).await;
    }
}

async fn pending(db: &Pool<MySql>) -> anyhow::Result<Vec<(u64, String, Vec<u8>)>> {
    sqlx::query_as("SELECT seq, routing_key, payload FROM outbox ORDER BY seq")
        .fetch_all(db)
        .await
        .context("read outbox")
}

async fn announce(
    db: &Pool<MySql>,
    channel: &Channel,
    seq: u64,
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
        .execute(db)
        .await
        .context("settle outbox row")?;
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
    let order_id = req.id;
    let event = OrderCreated {
        id: order_id,
        item: req.item.clone(),
        quantity: req.quantity,
    };
    let payload = serde_json::to_vec(&event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // The order and the intent to announce it are written together, so the
    // fleet cannot end up holding one without the other.
    let mut tx = state.db.begin().await.map_err(|e| failed(&e))?;
    sqlx::query("INSERT INTO orders (id, item, quantity) VALUES (?, ?, ?)")
        .bind(req.id)
        .bind(&req.item)
        .bind(req.quantity)
        .execute(&mut *tx)
        .await
        .map_err(|e| failed(&e))?;
    queue(&mut *tx, ROUTING_KEY, &payload).await?;
    tx.commit().await.map_err(|e| failed(&e))?;
    state.queued.notify_one();

    Ok(Json(OrderResponse { order_id }))
}

fn failed(e: &sqlx::Error) -> StatusCode {
    tracing::warn!(?e, "write failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

/// Put an event in the outbox, for the relay to announce.
///
/// Takes whatever it is given to write through, so a caller with something to
/// write alongside it can pass its transaction.
async fn queue<'e, E>(db: E, key: &str, payload: &[u8]) -> Result<(), StatusCode>
where
    E: sqlx::Executor<'e, Database = MySql>,
{
    sqlx::query("INSERT INTO outbox (routing_key, payload) VALUES (?, ?)")
        .bind(key)
        .bind(payload)
        .execute(db)
        .await
        .map_err(|e| failed(&e))?;
    Ok(())
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

    // Nothing to write beside it, so nothing to be atomic with.
    queue(&state.db, MODIFY_KEY, &payload).await?;
    state.queued.notify_one();

    Ok(StatusCode::ACCEPTED)
}
