use std::time::Duration;

use anyhow::Context;
use axum::{Router, http::StatusCode, routing::get};
use futures_util::StreamExt;
use lapin::{
    Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
};
use serde::Deserialize;
use sqlx::{MySql, Pool};
use tokio::net::TcpListener;

const EXCHANGE: &str = "orders";
const QUEUE: &str = "orders.inventory";
const BINDING: &str = "order.*";
const CREATED_KEY: &str = "order.created";
const MODIFY_KEY: &str = "order.modified";
const CONSUMER_TAG: &str = "orders.inventory";
const RETRY_ATTEMPTS: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(1);

const INITIAL_STOCK: &[(&str, i32)] = &[("book", 100), ("pen", 500), ("mug", 250)];

#[derive(Deserialize)]
struct OrderCreated {
    id: u64,
    item: String,
    quantity: i32,
    seq: i32,
}

/// A customer changing their mind, and where that sits in the order's own
/// history.
#[derive(Deserialize)]
struct OrderModified {
    id: u64,
    quantity: i32,
    seq: i32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let broker_url = std::env::var("BROKER_URL").context("BROKER_URL not set")?;

    let db = connect_db(&database_url).await?;
    init_stock(&db).await?;

    let health_addr = std::env::var("HEALTH_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".to_string());
    let health_listener = TcpListener::bind(&health_addr)
        .await
        .with_context(|| format!("bind {health_addr}"))?;
    let health_router = Router::new().route("/healthz", get(|| async { StatusCode::OK }));
    tokio::spawn(async move {
        if let Err(e) = axum::serve(health_listener, health_router).await {
            tracing::warn!(?e, "health server ended");
        }
    });
    tracing::info!(addr = %health_addr, "health server ready");

    // The broker going away ends the consumer. The queue holds what it was
    // sent, so the work is there to pick up on the way back.
    loop {
        if let Err(e) = consume(&db, &broker_url).await {
            tracing::warn!(?e, "consumer stopped");
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Consume until the broker goes away.
async fn consume(db: &Pool<MySql>, broker_url: &str) -> anyhow::Result<()> {
    let channel = connect_broker(broker_url).await?;
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
    channel
        .queue_declare(
            QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("declare queue")?;
    channel
        .queue_bind(
            QUEUE,
            EXCHANGE,
            BINDING,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("bind queue")?;

    let mut consumer = channel
        .basic_consume(
            QUEUE,
            CONSUMER_TAG,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("start consumer")?;
    tracing::info!(queue = QUEUE, "consuming");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.context("delivery error")?;
        // The id the producer gave this event. Every delivery carries the
        // same one.
        let event_id = delivery
            .properties
            .message_id()
            .as_ref()
            .and_then(|id| id.as_str().parse::<u64>().ok());
        // Whether the work this delivery asks for is done. A message the
        // consumer could not parse never will, so it is settled and not handed
        // back.
        let done = match delivery.routing_key.as_str() {
            CREATED_KEY => match serde_json::from_slice::<OrderCreated>(&delivery.data) {
                Ok(event) => match apply_order(db, &event, event_id).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(?e, order_id = event.id, "failed to apply order");
                        false
                    }
                },
                Err(e) => {
                    tracing::warn!(?e, "failed to parse order");
                    true
                }
            },
            MODIFY_KEY => match serde_json::from_slice::<OrderModified>(&delivery.data) {
                Ok(event) => match apply_modification(db, &event, event_id).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(?e, order_id = event.id, "failed to modify order");
                        false
                    }
                },
                Err(e) => {
                    tracing::warn!(?e, "failed to parse modification");
                    true
                }
            },
            key => {
                tracing::warn!(%key, "nothing consumes this");
                true
            }
        };
        if done {
            delivery
                .ack(BasicAckOptions::default())
                .await
                .context("ack")?;
        } else {
            // Back to the queue, to be tried again once whatever stopped it is
            // back. The claim rolled back with the work, so the retry applies.
            delivery
                .nack(BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await
                .context("nack")?;
        }
    }
    Ok(())
}

async fn init_stock(db: &Pool<MySql>) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS stock (
            item VARCHAR(255) PRIMARY KEY,
            level INT NOT NULL
        )",
    )
    .execute(db)
    .await
    .context("create stock table")?;
    // What the consumer did, one row per event it acted on.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS applied (
            seq BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            order_id BIGINT UNSIGNED NOT NULL,
            event VARCHAR(255) NOT NULL
        )",
    )
    .execute(db)
    .await
    .context("create applied table")?;
    // How far through each order's history this consumer has got.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS order_seq (
            order_id BIGINT UNSIGNED PRIMARY KEY,
            seq INT NOT NULL
        )",
    )
    .execute(db)
    .await
    .context("create order_seq table")?;
    // The events this consumer has already acted on.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS inbox (
            event_id BIGINT UNSIGNED PRIMARY KEY
        )",
    )
    .execute(db)
    .await
    .context("create inbox table")?;
    for (item, level) in INITIAL_STOCK {
        sqlx::query("INSERT IGNORE INTO stock (item, level) VALUES (?, ?)")
            .bind(item)
            .bind(level)
            .execute(db)
            .await
            .context("seed stock")?;
    }
    Ok(())
}

/// Take the order at what it was last changed to, and give back or take the
/// difference in stock.
///
/// What the stock ends at is worked out from what the order was, so two of
/// these applied the other way round leave both somewhere the fleet was never
/// told to put them.
async fn apply_modification(
    db: &Pool<MySql>,
    event: &OrderModified,
    event_id: Option<u64>,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await.context("begin")?;
    if !claim(&mut tx, event_id).await? {
        tx.rollback().await.context("rollback")?;
        tracing::info!(order_id = event.id, "already applied");
        return Ok(());
    }
    if !advance(&mut tx, event.id, event.seq).await? {
        // A later amendment already decided what this order is for, so the
        // right thing to do with this one is nothing. It is still an event the
        // consumer handled, and `applied` counts those.
        record(&mut tx, event.id, MODIFY_KEY).await?;
        tx.commit().await.context("commit")?;
        tracing::info!(order_id = event.id, seq = event.seq, "superseded");
        return Ok(());
    }
    let (item, was): (String, i32) =
        sqlx::query_as("SELECT item, quantity FROM orders WHERE id = ?")
            .bind(event.id)
            .fetch_one(&mut *tx)
            .await
            .context("read order")?;
    sqlx::query("UPDATE orders SET quantity = ? WHERE id = ?")
        .bind(event.quantity)
        .bind(event.id)
        .execute(&mut *tx)
        .await
        .context("modify order")?;
    sqlx::query("UPDATE stock SET level = level + ? WHERE item = ?")
        .bind(was - event.quantity)
        .bind(&item)
        .execute(&mut *tx)
        .await
        .context("adjust stock")?;
    record(&mut tx, event.id, MODIFY_KEY).await?;
    tx.commit().await.context("commit")?;
    tracing::info!(
        order_id = event.id,
        was,
        now = event.quantity,
        "modified order",
    );
    Ok(())
}

/// Take this event in the same transaction as the work it causes, so the work
/// happens once however often the broker delivers it. False when another
/// delivery took it already.
///
/// An event with no id cannot be told from a second copy of itself, so it is
/// taken every time.
async fn claim(
    tx: &mut sqlx::Transaction<'_, MySql>,
    event_id: Option<u64>,
) -> anyhow::Result<bool> {
    let Some(event_id) = event_id else {
        tracing::warn!("event carries no id");
        return Ok(true);
    };
    let claimed = sqlx::query("INSERT IGNORE INTO inbox (event_id) VALUES (?)")
        .bind(event_id)
        .execute(&mut **tx)
        .await
        .context("claim event")?;
    Ok(claimed.rows_affected() == 1)
}

/// Move this order on to `seq`, in the same transaction as the work that gets
/// it there. False when the order is already at or past it, which means a
/// later amendment arrived first.
async fn advance(
    tx: &mut sqlx::Transaction<'_, MySql>,
    order_id: u64,
    seq: i32,
) -> anyhow::Result<bool> {
    // Read and compare instead of letting an upsert report what it moved:
    // sqlx connects with `FOUND_ROWS`, so a write that changes nothing still
    // reports a row.
    let held: Option<(i32,)> =
        sqlx::query_as("SELECT seq FROM order_seq WHERE order_id = ? FOR UPDATE")
            .bind(order_id)
            .fetch_optional(&mut **tx)
            .await
            .context("read how far the order has got")?;
    match held {
        Some((held,)) if seq <= held => Ok(false),
        Some(_) => {
            sqlx::query("UPDATE order_seq SET seq = ? WHERE order_id = ?")
                .bind(seq)
                .bind(order_id)
                .execute(&mut **tx)
                .await
                .context("advance the order")?;
            Ok(true)
        }
        None => {
            sqlx::query("INSERT INTO order_seq (order_id, seq) VALUES (?, ?)")
                .bind(order_id)
                .bind(seq)
                .execute(&mut **tx)
                .await
                .context("start the order")?;
            Ok(true)
        }
    }
}

/// Note that this event was acted on, in the same transaction as what it did.
async fn record(
    tx: &mut sqlx::Transaction<'_, MySql>,
    order_id: u64,
    event: &str,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO applied (order_id, event) VALUES (?, ?)")
        .bind(order_id)
        .bind(event)
        .execute(&mut **tx)
        .await
        .context("record applied")?;
    Ok(())
}

async fn apply_order(
    db: &Pool<MySql>,
    event: &OrderCreated,
    event_id: Option<u64>,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await.context("begin")?;
    if !claim(&mut tx, event_id).await? {
        tx.rollback().await.context("rollback")?;
        tracing::info!(order_id = event.id, "already applied");
        return Ok(());
    }
    advance(&mut tx, event.id, event.seq).await?;
    let result = sqlx::query("UPDATE stock SET level = level - ? WHERE item = ?")
        .bind(event.quantity)
        .bind(&event.item)
        .execute(&mut *tx)
        .await
        .context("decrement stock")?;
    record(&mut tx, event.id, CREATED_KEY).await?;
    tx.commit().await.context("commit")?;
    if result.rows_affected() == 0 {
        tracing::warn!(item = %event.item, "unknown item, stock unchanged");
    } else {
        tracing::info!(
            order_id = event.id,
            item = %event.item,
            quantity = event.quantity,
            "applied order",
        );
    }
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
