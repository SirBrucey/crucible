use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
use lapin::{
    Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use serde::Deserialize;
use sqlx::{MySql, Pool};
use tracing::{info, warn};

const EXCHANGE: &str = "orders";
const QUEUE: &str = "orders.inventory";
const BINDING: &str = "order.*";
const CONSUMER_TAG: &str = "orders.inventory";
const RETRY_ATTEMPTS: u32 = 30;
const RETRY_DELAY: Duration = Duration::from_secs(1);

const INITIAL_STOCK: &[(&str, i32)] = &[("book", 100), ("pen", 500), ("mug", 250)];

#[derive(Deserialize)]
struct OrderCreated {
    id: u64,
    item: String,
    quantity: i32,
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
    info!(queue = QUEUE, "consuming");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.context("delivery error")?;
        match serde_json::from_slice::<OrderCreated>(&delivery.data) {
            Ok(event) => {
                if let Err(e) = apply_order(&db, &event).await {
                    warn!(?e, order_id = event.id, "failed to apply order");
                }
            }
            Err(e) => warn!(?e, "failed to parse event"),
        }
        delivery.ack(BasicAckOptions::default()).await.context("ack")?;
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

async fn apply_order(db: &Pool<MySql>, event: &OrderCreated) -> anyhow::Result<()> {
    let result = sqlx::query("UPDATE stock SET level = level - ? WHERE item = ?")
        .bind(event.quantity)
        .bind(&event.item)
        .execute(db)
        .await
        .context("decrement stock")?;
    if result.rows_affected() == 0 {
        warn!(item = %event.item, "unknown item, stock unchanged");
    } else {
        info!(
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
                warn!(?e, attempt, "db not ready");
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
                warn!(?e, attempt, "broker not ready");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    anyhow::bail!("broker never became reachable after {RETRY_ATTEMPTS} attempts");
}
