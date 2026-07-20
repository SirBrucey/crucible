//! `DbObserver`: reads the fleet's mariadb state after the scenario has run.

use sqlx::{MySql, Pool, Row};

use crate::verdict::{DbState, Observations, OrderRow, StockRow};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct DbObserver {
    pool: Pool<MySql>,
}

impl DbObserver {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = Pool::<MySql>::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn observe(&self, observations: &mut Observations) -> Result<()> {
        let orders = sqlx::query("SELECT id, item, quantity FROM orders ORDER BY id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| OrderRow {
                id: row.get::<u64, _>("id"),
                item: row.get::<String, _>("item"),
                quantity: row.get::<i32, _>("quantity"),
            })
            .collect();

        let stock = sqlx::query("SELECT item, level FROM stock ORDER BY item")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| StockRow {
                item: row.get::<String, _>("item"),
                level: row.get::<i32, _>("level"),
            })
            .collect();

        observations.db_state = Some(DbState { orders, stock });
        Ok(())
    }
}
