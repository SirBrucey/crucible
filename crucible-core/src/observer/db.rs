//! `DbObserver`: reads the fleet's mariadb state after the scenario has run.

use sqlx::{MySql, Pool, Row};

use crate::verdict::{DbState, Observations, OrderRow, StockRow};

pub struct DbObserver {
    pool: Pool<MySql>,
}

impl DbObserver {
    /// Open a connection pool to the fleet's mariadb at `url`.
    ///
    /// # Errors
    /// Returns an error if establishing the connection pool to `url` fails
    /// (unreachable host, bad credentials, or a malformed connection string).
    pub async fn connect(url: &str) -> super::Result<Self> {
        let pool = Pool::<MySql>::connect(url).await?;
        Ok(Self { pool })
    }

    /// Read the `orders` and `stock` tables and record them on `observations`.
    ///
    /// # Errors
    /// Returns an error if either the `orders` or `stock` query fails to execute
    /// against the pool (connection loss or a schema mismatch on the selected
    /// columns).
    pub async fn observe(&self, observations: &mut Observations) -> super::Result<()> {
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
