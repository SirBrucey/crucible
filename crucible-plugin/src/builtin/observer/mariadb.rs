//! The `MariaDB` observer plugin.

use std::net::SocketAddr;

use crucible_core::{
    plan,
    schema::{AttrDecl, AttrSchema, ClauseDecl, ClauseShape, CmpOp, HeadPattern, OpSig, ValueType},
};
use sqlx::{AssertSqlSafe, MySql, Pool, Row};

use crate::{
    error::Error as PluginError,
    role::{BoxFuture, Observer, ObserverRuntime, Query},
};

/// The clause narrowing which rows are counted.
const WHERE: &str = "where";
/// Schemas the server keeps for itself, which are never the fleet's state.
const RESERVED: [&str; 4] = ["information_schema", "mysql", "performance_schema", "sys"];

/// Reads persisted state from a `MariaDB` database.
pub struct Mariadb {
    user: String,
    password: String,
}

/// One count, in the terms this plugin runs it.
#[derive(Debug, PartialEq, Eq)]
pub struct Count {
    pub service: String,
    pub table: String,
    pub filter: Option<(String, plan::Value)>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("`{0}` is not an observable this reads")]
    Observable(String),
    #[error("`{0}` is not a table name")]
    Table(String),
    #[error("a filter compares a column against a string or a number")]
    Filter,
    #[error("the fleet holds no database to read")]
    NoDatabase,
    #[error("the fleet holds several databases, so which to read is ambiguous: {0}")]
    ManyDatabases(String),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

impl From<Error> for PluginError {
    fn from(e: Error) -> Self {
        Self::new(Mariadb::NAME, e)
    }
}

impl Mariadb {
    /// An observer reading as the credentials a service declares, defaulting to
    /// the unprivileged case a test fleet is usually brought up with.
    #[must_use]
    pub fn new(service: &plan::Service) -> Self {
        Self {
            user: service
                .attr("user")
                .and_then(plan::Value::as_str)
                .unwrap_or("root")
                .to_owned(),
            password: service
                .attr("password")
                .and_then(plan::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }
    }
}

impl Observer for Mariadb {
    const NAME: &'static str = "mariadb";
    type Query = Count;
    type Error = Error;

    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::observable(
                HeadPattern::wildcard("table", "count"),
                ValueType::Int,
                CmpOp::ALL.to_vec(),
            )
            .with_clause(ClauseDecl::new(WHERE, ClauseShape::Filter)),
        ]
    }

    /// Which database holds the state is discovered rather than declared, since
    /// the deployment is what creates it. Only the credentials to read it with
    /// are the author's to give, and they default to the unprivileged case.
    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![
            AttrDecl::optional("user", ValueType::Str),
            AttrDecl::optional("password", ValueType::Str),
        ])
    }

    fn bind(check: &plan::Check) -> Result<Self::Query, Self::Error> {
        let [table, "count"] = check
            .observable
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()[..]
        else {
            return Err(Error::Observable(check.observable.join(".")));
        };
        // The table goes into the statement rather than a bound parameter, so it
        // must be a bare name and nothing else.
        if !is_name(table) {
            return Err(Error::Table(table.to_owned()));
        }
        if let Some((column, value)) = &check.filter {
            if !is_name(column) {
                return Err(Error::Table(column.clone()));
            }
            if !matches!(value, plan::Value::Str(_) | plan::Value::Int(_)) {
                return Err(Error::Filter);
            }
        }
        Ok(Count {
            service: check.service.clone(),
            table: table.to_owned(),
            filter: check.filter.clone(),
        })
    }
}

impl ObserverRuntime for Mariadb {
    fn prepare(&self, check: &plan::Check) -> Result<Box<dyn Query>, PluginError> {
        Ok(Box::new(Read {
            count: Mariadb::bind(check)?,
            user: self.user.clone(),
            password: self.password.clone(),
        }))
    }
}

/// A bound count and the credentials to read it with.
struct Read {
    count: Count,
    user: String,
    password: String,
}

impl Query for Read {
    fn target(&self) -> &str {
        &self.count.service
    }

    fn read(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<plan::Value, PluginError>> {
        Box::pin(async move { self.count(endpoint).await.map_err(PluginError::from) })
    }
}

impl Read {
    async fn count(&self, endpoint: SocketAddr) -> Result<plan::Value, Error> {
        let credentials = if self.password.is_empty() {
            self.user.clone()
        } else {
            format!("{}:{}", self.user, self.password)
        };
        let pool = Pool::<MySql>::connect(&format!("mysql://{credentials}@{endpoint}")).await?;
        let database = sole_database(&pool).await?;

        let filter = match &self.count.filter {
            Some((column, _)) => format!(" WHERE `{column}` = ?"),
            None => String::new(),
        };
        let sql = format!(
            "SELECT COUNT(*) AS n FROM `{database}`.`{}`{filter}",
            self.count.table
        );
        // The statement is built rather than fixed, so what goes into it is
        // checked: the table and column are bare names, and the value the filter
        // compares against is bound rather than written in.
        let mut query = sqlx::query(AssertSqlSafe(sql));
        if let Some((_, value)) = &self.count.filter {
            query = match value {
                plan::Value::Str(s) => query.bind(s.clone()),
                plan::Value::Int(n) => query.bind(*n),
                _ => return Err(Error::Filter),
            };
        }
        let n: i64 = query.fetch_one(&pool).await?.get("n");
        Ok(plan::Value::Int(n))
    }
}

/// The one database the fleet keeps its state in. The deployment creates it, so
/// the author does not name it; several would leave the choice ambiguous.
async fn sole_database(pool: &Pool<MySql>) -> Result<String, Error> {
    let mut names: Vec<String> = sqlx::query("SHOW DATABASES")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>(0))
        .filter(|name| !RESERVED.contains(&name.as_str()))
        .collect();
    names.sort();
    match names.len() {
        0 => Err(Error::NoDatabase),
        1 => Ok(names.remove(0)),
        _ => Err(Error::ManyDatabases(names.join(", "))),
    }
}

/// Whether `s` is a bare name, safe to place in a statement directly.
fn is_name(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::{Count, Error, Mariadb};
    use crate::role::Observer;
    use crucible_core::{
        plan,
        schema::{ClauseShape, CmpOp, HeadPattern, ValueType},
    };

    fn check(observable: &[&str], filter: Option<(&str, plan::Value)>) -> plan::Check {
        plan::Check {
            service: "db".into(),
            observer: "mariadb".into(),
            observable: observable.iter().map(|s| (*s).to_string()).collect(),
            filter: filter.map(|(column, value)| (column.to_string(), value)),
            op: CmpOp::Eq,
            value: plan::Value::Int(3),
        }
    }

    #[test]
    fn count_is_an_int_observable_with_a_where_filter() {
        let signatures = Mariadb::signatures();
        let count = signatures
            .iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Wildcard { tail, .. } if tail == "count"))
            .expect("mariadb has a `<table>.count` observable");

        assert_eq!(count.result, Some(ValueType::Int));
        assert!(count.cmp_ops.contains(&CmpOp::Eq));
        assert!(
            count
                .clauses
                .iter()
                .any(|clause| clause.keyword == "where" && clause.shape == ClauseShape::Filter),
        );
    }

    #[test]
    fn a_check_binds_to_a_count() {
        let bound = Mariadb::bind(&check(&["orders", "count"], None)).expect("binds");
        assert_eq!(
            bound,
            Count {
                service: "db".into(),
                table: "orders".into(),
                filter: None,
            },
        );
    }

    #[test]
    fn a_filter_binds_with_the_check() {
        let bound = Mariadb::bind(&check(
            &["orders", "count"],
            Some(("item", plan::Value::Str("book".into()))),
        ))
        .expect("binds");
        assert_eq!(
            bound.filter,
            Some(("item".into(), plan::Value::Str("book".into()))),
        );
    }

    #[test]
    fn an_observable_this_does_not_read_is_rejected() {
        let bound = Mariadb::bind(&check(&["orders", "sum"], None));
        assert!(matches!(bound, Err(Error::Observable(_))));
    }

    #[test]
    fn a_table_that_is_not_a_bare_name_is_rejected() {
        // It is placed in the statement directly, so anything else could carry
        // SQL of its own.
        let bound = Mariadb::bind(&check(&["orders; DROP TABLE orders", "count"], None));
        assert!(matches!(bound, Err(Error::Table(_))));
    }
}
