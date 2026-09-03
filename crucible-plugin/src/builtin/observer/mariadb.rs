//! The `MariaDB` observer plugin.

use std::net::SocketAddr;

use crucible_core::{
    plan,
    schema::{
        AttrDecl, AttrSchema, ClauseDecl, ClauseShape, CmpOp, HeadPattern, Moves, OpSig, Param,
        ParamType, ValueType,
    },
};
use sqlx::{AssertSqlSafe, MySql, Pool, Row, mysql::MySqlRow};

use crate::{
    error::Error as PluginError,
    role::{BoxFuture, Observer, ObserverRuntime, Query, Targeted},
};

/// The clause narrowing which rows are read.
const WHERE: &str = "where";
/// The alias every projection is named by.
const ALIAS: &str = "n";

/// Reads persisted state from a `MariaDB` database.
pub struct Mariadb {
    user: String,
    password: String,
}

/// One reading, in the terms this plugin runs it: which database and table,
/// which rows of it, and what to take from those rows.
#[derive(Debug, PartialEq, Eq)]
pub struct Selection {
    pub service: String,
    pub database: String,
    pub table: String,
    pub filter: Option<Filter>,
    pub take: Take,
}

/// What a reading takes from the rows it matched.
#[derive(Debug, PartialEq, Eq)]
pub enum Take {
    /// How many rows there are.
    Count,
    /// One column of the one row, so a reading can say what a value is rather
    /// than only how many of something there are.
    Column(String),
}

/// A column and what it is compared against, narrowed when the check binds to
/// the shapes a statement can carry.
#[derive(Debug, PartialEq, Eq)]
pub struct Filter {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    Str(String),
    Int(i64),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("`{0}` is not an observable this reads")]
    Observable(String),
    #[error("`{0}` is not a bare name")]
    Name(String),
    #[error("a filter compares a column against a string or a number")]
    Filter,
    #[error("`select` names the column to read")]
    NoColumn,
    #[error("`select` needs a `where` narrowing the read to one row")]
    Unnarrowed,
    #[error("`{column}` of `{table}` narrowed to {rows} row(s), and a reading is of one")]
    NotOneRow {
        table: String,
        column: String,
        rows: usize,
    },
    #[error("`{0}` holds nothing a plan can carry")]
    Unreadable(String),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

impl From<Error> for PluginError {
    fn from(e: Error) -> Self {
        Self::new(Mariadb::NAME, e)
    }
}

impl Observer for Mariadb {
    const NAME: &'static str = "mariadb";
    type Query = Selection;
    type Error = Error;

    /// Reads as the credentials the service declares, defaulting to root with
    /// no password.
    fn runtime(service: &plan::Service) -> Self {
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

    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::observable(
                HeadPattern::wildcard(&["database", "table"], "count"),
                ValueType::Int,
                // Rows the fleet has written, which each step adds to.
                Moves::Counts,
                CmpOp::ALL.to_vec(),
            )
            .with_clause(ClauseDecl::new(WHERE, ClauseShape::Filter)),
            OpSig::observable(
                HeadPattern::wildcard(&["database", "table"], "select"),
                ValueType::Int,
                // One column of one row, which each step writes over.
                Moves::Sets,
                CmpOp::ALL.to_vec(),
            )
            .with_param(Param::required("column", ParamType::Ident))
            .with_clause(ClauseDecl::new(WHERE, ClauseShape::Filter)),
        ]
    }

    /// Only the credentials to read with are the author's to give, and they
    /// default to the unprivileged case.
    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![
            AttrDecl::optional("user", ValueType::Str),
            AttrDecl::optional("password", ValueType::Str),
        ])
    }

    fn bind(check: &plan::Check) -> Result<Self::Query, Self::Error> {
        let (database, table, take) = match check.observable.as_slice() {
            [database, table, reading] if reading == "count" => (database, table, Take::Count),
            [database, table, reading] if reading == "select" => {
                let Some(plan::Value::Ident(column)) = check.args.first() else {
                    return Err(Error::NoColumn);
                };
                if !is_bare_name(column) {
                    return Err(Error::Name(column.clone()));
                }
                // A column is one value, so which row it comes from has to be
                // settled before the read rather than after it.
                if check.filter.is_none() {
                    return Err(Error::Unnarrowed);
                }
                (database, table, Take::Column(column.clone()))
            }
            _ => return Err(Error::Observable(check.observable.join("."))),
        };
        // These go into the statement rather than a bound parameter, so they
        // must be bare names and nothing else.
        for name in [database, table] {
            if !is_bare_name(name) {
                return Err(Error::Name(name.clone()));
            }
        }
        let filter = check
            .filter
            .as_ref()
            .map(|(column, value)| {
                if !is_bare_name(column) {
                    return Err(Error::Name(column.clone()));
                }
                let value = match value {
                    plan::Value::Str(s) => Value::Str(s.clone()),
                    plan::Value::Int(n) => Value::Int(*n),
                    _ => return Err(Error::Filter),
                };
                Ok(Filter {
                    column: column.clone(),
                    value,
                })
            })
            .transpose()?;
        Ok(Selection {
            service: check.service.clone(),
            database: database.clone(),
            table: table.clone(),
            filter,
            take,
        })
    }
}

impl ObserverRuntime for Mariadb {
    fn prepare<'a>(
        &'a self,
        check: &'a plan::Check,
    ) -> BoxFuture<'a, Result<Box<dyn Query>, PluginError>> {
        Box::pin(async move {
            Ok(Box::new(Read {
                selection: Mariadb::bind(check)?,
                user: self.user.clone(),
                password: self.password.clone(),
            }) as Box<dyn Query>)
        })
    }
}

/// A bound reading and the credentials to take it with.
struct Read {
    selection: Selection,
    user: String,
    password: String,
}

impl Targeted for Read {
    fn kind(&self) -> &str {
        Mariadb::NAME
    }

    fn target(&self) -> &str {
        &self.selection.service
    }
}

impl Query for Read {
    fn read(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<plan::Value, PluginError>> {
        Box::pin(async move { self.take(endpoint).await.map_err(PluginError::from) })
    }
}

impl Read {
    async fn take(&self, endpoint: SocketAddr) -> Result<plan::Value, Error> {
        let credentials = if self.password.is_empty() {
            self.user.clone()
        } else {
            format!("{}:{}", self.user, self.password)
        };
        let pool = Pool::<MySql>::connect(&format!("mysql://{credentials}@{endpoint}")).await?;

        // The clause and the value it binds come from one place, so a statement
        // can never be built with one and not the other.
        let (clause, bound) = match &self.selection.filter {
            Some(filter) => (
                format!(" WHERE `{}` = ?", filter.column),
                Some(&filter.value),
            ),
            None => (String::new(), None),
        };
        let projection = match &self.selection.take {
            Take::Count => "COUNT(*)".to_owned(),
            Take::Column(column) => format!("`{column}`"),
        };
        let sql = format!(
            "SELECT {projection} AS {ALIAS} FROM `{}`.`{}`{clause}",
            self.selection.database, self.selection.table
        );
        // The statement is built rather than fixed, so what goes into it is
        // checked: the table and columns are bare names, and the value the
        // filter compares against is bound rather than written in.
        let mut query = sqlx::query(AssertSqlSafe(sql));
        query = match bound {
            Some(Value::Str(s)) => query.bind(s.clone()),
            Some(Value::Int(n)) => query.bind(*n),
            None => query,
        };
        let rows = query.fetch_all(&pool).await?;
        // A replica is killed and restarted under this observer, so its
        // connections are returned rather than left for the server to time out.
        pool.close().await;

        match &self.selection.take {
            // A count answers with one row whatever matched, including nothing.
            Take::Count => value_of(&rows[0], "count"),
            // A column is one value, so anything but one row has no answer.
            Take::Column(column) => match rows.as_slice() {
                [only] => value_of(only, column),
                rows => Err(Error::NotOneRow {
                    table: self.selection.table.clone(),
                    column: column.clone(),
                    rows: rows.len(),
                }),
            },
        }
    }
}

/// What a statement projected, in the terms a plan speaks. Which column a
/// `select` names is the author's, so its type is tried rather than declared.
fn value_of(row: &MySqlRow, name: &str) -> Result<plan::Value, Error> {
    if let Ok(n) = row.try_get::<i64, _>(ALIAS) {
        return Ok(plan::Value::Int(n));
    }
    if let Ok(s) = row.try_get::<String, _>(ALIAS) {
        return Ok(plan::Value::Str(s));
    }
    Err(Error::Unreadable(name.to_owned()))
}

/// Whether `s` is a bare name, safe to place in a statement directly.
fn is_bare_name(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::{Error, Filter, Mariadb, Selection, Take, Value};
    use crate::role::Observer;
    use crucible_core::{
        plan,
        schema::{ClauseShape, CmpOp, HeadPattern, Moves, ValueType},
    };

    fn check(observable: &[&str], filter: Option<(&str, plan::Value)>) -> plan::Check {
        plan::Check {
            moves: Moves::Counts,
            service: "db".into(),
            observer: "mariadb".into(),
            observable: observable.iter().map(|s| (*s).to_string()).collect(),
            args: Vec::new(),
            filter: filter.map(|(column, value)| (column.to_string(), value)),
            clauses: std::collections::BTreeMap::new(),
            op: CmpOp::Eq,
            value: plan::Value::Int(3),
        }
    }

    /// A `select` check: the column it names, and the row it narrows to.
    fn select(
        database: &str,
        table: &str,
        column: &str,
        filter: Option<(&str, plan::Value)>,
    ) -> plan::Check {
        plan::Check {
            args: vec![plan::Value::Ident(column.into())],
            ..check(&[database, table, "select"], filter)
        }
    }

    #[test]
    fn count_is_an_int_observable_with_a_where_filter() {
        let signatures = Mariadb::signatures();
        let count = signatures
            .iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Wildcard { tail, .. } if tail == "count"))
            .expect("mariadb has a `<database>.<table>.count` observable");

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
        let bound = Mariadb::bind(&check(&["orders", "orders", "count"], None)).expect("binds");
        assert_eq!(
            bound,
            Selection {
                service: "db".into(),
                database: "orders".into(),
                table: "orders".into(),
                filter: None,
                take: Take::Count,
            },
        );
    }

    #[test]
    fn a_select_binds_to_the_column_it_names() {
        let bound = Mariadb::bind(&select(
            "orders",
            "stock",
            "level",
            Some(("item", plan::Value::Str("book".into()))),
        ))
        .expect("binds");
        assert_eq!(bound.take, Take::Column("level".into()));
        assert_eq!(bound.database, "orders");
        assert_eq!(bound.table, "stock");
    }

    #[test]
    fn a_select_without_a_column_is_rejected() {
        let unnamed = plan::Check {
            args: Vec::new(),
            ..select(
                "orders",
                "stock",
                "level",
                Some(("item", plan::Value::Str("book".into()))),
            )
        };
        assert!(matches!(Mariadb::bind(&unnamed), Err(Error::NoColumn)));
    }

    #[test]
    fn a_select_reading_more_than_one_row_is_rejected() {
        assert!(matches!(
            Mariadb::bind(&select("orders", "stock", "level", None)),
            Err(Error::Unnarrowed),
        ));
    }

    #[test]
    fn a_filter_binds_with_the_check() {
        let bound = Mariadb::bind(&check(
            &["orders", "orders", "count"],
            Some(("item", plan::Value::Str("book".into()))),
        ))
        .expect("binds");
        assert_eq!(
            bound.filter,
            Some(Filter {
                column: "item".into(),
                value: Value::Str("book".into()),
            }),
        );
    }

    #[test]
    fn an_observable_this_does_not_read_is_rejected() {
        let bound = Mariadb::bind(&check(&["orders", "orders", "sum"], None));
        assert!(matches!(bound, Err(Error::Observable(_))));
    }

    /// The head the grammar matches has a segment per name the signature gives,
    /// so an unqualified one never reaches a fleet. It is refused here too,
    /// rather than read as a table with no database.
    #[test]
    fn a_table_without_a_database_is_rejected() {
        let bound = Mariadb::bind(&check(&["orders", "count"], None));
        assert!(matches!(bound, Err(Error::Observable(_))));
    }

    #[test]
    fn a_table_that_is_not_a_bare_name_is_rejected() {
        // It is placed in the statement directly, so anything else could carry
        // SQL of its own.
        let bound = Mariadb::bind(&check(
            &["orders", "orders; DROP TABLE orders", "count"],
            None,
        ));
        assert!(matches!(bound, Err(Error::Name(_))));
    }

    #[test]
    fn a_database_that_is_not_a_bare_name_is_rejected() {
        let bound = Mariadb::bind(&check(
            &["orders`; DROP TABLE orders; --", "orders", "count"],
            None,
        ));
        assert!(matches!(bound, Err(Error::Name(_))));
    }
}
