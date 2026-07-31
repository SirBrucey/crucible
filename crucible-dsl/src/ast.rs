//! The parsed form of a `.cru` file.

use std::time::Duration;

use crate::span::Spanned;

/// A parsed `.cru` file: one fleet and its scenarios.
#[derive(Clone, Debug, PartialEq)]
pub struct File {
    pub fleet: Spanned<Fleet>,
    pub scenarios: Vec<Spanned<Scenario>>,
}

/// A fleet: its name and the services it brings up.
#[derive(Clone, Debug, PartialEq)]
pub struct Fleet {
    pub name: Spanned<String>,
    pub services: Vec<Spanned<Service>>,
}

/// A service: its name and its bring-up attributes, a map the deployment plugin
/// interprets.
#[derive(Clone, Debug, PartialEq)]
pub struct Service {
    pub name: Spanned<String>,
    pub attrs: Spanned<Value>,
}

/// A scenario: a name, a heal-phase deadline, the driver steps to run, and the
/// settled-state expectation.
#[derive(Clone, Debug, PartialEq)]
pub struct Scenario {
    pub name: Spanned<String>,
    pub consistent_within: Option<Spanned<Duration>>,
    pub steps: Vec<Spanned<OpCall>>,
    pub expect: Vec<Spanned<Predicate>>,
}

/// An operation call: a head (`http POST`, `db.orders.count`), positional
/// arguments, and clauses.
#[derive(Clone, Debug, PartialEq)]
pub struct OpCall {
    pub head: Vec<Spanned<String>>,
    pub args: Vec<Spanned<Value>>,
    pub clauses: Vec<Spanned<Clause>>,
}

/// A clause attached to an operation call.
#[derive(Clone, Debug, PartialEq)]
pub enum Clause {
    /// A `body { ... }` payload.
    Body(Spanned<Value>),
    /// A `where <column> = <value>` filter.
    Where(Filter),
}

/// A single-column equality filter.
#[derive(Clone, Debug, PartialEq)]
pub struct Filter {
    pub column: Spanned<String>,
    pub value: Spanned<Value>,
}

/// A scenario-level expectation: an observable compared against a value.
#[derive(Clone, Debug, PartialEq)]
pub struct Predicate {
    pub left: Spanned<OpCall>,
    pub op: Spanned<CmpOp>,
    pub right: Spanned<Value>,
}

/// A comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A literal or composite value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Bool(bool),
    Duration(Duration),
    Ident(String),
    List(Vec<Spanned<Value>>),
    Map(Vec<(Spanned<String>, Spanned<Value>)>),
}
