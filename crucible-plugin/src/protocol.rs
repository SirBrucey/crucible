//! What the framework and a plugin process say to each other.
//!
//! Every plugin answers [`Request::Describe`], whatever it does. What it is
//! asked after that is the vocabulary of the role it declared, so a role is
//! added by adding a variant rather than by changing what a plugin is.
//!
//! Every message carries what answering it needs, so neither side keeps state
//! about the other and a plugin that dies costs only the answer it was giving.

use crucible_core::schema::{AttrSchema, OpSig};

/// The protocol this framework speaks. A plugin built against a different one
/// is skipped rather than guessed at.
pub const VERSION: u32 = 1;

/// What the framework asks a plugin.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Request {
    /// What are you?
    Describe,
    Observer(Box<observer::Request>),
}

/// What a plugin answers.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Response {
    Described(Description),
    Observer(observer::Response),
    /// The plugin understood the request and could not do it.
    Failed(String),
}

/// A plugin's account of itself: what any plugin must say, whatever it does.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Description {
    /// The protocol the plugin was built against.
    pub protocol: u32,
    /// The name a scenario selects this plugin by.
    pub name: String,
    /// What it reads of a service that speaks it.
    pub attrs: AttrSchema,
    /// Which vocabulary it will be asked in.
    pub role: Role,
}

/// The part a plugin plays, and what it offers in that part.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Role {
    /// Answers observables a scenario's `expect` names.
    Observer { signatures: Vec<OpSig> },
}

impl Role {
    /// Which part this is, without what it offers in it. One name may be
    /// claimed once per role: a service naming a kind means a driver for what
    /// it drives and an observer for what it reads, and those need not be the
    /// same plugin.
    #[must_use]
    pub fn part(&self) -> &'static str {
        match self {
            Role::Observer { .. } => "observer",
        }
    }
}

/// What an observer is asked, and what it answers.
pub mod observer {
    use std::net::SocketAddr;

    use crucible_core::plan;

    #[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub enum Request {
        /// Does this check make sense to you?
        Bind {
            service: plan::Service,
            check: plan::Check,
        },
        /// What does this check read, against the service answering here?
        Read {
            service: plan::Service,
            check: plan::Check,
            endpoint: SocketAddr,
        },
    }

    #[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub enum Response {
        Bound,
        Read(plan::Value),
    }
}
