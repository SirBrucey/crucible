use std::{io, net::AddrParseError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("pair `{pair}` must be in the form SERVICE=LISTEN=UPSTREAM")]
    MalformedPair { pair: String },
    #[error("fault-at `{spec}` must be in the form SERVICE=DIRECTION=K (DIRECTION is c2u or u2c)")]
    MalformedFaultAt { spec: String },
    #[error("fault-at names service `{service}`, which no --pair fronts")]
    UnknownFaultService { service: String },
    #[error("fault-at names client `{client}`, which no --service names")]
    UnknownFaultClient { client: String },
    #[error(
        "service `{service}` does not resolve at `{host}`, so nothing it dials would be recognised"
    )]
    UnresolvedService { service: String, host: String },
    #[error("parse listen `{addr}`: {source}")]
    ParseListen {
        addr: String,
        source: AddrParseError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
