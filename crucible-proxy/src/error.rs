use std::{io, net::AddrParseError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("pair `{pair}` must be in the form SERVICE=LISTEN=UPSTREAM")]
    MalformedPair { pair: String },
    #[error("freeze-at `{spec}` must be in the form SERVICE=DIRECTION=K (DIRECTION is c2u or u2c)")]
    MalformedFreezeAt { spec: String },
    #[error("freeze-at names service `{service}`, which no --pair fronts")]
    UnknownFreezeService { service: String },
    #[error("parse listen `{addr}`: {source}")]
    ParseListen {
        addr: String,
        source: AddrParseError,
    },
    #[error("resolve upstream `{upstream}` produced no addresses")]
    UpstreamUnresolved { upstream: String },
}

pub type Result<T> = std::result::Result<T, Error>;
