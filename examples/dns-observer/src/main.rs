//! An observer plugin that reads what a nameserver answers.
//!
//! Nothing in crucible knows this exists. It is built on its own, installed by
//! dropping the binary in a plugin directory, and found by being asked what it
//! is.

use std::net::SocketAddr;

use crucible_plugin::{
    BoxFuture, Error as PluginError, Observer, ObserverRuntime, Query, Targeted, plan,
    schema::{CmpOp, HeadPattern, OpSig, Param, ParamType, ValueType},
};
use hickory_resolver::{
    Resolver,
    config::{ConnectionConfig, NameServerConfig, ProtocolConfig, ResolverConfig},
    net::{NetError, runtime::TokioRuntimeProvider},
};

/// Reads the address a nameserver answers with.
struct Dns;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("`{0}` is not an observable this reads")]
    Observable(String),
    #[error("`resolve` names the host to look up")]
    NoHost,
    #[error("`{host}` did not resolve: {source}")]
    Unresolved { host: String, source: NetError },
    #[error("`{0}` resolved to nothing")]
    NoAddress(String),
}

impl From<Error> for PluginError {
    fn from(e: Error) -> Self {
        Self::new(Dns::NAME, e)
    }
}

impl Observer for Dns {
    const NAME: &'static str = "dns";
    type Query = Lookup;
    type Error = Error;

    fn runtime(_service: &plan::Service) -> Self {
        Dns
    }

    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::observable(
                HeadPattern::exact("resolve"),
                ValueType::Str,
                CmpOp::ALL.to_vec(),
            )
            .with_param(Param::required("host", ParamType::Str)),
        ]
    }

    fn bind(check: &plan::Check) -> Result<Self::Query, Self::Error> {
        let [reading] = check.observable.as_slice() else {
            return Err(Error::Observable(check.observable.join(".")));
        };
        if reading != "resolve" {
            return Err(Error::Observable(reading.clone()));
        }
        let Some(plan::Value::Str(host)) = check.args.first() else {
            return Err(Error::NoHost);
        };
        Ok(Lookup {
            service: check.service.clone(),
            host: host.clone(),
        })
    }
}

impl ObserverRuntime for Dns {
    fn prepare<'a>(
        &'a self,
        check: &'a plan::Check,
    ) -> BoxFuture<'a, Result<Box<dyn Query>, PluginError>> {
        Box::pin(async move { Ok(Box::new(Dns::bind(check)?) as Box<dyn Query>) })
    }
}

/// One host, to be looked up against the service that answers for it.
struct Lookup {
    service: String,
    host: String,
}

impl Targeted for Lookup {
    fn target(&self) -> &str {
        &self.service
    }
}

impl Query for Lookup {
    fn read(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<plan::Value, PluginError>> {
        Box::pin(async move { self.resolve(endpoint).await.map_err(PluginError::from) })
    }
}

impl Lookup {
    /// Ask the nameserver at `endpoint` rather than whatever this host resolves
    /// through: the answer being tested is the fleet's, not the machine's.
    async fn resolve(&self, endpoint: SocketAddr) -> Result<plan::Value, Error> {
        // The endpoint is where the deployment published the service, so the
        // port is the replica's rather than 53.
        let mut connection = ConnectionConfig::new(ProtocolConfig::Udp);
        connection.port = endpoint.port();
        let mut nameserver = NameServerConfig::udp(endpoint.ip());
        nameserver.connections = vec![connection];
        let config = ResolverConfig::from_parts(None, Vec::new(), vec![nameserver]);
        let resolver = Resolver::builder_with_config(config, TokioRuntimeProvider::default())
            .build()
            .map_err(|source| Error::Unresolved {
                host: self.host.clone(),
                source,
            })?;
        let answer = resolver
            .lookup_ip(&self.host)
            .await
            .map_err(|source| Error::Unresolved {
                host: self.host.clone(),
                source,
            })?;
        let address = answer
            .iter()
            .next()
            .ok_or_else(|| Error::NoAddress(self.host.clone()))?;
        Ok(plan::Value::Str(address.to_string()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crucible_plugin::serve_observer::<Dns>().await?;
    Ok(())
}
