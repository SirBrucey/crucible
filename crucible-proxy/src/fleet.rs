//! Who is at each address, which is what names both ends of an edge and
//! whoever reported a moment from inside itself.
//!
//! A service answers to its name only once it is up, so the fleet is named in
//! the background from start-up rather than at bind.

use std::{collections::HashMap, net::IpAddr, sync::Arc, time::Duration};

use crucible_protocol::ServiceHost;
use tokio::sync::OnceCell;

use crate::error::{Error, Result};

/// How long to leave a fleet that has not answered before asking again.
const AGAIN: Duration = Duration::from_millis(500);

/// The fleet's services and once it has answered their addresses.
pub struct Named {
    hosts: Vec<ServiceHost>,
    named: OnceCell<HashMap<IpAddr, String>>,
}

impl Named {
    #[must_use]
    pub fn new(hosts: Vec<ServiceHost>) -> Self {
        Self {
            hosts,
            named: OnceCell::new(),
        }
    }

    /// Every address the fleet holds, resolved all at once so a fleet still
    /// coming up caches nothing.
    ///
    /// # Errors
    /// Errors if any service does not answer.
    pub async fn all(&self) -> Result<&HashMap<IpAddr, String>> {
        self.named.get_or_try_init(|| resolve(&self.hosts)).await
    }

    /// Start naming the fleet in the background, asking again until it
    /// answers.
    pub fn warm(self: &Arc<Self>) {
        let naming = Arc::clone(self);
        tokio::spawn(async move {
            while let Err(e) = naming.all().await {
                tracing::debug!(%e, "the fleet has not answered to its names yet");
                tokio::time::sleep(AGAIN).await;
            }
        });
    }

    /// Which service is at `peer`, or `None` for an address the fleet does not
    /// hold or for a service that has not answered yet. Never waits.
    #[must_use]
    pub fn at(&self, peer: IpAddr) -> Option<&str> {
        self.named
            .get()
            .and_then(|named| named.get(&peer).map(String::as_str))
    }

    /// A fleet whose addresses are already known.
    /// For a test that has no fleet.
    /// to ask.
    #[cfg(test)]
    #[must_use]
    pub fn known(named: HashMap<IpAddr, String>) -> Self {
        Self {
            hosts: Vec::new(),
            named: OnceCell::from(named),
        }
    }

    /// What the fleet was told it holds, whether or not it has answered yet.
    #[must_use]
    pub fn hosts(&self) -> &[ServiceHost] {
        &self.hosts
    }
}

async fn resolve(hosts: &[ServiceHost]) -> Result<HashMap<IpAddr, String>> {
    let mut fleet = HashMap::new();
    for ServiceHost { name, host } in hosts {
        let Ok(addrs) = tokio::net::lookup_host((host.as_str(), 0)).await else {
            return Err(Error::UnresolvedService {
                service: name.clone(),
                host: host.clone(),
            });
        };
        for addr in addrs {
            fleet.insert(addr.ip(), name.clone());
        }
    }
    Ok(fleet)
}
