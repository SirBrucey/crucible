//! The first-party plugins.

use crucible_core::plan;

pub mod deployment;
pub mod driver;
pub mod observer;

pub use deployment::Docker;
pub use driver::Http;
pub use observer::{Mariadb, http::Http as HttpObserver};

/// The headers a service declares in its `headers` attribute.
pub(crate) fn http_headers(service: &plan::Service) -> Vec<(String, String)> {
    service
        .attr("headers")
        .and_then(plan::Value::as_map)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}
