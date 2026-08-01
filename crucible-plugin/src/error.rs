//! The error a plugin reports across the erased boundary.

/// A failure inside a plugin, naming the plugin it came from. Each plugin keeps
/// its own error type; this is what the framework sees once the concrete type is
/// erased.
#[derive(Debug, thiserror::Error)]
#[error("plugin `{plugin}`: {source}")]
pub struct Error {
    plugin: &'static str,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    pub fn new(
        plugin: &'static str,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            plugin,
            source: source.into(),
        }
    }

    /// Where the failure came from: a plugin name, or the framework component
    /// that could not reach one.
    #[must_use]
    pub fn plugin(&self) -> &'static str {
        self.plugin
    }
}
