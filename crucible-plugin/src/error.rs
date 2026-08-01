//! The error a plugin reports across the erased boundary.

use std::fmt;

/// A failure inside a plugin, naming the plugin it came from. Each plugin keeps
/// its own error type; this is what the framework sees once the concrete type is
/// erased.
#[derive(Debug)]
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

    /// The plugin that failed.
    #[must_use]
    pub fn plugin(&self) -> &'static str {
        self.plugin
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "plugin `{}`: {}", self.plugin, self.source)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
