//! What an instrumented service and the framework say to each other.
//!
//! A moment inside a service is not in the traffic the framework reads, so the
//! service reports its own span boundaries and waits at the one a run names.

use serde::{Deserialize, Serialize};

/// What an adapter loaded into a service does for a run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum Watching {
    /// Nothing: the service runs as though the framework were not there.
    Inert,
    /// Report each boundary as it passes, non-blocking.
    Reporting,
    /// Hold the service at each moment `at` names until the framework lets it
    /// go, and report the rest without blocking.
    Holding { at: Vec<String> },
}

impl Watching {
    /// Whether the moment `mark` names waits for the framework.
    #[must_use]
    pub fn holds(&self, mark: &str) -> bool {
        match self {
            Watching::Inert | Watching::Reporting => false,
            Watching::Holding { at } => at.iter().any(|held| held == mark),
        }
    }
}

/// Which end of a span a service has reached: OpenTelemetry's `on_start` and
/// `on_end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Side {
    Started,
    Ended,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Side::Started => "start",
            Side::Ended => "end",
        })
    }
}

/// A boundary and the service that reached it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Reached {
    pub service: String,
    pub boundary: Boundary,
}

impl Reached {
    /// What the moment names itself.
    #[must_use]
    pub fn mark(&self) -> String {
        self.boundary.mark()
    }
}

/// A span boundary a service has reached.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Boundary {
    /// What the service calls the span.
    pub span: String,
    pub side: Side,
    /// Which time the service has reached this boundary in this run.
    pub nth: u32,
}

impl Boundary {
    /// The moment as the schedule named it.
    #[must_use]
    pub fn mark(&self) -> String {
        format!("{}:{}:{}", self.span, self.nth, self.side)
    }
}

/// What the framework says back to a service it was holding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Released {
    /// When the framework let it go.
    pub at_ns: u128,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn holding() -> Watching {
        Watching::Holding {
            at: vec!["publish:1:start".to_owned()],
        }
    }

    #[rstest]
    #[case(Side::Started, 1, "publish:1:start")]
    #[case(Side::Ended, 2, "publish:2:end")]
    fn boundary_mark_format(#[case] side: Side, #[case] nth: u32, #[case] mark: &str) {
        let boundary = Boundary {
            span: "publish".to_owned(),
            side,
            nth,
        };

        assert_eq!(boundary.mark(), mark);
    }

    #[rstest]
    #[case(holding(), "publish:1:start", true)]
    #[case(holding(), "publish:1:end", false)]
    #[case(holding(), "publish:2:start", false)]
    #[case(holding(), "handle:1:start", false)]
    #[case(Watching::Reporting, "publish:1:start", false)]
    #[case(Watching::Inert, "publish:1:start", false)]
    fn watching_holds(#[case] watching: Watching, #[case] mark: &str, #[case] holds: bool) {
        assert_eq!(watching.holds(mark), holds);
    }
}
