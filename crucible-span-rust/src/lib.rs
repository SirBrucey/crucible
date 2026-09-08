//! Crucible's Rust adapter: a `tracing_subscriber::Layer` that offers the
//! moments inside a service.
//!
//! A service already spans the work it does. This reports the boundaries of
//! those spans to the framework and at a moment the run named waits until the
//! framework says the service may carry on. The wait blocks the thread that
//! reached the boundary.

use std::{
    collections::HashMap,
    sync::{Mutex, mpsc},
    time::Duration,
};

use crucible_protocol::{Boundary, Released, Side, Watching};
use tracing_core::span::{Attributes, Id};
use tracing_subscriber::{
    layer::{Context, Layer},
    registry::LookupSpan,
};

/// How long a held service waits before carrying on regardless.
/// So a framework that has gone away cannot wedge the fleet it was testing.
const HELD_TIMEOUT: Duration = Duration::from_secs(30);

/// Where the framework is, set on every service it brings up.
const FRAMEWORK: &str = "CRUCIBLE_SPAN";

/// Reports the span boundaries a service reaches, and waits at the moment this
/// run named.
pub struct Boundaries {
    watching: Watching,
    url: String,
    agent: ureq::Agent,
    /// Which time each span has been reached this run. Uses `tracing`'s ID,
    /// so a boundary start and its close name the same moment.
    nth: Mutex<HashMap<u64, u32>>,
    /// How many of each span have been reached.
    counts: Mutex<HashMap<String, u32>>,
    /// Boundaries this run only reports, handed to a thread so the service is
    /// not slowed by reporting them.
    reports: Option<mpsc::Sender<Boundary>>,
}

impl Boundaries {
    /// Join the run this service was brought up in, or `None` outside a run, so
    /// a service can install this unconditionally.
    ///
    /// # Errors
    /// Errors if the framework is named but cannot be reached.
    pub fn joining() -> Result<Option<Self>, ureq::Error> {
        match std::env::var(FRAMEWORK) {
            Ok(framework) => Self::joined(&framework).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Ask the framework what this run wants, and report accordingly.
    ///
    /// # Errors
    /// Errors if the framework cannot be reached or does not say.
    pub fn joined(framework: &str) -> Result<Self, ureq::Error> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(HELD_TIMEOUT))
            .build()
            .into();
        let watching: Watching = agent
            .get(&format!("{framework}/watching"))
            .call()?
            .body_mut()
            .read_json()?;
        Ok(Self::new(watching, framework, agent))
    }

    fn new(watching: Watching, framework: &str, agent: ureq::Agent) -> Self {
        let url = format!("{framework}/boundary");
        // A run that only reports must not be slowed by reporting.
        let reports = matches!(watching, Watching::Reporting).then(|| {
            let (tx, rx) = mpsc::channel::<Boundary>();
            let posting = agent.clone();
            let to = url.clone();
            std::thread::spawn(move || {
                for boundary in rx {
                    match posting.post(&to).send_json(&boundary) {
                        // Read even though there is nothing worth reading. The
                        // connection goes back in the pool only once its body
                        // has been read.
                        Ok(mut answer) => {
                            if let Err(e) = answer.body_mut().read_to_vec() {
                                tracing::debug!(
                                    target: "crucible::span", %e,
                                    "the framework answered a report with nothing readable",
                                );
                            }
                        }
                        Err(e) => tracing::warn!(
                            target: "crucible::span", mark = %boundary.mark(), %e,
                            "a moment could not be reported, so the run will not know of it",
                        ),
                    }
                }
            });
            tx
        });
        Self {
            watching,
            url,
            agent,
            nth: Mutex::new(HashMap::new()),
            counts: Mutex::new(HashMap::new()),
            reports,
        }
    }

    /// Say a boundary was reached, and wait if this run named it.
    fn reached(&self, span: &str, nth: u32, side: Side) {
        let boundary = Boundary {
            span: span.to_owned(),
            side,
            nth,
        };
        if !self.watching.holds(&boundary.mark()) {
            if let Some(reports) = &self.reports
                && let Err(e) = reports.send(boundary)
            {
                tracing::warn!(
                    target: "crucible::span", %e,
                    "a moment could not be queued, so the run will not know of it",
                );
            }
            return;
        }
        // Blocking on the thread that reached the boundary. The service is
        // held here until the framework answers.
        match self.agent.post(&self.url).send_json(&boundary) {
            Ok(mut answer) => match answer.body_mut().read_json::<Released>() {
                Ok(released) => tracing::debug!(
                    target: "crucible::span",
                    span, %side, at_ns = released.at_ns as u64,
                    "the framework let the service go",
                ),
                Err(e) => tracing::warn!(
                    target: "crucible::span",
                    span, %e,
                    "the framework did not say, so the service carries on",
                ),
            },
            Err(e) => tracing::warn!(
                target: "crucible::span",
                span, %e,
                "the framework could not be reached, so the service carries on",
            ),
        }
    }

    /// The number of this span.
    fn number(&self, span: &str, id: u64) -> u32 {
        let mut counts = self.held(&self.counts);
        let count = counts.entry(span.to_owned()).or_default();
        *count += 1;
        let nth = *count;
        self.held(&self.nth).insert(id, nth);
        nth
    }

    /// What number the span `id` was given, if this reported its opening.
    fn numbered(&self, id: u64) -> Option<u32> {
        self.held(&self.nth).remove(&id)
    }

    fn held<'a, T>(&self, lock: &'a Mutex<T>) -> std::sync::MutexGuard<'a, T> {
        lock.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<S> Layer<S> for Boundaries
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    /// A span starting, which is before the work it covers runs.
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
        let span = attrs.metadata().name();
        let nth = self.number(span, id.into_u64());
        self.reached(span, nth, Side::Started);
    }

    /// A span ending, which is after the work it covers has run.
    ///
    /// Taken from the span closing rather than `tracing` exiting it, as an
    /// asynchronous span is exited every time its future yields.
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(nth) = self.numbered(id.into_u64()) else {
            return;
        };
        if let Some(span) = ctx.span(&id) {
            let name = span.metadata().name();
            self.reached(name, nth, Side::Ended);
        }
    }
}

impl std::fmt::Debug for Boundaries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Boundaries")
            .field("watching", &self.watching)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}
