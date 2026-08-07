//! Talking to a plugin that runs as its own process.
//!
//! A request spawns the plugin, asks one thing, and lets it exit.

use std::{net::SocketAddr, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use crucible_core::{
    ipc::codec::{read_frame, write_frame},
    plan,
};
use tokio::{process::Command, time::timeout};

use crate::{
    error::Error,
    protocol::{Description, Request, Response, observer},
    role::{BoxFuture, ObserverRuntime, Query, Targeted},
};

/// How long a plugin has to answer. Its slowest answer is one query against a
/// service of the fleet, so anything approaching this is a plugin in trouble
/// rather than a plugin being thorough.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(5);

/// A plugin binary, and the name it answered to.
pub struct Plugin {
    name: String,
    path: PathBuf,
}

impl Plugin {
    /// Ask the binary at `path` what it is.
    ///
    /// # Errors
    /// Errors if it cannot be run, does not answer in time, or answers with
    /// something other than a description of itself.
    pub async fn describe(path: PathBuf) -> Result<Description, Error> {
        let asking = Plugin {
            name: path.display().to_string(),
            path,
        };
        match asking.ask(Request::Describe).await? {
            Response::Described(description) => Ok(description),
            other => Err(asking.confused("a description", &other)),
        }
    }

    /// A plugin known to be called `name` and to live at `path`.
    #[must_use]
    pub fn new(name: String, path: PathBuf) -> Self {
        Self { name, path }
    }

    /// This plugin, reading the state of one service.
    #[must_use]
    pub fn observing(self: &Arc<Self>, service: &plan::Service) -> Box<dyn ObserverRuntime> {
        Box::new(Observing {
            plugin: self.clone(),
            service: service.clone(),
        })
    }

    /// Run the plugin, ask it one thing, and let it exit.
    async fn ask(&self, request: Request) -> Result<Response, Error> {
        let mut child = Command::new(&self.path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| self.failed(&format!("cannot run {}: {e}", self.path.display())))?;
        let exchange = async {
            let mut input = child.stdin.take().ok_or_else(|| self.failed("no stdin"))?;
            let mut output = child
                .stdout
                .take()
                .ok_or_else(|| self.failed("no stdout"))?;
            write_frame(&mut input, &request)
                .await
                .map_err(|e| self.failed(&format!("cannot ask: {e}")))?;
            // Closing the pipe is how the plugin is told there is nothing more,
            // so it answers and exits rather than waiting to be killed.
            drop(input);
            read_frame(&mut output)
                .await
                .map_err(|e| self.failed(&format!("cannot read the answer: {e}")))
        };

        let answer = if let Ok(answer) = timeout(ANSWER_TIMEOUT, exchange).await {
            answer
        } else {
            let _ = child.kill().await;
            Err(self.failed(&format!("did not answer within {ANSWER_TIMEOUT:?}")))
        };
        // Reaped either way, so a plugin that answered and then hung around does
        // not become a zombie.
        let _ = child.wait().await;
        answer
    }

    fn failed(&self, what: &str) -> Error {
        Error::new("plugin", format!("`{}` {what}", self.name))
    }

    fn confused(&self, expected: &str, got: &Response) -> Error {
        self.failed(&format!("was asked for {expected} and answered {got:?}"))
    }
}

/// A plugin process reading the state of one service.
struct Observing {
    plugin: Arc<Plugin>,
    service: plan::Service,
}

impl ObserverRuntime for Observing {
    fn prepare<'a>(
        &'a self,
        check: &'a plan::Check,
    ) -> BoxFuture<'a, Result<Box<dyn Query>, Error>> {
        Box::pin(async move {
            let request = observer::Request::Bind {
                service: self.service.clone(),
                check: check.clone(),
            };
            match self
                .plugin
                .ask(Request::Observer(Box::new(request)))
                .await?
            {
                Response::Observer(observer::Response::Bound) => Ok(Box::new(Reading {
                    plugin: self.plugin.clone(),
                    service: self.service.clone(),
                    check: check.clone(),
                })
                    as Box<dyn Query>),
                Response::Failed(why) => {
                    Err(self.plugin.failed(&format!("refused the check: {why}")))
                }
                other => Err(self.plugin.confused("a bound check", &other)),
            }
        })
    }
}

/// A check a plugin has agreed it can answer.
struct Reading {
    plugin: Arc<Plugin>,
    service: plan::Service,
    check: plan::Check,
}

impl Targeted for Reading {
    fn target(&self) -> &str {
        &self.check.service
    }
}

impl Query for Reading {
    fn read(&self, endpoint: SocketAddr) -> BoxFuture<'_, Result<plan::Value, Error>> {
        Box::pin(async move {
            let request = observer::Request::Read {
                service: self.service.clone(),
                check: self.check.clone(),
                endpoint,
            };
            match self
                .plugin
                .ask(Request::Observer(Box::new(request)))
                .await?
            {
                Response::Observer(observer::Response::Read(value)) => Ok(value),
                Response::Failed(why) => Err(self.plugin.failed(&format!("could not read: {why}"))),
                other => Err(self.plugin.confused("a reading", &other)),
            }
        })
    }
}
