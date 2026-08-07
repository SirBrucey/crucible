//! Running a plugin as its own process.
//!
//! The framework ships the loop, so a plugin author writes the same impl a
//! compiled-in plugin does and hands it over:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     crucible_plugin::serve_observer::<Dns>().await?;
//!     Ok(())
//! }
//! ```

use crucible_core::ipc::codec::{self, read_frame, write_frame};
use tokio::io::{stdin, stdout};

use crate::{
    error::Error,
    protocol::{Description, Request, Response, Role, VERSION, observer},
    role::Observer,
};

/// Answer requests for `O` until the framework stops asking.
///
/// # Errors
/// Errors if a request cannot be read or an answer cannot be written. A request
/// the plugin cannot carry out is answered, not returned: only losing the
/// framework ends the process.
pub async fn serve_observer<O: Observer + 'static>() -> Result<(), codec::Error> {
    let mut input = stdin();
    let mut output = stdout();
    loop {
        let request: Request = match read_frame(&mut input).await {
            Ok(request) => request,
            // The framework closed the pipe, which is how it says it is done.
            Err(codec::Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let response = match answer::<O>(request).await {
            Ok(response) => response,
            Err(e) => Response::Failed(e.to_string()),
        };
        write_frame(&mut output, &response).await?;
    }
}

async fn answer<O: Observer + 'static>(request: Request) -> Result<Response, Error> {
    Ok(match request {
        Request::Describe => Response::Described(Description {
            protocol: VERSION,
            name: O::NAME.to_owned(),
            attrs: O::attr_schema(),
            role: Role::Observer {
                signatures: O::signatures(),
            },
        }),
        Request::Observer(request) => match *request {
            observer::Request::Bind { service, check } => {
                O::runtime(&service).prepare(&check)?;
                Response::Observer(observer::Response::Bound)
            }
            observer::Request::Read {
                service,
                check,
                endpoint,
            } => {
                let query = O::runtime(&service).prepare(&check)?;
                let value = query.read(endpoint).await?;
                Response::Observer(observer::Response::Read(value))
            }
        },
    })
}
