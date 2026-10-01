//! What the framework understands of the MySQL client/server protocol, which
//! MariaDB speaks.

/// The kind a service declares to be read as this.
pub const NAME: &str = "mariadb";

pub mod statement;

use std::sync::Arc;

use crucible_protocol::{Direction, Kind};

/// A reader for each direction of one connection, watching for `mark` on
/// whichever direction carries it.
///
/// Made as a pair, because the two directions have to agree about whether the
/// connection was encrypted and what the client put by to run later.
#[must_use]
pub fn readers(watching: Option<&(Direction, String)>) -> (Box<dyn Kind>, Box<dyn Kind>) {
    let shared = Arc::new(statement::Shared::default());
    let reader = |direction: Direction| -> Box<dyn Kind> {
        let mark = watching
            .filter(|(way, _)| *way == direction)
            .map(|(_, mark)| mark.clone());
        Box::new(statement::Reader::new(direction, Arc::clone(&shared), mark))
    };
    (
        reader(Direction::ClientToUpstream),
        reader(Direction::UpstreamToClient),
    )
}
