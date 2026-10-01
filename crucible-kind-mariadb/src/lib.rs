//! What the framework understands of the MySQL client/server protocol, which
//! MariaDB speaks.

/// The kind a service declares to be read as this.
pub const NAME: &str = "mariadb";

pub mod statement;

use std::sync::{Arc, atomic::AtomicBool};

use crucible_protocol::{Direction, Kind};

/// A reader for each direction of one connection, watching for `mark` on
/// whichever direction carries it.
///
/// Made as a pair, because the two directions have to agree about whether the
/// connection was encrypted.
#[must_use]
pub fn readers(watching: Option<&(Direction, String)>) -> (Box<dyn Kind>, Box<dyn Kind>) {
    // Only the client can ask to encrypt the connection, and if it is neither
    // direction can read the edge.
    let encrypted = Arc::new(AtomicBool::new(false));
    let reader = |direction: Direction| -> Box<dyn Kind> {
        let mark = watching
            .filter(|(way, _)| *way == direction)
            .map(|(_, mark)| mark.clone());
        Box::new(statement::Reader::new(
            direction,
            Arc::clone(&encrypted),
            mark,
        ))
    };
    (
        reader(Direction::ClientToUpstream),
        reader(Direction::UpstreamToClient),
    )
}
