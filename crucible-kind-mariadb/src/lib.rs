//! What the framework understands of the MySQL client/server protocol, which
//! MariaDB speaks.

/// The kind a service declares to be read as this.
pub const NAME: &str = "mariadb";

pub mod statement;

use crucible_protocol::{Direction, Kind};

/// A reader for each direction of one connection, watching for `mark` on
/// whichever direction carries it.
#[must_use]
pub fn readers(watching: Option<&(Direction, String)>) -> (Box<dyn Kind>, Box<dyn Kind>) {
    let reader = |direction: Direction| -> Box<dyn Kind> {
        let mark = watching
            .filter(|(way, _)| *way == direction)
            .map(|(_, mark)| mark.clone());
        Box::new(statement::Reader::new(direction, mark))
    };
    (
        reader(Direction::ClientToUpstream),
        reader(Direction::UpstreamToClient),
    )
}
