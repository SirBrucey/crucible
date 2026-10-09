//! What the framework understands of AMQP 0-9-1.

/// The kind a service declares to be read as this.
pub const NAME: &str = "amqp";

pub mod message;

use crucible_protocol::{Direction, Kind};

/// A reader for each direction of one connection, watching for `mark` on
/// whichever direction carries it.
///
/// Made as a pair, because the two directions have to agree. A delivery and its
/// ack cross opposite ways, so neither reader sees both.
#[must_use]
pub fn readers(watching: Option<&(Direction, String)>) -> (Box<dyn Kind>, Box<dyn Kind>) {
    let consuming = message::Consuming::default();
    let publishing = message::Publishing::default();
    let reader = |direction: Direction| -> Box<dyn Kind> {
        let mark = watching
            .filter(|(way, _)| *way == direction)
            .map(|(_, mark)| mark.clone());
        match mark {
            Some(mark) => Box::new(message::Reader::watching(
                direction,
                consuming.clone(),
                publishing.clone(),
                mark,
            )),
            None => Box::new(message::Reader::new(
                direction,
                consuming.clone(),
                publishing.clone(),
            )),
        }
    };
    (
        reader(Direction::ClientToUpstream),
        reader(Direction::UpstreamToClient),
    )
}
