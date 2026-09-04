//! What the framework understands of AMQP 0-9-1.

/// The kind a service declares to be read as this.
pub const NAME: &str = "amqp";

pub mod message;

use crucible_protocol::{Direction, Kind};

/// Something to read each direction of one connection with, watching for `mark`
/// on the direction that carries it.
///
/// Made as a pair, because the two directions have to agree: a delivery and the
/// ack cross opposite ways, so neither reader sees both.
#[must_use]
pub fn readers(watching: Option<&(Direction, String)>) -> (Box<dyn Kind>, Box<dyn Kind>) {
    let consuming = message::Consuming::default();
    let reader = |direction: Direction| -> Box<dyn Kind> {
        let mark = watching
            .filter(|(way, _)| *way == direction)
            .map(|(_, mark)| mark.clone());
        match mark {
            Some(mark) => Box::new(message::Reader::watching(
                direction,
                consuming.clone(),
                mark,
            )),
            None => Box::new(message::Reader::new(direction, consuming.clone())),
        }
    };
    (
        reader(Direction::ClientToUpstream),
        reader(Direction::UpstreamToClient),
    )
}
