//! The kind plugins compiled into this build, and what they read.
//!
//! Kinds are resolved by name at run time but linked at build time, so what a
//! fleet can be read as is settled when the framework is built. Whatever holds
//! this registry is the only thing that names a protocol; everything else asks
//! for a kind and gets something that can read it.

use std::borrow::Cow;

use crucible_protocol::{Carried, Direction, Kind};

/// What reads one pair's traffic, watching for a moment if a schedule named
/// one.
///
/// A reader is made per connection, since what one carries says nothing about
/// where another has got to. What they share stays here: a connection is not
/// the unit a count of reads means anything in, because the fleet may open
/// several.
#[derive(Clone)]
pub struct Kinds {
    kind: String,
    /// The moment to watch for, and which way the traffic carrying it runs.
    watching: Option<(Direction, String)>,
}

impl Kinds {
    #[must_use]
    pub fn new(kind: &str, watching: Option<(Direction, String)>) -> Self {
        Self {
            kind: kind.to_owned(),
            watching,
        }
    }

    /// Something to read one direction of one connection with.
    #[must_use]
    pub fn reader(&self, direction: Direction) -> Box<dyn Kind> {
        let mark = self
            .watching
            .as_ref()
            .filter(|(way, _)| *way == direction)
            .map(|(_, mark)| mark.clone());
        match (self.kind.as_str(), mark) {
            (crucible_kind_amqp::NAME, Some(mark)) => Box::new(
                crucible_kind_amqp::message::Reader::watching(direction, mark),
            ),
            (crucible_kind_amqp::NAME, None) => {
                Box::new(crucible_kind_amqp::message::Reader::new(direction))
            }
            // A mark on an unread kind is a count of reads, so every read is a
            // candidate and whoever holds the count picks between them.
            (_, mark) => Box::new(Unread {
                watching: mark.is_some(),
            }),
        }
    }
}

/// Whether a kind has a plugin that can read it.
#[must_use]
pub fn is_read(kind: &str) -> bool {
    matches!(kind, crucible_kind_amqp::NAME)
}

/// How many candidate moments must pass before the one `mark` names.
///
/// A kind with a plugin recognises its own moment, so the first candidate it
/// offers is the one. Without a plugin a mark is a count of reads, and the
/// count is kept outside: only the framework knows which of a pair's
/// connections carry the edge it was taken from.
#[must_use]
pub fn nth(kind: &str, mark: &str) -> u32 {
    if is_read(kind) {
        1
    } else {
        mark.parse().unwrap_or(1)
    }
}

/// Carries bytes it cannot read, offering every read off the wire as a moment.
///
/// One read is the only boundary it can see, which is what an anchor meant
/// before any protocol could be parsed. It can say nothing about where a fault
/// should go, so the framework works that out from the traffic it observed
/// instead.
struct Unread {
    watching: bool,
}

impl Kind for Unread {
    fn carry<'a>(&mut self, bytes: &'a [u8]) -> Carried<'a> {
        Carried {
            // One read, which it has to take whole because it cannot see
            // anything smaller.
            forward: vec![Cow::Borrowed(bytes)],
            freeze_after: self.watching.then_some(1),
            // It cannot say where a fault should go.
            found: Vec::new(),
            did: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An unread kind watching for a moment on the way in.
    fn watching() -> Kinds {
        Kinds::new("http", Some((Direction::ClientToUpstream, "2".to_owned())))
    }

    #[test]
    fn a_kind_with_a_plugin_is_read_by_it() {
        assert!(is_read(crucible_kind_amqp::NAME));
    }

    /// A fleet speaking something with no plugin still runs; the framework just
    /// has to work out where to fault it without help.
    #[test]
    fn a_kind_with_no_plugin_is_carried_untouched() {
        assert!(!is_read("http"));
        let mut reader = Kinds::new("http", None).reader(Direction::ClientToUpstream);
        let carried = reader.carry(b"anything at all");
        assert_eq!(carried.forward.concat(), b"anything at all");
        assert_eq!(carried.freeze_after, None);
        assert!(carried.found.is_empty());
    }

    /// With no plugin to recognise anything, one read off the wire is the only
    /// boundary there is, so every one of them is a moment a fault could go on.
    #[test]
    fn a_kind_with_no_plugin_offers_every_read() {
        let mut reader = watching().reader(Direction::ClientToUpstream);
        assert_eq!(reader.carry(b"one").freeze_after, Some(1));
        assert_eq!(reader.carry(b"two").freeze_after, Some(1));
    }

    /// Only the direction a schedule named offers moments; the other way is
    /// carried and nothing more.
    #[test]
    fn the_other_direction_offers_nothing() {
        let mut other = watching().reader(Direction::UpstreamToClient);
        assert_eq!(other.carry(b"reply").freeze_after, None);
    }

    /// A mark on an unread kind is a count the framework keeps, so a plugin's
    /// own mark is the first thing it offers and nothing is counted through.
    #[test]
    fn a_read_kind_names_its_own_moment() {
        assert_eq!(nth("http", "3"), 3);
        assert_eq!(nth(crucible_kind_amqp::NAME, "ack:7:before"), 1);
    }
}
