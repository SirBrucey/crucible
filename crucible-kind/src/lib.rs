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
/// The pair's settings, and nothing a connection carries. Readers are made a
/// connection at a time, since what one carries says nothing about where
/// another has got to, and what a connection's two directions agree on is made
/// along with them.
#[derive(Clone)]
pub struct Kinds {
    kind: String,
    /// The moment to watch for, and which way the traffic carrying it runs.
    watching: Option<(Direction, String)>,
}

/// Both directions of one connection, read by things that agree with each
/// other and with no other connection.
pub struct Readers {
    pub client_to_upstream: Box<dyn Kind>,
    pub upstream_to_client: Box<dyn Kind>,
}

impl Kinds {
    #[must_use]
    pub fn new(kind: &str, watching: Option<(Direction, String)>) -> Self {
        Self {
            kind: kind.to_owned(),
            watching,
        }
    }

    /// Something to read each direction of one connection with.
    ///
    /// The plugin that can read the kind makes both, since what the two
    /// directions share is its own. A kind nothing reads gets a pair that only
    /// counts what crosses.
    #[must_use]
    pub fn connection(&self) -> Readers {
        if self.kind == crucible_kind_amqp::NAME {
            let (client_to_upstream, upstream_to_client) =
                crucible_kind_amqp::readers(self.watching.as_ref());
            return Readers {
                client_to_upstream,
                upstream_to_client,
            };
        }
        // A mark on an unread kind is a count of reads, so every read is a
        // candidate and whoever holds the count picks between them.
        let watching = |direction: Direction| Unread {
            watching: self
                .watching
                .as_ref()
                .is_some_and(|(way, _)| *way == direction),
        };
        Readers {
            client_to_upstream: Box::new(watching(Direction::ClientToUpstream)),
            upstream_to_client: Box::new(watching(Direction::UpstreamToClient)),
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
    // It cannot read the traffic, so it cannot change it either, and whether a
    // fault may be placed here is not its business.
    fn carry<'a>(&mut self, bytes: &'a [u8], _placing: bool) -> Carried<'a> {
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
    use amq_protocol::{
        frame::{AMQPContentHeader, AMQPFrame, WriteContext, gen_frame},
        protocol::{
            AMQPClass,
            basic::{AMQPMethod, AMQPProperties, Ack, Deliver, Qos},
        },
    };

    use super::*;

    /// The channel the traffic under test runs on.
    const CHANNEL: u16 = 1;

    /// An unread kind watching for a moment on the way in.
    fn watching() -> Kinds {
        Kinds::new("http", Some((Direction::ClientToUpstream, "2".to_owned())))
    }

    /// A frame as it goes on the wire.
    fn wire(frame: &AMQPFrame) -> Vec<u8> {
        let write = gen_frame::<Vec<u8>>(frame);
        let (bytes, _) = write(WriteContext::from(Vec::new()))
            .expect("a frame serialises")
            .into_inner();
        bytes
    }

    fn method(method: AMQPMethod) -> Vec<u8> {
        wire(&AMQPFrame::Method(CHANNEL, AMQPClass::Basic(method)))
    }

    /// A consumer saying it will hold one delivery at a time, which leaves it
    /// nowhere to be told things out of order.
    fn takes_one_at_a_time() -> Vec<u8> {
        method(AMQPMethod::Qos(Qos {
            prefetch_count: 1,
            global: false,
        }))
    }

    /// A delivery the broker labelled `tag`, carrying a body.
    fn delivery(tag: u64) -> Vec<u8> {
        let payload = b"an order";
        let mut bytes = method(AMQPMethod::Deliver(Deliver {
            consumer_tag: "consumer-1".into(),
            delivery_tag: tag,
            redelivered: false,
            ..Default::default()
        }));
        bytes.extend(wire(&AMQPFrame::Header(
            CHANNEL,
            AMQPContentHeader {
                class_id: 60,
                body_size: payload.len() as u64,
                properties: AMQPProperties::default(),
            },
        )));
        bytes.extend(wire(&AMQPFrame::Body(CHANNEL, payload.to_vec())));
        bytes
    }

    /// A consumer finishing with the delivery the broker labelled `tag`.
    fn finished_with(tag: u64) -> Vec<u8> {
        method(AMQPMethod::Ack(Ack {
            delivery_tag: tag,
            multiple: false,
        }))
    }

    /// Whether two deliveries down `readers` offer somewhere to reorder.
    ///
    /// The consumer finishes with each before the next arrives, so what it was
    /// told it could hold is all this connection leaves behind.
    fn offers_a_reorder(readers: &mut Readers) -> bool {
        let mut offered = false;
        for tag in [1, 2] {
            offered |= readers
                .upstream_to_client
                .carry(&delivery(tag), false)
                .found
                .iter()
                .any(|placement| placement.mark.starts_with("reorder:"));
            readers.client_to_upstream.carry(&finished_with(tag), false);
        }
        offered
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
        let mut reader = Kinds::new("http", None).connection().client_to_upstream;
        let carried = reader.carry(b"anything at all", false);
        assert_eq!(carried.forward.concat(), b"anything at all");
        assert_eq!(carried.freeze_after, None);
        assert!(carried.found.is_empty());
    }

    /// With no plugin to recognise anything, one read off the wire is the only
    /// boundary there is, so every one of them is a moment a fault could go on.
    #[test]
    fn a_kind_with_no_plugin_offers_every_read() {
        let mut reader = watching().connection().client_to_upstream;
        assert_eq!(reader.carry(b"one", true).freeze_after, Some(1));
        assert_eq!(reader.carry(b"two", true).freeze_after, Some(1));
    }

    /// The two directions of one connection read the same traffic between
    /// them: what the consumer told the broker on the way out settles what the
    /// deliveries coming back are worth.
    #[test]
    fn the_two_directions_of_a_connection_agree() {
        let mut readers = Kinds::new(crucible_kind_amqp::NAME, None).connection();
        readers
            .client_to_upstream
            .carry(&takes_one_at_a_time(), false);
        assert!(!offers_a_reorder(&mut readers));
    }

    /// A connection agrees with itself and with nothing else. A channel numbers
    /// its deliveries from the connection it is on, so one connection's
    /// bookkeeping read as another's would answer for the wrong traffic.
    #[test]
    fn one_connection_does_not_answer_for_another() {
        let kinds = Kinds::new(crucible_kind_amqp::NAME, None);
        let mut consumer = kinds.connection();
        consumer
            .client_to_upstream
            .carry(&takes_one_at_a_time(), false);
        assert!(!offers_a_reorder(&mut consumer));

        let mut another = kinds.connection();
        assert!(
            offers_a_reorder(&mut another),
            "this one said nothing about how many it would hold"
        );
    }

    /// Only the direction a schedule named offers moments; the other way is
    /// carried and nothing more.
    #[test]
    fn the_other_direction_offers_nothing() {
        let mut other = watching().connection().upstream_to_client;
        assert_eq!(other.carry(b"reply", true).freeze_after, None);
    }

    /// A mark on an unread kind is a count the framework keeps, so a plugin's
    /// own mark is the first thing it offers and nothing is counted through.
    #[test]
    fn a_read_kind_names_its_own_moment() {
        assert_eq!(nth("http", "3"), 3);
        assert_eq!(nth(crucible_kind_amqp::NAME, "ack:7:before"), 1);
    }
}
