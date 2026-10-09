use std::ops::Range;

use super::frame::Framed;
use amq_protocol::{
    frame::AMQPFrame,
    protocol::{AMQPClass, basic::AMQPMethod, channel},
};

/// One of a connection's channels, which the broker numbers from one.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Channel(u16);

/// What the broker calls a message on a channel, counting from one.
///
/// The broker's own numbering, so a tag means nothing without the channel it
/// was given on.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tag(u64);

impl Tag {
    /// The tag after this one.
    pub(super) fn increment(self) -> Tag {
        Tag(self.0 + 1)
    }

    /// The tag before this one, or `None` for a tag the broker never gave out.
    ///
    /// It numbers from one, so nothing comes before zero.
    pub(super) fn decrement(self) -> Option<Tag> {
        self.0.checked_sub(1).map(Tag)
    }
}

macro_rules! wraps {
    ($name:ident, $inner:ty) => {
        impl $name {
            /// The broker's own number, read as one of these.
            pub const fn new(inner: $inner) -> Self {
                Self(inner)
            }
        }

        impl From<$inner> for $name {
            fn from(inner: $inner) -> Self {
                Self::new(inner)
            }
        }

        impl From<$name> for $inner {
            fn from(outer: $name) -> Self {
                outer.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

wraps!(Channel, u16);
wraps!(Tag, u64);

/// What a fleet was doing, in terms a fault is placed against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// A message going to the broker.
    Publish,
    /// A message handed to a consumer, whether pushed or fetched.
    Deliver {
        /// What the broker labelled it, which names it on this channel.
        tag: Tag,
        /// Whether this message has been delivered before.
        redelivered: bool,
    },
    /// A consumer saying it is done with one, or the broker confirming a
    /// publish. `multiple` is the broker answering every publish up to `tag`
    /// rather than that one alone, which it decides for itself.
    Ack { tag: Tag, multiple: bool },
    /// A consumer refusing one, which is what makes the broker requeue it.
    /// `multiple` is the consumer refusing every delivery up to `tag`, which
    /// only `basic.nack` can say.
    Reject { tag: Tag, multiple: bool },
    /// A consumer saying how many deliveries it will hold unacknowledged at
    /// once. Zero is AMQP's no limit.
    Prefetch { count: u16 },
    /// A consumer giving back everything on a channel it has not finished with,
    /// by stopping or by asking for it all again. What comes back comes back
    /// with a new tag.
    HandedBack,
    /// A channel ending. The broker takes back what was outstanding on it, and
    /// a channel opened on the number afterwards is a new one that counts from
    /// the beginning.
    ChannelClosed,
    /// Connection and channel management.
    Housekeeping,
}

impl Operation {
    /// What the fleet is doing, or `None` for a frame that starts nothing: a
    /// header or body belonging to a method frame that came before it.
    pub(super) fn of(frame: &AMQPFrame) -> Option<Self> {
        let AMQPFrame::Method(_, class) = frame else {
            return None;
        };
        let method = match class {
            AMQPClass::Basic(method) => method,
            // Both halves of the closing handshake, since each direction sees
            // only one of them and both have to let go of the channel.
            AMQPClass::Channel(channel::AMQPMethod::Close(_) | channel::AMQPMethod::CloseOk(_)) => {
                return Some(Operation::ChannelClosed);
            }
            _ => return Some(Operation::Housekeeping),
        };
        Some(match method {
            AMQPMethod::Publish(_) => Operation::Publish,
            AMQPMethod::Deliver(deliver) => Operation::Deliver {
                tag: Tag::from(deliver.delivery_tag),
                redelivered: deliver.redelivered,
            },
            AMQPMethod::GetOk(get) => Operation::Deliver {
                tag: Tag::from(get.delivery_tag),
                redelivered: get.redelivered,
            },
            AMQPMethod::Ack(ack) => Operation::Ack {
                tag: Tag::from(ack.delivery_tag),
                multiple: ack.multiple,
            },
            AMQPMethod::Reject(reject) => Operation::Reject {
                tag: Tag::from(reject.delivery_tag),
                // `basic.reject` refuses one delivery and has no say in it.
                multiple: false,
            },
            AMQPMethod::Nack(nack) => Operation::Reject {
                tag: Tag::from(nack.delivery_tag),
                multiple: nack.multiple,
            },
            AMQPMethod::Qos(qos) => Operation::Prefetch {
                count: qos.prefetch_count,
            },
            // A consumer stopping, which either side can start and which the
            // broker announces on its own when a queue goes away, or one asking
            // for everything it holds to be sent again.
            AMQPMethod::Cancel(_)
            | AMQPMethod::CancelOk(_)
            | AMQPMethod::Recover(_)
            | AMQPMethod::RecoverAsync(_)
            | AMQPMethod::RecoverOk(_) => Operation::HandedBack,
            _ => Operation::Housekeeping,
        })
    }

    /// Whether this carries a message body, and so spans more than its method
    /// frame.
    pub(super) fn has_body(self) -> bool {
        matches!(self, Operation::Publish | Operation::Deliver { .. })
    }
}

/// One operation, and every byte of it, so holding it back or letting it go is
/// a matter of copying `at` or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub operation: Operation,
    pub channel: Channel,
    pub at: Range<usize>,
    /// What this is, for an operation that carries a message.
    pub(super) identity: Option<Identity>,
}

/// What names a message across deliveries of it.
///
/// A delivery tag counts what the broker has sent on a channel, so the same
/// message sent twice is two tags. What the fleet called it holds across both;
/// failing that, what it carried does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Identity {
    /// What the fleet called it, in a message id or a correlation id.
    Named(String),
    /// What it carried, for a fleet that named nothing. Two messages of the
    /// same shape are one identity, which is as far as a body can tell.
    Carried(u64),
}

impl Identity {
    /// What the frames of a message from `at` say it is.
    pub(super) fn of(frames: &[Framed], at: usize) -> Option<Self> {
        let AMQPFrame::Header(_, header) = &frames.get(at + 1)?.frame else {
            return None;
        };
        let named = header
            .properties
            .message_id()
            .as_ref()
            .or(header.properties.correlation_id().as_ref());
        if let Some(named) = named {
            return Some(Identity::Named(named.to_string()));
        }
        let mut carried = std::hash::DefaultHasher::new();
        for framed in &frames[at + 2..] {
            let AMQPFrame::Body(_, payload) = &framed.frame else {
                break;
            };
            std::hash::Hash::hash(payload, &mut carried);
        }
        Some(Identity::Carried(std::hash::Hasher::finish(&carried)))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::message::tests::{TAG, fetched, operations, pushed};

    use super::*;

    #[rstest]
    fn a_delivery_says_whether_the_broker_has_sent_it_before(
        #[values(pushed, fetched)] delivery: fn(bool, &[u8]) -> Vec<u8>,
        #[values(true, false)] redelivered: bool,
    ) {
        assert_eq!(
            operations(&delivery(redelivered, b"an order")),
            [Operation::Deliver {
                tag: TAG,
                redelivered
            }]
        );
    }
}
