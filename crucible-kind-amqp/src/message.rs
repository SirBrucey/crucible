//! AMQP traffic read as the operations a fleet performs.
//!
//! Publishing is not one frame: a method frame saying so, a content header
//! giving the body's size, then body frames until that many bytes have gone. A
//! fault placed at the second publish has to mean the whole of it, which is what
//! this recovers from the frames the spec's own parser hands over.

use std::{borrow::Cow, collections::BTreeMap, ops::Range};

use amq_protocol::{
    frame::{AMQPFrame, parsing::parse_frame},
    protocol::{AMQPClass, basic::AMQPMethod},
};
use crucible_protocol::{Carried, Direction, Placement, Property};

/// What a fleet was doing, in terms a fault is placed against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// A message going to the broker.
    Publish,
    /// A message handed to a consumer, whether pushed or fetched.
    Deliver {
        /// What the broker labelled it, which names it on this channel.
        tag: u64,
        /// Whether this message has been delivered before.
        redelivered: bool,
    },
    /// A consumer saying it is done with one.
    Ack { tag: u64 },
    /// A consumer refusing one, which is what makes the broker requeue it.
    Reject { tag: u64 },
    /// Connection and channel management.
    Housekeeping,
}

impl Operation {
    /// What the fleet is doing, or `None` for a frame that starts nothing: a
    /// header or body belonging to a method frame that came before it.
    fn of(frame: &AMQPFrame) -> Option<Self> {
        let AMQPFrame::Method(_, class) = frame else {
            return None;
        };
        let AMQPClass::Basic(method) = class else {
            return Some(Operation::Housekeeping);
        };
        Some(match method {
            AMQPMethod::Publish(_) => Operation::Publish,
            AMQPMethod::Deliver(deliver) => Operation::Deliver {
                tag: deliver.delivery_tag,
                redelivered: deliver.redelivered,
            },
            AMQPMethod::GetOk(get) => Operation::Deliver {
                tag: get.delivery_tag,
                redelivered: get.redelivered,
            },
            AMQPMethod::Ack(ack) => Operation::Ack {
                tag: ack.delivery_tag,
            },
            AMQPMethod::Reject(reject) => Operation::Reject {
                tag: reject.delivery_tag,
            },
            AMQPMethod::Nack(nack) => Operation::Reject {
                tag: nack.delivery_tag,
            },
            _ => Operation::Housekeeping,
        })
    }

    /// Whether this carries a message body, and so spans more than its method
    /// frame.
    fn has_body(self) -> bool {
        matches!(self, Operation::Publish | Operation::Deliver { .. })
    }
}

/// One operation, and every byte of it, so holding it back or letting it go is
/// a matter of copying `at` or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub operation: Operation,
    pub channel: u16,
    pub at: Range<usize>,
}

/// One frame, and where it sat in the stream it was parsed from.
struct Framed {
    frame: AMQPFrame,
    at: Range<usize>,
}

/// Every whole frame at the front of `bytes`.
///
/// One still arriving is left where it is: the parser says so rather than
/// guessing, so a frame split across reads is never half read.
fn decode(bytes: &[u8]) -> Vec<Framed> {
    let mut frames = Vec::new();
    let mut at = 0;
    while let Ok((rest, frame)) = parse_frame(&bytes[at..]) {
        let end = bytes.len() - rest.len();
        frames.push(Framed { frame, at: at..end });
        at = end;
    }
    frames
}

/// Read `frames` as the operations they make up, and say how many frames those
/// account for.
///
/// An operation still arriving is left out along with the frames it has so far:
/// half a publish is not a publish, and holding one back before its body is
/// here would leave the broker waiting rather than the fleet losing a message.
fn read(frames: &[Framed]) -> (Vec<Message>, usize) {
    let mut messages = Vec::new();
    let mut taken = 0;
    while let Some(framed) = frames.get(taken) {
        let Some(operation) = Operation::of(&framed.frame) else {
            taken += 1;
            continue;
        };
        let last = if operation.has_body() {
            match body_ends(frames, taken) {
                Some(last) => last,
                None => return (messages, taken),
            }
        } else {
            taken
        };
        messages.push(Message {
            operation,
            channel: framed.frame.channel_id(),
            at: framed.at.start..frames[last].at.end,
        });
        taken = last + 1;
    }
    (messages, taken)
}

/// The last frame of the message starting at `at`, or `None` while its body is
/// still arriving. A message whose body is empty has no body frames at all.
fn body_ends(frames: &[Framed], at: usize) -> Option<usize> {
    let AMQPFrame::Header(_, header) = &frames.get(at + 1)?.frame else {
        // Not the content header this expects, so nothing says where the
        // message ends. Its method frame is all we can claim.
        return Some(at);
    };
    let wanted = header.body_size;
    let mut last = at + 1;
    let mut carried = 0;
    while carried < wanted {
        let AMQPFrame::Body(_, payload) = &frames.get(last + 1)?.frame else {
            return Some(last);
        };
        carried += payload.len() as u64;
        last += 1;
    }
    Some(last)
}

/// Reads one direction of one connection, across as many reads as it takes.
///
/// A frame can arrive split over several reads and a message over several
/// frames, so what is not yet whole is held here until the rest of it turns up.
#[derive(Debug)]
pub struct Reader {
    /// Bytes belonging to an operation that has not finished arriving.
    pending: Vec<u8>,
    /// Operations of each kind this has seen, which is what a mark counts.
    seen: Seen,
    /// The moment a schedule named, watched for as the run goes.
    watching: Option<String>,
    /// Which way this reader's traffic runs, which every placement it finds is
    /// on.
    direction: Direction,
}

/// Which side of an operation a fault goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Before,
    After,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Before => f.write_str("before"),
            Side::After => f.write_str("after"),
        }
    }
}

/// How many of each operation have crossed, so a placement can name the second
/// publish rather than the fifth packet.
#[derive(Debug, Default)]
struct Seen(BTreeMap<&'static str, u32>);

impl Seen {
    /// Count one of `what` and name it by where it fell in the run.
    fn nth(&mut self, what: &'static str) -> String {
        let seen = self.0.entry(what).or_default();
        *seen += 1;
        format!("{what}:{seen}")
    }

    /// Count `operation` and place a fault either side of it.
    fn count(&mut self, operation: Operation, direction: Direction) -> Vec<(Side, Placement)> {
        let (name, before, after) = match operation {
            Operation::Publish => (
                self.nth("publish"),
                "a publish the sender has committed to and the broker has not seen",
                "a publish the broker has taken but not confirmed",
            ),
            Operation::Deliver { tag, .. } => (
                format!("deliver:{tag}"),
                "a delivery the broker has released and the consumer has not seen",
                "a delivery the consumer has but has not acknowledged",
            ),
            Operation::Ack { tag } => (
                format!("ack:{tag}"),
                "an ack the consumer has sent and the broker has not seen",
                "an ack the broker has taken, releasing its copy",
            ),
            Operation::Reject { tag } => (
                format!("reject:{tag}"),
                "a refusal the consumer has sent and the broker has not seen",
                "a refusal the broker has taken, requeueing or dead-lettering it",
            ),
            Operation::Housekeeping => return Vec::new(),
        };
        [(Side::Before, before), (Side::After, after)]
            .into_iter()
            .map(|(side, why)| {
                let placement = Placement {
                    direction,
                    mark: format!("{name}:{side}"),
                    why: why.to_owned(),
                    exercises: Property::Durable,
                };
                (side, placement)
            })
            .collect()
    }
}

impl crucible_protocol::Kind for Reader {
    fn carry<'a>(&mut self, bytes: &'a [u8]) -> Carried<'a> {
        // What was already held, so an offset into the buffer can be given back
        // as one into what the caller just handed over.
        let held = self.pending.len();
        let mut freeze_after = None;
        let mut found = Vec::new();
        for message in self.read(bytes) {
            for (side, placement) in self.seen.count(message.operation, self.direction) {
                if self.watching.as_deref() == Some(placement.mark.as_str()) {
                    let at = match side {
                        Side::Before => message.at.start,
                        Side::After => message.at.end,
                    };
                    freeze_after = Some(at.saturating_sub(held));
                }
                found.push(placement);
            }
        }
        Carried {
            forward: Cow::Borrowed(bytes),
            freeze_after,
            found,
            did: None,
        }
    }
}

impl Reader {
    /// Reads a fault-free run, so we can say where a fault should go.
    #[must_use]
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
            pending: Vec::new(),
            seen: Seen::default(),
            watching: None,
        }
    }

    /// Reads a faulted run, holding the fleet when it sees `mark`.
    #[must_use]
    pub fn watching(direction: Direction, mark: String) -> Self {
        Self {
            watching: Some(mark),
            ..Self::new(direction)
        }
    }

    /// Take the next `bytes` off the wire and return the operations they
    /// completed, in the order they were sent.
    ///
    /// Each one's `at` indexes what this reader had buffered, which is its own
    /// bytes and not the caller's.
    pub fn read(&mut self, bytes: &[u8]) -> Vec<Message> {
        self.pending.extend_from_slice(bytes);
        let frames = decode(&self.pending);
        let (messages, whole) = read(&frames);
        // Frames belonging to an operation still arriving stay pending, along
        // with the bytes behind them.
        let consumed = match whole {
            0 => 0,
            whole => frames[whole - 1].at.end,
        };
        self.pending.drain(..consumed);
        messages
    }
}

#[cfg(test)]
mod tests {
    use amq_protocol::{
        frame::{AMQPContentHeader, WriteContext, gen_frame},
        protocol::basic::{AMQPMethod, AMQPProperties, Ack, Deliver, GetOk, Publish},
    };
    use crucible_protocol::Kind as _;
    use rstest::rstest;

    use super::*;

    const CHANNEL: u16 = 1;
    /// Which way the traffic these read runs.
    const WAY: Direction = Direction::ClientToUpstream;
    const TAG: u64 = 7;

    /// A frame as it goes on the wire.
    fn wire(frame: &AMQPFrame) -> Vec<u8> {
        let write = gen_frame::<Vec<u8>>(frame);
        let (bytes, _) = write(WriteContext::from(Vec::new()))
            .expect("a frame serialises")
            .into_inner();
        bytes
    }

    /// A method frame in the class a fault is placed against.
    fn method(method: AMQPMethod) -> Vec<u8> {
        wire(&AMQPFrame::Method(CHANNEL, AMQPClass::Basic(method)))
    }

    /// The content header that follows a message, stating its body size.
    fn header(size: u64) -> Vec<u8> {
        wire(&AMQPFrame::Header(
            CHANNEL,
            AMQPContentHeader {
                class_id: 60,
                body_size: size,
                properties: AMQPProperties::default(),
            },
        ))
    }

    fn body(payload: &[u8]) -> Vec<u8> {
        wire(&AMQPFrame::Body(CHANNEL, payload.to_vec()))
    }

    /// A publish carrying `payload`.
    fn publish(payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::Publish(Publish::default()));
        bytes.extend(header(payload.len() as u64));
        if !payload.is_empty() {
            bytes.extend(body(payload));
        }
        bytes
    }

    fn ack() -> Vec<u8> {
        method(AMQPMethod::Ack(Ack {
            delivery_tag: TAG,
            multiple: false,
        }))
    }

    /// A delivery the broker pushed. This names its consumer.
    fn pushed(redelivered: bool) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::Deliver(Deliver {
            consumer_tag: "consumer-1".into(),
            delivery_tag: TAG,
            redelivered,
            ..Default::default()
        }));
        bytes.extend(header(0));
        bytes
    }

    /// A delivery a consumer fetched.
    fn fetched(redelivered: bool) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::GetOk(GetOk {
            delivery_tag: TAG,
            redelivered,
            ..Default::default()
        }));
        bytes.extend(header(0));
        bytes
    }

    /// What `bytes` say the fleet did.
    fn ops(bytes: &[u8]) -> (Vec<Message>, usize) {
        read(&decode(bytes))
    }

    /// The operations `bytes` carry.
    fn operations(bytes: &[u8]) -> Vec<Operation> {
        ops(bytes)
            .0
            .iter()
            .map(|message| message.operation)
            .collect()
    }

    /// Placements a fault-free run of `bytes` offers.
    fn placements(bytes: &[u8]) -> Vec<Placement> {
        Reader::new(WAY).carry(bytes).found
    }

    /// Where a run watching `mark` holds the fleet.
    fn freeze(bytes: &[u8], mark: &str) -> Option<usize> {
        Reader::watching(WAY, mark.to_owned())
            .carry(bytes)
            .freeze_after
    }

    /// A fault before an operation and one after it leave different sides
    /// holding the message, so each is its own placement.
    #[test]
    fn a_fault_can_go_either_side_of_an_operation() {
        let marks: Vec<String> = placements(&ack())
            .into_iter()
            .map(|placement| placement.mark)
            .collect();
        assert_eq!(
            marks,
            [format!("ack:{TAG}:before"), format!("ack:{TAG}:after")]
        );
    }

    #[test]
    fn the_fleet_is_held_where_the_watched_mark_falls() {
        let bytes = ack();
        assert_eq!(freeze(&bytes, &format!("ack:{TAG}:before")), Some(0));
        assert_eq!(
            freeze(&bytes, &format!("ack:{TAG}:after")),
            Some(bytes.len())
        );
    }

    #[test]
    fn a_mark_this_run_never_reaches_holds_nothing() {
        assert_eq!(freeze(&ack(), "ack:999:after"), None);
    }

    /// The delivery tag names the message, so a fault placed here is placed on
    /// what the broker labelled rather than on however far into the run it fell.
    #[test]
    fn a_placement_on_a_delivery_names_the_message() {
        let marks: Vec<String> = placements(&pushed(false))
            .into_iter()
            .map(|placement| placement.mark)
            .collect();
        assert_eq!(
            marks,
            [
                format!("deliver:{TAG}:before"),
                format!("deliver:{TAG}:after")
            ]
        );
    }

    #[rstest]
    fn a_delivery_says_whether_the_broker_has_sent_it_before(
        #[values(pushed, fetched)] delivery: fn(bool) -> Vec<u8>,
        #[values(true, false)] redelivered: bool,
    ) {
        assert_eq!(
            operations(&delivery(redelivered)),
            [Operation::Deliver {
                tag: TAG,
                redelivered
            }]
        );
    }

    #[test]
    fn a_publish_is_its_method_header_and_body() {
        let bytes = publish(b"an order");
        let (messages, taken) = ops(&bytes);
        assert_eq!(taken, 3);
        assert_eq!(messages[0].operation, Operation::Publish);
        assert_eq!(messages[0].at, 0..bytes.len(), "the whole of it");
    }
}
