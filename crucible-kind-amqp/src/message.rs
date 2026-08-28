//! AMQP traffic read as the operations a fleet performs.
//!
//! Publishing is not one frame: a method frame saying so, a content header
//! giving the body's size, then body frames until that many bytes have gone. A
//! fault placed at the second publish has to mean the whole of it, which is what
//! this recovers from the frames the spec's own parser hands over.

use std::{borrow::Cow, collections::BTreeMap, ops::Range};

use amq_protocol::{
    frame::{AMQPFrame, WriteContext, gen_frame, parsing::parse_frame},
    protocol::{
        AMQPClass,
        basic::{AMQPMethod, Nack},
    },
};
use crucible_protocol::{Carried, Did, Direction, Placement, Property};

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
    /// What this is, for an operation that carries a message.
    identity: Option<Identity>,
}

/// What names a message across deliveries of it.
///
/// A delivery tag counts what the broker has sent on a channel, so the same
/// message sent twice is two tags. What the fleet called it holds across both;
/// failing that, what it carried does.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Identity {
    /// What the fleet called it, in a message id or a correlation id.
    Named(String),
    /// What it carried, for a fleet that named nothing. Two messages of the
    /// same shape are one identity, which is as far as a body can tell.
    Carried(u64),
}

impl Identity {
    /// What the frames of a message from `at` say it is.
    fn of(frames: &[Framed], at: usize) -> Option<Self> {
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

/// One frame, and where it sat in the stream it was parsed from.
#[derive(Debug)]
struct Framed {
    frame: AMQPFrame,
    at: Range<usize>,
}

/// One whole frame on its way back to the wire, and where it sat in the stream.
///
/// Its bytes are the ones that arrived, so nothing the parser does not
/// understand can be lost by writing it out again. Only a frame put back
/// together across reads, or one a fault replaced, is owned.
struct Wire<'a> {
    at: Range<usize>,
    bytes: Cow<'a, [u8]>,
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
            identity: operation
                .has_body()
                .then(|| Identity::of(frames, taken))
                .flatten(),
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

/// What the two directions of a connection have to agree on.
///
/// A delivery and the ack that ends it cross opposite ways, so neither reader
/// sees both. What a redelivery can be recognised by came with the delivery,
/// and what asks for one is the ack.
#[derive(Clone, Default, Debug)]
pub struct Consuming(std::sync::Arc<std::sync::Mutex<Deliveries>>);

#[derive(Default, Debug)]
struct Deliveries {
    /// What the consumer holds and has not finished with, by the tag naming
    /// each delivery of it. Bounded by the prefetch the channel negotiated,
    /// since that is how many the broker will let it hold at once.
    outstanding: BTreeMap<(u16, u64), Identity>,
    /// The message a fault asked the broker to send again.
    asked: Option<Identity>,
}

impl Consuming {
    /// The broker has handed `identity` to the consumer as `tag`.
    fn delivered(&self, channel: u16, tag: u64, identity: Option<Identity>) {
        if let Some(identity) = identity {
            self.held().outstanding.insert((channel, tag), identity);
        }
    }

    /// What the consumer is holding as `tag`.
    fn holding(&self, channel: u16, tag: u64) -> Option<Identity> {
        self.held().outstanding.get(&(channel, tag)).cloned()
    }

    /// The consumer is done with `tag`, one way or another.
    fn finished(&self, channel: u16, tag: u64) {
        self.held().outstanding.remove(&(channel, tag));
    }

    /// Ask for `identity` to be sent again.
    fn ask(&self, identity: Identity) {
        self.held().asked = Some(identity);
    }

    /// Whether `identity` is what was asked for, forgetting it if it is: one
    /// fault asks once, so the next redelivery is the fleet's own doing.
    fn answers(&self, identity: Option<&Identity>) -> bool {
        let mut held = self.held();
        if held.asked.is_none() || held.asked.as_ref() != identity {
            return false;
        }
        held.asked = None;
        true
    }

    /// Follow what the consumer is holding, and say when the message a fault
    /// asked for comes round again.
    fn track(&self, message: &Message) -> Option<Did> {
        match message.operation {
            Operation::Deliver { tag, redelivered } => {
                self.delivered(message.channel, tag, message.identity.clone());
                (redelivered && self.answers(message.identity.as_ref()))
                    .then(|| Did::Placed("the broker delivered the message again".to_owned()))
            }
            Operation::Ack { tag } | Operation::Reject { tag } => {
                self.finished(message.channel, tag);
                None
            }
            Operation::Publish | Operation::Housekeeping => None,
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Deliveries> {
        // A poisoned lock means a reader panicked mid-update, so what the two
        // directions agree on is already unreliable.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Reads one direction of one connection, across as many reads as it takes.
///
/// A frame can arrive split over several reads and a message over several
/// frames, so what is not yet whole is held here until the rest of it turns up.
#[derive(Debug)]
pub struct Reader {
    /// Bytes of a frame that has not finished arriving.
    pending: Vec<u8>,
    /// Whole frames of an operation that has not finished arriving.
    frames: Vec<Framed>,
    /// Bytes parsed into `frames`, which is what makes every extent count from
    /// the same place however the stream was broken up.
    taken: usize,
    /// Operations of each kind this has seen, which is what a mark counts.
    seen: Seen,
    /// The moment a schedule named, watched for as the run goes.
    watching: Option<String>,
    /// What both directions of this connection agree on.
    consuming: Consuming,
    /// The last delivery this carried on each channel, so the one before can be
    /// offered as somewhere the fleet could be told things out of order.
    last: BTreeMap<u16, u64>,
    /// A message kept back, waiting for the one the broker sent next to go
    /// first.
    held: Option<Vec<u8>>,
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

impl Side {
    /// How many of `wire` go out before the fleet is held on `message`.
    ///
    /// A message that began in an earlier read has already had those frames go,
    /// so the most this side can still hold back is everything in this one.
    fn holds(self, wire: &[Wire<'_>], message: &Message) -> usize {
        let boundary = match self {
            Side::Before => message.at.start,
            Side::After => message.at.end,
        };
        wire.iter()
            .position(|frame| frame.at.end > boundary)
            .unwrap_or(wire.len())
    }
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
    fn carry<'a>(&mut self, bytes: &'a [u8], placing: bool) -> Carried<'a> {
        let (mut wire, messages) = self.read(bytes);
        let mut freeze_after = None;
        let mut found = Vec::new();
        let mut did = None;
        // What a reorder keeps back, and what lets it go.
        let mut keep: Option<Range<usize>> = None;
        let mut release = false;
        for message in messages {
            if let Some(placement) = redelivery(&message, self.direction) {
                if placing && self.watches(&placement) {
                    // The schedule names one moment, so once this is done there
                    // is nothing left to watch for.
                    self.watching = None;
                    did = Some(self.refuse(&mut wire, &message));
                }
                found.push(placement);
            }
            if let Operation::Deliver { tag, .. } = message.operation {
                // A delivery is offered once another follows it, so what the
                // schedule names is known to have something to go behind.
                if let Some(before) = self.last.insert(message.channel, tag) {
                    found.push(reorderable(before, self.direction));
                }
                if placing && self.watches(&reorderable(tag, self.direction)) {
                    self.watching = None;
                    keep = Some(message.at.clone());
                } else if self.held.is_some() {
                    // The one the broker sent next, which goes first.
                    release = true;
                }
            }
            // After the fault, which reads what the consumer is holding before
            // this lets go of it.
            if let Some(answered) = self.consuming.track(&message) {
                did = Some(answered);
            }
            for (side, placement) in self.seen.count(message.operation, self.direction) {
                if self.watches(&placement) {
                    freeze_after = Some(side.holds(&wire, &message));
                }
                found.push(placement);
            }
        }

        let mut forward: Vec<Cow<'a, [u8]>> = Vec::with_capacity(wire.len());
        let mut kept = Vec::new();
        for frame in wire {
            match &keep {
                Some(at) if at.contains(&frame.at.start) => kept.extend_from_slice(&frame.bytes),
                _ => forward.push(frame.bytes),
            }
        }
        if !kept.is_empty() {
            self.held = Some(kept);
            did = Some(Did::Asked);
        }
        if release && let Some(kept) = self.held.take() {
            forward.push(Cow::Owned(kept));
            did = Some(Did::Placed(
                "a message the broker sent first arrived after the one it sent next".to_owned(),
            ));
        }

        Carried {
            forward,
            freeze_after,
            found,
            did,
        }
    }
}

/// Where `message` offers to have the fleet do the same thing twice.
///
/// A consumer's ack is the broker letting go of its copy. Refusing it instead,
/// asking for the message back, is how the broker itself is made to send it
/// again, so what arrives is a redelivery the fleet would have to be ready for
/// rather than a copy this made up.
fn redelivery(message: &Message, direction: Direction) -> Option<Placement> {
    let Operation::Ack { tag } = message.operation else {
        return None;
    };
    Some(Placement {
        direction,
        mark: format!("redeliver:{tag}"),
        why: "a message the consumer finished with, delivered to it again".to_owned(),
        exercises: Property::Idempotent,
    })
}

/// Where a message the broker has already sent offers to arrive after one sent
/// later, which is what a fleet relying on the order it was told things would
/// not survive.
///
/// A delivery is only offered once another follows it: holding one back with
/// nothing behind it to go first would take the message away rather than
/// reorder it, and that is a different fault.
fn reorderable(followed: u64, direction: Direction) -> Placement {
    Placement {
        direction,
        mark: format!("reorder:{followed}"),
        why: "a message held back until after the one the broker sent next".to_owned(),
        exercises: Property::Converges,
    }
}

/// `frame` as it goes on the wire.
fn write(frame: &AMQPFrame) -> Option<Vec<u8>> {
    let write = gen_frame::<Vec<u8>>(frame);
    let (bytes, _) = write(WriteContext::from(Vec::new())).ok()?.into_inner();
    Some(bytes)
}

impl Reader {
    /// Reads a fault-free run, so we can say where a fault should go.
    #[must_use]
    pub fn new(direction: Direction, consuming: Consuming) -> Self {
        Self {
            direction,
            consuming,
            last: BTreeMap::new(),
            held: None,
            pending: Vec::new(),
            frames: Vec::new(),
            taken: 0,
            seen: Seen::default(),
            watching: None,
        }
    }

    /// Whether `placement` is the moment a schedule named.
    fn watches(&self, placement: &Placement) -> bool {
        self.watching.as_deref() == Some(placement.mark.as_str())
    }

    /// Refuse `message` in place of the ack that would have let the broker drop
    /// it, and ask for the message it ends to be sent again.
    ///
    /// Nothing is placed yet, the broker may dead-letter the message instead of
    /// requeueing it.
    fn refuse(&self, wire: &mut [Wire<'_>], message: &Message) -> Did {
        let Operation::Ack { tag } = message.operation else {
            return Did::Unplaceable("not an ack, so there is nothing to refuse".to_owned());
        };
        let Some(identity) = self.consuming.holding(message.channel, tag) else {
            return Did::Unplaceable(
                "nothing said what the consumer was finishing with, so a redelivery of it could \
                 not be told from any other"
                    .to_owned(),
            );
        };
        let Some(ack) = wire.iter_mut().find(|frame| frame.at == message.at) else {
            return Did::Unplaceable("the ack had already gone".to_owned());
        };
        let nack = AMQPFrame::Method(
            message.channel,
            AMQPClass::Basic(AMQPMethod::Nack(Nack {
                delivery_tag: tag,
                multiple: false,
                requeue: true,
            })),
        );
        let Some(refusal) = write(&nack) else {
            return Did::Unplaceable("a refusal could not be written".to_owned());
        };
        ack.bytes = Cow::Owned(refusal);
        self.consuming.ask(identity);
        Did::Asked
    }

    /// Reads a faulted run, holding the fleet when it sees `mark`.
    #[must_use]
    pub fn watching(direction: Direction, consuming: Consuming, mark: String) -> Self {
        Self {
            watching: Some(mark),
            ..Self::new(direction, consuming)
        }
    }

    /// Take the next `bytes` off the wire and return every whole frame in them
    /// along with the operations those completed, in the order they were sent.
    ///
    /// A frame that has not finished arriving is held back until the rest of it
    /// turns up, so what comes out is always something the protocol recognises.
    /// Extents count from the first byte this reader ever saw, so a message
    /// that arrived over several reads still says where it began.
    fn read<'a>(&mut self, bytes: &'a [u8]) -> (Vec<Wire<'a>>, Vec<Message>) {
        // What was already held, so a frame that arrived whole can be given
        // back as a slice of what the caller just handed over.
        let held = self.pending.len();
        self.pending.extend_from_slice(bytes);
        // A frame is parsed once. Only the tail of one still arriving is ever
        // looked at again, so a long message costs what it is long.
        let decoded = decode(&self.pending);
        let consumed = decoded.last().map_or(0, |framed| framed.at.end);

        let wire = decoded
            .iter()
            .map(|framed| Wire {
                at: self.taken + framed.at.start..self.taken + framed.at.end,
                bytes: match framed.at.start.checked_sub(held) {
                    Some(start) => Cow::Borrowed(&bytes[start..framed.at.end - held]),
                    None => Cow::Owned(self.pending[framed.at.clone()].to_vec()),
                },
            })
            .collect();

        self.frames.extend(decoded.into_iter().map(|framed| Framed {
            frame: framed.frame,
            at: self.taken + framed.at.start..self.taken + framed.at.end,
        }));
        self.pending.drain(..consumed);
        self.taken += consumed;

        // Frames belonging to an operation still arriving are left for the rest
        // of it to turn up.
        let (messages, whole) = read(&self.frames);
        self.frames.drain(..whole);
        (wire, messages)
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
    fn pushed(redelivered: bool, payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::Deliver(Deliver {
            consumer_tag: "consumer-1".into(),
            delivery_tag: TAG,
            redelivered,
            ..Default::default()
        }));
        bytes.extend(header(payload.len() as u64));
        bytes.extend(body(payload));
        bytes
    }

    /// A delivery a consumer fetched.
    fn fetched(redelivered: bool, payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::GetOk(GetOk {
            delivery_tag: TAG,
            redelivered,
            ..Default::default()
        }));
        bytes.extend(header(payload.len() as u64));
        bytes.extend(body(payload));
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

    /// A delivery of `payload` the broker labelled `tag`.
    fn delivered(tag: u64, payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::Deliver(Deliver {
            consumer_tag: "consumer-1".into(),
            delivery_tag: tag,
            redelivered: false,
            ..Default::default()
        }));
        bytes.extend(header(payload.len() as u64));
        bytes.extend(body(payload));
        bytes
    }

    /// The marks a run offered.
    fn marks(found: Vec<Placement>) -> Vec<String> {
        found.into_iter().map(|placement| placement.mark).collect()
    }

    /// The delivery tags in `bytes`, in the order they go on the wire.
    fn tags(bytes: &[u8]) -> Vec<u64> {
        operations(bytes)
            .into_iter()
            .filter_map(|operation| match operation {
                Operation::Deliver { tag, .. } => Some(tag),
                _ => None,
            })
            .collect()
    }

    /// Placements a fault-free run of `bytes` offers.
    fn placements(bytes: &[u8]) -> Vec<Placement> {
        Reader::new(WAY, Consuming::default())
            .carry(bytes, false)
            .found
    }

    /// Where a run watching `mark` holds the fleet.
    fn freeze(bytes: &[u8], mark: &str) -> Option<usize> {
        Reader::watching(WAY, Consuming::default(), mark.to_owned())
            .carry(bytes, true)
            .freeze_after
    }

    /// A consumer's connection to the broker: what it is sent, and what it
    /// sends back, watching for `mark` on the way out.
    fn consumer(mark: &str) -> (Reader, Reader) {
        let consuming = Consuming::default();
        (
            Reader::new(Direction::UpstreamToClient, consuming.clone()),
            Reader::watching(WAY, consuming, mark.to_owned()),
        )
    }

    /// Every ack is a message the consumer has finished with, so every ack is
    /// somewhere the fleet could be asked to do the same work twice.
    #[test]
    fn an_ack_offers_to_have_the_message_delivered_again() {
        let offered: Vec<(String, Property)> = placements(&ack())
            .into_iter()
            .map(|placement| (placement.mark, placement.exercises))
            .collect();
        assert!(
            offered.contains(&(format!("redeliver:{TAG}"), Property::Idempotent)),
            "{offered:?}"
        );
    }

    /// Nothing else is: a publish has not been delivered to anyone, and a
    /// delivery has not been finished with.
    #[test]
    fn nothing_else_offers_a_redelivery() {
        for bytes in [
            publish(b"an order"),
            pushed(false, b"an order"),
            fetched(false, b"an order"),
        ] {
            let marks: Vec<String> = placements(&bytes)
                .into_iter()
                .map(|placement| placement.mark)
                .collect();
            assert!(
                !marks.iter().any(|mark| mark.starts_with("redeliver:")),
                "{marks:?}"
            );
        }
    }

    #[test]
    fn a_redelivery_refuses_the_ack_and_asks_for_the_message_back() {
        let (mut from_broker, mut to_broker) = consumer(&format!("redeliver:{TAG}"));
        from_broker.carry(&pushed(false, b"an order"), false);
        let forward = to_broker.carry(&ack(), true).forward.concat();

        assert_eq!(operations(&forward), [Operation::Reject { tag: TAG }]);
        let AMQPFrame::Method(_, AMQPClass::Basic(AMQPMethod::Nack(nack))) =
            &decode(&forward)[0].frame
        else {
            panic!("a refusal is a nack: {:?}", decode(&forward)[0].frame);
        };
        assert!(
            nack.requeue,
            "asking for it back is what makes it come again"
        );
    }

    #[test]
    fn a_redelivery_is_placed_only_when_the_message_comes_round_again() {
        let (mut from_broker, mut to_broker) = consumer(&format!("redeliver:{TAG}"));
        from_broker.carry(&pushed(false, b"an order"), false);
        assert_eq!(to_broker.carry(&ack(), true).did, Some(Did::Asked));

        let redelivered = pushed(true, b"an order");
        let again = from_broker.carry(&redelivered, false);
        assert!(matches!(again.did, Some(Did::Placed(_))), "{:?}", again.did);
    }

    #[test]
    fn another_message_coming_round_again_places_nothing() {
        let (mut from_broker, mut to_broker) = consumer(&format!("redeliver:{TAG}"));
        from_broker.carry(&pushed(false, b"an order"), false);
        to_broker.carry(&ack(), true);

        let unrelated = pushed(true, b"a different order");
        assert_eq!(from_broker.carry(&unrelated, false).did, None);
    }

    /// A delivery is only somewhere to reorder once another follows it: with
    /// nothing behind it to go first, holding it back takes the message away.
    #[test]
    fn a_delivery_offers_a_reorder_once_another_follows_it() {
        let mut reader = Reader::new(Direction::UpstreamToClient, Consuming::default());
        let first = delivered(1, b"an order");
        assert!(
            marks(reader.carry(&first, false).found)
                .iter()
                .all(|mark| !mark.starts_with("reorder:")),
            "nothing has followed it yet"
        );
        let second = delivered(2, b"another order");
        assert!(marks(reader.carry(&second, false).found).contains(&"reorder:1".to_owned()));
    }

    #[test]
    fn a_reorder_holds_a_message_back_until_the_next_one_has_gone() {
        let mut reader = Reader::watching(
            Direction::UpstreamToClient,
            Consuming::default(),
            "reorder:1".into(),
        );

        let first = delivered(1, b"an order");
        let held = reader.carry(&first, true);
        assert!(held.forward.is_empty(), "the marked message is kept back");
        assert_eq!(held.did, Some(Did::Asked));

        let second = delivered(2, b"another order");
        let released = reader.carry(&second, true);
        assert_eq!(
            tags(&released.forward.concat()),
            [2, 1],
            "the one sent first arrives second"
        );
        assert!(
            matches!(released.did, Some(Did::Placed(_))),
            "{:?}",
            released.did
        );
    }

    #[test]
    fn an_ack_for_a_delivery_nothing_saw_is_not_refused() {
        let (_, mut to_broker) = consumer(&format!("redeliver:{TAG}"));
        let bytes = ack();
        let carried = to_broker.carry(&bytes, true);
        assert!(
            matches!(carried.did, Some(Did::Unplaceable(_))),
            "{:?}",
            carried.did
        );
        assert_eq!(
            operations(&carried.forward.concat()),
            [Operation::Ack { tag: TAG }],
            "left alone"
        );
    }

    /// A fault before an operation and one after it leave different sides
    /// holding the message, so each is its own placement.
    #[test]
    fn a_fault_can_go_either_side_of_an_operation() {
        let marks: Vec<String> = placements(&ack())
            .into_iter()
            .map(|placement| placement.mark)
            .collect();
        for side in ["before", "after"] {
            assert!(marks.contains(&format!("ack:{TAG}:{side}")), "{marks:?}");
        }
    }

    /// An ack is one frame, so a fault before it lets nothing of it go and a
    /// fault after it lets the whole of it go.
    #[test]
    fn the_fleet_is_held_where_the_watched_mark_falls() {
        let bytes = ack();
        assert_eq!(freeze(&bytes, &format!("ack:{TAG}:before")), Some(0));
        assert_eq!(freeze(&bytes, &format!("ack:{TAG}:after")), Some(1));
    }

    #[test]
    fn a_mark_this_run_never_reaches_holds_nothing() {
        assert_eq!(freeze(&ack(), "ack:999:after"), None);
    }

    /// The delivery tag names the message, so a fault placed here is placed on
    /// what the broker labelled rather than on however far into the run it fell.
    #[test]
    fn a_placement_on_a_delivery_names_the_message() {
        let marks: Vec<String> = placements(&pushed(false, b"an order"))
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

    #[test]
    fn a_publish_is_its_method_header_and_body() {
        let bytes = publish(b"an order");
        let (messages, taken) = ops(&bytes);
        assert_eq!(taken, 3);
        assert_eq!(messages[0].operation, Operation::Publish);
        assert_eq!(messages[0].at, 0..bytes.len(), "the whole of it");
    }

    /// A message arrives over as many reads as the kernel gives it. The one
    /// that completes it is where the fleet is held, and the offset is into
    /// that read rather than into everything the reader has seen.
    #[test]
    fn a_message_split_across_reads_is_held_on_the_read_that_finishes_it() {
        let bytes = publish(b"an order");
        let split = bytes.len() - 4;
        let mut reader = Reader::watching(WAY, Consuming::default(), "publish:1:after".to_owned());
        assert_eq!(
            reader.carry(&bytes[..split], true).freeze_after,
            None,
            "still arriving"
        );
        assert_eq!(
            reader.carry(&bytes[split..], true).freeze_after,
            Some(1),
            "the frame that finished it"
        );
    }

    /// What comes back is what arrived, so nothing the parser does not
    /// understand can be lost by writing it out again.
    #[test]
    fn what_is_carried_is_what_arrived() {
        let bytes = [publish(b"an order"), ack()].concat();
        assert_eq!(
            Reader::new(WAY, Consuming::default())
                .carry(&bytes, false)
                .forward
                .concat(),
            bytes
        );
    }

    /// A read can end mid-frame. Half a frame is not something the fleet can
    /// act on, so it waits for the rest and goes out whole.
    #[test]
    fn a_frame_split_across_reads_goes_out_whole() {
        let bytes = ack();
        let split = bytes.len() - 4;
        let mut reader = Reader::new(WAY, Consuming::default());
        assert!(
            reader.carry(&bytes[..split], false).forward.is_empty(),
            "none of it is whole yet"
        );
        assert_eq!(reader.carry(&bytes[split..], false).forward.concat(), bytes);
    }

    /// A fault that goes before an operation which began in an earlier read
    /// holds everything in this one, since what it was to precede has gone.
    #[test]
    fn a_message_split_before_the_mark_holds_the_whole_read() {
        let bytes = publish(b"an order");
        let split = bytes.len() - 4;
        let mut reader = Reader::watching(WAY, Consuming::default(), "publish:1:before".to_owned());
        assert_eq!(reader.carry(&bytes[..split], true).freeze_after, None);
        assert_eq!(reader.carry(&bytes[split..], true).freeze_after, Some(0));
    }
}
