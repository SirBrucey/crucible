use std::{borrow::Cow, collections::BTreeMap, ops::Range};

use amq_protocol::{
    frame::AMQPFrame,
    protocol::{
        AMQPClass,
        basic::{AMQPMethod, Nack},
    },
};
use crucible_protocol::{Carried, Did, Direction, Placement};

use super::consuming::Consuming;
use super::frame::{Framed, Wire, decode, read, write};
use super::naming::{
    PART_GONE, WOULD_STALL, boundaries, confirm, drop_confirm, redelivery, reorderable,
};
use super::operation::{Channel, Message, Operation, Tag};
use super::publishing::Publishing;

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
    /// The moment a schedule named, watched for as the run goes.
    watching: Option<String>,
    /// What both directions of this connection agree on.
    consuming: Consuming,
    /// The publishes this connection has carried.
    publishing: Publishing,
    /// The last delivery this carried on each channel, so the one before it can
    /// be offered as somewhere to be told things out of order. The flag is
    /// whether the broker had room to send behind that delivery.
    last: BTreeMap<Channel, (Tag, bool)>,
    /// A message kept back, waiting for the one the broker sent next to go
    /// first.
    held: Option<Vec<u8>>,
    /// Which way this reader's traffic runs, which every placement it finds is
    /// on.
    direction: Direction,
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
        // The frame a drop takes off the wire.
        let mut losing: Option<Range<usize>> = None;
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
            let answers = self.answers(&message);
            for placement in confirm(&message, self.direction, &answers) {
                if placing && self.watches(&placement) {
                    self.watching = None;
                    let (at, said) = drop_confirm(&wire, &message);
                    losing = at;
                    did = Some(said);
                }
                found.push(placement);
            }
            if let Operation::Deliver { tag, .. } = message.operation {
                let stalls = self.consuming.would_stall(message.channel);
                // The room is read as the offered delivery arrived, rather
                // than as the one that followed it did.
                if let Some((before, stalled)) = self.last.insert(message.channel, (tag, stalls))
                    && !stalled
                {
                    found.push(reorderable(before, self.direction));
                }
                if placing && self.watches(&reorderable(tag, self.direction)) {
                    // Watching on would take the next delivery to answer to
                    // this name, and on another channel that is a different
                    // message.
                    self.watching = None;
                    if stalls {
                        did = Some(Did::Unplaceable(WOULD_STALL.to_owned()));
                    } else if !wire.iter().any(|frame| frame.at.start == message.at.start) {
                        did = Some(Did::Unplaceable(PART_GONE.to_owned()));
                    } else {
                        keep = Some(message.at.clone());
                    }
                } else if self.held.is_some() || keep.is_some() {
                    // The one the broker sent next, which goes first. What is
                    // being held may have been held in this read rather than an
                    // earlier one, since the broker can send both in one go.
                    release = true;
                }
            }
            if message.operation == Operation::ChannelClosed {
                // The next channel on this number starts again, so what went
                // before would name a different message.
                self.last.remove(&message.channel);
            }
            // After the fault, which reads what the consumer is holding before
            // this lets go of it.
            if let Some(answered) = self.consuming.track(&message, self.direction) {
                did = Some(answered);
            }
            for (side, placement) in boundaries(message.operation, self.direction, &answers) {
                if self.watches(&placement) {
                    freeze_after = Some(side.holds(&wire, &message));
                }
                found.push(placement);
            }
        }

        let mut forward: Vec<Cow<'a, [u8]>> = Vec::with_capacity(wire.len());
        let mut kept = Vec::new();
        for frame in wire {
            if losing.as_ref() == Some(&frame.at) {
                continue;
            }
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
            unreadable: None,
        }
    }

    /// Count the operations this names from the scenario rather than from the
    /// connection.
    ///
    /// Only the counts this keeps itself. Delivery tags are the broker's own
    /// sequence, and the protocol state carries on untouched.
    fn scenario_started(&mut self) {
        self.publishing.scenario_started();
    }
}

/// Where `message` offers to have the fleet do the same thing twice.
///
impl Reader {
    /// Reads a fault-free run, so we can say where a fault should go.
    #[must_use]
    pub fn new(direction: Direction, consuming: Consuming, publishing: Publishing) -> Self {
        Self {
            direction,
            consuming,
            publishing,
            last: BTreeMap::new(),
            held: None,
            pending: Vec::new(),
            frames: Vec::new(),
            taken: 0,
            watching: None,
        }
    }

    /// Which publishes a message is, or answers, counting from one.
    ///
    /// One for a publish, however many it answers for a confirm, none for
    /// anything else.
    fn answers(&self, message: &Message) -> Vec<usize> {
        match message.operation {
            Operation::Publish if self.direction == Direction::ClientToUpstream => {
                vec![self.publishing.sent(message.channel)]
            }
            Operation::Ack { tag, multiple } if self.direction == Direction::UpstreamToClient => {
                self.publishing.answered(message.channel, tag, multiple)
            }
            _ => Vec::new(),
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
        let Operation::Ack { tag, .. } = message.operation else {
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
            u16::from(message.channel),
            AMQPClass::Basic(AMQPMethod::Nack(Nack {
                delivery_tag: u64::from(tag),
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
    pub fn watching(
        direction: Direction,
        consuming: Consuming,
        publishing: Publishing,
        mark: String,
    ) -> Self {
        Self {
            watching: Some(mark),
            ..Self::new(direction, consuming, publishing)
        }
    }

    /// Take the next `bytes` off the wire and return every whole frame in them
    /// along with the operations those completed, in the order they were sent.
    ///
    /// A frame that has not finished arriving is held back until the rest
    /// turns up. Extents count from the first byte this reader ever saw.
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
    use crucible_protocol::Kind as _;

    use crate::message::tests::{
        TAG, WAY, ack, closed, connection, consumer, delivered, first_confirm, freeze, marks,
        offered_a_reorder, operations, publish, pushed, qos,
    };

    use super::*;

    /// Holding back what is left would put the next message between a content
    /// header and its body, which no consumer can read.
    #[test]
    fn a_delivery_that_began_in_an_earlier_read_is_not_held_back() {
        let mut broker = Reader::watching(
            Direction::UpstreamToClient,
            Consuming::default(),
            Publishing::default(),
            "reorder:1".to_owned(),
        );
        let whole = delivered(1, b"an order");
        let body = decode(&whole)[2].at.start;

        let front = broker.carry(&whole[..body], true).forward.concat();
        let rest = broker.carry(&whole[body..], true);

        assert!(
            matches!(rest.did, Some(Did::Unplaceable(_))),
            "{:?}",
            rest.did
        );
        assert_eq!([front, rest.forward.concat()].concat(), whole, "left alone");
    }

    /// The one held back goes behind the one that followed it, in the same
    /// read, as it would if they arrived separately.
    #[test]
    fn two_deliveries_in_one_read_reorder_against_each_other() {
        let mut broker = Reader::watching(
            Direction::UpstreamToClient,
            Consuming::default(),
            Publishing::default(),
            "reorder:1".to_owned(),
        );
        let mut bytes = delivered(1, b"an order");
        bytes.extend(delivered(2, b"another order"));
        let carried = broker.carry(&bytes, true);

        assert!(
            matches!(carried.did, Some(Did::Placed(_))),
            "{:?}",
            carried.did
        );
        let forwarded: Vec<u8> = carried.forward.concat();
        assert_eq!(tags(&forwarded), [Tag::new(2), Tag::new(1)]);
    }

    /// The delivery tags in `bytes`, in the order they go on the wire.
    fn tags(bytes: &[u8]) -> Vec<Tag> {
        operations(bytes)
            .into_iter()
            .filter_map(|operation| match operation {
                Operation::Deliver { tag, .. } => Some(tag),
                _ => None,
            })
            .collect()
    }

    /// A publisher's connection to the broker: what it sends, and the confirms
    /// coming back, watching for `mark` on the way in.
    fn publisher(mark: &str) -> Reader {
        // A confirm answers a publish, so there has to have been one.
        let publishing = Publishing::default();
        Reader::new(
            Direction::ClientToUpstream,
            Consuming::default(),
            publishing.clone(),
        )
        .carry(&publish(b"an order"), false);
        Reader::watching(
            Direction::UpstreamToClient,
            Consuming::default(),
            publishing,
            mark.to_owned(),
        )
    }

    /// Nothing is asked of the broker, so the frame not going is the whole of
    /// the fault.
    #[test]
    fn dropping_a_confirm_takes_it_off_the_wire() {
        let mut broker = publisher("confirm:1");
        let confirm = first_confirm();
        let carried = broker.carry(&confirm, true);
        assert!(
            carried.forward.concat().is_empty(),
            "the confirm never goes"
        );
        assert!(
            matches!(carried.did, Some(Did::Placed(_))),
            "{:?}",
            carried.did
        );
    }

    /// One moment is dropped, not every confirm on the connection.
    #[test]
    fn a_confirm_the_schedule_did_not_name_still_goes() {
        let mut broker = publisher("confirm:999");
        let bytes = ack();
        assert_eq!(broker.carry(&bytes, true).forward.concat(), bytes);
    }

    /// The schedule names one moment. Once it is placed there is nothing left
    /// to watch for, so the next confirm back goes as it was sent.
    #[test]
    fn only_the_first_confirm_of_the_moment_is_dropped() {
        let mut broker = publisher("confirm:1");
        let first = ack();
        broker.carry(&first, true);
        let bytes = ack();
        let again = broker.carry(&bytes, true);
        assert_eq!(again.forward.concat(), bytes);
        assert_eq!(again.did, None);
    }

    /// A confirm rides in with other traffic. Only it is taken.
    #[test]
    fn dropping_a_confirm_leaves_the_rest_of_the_read_alone() {
        let mut broker = publisher("confirm:1");
        let delivery = pushed(false, b"an order");
        let bytes = [first_confirm(), delivery.clone()].concat();
        let carried = broker.carry(&bytes, true);
        assert_eq!(carried.forward.concat(), delivery);
    }

    /// The fault happens in the byte stream as it passes, so there is nothing
    /// to hold the fleet for.
    #[test]
    fn dropping_a_confirm_holds_nothing_back() {
        let mut broker = publisher("confirm:1");
        let confirm = ack();
        assert_eq!(broker.carry(&confirm, true).freeze_after, None);
    }

    /// A delivery is only somewhere to reorder once another follows it: with
    /// nothing behind it to go first, holding it back takes the message away.
    #[test]
    fn a_delivery_offers_a_reorder_once_another_follows_it() {
        let mut reader = Reader::new(
            Direction::UpstreamToClient,
            Consuming::default(),
            Publishing::default(),
        );
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
            Publishing::default(),
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
            [Tag::new(2), Tag::new(1)],
            "the one sent first arrives second"
        );
        assert!(
            matches!(released.did, Some(Did::Placed(_))),
            "{:?}",
            released.did
        );
    }

    /// The next channel on that number counts from the beginning, so a
    /// delivery either side of a close is two messages sharing a tag.
    #[test]
    fn a_delivery_from_a_channel_that_closed_is_nothing_to_reorder_behind() {
        let (mut broker, _consumer) = connection();
        broker.carry(&delivered(1, b"an order"), false);
        broker.carry(&closed(), false);

        assert!(
            !offered_a_reorder(&mut broker, &delivered(1, b"an order")),
            "nothing has followed this one yet"
        );
    }

    /// Saying so beats holding a message back and waiting for one that is not
    /// coming.
    #[test]
    fn a_reorder_that_would_stall_the_broker_says_it_cannot_be_placed() {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        Reader::new(
            Direction::ClientToUpstream,
            consuming.clone(),
            publishing.clone(),
        )
        .carry(&qos(1), false);
        let mut broker = Reader::watching(
            Direction::UpstreamToClient,
            consuming,
            publishing.clone(),
            "reorder:1".into(),
        );

        let delivery = delivered(1, b"an order");
        let carried = broker.carry(&delivery, true);
        assert!(
            matches!(carried.did, Some(Did::Unplaceable(_))),
            "{:?}",
            carried.did
        );
        assert_eq!(
            tags(&carried.forward.concat()),
            [Tag::new(1)],
            "the delivery goes on untouched"
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
            [Operation::Ack {
                tag: TAG,
                multiple: false
            }],
            "left alone"
        );
    }

    #[test]
    fn a_mark_this_run_never_reaches_holds_nothing() {
        assert_eq!(freeze(&ack(), "ack:999:after"), None);
    }

    /// The offset is into the read that completed the message, rather than
    /// into everything the reader has seen.
    #[test]
    fn a_message_split_across_reads_is_held_on_the_read_that_finishes_it() {
        let bytes = publish(b"an order");
        let split = bytes.len() - 4;
        let mut reader = Reader::watching(
            WAY,
            Consuming::default(),
            Publishing::default(),
            "publish:1:after".to_owned(),
        );
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
            Reader::new(WAY, Consuming::default(), Publishing::default())
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
        let mut reader = Reader::new(WAY, Consuming::default(), Publishing::default());
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
        let mut reader = Reader::watching(
            WAY,
            Consuming::default(),
            Publishing::default(),
            "publish:1:before".to_owned(),
        );
        assert_eq!(reader.carry(&bytes[..split], true).freeze_after, None);
        assert_eq!(reader.carry(&bytes[split..], true).freeze_after, Some(0));
    }
}
