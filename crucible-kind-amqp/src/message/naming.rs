use std::ops::Range;

use crucible_protocol::{Did, Direction, Doing, Placement, Primitive};

use super::frame::Wire;
use super::operation::{Message, Operation, Tag};

/// Which side of an operation a fault goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Side {
    Before,
    After,
}

impl Side {
    /// How many of `wire` go out before the fleet is held on `message`.
    ///
    /// A message that began in an earlier read has already had those frames go,
    /// so the most this side can still hold back is everything in this one.
    pub(super) fn holds(self, wire: &[Wire<'_>], message: &Message) -> usize {
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

/// Name `operation` and place a fault either side of it.
///
/// `answers` is which publishes this is, counting from one.
pub(super) fn boundaries(
    operation: Operation,
    direction: Direction,
    answers: &[usize],
) -> Vec<(Side, Placement)> {
    let (name, before, after) = match operation {
        Operation::Publish => (
            match answers {
                [at] => format!("publish:{at}"),
                // A publish the scenario did not cause names nothing.
                _ => return Vec::new(),
            },
            "a publish the sender has committed to and the broker has not seen",
            "a publish the broker has taken but not confirmed",
        ),
        Operation::Deliver { tag, .. } => (
            format!("deliver:{tag}"),
            "a delivery the broker has released and the consumer has not seen",
            "a delivery the consumer has but has not acknowledged",
        ),
        // `basic.ack` is a consumer ending a delivery on the way to the
        // broker, and a publisher confirm on the way back.
        Operation::Ack { tag, .. } if direction == Direction::ClientToUpstream => (
            format!("ack:{tag}"),
            "an ack the consumer has sent and the broker has not seen",
            "an ack the broker has taken, releasing its copy",
        ),
        // One frame can stand for several publishes, so it is offered as each
        // of them. Those are the same publishes however the broker batched.
        Operation::Ack { .. } => {
            return answers
                .iter()
                .flat_map(|at| {
                    [
                        (
                            Side::Before,
                            "a confirm the broker has sent and the publisher has not seen",
                        ),
                        (
                            Side::After,
                            "a confirm the publisher has, so it knows the publish landed",
                        ),
                    ]
                    .into_iter()
                    .map(move |(side, why)| {
                        (
                            side,
                            Placement {
                                direction,
                                mark: format!("confirmed:{at}:{side}"),
                                why: why.to_owned(),
                                doing: Doing::Holding,
                            },
                        )
                    })
                })
                .collect();
        }
        Operation::Reject { tag } => (
            format!("reject:{tag}"),
            "a refusal the consumer has sent and the broker has not seen",
            "a refusal the broker has taken, requeueing or dead-lettering it",
        ),
        // Setting a limit, giving deliveries back, and minding the
        // connection are the fleet arranging itself, not work a fault has
        // anything to catch either side of.
        Operation::Prefetch { .. }
        | Operation::HandedBack
        | Operation::ChannelClosed
        | Operation::Housekeeping => return Vec::new(),
    };
    [(Side::Before, before), (Side::After, after)]
        .into_iter()
        .map(|(side, why)| {
            let placement = Placement {
                direction,
                mark: format!("{name}:{side}"),
                why: why.to_owned(),
                doing: Doing::Holding,
            };
            (side, placement)
        })
        .collect()
}

/// A consumer's ack is the broker letting go of its copy. Refusing it instead,
/// and asking for the message back, makes the broker send it again, so what
/// arrives is a redelivery the fleet would have to be ready for.
///
/// Only a consumer's ack ends a delivery there is anything to ask for again.
pub(super) fn redelivery(message: &Message, direction: Direction) -> Option<Placement> {
    let Operation::Ack { tag, .. } = message.operation else {
        return None;
    };
    if direction != Direction::ClientToUpstream {
        return None;
    }
    Some(Placement {
        direction,
        mark: format!("redeliver:{tag}"),
        why: "a message the consumer finished with, delivered to it again".to_owned(),
        doing: Doing::Rewriting(Primitive::Redeliver),
    })
}

/// Where `message` offers to leave a publisher never told whether its message
/// landed.
///
/// Named by the publishes it answers rather than by how many frames the broker
/// chose to reply with. One frame answering several is offered as each of them,
/// since dropping it leaves every one unconfirmed.
pub(super) fn confirm(
    message: &Message,
    direction: Direction,
    answers: &[usize],
) -> Vec<Placement> {
    let Operation::Ack { .. } = message.operation else {
        return Vec::new();
    };
    if direction != Direction::UpstreamToClient {
        return Vec::new();
    }
    let why = match answers {
        [] => return Vec::new(),
        [one] => format!(
            "the publish the broker took and never told the publisher about, which is publish {one}"
        ),
        [first, .., last] => format!(
            "a publish the broker took and the publisher was never told about, in a confirm answering publishes {first} to {last}"
        ),
    };
    answers
        .iter()
        .map(|at| Placement {
            direction,
            mark: format!("confirm:{at}"),
            why: why.clone(),
            doing: Doing::Rewriting(Primitive::Drop),
        })
        .collect()
}

/// Why a delivery whose first frames have already crossed cannot be held back.
///
/// Holding back only what is left of one would put the next message's frames
/// between a header and its body, which no consumer can read.
pub(super) const PART_GONE: &str = "the delivery began in an earlier read, so the front of it had already \
                         reached the consumer";

/// Why a delivery that fills the room the broker has left has nowhere to
/// reorder.
pub(super) const WOULD_STALL: &str = "the broker keeps at most as many deliveries unacknowledged as the \
                           consumer asked for, this one fills that room, and the consumer has none \
                           in hand to acknowledge, so nothing would be sent for this to go behind";

/// Where a message the broker has already sent offers to arrive after one sent
/// later, which is what a fleet relying on the order it was told things would
/// not survive.
///
/// Only offered once another delivery follows it, since holding one back with
/// nothing behind it would take the message away instead, and only where the
/// broker would have something left to send while this one is held.
pub(super) fn reorderable(followed: Tag, direction: Direction) -> Placement {
    Placement {
        direction,
        mark: format!("reorder:{followed}"),
        why: "a message held back until after the one the broker sent next".to_owned(),
        doing: Doing::Rewriting(Primitive::Reorder),
    }
}

/// Take the confirm `message` carries off the wire, and say what came of it.
///
/// Nothing has to be asked of the fleet: the frame not going on is the whole of
/// the fault, so it is placed the moment the frame is found.
pub(super) fn drop_confirm(wire: &[Wire<'_>], message: &Message) -> (Option<Range<usize>>, Did) {
    match wire.iter().find(|frame| frame.at == message.at) {
        Some(frame) => (
            Some(frame.at.clone()),
            Did::Placed(
                "the broker's confirmation of a publish never reached the publisher".to_owned(),
            ),
        ),
        None => (
            None,
            Did::Unplaceable("the confirm had already gone".to_owned()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::Kind as _;

    use crate::message::consuming::Consuming;
    use crate::message::publishing::Publishing;
    use crate::message::reader::Reader;
    use crate::message::tests::{
        TAG, WAY, ack, acked, confirming, fetched, first_confirm, freeze, marks, publish, pushed,
        qos,
    };

    use super::*;

    /// Placements a fault-free run of `bytes` offers.
    fn placements(bytes: &[u8]) -> Vec<Placement> {
        Reader::new(WAY, Consuming::default(), Publishing::default())
            .carry(bytes, false)
            .found
    }

    /// Every ack is a message the consumer has finished with, so every ack is
    /// somewhere the fleet could be asked to do the same work twice.
    #[test]
    fn an_ack_offers_to_have_the_message_delivered_again() {
        let offered: Vec<(String, Doing)> = placements(&ack())
            .into_iter()
            .map(|placement| (placement.mark, placement.doing))
            .collect();
        assert!(
            offered.contains(&(
                format!("redeliver:{TAG}"),
                Doing::Rewriting(Primitive::Redeliver)
            )),
            "{offered:?}"
        );
    }

    /// `basic.ack` from the broker confirms a publish rather than ending a
    /// delivery, so there is nothing there to ask for again.
    #[test]
    fn a_publisher_confirm_offers_no_redelivery() {
        let marks: Vec<String> = Reader::new(
            Direction::UpstreamToClient,
            Consuming::default(),
            Publishing::default(),
        )
        .carry(&ack(), false)
        .found
        .into_iter()
        .map(|placement| placement.mark)
        .collect();
        assert!(
            !marks.iter().any(|mark| mark.starts_with("redeliver:")),
            "{marks:?}"
        );
    }

    /// Every confirm is a publish the broker has taken, so every confirm is
    /// somewhere the publisher could be left never knowing whether it landed.
    #[test]
    fn a_confirm_offers_to_leave_the_publisher_in_doubt() {
        let publishing = Publishing::default();
        Reader::new(
            Direction::ClientToUpstream,
            Consuming::default(),
            publishing.clone(),
        )
        .carry(&publish(b"an order"), false);
        let offered: Vec<(String, Doing)> = Reader::new(
            Direction::UpstreamToClient,
            Consuming::default(),
            publishing,
        )
        .carry(&first_confirm(), false)
        .found
        .into_iter()
        .map(|placement| (placement.mark, placement.doing))
        .collect();
        assert!(
            offered.contains(&("confirm:1".to_owned(), Doing::Rewriting(Primitive::Drop))),
            "{offered:?}"
        );
    }

    /// A report that names one publish describes a smaller fault than the
    /// fleet met.
    #[test]
    fn a_confirm_says_which_publishes_it_answers() {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        let mut client = Reader::new(
            Direction::ClientToUpstream,
            consuming.clone(),
            publishing.clone(),
        );
        let mut broker = Reader::new(Direction::UpstreamToClient, consuming, publishing.clone());
        for i in 0..3 {
            client.carry(&publish(format!("m{i}").as_bytes()), false);
        }

        let found = broker.carry(&confirming(3, true), false).found;
        let why = found
            .iter()
            .find(|placement| placement.mark == "confirm:1")
            .map(|placement| placement.why.clone())
            .expect("the frame answers the first publish");
        assert!(why.contains("publishes 1 to 3"), "{why}");
    }

    /// A consumer's ack ends the delivery the broker gave it that tag for, so
    /// the tag identifies the message and the mark keeps it.
    #[test]
    fn a_consumers_ack_is_named_by_the_delivery_it_ends() {
        let marks = marks(placements(&acked(7)));
        assert!(marks.contains(&"ack:7:before".to_owned()), "{marks:?}");
    }

    /// A consumer's ack going the other way ends a delivery. There is no
    /// publisher on that side waiting to be told anything.
    #[test]
    fn a_consumers_ack_offers_no_confirm_to_drop() {
        let marks = marks(placements(&ack()));
        assert!(
            !marks.iter().any(|mark| mark.starts_with("confirm:")),
            "{marks:?}"
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

    /// Saying how many to send is the consumer getting ready, not work in
    /// flight for a fault to catch either side of.
    #[test]
    fn a_prefetch_offers_nowhere_to_fault() {
        assert!(marks(placements(&qos(1))).is_empty());
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
}
