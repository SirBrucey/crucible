use std::collections::BTreeMap;

use crucible_protocol::{Did, Direction};

use super::operation::{Channel, Identity, Message, Operation, Tag};

/// What the two directions of a connection have to agree on.
///
/// A delivery and the ack that ends it cross opposite ways, so neither reader
/// sees both. What a redelivery can be recognised by came with the delivery,
/// and what asks for one is the ack.
///
/// One belongs to one connection. Channel numbers and delivery tags start again
/// on the next connection.
#[derive(Clone, Default, Debug)]
pub struct Consuming(std::sync::Arc<std::sync::Mutex<Deliveries>>);

#[derive(Default, Debug)]
struct Deliveries {
    /// What the consumer holds and has not finished with, by the tag naming
    /// each delivery of it, and what names it across deliveries.
    outstanding: BTreeMap<(Channel, Tag), Option<Identity>>,
    /// The message a fault asked the broker to send again.
    asked: Option<Identity>,
    /// How many deliveries each channel said it would hold at once.
    prefetch: BTreeMap<Channel, u16>,
}

impl Consuming {
    /// The broker has handed `identity` to the consumer as `tag`.
    ///
    /// A delivery nothing names still fills a place the broker is counting, so
    /// it is held here either way.
    pub(super) fn delivered(&self, channel: Channel, tag: Tag, identity: Option<Identity>) {
        self.held().outstanding.insert((channel, tag), identity);
    }

    /// What names the delivery the consumer is holding as `tag`.
    pub(super) fn holding(&self, channel: Channel, tag: Tag) -> Option<Identity> {
        self.held()
            .outstanding
            .get(&(channel, tag))
            .cloned()
            .flatten()
    }

    /// The consumer is done with `tag`, one way or another.
    pub(super) fn finished(&self, channel: Channel, tag: Tag) {
        self.held().outstanding.remove(&(channel, tag));
    }

    /// The broker takes back everything on `channel` the consumer never
    /// acknowledged.
    ///
    /// A delivery tag does not say which consumer held it, so the whole channel
    /// is emptied.
    pub(super) fn handed_back(&self, channel: Channel) {
        self.held().outstanding.retain(|(on, _), _| *on != channel);
    }

    /// The channel has ended, so nothing counted against it still holds. A
    /// channel opened on the number afterwards asks for its own limit.
    pub(super) fn closed(&self, channel: Channel) {
        let mut held = self.held();
        held.outstanding.retain(|(on, _), _| *on != channel);
        held.prefetch.remove(&channel);
    }

    /// The consumer will hold at most `count` deliveries at once on `channel`.
    pub(super) fn holds_at_most(&self, channel: Channel, count: u16) {
        self.held().prefetch.insert(channel, count);
    }

    /// Whether holding back the delivery now arriving on `channel` would leave
    /// the broker with nothing to send behind it.
    ///
    /// A held delivery fills a place and is never acknowledged, so only the
    /// consumer acknowledging one it already has in hand can free the broker.
    pub(super) fn would_stall(&self, channel: Channel) -> bool {
        let held = self.held();
        // Only a limit of one leaves the broker nothing to send behind a held
        // delivery. A larger limit leaves room, and a channel that said nothing
        // or asked for zero has AMQP's no limit at all.
        if held.prefetch.get(&channel) != Some(&1) {
            return false;
        }
        held.outstanding
            .range((channel, Tag::new(u64::MIN))..=(channel, Tag::new(u64::MAX)))
            .next()
            .is_none()
    }

    /// Ask for `identity` to be sent again.
    pub(super) fn ask(&self, identity: Identity) {
        self.held().asked = Some(identity);
    }

    /// Whether `identity` is what was asked for, forgetting it if it is: one
    /// fault asks once, so the next redelivery is the fleet's own doing.
    pub(super) fn answers(&self, identity: Option<&Identity>) -> bool {
        let mut held = self.held();
        if held.asked.is_none() || held.asked.as_ref() != identity {
            return false;
        }
        held.asked = None;
        true
    }

    /// Follow what the consumer is holding, and say when the message a fault
    /// asked for comes round again.
    ///
    /// `direction` tells a consumer's ack from the broker's confirm, which
    /// share a frame and number their tags separately. Only a consumer can set
    /// prefetch.
    pub(super) fn track(&self, message: &Message, direction: Direction) -> Option<Did> {
        match message.operation {
            Operation::Deliver { tag, redelivered } => {
                self.delivered(message.channel, tag, message.identity.clone());
                (redelivered && self.answers(message.identity.as_ref()))
                    .then(|| Did::Placed("the broker delivered the message again".to_owned()))
            }
            Operation::Ack { tag, .. } | Operation::Reject { tag }
                if direction == Direction::ClientToUpstream =>
            {
                self.finished(message.channel, tag);
                None
            }
            Operation::Prefetch { count } if direction == Direction::ClientToUpstream => {
                self.holds_at_most(message.channel, count);
                None
            }
            // Either way round: a consumer gives its deliveries back itself,
            // and the broker says so when a queue goes out from under one.
            Operation::HandedBack => {
                self.handed_back(message.channel);
                None
            }
            Operation::ChannelClosed => {
                self.closed(message.channel);
                None
            }
            Operation::Ack { .. }
            | Operation::Reject { .. }
            | Operation::Prefetch { .. }
            | Operation::Publish
            | Operation::Housekeeping => None,
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

#[cfg(test)]
mod tests {
    use crucible_protocol::Kind as _;
    use rstest::rstest;

    use crate::message::publishing::Publishing;
    use crate::message::reader::Reader;
    use crate::message::tests::{
        TAG, ack, acked, cancel, closed, closing, connection, consumer, delivered, marks,
        offered_a_reorder, pushed, qos, recover,
    };

    use super::*;

    /// The reorders a consumer is offered over two deliveries, having asked the
    /// broker for `prefetch` at once or having asked for nothing, and having
    /// acknowledged the first delivery before the second arrived if `keeps_up`.
    fn reorders(prefetch: Option<u16>, keeps_up: bool) -> Vec<String> {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        let mut consumer = Reader::new(
            Direction::ClientToUpstream,
            consuming.clone(),
            publishing.clone(),
        );
        if let Some(count) = prefetch {
            consumer.carry(&qos(count), false);
        }
        let mut broker = Reader::new(Direction::UpstreamToClient, consuming, publishing.clone());
        broker.carry(&delivered(1, b"an order"), false);
        if keeps_up {
            consumer.carry(&acked(1), false);
        }
        marks(broker.carry(&delivered(2, b"another order"), false).found)
            .into_iter()
            .filter(|mark| mark.starts_with("reorder:"))
            .collect()
    }

    /// A confirm numbers its tags apart from a delivery's, so one arriving on a
    /// tag a delivery is using must not end that delivery.
    #[test]
    fn a_publisher_confirm_does_not_end_a_delivery() {
        let (mut from_broker, mut to_broker) = consumer(&format!("redeliver:{TAG}"));
        from_broker.carry(&pushed(false, b"an order"), false);
        from_broker.carry(&ack(), false);

        let ack = ack();
        let carried = to_broker.carry(&ack, true);
        assert_eq!(carried.did, Some(Did::Asked));
    }

    /// A place taken by a delivery the consumer has not acknowledged is still a
    /// place the held one is not in, so keeping up does not come into it.
    #[rstest]
    fn a_consumer_the_broker_can_still_send_to_is_offered_a_reorder(
        #[values(None, Some(0), Some(2), Some(3))] prefetch: Option<u16>,
        #[values(true, false)] keeps_up: bool,
    ) {
        assert_eq!(reorders(prefetch, keeps_up), ["reorder:1"]);
    }

    /// With nothing in hand to acknowledge, the broker sends nothing behind the
    /// held delivery and the fault could never be placed.
    #[rstest]
    fn a_consumer_with_one_place_is_offered_no_reorder(#[values(true, false)] keeps_up: bool) {
        assert!(reorders(Some(1), keeps_up).is_empty());
    }

    /// Acknowledging the one in hand frees the broker to send behind the held
    /// one.
    #[test]
    fn a_consumer_that_can_acknowledge_its_way_out_is_offered_a_reorder() {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        let mut consumer = Reader::new(
            Direction::ClientToUpstream,
            consuming.clone(),
            publishing.clone(),
        );
        consumer.carry(&qos(2), false);
        let mut broker = Reader::new(Direction::UpstreamToClient, consuming, publishing.clone());

        broker.carry(&delivered(1, b"an order"), false);
        // Both places taken, so the broker sends no more until one is freed.
        broker.carry(&delivered(2, b"another order"), false);
        consumer.carry(&acked(1), false);

        let third = delivered(3, b"a third order");
        assert!(
            marks(broker.carry(&third, false).found).contains(&"reorder:2".to_owned()),
            "holding the second still leaves the first to be acknowledged"
        );
    }

    /// One left behind would make a consumer with a single place look like one
    /// with room to spare.
    #[test]
    fn a_channel_opened_after_a_close_holds_none_of_the_old_one() {
        let (mut broker, mut consumer) = connection();
        consumer.carry(&qos(1), false);
        broker.carry(&delivered(1, b"an order"), false);

        // Ends with that delivery never finished with, each side seeing its own
        // half of the handshake.
        consumer.carry(&closing(), false);
        broker.carry(&closed(), false);

        consumer.carry(&qos(1), false);
        broker.carry(&delivered(1, b"an order"), false);
        assert!(
            !offered_a_reorder(&mut broker, &delivered(2, b"another order")),
            "the new channel takes one delivery at a time, like the old one"
        );
    }

    /// What a channel said it would hold goes with it, so a channel opened on
    /// the number afterwards is read on what it asks for itself.
    #[test]
    fn a_channel_opened_after_a_close_does_not_inherit_its_limit() {
        let (mut broker, mut consumer) = connection();
        consumer.carry(&qos(1), false);
        consumer.carry(&closing(), false);
        broker.carry(&closed(), false);

        broker.carry(&delivered(1, b"an order"), false);
        assert!(
            offered_a_reorder(&mut broker, &delivered(2, b"another order")),
            "this channel has said nothing about how many it will hold"
        );
    }

    /// Delivery tags carry on across both, so what the channel said it would
    /// hold still stands.
    #[rstest]
    fn a_consumer_that_gave_its_deliveries_back_is_holding_nothing(
        #[values(cancel, recover)] gives_back: fn() -> Vec<u8>,
    ) {
        let (mut broker, mut consumer) = connection();
        consumer.carry(&qos(1), false);
        broker.carry(&delivered(1, b"an order"), false);
        consumer.carry(&gives_back(), false);

        broker.carry(&delivered(2, b"another order"), false);
        assert!(
            !offered_a_reorder(&mut broker, &delivered(3, b"a third order")),
            "it still takes one delivery at a time"
        );
    }

    /// Taking the broker's answer as a limit would silence reorders on a
    /// channel that never set one.
    #[test]
    fn a_limit_the_broker_sent_back_is_not_the_consumer_asking() {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        Reader::new(
            Direction::UpstreamToClient,
            consuming.clone(),
            publishing.clone(),
        )
        .carry(&qos(1), false);
        let mut broker = Reader::new(Direction::UpstreamToClient, consuming, publishing.clone());
        broker.carry(&delivered(1, b"an order"), false);
        assert!(
            marks(broker.carry(&delivered(2, b"another order"), false).found)
                .contains(&"reorder:1".to_owned())
        );
    }
}
