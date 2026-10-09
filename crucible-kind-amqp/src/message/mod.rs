//! AMQP traffic read as the operations a fleet performs.
//!
//! Publishing is not one frame: a method frame saying so, a content header
//! giving the body's size, then body frames until that many bytes have gone. A
//! fault placed at the second publish has to mean the whole of it, which is what
//! this recovers from the frames the spec's own parser hands over.
//!
//! Each piece of that is its own module: the frames themselves, the operations
//! they carry, what a consumer and a publisher each have outstanding, how a
//! moment is named, and the reader that drives all of it.

mod consuming;
mod frame;
mod naming;
mod operation;
mod publishing;
mod reader;

pub use consuming::Consuming;
pub use operation::{Channel, Message, Operation, Tag};
pub use publishing::Publishing;
pub use reader::Reader;

/// The traffic every module's tests are built from, and the tests no one
/// module owns.
#[cfg(test)]
mod tests {
    use amq_protocol::{
        frame::AMQPFrame,
        protocol::{AMQPClass, channel},
    };
    use amq_protocol::{
        frame::{AMQPContentHeader, WriteContext, gen_frame},
        protocol::basic::{
            AMQPMethod, AMQPProperties, Ack, Cancel, Deliver, GetOk, Publish, Qos, Recover,
        },
    };
    use crucible_protocol::{Did, Direction, Kind as _, Placement};

    use super::{
        consuming::Consuming,
        frame::{decode, read},
        operation::{Channel, Message, Operation, Tag},
        publishing::Publishing,
        reader::Reader,
    };

    const CHANNEL: Channel = Channel::new(1);
    /// Which way the traffic these read runs.
    pub(super) const WAY: Direction = Direction::ClientToUpstream;
    pub(super) const TAG: Tag = Tag::new(7);

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
        wire(&AMQPFrame::Method(
            u16::from(CHANNEL),
            AMQPClass::Basic(method),
        ))
    }

    /// The content header that follows a message, stating its body size.
    fn header(size: u64) -> Vec<u8> {
        wire(&AMQPFrame::Header(
            u16::from(CHANNEL),
            AMQPContentHeader {
                class_id: 60,
                body_size: size,
                properties: AMQPProperties::default(),
            },
        ))
    }

    fn body(payload: &[u8]) -> Vec<u8> {
        wire(&AMQPFrame::Body(u16::from(CHANNEL), payload.to_vec()))
    }

    /// A publish on `channel`, for a publisher using more than one.
    pub(super) fn publish_on(channel: Channel, payload: &[u8]) -> Vec<u8> {
        let mut bytes = wire(&AMQPFrame::Method(
            u16::from(channel),
            AMQPClass::Basic(AMQPMethod::Publish(Publish::default())),
        ));
        bytes.extend(wire(&AMQPFrame::Header(
            u16::from(channel),
            AMQPContentHeader {
                class_id: 60,
                body_size: payload.len() as u64,
                properties: AMQPProperties::default(),
            },
        )));
        bytes.extend(wire(&AMQPFrame::Body(u16::from(channel), payload.to_vec())));
        bytes
    }

    /// A broker confirming up to `tag`, on `channel`.
    pub(super) fn confirming_on(channel: Channel, tag: Tag, multiple: bool) -> Vec<u8> {
        wire(&AMQPFrame::Method(
            u16::from(channel),
            AMQPClass::Basic(AMQPMethod::Ack(Ack {
                delivery_tag: u64::from(tag),
                multiple,
            })),
        ))
    }

    /// A broker confirming up to `tag` on the channel the tests publish on.
    pub(super) fn confirming(tag: u64, multiple: bool) -> Vec<u8> {
        confirming_on(CHANNEL, Tag::new(tag), multiple)
    }

    /// A publish carrying `payload`.
    pub(super) fn publish(payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::Publish(Publish::default()));
        bytes.extend(header(payload.len() as u64));
        if !payload.is_empty() {
            bytes.extend(body(payload));
        }
        bytes
    }

    pub(super) fn ack() -> Vec<u8> {
        acked(u64::from(TAG))
    }

    /// The broker confirming the first publish on the channel.
    pub(super) fn first_confirm() -> Vec<u8> {
        acked(1)
    }

    /// A consumer finishing with the delivery the broker labelled `tag`.
    pub(super) fn acked(tag: u64) -> Vec<u8> {
        method(AMQPMethod::Ack(Ack {
            delivery_tag: tag,
            multiple: false,
        }))
    }

    /// A delivery the broker pushed. This names its consumer.
    pub(super) fn pushed(redelivered: bool, payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::Deliver(Deliver {
            consumer_tag: "consumer-1".into(),
            delivery_tag: u64::from(TAG),
            redelivered,
            ..Default::default()
        }));
        bytes.extend(header(payload.len() as u64));
        bytes.extend(body(payload));
        bytes
    }

    /// A delivery a consumer fetched.
    pub(super) fn fetched(redelivered: bool, payload: &[u8]) -> Vec<u8> {
        let mut bytes = method(AMQPMethod::GetOk(GetOk {
            delivery_tag: u64::from(TAG),
            redelivered,
            ..Default::default()
        }));
        bytes.extend(header(payload.len() as u64));
        bytes.extend(body(payload));
        bytes
    }

    /// What `bytes` say the fleet did.
    pub(super) fn ops(bytes: &[u8]) -> (Vec<Message>, usize) {
        read(&decode(bytes))
    }

    /// The operations `bytes` carry.
    pub(super) fn operations(bytes: &[u8]) -> Vec<Operation> {
        ops(bytes)
            .0
            .iter()
            .map(|message| message.operation)
            .collect()
    }

    /// A delivery of `payload` the broker labelled `tag`.
    pub(super) fn delivered(tag: u64, payload: &[u8]) -> Vec<u8> {
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

    /// One side asking to end the channel.
    pub(super) fn closing() -> Vec<u8> {
        wire(&AMQPFrame::Method(
            u16::from(CHANNEL),
            AMQPClass::Channel(channel::AMQPMethod::Close(channel::Close::default())),
        ))
    }

    /// The other side agreeing to it.
    pub(super) fn closed() -> Vec<u8> {
        wire(&AMQPFrame::Method(
            u16::from(CHANNEL),
            AMQPClass::Channel(channel::AMQPMethod::CloseOk(channel::CloseOk {})),
        ))
    }

    /// A consumer stopping, which hands back what it never finished with.
    pub(super) fn cancel() -> Vec<u8> {
        method(AMQPMethod::Cancel(Cancel::default()))
    }

    /// A consumer asking for everything it holds to be sent again, which the
    /// broker does under new tags.
    pub(super) fn recover() -> Vec<u8> {
        method(AMQPMethod::Recover(Recover::default()))
    }

    /// A consumer's connection to the broker, watching for nothing.
    pub(super) fn connection() -> (Reader, Reader) {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        (
            Reader::new(
                Direction::UpstreamToClient,
                consuming.clone(),
                publishing.clone(),
            ),
            Reader::new(WAY, consuming, publishing.clone()),
        )
    }

    /// Whether what `reader` made of `bytes` offered somewhere to reorder.
    pub(super) fn offered_a_reorder(reader: &mut Reader, bytes: &[u8]) -> bool {
        marks(reader.carry(bytes, false).found)
            .iter()
            .any(|mark| mark.starts_with("reorder:"))
    }

    /// A consumer telling the broker how many deliveries it will hold at once.
    pub(super) fn qos(count: u16) -> Vec<u8> {
        method(AMQPMethod::Qos(Qos {
            prefetch_count: count,
            global: false,
        }))
    }

    /// The marks a run offered.
    pub(super) fn marks(found: Vec<Placement>) -> Vec<String> {
        found.into_iter().map(|placement| placement.mark).collect()
    }

    /// Where a run watching `mark` holds the fleet.
    pub(super) fn freeze(bytes: &[u8], mark: &str) -> Option<usize> {
        Reader::watching(
            WAY,
            Consuming::default(),
            Publishing::default(),
            mark.to_owned(),
        )
        .carry(bytes, true)
        .freeze_after
    }

    /// A consumer's connection to the broker: what it is sent, and what it
    /// sends back, watching for `mark` on the way out.
    pub(super) fn consumer(mark: &str) -> (Reader, Reader) {
        let consuming = Consuming::default();
        let publishing = Publishing::default();
        (
            Reader::new(
                Direction::UpstreamToClient,
                consuming.clone(),
                publishing.clone(),
            ),
            Reader::watching(WAY, consuming, publishing.clone(), mark.to_owned()),
        )
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
}
