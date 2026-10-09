use std::{borrow::Cow, ops::Range};

use amq_protocol::frame::{AMQPFrame, WriteContext, gen_frame, parsing::parse_frame};

use super::operation::{Channel, Identity, Message, Operation};

/// One frame, and where it sat in the stream it was parsed from.
#[derive(Debug)]
pub(super) struct Framed {
    pub(super) frame: AMQPFrame,
    pub(super) at: Range<usize>,
}

/// One whole frame on its way back to the wire, and where it sat in the stream.
///
/// Its bytes are the ones that arrived, so nothing the parser does not
/// understand can be lost by writing it out again. Only a frame put back
/// together across reads, or one a fault replaced, is owned.
pub(super) struct Wire<'a> {
    pub(super) at: Range<usize>,
    pub(super) bytes: Cow<'a, [u8]>,
}

/// Every whole frame at the front of `bytes`.
///
/// One still arriving is left where it is: the parser says so rather than
/// guessing, so a frame split across reads is never half read.
pub(super) fn decode(bytes: &[u8]) -> Vec<Framed> {
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
/// An operation still arriving is left out along with the frames it has so
/// far, since holding one back before its body is here would stall the broker.
pub(super) fn read(frames: &[Framed]) -> (Vec<Message>, usize) {
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
            channel: Channel::from(framed.frame.channel_id()),
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
pub(super) fn body_ends(frames: &[Framed], at: usize) -> Option<usize> {
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

/// `frame` as it goes on the wire.
pub(super) fn write(frame: &AMQPFrame) -> Option<Vec<u8>> {
    let write = gen_frame::<Vec<u8>>(frame);
    let (bytes, _) = write(WriteContext::from(Vec::new())).ok()?.into_inner();
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use crate::message::tests::{ops, publish};

    use super::*;

    #[test]
    fn a_publish_is_its_method_header_and_body() {
        let bytes = publish(b"an order");
        let (messages, taken) = ops(&bytes);
        assert_eq!(taken, 3);
        assert_eq!(messages[0].operation, Operation::Publish);
        assert_eq!(messages[0].at, 0..bytes.len(), "the whole of it");
    }
}
