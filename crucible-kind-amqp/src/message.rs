//! Frames read as the operations a fleet performs.
//!
//! Publishing is not one frame: a method frame saying so, a content header
//! giving the body's size, then body frames until that many bytes have gone. A
//! fault placed at the second publish has to mean the whole of it, which is what
//! this recovers from the framing.

use crate::frame::{Frame, Kind};

/// The class and methods a fault is placed against. Everything else on the wire
/// is what the fleet needs to talk at all.
mod basic {
    pub const CLASS: u16 = 60;
    pub const PUBLISH: u16 = 40;
    pub const DELIVER: u16 = 60;
    pub const GET_OK: u16 = 71;
    pub const ACK: u16 = 80;
    pub const REJECT: u16 = 90;
    pub const NACK: u16 = 120;
}

/// What a fleet was doing, in terms a fault is placed against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// A message going to the broker.
    Publish,
    /// A message handed to a consumer, whether pushed or fetched.
    Deliver,
    /// A consumer saying it is done with one.
    Ack,
    /// A consumer refusing one, which is what makes the broker requeue it.
    Reject,
    /// Connection and channel management.
    Housekeeping,
}

impl Operation {
    /// Whether this carries a message body, and so spans more than its method
    /// frame.
    fn has_body(self) -> bool {
        matches!(self, Operation::Publish | Operation::Deliver)
    }
}

/// One operation, and every byte of it, so holding it back or letting it go is
/// a matter of copying `at` or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub operation: Operation,
    pub channel: u16,
    pub at: std::ops::Range<usize>,
}

impl Frame {
    /// What this starts, or `None` if it starts nothing: a header or body whose
    /// method frame came before it.
    fn operation(&self) -> Option<Operation> {
        let Kind::Method { class, method } = self.kind else {
            return None;
        };
        if class != basic::CLASS {
            return Some(Operation::Housekeeping);
        }
        Some(match method {
            basic::PUBLISH => Operation::Publish,
            basic::DELIVER | basic::GET_OK => Operation::Deliver,
            basic::ACK => Operation::Ack,
            basic::REJECT | basic::NACK => Operation::Reject,
            _ => Operation::Housekeeping,
        })
    }
}

/// Read `frames` as the operations they make up, and say how many frames those
/// account for.
///
/// An operation still arriving is left out along with the frames it has so far:
/// half a publish is not a publish, and holding one back before its body is
/// here would leave the broker waiting rather than the fleet losing a message.
#[must_use]
pub fn read(frames: &[Frame]) -> (Vec<Message>, usize) {
    let mut messages = Vec::new();
    let mut taken = 0;
    while let Some(frame) = frames.get(taken) {
        let Some(operation) = frame.operation() else {
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
            channel: frame.channel,
            at: frame.at.start..frames[last].at.end,
        });
        taken = last + 1;
    }
    (messages, taken)
}

/// The last frame of the message starting at `at`, or `None` while its body is
/// still arriving. A message whose body is empty has no body frames at all.
fn body_ends(frames: &[Frame], at: usize) -> Option<usize> {
    let header = frames.get(at + 1)?;
    let Some(wanted) = header.body_size else {
        // Not the content header this expects, so nothing says where the
        // message ends. Its method frame is all we can claim.
        return Some(at);
    };
    let mut last = at + 1;
    let mut carried = 0;
    while carried < wanted {
        let body = frames.get(last + 1)?;
        if body.kind != Kind::Body {
            return Some(last);
        }
        carried += body.payload_len() as u64;
        last += 1;
    }
    Some(last)
}

/// Reads one direction of one connection, across as many reads as it takes.
///
/// A frame can arrive split over several reads and a message over several
/// frames, so what is not yet whole is held here until the rest of it turns up.
#[derive(Debug, Default)]
pub struct Reader {
    /// Bytes belonging to an operation that has not finished arriving.
    pending: Vec<u8>,
}

impl crucible_protocol::Operations for Reader {
    fn read(&mut self, bytes: &[u8]) -> usize {
        Reader::read(self, bytes).len()
    }
}

impl Reader {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Take the next `bytes` off the wire and return the operations they
    /// completed, in the order they were sent.
    ///
    /// Each one's `at` indexes what this reader had buffered, which is its own
    /// bytes and not the caller's.
    pub fn read(&mut self, bytes: &[u8]) -> Vec<Message> {
        self.pending.extend_from_slice(bytes);
        let decoded = crate::frame::decode(&self.pending);
        let (messages, whole) = read(&decoded.frames);
        // Frames belonging to an operation still arriving stay pending, along
        // with the bytes behind them.
        let consumed = match whole {
            0 => 0,
            whole => decoded.frames[whole - 1].at.end,
        };
        self.pending.drain(..consumed);
        messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// basic.qos sets a channel's prefetch. It is in the class a fault is
    /// placed against but moves no message, so it is housekeeping.
    const QOS: u16 = 10;

    /// A frame of `kind` taking `payload` bytes, laid after the ones before it.
    fn frame(kind: Kind, payload: usize, at: &mut usize) -> Frame {
        let start = *at;
        *at += payload + 8;
        Frame {
            kind,
            channel: 1,
            at: start..*at,
            body_size: None,
        }
    }

    fn method(method: u16, at: &mut usize) -> Frame {
        frame(
            Kind::Method {
                class: basic::CLASS,
                method,
            },
            4,
            at,
        )
    }

    /// A content header stating a body of `size` bytes.
    fn header(size: u64, at: &mut usize) -> Frame {
        let mut header = frame(Kind::Header, 14, at);
        header.body_size = Some(size);
        header
    }

    /// The bytes of a frame of `ty` on channel 1 carrying `payload`.
    fn wire(ty: u8, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![ty, 0, 1];
        bytes.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("a test payload")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(payload);
        bytes.push(0xCE);
        bytes
    }

    /// The bytes of a publish carrying `body`.
    fn publish(body: &[u8]) -> Vec<u8> {
        let mut method = 60u16.to_be_bytes().to_vec();
        method.extend_from_slice(&basic::PUBLISH.to_be_bytes());
        let mut header = vec![0, 60, 0, 0];
        header.extend_from_slice(&(body.len() as u64).to_be_bytes());
        header.extend_from_slice(&[0, 0]);

        let mut bytes = wire(1, &method);
        bytes.extend(wire(2, &header));
        if !body.is_empty() {
            bytes.extend(wire(3, body));
        }
        bytes
    }

    /// A message split anywhere still arrives once, whole. This is what the
    /// reader is for: the wire hands over bytes, not messages.
    #[test]
    fn a_publish_split_across_reads_is_read_once_it_is_whole() {
        let bytes = publish(b"an order");
        for split in 1..bytes.len() {
            let mut reader = Reader::new();
            let first = reader.read(&bytes[..split]);
            let second = reader.read(&bytes[split..]);
            assert_eq!(
                first.len() + second.len(),
                1,
                "split at {split} read it {} times",
                first.len() + second.len()
            );
        }
    }

    #[test]
    fn a_reader_counts_across_reads_rather_than_within_one() {
        let mut reader = Reader::new();
        let mut bytes = publish(b"one");
        bytes.extend(publish(b"two"));
        assert_eq!(reader.read(&bytes).len(), 2, "both in one read");

        let mut reader = Reader::new();
        let mut seen = 0;
        for byte in bytes.chunks(1) {
            seen += reader.read(byte).len();
        }
        assert_eq!(seen, 2, "the same two, a byte at a time");
    }

    #[test]
    fn a_publish_is_its_method_header_and_body() {
        let at = &mut 0;
        let frames = [
            method(basic::PUBLISH, at),
            header(10, at),
            frame(Kind::Body, 10, at),
        ];
        let (messages, taken) = read(&frames);
        assert_eq!(taken, 3);
        assert_eq!(messages[0].operation, Operation::Publish);
        assert_eq!(messages[0].at, 0..*at, "the whole of it");
    }

    #[test]
    fn a_body_spanning_several_frames_is_one_message() {
        let at = &mut 0;
        let frames = [
            method(basic::PUBLISH, at),
            header(30, at),
            frame(Kind::Body, 10, at),
            frame(Kind::Body, 20, at),
        ];
        let (messages, taken) = read(&frames);
        assert_eq!(taken, 4);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].at, 0..*at);
    }

    #[test]
    fn a_message_whose_body_is_still_arriving_is_not_read_yet() {
        let at = &mut 0;
        let frames = [
            method(basic::PUBLISH, at),
            header(30, at),
            frame(Kind::Body, 10, at),
        ];
        let (messages, taken) = read(&frames);
        assert_eq!(messages, []);
        assert_eq!(taken, 0, "its frames are held for what follows");
    }

    #[test]
    fn a_message_with_an_empty_body_needs_no_body_frame() {
        let at = &mut 0;
        let frames = [
            method(basic::PUBLISH, at),
            header(0, at),
            method(basic::ACK, at),
        ];
        let (messages, taken) = read(&frames);
        assert_eq!(taken, 3);
        assert_eq!(
            messages.iter().map(|m| m.operation).collect::<Vec<_>>(),
            [Operation::Publish, Operation::Ack]
        );
    }

    #[test]
    fn an_ack_is_one_frame() {
        let at = &mut 0;
        let (messages, taken) = read(&[method(basic::ACK, at)]);
        assert_eq!(taken, 1);
        assert_eq!(messages[0].at, 0..*at);
    }

    #[test]
    fn the_operations_a_fault_is_placed_against_are_told_apart() {
        for (method_id, expected) in [
            (basic::ACK, Operation::Ack),
            (basic::REJECT, Operation::Reject),
            (basic::NACK, Operation::Reject),
            (QOS, Operation::Housekeeping),
        ] {
            let at = &mut 0;
            let (messages, _) = read(&[method(method_id, at)]);
            assert_eq!(messages[0].operation, expected, "method {method_id}");
        }
    }

    #[test]
    fn a_delivery_is_read_however_it_was_asked_for() {
        for method_id in [basic::DELIVER, basic::GET_OK] {
            let at = &mut 0;
            let frames = [method(method_id, at), header(0, at)];
            let (messages, _) = read(&frames);
            assert_eq!(messages[0].operation, Operation::Deliver, "{method_id}");
        }
    }

    #[test]
    fn another_class_is_housekeeping() {
        let at = &mut 0;
        // channel.open, which the fleet needs and no fault is placed against.
        let frames = [frame(
            Kind::Method {
                class: 20,
                method: 10,
            },
            4,
            at,
        )];
        let (messages, _) = read(&frames);
        assert_eq!(messages[0].operation, Operation::Housekeeping);
    }

    /// Heartbeats and the opening handshake belong to no operation, and must
    /// not be counted as one.
    #[test]
    fn what_belongs_to_no_operation_is_passed_over() {
        let at = &mut 0;
        let frames = [
            frame(Kind::Heartbeat, 0, at),
            method(basic::ACK, at),
            frame(Kind::Other(0), 0, at),
        ];
        let (messages, taken) = read(&frames);
        assert_eq!(taken, 3);
        assert_eq!(messages.len(), 1);
    }
}
