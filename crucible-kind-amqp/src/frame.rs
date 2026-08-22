//! AMQP 0-9-1 wire framing.
//!
//! ```text
//! | type | channel | size | payload | frame end |
//! |   1  |    2    |   4  |  size   |     1     |
//! ```
//!
//! A connection opens with a protocol header instead, which is not a frame.
//!
//! Only what a fault needs to recognise is decoded. Everything else is carried
//! through as bytes: the proxy forwards what it does not understand rather than
//! rewriting it, so a fleet speaking a dialect this does not know still runs.

/// Marks the end of every frame.
const FRAME_END: u8 = 0xCE;
/// `AMQP` followed by four version bytes, sent once before any frame.
const PROTOCOL_HEADER: &[u8; 4] = b"AMQP";
const HEADER_LEN: usize = 7;

/// What a frame carries, as far as this needs to know.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A class and method the peers are invoking on each other.
    Method { class: u16, method: u16 },
    /// The properties and body size of a message about to follow.
    Header,
    /// A run of message payload. A message may take several.
    Body,
    /// Keeps an idle connection open, and belongs to no message.
    Heartbeat,
    /// A frame type this does not know, which is carried through untouched.
    Other(u8),
}

impl Kind {
    fn of(ty: u8, payload: &[u8]) -> Self {
        match ty {
            1 => match payload.first_chunk::<4>() {
                Some(&[c0, c1, m0, m1]) => Kind::Method {
                    class: u16::from_be_bytes([c0, c1]),
                    method: u16::from_be_bytes([m0, m1]),
                },
                // A method frame too short to name one. Nothing can be done
                // with it, so it travels as itself.
                None => Kind::Other(ty),
            },
            2 => Kind::Header,
            3 => Kind::Body,
            8 => Kind::Heartbeat,
            other => Kind::Other(other),
        }
    }
}

/// One frame, and where it sat in the stream it was decoded from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub kind: Kind,
    /// Which channel it belongs to. Zero is the connection itself.
    pub channel: u16,
    /// Its bytes, header and frame-end included, so forwarding one is copying
    /// this range back out.
    pub at: std::ops::Range<usize>,
    /// For a content header, how many body bytes follow it. This is the only
    /// thing that says where a message ends.
    pub body_size: Option<u64>,
}

impl Frame {
    /// The bytes it carries, which is its own less the framing around them.
    #[must_use]
    pub fn payload_len(&self) -> usize {
        const FRAMING: usize = HEADER_LEN + 1;
        self.at.len().saturating_sub(FRAMING)
    }
}

/// What was read out of a stream, and how much of it was consumed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decoded {
    pub frames: Vec<Frame>,
    /// Bytes the frames occupy. What follows is a frame still arriving, which
    /// the caller keeps until the rest of it does.
    pub consumed: usize,
}

/// Decode every whole frame at the front of `bytes`.
///
/// A partial frame at the end is left for the next call: the caller holds the
/// remainder and hands it back with what arrives next.
#[must_use]
pub fn decode(bytes: &[u8]) -> Decoded {
    let mut decoded = Decoded::default();
    loop {
        let rest = &bytes[decoded.consumed..];
        let Some(frame) = one(rest, decoded.consumed) else {
            return decoded;
        };
        decoded.consumed = frame.at.end;
        decoded.frames.push(frame);
    }
}

/// The frame at the front of `bytes`, or `None` if not all of it is here yet.
/// `offset` is where `bytes` sits in the stream, so the frame can say where it
/// was found.
fn one(bytes: &[u8], offset: usize) -> Option<Frame> {
    if bytes.starts_with(PROTOCOL_HEADER) {
        // The opening handshake, which is 8 bytes and no frame at all. Reported
        // as one so a caller can forward it without knowing that.
        let at = offset..offset + 8;
        return (bytes.len() >= 8).then_some(Frame {
            kind: Kind::Other(0),
            channel: 0,
            at,
            body_size: None,
        });
    }
    let header = bytes.first_chunk::<HEADER_LEN>()?;
    let size = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    // The frame-end byte follows the payload and is part of the frame.
    let end = HEADER_LEN.checked_add(size)?.checked_add(1)?;
    if bytes.len() < end || bytes[end - 1] != FRAME_END {
        // Not all here, or not a frame at all. Either way there is nothing to
        // hand back; a bad frame-end means the stream is not AMQP and the
        // caller carries the bytes through.
        return None;
    }
    let payload = &bytes[HEADER_LEN..end - 1];
    let kind = Kind::of(header[0], payload);
    Some(Frame {
        kind,
        channel: u16::from_be_bytes([header[1], header[2]]),
        at: offset..offset + end,
        body_size: (kind == Kind::Header).then(|| body_size(payload)).flatten(),
    })
}

/// How many body bytes a content header says will follow it.
///
/// ```text
/// | class id | weight | body size | properties |
/// |    2     |   2    |     8     |     ..     |
/// ```
fn body_size(payload: &[u8]) -> Option<u64> {
    let size = payload.get(4..12)?.first_chunk::<8>().copied()?;
    Some(u64::from_be_bytes(size))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame of `ty` on `channel` carrying `payload`.
    fn frame(ty: u8, channel: u16, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![ty];
        bytes.extend_from_slice(&channel.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("a test payload")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(payload);
        bytes.push(FRAME_END);
        bytes
    }

    /// The payload of a method frame naming `class` and `method`.
    fn method(class: u16, method: u16) -> Vec<u8> {
        let mut payload = class.to_be_bytes().to_vec();
        payload.extend_from_slice(&method.to_be_bytes());
        payload
    }

    #[test]
    fn a_method_frame_names_its_class_and_method() {
        let bytes = frame(1, 1, &method(60, 40));
        let decoded = decode(&bytes);
        assert_eq!(decoded.consumed, bytes.len());
        assert_eq!(
            decoded.frames,
            [Frame {
                kind: Kind::Method {
                    class: 60,
                    method: 40
                },
                channel: 1,
                at: 0..bytes.len(),
                body_size: None,
            }]
        );
    }

    #[test]
    fn frame_types_are_told_apart() {
        for (ty, expected) in [
            (2u8, Kind::Header),
            (3, Kind::Body),
            (8, Kind::Heartbeat),
            (9, Kind::Other(9)),
        ] {
            let decoded = decode(&frame(ty, 0, b"x"));
            assert_eq!(decoded.frames[0].kind, expected, "frame type {ty}");
        }
    }

    #[test]
    fn a_method_frame_too_short_to_name_one_is_carried_through() {
        for truncated in 0..4 {
            let decoded = decode(&frame(1, 1, &method(60, 40)[..truncated]));
            assert_eq!(decoded.frames[0].kind, Kind::Other(1), "{truncated} bytes");
        }
    }

    #[test]
    fn a_content_header_states_how_much_body_follows() {
        let mut payload = vec![0, 60, 0, 0];
        payload.extend_from_slice(&4_096u64.to_be_bytes());
        payload.extend_from_slice(&[0, 0]);
        let decoded = decode(&frame(2, 1, &payload));
        assert_eq!(decoded.frames[0].body_size, Some(4_096));
    }

    #[test]
    fn only_a_content_header_states_a_body_size() {
        let decoded = decode(&frame(1, 1, &method(60, 40)));
        assert_eq!(decoded.frames[0].body_size, None);
    }

    #[test]
    fn several_frames_in_one_read_are_all_decoded() {
        let mut bytes = frame(1, 1, &method(60, 40));
        bytes.extend(frame(2, 1, b"header"));
        bytes.extend(frame(3, 1, b"body"));
        let decoded = decode(&bytes);
        assert_eq!(decoded.consumed, bytes.len());
        assert_eq!(decoded.frames.len(), 3);
    }

    /// A frame can arrive across several reads, so what is not yet whole is
    /// left where it is rather than guessed at.
    #[test]
    fn a_frame_still_arriving_is_not_decoded() {
        let whole = frame(1, 1, &method(60, 40));
        for split in 1..whole.len() {
            let decoded = decode(&whole[..split]);
            assert_eq!(decoded.frames, [], "a frame cut at {split} is not a frame");
            assert_eq!(decoded.consumed, 0);
        }
    }

    #[test]
    fn a_whole_frame_is_decoded_and_a_partial_one_left_behind() {
        let mut bytes = frame(1, 1, &method(60, 40));
        let whole = bytes.len();
        bytes.extend(&frame(3, 1, b"body")[..3]);
        let decoded = decode(&bytes);
        assert_eq!(decoded.frames.len(), 1);
        assert_eq!(decoded.consumed, whole, "the partial frame is left");
    }

    #[test]
    fn the_opening_handshake_is_carried_through() {
        let bytes = b"AMQP\x00\x00\x09\x01";
        let decoded = decode(bytes);
        assert_eq!(decoded.consumed, 8);
        assert_eq!(decoded.frames.len(), 1);
    }

    /// A stream that is not AMQP must not be chopped up as though it were.
    #[test]
    fn bytes_that_are_not_a_frame_are_not_decoded() {
        let decoded = decode(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(decoded.frames, []);
        assert_eq!(decoded.consumed, 0);
    }

    #[test]
    fn a_frame_without_its_end_byte_is_refused() {
        let mut bytes = frame(1, 1, &method(60, 40));
        let last = bytes.len() - 1;
        bytes[last] = 0x00;
        assert_eq!(decode(&bytes).frames, []);
    }
}
