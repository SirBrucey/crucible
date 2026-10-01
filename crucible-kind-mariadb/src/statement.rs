//! Reading a connection as packets, and finding the commits among them.

use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crucible_protocol::{Carried, Direction, Doing, Placement};

/// A packet is three bytes of payload length, a sequence number, and the
/// payload.
const HEADER: usize = 4;
/// `COM_QUERY`. The rest of the payload is the statement being run.
const COM_QUERY: u8 = 0x03;
/// A client numbers each command from zero.
const FIRST: u8 = 0;
/// `CLIENT_SSL`, set in the capabilities the client answers the greeting with.
const CLIENT_SSL: u32 = 0x0000_0800;
/// How many bytes of capabilities the answer starts with.
const CAPABILITIES: usize = 4;

/// Which side of a statement a fault goes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Before,
    After,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Side::Before => "before",
            Side::After => "after",
        })
    }
}

/// One packet off the wire.
struct Packet {
    seq: u8,
    /// Where the payload starts inside `bytes`.
    payload: usize,
    bytes: Vec<u8>,
}

impl Packet {
    /// The statement this packet runs, if it is a client running one.
    fn statement(&self) -> Option<&[u8]> {
        if self.seq != FIRST {
            return None;
        }
        let payload = self.bytes.get(self.payload..)?;
        match payload.split_first() {
            Some((&COM_QUERY, statement)) => Some(statement),
            _ => None,
        }
    }

    /// Whether the client is asking to encrypt the rest of the connection.
    ///
    /// The next thing it sends is a TLS handshake, so this is the last packet
    /// anything can read.
    fn upgrades(&self) -> bool {
        self.bytes
            .get(self.payload..self.payload + CAPABILITIES)
            .and_then(|flags| <[u8; CAPABILITIES]>::try_from(flags).ok())
            .is_some_and(|flags| u32::from_le_bytes(flags) & CLIENT_SSL != 0)
    }

    /// Whether this ends a transaction.
    fn commits(&self) -> bool {
        self.statement().is_some_and(|statement| {
            let statement = statement
                .iter()
                .rposition(|byte| !byte.is_ascii_whitespace() && *byte != b';')
                .map_or(&[][..], |last| &statement[..=last]);
            statement.trim_ascii_start().eq_ignore_ascii_case(b"commit")
        })
    }
}

/// One direction of one connection, read as packets.
///
/// A packet can arrive split over several reads, so what is not yet whole is
/// held here until the rest of it turns up.
pub struct Reader {
    /// Bytes of a packet that has not finished arriving.
    pending: Vec<u8>,
    /// How many packets this has taken off the wire.
    read: usize,
    /// How many commits this has carried.
    commits: u32,
    /// Whether the two ends encrypted this connection.
    encrypted: Arc<AtomicBool>,
    /// The moment a schedule named.
    watching: Option<String>,
    /// Which way this reader's traffic runs.
    direction: Direction,
}

impl Reader {
    #[must_use]
    pub fn new(direction: Direction, encrypted: Arc<AtomicBool>, watching: Option<String>) -> Self {
        Self {
            pending: Vec::new(),
            read: 0,
            commits: 0,
            encrypted,
            watching,
            direction,
        }
    }

    /// Every packet these bytes complete, and any tail that cannot be read.
    ///
    /// A tail that is still arriving is kept for the read that finishes it. An
    /// encrypted one is handed straight back.
    fn read(&mut self, bytes: &[u8]) -> (Vec<Packet>, Vec<u8>) {
        if self.encrypted.load(Ordering::Relaxed) {
            self.pending.extend_from_slice(bytes);
            return (Vec::new(), std::mem::take(&mut self.pending));
        }
        self.pending.extend_from_slice(bytes);
        let mut packets = Vec::new();
        while let Some(header) = self.pending.get(..HEADER) {
            let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
            let whole = HEADER + length;
            if self.pending.len() < whole {
                break;
            }
            let bytes: Vec<u8> = self.pending.drain(..whole).collect();
            let packet = Packet {
                seq: bytes[3],
                payload: HEADER,
                bytes,
            };
            // The client's first packet is the only place it can ask for TLS,
            // and whatever came with it is already encrypted.
            let upgrading = self.read == 0
                && self.direction == Direction::ClientToUpstream
                && packet.upgrades();
            self.read += 1;
            packets.push(packet);
            if upgrading {
                self.encrypted.store(true, Ordering::Relaxed);
                return (packets, std::mem::take(&mut self.pending));
            }
        }
        (packets, Vec::new())
    }

    /// Where a fault could go either side of a commit.
    fn either_side(&self, nth: u32) -> [(Side, Placement); 2] {
        let placement = |side: Side, why: &str| Placement {
            direction: self.direction,
            mark: format!("commit:{nth}:{side}"),
            why: why.to_owned(),
            doing: Doing::Holding,
        };
        [
            (
                Side::Before,
                placement(
                    Side::Before,
                    "a commit the client has written and the server has not seen",
                ),
            ),
            (
                Side::After,
                placement(
                    Side::After,
                    "a commit the server has been asked for and the client has had no answer to",
                ),
            ),
        ]
    }

    /// Whether this is the moment the schedule named.
    fn watches(&self, placement: &Placement) -> bool {
        self.watching.as_deref() == Some(placement.mark.as_str())
    }
}

impl crucible_protocol::Kind for Reader {
    fn carry<'a>(&mut self, bytes: &'a [u8], placing: bool) -> Carried<'a> {
        let (packets, opaque) = self.read(bytes);
        let mut freeze_after = None;
        let mut found = Vec::new();
        for (at, packet) in packets.iter().enumerate() {
            if !packet.commits() {
                continue;
            }
            self.commits += 1;
            for (side, placement) in self.either_side(self.commits) {
                if placing && self.watches(&placement) {
                    freeze_after = Some(match side {
                        Side::Before => at,
                        Side::After => at + 1,
                    });
                }
                found.push(placement);
            }
        }
        let mut forward: Vec<Cow<'a, [u8]>> = packets
            .into_iter()
            .map(|packet| Cow::Owned(packet.bytes))
            .collect();
        if !opaque.is_empty() {
            forward.push(Cow::Owned(opaque));
        }
        Carried {
            forward,
            freeze_after,
            found,
            did: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crucible_protocol::Kind;

    use super::*;

    /// A packet carrying `payload`, numbered `seq` in its exchange.
    fn packet(seq: u8, payload: &[u8]) -> Vec<u8> {
        let length = u32::try_from(payload.len()).expect("a test payload fits in a packet");
        let [a, b, c, _] = length.to_le_bytes();
        let mut bytes = vec![a, b, c, seq];
        bytes.extend_from_slice(payload);
        bytes
    }

    /// The client running `statement`, which starts a new exchange.
    fn query(statement: &str) -> Vec<u8> {
        let mut payload = vec![COM_QUERY];
        payload.extend_from_slice(statement.as_bytes());
        packet(FIRST, &payload)
    }

    fn reading() -> Reader {
        Reader::new(
            Direction::ClientToUpstream,
            Arc::new(AtomicBool::new(false)),
            None,
        )
    }

    /// A pair of readers that share whether the connection was encrypted.
    fn pair(watching: Option<&str>) -> (Reader, Reader) {
        let encrypted = Arc::new(AtomicBool::new(false));
        let watch = |direction| {
            Reader::new(
                direction,
                Arc::clone(&encrypted),
                watching
                    .filter(|_| direction == Direction::ClientToUpstream)
                    .map(ToOwned::to_owned),
            )
        };
        (
            watch(Direction::ClientToUpstream),
            watch(Direction::UpstreamToClient),
        )
    }

    /// The client answering the server's greeting with the capabilities it
    /// wants.
    fn answers_greeting(capabilities: u32) -> Vec<u8> {
        let mut payload = capabilities.to_le_bytes().to_vec();
        payload.extend_from_slice(&[0; 28]);
        packet(FIRST + 1, &payload)
    }

    fn marks(carried: &Carried<'_>) -> Vec<String> {
        carried
            .found
            .iter()
            .map(|placement| placement.mark.clone())
            .collect()
    }

    #[test]
    fn a_commit_offers_a_moment_either_side_of_itself() {
        let commit = query("COMMIT");
        let carried = reading().carry(&commit, false);
        assert_eq!(marks(&carried), ["commit:1:before", "commit:1:after"]);
    }

    #[rstest::rstest]
    #[case("BEGIN")]
    #[case("ROLLBACK")]
    #[case("INSERT INTO orders (id) VALUES (1)")]
    #[case("SELECT seq FROM orders WHERE id = 1 FOR UPDATE")]
    fn anything_else_offers_nothing(#[case] statement: &str) {
        assert!(reading().carry(&query(statement), false).found.is_empty());
    }

    #[rstest::rstest]
    #[case("commit")]
    #[case("Commit ")]
    #[case("COMMIT;")]
    #[case("  COMMIT ; ")]
    fn a_commit_is_read_however_it_is_spelt(#[case] statement: &str) {
        assert_eq!(reading().carry(&query(statement), false).found.len(), 2);
    }

    #[test]
    fn commits_are_numbered_along_the_connection() {
        let mut reader = reading();
        reader.carry(&query("COMMIT"), false);
        let commit = query("COMMIT");
        let carried = reader.carry(&commit, false);
        assert_eq!(marks(&carried), ["commit:2:before", "commit:2:after"]);
    }

    #[test]
    fn an_answer_that_reads_like_a_commit_is_not_one() {
        let mut payload = vec![COM_QUERY];
        payload.extend_from_slice(b"COMMIT");
        let answer = packet(2, &payload);
        let carried = reading().carry(&answer, false);
        assert!(carried.found.is_empty());
    }

    #[test]
    fn a_commit_split_across_reads_is_still_one_commit() {
        let bytes = query("COMMIT");
        let mut reader = reading();
        let split = bytes.len() / 2;
        assert!(reader.carry(&bytes[..split], false).found.is_empty());
        assert_eq!(reader.carry(&bytes[split..], false).found.len(), 2);
    }

    #[test]
    fn everything_read_is_carried_on_unchanged() {
        let bytes = [query("BEGIN"), query("COMMIT")].concat();
        let mut reader = reading();
        let split = bytes.len() / 3;
        let mut carried = reader.carry(&bytes[..split], false).forward.concat();
        carried.extend(reader.carry(&bytes[split..], false).forward.concat());
        assert_eq!(carried, bytes);
    }

    /// A fault is never placed part way through a packet.
    #[test]
    fn a_packet_that_has_not_finished_arriving_is_held_back() {
        let bytes = query("COMMIT");
        assert!(
            reading()
                .carry(&bytes[..bytes.len() - 1], false)
                .forward
                .is_empty()
        );
    }

    #[test]
    fn holding_before_a_commit_keeps_it_off_the_wire() {
        let (mut reader, _) = pair(Some("commit:1:before"));
        let bytes = [query("BEGIN"), query("COMMIT")].concat();
        assert_eq!(reader.carry(&bytes, true).freeze_after, Some(1));
    }

    #[test]
    fn holding_after_a_commit_lets_it_go_first() {
        let (mut reader, _) = pair(Some("commit:1:after"));
        let bytes = [query("BEGIN"), query("COMMIT")].concat();
        assert_eq!(reader.carry(&bytes, true).freeze_after, Some(2));
    }

    #[test]
    fn a_connection_the_client_encrypts_stops_being_read() {
        let (mut client, _) = pair(None);
        client.carry(&answers_greeting(CLIENT_SSL), false);
        // A TLS record, whose first three bytes read as a length far longer
        // than anything that follows.
        let hello = [0x16, 0x03, 0x01, 0x00, 0x05, 1, 2, 3, 4, 5];
        let carried = client.carry(&hello, false);
        assert_eq!(carried.forward.concat(), hello);
        assert!(carried.found.is_empty());
    }

    #[test]
    fn what_comes_back_over_an_encrypted_connection_is_carried_whole() {
        let (mut client, mut server) = pair(None);
        client.carry(&answers_greeting(CLIENT_SSL), false);
        let hello = [0x16, 0x03, 0x03, 0xff, 0xff, 9];
        assert_eq!(server.carry(&hello, false).forward.concat(), hello);
    }

    #[test]
    fn a_connection_the_client_leaves_alone_is_read_normally() {
        let (mut client, _) = pair(None);
        client.carry(&answers_greeting(0), false);
        assert_eq!(client.carry(&query("COMMIT"), false).found.len(), 2);
    }

    #[test]
    fn a_moment_is_offered_before_it_is_placed() {
        let (mut reader, _) = pair(Some("commit:1:before"));
        let commit = query("COMMIT");
        let carried = reader.carry(&commit, false);
        assert_eq!(carried.freeze_after, None);
        assert_eq!(carried.found.len(), 2);
    }
}
