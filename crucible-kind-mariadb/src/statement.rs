//! Reading a connection as packets, and finding the commits among them.

use std::borrow::Cow;

use crucible_protocol::{Carried, Direction, Doing, Placement};

/// A packet is three bytes of payload length, a sequence number, and the
/// payload.
const HEADER: usize = 4;
/// `COM_QUERY`. The rest of the payload is the statement being run.
const COM_QUERY: u8 = 0x03;
/// A client numbers each command from zero.
const FIRST: u8 = 0;

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
    /// How many commits this has carried.
    commits: u32,
    /// The moment a schedule named.
    watching: Option<String>,
    /// Which way this reader's traffic runs.
    direction: Direction,
}

impl Reader {
    #[must_use]
    pub fn new(direction: Direction, watching: Option<String>) -> Self {
        Self {
            pending: Vec::new(),
            commits: 0,
            watching,
            direction,
        }
    }

    /// Every packet these bytes complete. A tail that is still arriving is
    /// kept for the read that finishes it.
    fn read(&mut self, bytes: &[u8]) -> Vec<Packet> {
        self.pending.extend_from_slice(bytes);
        let mut packets = Vec::new();
        while let Some(header) = self.pending.get(..HEADER) {
            let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
            let whole = HEADER + length;
            if self.pending.len() < whole {
                break;
            }
            let bytes: Vec<u8> = self.pending.drain(..whole).collect();
            packets.push(Packet {
                seq: bytes[3],
                payload: HEADER,
                bytes,
            });
        }
        packets
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
        let packets = self.read(bytes);
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
        Carried {
            forward: packets
                .into_iter()
                .map(|packet| Cow::Owned(packet.bytes))
                .collect(),
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
        Reader::new(Direction::ClientToUpstream, None)
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
        let mut reader = Reader::new(
            Direction::ClientToUpstream,
            Some("commit:1:before".to_owned()),
        );
        let bytes = [query("BEGIN"), query("COMMIT")].concat();
        assert_eq!(reader.carry(&bytes, true).freeze_after, Some(1));
    }

    #[test]
    fn holding_after_a_commit_lets_it_go_first() {
        let mut reader = Reader::new(
            Direction::ClientToUpstream,
            Some("commit:1:after".to_owned()),
        );
        let bytes = [query("BEGIN"), query("COMMIT")].concat();
        assert_eq!(reader.carry(&bytes, true).freeze_after, Some(2));
    }

    #[test]
    fn a_moment_is_offered_before_it_is_placed() {
        let mut reader = Reader::new(
            Direction::ClientToUpstream,
            Some("commit:1:before".to_owned()),
        );
        let commit = query("COMMIT");
        let carried = reader.carry(&commit, false);
        assert_eq!(carried.freeze_after, None);
        assert_eq!(carried.found.len(), 2);
    }
}
