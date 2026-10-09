//! Reading a connection as packets, and finding the commits among them.

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
};

use crucible_protocol::{Carried, Direction, Doing, Placement};

/// A packet is three bytes of payload length, a sequence number, and the
/// payload.
const HEADER: usize = 4;
/// `COM_QUERY`. The rest of the payload is the statement being run.
const COM_QUERY: u8 = 0x03;
/// `COM_STMT_PREPARE`. The rest is the statement to prepare.
const COM_STMT_PREPARE: u8 = 0x16;
/// `COM_STMT_EXECUTE`. The rest opens with the statement id to execute.
const COM_STMT_EXECUTE: u8 = 0x17;
/// `COM_STMT_CLOSE`. The rest is the statement id to close.
const COM_STMT_CLOSE: u8 = 0x19;
/// The first byte of an OK packet.
const OK: u8 = 0x00;
/// `SERVER_STATUS_IN_TRANS`: a transaction is currently active.
const IN_TRANS: u16 = 0x0001;
/// `SERVER_STATUS_AUTOCOMMIT`: autocommit mode is set.
const AUTOCOMMIT: u16 = 0x0002;
/// How many bytes a statement id takes.
const STATEMENT_ID: usize = 4;
/// A client numbers each command from zero.
const FIRST: u8 = 0;
/// `CLIENT_SSL`, set in the capabilities the client answers the greeting with.
const CLIENT_SSL: u32 = 0x0000_0800;
/// How many bytes of capabilities the answer starts with.
const CAPABILITIES: usize = 4;
/// DML statements that change data. Outside a transaction each is its own
/// commit.
const WRITES: [&[u8]; 4] = [b"insert", b"update", b"delete", b"replace"];
/// DDL statements that change the schema. MySQL commits an open transaction
/// before running one.
const DEFINES: [&[u8]; 3] = [b"create", b"alter", b"drop"];

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
    /// The command this packet carries and what follows it, if it is a client
    /// sending one.
    fn command(&self) -> Option<(u8, &[u8])> {
        if self.seq != FIRST {
            return None;
        }
        let (command, rest) = self.bytes.get(self.payload..)?.split_first()?;
        Some((*command, rest))
    }

    /// What the server says about the session, from a packet that carries its
    /// status flags.
    ///
    /// An OK packet carries them after the rows it affected and the id it
    /// generated, both length encoded. An EOF packet carries them after its
    /// warning count. Nothing else does.
    fn session(&self) -> Option<u16> {
        let (&header, rest) = self.bytes.get(self.payload..)?.split_first()?;
        let status = match header {
            OK => {
                let (_, affected) = lenenc(rest)?;
                let (_, generated) = lenenc(rest.get(affected..)?)?;
                rest.get(affected + generated..)?
            }
            // An EOF packet, which the newer protocol replaces with an OK.
            0xfe if rest.len() < 8 => rest.get(2..)?,
            _ => return None,
        };
        Some(u16::from_le_bytes(
            <[u8; 2]>::try_from(status.get(..2)?).ok()?,
        ))
    }

    /// The statement id in a prepare response.
    fn prepared_id(&self) -> Option<u32> {
        if self.seq != FIRST + 1 {
            return None;
        }
        let (&OK, rest) = self.bytes.get(self.payload..)?.split_first()? else {
            return None;
        };
        statement_id(rest)
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
}

/// The command that carried a statement to the server.
fn sent_as(command: u8) -> &'static str {
    match command {
        COM_STMT_EXECUTE => "COM_STMT_EXECUTE",
        _ => "COM_QUERY",
    }
}

/// The statement id a command opens with.
/// A length-encoded integer and how many bytes it took.
fn lenenc(rest: &[u8]) -> Option<(u64, usize)> {
    let (&first, after) = rest.split_first()?;
    let wide = |width: usize| {
        let mut eight = [0u8; 8];
        eight[..width].copy_from_slice(after.get(..width)?);
        Some((u64::from_le_bytes(eight), 1 + width))
    };
    match first {
        0xfc => wide(2),
        0xfd => wide(3),
        0xfe => wide(8),
        // 0xfb says a column is null, which only row data carries.
        0xfb => None,
        _ => Some((u64::from(first), 1)),
    }
}

fn statement_id(rest: &[u8]) -> Option<u32> {
    let named = rest.get(..STATEMENT_ID)?;
    Some(u32::from_le_bytes(
        <[u8; STATEMENT_ID]>::try_from(named).ok()?,
    ))
}

/// What `statement` asks the server to do.
fn asking(statement: &[u8]) -> Asking {
    {
        let trimmed = statement
            .iter()
            .rposition(|byte| !byte.is_ascii_whitespace() && *byte != b';')
            .map_or(&[][..], |last| &statement[..=last])
            .trim_ascii_start();
        let opens = |word: &[u8]| {
            trimmed.len() == word.len() && trimmed.eq_ignore_ascii_case(word)
                || trimmed
                    .get(..word.len())
                    .is_some_and(|start| start.eq_ignore_ascii_case(word))
                    && trimmed.get(word.len()).is_some_and(u8::is_ascii_whitespace)
        };
        match () {
            () if opens(b"commit") => Asking::Commit,
            () if opens(b"begin") || opens(b"start") => Asking::Open,
            () if opens(b"rollback") => Asking::Abandon,
            () if WRITES.iter().any(|word| opens(word)) => Asking::Write,
            () if DEFINES.iter().any(|word| opens(word)) => Asking::Define,
            () => Asking::Other,
        }
    }
}

/// What both directions of one connection agree on.
///
/// A prepared statement carries its text once in `COM_STMT_PREPARE` and is
/// executed by statement id after that. The id is in the server's prepare
/// response, so neither direction can read it alone.
#[derive(Debug)]
pub struct Shared {
    /// Whether the two ends encrypted this connection.
    encrypted: AtomicBool,
    /// What the server last said about the session.
    session: AtomicU16,
    /// The prepared statements on this connection.
    prepared: Mutex<Prepared>,
}

/// The prepared statements on one connection, and what each asks for.
#[derive(Debug, Default)]
struct Prepared {
    /// Prepares with no response yet, oldest first, each with what it asks
    /// for and what its text names.
    awaiting: VecDeque<(Asking, u32)>,
    /// What the statement with each id asks for, and what its text names.
    by_id: HashMap<u32, (Asking, u32)>,
}

/// FNV-1a over the bytes that say what a statement did.
///
/// `std::hash::DefaultHasher` is unspecified across releases, and a mark has
/// to match the one a schedule saved by an earlier run carries. The constants
/// and the loop are FNV-1a as published, taken from
/// <https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function>.
#[derive(Clone, Copy, Debug)]
struct Fingerprint(u32);

impl Fingerprint {
    const OFFSET: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn identity(self) -> Identity {
        Identity(self.0)
    }
}

/// A transaction open on a connection.
#[derive(Clone, Copy, Debug)]
struct Open {
    /// What its statements name so far.
    doing: Fingerprint,
    /// Whether any of them changed anything. A transaction that only read
    /// commits nothing, so it is nowhere to put a fault.
    wrote: bool,
}

impl Open {
    fn new() -> Self {
        Self {
            doing: Fingerprint::new(),
            wrote: false,
        }
    }
}

/// What names a transaction, taken from the statements it carried.
///
/// Two transactions sending the same statements with the same literals are one
/// identity, which is as far as the wire can tell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Identity(u32);

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            encrypted: AtomicBool::new(false),
            // What a server's own default is, until we see otherwise.
            session: AtomicU16::new(AUTOCOMMIT),
            prepared: Mutex::default(),
        }
    }
}

/// What a statement asks the server to do, as far as transactions go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Asking {
    /// `BEGIN` or `START TRANSACTION`.
    Open,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Abandon,
    /// DML. Its own commit when no transaction is open.
    Write,
    /// DDL, which commits an open transaction first.
    Define,
    /// Anything else.
    Other,
}

impl Asking {
    /// What the server commits on.
    fn commits_on(self) -> &'static str {
        match self {
            Asking::Commit => "COMMIT",
            Asking::Write => "of an autocommit DML statement",
            Asking::Define => "of a DDL statement",
            Asking::Open | Asking::Abandon | Asking::Other => "of a statement",
        }
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
    /// The transaction open on this connection.
    open: Option<Open>,
    /// How many times this connection has committed each identity, so the
    /// same work twice is two moments.
    seen: HashMap<Identity, u32>,
    shared: Arc<Shared>,
    /// The moment a schedule named.
    watching: Option<String>,
    /// Which way this reader's traffic runs.
    direction: Direction,
}

impl Reader {
    #[must_use]
    pub fn new(direction: Direction, shared: Arc<Shared>, watching: Option<String>) -> Self {
        Self {
            pending: Vec::new(),
            read: 0,
            open: None,
            seen: HashMap::new(),
            shared,
            watching,
            direction,
        }
    }

    /// The prepared statements on this connection, locked for this read.
    fn prepared(&self) -> MutexGuard<'_, Prepared> {
        self.shared
            .prepared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// What this packet asks the server to do.
    ///
    /// A `COM_QUERY` carries its own text. A prepared statement carried its
    /// text when it was prepared, so what it asks for is looked up by statement
    /// id.
    fn asked(&mut self, packet: &Packet) -> Option<(Asking, u32)> {
        let (command, rest) = packet.command()?;
        let mut held = self.prepared();
        match command {
            COM_QUERY => {
                let mut text = Fingerprint::new();
                text.update(rest);
                Some((asking(rest), text.0))
            }
            COM_STMT_PREPARE => {
                let mut text = Fingerprint::new();
                text.update(rest);
                held.awaiting.push_back((asking(rest), text.0));
                // Preparing runs nothing.
                None
            }
            COM_STMT_EXECUTE => {
                let (asking, text) = *held.by_id.get(&statement_id(rest)?)?;
                // The statement id is the server's to choose, so what is
                // eaten is the text it stands for and the parameters after it.
                let mut statement = Fingerprint(text);
                statement.update(rest.get(STATEMENT_ID..)?);
                Some((asking, statement.0))
            }
            COM_STMT_CLOSE => {
                held.by_id.remove(&statement_id(rest)?);
                None
            }
            _ => None,
        }
    }

    /// Record the statement id a prepare response carries, and say whether
    /// this packet was one.
    fn names_prepared(&self, packet: &Packet) -> bool {
        let mut held = self.prepared();
        // A client waits for each response before sending again, so the first
        // response after a prepare is that prepare's.
        let Some(prepared) = held.awaiting.pop_front() else {
            return false;
        };
        if let Some(named) = packet.prepared_id() {
            held.by_id.insert(named, prepared);
        }
        true
    }

    /// Whether the server has a transaction open that no `BEGIN` announced.
    ///
    /// A client with autocommit off opens one by running its first statement,
    /// so the only account of it is what the server reports.
    fn server_holds_a_transaction(&self) -> bool {
        let session = self.shared.session.load(Ordering::Relaxed);
        session & IN_TRANS != 0 || session & AUTOCOMMIT == 0
    }

    /// Every packet these bytes complete, and any tail that cannot be read.
    ///
    /// A tail that is still arriving is kept for the read that finishes it. An
    /// encrypted one is handed straight back.
    fn read(&mut self, bytes: &[u8]) -> (Vec<Packet>, Vec<u8>) {
        if self.shared.encrypted.load(Ordering::Relaxed) {
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
                self.shared.encrypted.store(true, Ordering::Relaxed);
                return (packets, std::mem::take(&mut self.pending));
            }
        }
        (packets, Vec::new())
    }

    /// Where a fault could go either side of a commit.
    fn either_side(&self, at: (Identity, u32), sent: &str) -> [(Side, Placement); 2] {
        let (identity, nth) = at;
        let placement = |side: Side, why: String| Placement {
            direction: self.direction,
            mark: format!("commit:{identity}:{nth}:{side}"),
            why,
            doing: Doing::Holding,
        };
        [
            (
                Side::Before,
                placement(Side::Before, format!("a {sent} the server has not seen")),
            ),
            (
                Side::After,
                placement(Side::After, format!("a {sent} with no OK packet back yet")),
            ),
        ]
    }

    /// What the server commits on in this packet, or `None` where the packet
    /// ends no transaction.
    ///
    /// A client that opened a transaction commits when it says so. One that did
    /// not is in autocommit, where the server commits each write as it runs
    /// it.
    fn commits_at(&mut self, packet: &Packet) -> Option<(Identity, u32, String)> {
        let command = packet.command()?.0;
        let (asking, statement) = self.asked(packet)?;
        let was_open = self.open.is_some();
        // What the transaction named.
        let committed = match asking {
            // `BEGIN` opens one and says nothing about what it will do.
            Asking::Open => {
                self.open = Some(Open::new());
                None
            }
            // Nor does `COMMIT`, which commits whatever is open and nothing
            // where none is.
            Asking::Commit => self.open.take(),
            Asking::Abandon => {
                self.open = None;
                None
            }
            // DDL commits whatever was open before it runs, so it ends a
            // transaction either way, and it changes the schema.
            Asking::Define => {
                let mut open = self.open.take().unwrap_or_else(Open::new);
                open.doing.update(&statement.to_le_bytes());
                open.wrote = true;
                (!was_open).then_some(open)
            }
            // Work, which commits itself only where no transaction is open.
            Asking::Write => {
                let mut open = self.open.take().unwrap_or_else(Open::new);
                open.doing.update(&statement.to_le_bytes());
                open.wrote = true;
                if was_open || self.server_holds_a_transaction() {
                    self.open = Some(open);
                    None
                } else {
                    Some(open)
                }
            }
            // Anything else, which belongs to a transaction the server may
            // have opened for it. Outside it belongs to nothing.
            Asking::Other => {
                if was_open || self.server_holds_a_transaction() {
                    let mut open = self.open.take().unwrap_or_else(Open::new);
                    open.doing.update(&statement.to_le_bytes());
                    self.open = Some(open);
                }
                None
            }
        };
        // A transaction that changed nothing commits nothing, so there is no
        // work for a fault to catch either side of it.
        let identity = committed.filter(|open| open.wrote)?.doing.identity();
        let nth = self.seen.entry(identity).or_insert(0);
        *nth += 1;
        Some((
            identity,
            *nth,
            format!("{} {}", sent_as(command), asking.commits_on()),
        ))
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
            // Only the client sends commands; what comes back carries the
            // statement ids.
            if self.direction == Direction::UpstreamToClient {
                if self.names_prepared(packet) {
                    continue;
                }
                if let Some(session) = packet.session() {
                    self.shared.session.store(session, Ordering::Relaxed);
                }
                continue;
            }
            let Some((identity, nth, sent)) = self.commits_at(packet) else {
                continue;
            };
            for (side, placement) in self.either_side((identity, nth), &sent) {
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
            unreadable: None,
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

    /// A client reader on a connection whose greeting has been answered.
    /// A client with autocommit on and no transaction open.
    fn reading() -> Reader {
        let (client, _) = session(None);
        client
    }

    /// A client with a transaction open that has written something, so the
    /// next `COMMIT` it sends is a moment. Watching for `mark`, where given.
    fn committing(watching: Option<&str>) -> Reader {
        let (mut client, mut server) = pair(watching);
        client.carry(&answers_greeting(0), false);
        // Autocommit off, so the server opens the transaction itself.
        server.carry(&reports(IN_TRANS), false);
        client.carry(&wrote(), false);
        client
    }

    /// A statement that puts work in the open transaction.
    fn wrote() -> Vec<u8> {
        query("INSERT INTO records VALUES ('www')")
    }

    #[test]
    fn a_commit_that_carried_no_statements_is_not_a_moment() {
        let (mut client, _) = session(None);
        assert!(marks(&client.carry(&query("COMMIT"), false)).is_empty());
    }

    /// A pair of readers on a connection whose greeting has been answered,
    /// which is where every session starts.
    /// A service that opens a connection per request numbers every
    /// connection's first commit the same.
    #[test]
    fn two_connections_doing_different_work_name_different_moments() {
        let (mut one, _) = session(None);
        let (mut two, _) = session(None);
        for (reader, name) in [(&mut one, "www"), (&mut two, "mail")] {
            reader.carry(&query("BEGIN"), false);
            reader.carry(
                &query(&format!("INSERT INTO records VALUES ('{name}')")),
                false,
            );
        }

        let named = |carried: &Carried<'_>| marks(carried)[0].clone();
        assert_ne!(
            named(&one.carry(&query("COMMIT"), false)),
            named(&two.carry(&query("COMMIT"), false)),
        );
    }

    /// A saved schedule is replayed against the name an earlier run gave.
    #[test]
    fn the_same_work_names_the_same_moment_twice_over() {
        let same = || {
            let (mut reader, _) = session(None);
            reader.carry(&query("BEGIN"), false);
            reader.carry(&query("INSERT INTO records VALUES ('www')"), false);
            marks(&reader.carry(&query("COMMIT"), false))
        };
        assert_eq!(same(), same());
    }

    fn session(watching: Option<&str>) -> (Reader, Reader) {
        let (mut client, server) = pair(watching);
        client.carry(&answers_greeting(0), false);
        (client, server)
    }

    /// The same, before the client has answered the greeting.
    fn pair(watching: Option<&str>) -> (Reader, Reader) {
        let shared = Arc::new(Shared::default());
        let watch = |direction| {
            Reader::new(
                direction,
                Arc::clone(&shared),
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
    /// The server answering a statement, reporting `status` about the session.
    fn reports(status: u16) -> Vec<u8> {
        let mut payload = vec![OK, 0, 0];
        payload.extend_from_slice(&status.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        packet(FIRST + 1, &payload)
    }

    #[test]
    fn a_statement_the_server_holds_a_transaction_for_is_not_its_own_commit() {
        let (mut client, mut server) = session(None);
        server.carry(&reports(IN_TRANS), false);

        let wrote = marks(&client.carry(&query("INSERT INTO records VALUES ('www')"), false));
        assert!(
            wrote.is_empty(),
            "the statement committed nothing: {wrote:?}"
        );

        let committed = marks(&client.carry(&query("COMMIT"), false));
        assert!(!committed.is_empty(), "the COMMIT is the moment");

        // A bare commit names an empty transaction, and this one is not that.
        let (mut bare, _) = session(None);
        let nothing = marks(&bare.carry(&query("COMMIT"), false));
        assert_ne!(committed, nothing, "the commit carries the statement");
    }

    /// A `SELECT` opens a transaction just as a write does, and is part of
    /// what the commit that follows names.
    #[test]
    fn a_select_the_server_holds_a_transaction_for_is_carried_by_the_commit() {
        let committed = |read: bool| {
            let (mut client, mut server) = session(None);
            server.carry(&reports(IN_TRANS), false);
            if read {
                client.carry(&query("SELECT id FROM records"), false);
            }
            client.carry(&wrote(), false);
            marks(&client.carry(&query("COMMIT"), false))
        };
        assert_ne!(committed(true), committed(false));
    }

    /// A transaction that only read changed nothing, so its commit is nowhere
    /// to put a fault.
    #[test]
    fn a_transaction_that_only_read_is_not_a_moment() {
        let (mut client, mut server) = session(None);
        server.carry(&reports(IN_TRANS), false);
        client.carry(&query("SELECT id FROM records"), false);
        assert!(marks(&client.carry(&query("COMMIT"), false)).is_empty());
    }

    #[test]
    fn a_statement_the_server_holds_no_transaction_for_commits_itself() {
        let (mut client, mut server) = session(None);
        server.carry(&reports(AUTOCOMMIT), false);

        let wrote = marks(&client.carry(&query("INSERT INTO records VALUES ('www')"), false));
        assert_eq!(wrote.len(), 2, "either side of its own commit: {wrote:?}");
    }

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
        let carried = committing(None).carry(&commit, false);
        let named = marks(&carried);
        let [before, after] = named.as_slice() else {
            panic!("one moment each side: {named:?}");
        };
        assert_eq!(
            before.strip_suffix("before"),
            after.strip_suffix("after"),
            "both sides of the one moment"
        );
    }

    #[rstest::rstest]
    #[case("BEGIN")]
    #[case("ROLLBACK")]
    #[case("SELECT seq FROM orders WHERE id = 1 FOR UPDATE")]
    fn anything_else_offers_nothing(#[case] statement: &str) {
        assert!(reading().carry(&query(statement), false).found.is_empty());
    }

    #[rstest::rstest]
    #[case("INSERT INTO orders (id) VALUES (1)")]
    #[case("UPDATE stock SET level = 1 WHERE item = 'book'")]
    #[case("DELETE FROM outbox WHERE seq = 1")]
    #[case("CREATE TABLE IF NOT EXISTS orders (id INT)")]
    fn a_write_outside_a_transaction_is_its_own_commit(#[case] statement: &str) {
        assert_eq!(reading().carry(&query(statement), false).found.len(), 2);
    }

    #[test]
    fn a_write_inside_a_transaction_is_not_a_commit() {
        let mut reader = reading();
        reader.carry(&query("BEGIN"), false);
        let inside = [
            query("INSERT INTO orders (id) VALUES (1)"),
            query("DELETE FROM outbox WHERE seq = 1"),
        ]
        .concat();
        assert!(reader.carry(&inside, false).found.is_empty());
        assert_eq!(reader.carry(&query("COMMIT"), false).found.len(), 2);
    }

    #[test]
    fn a_rollback_ends_a_transaction_without_committing() {
        let mut reader = reading();
        reader.carry(&query("BEGIN"), false);
        assert!(reader.carry(&query("ROLLBACK"), false).found.is_empty());
        assert_eq!(
            reader
                .carry(&query("INSERT INTO orders (id) VALUES (1)"), false)
                .found
                .len(),
            2
        );
    }

    #[test]
    fn a_schema_change_ends_an_open_transaction() {
        let mut reader = reading();
        reader.carry(&query("BEGIN"), false);
        // The transaction it ends is the commit, so the statement is not a
        // second one.
        assert!(
            reader
                .carry(&query("CREATE TABLE t (id INT)"), false)
                .found
                .is_empty()
        );
        assert_eq!(
            reader
                .carry(&query("INSERT INTO orders (id) VALUES (1)"), false)
                .found
                .len(),
            2
        );
    }

    #[rstest::rstest]
    #[case("committed_at = NOW()")]
    #[case("SELECT * FROM commits")]
    #[case("INSERTED")]
    fn a_word_that_merely_starts_the_same_is_not_a_statement(#[case] statement: &str) {
        assert!(reading().carry(&query(statement), false).found.is_empty());
    }

    #[rstest::rstest]
    #[case("commit")]
    #[case("Commit ")]
    #[case("COMMIT;")]
    #[case("  COMMIT ; ")]
    fn a_commit_is_read_however_it_is_spelt(#[case] statement: &str) {
        let found = committing(None).carry(&query(statement), false).found;
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn commits_are_numbered_along_the_connection() {
        let mut reader = committing(None);
        let first = marks(&reader.carry(&query("COMMIT"), false));
        reader.carry(&wrote(), false);
        let commit = query("COMMIT");
        let again = marks(&reader.carry(&commit, false));

        assert_ne!(first, again, "the same work twice is two moments");
        assert!(again[0].ends_with(":2:before"), "{again:?}");
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
        let mut reader = committing(None);
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
        let bytes = [query("BEGIN"), wrote(), query("COMMIT")].concat();
        let (mut reader, _) = session(Some(&learned(&bytes, ":before")));
        assert_eq!(reader.carry(&bytes, true).freeze_after, Some(2));
    }

    #[test]
    fn holding_after_a_commit_lets_it_go_first() {
        let bytes = [query("BEGIN"), wrote(), query("COMMIT")].concat();
        let (mut reader, _) = session(Some(&learned(&bytes, ":after")));
        assert_eq!(reader.carry(&bytes, true).freeze_after, Some(3));
    }

    /// The mark a fault-free run offers for the moment on `side` of `bytes`.
    fn learned(bytes: &[u8], side: &str) -> String {
        let (mut reader, _) = session(None);
        marks(&reader.carry(bytes, false))
            .into_iter()
            .find(|mark| mark.ends_with(side))
            .expect("a commit was named")
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
        let mut client = committing(None);
        assert_eq!(client.carry(&query("COMMIT"), false).found.len(), 2);
    }

    /// `COM_STMT_PREPARE` for `statement`.
    fn prepares(statement: &str) -> Vec<u8> {
        let mut payload = vec![COM_STMT_PREPARE];
        payload.extend_from_slice(statement.as_bytes());
        packet(FIRST, &payload)
    }

    /// A prepare response giving the statement id `id`.
    fn prepare_response(id: u32) -> Vec<u8> {
        let mut payload = vec![OK];
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&[0; 6]);
        packet(FIRST + 1, &payload)
    }

    /// `COM_STMT_EXECUTE` for the statement id `id`.
    fn executes(id: u32) -> Vec<u8> {
        let mut payload = vec![COM_STMT_EXECUTE];
        payload.extend_from_slice(&id.to_le_bytes());
        payload.push(0);
        packet(FIRST, &payload)
    }

    /// A parameterised statement carries its text only in the prepare, so what
    /// it asks for has to be read from there.
    #[test]
    fn a_prepared_write_is_its_own_commit() {
        let (mut client, mut server) = session(None);
        // Preparing runs nothing.
        assert!(
            client
                .carry(&prepares("INSERT INTO orders (id) VALUES (?)"), false)
                .found
                .is_empty()
        );
        server.carry(&prepare_response(7), false);
        assert_eq!(client.carry(&executes(7), false).found.len(), 2);
    }

    /// The statement id is in the server's prepare response, so the direction
    /// that sees it is not the one that sees the text.
    #[test]
    fn a_statement_with_no_id_asks_nothing() {
        let (mut client, _) = session(None);
        client.carry(&prepares("INSERT INTO orders (id) VALUES (?)"), false);
        assert!(client.carry(&executes(7), false).found.is_empty());
    }

    #[test]
    fn a_prepared_write_inside_a_transaction_is_not_a_commit() {
        let (mut client, mut server) = session(None);
        client.carry(&prepares("INSERT INTO orders (id) VALUES (?)"), false);
        server.carry(&prepare_response(7), false);
        client.carry(&query("BEGIN"), false);
        assert!(client.carry(&executes(7), false).found.is_empty());
    }

    #[test]
    fn a_statement_the_client_closed_is_forgotten() {
        let (mut client, mut server) = session(None);
        client.carry(&prepares("INSERT INTO orders (id) VALUES (?)"), false);
        server.carry(&prepare_response(7), false);
        let mut closes = vec![COM_STMT_CLOSE];
        closes.extend_from_slice(&7u32.to_le_bytes());
        client.carry(&packet(FIRST, &closes), false);
        assert!(client.carry(&executes(7), false).found.is_empty());
    }

    /// The reporter reads a placement as "on <this>", so it is a noun phrase.
    #[rstest::rstest]
    #[case("COMMIT", "COM_QUERY COMMIT", true)]
    #[case(
        "INSERT INTO orders (id) VALUES (1)",
        "COM_QUERY of an autocommit DML statement",
        false
    )]
    #[case("CREATE TABLE t (id INT)", "COM_QUERY of a DDL statement", false)]
    fn a_placement_names_the_wire_event(
        #[case] statement: &str,
        #[case] names: &str,
        #[case] open: bool,
    ) {
        let sent = query(statement);
        let mut reader = if open { committing(None) } else { reading() };
        let carried = reader.carry(&sent, false);
        let whys: Vec<&str> = carried.found.iter().map(|p| p.why.as_str()).collect();
        assert_eq!(
            whys,
            [
                format!("a {names} the server has not seen"),
                format!("a {names} with no OK packet back yet"),
            ]
        );
    }

    /// A statement run by id reached the server as `COM_STMT_EXECUTE`.
    #[test]
    fn a_prepared_statement_is_named_by_the_command_that_ran_it() {
        let (mut client, mut server) = session(None);
        client.carry(&prepares("INSERT INTO orders (id) VALUES (?)"), false);
        server.carry(&prepare_response(7), false);
        let runs = executes(7);
        let carried = client.carry(&runs, false);
        assert_eq!(
            carried.found.first().map(|p| p.why.as_str()),
            Some("a COM_STMT_EXECUTE of an autocommit DML statement the server has not seen")
        );
    }

    #[test]
    fn a_moment_is_offered_before_it_is_placed() {
        let mut reader = committing(Some("commit:1:before"));
        let commit = query("COMMIT");
        let carried = reader.carry(&commit, false);
        assert_eq!(carried.freeze_after, None);
        assert_eq!(carried.found.len(), 2);
    }
}
