//! Replay-only pseudo-commands: the `MOON.*` records a writer interleaves with
//! the command log, and the ONE intercept every replay routes them through.
//!
//! A pseudo-command is a RESP array like any other record, so every reader of
//! every moon log parses it (a `#…` annotation line would not: `#` is the
//! RESP3 boolean type, a parse error that stops a flat replay and fails a
//! framed one; a new WAL record type byte reads as a torn record). A client
//! that sends one gets "unknown command": they never reach dispatch here, and
//! an OLDER binary that meets one sends it to dispatch, gets "unknown
//! command", classifies it [`ReplayRoute::Unhandled`] and skips it.
//!
//! The records, by class:
//!
//! | record | class | effect on replay |
//! |---|---|---|
//! | `MOON.TS <ms>` | clock observation | sets the expiry-judgment clock ([`super::clock`]) |
//! | `MOON.TS <ms> CLOSE` | clock observation | a clean close by this binary: ends its session in the file ([`super::clock`]) |
//! | `MOON.COLDCUT <watermark>` | cold plane | opens the cold replay gate ([`crate::persistence::cold_records`]) |
//! | `MOON.SPILLED <file_id> key…` | cold plane | demotes replay-built hot copies to their cold entries |
//! | `MOON.TXN BEGIN <id>` | transaction block | the data records up to the next `MOON.TXN` belong to open transaction `<id>` ([`super::txn`]) |
//! | `MOON.TXN PAUSE <id>` | transaction block | the records after it belong to no transaction; `<id>` stays open |
//! | `MOON.TXN END <id>` | transaction block | `<id>` ended (committed, or rolled back with its compensation logged before it) |
//! | `MOON.TXN RESET` | transaction block | every open transaction is dead: rolled back here (a writer reopening a file a crash left inside a block) |
//!
//! A `MOON.TXN` `<id>` is a non-zero decimal `u64`: the writers log the
//! transaction's LOG id, its origin shard above its shard's id
//! (`aof::txn_log_id`, R2b W1) — unique in a merged replication stream.
//!
//! ## `MOON.TS <ms>` (moon#1283)
//!
//! `<ms>` is the shard's cached clock (`CachedClock`, the value
//! `Database::now_ms` judged the command with) in unix milliseconds, captured
//! in the same synchronous section as the mutation the next records log
//! (`aof::AppendStamp`). The writer emits it before a record whose clock
//! differs from the last one it emitted in stream order, and once in every
//! generation head next to `MOON.COLDCUT`; it is written with `lsn = 0` in the
//! framed per-shard incr, so it never moves `max_lsn`. AOF only: the
//! replication stream never carries it (a replica applies on its own clock).
//!
//! A replay sets its judgment clock to the LAST `MOON.TS` read — never a
//! running maximum: a producer that parked between its mutation and its
//! enqueue lands after newer records, and its own, older stamp is the right
//! clock for it. Until a file's first `MOON.TS` the file's mtime pin rules
//! (an older binary's log, or its stamp-less prefix, replays exactly as
//! before). `MOON.TS 0`, a value past the year 9999 and a malformed record
//! are skipped and leave the clock where it was.
//!
//! ## `MOON.TS <ms> CLOSE` (R2 review of moon#1283)
//!
//! The writer appends it, as its last record, whenever it stops in order
//! (SHUTDOWN, SIGTERM, a closed channel) with the file still the live incr,
//! and makes it durable with the final sync. `<ms>` is the writer's clock at
//! the close. This binary writes a stamp before its FIRST record after it
//! opens a file ([`crate::persistence::aof::record_ctx`]), so records that
//! follow a `CLOSE` and precede the next stamp were written by another binary
//! (an older one, after a downgrade): a foreign segment, judged by
//! [`super::clock`]'s positional rule. A binary that predates `MOON.TS` sends
//! the record to dispatch ("unknown command") and skips it; one that knows
//! only the one-argument form classifies it [`Pseudo::MalformedTs`] and skips
//! it too.
//!
//! ## Clock records are observations, not data (decision Q6)
//!
//! A `MOON.TS` states when the records after it were judged. It is applied
//! the moment it is read, whatever happens to the data records around it:
//! a replay that skips data (an unterminated `MOON.TXN` block, moon#1300)
//! must still apply the block's clock records, or every record after the
//! block would be judged by a clock older than the one it was written
//! under. [`intercept`] therefore runs FIRST, before any data-skipping
//! decision, and a clock record's route ([`ReplayRoute::Marker`]) is never
//! KV history. The `MOON.TXN` blocks (moon#1300, [`super::txn`]) keep it by
//! construction: a block's data records are applied in place, under the
//! clocks around them, and a cut block is rolled back by restoring captured
//! pre-images — nothing is buffered, so no clock record is ever deferred or
//! dropped with one.

use crate::protocol::Frame;
use crate::storage::Database;

use super::ReplayRoute;

/// `MOON.TS <ms>`: the clock the records after it were judged under.
pub const TS: &[u8] = b"MOON.TS";

/// `MOON.TXN BEGIN|PAUSE|END <id>` / `MOON.TXN RESET`: the cross-store
/// transaction blocks (moon#1300, [`super::txn`]).
pub const TXN: &[u8] = b"MOON.TXN";

/// The largest `MOON.TS` a replay accepts: 9999-12-31T23:59:59.999Z. A
/// larger value is not a clock reading — a corrupt record — and is skipped
/// like any malformed one rather than judging every key expired.
pub const MAX_TS_MS: u64 = 253_402_300_799_999;

/// The common prefix of every pseudo-command name. A record without it is
/// data, so [`intercept`] costs one length check and a 5-byte compare per
/// data record.
const PREFIX: &[u8] = b"MOON.";

/// Longest RESP encoding of `MOON.TS <u64>`:
/// `*2\r\n$7\r\nMOON.TS\r\n$20\r\n<20 digits>\r\n` = 44 bytes.
pub const TS_RECORD_MAX_LEN: usize = 44;

/// The third argument of the clean-close marker `MOON.TS <ms> CLOSE`.
pub const CLOSE: &[u8] = b"CLOSE";

/// Longest RESP encoding of `MOON.TS <u64> CLOSE`: the 44 bytes above with
/// `*3` for `*2` and `$5\r\nCLOSE\r\n` (11 bytes) appended.
pub const CLOSE_RECORD_MAX_LEN: usize = TS_RECORD_MAX_LEN + 11;

/// `MOON.TS <ms>` (or `MOON.TS <ms> CLOSE`) encoded on the stack — no
/// allocation. The writer copies it into its record arena
/// (`aof::record_ctx`); a generation head writes it straight into the file.
#[derive(Clone, Copy)]
pub struct TsRecord {
    buf: [u8; CLOSE_RECORD_MAX_LEN],
    len: usize,
}

impl TsRecord {
    /// Encode `MOON.TS <ms>`.
    #[must_use]
    pub fn new(ms: u64) -> Self {
        Self::encode(ms, false)
    }

    /// Encode the clean-close marker `MOON.TS <ms> CLOSE` (see the module
    /// doc).
    #[must_use]
    pub fn close(ms: u64) -> Self {
        Self::encode(ms, true)
    }

    fn encode(ms: u64, close: bool) -> Self {
        let mut digits = itoa::Buffer::new();
        let d = digits.format(ms).as_bytes();
        let mut dl = itoa::Buffer::new();
        let dlen = dl.format(d.len()).as_bytes();
        let mut buf = [0u8; CLOSE_RECORD_MAX_LEN];
        let mut len = 0;
        let head: &[u8] = if close {
            b"*3\r\n$7\r\nMOON.TS\r\n$"
        } else {
            b"*2\r\n$7\r\nMOON.TS\r\n$"
        };
        let tail: &[u8] = if close { b"$5\r\nCLOSE\r\n" } else { b"" };
        for part in [head, dlen, b"\r\n", d, b"\r\n", tail] {
            buf[len..len + part.len()].copy_from_slice(part);
            len += part.len();
        }
        Self { buf, len }
    }

    /// The RESP bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// The verb of a `MOON.TXN` record (moon#1300, see [`super::txn`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnMarker {
    /// `MOON.TXN BEGIN <id>`: the data records up to the next `MOON.TXN`
    /// record belong to transaction `<id>`, which is opened the first time
    /// it is named and resumed after a [`TxnMarker::Pause`].
    Begin(u64),
    /// `MOON.TXN PAUSE <id>`: the records after it belong to no transaction
    /// (another client's writes); `<id>` stays open.
    Pause(u64),
    /// `MOON.TXN END <id>`: `<id>` is over — committed, or rolled back with
    /// its compensating records logged before this marker. Its records stand.
    End(u64),
    /// `MOON.TXN RESET`: every transaction still open is dead (the process
    /// that ran it is gone) and is rolled back at this point.
    Reset,
}

/// Longest RESP encoding of a `MOON.TXN` record:
/// `*3\r\n$8\r\nMOON.TXN\r\n$5\r\nPAUSE\r\n$20\r\n<20 digits>\r\n` = 56 bytes.
pub const TXN_RECORD_MAX_LEN: usize = 56;

/// A `MOON.TXN` record encoded on the stack — no allocation.
#[derive(Clone, Copy)]
pub struct TxnRecord {
    buf: [u8; TXN_RECORD_MAX_LEN],
    len: usize,
}

impl TxnRecord {
    /// Encode `marker`.
    #[must_use]
    pub fn new(marker: TxnMarker) -> Self {
        let (verb, id): (&[u8], Option<u64>) = match marker {
            TxnMarker::Begin(id) => (b"BEGIN", Some(id)),
            TxnMarker::Pause(id) => (b"PAUSE", Some(id)),
            TxnMarker::End(id) => (b"END", Some(id)),
            TxnMarker::Reset => (b"RESET", None),
        };
        let mut buf = [0u8; TXN_RECORD_MAX_LEN];
        let mut len = 0;
        let mut put = |part: &[u8]| {
            buf[len..len + part.len()].copy_from_slice(part);
            len += part.len();
        };
        put(if id.is_some() { b"*3\r\n" } else { b"*2\r\n" });
        put(b"$8\r\nMOON.TXN\r\n$");
        let mut vl = itoa::Buffer::new();
        put(vl.format(verb.len()).as_bytes());
        put(b"\r\n");
        put(verb);
        put(b"\r\n");
        if let Some(id) = id {
            let mut digits = itoa::Buffer::new();
            let d = digits.format(id).as_bytes();
            let mut dl = itoa::Buffer::new();
            put(b"$");
            put(dl.format(d.len()).as_bytes());
            put(b"\r\n");
            put(d);
            put(b"\r\n");
        }
        Self { buf, len }
    }

    /// The RESP bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// A recognised pseudo-command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pseudo {
    /// `MOON.TS <ms>` with a well-formed, non-zero `<ms>`.
    Ts(u64),
    /// `MOON.TS <ms> CLOSE` with a well-formed, non-zero `<ms>`: this binary
    /// closed the file cleanly at `<ms>` (see the module doc).
    Close(u64),
    /// `MOON.TS` with no argument, extra arguments (other than one `CLOSE`),
    /// a non-numeric, zero or out-of-range (> [`MAX_TS_MS`]) `<ms>`:
    /// skipped, the clock does not move.
    MalformedTs,
    /// `MOON.COLDCUT` / `MOON.SPILLED` (applied by
    /// [`crate::persistence::cold_records::replay_cold_plane_record`]).
    ColdPlane,
    /// A well-formed `MOON.TXN` record (moon#1300): applied by the replay's
    /// transaction state ([`super::txn`]), not by [`apply`].
    Txn(TxnMarker),
    /// `MOON.TXN` with an unknown verb, a missing, extra, non-numeric or zero
    /// `<id>`: skipped; no block opens, closes or rolls back.
    MalformedTxn,
}

impl Pseudo {
    /// The route a replay reports for this record.
    #[inline]
    #[must_use]
    pub fn route(self) -> ReplayRoute {
        match self {
            Pseudo::Ts(_)
            | Pseudo::Close(_)
            | Pseudo::MalformedTs
            | Pseudo::Txn(_)
            | Pseudo::MalformedTxn => ReplayRoute::Marker,
            Pseudo::ColdPlane => ReplayRoute::ColdPlane,
        }
    }
}

#[inline]
fn frame_ms(f: &Frame) -> Option<u64> {
    match f {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<u64>().ok(),
        Frame::Integer(n) => u64::try_from(*n).ok(),
        _ => None,
    }
}

/// Classify a record. `None` for every data record (anything not named
/// `MOON.*`) and for a `MOON.*` name this build does not know, which then
/// takes the ordinary path (dispatch answers "unknown command").
#[inline]
#[must_use]
pub fn classify(cmd: &[u8], args: &[Frame]) -> Option<Pseudo> {
    if cmd.len() <= PREFIX.len() || !cmd[..PREFIX.len()].eq_ignore_ascii_case(PREFIX) {
        return None;
    }
    if cmd.eq_ignore_ascii_case(TS) {
        let valid = |ms: &Frame| frame_ms(ms).filter(|&ms| ms != 0 && ms <= MAX_TS_MS);
        return Some(match args {
            [ms] => valid(ms).map_or(Pseudo::MalformedTs, Pseudo::Ts),
            [ms, Frame::BulkString(tag)] if tag.eq_ignore_ascii_case(CLOSE) => {
                valid(ms).map_or(Pseudo::MalformedTs, Pseudo::Close)
            }
            _ => Pseudo::MalformedTs,
        });
    }
    if cmd.eq_ignore_ascii_case(TXN) {
        return Some(classify_txn(args).map_or(Pseudo::MalformedTxn, Pseudo::Txn));
    }
    if cmd.eq_ignore_ascii_case(crate::persistence::cold_records::COLD_CUT)
        || cmd.eq_ignore_ascii_case(crate::persistence::cold_records::SPILLED)
    {
        return Some(Pseudo::ColdPlane);
    }
    None
}

/// `MOON.TXN <verb> [<id>]` → its marker, `None` when malformed.
fn classify_txn(args: &[Frame]) -> Option<TxnMarker> {
    let verb = match args.first()? {
        Frame::BulkString(b) | Frame::SimpleString(b) => b,
        _ => return None,
    };
    if verb.eq_ignore_ascii_case(b"RESET") {
        return (args.len() == 1).then_some(TxnMarker::Reset);
    }
    let [_, id] = args else {
        return None;
    };
    let id = frame_ms(id).filter(|&id| id != 0)?;
    if verb.eq_ignore_ascii_case(b"BEGIN") {
        Some(TxnMarker::Begin(id))
    } else if verb.eq_ignore_ascii_case(b"PAUSE") {
        Some(TxnMarker::Pause(id))
    } else if verb.eq_ignore_ascii_case(b"END") {
        Some(TxnMarker::End(id))
    } else {
        None
    }
}

/// Apply a classified pseudo-command.
pub fn apply(
    record: Pseudo,
    databases: &mut [Database],
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
) -> ReplayRoute {
    match record {
        Pseudo::Ts(ms) => {
            super::clock::observe_log_ts(ms);
        }
        Pseudo::Close(ms) => {
            super::clock::observe_close(ms);
        }
        Pseudo::MalformedTs => tracing::warn!(
            "AOF replay: malformed MOON.TS ({} args) skipped; the expiry judgment clock \
             stays where it was",
            args.len()
        ),
        Pseudo::ColdPlane => {
            crate::persistence::cold_records::replay_cold_plane_record(
                databases,
                cmd,
                args,
                selected_db,
            );
        }
        // The transaction state lives in the engine ([`super::txn`]); an
        // engine without one (or this free function) moves nothing.
        Pseudo::Txn(_) => {}
        Pseudo::MalformedTxn => tracing::warn!(
            "AOF replay: malformed MOON.TXN ({} args) skipped; no transaction block \
             opened, closed or rolled back",
            args.len()
        ),
    }
    record.route()
}

/// The pseudo-command intercept: applies `cmd` and returns its route when it
/// is a pseudo-command, `None` (nothing done) when it is a data record. Every
/// replay engine that reads a moon log calls it before anything else — see
/// the module doc for why it must run before any data-skipping decision.
#[inline]
pub fn intercept(
    databases: &mut [Database],
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
) -> Option<ReplayRoute> {
    let record = classify(cmd, args)?;
    Some(apply(record, databases, cmd, args, selected_db))
}

/// Whether `resp` is one `MOON.TS` record (test helper for byte-level
/// assertions on writer output).
#[cfg(test)]
pub(crate) fn is_ts_record(resp: &[u8]) -> bool {
    resp.starts_with(b"*2\r\n$7\r\nMOON.TS\r\n")
}

/// Whether `resp` is one clean-close marker (test helper).
#[cfg(test)]
pub(crate) fn is_close_record(resp: &[u8]) -> bool {
    resp.starts_with(b"*3\r\n$7\r\nMOON.TS\r\n") && resp.ends_with(b"$5\r\nCLOSE\r\n")
}

#[cfg(test)]
#[path = "pseudo_tests.rs"]
mod tests;
