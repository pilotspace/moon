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
//! | `MOON.COLDCUT <watermark>` | cold plane | opens the cold replay gate ([`crate::persistence::cold_records`]) |
//! | `MOON.SPILLED <file_id> key…` | cold plane | demotes replay-built hot copies to their cold entries |
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
//! clock for it. The one exception is a foreign tail: the records after a
//! file's LAST stamp, when its mtime is more than a second later, are judged
//! by the mtime ([`super::clock`], R1 review: an older binary appended them
//! after a downgrade). Until a file's first `MOON.TS` the file's mtime pin rules
//! (an older binary's log, or its stamp-less prefix, replays exactly as
//! before). `MOON.TS 0`, a value past the year 9999 and a malformed record
//! are skipped and leave the clock where it was.
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
//! KV history. A block-buffering replay that defers data records until a
//! terminator must keep each `MOON.TS` IN ORDER with the deferred records it
//! precedes (so a committed block replays under the clocks it was written
//! under) and, when it discards the block, still apply the block's last
//! `MOON.TS` ([`apply`] on the [`Pseudo::Ts`] it buffered).

use crate::protocol::Frame;
use crate::storage::Database;

use super::ReplayRoute;

/// `MOON.TS <ms>`: the clock the records after it were judged under.
pub const TS: &[u8] = b"MOON.TS";

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

/// `MOON.TS <ms>` encoded on the stack — no allocation. The writer copies it
/// into its record arena (`aof::record_ctx`); a generation head writes it
/// straight into the file.
#[derive(Clone, Copy)]
pub struct TsRecord {
    buf: [u8; TS_RECORD_MAX_LEN],
    len: usize,
}

impl TsRecord {
    /// Encode `MOON.TS <ms>`.
    #[must_use]
    pub fn new(ms: u64) -> Self {
        let mut digits = itoa::Buffer::new();
        let d = digits.format(ms).as_bytes();
        let mut dl = itoa::Buffer::new();
        let dlen = dl.format(d.len()).as_bytes();
        let mut buf = [0u8; TS_RECORD_MAX_LEN];
        let mut len = 0;
        for part in [
            b"*2\r\n$7\r\nMOON.TS\r\n$".as_slice(),
            dlen,
            b"\r\n",
            d,
            b"\r\n",
        ] {
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

/// A recognised pseudo-command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pseudo {
    /// `MOON.TS <ms>` with a well-formed, non-zero `<ms>`.
    Ts(u64),
    /// `MOON.TS` with no argument, extra arguments, a non-numeric, zero or
    /// out-of-range (> [`MAX_TS_MS`]) `<ms>`: skipped, the clock does not
    /// move.
    MalformedTs,
    /// `MOON.COLDCUT` / `MOON.SPILLED` (applied by
    /// [`crate::persistence::cold_records::replay_cold_plane_record`]).
    ColdPlane,
}

impl Pseudo {
    /// The route a replay reports for this record.
    #[inline]
    #[must_use]
    pub fn route(self) -> ReplayRoute {
        match self {
            Pseudo::Ts(_) | Pseudo::MalformedTs => ReplayRoute::Marker,
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
        return Some(match args {
            [ms] => frame_ms(ms)
                .filter(|&ms| ms != 0 && ms <= MAX_TS_MS)
                .map_or(Pseudo::MalformedTs, Pseudo::Ts),
            _ => Pseudo::MalformedTs,
        });
    }
    if cmd.eq_ignore_ascii_case(crate::persistence::cold_records::COLD_CUT)
        || cmd.eq_ignore_ascii_case(crate::persistence::cold_records::SPILLED)
    {
        return Some(Pseudo::ColdPlane);
    }
    None
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

#[cfg(test)]
#[path = "pseudo_tests.rs"]
mod tests;
