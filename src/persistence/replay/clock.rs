//! The expiry-judgment clock of an AOF replay (moon#1277).
//!
//! A replayed command judges whether a key has expired against its
//! database's cached clock, which is the wall clock at replay time. A key
//! whose TTL passed while the server was DOWN therefore read as expired to
//! every record that touched it — records that were all written while it was
//! alive. A TTL-preserving read-modify-write (`APPEND`, `INCR`, `HSET`,
//! `SETRANGE`, …) logged after the key's `SET … PXAT`/`PEXPIREAT` then
//! replayed onto an absent key and built a NEW key with no TTL: it came back
//! after the restart with a wrong value and never expired.
//!
//! redis never expires a key while loading (`keyIsExpired` returns 0 during
//! loading) — which is exact there because redis logs a `DEL` for every
//! expiry a command observes, BEFORE that command. moon defers that `DEL`
//! (moon#542: a lazily-hidden key is deleted, and its `DEL` logged, by the
//! next active-expiry tick, or never if a write replaced it first), so a
//! write that observed a just-expired key live is logged with no `DEL` ahead
//! of it; suppressing expiry during replay would replay it onto the OLD value
//! (an `INCR` right after a rate-limit window ends continuing the old count,
//! a `SET … NX` re-acquiring a lock refused).
//!
//! So the judgment clock is pinned instead to the moment the log was last
//! written — the newest modification time of the files being replayed,
//! capped at the wall clock. Every replayed record was written no later than
//! that, so:
//! - a key whose deadline is after it was alive for every record: judged
//!   alive (the moon#1277 case — every record replays onto the value it was
//!   written against); the active expiry reaps it once the server runs;
//! - a key whose deadline is at or before it is judged expired, exactly as
//!   before this change (and its expiry `DEL`, logged by the active expiry
//!   shortly after the deadline, is in the log too).
//!
//! With a truthful modification time it is never worse than the wall clock:
//! the two differ only for deadlines that fell while the server was down, and
//! for those the pinned clock is the right one.
//!
//! No deadline is COMPUTED from the pinned clock by a current log: every
//! relative expiry is logged as an absolute deadline — `EXPIRE`, `PEXPIRE`,
//! `SETEX`, `PSETEX`, `SET … EX|PX`, `GETEX … EX|PX` as `PEXPIREAT` /
//! `SET … PXAT` (`replication::expire_rewrite`), `HEXPIRE` / `HPEXPIRE` /
//! `HGETEX … EX|PX` as `HPEXPIREAT` (`replication::effect_rewrite`). Only a
//! log written before those rewrites (a verbatim `HEXPIRE`) has its relative
//! deadlines re-based, to the log's time instead of the replay's.
//!
//! The modification time is trusted, and it can lie:
//! - LATER than the last write (a copied or restored file, `touch`): the
//!   judgment drifts towards the wall clock, the old behaviour (moon#1277's
//!   symptom can come back); never past the wall clock.
//! - EARLIER than the last write (the clock stepped back after the write, a
//!   network or virtio filesystem whose server clock lags, `touch -d`): the
//!   judgment predates records, so keys that expired while the server RAN
//!   read alive to them — the replay behaves like expiry suppression, and a
//!   key lazily expired and rewritten before its `DEL` was logged (moon#542)
//!   replays onto its OLD value. REVIEW-FINAL-P5B measured 27-36 of 40 such
//!   keys wrong with the mtime set an hour back (main: 0 of 40).
//!
//! moon#1283 closes that hole with a time record in the log itself:
//! `MOON.TS <ms>` ([`super::pseudo`]), the shard clock each record was judged
//! under, which the writer emits whenever it changes. A replay that reads one
//! judges every later record of that file by the LAST stamp read
//! ([`observe_log_ts`]) — never a running maximum, since a parked producer's
//! record carries an older stamp than the records it lands after — and falls
//! back to the pin above only until the file's first stamp. A log written
//! before moon#1283 (or its stamp-less prefix) therefore replays exactly as
//! described above; a stamped one no longer depends on the mtime at all.
//! Each pin guard is one replayed file (a flat `appendonly.aof`, one incr, one
//! WAL directory): opening a guard starts with no stamp, closing it restores
//! the enclosing file's stamp, and a stamp read outside any guard (a live
//! apply) is ignored. A stamp is not capped at the wall clock: it is the
//! write-time clock by construction, and a clock stepped back since then is
//! exactly the case it exists for.
//!
//! Pinned by every production replay of a command log: `aof::replay_aof`
//! (the flat `appendonly.aof`), `aof_manifest::replay_multi_part` and
//! `replay_per_shard` (the incremental files; a base is an image and judges
//! nothing), the Phase 4 WAL pass of `recovery::recover_shard_v3_pitr`, and
//! the last-resort WAL replay (`wal_v3::replay::replay_wal_v3_dir_commands`).
//! Not pinned: `replay_ordered_merge` (no production emitter yet) and the
//! replica's apply of the master stream, which runs live on the wall clock.
//!
//! ## A segment another binary appended (R2 review of moon#1283)
//!
//! After a downgrade, an older binary appends to the same file with no
//! stamps; judged by the last stamp the newer binary wrote before the
//! downgrade — hours or days stale — a key the older binary saw expire and
//! rewrote (moon#542: no `DEL` first) replayed onto its old value and old
//! deadline, and was lost. The rule that catches those records is
//! POSITIONAL, not a guess from the file's timestamps:
//! - whenever this binary's writer stops in order it appends a clean-close
//!   marker, `MOON.TS <ms> CLOSE` ([`super::pseudo`]), made durable by the
//!   final sync;
//! - whenever it (re)opens a file it writes a stamp before its FIRST append
//!   (`aof::record_ctx`).
//!
//! So the records after a `CLOSE` and before the next stamp cannot be this
//! binary's: they are a foreign segment. At the `CLOSE` the replay scans
//! forward ([`super::log_segment`]) — only across that segment — for the
//! next stamp, and judges the segment's records by it: the later binary's
//! session stamp, never earlier than their real write time (late judgment is
//! how the older binary itself replays them, by the mtime). A segment that
//! runs to the end of the file is judged by `max(close stamp, mtime pin)`,
//! the moment it was last written; that judgment is reported
//! ([`take_open_foreign_segment`]) to the writer that reopens the file,
//! which writes it as its session stamp before its first append, so every
//! later boot judges the segment exactly as this one did. No rewrite is
//! needed: the rule holds wherever the segment ends up in the file.
//!
//! Unprotected, by construction: an older binary appending after an UNCLEAN
//! stop of this one (no `CLOSE`) — its records follow the last stamp and are
//! judged by it (`docs/STORAGE-FORMAT-V1.md` §3.3 gives the procedure).
//!
//! A snapshot the logs replay over (`KvSources::SnapshotAndLogs`) keeps its
//! expired entries ([`keep_expired_image_entries`]): its loader skips them on
//! the WALL clock, and a key dropped there would be absent for a record the
//! pinned clock judges it alive to — moon#1277 again, one layer down. Kept,
//! the replay judges them like any other key and the active expiry reaps the
//! rest, as for an AOF base (moon#1236).

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};

/// How a replayed log file frames its records (for the forward scan across a
/// foreign segment, [`super::log_segment`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LogFormat {
    /// Bare RESP: the flat `appendonly.aof` (after any RDB preamble) and the
    /// multi-part top-level incr.
    #[default]
    Resp,
    /// The per-shard incr: `[u64 lsn LE][u32 len LE][RESP]` per record.
    Framed,
}

/// The file a pin scope replays.
#[derive(Default)]
struct LogSource {
    /// The replayed log (None: no file, e.g. a WAL directory or a test pin).
    path: Option<PathBuf>,
    format: LogFormat,
    /// The judgment of a non-empty foreign segment that runs to the end of
    /// the file (0 = none), handed to the writer when the scope closes.
    open_segment_ms: u64,
}

/// Judgments of foreign segments that ran to the end of their file, by file
/// (see the module doc): set by a replay, taken by the writer that reopens
/// the file. Touched once per replayed file and once per writer session.
static OPEN_SEGMENTS: parking_lot::Mutex<Vec<(PathBuf, u64)>> =
    parking_lot::const_mutex(Vec::new());

/// The same file under the name a replay and a writer each built for it.
fn segment_key(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// The judgment this process's replay gave a foreign segment at the END of
/// `path` (see the module doc), if it met one: the writer that appends to
/// `path` next writes it as its first stamp. Taken once.
pub fn take_open_foreign_segment(path: &Path) -> Option<u64> {
    let key = segment_key(path);
    let mut open = OPEN_SEGMENTS.lock();
    let at = open.iter().position(|(p, _)| *p == key)?;
    Some(open.swap_remove(at).1)
}

fn report_open_segment(path: &Path, ms: u64) {
    let key = segment_key(path);
    let mut open = OPEN_SEGMENTS.lock();
    open.retain(|(p, _)| *p != key);
    open.push((key, ms));
}

thread_local! {
    /// The pinned judgment clock of this thread's replay (0 = not pinned:
    /// the databases keep their own clock).
    static PINNED_MS: Cell<u64> = const { Cell::new(0) };
    /// The last `MOON.TS` the replay of the current file read (0 = none yet:
    /// [`PINNED_MS`] rules). moon#1283.
    static LOG_TS_MS: Cell<u64> = const { Cell::new(0) };
    /// How many pin guards are open on this thread: a stamp is honoured only
    /// inside one.
    static PIN_DEPTH: Cell<u32> = const { Cell::new(0) };
    /// The judgment clock of the foreign segment the replay is in (0 = not in
    /// one). R2 review of moon#1283.
    static SEGMENT_MS: Cell<u64> = const { Cell::new(0) };
    /// The file offset just past the record being replayed ([`at_record_end`]).
    static RECORD_END: Cell<u64> = const { Cell::new(0) };
    /// The current file.
    static SRC: RefCell<LogSource> = RefCell::new(LogSource::default());
}

/// Restores the previous pin (or none) and the enclosing file's stamp when
/// dropped; reports a foreign segment left open at the end of its file.
#[must_use = "the pin lasts only while the guard lives"]
pub struct ReplayClockGuard {
    previous: u64,
    previous_ts: u64,
    previous_segment: u64,
    previous_end: u64,
    previous_src: LogSource,
}

impl Drop for ReplayClockGuard {
    fn drop(&mut self) {
        PINNED_MS.with(|c| c.set(self.previous));
        LOG_TS_MS.with(|c| c.set(self.previous_ts));
        SEGMENT_MS.with(|c| c.set(self.previous_segment));
        RECORD_END.with(|c| c.set(self.previous_end));
        let src = SRC.with(|c| c.replace(std::mem::take(&mut self.previous_src)));
        if let (Some(path), ms) = (src.path, src.open_segment_ms)
            && ms != 0
        {
            report_open_segment(&path, ms);
        }
        PIN_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Pin this thread's replay judgment clock to `ms` until the guard drops, and
/// open the scope of one replayed file: no `MOON.TS` read yet. `0` pins
/// nothing (the databases' own clock) but still opens the scope.
pub fn pin_replay_clock_ms(ms: u64) -> ReplayClockGuard {
    pin_scope(ms, LogSource::default())
}

fn pin_scope(ms: u64, src: LogSource) -> ReplayClockGuard {
    PIN_DEPTH.with(|c| c.set(c.get().saturating_add(1)));
    ReplayClockGuard {
        previous: PINNED_MS.with(|c| c.replace(ms)),
        previous_ts: LOG_TS_MS.with(|c| c.replace(0)),
        previous_segment: SEGMENT_MS.with(|c| c.replace(0)),
        previous_end: RECORD_END.with(|c| c.replace(0)),
        previous_src: SRC.with(|c| c.replace(src)),
    }
}

/// The record about to be replayed ends at file offset `offset`. Every log
/// reader that pins a file ([`pin_replay_clock_to_log`]) calls it before it
/// hands the record to the engine, so a `CLOSE` marker knows where the
/// segment after it starts. One thread-local store.
#[inline]
pub fn at_record_end(offset: u64) {
    RECORD_END.with(|c| c.set(offset));
}

/// A `MOON.TS <ms>` record was read (moon#1283): judge the records after it
/// by `ms` until the next stamp or the end of the file. Ignored outside a pin
/// scope (nothing is being replayed) and for `ms == 0`. Returns whether the
/// stamp was taken.
pub fn observe_log_ts(ms: u64) -> bool {
    if ms == 0 || PIN_DEPTH.with(Cell::get) == 0 {
        return false;
    }
    LOG_TS_MS.with(|c| c.set(ms));
    SEGMENT_MS.with(|c| c.set(0));
    true
}

/// A clean-close marker `MOON.TS <ms> CLOSE` was read (R2 review of
/// moon#1283): the records after it, up to the next stamp, are a foreign
/// segment (see the module doc). Scans forward for that stamp and judges the
/// segment by it — or, when the segment runs to the end of the file, by
/// `max(ms, pin)`, which is also reported to the writer that reopens the
/// file. Ignored outside a pin scope and for `ms == 0`. Returns whether the
/// marker was taken.
pub fn observe_close(ms: u64) -> bool {
    if ms == 0 || PIN_DEPTH.with(Cell::get) == 0 {
        return false;
    }
    LOG_TS_MS.with(|c| c.set(ms));
    let (path, format) = SRC.with(|c| {
        let src = c.borrow();
        (src.path.clone(), src.format)
    });
    let from = RECORD_END.with(Cell::get);
    let scan = match path.as_deref() {
        Some(p) => super::log_segment::scan_after_close(p, from, format),
        // No file to scan (a WAL directory, a test pin): no writer of this
        // binary writes a `CLOSE` there; judge conservatively, as at EOF.
        None => super::log_segment::SegmentScan {
            next_stamp_ms: None,
            records: 1,
        },
    };
    let judged = match (scan.records, scan.next_stamp_ms) {
        (0, _) => 0,
        (_, Some(next)) => next,
        (_, None) => {
            let at_eof = ms.max(PINNED_MS.with(Cell::get));
            SRC.with(|c| c.borrow_mut().open_segment_ms = at_eof);
            at_eof
        }
    };
    SEGMENT_MS.with(|c| c.set(judged));
    if judged != 0 {
        tracing::info!(
            "AOF replay: {} holds {} record(s) another binary appended after this binary \
             closed it cleanly at {} ms (a downgrade); they are judged by {} ms, {}",
            path.as_deref()
                .map_or_else(|| "a log".into(), |p| p.display().to_string()),
            scan.records,
            ms,
            judged,
            if scan.next_stamp_ms.is_some() {
                "the next stamp in the file"
            } else {
                "the time the file was last written (they run to its end)"
            },
        );
    }
    true
}

/// Pin this thread's replay judgment clock to the newest modification time of
/// `files` (those that exist), capped at the wall clock. With none readable
/// nothing is pinned (the wall clock, as before).
pub fn pin_replay_clock_to_files(files: &[&Path]) -> ReplayClockGuard {
    let newest = files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok()?.modified().ok())
        .filter_map(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .max()
        .map_or(0, |ms| ms.min(crate::storage::entry::current_time_ms()));
    pin_replay_clock_ms(newest)
}

/// Pin this thread's replay judgment clock to the modification time of the
/// AOF file `path` (see [`pin_replay_clock_to_files`]) and open its scope for
/// the positional foreign-segment rule (the module doc): its reader frames
/// records as `format` and reports each record's end ([`at_record_end`]).
pub fn pin_replay_clock_to_log(path: &Path, format: LogFormat) -> ReplayClockGuard {
    let guard = pin_replay_clock_to_files(&[path]);
    SRC.with(|c| {
        let mut src = c.borrow_mut();
        src.path = Some(path.to_path_buf());
        src.format = format;
    });
    guard
}

/// Pin this thread's replay judgment clock to the newest `*.wal` segment in
/// `wal_dir` (see [`pin_replay_clock_to_files`]).
pub fn pin_replay_clock_to_wal_dir(wal_dir: &Path) -> ReplayClockGuard {
    let segments: Vec<std::path::PathBuf> = std::fs::read_dir(wal_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "wal"))
                .collect()
        })
        .unwrap_or_default();
    let files: Vec<&Path> = segments.iter().map(std::path::PathBuf::as_path).collect();
    pin_replay_clock_to_files(&files)
}

/// Put back the entries a snapshot load skipped as expired
/// (`snapshot::shard_snapshot_load_noting_expired`), for a snapshot the KV
/// logs replay over (see the module doc), and empty `expired`. Booting is no
/// keyspace change (moon#1232), so none is counted.
pub fn keep_expired_image_entries(
    databases: &mut [crate::storage::Database],
    expired: &mut Vec<(usize, bytes::Bytes, crate::storage::entry::Entry)>,
) {
    let _quiet = crate::admin::metrics_setup::mute_keyspace_changes();
    for (db, key, entry) in expired.drain(..) {
        if let Some(db) = databases.get_mut(db) {
            db.set(&key, entry);
        }
    }
}

/// The file the innermost pin scope on this thread replays, if it pinned
/// one ([`pin_replay_clock_to_log`]). moon#1300: where a replay that ended
/// inside a `MOON.TXN` block reports it for the reopening writer.
pub fn current_log_path() -> Option<PathBuf> {
    if PIN_DEPTH.with(Cell::get) == 0 {
        return None;
    }
    SRC.with(|c| c.borrow().path.clone())
}

/// The judgment clock of this thread's replay, if any: a foreign segment's
/// judgment (the module doc), else the last `MOON.TS` of the current file,
/// else the pin.
#[inline]
pub fn pinned_replay_clock_ms() -> Option<u64> {
    let segment = SEGMENT_MS.with(Cell::get);
    if segment != 0 {
        return Some(segment);
    }
    let ts = LOG_TS_MS.with(Cell::get);
    if ts != 0 {
        return Some(ts);
    }
    PINNED_MS.with(|c| Some(c.get()).filter(|&ms| ms != 0))
}

/// Test access to the foreign-segment registry.
#[cfg(test)]
pub(crate) mod tests_support {
    /// As a replay does when a foreign segment runs to the end of `path`.
    pub(crate) fn report_open_segment(path: &std::path::Path, ms: u64) {
        super::report_open_segment(path, ms);
    }
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;
