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
//! ## A tail an older binary appended (R1 review of moon#1283, finding 1)
//!
//! After a downgrade, an older binary appends to the same file with no
//! stamps; after the re-upgrade those records would all be judged by the last
//! stamp the newer binary wrote before the downgrade — hours or days stale —
//! so a key the older binary saw expire and rewrote (moon#542: no `DEL`
//! first) replayed onto its old value and old deadline, and was lost.
//! This binary re-stamps whenever its clock moves, so the records after its
//! last stamp were written within that clock tick, and the file's mtime is
//! that tick (plus the writer's pickup latency). An mtime more than
//! [`FOREIGN_TAIL_TOLERANCE_MS`] past the file's LAST stamp therefore means
//! something else appended after it (or the mtime was moved forward). The
//! records that stamp covers — found by [`super::log_tail::last_ts_in_file`]
//! from the end of the file, matched by value — are then judged by
//! `max(last stamp, mtime pin)`, which is the pin: exactly the judgment the
//! older binary itself replays them with. A stamped file whose mtime is
//! EARLIER than its last stamp (the moon#1283 case) never engages the rule.
//! The replay reports it ([`foreign_tail_replayed`]) so boot runs one AOF
//! rewrite: once this binary appends stamped records behind that tail, it is
//! no longer at the end of the file, and only a new generation keeps a later
//! boot from judging it by the stale stamp again.
//!
//! A snapshot the logs replay over (`KvSources::SnapshotAndLogs`) keeps its
//! expired entries ([`keep_expired_image_entries`]): its loader skips them on
//! the WALL clock, and a key dropped there would be absent for a record the
//! pinned clock judges it alive to — moon#1277 again, one layer down. Kept,
//! the replay judges them like any other key and the active expiry reaps the
//! rest, as for an AOF base (moon#1236).

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// How far a file's mtime may pass its last `MOON.TS` before the records
/// that stamp covers are judged as a foreign tail (see the module doc). The
/// gap a writer of THIS binary leaves is its pickup latency — well under a
/// millisecond while it writes, longer only while it is stalled (a slow
/// disk under `always`); an older binary appending after a downgrade leaves
/// at least a restart.
pub const FOREIGN_TAIL_TOLERANCE_MS: u64 = 1_000;

/// Set once any replay in this process judged a foreign tail by its file's
/// mtime ([`foreign_tail_replayed`]).
static FOREIGN_TAIL_REPLAYED: AtomicBool = AtomicBool::new(false);

/// Whether a replay in this process met a tail an older binary appended after
/// the file's last `MOON.TS` (see the module doc). Boot then asks for one AOF
/// rewrite, so the tail stops being judged by that stale stamp once this
/// binary appends behind it.
pub fn foreign_tail_replayed() -> bool {
    FOREIGN_TAIL_REPLAYED.load(Ordering::Relaxed)
}

/// The file a pin scope replays, for the foreign-tail rule.
#[derive(Default)]
struct TailSource {
    /// The replayed log (None: no file, e.g. a WAL directory or a test pin).
    path: Option<PathBuf>,
    /// Its last `MOON.TS`, scanned lazily on first need: `None` = not yet
    /// scanned, `Some(None)` = it has none.
    last_ts: Option<Option<u64>>,
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
    /// The judgment clock of the records after the current file's LAST stamp
    /// when they are a foreign tail (0 = not in one). R1 review, finding 1.
    static TAIL_MS: Cell<u64> = const { Cell::new(0) };
    /// The current file, for the foreign-tail rule.
    static TAIL_SRC: RefCell<TailSource> = RefCell::new(TailSource::default());
}

/// Restores the previous pin (or none) and the enclosing file's stamp when
/// dropped.
#[must_use = "the pin lasts only while the guard lives"]
pub struct ReplayClockGuard {
    previous: u64,
    previous_ts: u64,
    previous_tail: u64,
    previous_src: TailSource,
}

impl Drop for ReplayClockGuard {
    fn drop(&mut self) {
        PINNED_MS.with(|c| c.set(self.previous));
        LOG_TS_MS.with(|c| c.set(self.previous_ts));
        TAIL_MS.with(|c| c.set(self.previous_tail));
        TAIL_SRC.with(|c| *c.borrow_mut() = std::mem::take(&mut self.previous_src));
        PIN_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Pin this thread's replay judgment clock to `ms` until the guard drops, and
/// open the scope of one replayed file: no `MOON.TS` read yet. `0` pins
/// nothing (the databases' own clock) but still opens the scope.
pub fn pin_replay_clock_ms(ms: u64) -> ReplayClockGuard {
    pin_scope(ms, None)
}

fn pin_scope(ms: u64, path: Option<PathBuf>) -> ReplayClockGuard {
    PIN_DEPTH.with(|c| c.set(c.get().saturating_add(1)));
    ReplayClockGuard {
        previous: PINNED_MS.with(|c| c.replace(ms)),
        previous_ts: LOG_TS_MS.with(|c| c.replace(0)),
        previous_tail: TAIL_MS.with(|c| c.replace(0)),
        previous_src: TAIL_SRC.with(|c| {
            c.replace(TailSource {
                path,
                last_ts: None,
            })
        }),
    }
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
    TAIL_MS.with(|c| c.set(foreign_tail_clock(ms)));
    true
}

/// The clock for the records after the stamp `ms` when `ms` is the current
/// file's LAST stamp and the file's mtime pin is more than
/// [`FOREIGN_TAIL_TOLERANCE_MS`] past it (a foreign tail, see the module
/// doc): the pin. 0 otherwise. The file is scanned at most once per scope,
/// and only once a stamp trails the pin by more than the tolerance.
fn foreign_tail_clock(ms: u64) -> u64 {
    let pin = PINNED_MS.with(Cell::get);
    if pin <= ms.saturating_add(FOREIGN_TAIL_TOLERANCE_MS) {
        return 0;
    }
    let last = TAIL_SRC.with(|c| {
        let mut src = c.borrow_mut();
        if src.last_ts.is_none() {
            let scanned = src
                .path
                .as_deref()
                .and_then(super::log_tail::last_ts_in_file);
            src.last_ts = Some(scanned);
        }
        src.last_ts.flatten()
    });
    if last != Some(ms) {
        return 0;
    }
    if !FOREIGN_TAIL_REPLAYED.swap(true, Ordering::Relaxed) {
        let path = TAIL_SRC.with(|c| c.borrow().path.clone());
        tracing::warn!(
            "AOF replay: {} was appended to {:.1}s after its last MOON.TS; the records \
             after that stamp have no stamp of their own (an older moon binary wrote \
             them after a downgrade, or the file's mtime was moved forward) and are \
             judged by the file's mtime instead of that stale stamp. One AOF rewrite \
             runs after boot so later boots do not depend on it.",
            path.as_deref()
                .map_or_else(|| "a log".into(), |p| p.display().to_string()),
            (pin - ms) as f64 / 1000.0,
        );
    }
    pin
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
/// the foreign-tail rule (the module doc): the records after the file's last
/// `MOON.TS` are judged by the mtime when it is more than
/// [`FOREIGN_TAIL_TOLERANCE_MS`] later than that stamp.
pub fn pin_replay_clock_to_log(path: &Path) -> ReplayClockGuard {
    let guard = pin_replay_clock_to_files(&[path]);
    TAIL_SRC.with(|c| c.borrow_mut().path = Some(path.to_path_buf()));
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

/// The judgment clock of this thread's replay, if any: the mtime pin past
/// a foreign tail's cut (the module doc), else the last `MOON.TS` of the
/// current file, else the pin.
#[inline]
pub fn pinned_replay_clock_ms() -> Option<u64> {
    let tail = TAIL_MS.with(Cell::get);
    if tail != 0 {
        return Some(tail);
    }
    let ts = LOG_TS_MS.with(Cell::get);
    if ts != 0 {
        return Some(ts);
    }
    PINNED_MS.with(|c| Some(c.get()).filter(|&ms| ms != 0))
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;
