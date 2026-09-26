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
//! The durable fix is a time record in the log itself (redis's
//! `aof-timestamp-enabled`), a format change tracked as a follow-up.
//!
//! Pinned by every production replay of a command log: `aof::replay_aof`
//! (the flat `appendonly.aof`), `aof_manifest::replay_multi_part` and
//! `replay_per_shard` (the incremental files; a base is an image and judges
//! nothing), the Phase 4 WAL pass of `recovery::recover_shard_v3_pitr`, and
//! the last-resort WAL replay (`wal_v3::replay::replay_wal_v3_dir_commands`).
//! Not pinned: `replay_ordered_merge` (no production emitter yet) and the
//! replica's apply of the master stream, which runs live on the wall clock.
//!
//! A snapshot the logs replay over (`KvSources::SnapshotAndLogs`) keeps its
//! expired entries ([`keep_expired_image_entries`]): its loader skips them on
//! the WALL clock, and a key dropped there would be absent for a record the
//! pinned clock judges it alive to — moon#1277 again, one layer down. Kept,
//! the replay judges them like any other key and the active expiry reaps the
//! rest, as for an AOF base (moon#1236).

use std::cell::Cell;
use std::path::Path;

thread_local! {
    /// The pinned judgment clock of this thread's replay (0 = not pinned:
    /// the databases keep their own clock).
    static PINNED_MS: Cell<u64> = const { Cell::new(0) };
}

/// Restores the previous pin (or none) when dropped.
#[must_use = "the pin lasts only while the guard lives"]
pub struct ReplayClockGuard {
    previous: u64,
}

impl Drop for ReplayClockGuard {
    fn drop(&mut self) {
        PINNED_MS.with(|c| c.set(self.previous));
    }
}

/// Pin this thread's replay judgment clock to `ms` until the guard drops.
/// `0` unpins.
pub fn pin_replay_clock_ms(ms: u64) -> ReplayClockGuard {
    ReplayClockGuard {
        previous: PINNED_MS.with(|c| c.replace(ms)),
    }
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

/// The pinned judgment clock, if any.
#[inline]
pub fn pinned_replay_clock_ms() -> Option<u64> {
    PINNED_MS.with(|c| Some(c.get()).filter(|&ms| ms != 0))
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;
