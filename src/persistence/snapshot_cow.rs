//! Deferred copy-on-write pre-image capture for writes that happen OFF the
//! shard event loop's own stack (moon#517).
//!
//! # Why this exists
//!
//! The incremental snapshot ([`crate::persistence::snapshot::SnapshotState`])
//! is a fuzzy, segment-at-a-time serializer: the shard event loop writes one
//! segment per tick while the shard keeps serving traffic. Correctness comes
//! from COW — a key overwritten BEFORE its segment has been serialized must
//! have its epoch-start value stashed in the snapshot's overflow buffer, or
//! recovery double-applies the WAL record that replays on top of the
//! already-new value (`INCR` is the classic case: snapshot holds 2, WAL
//! replays the INCR that produced it, key ends at 3).
//!
//! `spsc_handler::cow_intercept` does that capture inline — but it can only
//! be called where the `&mut Option<SnapshotState>` is in scope, i.e. from
//! the shard event loop's own stack. A **Lua script** runs on a connection
//! task (or inside the routed `ShardMessage::Execute` arm) and issues its
//! writes from `scripting::bridge`, which has no access to that state.
//! Worse, `EVAL <script> <n> k` names its keys by position only — which of
//! them the script WRITES is known only per `redis.call` — so wrapping the
//! `handle_eval` call sites in `cow_intercept` could not capture the right
//! keys (it used to key off `command[1]`, the script body).
//!
//! # Design: capture eagerly, filter at the drain
//!
//! The pre-image must be taken at mutation time (only then is the old value
//! still there), but the "is this segment still pending?" question can be
//! answered later — because ONLY the event loop's per-tick
//! `advance_snapshot_segment` ever marks a segment serialized. So:
//!
//! 1. A snapshot starting on this shard [`arm`]s a thread-local queue.
//! 2. Every off-loop write path calls [`capture_command_pre_image`] before
//!    it mutates. Armed: clone the old entry into the queue (first write to
//!    a key wins — later ones would overwrite the epoch-start value).
//!    Disarmed (the overwhelmingly common case, no BGSAVE in flight): one
//!    thread-local `bool` load and return.
//! 3. The next tick [`drain_into`]s the queue into the live `SnapshotState`
//!    **before** advancing another segment, dropping entries whose segment
//!    was already written (their pre-image is already in the file).
//!
//! One shard per OS thread, and every producer/consumer here runs on that
//! shard's thread, so a `thread_local!` IS the per-shard queue — same
//! pattern as [`crate::shard::slice`] and [`crate::shard::self_msg`]. A
//! non-shard thread is never armed, so it can never accumulate a queue that
//! nobody drains.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;

use bytes::Bytes;

use crate::persistence::snapshot::{PreImage, SnapshotState};
use crate::protocol::Frame;
use crate::storage::db::Database;
#[cfg(test)]
use crate::storage::entry::Entry;

thread_local! {
    /// Is a snapshot in flight on this shard? The whole capture path is one
    /// `Cell<bool>` load when it is not.
    static ARMED: Cell<bool> = const { Cell::new(false) };
    /// Pre-images captured since the last drain: `(db_index, key, state)`,
    /// where `state` is the key's entry or `None` if it did not exist
    /// (moon#1216: absence is part of the epoch-start keyspace too).
    static PENDING: RefCell<Vec<(usize, Bytes, PreImage)>> = const { RefCell::new(Vec::new()) };
    /// First-wins dedupe set: every key whose pre-image was captured this
    /// EPOCH. Held for the whole snapshot (moon#1186) — it used to be
    /// cleared on every drain, so a hot key was deep-cloned again on every
    /// tick only for `SnapshotState::capture_cow` to discard the copy.
    static PENDING_KEYS: RefCell<HashSet<(usize, Bytes)>> =
        RefCell::new(HashSet::new());
    /// Serialization progress of the armed snapshot (moon#1186): lets
    /// `capture_key` skip keys whose range is already written, whose
    /// pre-image the drain would drop anyway. `None` = unknown (capture
    /// everything, the pre-moon#1186 behaviour).
    static PROGRESS: RefCell<Option<Progress>> = const { RefCell::new(None) };
    /// A whole-table change the armed epoch cannot follow (moon#1224): a
    /// FLUSHDB / FLUSHALL / SWAPDB that hit a database the epoch has not
    /// finished. The next drain fails the snapshot with this reason.
    static ABORT: Cell<Option<&'static str>> = const { Cell::new(None) };
}

/// Mirror of `SnapshotState`'s serialization cursor, so a capture can
/// answer `is_hash_pending` without the state in scope.
struct Progress {
    current_db: usize,
    /// Hash-space position within `current_db` (moon#1216).
    cursor: u64,
    num_databases: usize,
}

impl Progress {
    /// Exactly `SnapshotState::is_hash_pending`. A mirror that lags the
    /// state (a cursor published late) only answers "pending" for more keys,
    /// which costs a clone the drain then drops — never a missed pre-image.
    fn is_pending(&self, db_index: usize, hash: u64) -> bool {
        db_index < self.num_databases
            && (db_index > self.current_db || (db_index == self.current_db && hash >= self.cursor))
    }

    /// Is any of `db_index`'s epoch-start contents still to be written?
    fn is_unfinished(&self, db_index: usize) -> bool {
        db_index < self.num_databases && db_index >= self.current_db
    }
}

/// Arm capture for a snapshot that just began on this shard.
pub(crate) fn arm() {
    clear();
    ARMED.with(|a| a.set(true));
}

/// [`arm`] with the snapshot's epoch-start layout, so captures for keys
/// whose range is already written are skipped at the source (moon#1186).
/// Only the number of databases matters since moon#1216: progress is a
/// position in hash space that starts at database 0, hash 0.
pub(crate) fn arm_with_layout(segment_counts: Vec<usize>) {
    arm();
    PROGRESS.with(|p| {
        *p.borrow_mut() = Some(Progress {
            current_db: 0,
            cursor: 0,
            num_databases: segment_counts.len(),
        })
    });
}

/// Publish the snapshot's cursor after a segment advance (moon#1186). Must
/// be called AFTER the tick's [`drain_into`] and advance, never between
/// them: a pre-image is filtered against the cursor it was captured under.
pub(crate) fn note_progress(current_db: usize, cursor: u64) {
    PROGRESS.with(|p| {
        if let Some(progress) = p.borrow_mut().as_mut() {
            progress.current_db = current_db;
            progress.cursor = cursor;
        }
    });
}

/// Disarm and drop anything still queued (the snapshot finished or failed —
/// the file is closed, so a pre-image has nowhere left to go).
pub(crate) fn disarm() {
    ARMED.with(|a| a.set(false));
    clear();
}

/// True while a snapshot on this shard wants pre-images.
#[inline]
pub(crate) fn is_armed() -> bool {
    ARMED.with(|a| a.get())
}

/// Test-only view of the queue, for suites that exercise a write path end
/// to end (e.g. a Lua script) and need to assert the pre-image was taken
/// without standing up a whole shard event loop to drain it. Lists the
/// captured ENTRIES only; see [`pending_tombstones_for_test`] for keys
/// captured as absent.
#[cfg(test)]
pub(crate) fn pending_for_test() -> Vec<(usize, Bytes, Entry)> {
    PENDING.with(|p| {
        p.borrow()
            .iter()
            .filter_map(|(db, k, e)| e.as_ref().map(|e| (*db, k.clone(), e.clone())))
            .collect()
    })
}

/// Test-only: keys captured as ABSENT at epoch start (tombstones).
#[cfg(test)]
pub(crate) fn pending_tombstones_for_test() -> Vec<(usize, Bytes)> {
    PENDING.with(|p| {
        p.borrow()
            .iter()
            .filter(|(_, _, e)| e.is_none())
            .map(|(db, k, _)| (*db, k.clone()))
            .collect()
    })
}

/// Test-only [`drain_into`] without a shard slice: what the persistence
/// tick does before every advance.
#[cfg(test)]
pub(crate) fn drain_pending_for_test(snap: &mut SnapshotState) {
    apply_queued_abort(snap);
    let captured = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
    drain_captured(snap, captured);
}

/// Test-only: the abort a structural change queued for the next drain.
#[cfg(test)]
pub(crate) fn abort_pending_for_test() -> Option<&'static str> {
    ABORT.with(Cell::get)
}

fn clear() {
    PENDING.with(|p| p.borrow_mut().clear());
    PENDING_KEYS.with(|k| k.borrow_mut().clear());
    PROGRESS.with(|p| *p.borrow_mut() = None);
    ABORT.with(|a| a.set(None));
}

/// Would the armed epoch still write any of `db_index`? `None` = every
/// database. Without a published layout the answer is a conservative yes.
fn epoch_unfinished(db_index: Option<usize>) -> bool {
    PROGRESS.with(|p| match p.borrow().as_ref() {
        None => true,
        Some(progress) => match db_index {
            Some(db) => progress.is_unfinished(db),
            None => progress.current_db < progress.num_databases,
        },
    })
}

/// Queue the failure of the armed epoch (moon#1224): the next drain aborts
/// it, before another segment is written.
fn abort_epoch(why: &'static str) {
    ABORT.with(|a| {
        if a.get().is_none() {
            a.set(Some(why));
        }
    });
}

/// `SWAPDB a b` is about to exchange two databases' tables on this shard
/// (moon#1224). Called from `ShardDbSet::swap`, the one place every SWAPDB
/// path — the coordinator's local leg, the SPSC arm, replica apply —
/// exchanges them.
///
/// An epoch that had not finished with `a` or `b` would write the rest of
/// one database's contents under the other's index (and lose the other's
/// entirely), so it is aborted: the BGSAVE fails loudly and the previous
/// snapshot file stays. A swap of two databases the epoch already wrote is
/// harmless — the file holds their epoch-start contents and the logged
/// SWAPDB replays on top. One thread-local `bool` load when nothing is
/// armed.
pub(crate) fn note_swapdb(a: usize, b: usize) {
    if !is_armed() || a == b {
        return;
    }
    if epoch_unfinished(Some(a)) || epoch_unfinished(Some(b)) {
        abort_epoch("SWAPDB exchanged a database the snapshot had not finished writing");
    }
}

/// A FLUSHDB / FLUSHALL is about to run against `db` (`databases[db_index]`)
/// while an epoch is armed (moon#1224). `Database::clear` replaces the whole
/// table, so the epoch-start contents of an unfinished database are gone
/// before the epoch wrote them: the snapshot is aborted — redis's own answer
/// to `FLUSHALL` during a `BGSAVE` is to kill the child. Nothing happens
/// when the flushed databases are already written (the logged FLUSH replays
/// on top of their epoch-start contents), when a FLUSHDB empties an already
/// EMPTY table (the file's contents for it are its pre-images, untouched),
/// or when the command will refuse its arguments and flush nothing.
fn note_flush(db: &Database, db_index: usize, all: bool, args: &[Frame]) {
    if !flush_args_accepted(args) {
        return;
    }
    if all {
        if epoch_unfinished(None) {
            abort_epoch("FLUSHALL cleared databases the snapshot had not finished writing");
        }
    } else if epoch_unfinished(Some(db_index)) && !db.data().is_empty() {
        abort_epoch("FLUSHDB cleared a database the snapshot had not finished writing");
    }
}

/// Exactly `command::server_admin`'s FLUSHDB/FLUSHALL argument check: no
/// argument, or one of `ASYNC` / `SYNC`. Anything else is refused before
/// `Database::clear` runs (pinned by `table_swap_tests::a_refused_flush_does_not_abort`).
fn flush_args_accepted(args: &[Frame]) -> bool {
    match args {
        [] => true,
        [only] => crate::command::helpers::extract_bytes(only)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"ASYNC") || s.eq_ignore_ascii_case(b"SYNC")),
        _ => false,
    }
}

/// Capture the pre-images of the keys a script's `redis.call(cmd, args..)`
/// is about to write.
///
/// `cmd_and_args[0]` is the command name. Every WRITTEN key position is
/// captured (moon#1217), through the same walker as
/// [`capture_dispatch_pre_image`] — a script's writes get exactly the
/// fidelity the generic path has.
///
/// Costs one thread-local `bool` load when no snapshot is in flight.
#[inline]
pub(crate) fn capture_command_pre_image(db: &Database, db_index: usize, cmd_and_args: &[Frame]) {
    if !is_armed() {
        return;
    }
    let Some((Frame::BulkString(cmd), args)) = cmd_and_args.split_first() else {
        return;
    };
    if !crate::command::metadata::is_write(cmd) {
        return;
    }
    capture_written_keys(db, db_index, cmd, args);
}

/// Capture the pre-image for a generic command about to run against `db`
/// (moon#558).
///
/// This is the choke point for every write that executes on the shard's own
/// stack instead of the event loop's: [`crate::command::dispatch`] is what
/// the monoio local arm, the tokio sharded local arm, `handler_single`, both
/// MULTI/EXEC executors, the coordinator's scatter arms and the SPSC drain
/// all funnel through. `spsc_handler::cow_intercept` (the routed arms)
/// captures through this same function, so every capture of an epoch lands
/// in ONE queue with ONE first-wins dedupe set, in the order the writes ran.
///
/// Cost when no snapshot is in flight — the overwhelmingly common case — is
/// one thread-local `bool` load; the `is_write` PHF lookup and the key
/// extraction are behind that gate.
///
/// Invariant: `db` MUST be `databases[db_index]` on the shard that armed the
/// capture — the drain files the pre-image under `db_index`, so a mismatched
/// pair would file it against the wrong database. Every live caller
/// satisfies it (`dispatch` is always handed `databases[*selected_db]`).
/// The one structural exception, `conn::shared::execute_transaction`, holds
/// a lock on the ENTRY db while `*selected_db` can be moved by a `SELECT`
/// queued inside the same MULTI — that executor belongs to `handler_single`,
/// which is not wired into the shipped server and runs no shard event loop,
/// so it can never be armed.
///
/// Every key position the command may WRITE is captured (moon#1217) — the
/// destination of `LMOVE`/`SMOVE`/`RENAME`/`COPY`/`SORT ... STORE`,
/// `k2..kN` of `MSET`/`DEL`, the non-first keys of `LMPOP`/`ZMPOP` —
/// not just `command[1]`: otherwise the file mixes pre- and post-epoch
/// states of the keys one atomic command touched.
#[inline]
pub(crate) fn capture_dispatch_pre_image(
    db: &Database,
    db_index: usize,
    cmd: &[u8],
    args: &[Frame],
) {
    if !is_armed() {
        return;
    }
    // moon#1224: the whole-table writes. Every FLUSHDB / FLUSHALL on a shard
    // — client, MULTI/EXEC, script, routed, replicated — runs through
    // `dispatch`; FLUSHALL's other databases are cleared right after by
    // `flush_every_database`, on the same shard.
    if cmd.eq_ignore_ascii_case(b"FLUSHDB") || cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        note_flush(db, db_index, cmd.len() == 8, args);
        return;
    }
    if !crate::command::metadata::is_write(cmd) {
        return;
    }
    capture_written_keys(db, db_index, cmd, args);
}

/// Capture every key position `cmd args..` may WRITE (moon#1217).
///
/// The positions come from the one key walker every consumer shares
/// (`acl::keyspec::command_key_positions`, which also drives blocking
/// wake-ups and client-tracking invalidation): its `Write` role is
/// deliberately over-inclusive where the argv cannot say (`LMPOP 2 a b`
/// writes whichever is non-empty, so both are `Write`) — for a snapshot that
/// costs at most a clone, while a missed key would be a post-epoch value in
/// the file. An argv the walker cannot enumerate falls back to the primary
/// key, the pre-moon#1217 contract.
fn capture_written_keys(db: &Database, db_index: usize, cmd: &[u8], args: &[Frame]) {
    use crate::acl::keyspec::{KeyPositions, KeyRole, command_key_positions};
    match command_key_positions(cmd, args) {
        KeyPositions::At(positions) | KeyPositions::AtPlusComputed(positions) => {
            for at in positions.iter().filter(|at| at.role == KeyRole::Write) {
                if let Some(key) = args
                    .get(at.idx)
                    .and_then(crate::command::helpers::extract_bytes)
                {
                    capture_key(db, db_index, key);
                }
            }
        }
        KeyPositions::None => {}
        KeyPositions::Unknown => {
            if let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, args) {
                capture_key(db, db_index, key);
            }
        }
    }
}

/// Capture the pre-image for a write whose key is already parsed — the
/// monoio inline fast path (`server::conn::blocking::try_inline_dispatch`),
/// which frames a plain `SET` straight from the read buffer and never builds
/// a `Frame` or enters [`crate::command::dispatch`] at all (moon#558).
///
/// `cfg`-gated to match its sole caller — the inline fast path only exists
/// under the monoio runtime.
#[cfg(feature = "runtime-monoio")]
#[inline]
pub(crate) fn capture_key_pre_image(db: &Database, db_index: usize, key: &Bytes) {
    if !is_armed() {
        return;
    }
    capture_key(db, db_index, key);
}

/// Out-of-line slow path: record the key's current state, first write wins.
fn capture_key(db: &Database, db_index: usize, key: &Bytes) {
    // moon#1186: a key whose range is already written needs no pre-image —
    // the file holds its epoch-start bytes and the drain would drop the copy.
    // Skip it BEFORE the deep clone.
    let written = PROGRESS.with(|p| {
        p.borrow().as_ref().is_some_and(|progress| {
            !progress.is_pending(db_index, crate::storage::dashtable::hash_key(key))
        })
    });
    if written {
        return;
    }
    if PENDING_KEYS.with(|k| k.borrow().contains(&(db_index, key.clone()))) {
        // Already captured this epoch — the FIRST pre-image is the
        // epoch-start state; a later one would be a state the snapshot must
        // not contain.
        return;
    }
    // A key that does not exist yet is captured too, as a TOMBSTONE
    // (moon#1216): its epoch-start state is "absent", and without the
    // tombstone the serializer would write the entry this write is about to
    // create. The key is copied so the capture never pins the connection's
    // read buffer (`key` is usually a slice of it) for the rest of the epoch.
    let pre_image: PreImage = db.data().get(key).cloned();
    let owned = Bytes::copy_from_slice(key);
    PENDING_KEYS.with(|k| k.borrow_mut().insert((db_index, owned.clone())));
    PENDING.with(|p| p.borrow_mut().push((db_index, owned, pre_image)));
}

/// Fold everything captured since the last drain into `snap`, dropping
/// pre-images whose range was already serialized.
///
/// MUST run before the tick advances another segment, otherwise a pre-image
/// captured while its range was still pending would be filtered out by a
/// cursor that moved past it in the meantime.
pub(crate) fn drain_into(snap: &mut SnapshotState) {
    apply_queued_abort(snap);
    if PENDING.with(|p| p.borrow().is_empty()) {
        return;
    }
    let captured: Vec<(usize, Bytes, PreImage)> =
        PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
    // PENDING_KEYS is NOT reset here (moon#1186): first-wins holds for the
    // whole epoch, so a hot key is cloned once, not once per tick.
    drain_captured(snap, captured);
}

/// Fail the snapshot if a structural change queued an abort (moon#1224).
/// Runs first in every drain, so no segment is written after the change.
fn apply_queued_abort(snap: &mut SnapshotState) {
    if let Some(why) = ABORT.with(|a| a.take()) {
        snap.abort(why);
    }
}

/// Core of [`drain_into`]. Needs no database: whether a key's range is still
/// pending is a function of its hash and the cursor alone (moon#1216), so a
/// split between the capture and this drain — which moves the key to a new
/// segment — cannot misfile or drop its pre-image.
fn drain_captured(snap: &mut SnapshotState, captured: Vec<(usize, Bytes, PreImage)>) {
    for (db_index, key, pre_image) in captured {
        snap.capture_cow(db_index, key, pre_image);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::snapshot::shard_snapshot_load;
    use crate::storage::compact_value::RedisValueRef;

    fn armed_guard() {
        // Tests run in parallel THREADS; every piece of state here is
        // thread-local, so each test owns its own queue.
        arm();
    }

    /// The disarmed path must never queue anything — this is the invariant
    /// that keeps the capture free on the hot path when no BGSAVE runs.
    #[test]
    fn capture_is_inert_when_disarmed() {
        disarm();
        let mut db = Database::new();
        db.set_string(b"k", Bytes::from_static(b"old"));
        let cmd = [
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"k")),
        ];
        capture_command_pre_image(&db, 0, &cmd);
        assert!(PENDING.with(|p| p.borrow().is_empty()));
    }

    /// First write wins: a script that writes the same key twice inside one
    /// snapshot epoch must stash the EPOCH-START value, not the value the
    /// first write produced.
    #[test]
    fn capture_keeps_the_first_pre_image_only() {
        armed_guard();
        let mut db = Database::new();
        db.set_string(b"k", Bytes::from_static(b"v0"));
        let cmd = [
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"k")),
        ];
        capture_command_pre_image(&db, 0, &cmd);
        db.set_string(b"k", Bytes::from_static(b"v1"));
        capture_command_pre_image(&db, 0, &cmd);

        let captured = pending_for_test();
        assert_eq!(captured.len(), 1, "second write must not re-capture");
        match captured[0].2.value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(s as &[u8], b"v0"),
            _ => panic!("expected a string entry"),
        }
        disarm();
    }

    /// moon#1216: a write that CREATES a key captures its epoch-start state
    /// too — "absent" — so the serializer does not write the new entry.
    /// A second write of the same key (now present) must not replace it.
    #[test]
    fn capture_records_absence_for_a_key_created_during_the_epoch() {
        armed_guard();
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = [Frame::BulkString(Bytes::from_static(b"fresh"))];
        let _ = crate::command::dispatch(&mut db, b"INCR", &args, &mut selected, 16);
        let _ = crate::command::dispatch(&mut db, b"INCR", &args, &mut selected, 16);
        let tombstones = pending_tombstones_for_test();
        let entries = pending_for_test();
        disarm();
        assert_eq!(tombstones, vec![(0, Bytes::from_static(b"fresh"))]);
        assert!(entries.is_empty(), "the second write must not re-capture");
    }

    /// End-to-end drain semantics: a pre-image captured off-loop must land
    /// in the snapshot file, and a pre-image for an ALREADY-serialized
    /// segment must be dropped (the file already holds that segment's
    /// epoch-start bytes; re-adding it would append a duplicate record that
    /// load-order would resolve to the wrong value).
    #[test]
    fn drain_folds_pending_segments_and_drops_serialized_ones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard-0.rrdshard");
        let mut dbs = vec![Database::new()];
        for i in 0..100 {
            dbs[0].set_string(
                &Bytes::from(format!("cow_{:04}", i)),
                Bytes::from(format!("val_{:04}", i)),
            );
        }
        assert!(
            dbs[0].data().segment_count() > 1,
            "fixture needs multiple segments"
        );

        let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
        // Serialize segment 0 — anything in it is already on disk.
        assert!(!state.advance_one_segment(&dbs));

        let seg0_key = dbs[0]
            .data()
            .segment(0)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes();
        let seg1_key = dbs[0]
            .data()
            .segment(1)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes();

        let captured = vec![
            (
                0,
                seg0_key.clone(),
                Some(dbs[0].data().get(&seg0_key).unwrap().clone()),
            ),
            (
                0,
                seg1_key.clone(),
                Some(dbs[0].data().get(&seg1_key).unwrap().clone()),
            ),
        ];
        drain_captured(&mut state, captured);

        // Both keys are overwritten AFTER the capture, exactly as a script
        // write would have done.
        dbs[0].set_string(&seg0_key, Bytes::from_static(b"NEW_VALUE"));
        dbs[0].set_string(&seg1_key, Bytes::from_static(b"NEW_VALUE"));

        while !state.advance_one_segment(&dbs) {}
        state.finalize().unwrap();

        let mut loaded = vec![Database::new()];
        shard_snapshot_load(&mut loaded, &path).unwrap();

        // Segment 1 was still pending at capture time -> old value survives.
        match loaded[0].get(&seg1_key).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_ne!(
                s as &[u8], b"NEW_VALUE",
                "pending-segment pre-image must reach the snapshot"
            ),
            _ => panic!("expected a string entry"),
        }
        // Segment 0 was already serialized before the capture was drained;
        // its epoch-start bytes are in the file from the first advance.
        match loaded[0].get(&seg0_key).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_ne!(
                s as &[u8], b"NEW_VALUE",
                "already-serialized segment keeps its epoch-start bytes"
            ),
            _ => panic!("expected a string entry"),
        }
    }

    /// moon#558: `spsc_handler::cow_intercept` only runs on the ROUTED /
    /// queued arms. An ordinary LOCAL write — every write at `--shards 1`,
    /// and the same-shard fraction at `--shards N` — reaches the database
    /// through `command::dispatch` called straight from the connection task,
    /// with no `&mut Option<SnapshotState>` anywhere in scope.
    ///
    /// RED before the fix: `pending` is empty, so the snapshot serializes the
    /// POST-`INCR` value while the WAL still holds the `INCR` — recovery
    /// double-applies it.
    #[test]
    fn generic_dispatch_captures_local_write_pre_image() {
        disarm();
        let mut db = Database::new();
        db.set_string(b"n", Bytes::from_static(b"1"));
        arm();
        let mut selected = 0usize;
        let args = [Frame::BulkString(Bytes::from_static(b"n"))];
        let _ = crate::command::dispatch(&mut db, b"INCR", &args, &mut selected, 16);
        let pending = pending_for_test();
        disarm();

        assert_eq!(
            pending.len(),
            1,
            "a LOCAL INCR during a snapshot must capture its pre-image"
        );
        assert_eq!(pending[0].0, 0, "captured under the executing db index");
        assert_eq!(pending[0].1.as_ref(), b"n");
        match pending[0].2.value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(
                s as &[u8], b"1",
                "the pre-image must be the EPOCH-START value, not the INCR result"
            ),
            _ => panic!("expected a string entry"),
        }
    }

    /// The capture is gated on `metadata::is_write`. A NON-idempotent write
    /// that the flag table does not mark `WRITE` would silently fall back
    /// through the gate and double-apply on replay — exactly the failure
    /// moon#558 fixes, just moved one layer down. Pin the whole family of
    /// read-modify-write commands (one per value type) so a flag-table edit
    /// cannot quietly reopen it.
    #[test]
    fn every_read_modify_write_command_passes_the_is_write_gate() {
        for (cmd, args) in [
            (&b"INCR"[..], vec![Bytes::from_static(b"n")]),
            (&b"DECR"[..], vec![Bytes::from_static(b"n")]),
            (
                &b"INCRBY"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"2")],
            ),
            (
                &b"INCRBYFLOAT"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"1.5")],
            ),
            (
                &b"APPEND"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"x")],
            ),
            (
                &b"SETRANGE"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"x"),
                ],
            ),
            (&b"GETDEL"[..], vec![Bytes::from_static(b"n")]),
            (
                &b"HINCRBY"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"f"),
                    Bytes::from_static(b"1"),
                ],
            ),
            (
                &b"LPUSH"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"v")],
            ),
            (
                &b"RPUSH"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"v")],
            ),
            (&b"LPOP"[..], vec![Bytes::from_static(b"n")]),
            (
                &b"ZINCRBY"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"1"),
                    Bytes::from_static(b"m"),
                ],
            ),
            (
                &b"SETBIT"[..],
                vec![
                    Bytes::from_static(b"n"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"1"),
                ],
            ),
            (
                &b"EXPIRE"[..],
                vec![Bytes::from_static(b"n"), Bytes::from_static(b"100")],
            ),
            (&b"PERSIST"[..], vec![Bytes::from_static(b"n")]),
            (&b"DEL"[..], vec![Bytes::from_static(b"n")]),
        ] {
            disarm();
            let mut db = Database::new();
            // A STRING pre-image is all this asserts on: the point is that the
            // gate LET THE COMMAND THROUGH, not that the command succeeded.
            db.set_string(b"n", Bytes::from_static(b"1"));
            arm();
            let frames: Vec<Frame> = args.into_iter().map(Frame::BulkString).collect();
            let mut selected = 0usize;
            let _ = crate::command::dispatch(&mut db, cmd, &frames, &mut selected, 16);
            let pending = pending_for_test();
            disarm();
            assert_eq!(
                pending.len(),
                1,
                "{} must capture a pre-image while a snapshot is armed",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    /// The choke point must stay inert for reads and for keyless commands —
    /// otherwise every GET on an armed shard pays a DashTable lookup plus a
    /// queue push for a key nothing is about to overwrite.
    #[test]
    fn generic_dispatch_does_not_capture_reads_or_keyless_commands() {
        disarm();
        let mut db = Database::new();
        db.set_string(b"n", Bytes::from_static(b"1"));
        arm();
        let mut selected = 0usize;
        let key = [Frame::BulkString(Bytes::from_static(b"n"))];
        let _ = crate::command::dispatch(&mut db, b"GET", &key, &mut selected, 16);
        let _ = crate::command::dispatch(&mut db, b"TTL", &key, &mut selected, 16);
        let _ = crate::command::dispatch(&mut db, b"PING", &[], &mut selected, 16);
        let pending = pending_for_test();
        disarm();
        assert!(
            pending.is_empty(),
            "reads and keyless commands must not queue pre-images, got {}",
            pending.len()
        );
    }

    /// End-to-end statement of the corruption moon#558 describes: a local
    /// `INCR` lands on a key whose segment has NOT been serialized yet, the
    /// snapshot then finishes, and the loaded file must hold the EPOCH-START
    /// value. If it holds the post-`INCR` value, WAL replay of that same
    /// `INCR` on top of the snapshot double-counts the key.
    ///
    /// RED before the fix: the loaded value is `2` (post-INCR), so recovery
    /// would land on `3`.
    #[test]
    fn local_incr_during_snapshot_does_not_double_apply_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard-0.rrdshard");
        let mut dbs = vec![Database::new()];
        for i in 0..100 {
            dbs[0].set_string(
                &Bytes::from(format!("cow_{:04}", i)),
                Bytes::from(format!("{}", i)),
            );
        }
        assert!(
            dbs[0].data().segment_count() > 1,
            "fixture needs multiple segments"
        );

        let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
        // Serialize segment 0 only; segment 1 is still pending.
        assert!(!state.advance_one_segment(&dbs));
        let victim = dbs[0]
            .data()
            .segment(1)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes();
        let epoch_start = match dbs[0].get(&victim).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => s.to_vec(),
            _ => panic!("expected a string entry"),
        };

        // The BGSAVE is in flight on this shard.
        disarm();
        arm();
        // ... and a connection task on the same thread runs a LOCAL INCR
        // between two `advance_snapshot_segment` ticks. This is exactly the
        // call the monoio/tokio local dispatch arms make.
        let mut selected = 0usize;
        let args = [Frame::BulkString(victim.clone())];
        let _ = crate::command::dispatch(&mut dbs[0], b"INCR", &args, &mut selected, 16);

        // Next tick: drain, then advance (the real ordering in
        // `shard::persistence_tick::advance_snapshot_segment`).
        drain_captured(&mut state, {
            let captured = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
            PENDING_KEYS.with(|k| k.borrow_mut().clear());
            captured
        });
        disarm();
        while !state.advance_one_segment(&dbs) {}
        state.finalize().unwrap();

        let mut loaded = vec![Database::new()];
        shard_snapshot_load(&mut loaded, &path).unwrap();
        match loaded[0].get(&victim).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(
                s as &[u8],
                &epoch_start[..],
                "snapshot must hold the epoch-start value; holding the post-INCR \
                 value double-counts when the WAL replays that INCR"
            ),
            _ => panic!("expected a string entry"),
        }
    }

    /// moon#1186: first-wins holds for the whole EPOCH. A hot key written on
    /// every tick used to be deep-cloned again after each drain (the dedupe
    /// set was cleared per drain) only for `capture_cow` to drop the copy.
    #[test]
    fn hot_key_is_cloned_once_per_epoch_not_once_per_tick() {
        let mut dbs = vec![Database::new()];
        for i in 0..100 {
            dbs[0].set_string(format!("hot_{i:03}").as_bytes(), Bytes::from_static(b"0"));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut state = SnapshotState::new(0, 1, &dbs, dir.path().join("s.rrdshard"));
        disarm();
        arm_with_layout(state.segment_counts().to_vec());
        let hot = Bytes::from_static(b"hot_050");
        let args = [Frame::BulkString(hot.clone())];
        for tick in 0..5 {
            let mut selected = 0usize;
            let _ = crate::command::dispatch(&mut dbs[0], b"INCR", &args, &mut selected, 16);
            let captured = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
            if tick == 0 {
                assert_eq!(
                    captured.len(),
                    1,
                    "first write captures the epoch-start value"
                );
            } else {
                assert!(
                    captured.is_empty(),
                    "tick {tick}: a key already captured this epoch was cloned again"
                );
            }
            drain_captured(&mut state, captured);
        }
        disarm();
    }

    /// moon#1186: a write to a key whose segment is already written needs no
    /// pre-image — it is skipped BEFORE the deep clone, not dropped by the
    /// drain after it.
    #[test]
    fn capture_skips_keys_in_already_written_segments() {
        let mut dbs = vec![Database::new()];
        for i in 0..200 {
            dbs[0].set_string(format!("w_{i:04}").as_bytes(), Bytes::from_static(b"1"));
        }
        let dir = tempfile::tempdir().unwrap();
        let mut state = SnapshotState::new(0, 1, &dbs, dir.path().join("s.rrdshard"));
        assert!(dbs[0].data().segment_count() > 2, "fixture needs segments");
        disarm();
        arm_with_layout(state.segment_counts().to_vec());
        assert!(!state.advance_one_segment(&dbs));
        note_progress(state.current_db_index(), state.cursor());

        let written = key_in(&dbs[0], 0);
        let pending = key_in(&dbs[0], 1);
        let mut selected = 0usize;
        let _ = crate::command::dispatch(
            &mut dbs[0],
            b"INCR",
            &[Frame::BulkString(written)],
            &mut selected,
            16,
        );
        assert!(
            pending_for_test().is_empty(),
            "a written segment's key must not be cloned"
        );
        let _ = crate::command::dispatch(
            &mut dbs[0],
            b"INCR",
            &[Frame::BulkString(pending.clone())],
            &mut selected,
            16,
        );
        let captured = pending_for_test();
        disarm();
        assert_eq!(
            captured.len(),
            1,
            "a pending segment's key is still captured"
        );
        assert_eq!(captured[0].1, pending);
    }

    fn key_in(db: &Database, seg: usize) -> Bytes {
        db.data()
            .segment(seg)
            .iter_occupied()
            .next()
            .unwrap()
            .0
            .to_bytes()
    }
}
