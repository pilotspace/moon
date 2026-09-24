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
//! Worse, `cow_intercept` keys off `command[1]` — for `EVAL <script> <n> k`
//! that argument is the SCRIPT BODY, not a key, so even wrapping the
//! `handle_eval` call sites in it would capture the wrong thing.
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

use crate::persistence::snapshot::SnapshotState;
use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::entry::Entry;

thread_local! {
    /// Is a snapshot in flight on this shard? The whole capture path is one
    /// `Cell<bool>` load when it is not.
    static ARMED: Cell<bool> = const { Cell::new(false) };
    /// Pre-images captured since the last drain: `(db_index, key, old_entry)`.
    static PENDING: RefCell<Vec<(usize, Bytes, Entry)>> = const { RefCell::new(Vec::new()) };
    /// First-wins dedupe set: every key whose pre-image was captured this
    /// EPOCH. Held for the whole snapshot (moon#1186) — it used to be
    /// cleared on every drain, so a hot key was deep-cloned again on every
    /// tick only for `SnapshotState::capture_cow` to discard the copy.
    static PENDING_KEYS: RefCell<HashSet<(usize, Bytes)>> =
        RefCell::new(HashSet::new());
    /// Serialization progress of the armed snapshot (moon#1186): lets
    /// `capture_key` skip keys whose segment is already written, whose
    /// pre-image the drain would drop anyway. `None` = unknown (capture
    /// everything, the pre-moon#1186 behaviour).
    static PROGRESS: RefCell<Option<Progress>> = const { RefCell::new(None) };
}

/// Mirror of `SnapshotState`'s serialization cursor, so a capture can
/// answer `is_segment_pending` without the state in scope.
struct Progress {
    current_db: usize,
    current_segment: usize,
    /// Segment counts per db captured at epoch start.
    segment_counts: Vec<usize>,
}

impl Progress {
    /// Exactly `SnapshotState::is_segment_pending`: segments are written in
    /// order, so within the current db the written ones are those below
    /// `current_segment`; a segment created after epoch start (index past
    /// the captured count) is never written and never pending.
    fn is_pending(&self, db_index: usize, seg_idx: usize) -> bool {
        if db_index > self.current_db {
            return true;
        }
        if db_index < self.current_db {
            return false;
        }
        let count = self.segment_counts.get(db_index).copied().unwrap_or(0);
        seg_idx < count && seg_idx >= self.current_segment
    }
}

/// Arm capture for a snapshot that just began on this shard.
pub(crate) fn arm() {
    clear();
    ARMED.with(|a| a.set(true));
}

/// [`arm`] with the snapshot's epoch-start segment layout, so captures for
/// already-written segments are skipped at the source (moon#1186).
pub(crate) fn arm_with_layout(segment_counts: Vec<usize>) {
    arm();
    PROGRESS.with(|p| {
        *p.borrow_mut() = Some(Progress {
            current_db: 0,
            current_segment: 0,
            segment_counts,
        })
    });
}

/// Publish the snapshot's cursor after a segment advance (moon#1186). Must
/// be called AFTER the tick's [`drain_into`] and advance, never between
/// them: a pre-image is filtered against the cursor it was captured under.
pub(crate) fn note_progress(current_db: usize, current_segment: usize) {
    PROGRESS.with(|p| {
        if let Some(progress) = p.borrow_mut().as_mut() {
            progress.current_db = current_db;
            progress.current_segment = current_segment;
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
/// without standing up a whole shard event loop to drain it.
#[cfg(test)]
pub(crate) fn pending_for_test() -> Vec<(usize, Bytes, Entry)> {
    PENDING.with(|p| p.borrow().clone())
}

fn clear() {
    PENDING.with(|p| p.borrow_mut().clear());
    PENDING_KEYS.with(|k| k.borrow_mut().clear());
    PROGRESS.with(|p| *p.borrow_mut() = None);
}

/// Capture the pre-image of the key a `cmd + args` write is about to touch.
///
/// `cmd_and_args[0]` is the command name and `cmd_and_args[1]` its first
/// argument — the same "primary key = `command[1]`" contract
/// `spsc_handler::cow_intercept` uses for ordinary commands, so a script's
/// writes get exactly the fidelity the generic path has (multi-key writes
/// capture their first key; extending that is one shared follow-up for both
/// paths, not a script-specific gap).
///
/// Costs one thread-local `bool` load when no snapshot is in flight.
#[inline]
pub(crate) fn capture_command_pre_image(db: &Database, db_index: usize, cmd_and_args: &[Frame]) {
    if !is_armed() {
        return;
    }
    let Some(Frame::BulkString(key)) = cmd_and_args.get(1) else {
        return;
    };
    capture_key(db, db_index, key);
}

/// Capture the pre-image for a generic command about to run against `db`
/// (moon#558).
///
/// This is the choke point for every write that executes on the shard's own
/// stack instead of the event loop's: [`crate::command::dispatch`] is what
/// the monoio local arm, the tokio sharded local arm, `handler_single`, both
/// MULTI/EXEC executors, the coordinator's scatter arms and the SPSC drain
/// all funnel through. `spsc_handler::cow_intercept` covers only the last of
/// those — every other caller has no `&mut Option<SnapshotState>` in scope,
/// so before this existed a local `INCR` during a BGSAVE was serialized at
/// its POST-write value while the WAL still held the `INCR` to replay.
///
/// Double capture with `cow_intercept` on the routed arms is harmless:
/// `SnapshotState::capture_cow` is first-wins deduped, and both captures are
/// taken from the same pre-mutation state.
///
/// Cost when no snapshot is in flight — the overwhelmingly common case — is
/// one thread-local `bool` load; the `is_write` PHF lookup and the key
/// extraction are behind that gate.
///
/// Invariant: `db` MUST be `databases[db_index]` on the shard that armed the
/// capture — the drain re-derives the segment from `db_index`, so a
/// mismatched pair would file a pre-image against the wrong database. Every
/// live caller satisfies it (`dispatch` is always handed
/// `databases[*selected_db]`). The one structural exception,
/// `conn::shared::execute_transaction`, holds a lock on the ENTRY db while
/// `*selected_db` can be moved by a `SELECT` queued inside the same MULTI —
/// that executor belongs to `handler_single`, which is not wired into the
/// shipped server and runs no shard event loop, so it can never be armed.
///
/// Fidelity note: multi-key writes capture their PRIMARY key only, the same
/// contract `cow_intercept` has always had. Every non-idempotent single-key
/// write (`INCR`, `APPEND`, `SETRANGE`, `HINCRBY`, `LPUSH`, `ZINCRBY`, …) is
/// therefore covered; a destination-key write like `LMOVE src dst` still
/// captures only `src`. Widening that is one shared follow-up for both
/// paths, not a local-path gap.
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
    if !crate::command::metadata::is_write(cmd) {
        return;
    }
    let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, args) else {
        return;
    };
    capture_key(db, db_index, key);
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

/// Out-of-line slow path: look up and stash the old entry, first write wins.
fn capture_key(db: &Database, db_index: usize, key: &Bytes) {
    // moon#1186: a key whose segment is already written needs no pre-image —
    // the file holds its epoch-start bytes and the drain would drop the copy.
    // Skip it BEFORE the deep clone.
    let written = PROGRESS.with(|p| {
        p.borrow().as_ref().is_some_and(|progress| {
            let hash = crate::storage::dashtable::hash_key(key);
            !progress.is_pending(db_index, db.data().segment_index_for_hash(hash))
        })
    });
    if written {
        return;
    }
    if PENDING_KEYS.with(|k| k.borrow().contains(&(db_index, key.clone()))) {
        // Already captured this epoch — the FIRST pre-image is the
        // epoch-start value; a later one would be a value the snapshot must
        // not contain.
        return;
    }
    if let Some(old_entry) = db.data().get(key) {
        PENDING_KEYS.with(|k| k.borrow_mut().insert((db_index, key.clone())));
        PENDING.with(|p| {
            p.borrow_mut()
                .push((db_index, key.clone(), old_entry.clone()))
        });
    }
    // A key that does not exist yet needs no pre-image: the snapshot's
    // correct content for it is "absent", which is what serializing the
    // segment without an overflow record produces. It is not marked either:
    // its first write that finds it present captures it — the same value
    // the per-drain reset used to capture on the next tick.
}

/// Fold everything captured since the last drain into `snap`, dropping
/// pre-images for segments that were already serialized.
///
/// MUST run before the tick advances another segment, otherwise a pre-image
/// captured while its segment was still pending would be filtered out by a
/// bitmap that moved past it in the meantime.
pub(crate) fn drain_into(snap: &mut SnapshotState) {
    if PENDING.with(|p| p.borrow().is_empty()) {
        return;
    }
    let captured: Vec<(usize, Bytes, Entry)> =
        PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
    // PENDING_KEYS is NOT reset here (moon#1186): first-wins holds for the
    // whole epoch, so a hot key is cloned once, not once per tick.
    crate::shard::slice::with_shard(|s| {
        // Read guards only: the drain inspects each db's segment layout, it
        // never mutates one. Held across every captured pre-image so the
        // segment-index lookups all come from one consistent view, exactly as
        // the single-threaded slice gave them.
        s.databases
            .with_all_read(|dbs| drain_into_with_dbs(snap, dbs, captured))
    });
}

/// Testable core of [`drain_into`], parameterized on the database slice
/// instead of reaching for the thread-local shard slice.
fn drain_into_with_dbs<D: std::borrow::Borrow<Database>>(
    snap: &mut SnapshotState,
    databases: &[D],
    captured: Vec<(usize, Bytes, Entry)>,
) {
    for (db_index, key, entry) in captured {
        let Some(db) = databases.get(db_index) else {
            continue;
        };
        let db = db.borrow();
        let hash = crate::storage::dashtable::hash_key(&key);
        let seg_idx = db.data().segment_index_for_hash(hash);
        if snap.is_segment_pending(db_index, seg_idx) {
            snap.capture_cow(db_index, seg_idx, key, entry);
        }
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

        let captured = PENDING.with(|p| p.borrow().clone());
        assert_eq!(captured.len(), 1, "second write must not re-capture");
        match captured[0].2.value.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(s as &[u8], b"v0"),
            _ => panic!("expected a string entry"),
        }
        disarm();
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
                dbs[0].data().get(&seg0_key).unwrap().clone(),
            ),
            (
                0,
                seg1_key.clone(),
                dbs[0].data().get(&seg1_key).unwrap().clone(),
            ),
        ];
        drain_into_with_dbs(&mut state, &dbs, captured);

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
        drain_into_with_dbs(&mut state, &dbs, {
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
            drain_into_with_dbs(&mut state, &dbs, captured);
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
        note_progress(state.current_db_index(), state.current_segment_index());

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
