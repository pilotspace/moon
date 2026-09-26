//! The moon#1231 unlink hold for a server WITHOUT an AOF (moon#1260).
//!
//! Without an AOF the durable state after a crash is the shard's last
//! snapshot plus every spill file the manifest lists. A snapshot holds the
//! HOT keyspace only, so a key that was cold when the snapshot ran has one
//! durable copy: its slot in a spill file. Reading it back into RAM (a GET
//! promotion; there is no log to record it) takes it out of the cold index,
//! and once every key of the file has left, the orphan sweep unlinked the
//! file — so a kill -9 before the next snapshot lost a key that had not
//! changed since the last save. With an AOF, [`super::unlink_hold`] holds such
//! a file until a committed fold covers it; without one there was no fold
//! and so no hold.
//!
//! Here a SNAPSHOT plays the fold's part, with the same [`FoldView`] rule:
//!
//! - **epoch** counts snapshot starts and finishes on this shard (each moves
//!   it), so the hold's bound is raised to the spill-file counter at every
//!   start and finish, and on the first view (every file that existed at
//!   boot, e.g. inherited from an earlier `--appendonly yes` run, is held);
//! - a held file is stamped with the epoch at the decision and released once
//!   a snapshot that STARTED after it has finished successfully
//!   (`committed_floor` = that snapshot's start epoch) — its image holds every
//!   key the file backed that is still alive, in RAM at that point;
//! - while a snapshot is in progress every zero-ref file is held
//!   ([`FoldView::hold_all`]): the snapshot walks the keyspace segment by
//!   segment, and a key evicted into a new file before its segment was
//!   written and promoted back after it is in neither the image nor any file
//!   once that file goes.
//!
//! - (review F1) a file that went zero-ref BEFORE a snapshot started must be
//!   released by that snapshot, not by one that starts after the next orphan
//!   sweep: the snapshot already excludes the keys it backed (deleted or
//!   flushed — a FLUSHALL's own save, or a BGSAVE, runs before the sweep
//!   sees the file). So the start hook holds every queued zero-ref file with
//!   the epoch BEFORE the start ([`ColdIndex::hold_queued_before_snapshot`]),
//!   and the shard sweeps right after a successful snapshot
//!   (`shard::timers::sweep_after_snapshot`), so those files go at once
//!   instead of bringing the deleted keys back after a crash.
//!
//! State is per shard thread: the snapshot lifecycle hooks
//! (`shard::persistence_tick`) and the orphan sweep (`shard::timers`) both run
//! on the shard's own thread, on both runtimes. Only a process with a place
//! to write snapshots gets the hold (`SNAPSHOT_DIR_ABSENT` false): with no
//! snapshot possible nothing could ever release a held file, and a restart
//! does not load hot data anyway.

use std::cell::Cell;

use super::cold_index::ColdIndex;
use super::unlink_hold::FoldView;

/// Snapshot lifecycle counters of the current shard thread.
#[derive(Debug, Clone, Copy, Default)]
struct SnapshotCounters {
    /// Starts + finishes seen: the view's epoch.
    events: u64,
    /// `events` right after the running (or last) snapshot started.
    started_at: u64,
    /// `started_at` of the latest snapshot that finished successfully.
    committed: u64,
    in_progress: bool,
}

thread_local! {
    static COUNTERS: Cell<SnapshotCounters> = const {
        Cell::new(SnapshotCounters {
            events: 0,
            started_at: 0,
            committed: 0,
            in_progress: false,
        })
    };
}

/// Whether this process uses the snapshot hold: it has no AOF writer (an AOF
/// process holds by folds, [`super::unlink_hold`]). Every AOF writer pool is
/// created at boot, before any shard runs (`dead_slots::enable_ledger`).
#[must_use]
pub fn applies() -> bool {
    #[cfg(test)]
    {
        FORCE_APPLIES.with(Cell::get)
    }
    #[cfg(not(test))]
    {
        !super::dead_slots::aof_consumer_present()
    }
}

/// The epoch before the next start: the stamp of a file already zero-ref
/// when a snapshot starts (released once that snapshot succeeds).
#[must_use]
pub fn epoch_before_start() -> u64 {
    COUNTERS.with(|c| c.get().events)
}

impl ColdIndex {
    /// A snapshot is about to start (moon#1260 review F1): hold every queued
    /// zero-ref file with `stamp` ([`epoch_before_start`]), so the
    /// snapshot's success releases it. A file the boot rebuild found missing
    /// keeps its fast path (nothing to protect).
    pub fn hold_queued_before_snapshot(&mut self, stamp: u64) {
        if self.pending_unlink.is_empty() {
            return;
        }
        let missing = &self.missing_at_rebuild;
        let (keep, hold): (Vec<u64>, Vec<u64>) = std::mem::take(&mut self.pending_unlink)
            .into_iter()
            .partition(|f| missing.contains(f));
        self.pending_unlink = keep;
        self.hold.hold_stamped(hold, stamp);
    }
}

/// A snapshot of this shard started (BGSAVE, an auto-save, SHUTDOWN SAVE).
/// Call [`ColdIndex::hold_queued_before_snapshot`] on every database first
/// (`shard::timers::note_snapshot_started` does both).
pub fn note_snapshot_started() {
    COUNTERS.with(|c| {
        let mut s = c.get();
        s.events += 1;
        s.started_at = s.events;
        s.in_progress = true;
        c.set(s);
    });
}

/// The running snapshot of this shard finished — published (`ok`) or failed.
pub fn note_snapshot_finished(ok: bool) {
    COUNTERS.with(|c| {
        let mut s = c.get();
        if !s.in_progress {
            return;
        }
        s.events += 1;
        s.in_progress = false;
        if ok {
            s.committed = s.committed.max(s.started_at);
        }
        c.set(s);
    });
}

/// The hold view for a no-AOF process, or `None` when no snapshot can be
/// written (no persistence directory): then files go as before.
pub fn snapshot_fold_view(next_file_id: u64) -> Option<FoldView> {
    if crate::command::persistence::SNAPSHOT_DIR_ABSENT.load(std::sync::atomic::Ordering::Relaxed)
        || no_snapshot_dir_forced()
    {
        return None;
    }
    let s = COUNTERS.with(Cell::get);
    Some(FoldView {
        epoch: s.events,
        committed_floor: s.committed,
        next_file_id,
        hold_all: s.in_progress,
    })
}

#[cfg(test)]
thread_local! {
    static NO_SNAPSHOT_DIR: Cell<bool> = const { Cell::new(false) };
    /// Test-only [`applies`]: off unless a test opts in (the process-wide
    /// AOF flag depends on which other tests ran).
    static FORCE_APPLIES: Cell<bool> = const { Cell::new(false) };
}

/// Test-only: this thread behaves as a process with no AOF writer.
#[cfg(test)]
pub(crate) fn force_applies(on: bool) {
    FORCE_APPLIES.with(|c| c.set(on));
}

#[cfg(test)]
fn no_snapshot_dir_forced() -> bool {
    NO_SNAPSHOT_DIR.with(Cell::get)
}

#[cfg(not(test))]
#[inline]
fn no_snapshot_dir_forced() -> bool {
    false
}

/// Test-only: behave, on this thread, as a process started with no
/// persistence directory (`SNAPSHOT_DIR_ABSENT` is process-global, and a
/// parallel test's BGSAVE must not see it flip) until the guard drops.
#[cfg(test)]
pub(crate) fn force_no_snapshot_dir() -> NoSnapshotDirGuard {
    NO_SNAPSHOT_DIR.with(|c| c.set(true));
    NoSnapshotDirGuard
}

#[cfg(test)]
pub(crate) struct NoSnapshotDirGuard;

#[cfg(test)]
impl Drop for NoSnapshotDirGuard {
    fn drop(&mut self) {
        NO_SNAPSHOT_DIR.with(|c| c.set(false));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tiered::unlink_hold::UnlinkHold;

    /// Runs on its own test thread, so the counters start at zero.
    #[test]
    fn a_zero_ref_file_waits_for_a_snapshot_that_starts_after_it() {
        let mut h = UnlinkHold::default();
        // Boot: file 5 existed (inherited), the counter is at 10.
        h.observe(snapshot_fold_view(10).expect("view"));
        assert!(h.admit(vec![5]).unlink.is_empty(), "held: no snapshot yet");

        // A snapshot that started BEFORE the next decision only... starts.
        note_snapshot_started();
        h.observe(snapshot_fold_view(12).expect("view"));
        assert!(
            h.admit(vec![11]).unlink.is_empty(),
            "a file minted before the snapshot's start is held too"
        );
        note_snapshot_finished(true);
        h.observe(snapshot_fold_view(12).expect("view"));
        assert_eq!(
            h.admit(vec![]).unlink,
            vec![5],
            "5 went zero-ref before that snapshot started: released; 11 did not"
        );
        assert!(h.is_held(11));

        // A failed snapshot releases nothing; a later good one does.
        note_snapshot_started();
        note_snapshot_finished(false);
        h.observe(snapshot_fold_view(12).expect("view"));
        assert!(h.admit(vec![]).unlink.is_empty());
        note_snapshot_started();
        note_snapshot_finished(true);
        h.observe(snapshot_fold_view(12).expect("view"));
        assert_eq!(h.admit(vec![]).unlink, vec![11]);
        assert!(h.is_empty());
    }

    /// REVIEW-WS20 F1 unit proof, made permanent at the index level: a file
    /// whose keys were all deleted before a snapshot started must be released
    /// by that snapshot's success. The reviewer's first shape queued the file
    /// only at the sweep AFTER the save (stamp = post-save epoch), which held
    /// it until a SECOND snapshot; the start hook now holds it pre-start.
    #[test]
    fn review_ws20_a_file_emptied_before_a_snapshot_is_released_by_it() {
        use crate::persistence::kv_page::ValueType;
        use crate::storage::tiered::cold_index::ColdLocation;
        let mut ci = ColdIndex::new();
        let loc = ColdLocation {
            file_id: 5,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: ValueType::String,
        };
        ci.insert(bytes::Bytes::from_static(b"k"), loc);
        // Boot: the first sweep observes a view and has nothing to decide.
        ci.hold.observe(snapshot_fold_view(10).expect("view"));
        assert!(ci.hold.admit(Vec::new()).unlink.is_empty());
        // FLUSHALL / DEL empties file 5: queued zero-ref.
        assert!(ci.remove(b"k"));
        // Its save (moon#1264), or a BGSAVE, starts and succeeds before the
        // next sweep. The start hook holds the queued file pre-start.
        ci.hold_queued_before_snapshot(epoch_before_start());
        note_snapshot_started();
        note_snapshot_finished(true);
        // The next sweep (the event loop runs one right after the save).
        ci.hold.observe(snapshot_fold_view(10).expect("view"));
        let queued = std::mem::take(&mut ci.pending_unlink);
        let first = ci.hold.admit(queued).unlink;
        assert!(
            first.contains(&5),
            "file 5 is still held after a successful snapshot that already excludes \
             its deleted keys (first={first:?}); a kill -9 now brings them back"
        );
    }

    /// While a snapshot runs, even a file minted after its start is held (its
    /// key may be missing from the image), and only a LATER snapshot frees it.
    #[test]
    fn a_file_that_goes_zero_ref_during_a_snapshot_is_held_past_it() {
        let mut h = UnlinkHold::default();
        h.observe(snapshot_fold_view(10).expect("view"));
        assert_eq!(
            h.admit(vec![10]).unlink,
            vec![10],
            "minted after the boot view"
        );
        note_snapshot_started();
        h.observe(snapshot_fold_view(20).expect("view"));
        assert!(
            h.admit(vec![15]).unlink.is_empty(),
            "held while the snapshot runs"
        );
        note_snapshot_finished(true);
        h.observe(snapshot_fold_view(20).expect("view"));
        assert!(
            h.admit(vec![]).unlink.is_empty(),
            "that snapshot started before it"
        );
        note_snapshot_started();
        note_snapshot_finished(true);
        h.observe(snapshot_fold_view(20).expect("view"));
        assert_eq!(h.admit(vec![]).unlink, vec![15]);
    }
}
