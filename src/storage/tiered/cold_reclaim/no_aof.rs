//! Cold reclaim WITHOUT an AOF (moon#1297): the same compact-then-adopt
//! pipeline as [`super`], with a committed SNAPSHOT as the commit point
//! instead of a committed fold.
//!
//! Without an AOF the durable state after a crash is the shard's last
//! successful snapshot — the hot keyspace plus its cold-graves trailer
//! (moon#1281, `persistence::snapshot::cold_graves`) — and every spill file
//! the manifest lists. A spill file is unlinked only when its last live key
//! leaves it, so one surviving key pins every dead slot of its file on disk,
//! and since moon#1290 (no-AOF eviction tiers durably, 1024-entry / 1 MiB
//! batches) those files are large. Nothing reclaimed them.
//!
//! # The state machine (per compaction of a mostly-dead file `F`)
//!
//! ```text
//!  candidate ──Read job──▶ reading ──plan──▶ writing ──record──▶ PENDING(stamp e)
//!  (graves ≥ live,          (spill thread)    (F' fsynced,        │  F' unlisted; a snapshot
//!   shard gate)                                unlisted)          │  starting now carries the
//!                                                                 │  F' slots of every survivor
//!                                                                 │  that changed (trailer)
//!                     a snapshot that STARTED after the record     │
//!                     commits: committed_floor > e                ▼
//!                                                       ADOPTING (list F', commit_acked)
//!                                                                 │ ack durable
//!                                                                 ▼
//!                       re-point unchanged survivors to F'; grave the F' slots of the
//!                       changed ones (the graves record: every later trailer); unlink F
//!                       (its graves forgotten, tombstone a deferred commit)
//! ```
//!
//! The stamp `e` is the shard's snapshot epoch at the RECORD (not at the
//! plan, as the fold pipeline stamps): the adopting snapshot's trailer must be
//! built while the compaction is known, so that it can name `F'`'s slots.
//!
//! # Why every kill point is safe
//!
//! Let `S` be the last committed snapshot at the crash. The boot rebuilds the
//! cold index from the listed files, drops `S`'s graves, and picks each key's
//! newest copy by file id (`F'` > `F`, minted later).
//!
//! - **Before `F'` is listed** (compacted, snapshot started or running,
//!   snapshot committed): `F'` is an unlisted file the boot's orphan sweep
//!   removes; everything else is as if no compaction had run.
//! - **`F'` listed, `F` still listed** (the listing is durable, nothing
//!   re-pointed or unlinked yet): `S` started after the record (that is what
//!   made the compaction ready), so its trailer names `F`'s dead slots AND the
//!   `F'` slot of every survivor that changed before `S` started. A survivor
//!   unchanged at `S`'s start is live in both files and `F'`'s identical copy
//!   wins; a changed one is a grave in both. `S`'s point in time exactly.
//! - **`F` unlinked**: every key `S` saw cold in `F` is a survivor unchanged
//!   at `S`'s start (no key enters an old file), so its `F'` slot is live in
//!   `S`'s view and `F'` is durably listed: nothing needs `F` any more. A
//!   crash between `F`'s removal and its tombstone commit leaves a listed but
//!   missing file, which the boot counts and retires — the orphan sweep's own
//!   window.
//! - **The next snapshot** carries the `F'` graves from the graves record
//!   (noted at adoption) and none of `F`'s (forgotten at its unlink): the
//!   trailer is exact.
//!
//! An output none of whose survivors is unchanged at listing time is not
//! listed, and then `F` is not unlinked by the adoption — a survivor that
//! changed after `S` started may still be cold in `S`'s view, readable only
//! from `F` — and goes through the snapshot hold instead (`super::super::
//! snapshot_hold`), exactly as the fold pipeline does (moon#1231).
//!
//! # The trigger
//!
//! Without an AOF there is no key ledger (and so no ledger pressure); the
//! grave record ([`super::super::slot_graves`]) counts each listed file's
//! dead slots instead. A file is a candidate when it still backs a live key
//! and at least a third of its slots are dead ([`NO_AOF_LIVE_PER_DEAD`]: a
//! compaction writes at most two slots per slot it frees, and the files
//! settle at no more than ~1.5x their live bytes); a database
//! starts compacting once its candidates hold [`NO_AOF_MIN_DEAD_SLOTS`] dead
//! slots between them (a quarter of a full durable batch), most dead first.
//! [`NO_AOF_MAX_PENDING_PER_DB`] bounds the unlisted outputs waiting for a
//! snapshot. Their records are RAM charged to write admission until then,
//! so the shard tick also keeps them under a sixteenth of the shard's
//! budget, and starts at most one compaction a second while the shard is
//! spilling (`shard::persistence_tick::cold_reclaim_tick`): unbounded,
//! a write flood at `maxmemory` ran them past half the budget, where writes
//! are refused. A save rule usually supplies that snapshot; when compactions
//! wait for [`super::super::held_release::STALE_AFTER_SWEEPS`] sweeps with no
//! snapshot committing, the shard requests one
//! (`persistence::snapshot_request`, [`SnapshotReason::ColdReclaim`]) under
//! the moon#1289 spacing.
//!
//! # Open transactions (moon#1300)
//!
//! The requested snapshot runs whether a `TXN` is open or not: every
//! snapshot stores a key an open transaction holds at its pre-transaction
//! image (`persistence::snapshot_cow::capture_held_pre_images`, armed in the
//! same synchronous section that encodes the trailer). Nothing here needs to
//! know about transactions:
//!
//! - A connection's `TXN` write of a cold survivor reads it first
//!   (`transaction::conn_capture`: `Database::get` promotes it, and the hold
//!   keeps the promoted value), so the survivor is no longer where the
//!   compaction read it: the trailer graves its `F'` slot like any changed
//!   survivor, and the image carries its pre-transaction value. A crash at
//!   any point above restores that value, not the transaction's
//!   (`cold_block_reclaim_no_aof_1297`, every kill point with a `TXN` open).
//! - The adoption moves only unchanged survivors and never reads a value, so
//!   a key a transaction holds is never re-pointed under it.
//! - A commit's writes land after the snapshot's start, like any write; an
//!   abort's restore re-writes the held key, which a later trailer judges
//!   like any other change.
//!
//! [`SnapshotReason::ColdReclaim`]: crate::persistence::snapshot_request::SnapshotReason::ColdReclaim

use std::cell::Cell;

use crate::persistence::snapshot::cold_graves::{Encoder, pack_slot};
use crate::storage::tiered::cold_index::ColdIndex;

use super::CompactedFile;
use super::test_hooks::{ReclaimCrashPoint, crash_point};

/// Dead slots a database's candidate files must hold between them before it
/// compacts: a quarter of a full durable spill batch (`NO_AOF_BATCH_CAP` =
/// 1024), so a few stray deletions never start a read-write-snapshot cycle,
/// while a small shard's batches (`target / 16` bytes, a few hundred entries
/// at 8 MB `maxmemory`) still qualify after one mostly-dead file.
pub const NO_AOF_MIN_DEAD_SLOTS: usize = 256;

/// A file is a no-AOF candidate once it has at least one dead slot for every
/// this many live ones (a third of it dead). A compaction writes the live
/// slots and frees the whole file, so it writes at most this many slots per
/// slot of disk it frees, and the spill files settle at no more than
/// `1 + 1/this` times their live slots — about 1.5x. The fold pipeline's rule
/// is one (half dead): it bounds a RAM ledger, where each rewrite costs a
/// fold, while here the cost is a sequential rewrite of a small file.
pub const NO_AOF_LIVE_PER_DEAD: usize = 2;

/// Compactions one database may have waiting for a snapshot. Their outputs
/// are unlisted files on disk until adoption (at most half of each old
/// file's slots, by the candidate rule); this bounds them.
pub const NO_AOF_MAX_PENDING_PER_DB: usize = 64;

impl ColdIndex {
    /// Up to `max` files worth compacting now without an AOF, most dead slots
    /// first (see the module docs): files that still back a live key and
    /// have a dead slot per [`NO_AOF_LIVE_PER_DEAD`] live ones, not being compacted,
    /// adopted or given up on — and only once they hold
    /// [`NO_AOF_MIN_DEAD_SLOTS`] dead slots between them. Empty during
    /// recovery (older copies still hold file references).
    ///
    /// O(files with a grave), skipped while the grave record has not changed
    /// since a scan that found nothing to start.
    pub fn reclaim_candidates_no_aof(&mut self, max: usize) -> Vec<u64> {
        if max == 0 || !self.older_copies.is_empty() {
            return Vec::new();
        }
        let graves = self.graves.generation();
        if self.reclaim.no_aof_idle_at == Some(graves) {
            return Vec::new();
        }
        let mut eligible_dead = 0usize;
        let mut candidates: Vec<(usize, u64)> = self
            .graves
            .files()
            .filter_map(|(file_id, dead)| {
                let live = *self.file_refs.get(&file_id)? as usize;
                let eligible = live > 0
                    && dead.saturating_mul(NO_AOF_LIVE_PER_DEAD) >= live
                    && !self.reclaim.skip.contains(&file_id)
                    && !self.reclaim.is_busy(file_id);
                eligible.then_some((dead, file_id))
            })
            .inspect(|(dead, _)| eligible_dead += dead)
            .collect();
        if eligible_dead < NO_AOF_MIN_DEAD_SLOTS {
            self.reclaim.no_aof_idle_at = Some(graves);
            return Vec::new();
        }
        self.reclaim.no_aof_idle_at = None;
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        candidates.truncate(max);
        candidates.into_iter().map(|(_, id)| id).collect()
    }

    /// Compactions whose snapshot has committed (`committed_floor` above
    /// their stamp): the next [`Self::begin_adoption`] lists them.
    pub fn compactions_ready(&self, committed_floor: u64) -> usize {
        self.reclaim
            .pending
            .iter()
            .filter(|p| p.epoch < committed_floor)
            .count()
    }

    /// Whether a compaction waits for a snapshot that has not committed at
    /// `committed_floor` (the no-AOF snapshot request's input).
    pub fn awaits_reclaim_snapshot(&self, committed_floor: u64) -> bool {
        self.reclaim
            .pending
            .iter()
            .any(|p| p.epoch >= committed_floor)
    }

    /// A snapshot is starting without an AOF: write into its cold-graves
    /// trailer the slot, in its compacted output `F'`, of every survivor
    /// that is no longer exactly where it was read — for each compaction
    /// pending or being adopted. Such an `F'` may be listed before the next
    /// snapshot, and this snapshot is then the authority that says those
    /// copies are dead (module docs). A slot of an output that is never
    /// listed is ignored by every boot (only listed files' graves apply).
    ///
    /// O(survivors of the compactions in progress); nothing when none is.
    pub fn encode_compaction_graves_into(&self, enc: &mut Encoder) {
        if self.reclaim.pending.is_empty() && self.reclaim.adopting.is_empty() {
            return;
        }
        if !self.reclaim.pending.is_empty() {
            crash_point(ReclaimCrashPoint::SnapshotStart);
        }
        let mut packed: Vec<u64> = Vec::new();
        for out in self.compaction_outputs() {
            packed.clear();
            packed.extend(
                out.moved
                    .iter()
                    .filter(|m| self.lookup(&m.key) != Some(m.from))
                    .map(|m| pack_slot(m.to.page_idx, m.to.slot_idx)),
            );
            enc.add_file(out.entry.file_id, &packed);
        }
    }

    /// [`Self::encode_compaction_graves_into`] as `(file_id, packed slots)`
    /// (tests).
    #[cfg(test)]
    pub(crate) fn collect_compaction_graves(&self) -> Vec<(u64, Vec<u64>)> {
        self.compaction_outputs()
            .map(|out| {
                let packed = out
                    .moved
                    .iter()
                    .filter(|m| self.lookup(&m.key) != Some(m.from))
                    .map(|m| pack_slot(m.to.page_idx, m.to.slot_idx))
                    .collect();
                (out.entry.file_id, packed)
            })
            .filter(|(_, p): &(u64, Vec<u64>)| !p.is_empty())
            .collect()
    }

    /// Every output of a compaction pending or being adopted.
    fn compaction_outputs(&self) -> impl Iterator<Item = &CompactedFile> {
        self.reclaim
            .pending
            .iter()
            .flat_map(|p| p.outputs.iter())
            .chain(self.reclaim.adopting.iter().flat_map(|a| a.outputs.iter()))
    }
}

thread_local! {
    /// This shard's consecutive sweeps that saw a compaction waiting for a
    /// snapshot, and the committed floor they saw (the moon#1289 rule of
    /// `held_release::HeldWait`, per shard: a snapshot is per shard).
    static WAIT: Cell<(u32, u64)> = const { Cell::new((0, 0)) };
}

/// One orphan sweep's observation on this shard thread: `awaiting` — some
/// compaction waits for a snapshot not committed at `committed_floor`.
/// Returns whether the wait is stale ([`super::super::held_release::STALE_AFTER_SWEEPS`]
/// sweeps running at one floor): no save rule is going to commit it, so the
/// shard should request a snapshot. A committed snapshot moves the floor and
/// restarts the count.
pub fn note_reclaim_sweep(awaiting: bool, committed_floor: u64) -> bool {
    WAIT.with(|w| {
        let (sweeps, floor) = w.get();
        let next = if !awaiting {
            (0, committed_floor)
        } else if sweeps == 0 || floor != committed_floor {
            (1, committed_floor)
        } else {
            (sweeps.saturating_add(1), floor)
        };
        w.set(next);
        next.0 >= super::super::held_release::STALE_AFTER_SWEEPS
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs on its own test thread, so the counter starts at zero.
    #[test]
    fn a_compaction_waiting_three_sweeps_at_one_floor_is_stale() {
        assert!(!note_reclaim_sweep(true, 4));
        assert!(!note_reclaim_sweep(true, 4));
        assert!(note_reclaim_sweep(true, 4));
        assert!(
            !note_reclaim_sweep(true, 6),
            "a committed snapshot restarts it"
        );
        assert!(!note_reclaim_sweep(false, 6));
        assert!(!note_reclaim_sweep(true, 6));
    }
}
