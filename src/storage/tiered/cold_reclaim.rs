//! Reclaim of mostly-dead spill files — the bound on the cold tier's
//! dead-slot ledger (moon#1215, PR #1233 review).
//!
//! A spill file is unlinked only when its LAST live key leaves it, and until
//! then the ledger keeps a key copy for each of its dead slots
//! ([`super::dead_slots`]). One surviving key per file is enough to keep a
//! whole file's dead keys in RAM. Eviction cannot help (it moves live keys
//! to disk; it frees no ledger byte), so the write gate only refuses writes
//! once the ledger is large (`eviction::evict_to_budget`). This module is
//! what actually brings it down: it moves a mostly-dead file's few live keys
//! into a new file so the old one can be unlinked, taking its ledger entries
//! with it.
//!
//! # Design: compact now, list after the next committed fold
//!
//! 1. **Compact** (while the shard's ledger is over its share): read a
//!    mostly-dead file `F`, and write every slot whose key's index entry
//!    still points at it into a NEW spill file `F'` (fresh id from the
//!    shard's one counter) through the spill writer
//!    (`spill_thread::flush_buffer`: temp file, fsync, rename, directory
//!    fsync). `F'` is NOT listed in the manifest and the index is NOT
//!    re-pointed yet: until adoption nothing but this record knows `F'`.
//!    The compaction is stamped with the writer's current fold epoch `E`
//!    (`RewriteOverflow::stamp`), in the same synchronous section that mints
//!    the ids of `F'`.
//! 2. **Adopt** (once the writer has adopted a COMMITTED fold whose snapshot
//!    epoch is above `E`, i.e. a fold cut after the ids were minted): list
//!    `F'` and wait until that listing is durable; then re-point every
//!    survivor still exactly where it was read to `F'`, record the `F'` slots
//!    of survivors that changed meanwhile as dead (ghost slots of a listed
//!    file), and unlink `F` — nothing references it any more — with its
//!    ledger entries.
//!
//! # Where the I/O runs (moon#1240)
//!
//! None of it on the shard thread. The shard's tick drives a pipeline
//! through the spill thread (`super::reclaim_io`): a `Read` job reads and
//! decodes `F`; the shard filters the survivors ([`ColdIndex::plan_compaction`],
//! in memory) and sends a `Write` job; the spill thread writes and fsyncs
//! `F'`; the shard records the compaction ([`ColdIndex::record_compaction`]).
//! The adoption lists `F'` with a commit whose fsync runs on the
//! manifest-sync thread and whose ack the shard polls from a later tick
//! ([`ColdIndex::begin_adoption`] / [`ColdIndex::finish_adoptions`]); the
//! tombstones of the unlinked old files are a deferred commit. What is left
//! on the shard thread is in-memory work and the `remove_file` of an old or
//! unadopted file — metadata operations, as in the orphan sweep.
//! [`ColdIndex::compact_file`] and [`ColdIndex::adopt_compactions`] run the
//! same steps inline, for tests and tools.
//!
//! # Why the order is what it is (crash windows in the WS15 and WS19 NOTES)
//!
//! The committed AOF generation must be able to rebuild every key its replay
//! reads. A survivor `k` of `F` was cold at the committed fold's cut (or
//! spilled after it): its value lives only in `F`'s slot, and that
//! generation's records — a `SUNIONSTORE dst k` reads a cold key without
//! promoting it — may read it during replay. Replay only reads files below
//! the generation's `MOON.COLDCUT` or authorized by a `MOON.SPILLED` marker
//! (moon#902), and `F'` is neither for the generation committed at
//! compaction time: re-pointing `k` to `F'` and unlinking `F` right away
//! would lose `k` to such a replay after a crash — the promote-then-sweep
//! class (moon#1231). Waiting for a committed fold cut AFTER the compaction
//! makes `F'` (minted before that cut) a file below the committed
//! generation's cut, so the replay reads `k` from `F'` and `F` is no longer
//! needed by anything — once `F'` is durably listed, which is why `F` goes
//! only after the listing's ack. Until adoption `F` stays listed and
//! referenced: its dead slots keep their ledger entries (the fold that
//! commits writes their `DEL`s), and a crash before adoption leaves `F'` an
//! unlisted file the startup orphan sweep removes.
//!
//! A reclaim therefore completes only across a fold. When compactions are
//! waiting, the AOF auto-rewrite monitor dispatches one
//! ([`awaiting_fold`]); a manual or growth-triggered `BGREWRITEAOF` serves
//! the same purpose.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use super::cold_index::{ColdIndex, ColdLocation};
use super::reclaim_io::{FileSlots, discard_output, read_file_slots, write_outputs};
use crate::persistence::kv_page::entry_flags;
use crate::persistence::manifest::{FileEntry, ShardManifest};
use crate::persistence::manifest_sync::CommitAck;
use crate::persistence::page::PageType;
use crate::storage::tiered::spill_thread::{SpillCompletion, SpillRequest};

/// Compactions waiting for a committed fold, over every shard and database.
/// The AOF auto-rewrite monitor dispatches a rewrite while this is non-zero
/// and automatic rewrites are enabled.
static AWAITING_FOLD: AtomicUsize = AtomicUsize::new(0);

/// Databases whose held spill files (moon#1231) keep the ledger over the
/// reclaim threshold until a fold. Counted apart from [`AWAITING_FOLD`]
/// because the monitor answers it even with `auto-aof-rewrite-percentage 0`
/// (moon#1231 review): the hold is a correctness rule, and without a fold
/// its files — and the ledger bytes write admission is charged — are never
/// released.
static HELD_FILES_PRESSURE: AtomicUsize = AtomicUsize::new(0);

/// How many compactions wait for a committed fold, process-wide.
#[inline]
pub fn awaiting_fold() -> usize {
    AWAITING_FOLD.load(Ordering::Relaxed)
}

/// How many databases' held spill files wait for a committed fold while
/// their shard's ledger is over the reclaim threshold, process-wide.
#[inline]
pub fn held_files_pressure() -> usize {
    HELD_FILES_PRESSURE.load(Ordering::Relaxed)
}

/// One survivor moved by a compaction: where it was read and where its copy
/// now is.
#[derive(Debug)]
struct Moved {
    key: Bytes,
    from: ColdLocation,
    to: ColdLocation,
}

/// One new spill file a compaction wrote (unlisted until adoption).
#[derive(Debug)]
struct CompactedFile {
    entry: FileEntry,
    moved: Vec<Moved>,
}

/// A compacted file waiting for a committed fold.
#[derive(Debug)]
struct PendingCompaction {
    old_file: u64,
    /// `RewriteOverflow::stamp` when its output ids were minted.
    epoch: u64,
    outputs: Vec<CompactedFile>,
    /// RAM this record holds, charged with the ledger.
    bytes: usize,
}

/// Outputs listed by [`ColdIndex::begin_adoption`], waiting for the
/// listing's commit to be durable.
#[derive(Debug)]
struct Adopting {
    outputs: Vec<CompactedFile>,
    /// Old files whose every output is in `outputs` (moon#1231: only those
    /// may be unlinked by the adoption).
    old_files: Vec<u64>,
    ack: CommitAck,
    bytes: usize,
}

/// Per-database reclaim state, kept inside [`ColdIndex`].
#[derive(Debug, Default)]
pub struct ReclaimState {
    pending: Vec<PendingCompaction>,
    /// Old files with a `Read` or `Write` job on the spill thread.
    in_flight: HashSet<u64>,
    adopting: Vec<Adopting>,
    /// Files a compaction gave up on (unreadable, or a live slot that did not
    /// decode): never retried by this process, so a damaged file cannot turn
    /// every tick into a failing read. Tiny: one id per such file.
    skip: HashSet<u64>,
    bytes: usize,
    /// Whether this database counts in [`HELD_FILES_PRESSURE`] (see
    /// [`ColdIndex::note_held_files_pressure`]).
    held_signal: bool,
}

impl Drop for ReclaimState {
    fn drop(&mut self) {
        // A cold index dropped with compactions pending leaves their
        // unlisted files to the next startup orphan sweep.
        AWAITING_FOLD.fetch_sub(self.pending.len(), Ordering::Relaxed);
        HELD_FILES_PRESSURE.fetch_sub(usize::from(self.held_signal), Ordering::Relaxed);
    }
}

impl ReclaimState {
    /// RAM the pending compactions hold (charged with the ledger).
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.bytes
    }

    /// Compactions waiting for a committed fold.
    #[inline]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Whether `file_id` is being compacted or adopted.
    fn is_busy(&self, file_id: u64) -> bool {
        self.in_flight.contains(&file_id)
            || self.pending.iter().any(|p| p.old_file == file_id)
            || self.adopting.iter().any(|a| a.old_files.contains(&file_id))
    }
}

/// What an adoption step did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AdoptReport {
    /// Compactions whose fold had committed (adopted or discarded).
    pub compactions: usize,
    /// New files listed in the manifest.
    pub files_listed: usize,
    /// Survivors re-pointed to their new file.
    pub keys_moved: usize,
    /// Compacted files unlinked (their ledger entries with them).
    pub files_unlinked: usize,
    /// Bytes those unlinked files held on disk.
    pub bytes_unlinked: u64,
}

impl AdoptReport {
    fn add(&mut self, other: AdoptReport) {
        self.compactions += other.compactions;
        self.files_listed += other.files_listed;
        self.keys_moved += other.keys_moved;
        self.files_unlinked += other.files_unlinked;
        self.bytes_unlinked += other.bytes_unlinked;
    }
}

/// A compaction's survivors, ready for the spill writer: `moved[i]` is the
/// key and source slot of `requests[i]`.
pub(crate) struct WritePlan {
    pub(crate) moved: Vec<(Bytes, ColdLocation)>,
    pub(crate) requests: Vec<SpillRequest>,
}

impl ColdIndex {
    /// Compactions waiting for a committed fold.
    #[inline]
    pub fn pending_compactions(&self) -> usize {
        self.reclaim.pending()
    }

    /// Compactions with a job on the spill thread (moon#1240).
    #[inline]
    pub fn compactions_in_flight(&self) -> usize {
        self.reclaim.in_flight.len()
    }

    /// Adoptions waiting for their listing's commit to be durable.
    #[inline]
    pub fn adoptions_in_flight(&self) -> usize {
        self.reclaim.adopting.len()
    }

    /// moon#1231: a held spill file keeps its dead-slot ledger entries until
    /// a committed fold releases it, and compaction cannot help (it has no
    /// live key). So while the shard's ledger is over the reclaim threshold
    /// (`over`) and some held file still waits for a fold, this database asks
    /// the auto-rewrite monitor for one — even with automatic rewrites
    /// disabled, which a waiting compaction does not do — otherwise write
    /// admission could stay refused on bytes only a fold can free.
    /// Idempotent; the tick calls it every time.
    pub fn note_held_files_pressure(&mut self, over: bool, committed_floor: u64) {
        let want = over && self.hold.awaits_fold(committed_floor);
        if want != self.reclaim.held_signal {
            self.reclaim.held_signal = want;
            if want {
                HELD_FILES_PRESSURE.fetch_add(1, Ordering::Relaxed);
            } else {
                HELD_FILES_PRESSURE.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Up to `max` files worth compacting now, most dead bytes first: files
    /// with at least one live key (a file with none is the orphan sweep's)
    /// and at least as many dead slots as live ones — so a compaction
    /// rewrites at most as many slots as it frees — not already being
    /// compacted or adopted and not given up on. Empty during recovery (older
    /// copies still hold file references). O(files with a dead slot).
    pub fn reclaim_candidates(&self, max: usize) -> Vec<u64> {
        if max == 0 || !self.older_copies.is_empty() {
            return Vec::new();
        }
        let mut candidates: Vec<(usize, u64)> = self
            .dead
            .files()
            .filter_map(|(file_id, dead, bytes)| {
                let live = *self.file_refs.get(&file_id)? as usize;
                let eligible = live > 0
                    && dead >= live
                    && !self.reclaim.skip.contains(&file_id)
                    && !self.reclaim.is_busy(file_id);
                eligible.then_some((bytes, file_id))
            })
            .collect();
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        candidates.truncate(max);
        candidates.into_iter().map(|(_, id)| id).collect()
    }

    /// Mark `file_id` as being compacted (its `Read` job is about to be
    /// sent). `false` if it already is, or is pending or adopting.
    pub fn start_compaction(&mut self, file_id: u64) -> bool {
        if self.reclaim.is_busy(file_id) {
            return false;
        }
        self.reclaim.in_flight.insert(file_id);
        true
    }

    /// A compaction ends without output (its job could not be sent, or found
    /// nothing to do). `give_up`: never try this file again in this process.
    pub fn abandon_compaction(&mut self, file_id: u64, give_up: bool) {
        self.reclaim.in_flight.remove(&file_id);
        if give_up {
            self.reclaim.skip.insert(file_id);
        }
    }

    /// Given every slot of `file_id` as read from disk, pick the survivors —
    /// slots whose key's index entry is exactly that slot — and give each a
    /// fresh id from `next_file_id` (one per request, as the durable batch
    /// spill does; a sub-batch is named after its first). In memory only.
    ///
    /// `Ok(None)`: no live key left (the orphan sweep's file now); the
    /// compaction ends. `Err`: a live slot did not decode — the file is given
    /// up on. Either way the in-flight mark is cleared; on `Ok(Some)` it stays
    /// until [`Self::record_compaction`].
    pub(crate) fn plan_compaction(
        &mut self,
        file_id: u64,
        db_index: usize,
        slots: FileSlots,
        shard_dir: &Path,
        next_file_id: &mut u64,
    ) -> Result<Option<WritePlan>, String> {
        let live = self.file_refs.get(&file_id).copied().unwrap_or(0) as usize;
        if live == 0 {
            self.abandon_compaction(file_id, false);
            return Ok(None);
        }
        let mut moved = Vec::with_capacity(live);
        let mut requests = Vec::with_capacity(live);
        for slot in slots.slots {
            let Some(loc) = self.lookup(&slot.key) else {
                continue;
            };
            if loc.file_id != file_id
                || loc.page_idx != slot.page_idx
                || loc.slot_idx != slot.slot_idx
            {
                continue;
            }
            let Some(value) = slot.value else {
                self.abandon_compaction(file_id, true);
                return Err("broken overflow chain in a live slot".to_string());
            };
            let id = *next_file_id;
            *next_file_id += 1;
            requests.push(SpillRequest {
                key: slot.key.clone(),
                db_index,
                value_bytes: value,
                value_type: loc.value_type,
                flags: if loc.ttl_ms.is_some() {
                    entry_flags::HAS_TTL
                } else {
                    0
                },
                ttl_ms: loc.ttl_ms,
                file_id: id,
                shard_dir: shard_dir.to_path_buf(),
            });
            moved.push((slot.key, loc));
        }
        if moved.len() != live {
            self.abandon_compaction(file_id, true);
            return Err(format!(
                "{} of {live} live slots decoded; the file stays as it is",
                moved.len()
            ));
        }
        Ok(Some(WritePlan { moved, requests }))
    }

    /// The spill writer's answer for `old_file`'s survivors (`moved`, in
    /// request order), stamped `epoch`: record the compaction for adoption
    /// after a committed fold past `epoch`. On a failed or unexpected write,
    /// every output is removed and the file is not tried again. A compaction
    /// this index does not know (its cold index was replaced meanwhile) only
    /// has its outputs removed.
    pub(crate) fn record_compaction(
        &mut self,
        old_file: u64,
        epoch: u64,
        moved: Vec<(Bytes, ColdLocation)>,
        written: Result<Vec<SpillCompletion>, String>,
        shard_dir: &Path,
    ) -> Result<(), String> {
        let known = self.reclaim.in_flight.remove(&old_file);
        let completions = match written {
            Ok(completions) => completions,
            Err(why) => {
                if known {
                    self.reclaim.skip.insert(old_file);
                }
                return Err(why);
            }
        };
        let discard_all = |completions: &[SpillCompletion]| {
            for c in completions {
                discard_output(shard_dir, c.file_entry.file_id);
            }
        };
        if !known {
            discard_all(&completions);
            return Err("the compaction is no longer known to this index".to_string());
        }
        // Pair each written entry with the slot it was read from (the writer
        // keeps request order; checked, not assumed).
        let entries: usize = completions.iter().map(|c| c.entries.len()).sum();
        if entries != moved.len() {
            discard_all(&completions);
            self.reclaim.skip.insert(old_file);
            return Err("the spill writer returned a different number of entries".into());
        }
        let mut from = moved.into_iter();
        let mut outputs = Vec::with_capacity(completions.len());
        let mut bytes = std::mem::size_of::<PendingCompaction>();
        let mut reordered = false;
        for c in &completions {
            let mut out_moved = Vec::with_capacity(c.entries.len());
            for e in &c.entries {
                let Some((key, loc)) = from.next() else {
                    reordered = true;
                    break;
                };
                if key != e.key {
                    reordered = true;
                    break;
                }
                bytes += key.len() + std::mem::size_of::<Moved>();
                out_moved.push(Moved {
                    key,
                    from: loc,
                    to: ColdLocation {
                        file_id: c.file_entry.file_id,
                        page_idx: e.page_idx,
                        slot_idx: e.slot_idx,
                        ttl_ms: e.ttl_ms,
                        value_type: e.value_type,
                    },
                });
            }
            if reordered {
                break;
            }
            bytes += std::mem::size_of::<CompactedFile>();
            outputs.push(CompactedFile {
                entry: c.file_entry.clone(),
                moved: out_moved,
            });
        }
        if reordered {
            discard_all(&completions);
            self.reclaim.skip.insert(old_file);
            return Err("the spill writer reordered its entries".into());
        }
        self.reclaim.bytes += bytes;
        self.reclaim.pending.push(PendingCompaction {
            old_file,
            epoch,
            outputs,
            bytes,
        });
        AWAITING_FOLD.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Compact `file_id` (a file of database `db_index`) in one call: the
    /// same read, plan, durable write and record the spill-thread pipeline
    /// runs, inline on the caller's thread (tests and tools; the shard's tick
    /// never calls this — moon#1240). Nothing the index, the ledger or the
    /// manifest says changes.
    ///
    /// Returns the bytes read. On any failure nothing is recorded, every file
    /// written is removed, and the file is not tried again by this process.
    pub fn compact_file(
        &mut self,
        file_id: u64,
        db_index: usize,
        shard_dir: &Path,
        next_file_id: &mut u64,
        epoch: u64,
    ) -> Result<u64, String> {
        if !self.start_compaction(file_id) {
            return Err("already being compacted or adopted".to_string());
        }
        let slots = match read_file_slots(shard_dir, file_id) {
            Ok(slots) => slots,
            Err(why) => {
                self.abandon_compaction(file_id, true);
                return Err(why);
            }
        };
        let read = slots.bytes_read;
        let Some(plan) = self.plan_compaction(file_id, db_index, slots, shard_dir, next_file_id)?
        else {
            self.reclaim.skip.insert(file_id);
            return Err("no live key left (the orphan sweep's file)".to_string());
        };
        let written = write_outputs(plan.requests, shard_dir);
        self.record_compaction(file_id, epoch, plan.moved, written, shard_dir)?;
        Ok(read)
    }

    /// Phase A of adoption: for every pending compaction whose fold has
    /// committed (`epoch < committed_floor`, see the module doc), list every
    /// output that still backs an unchanged survivor and hand the manifest
    /// to the sync thread with an ack ([`ShardManifest::commit_acked`] —
    /// the fsync never blocks this thread). Nothing is re-pointed or
    /// unlinked until [`Self::finish_adoptions`] sees the ack.
    pub fn begin_adoption(
        &mut self,
        committed_floor: u64,
        shard_dir: &Path,
        manifest: &mut ShardManifest,
    ) -> AdoptReport {
        let mut report = AdoptReport::default();
        if self.reclaim.pending.is_empty() {
            return report;
        }
        let (ready, waiting): (Vec<PendingCompaction>, Vec<PendingCompaction>) =
            std::mem::take(&mut self.reclaim.pending)
                .into_iter()
                .partition(|p| p.epoch < committed_floor);
        self.reclaim.pending = waiting;
        if ready.is_empty() {
            return report;
        }
        report.compactions = ready.len();
        AWAITING_FOLD.fetch_sub(ready.len(), Ordering::Relaxed);
        let bytes: usize = ready.iter().map(|p| p.bytes).sum();

        // moon#1231: the old file is unlinked by the adoption only if EVERY
        // output of its compaction is listed. A survivor that changed after
        // the fold that made this adoption possible (a read promotion, say)
        // was cold in the old file at that fold, so the committed generation
        // still reads it there — unless its copy is listed below the cut. A
        // discarded output leaves some such survivor with no other copy; its
        // old file then goes through the orphan sweep's hold instead.
        let mut outputs: Vec<CompactedFile> = Vec::new();
        let mut old_files: Vec<u64> = Vec::with_capacity(ready.len());
        for compaction in ready {
            let mut every_output_listed = true;
            for out in compaction.outputs {
                let any_unchanged = out
                    .moved
                    .iter()
                    .any(|m| self.lookup(&m.key) == Some(m.from));
                if !any_unchanged {
                    // Every survivor changed since: nothing would point at
                    // the copy, so it is never listed.
                    discard_output(shard_dir, out.entry.file_id);
                    every_output_listed = false;
                    continue;
                }
                match manifest.add_file(out.entry.clone()) {
                    Ok(()) => outputs.push(out),
                    Err(e) => {
                        every_output_listed = false;
                        // Unreachable while the file-id seed holds (moon#893):
                        // the manifest describes some other file under this
                        // id, so leave that file alone and adopt nothing here.
                        tracing::error!(
                            file_id = out.entry.file_id,
                            error = %e,
                            "cold reclaim: the manifest already lists a compacted file's id; \
                             not adopting it"
                        );
                    }
                }
            }
            if every_output_listed {
                old_files.push(compaction.old_file);
            }
        }
        if outputs.is_empty() {
            self.reclaim.bytes = self.reclaim.bytes.saturating_sub(bytes);
            return report;
        }
        report.files_listed = outputs.len();
        let ack = manifest.commit_acked();
        self.reclaim.adopting.push(Adopting {
            outputs,
            old_files,
            ack,
            bytes,
        });
        report
    }

    /// Phase B of adoption, for every listing whose commit is now known:
    /// durable -> re-point every survivor still exactly where it was read
    /// (checked NOW), record the new slots of the ones that changed as dead,
    /// and unlink the old files nothing references any more (their ledger
    /// entries go with them; the tombstones are a deferred commit — a lost
    /// one leaves a listed-but-missing file, which recovery counts and
    /// retires). Failed -> unlist and remove the outputs; the survivors stay
    /// where they were and the files may be compacted again later. Listings
    /// still in flight wait for a later call. Never blocks.
    pub fn finish_adoptions(
        &mut self,
        shard_dir: &Path,
        manifest: &mut ShardManifest,
    ) -> AdoptReport {
        let mut report = AdoptReport::default();
        if self.reclaim.adopting.is_empty() {
            return report;
        }
        for mut adopting in std::mem::take(&mut self.reclaim.adopting) {
            let Some(outcome) = adopting.ack.poll() else {
                self.reclaim.adopting.push(adopting);
                continue;
            };
            self.reclaim.bytes = self.reclaim.bytes.saturating_sub(adopting.bytes);
            if let Err(e) = outcome {
                for out in &adopting.outputs {
                    manifest.remove_file(out.entry.file_id, PageType::KvLeaf);
                    discard_output(shard_dir, out.entry.file_id);
                }
                tracing::error!(
                    err = %e,
                    files = adopting.outputs.len(),
                    "cold reclaim: manifest commit failed; the compacted files are not adopted \
                     and their keys stay in the files they were read from"
                );
                continue;
            }
            for out in adopting.outputs {
                for m in out.moved {
                    if self.lookup(&m.key) == Some(m.from) {
                        self.insert(m.key, m.to);
                        report.keys_moved += 1;
                    } else {
                        self.note_dead_slot(m.to.file_id, m.key, m.to.ttl_ms);
                    }
                }
            }
            let mut old_files = adopting.old_files;
            old_files.retain(|id| !self.file_refs.contains_key(id));
            let queued = |ci: &ColdIndex| {
                old_files
                    .iter()
                    .filter(|id| ci.pending_unlink.contains(id) || ci.hold.is_held(**id))
                    .count()
            };
            let before = queued(self);
            match self.unlink_now(&old_files, shard_dir, Some(manifest)) {
                Ok(bytes) => {
                    report.files_unlinked += before - queued(self);
                    report.bytes_unlinked += bytes;
                }
                Err(e) => tracing::error!(
                    err = %e,
                    "cold reclaim: manifest commit after unlinking compacted files failed; the \
                     orphan sweep retries"
                ),
            }
        }
        report
    }

    /// Both adoption phases in one call, waiting (at most 30 s) for the
    /// listing to be durable — for tests and tools; the shard's tick runs
    /// the phases on separate ticks (moon#1240).
    pub fn adopt_compactions(
        &mut self,
        committed_floor: u64,
        shard_dir: &Path,
        manifest: &mut ShardManifest,
    ) -> AdoptReport {
        let mut report = self.begin_adoption(committed_floor, shard_dir, manifest);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            report.add(self.finish_adoptions(shard_dir, manifest));
            if self.reclaim.adopting.is_empty() || std::time::Instant::now() >= deadline {
                return report;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
