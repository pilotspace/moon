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
//! 1. **Compact** (shard tick, while the shard's ledger is over its share):
//!    read a mostly-dead file `F`, and write every slot whose key's index
//!    entry still points at it into a NEW spill file `F'` (fresh id from the
//!    shard's one counter) through the spill writer
//!    (`spill_thread::flush_buffer`: temp file, fsync, rename, directory
//!    fsync). `F'` is NOT listed in the manifest and the index is NOT
//!    re-pointed yet: until adoption nothing but this record knows `F'`.
//!    The compaction is stamped with the writer's current fold epoch `E`
//!    (`RewriteOverflow::stamp`).
//! 2. **Adopt** (shard tick, once the writer has adopted a COMMITTED fold
//!    whose snapshot epoch is above `E`, i.e. a fold cut after the
//!    compaction): list `F'` with a durable manifest commit, then re-point
//!    every survivor still exactly where it was read to `F'`, record the `F'`
//!    slots of survivors that changed meanwhile as dead (ghost slots of a
//!    listed file), and unlink `F` — nothing references it any more — with
//!    its ledger entries.
//!
//! # Why the order is what it is (crash windows in the WS15 NOTES.md)
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
//! needed by anything. Until adoption `F` stays listed and referenced: its
//! dead slots keep their ledger entries (the fold that commits writes their
//! `DEL`s), and a crash before adoption leaves `F'` an unlisted file the
//! startup orphan sweep removes.
//!
//! A reclaim therefore completes only across a fold. When compactions are
//! waiting, the AOF auto-rewrite monitor dispatches one
//! ([`awaiting_fold`]); a manual or growth-triggered `BGREWRITEAOF` serves
//! the same purpose.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use super::cold_index::{ColdIndex, ColdLocation, PageVerdict, classify_page};
use crate::persistence::kv_page::{KvLeafPage, entry_flags, read_overflow_chain};
use crate::persistence::manifest::{FileEntry, ShardManifest};
use crate::persistence::page::{PAGE_4K, PageType};
use crate::storage::tiered::spill_thread::{SpillRequest, flush_buffer};

/// Compactions waiting for a committed fold, over every shard and database.
/// The AOF auto-rewrite monitor dispatches a rewrite while this is non-zero.
static AWAITING_FOLD: AtomicUsize = AtomicUsize::new(0);

/// How many compactions wait for a committed fold, process-wide.
#[inline]
pub fn awaiting_fold() -> usize {
    AWAITING_FOLD.load(Ordering::Relaxed)
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
    /// `RewriteOverflow::stamp` when it was compacted.
    epoch: u64,
    outputs: Vec<CompactedFile>,
    /// RAM this record holds, charged with the ledger.
    bytes: usize,
}

/// Per-database reclaim state, kept inside [`ColdIndex`].
#[derive(Debug, Default)]
pub struct ReclaimState {
    pending: Vec<PendingCompaction>,
    /// Files a compaction gave up on (unreadable, or a live slot that did not
    /// decode): never retried by this process, so a damaged file cannot turn
    /// every tick into a failing read. Tiny: one id per such file.
    skip: HashSet<u64>,
    bytes: usize,
}

impl Drop for ReclaimState {
    fn drop(&mut self) {
        // A cold index dropped with compactions pending leaves their
        // unlisted files to the next startup orphan sweep.
        AWAITING_FOLD.fetch_sub(self.pending.len(), Ordering::Relaxed);
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

    fn is_pending(&self, file_id: u64) -> bool {
        self.pending.iter().any(|p| p.old_file == file_id)
    }
}

/// What [`ColdIndex::adopt_compactions`] did.
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

fn heap_path(shard_dir: &Path, file_id: u64) -> std::path::PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"))
}

/// Remove an unlisted compaction output (a write that will never be
/// adopted). Best effort: a leftover is an unlisted file the startup orphan
/// sweep removes.
fn discard_output(shard_dir: &Path, file_id: u64) {
    if let Err(e) = std::fs::remove_file(heap_path(shard_dir, file_id))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            file_id,
            err = %e,
            "cold reclaim: could not remove an unadopted compaction file; the startup \
             orphan sweep removes it"
        );
    }
}

impl ColdIndex {
    /// Compactions waiting for a committed fold.
    #[inline]
    pub fn pending_compactions(&self) -> usize {
        self.reclaim.pending()
    }

    /// Up to `max` files worth compacting now, most dead bytes first: files
    /// with at least one live key (a file with none is the orphan sweep's)
    /// and at least as many dead slots as live ones — so a compaction
    /// rewrites at most as many slots as it frees — not already compacted
    /// and not given up on. Empty during recovery (older copies still hold
    /// file references). O(files with a dead slot).
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
                    && !self.reclaim.is_pending(file_id);
                eligible.then_some((bytes, file_id))
            })
            .collect();
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        candidates.truncate(max);
        candidates.into_iter().map(|(_, id)| id).collect()
    }

    /// Compact `file_id` (a file of database `db_index`): write its live
    /// slots into new, unlisted spill files and record the compaction for
    /// adoption after a fold committed past `epoch` (see the module doc).
    /// Nothing the index, the ledger or the manifest says changes here.
    ///
    /// Returns the bytes read (the caller's per-tick budget). On any failure
    /// nothing is recorded, every file written is removed, and the file is
    /// not tried again by this process.
    pub fn compact_file(
        &mut self,
        file_id: u64,
        db_index: usize,
        shard_dir: &Path,
        next_file_id: &mut u64,
        epoch: u64,
    ) -> Result<u64, String> {
        match self.try_compact_file(file_id, db_index, shard_dir, next_file_id, epoch) {
            Ok(read) => Ok(read),
            Err(why) => {
                self.reclaim.skip.insert(file_id);
                Err(why)
            }
        }
    }

    fn try_compact_file(
        &mut self,
        file_id: u64,
        db_index: usize,
        shard_dir: &Path,
        next_file_id: &mut u64,
        epoch: u64,
    ) -> Result<u64, String> {
        let live = self.file_refs.get(&file_id).copied().unwrap_or(0) as usize;
        if live == 0 {
            return Err("no live key left (the orphan sweep's file)".to_string());
        }
        let raw = std::fs::read(heap_path(shard_dir, file_id)).map_err(|e| e.to_string())?;

        // The survivors: slots whose key's index entry is exactly this slot.
        let mut survivors: Vec<(Bytes, ColdLocation, Bytes)> = Vec::with_capacity(live);
        for (page_idx, chunk) in raw.chunks_exact(PAGE_4K).enumerate() {
            if classify_page(chunk) != PageVerdict::Leaf {
                continue;
            }
            let mut buf = [0u8; PAGE_4K];
            buf.copy_from_slice(chunk);
            let Some(page) = KvLeafPage::from_bytes(buf) else {
                continue;
            };
            for slot_idx in 0..page.slot_count() {
                let Some(kv) = page.get(slot_idx) else {
                    continue;
                };
                let Some(loc) = self.lookup(&kv.key) else {
                    continue;
                };
                if loc.file_id != file_id
                    || loc.page_idx != page_idx as u32
                    || loc.slot_idx != slot_idx
                {
                    continue;
                }
                let value = if kv.flags & entry_flags::OVERFLOW != 0 {
                    let start = kv
                        .value
                        .get(..4)
                        .and_then(|b| <[u8; 4]>::try_from(b).ok())
                        .map(u32::from_le_bytes)
                        .ok_or_else(|| "broken overflow pointer".to_string())?;
                    read_overflow_chain(&raw, start as usize)
                        .ok_or_else(|| "broken overflow chain".to_string())?
                } else {
                    kv.value
                };
                survivors.push((Bytes::from(kv.key), loc, Bytes::from(value)));
            }
        }
        if survivors.len() != live {
            return Err(format!(
                "{} of {live} live slots decoded; the file stays as it is",
                survivors.len()
            ));
        }

        // Write them through the spill writer, one fresh id per request as
        // the durable batch spill does (a sub-batch is named after its first).
        let mut requests: Vec<SpillRequest> = survivors
            .iter()
            .map(|(key, loc, value)| {
                let id = *next_file_id;
                *next_file_id += 1;
                SpillRequest {
                    key: key.clone(),
                    db_index,
                    value_bytes: value.clone(),
                    value_type: loc.value_type,
                    flags: if loc.ttl_ms.is_some() {
                        entry_flags::HAS_TTL
                    } else {
                        0
                    },
                    ttl_ms: loc.ttl_ms,
                    file_id: id,
                    shard_dir: shard_dir.to_path_buf(),
                }
            })
            .collect();
        let completions = flush_buffer(&mut requests);
        drop(requests);
        if completions.iter().any(|c| !c.success) {
            for c in completions.iter().filter(|c| c.success) {
                discard_output(shard_dir, c.file_entry.file_id);
            }
            return Err("writing the compacted file failed".to_string());
        }

        // Pair each written entry with the slot it was read from (the writer
        // keeps request order; checked, not assumed).
        let mut from = survivors.into_iter();
        let mut outputs = Vec::with_capacity(completions.len());
        let mut bytes = std::mem::size_of::<PendingCompaction>();
        for c in completions {
            let new_file = c.file_entry.file_id;
            let mut moved = Vec::with_capacity(c.entries.len());
            for e in c.entries {
                let Some((key, loc, _)) = from.next() else {
                    discard_output(shard_dir, new_file);
                    return Err("the spill writer returned more entries than it was given".into());
                };
                if key != e.key {
                    discard_output(shard_dir, new_file);
                    return Err("the spill writer reordered its entries".into());
                }
                bytes += key.len() + std::mem::size_of::<Moved>();
                moved.push(Moved {
                    key,
                    from: loc,
                    to: ColdLocation {
                        file_id: new_file,
                        page_idx: e.page_idx,
                        slot_idx: e.slot_idx,
                        ttl_ms: e.ttl_ms,
                        value_type: e.value_type,
                    },
                });
            }
            bytes += std::mem::size_of::<CompactedFile>();
            outputs.push(CompactedFile {
                entry: c.file_entry,
                moved,
            });
        }
        self.reclaim.bytes += bytes;
        self.reclaim.pending.push(PendingCompaction {
            old_file: file_id,
            epoch,
            outputs,
            bytes,
        });
        AWAITING_FOLD.fetch_add(1, Ordering::Relaxed);
        Ok(raw.len() as u64)
    }

    /// Adopt every pending compaction whose fold has committed: a committed
    /// fold's snapshot epoch `committed_floor` is above the compaction's
    /// stamp (see the module doc for why nothing may happen earlier).
    ///
    /// In order: list every output that still backs an unchanged survivor
    /// and make the manifest durable (one commit); re-point those survivors
    /// and record the new slots of the ones that changed as dead; unlink the
    /// old files nothing references any more (their ledger entries go with
    /// them). A failed commit adopts nothing — the outputs are unlisted
    /// again and removed, the survivors stay where they were — and the files
    /// may be compacted again later.
    pub fn adopt_compactions(
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
        let held: usize = ready.iter().map(|p| p.bytes).sum();
        self.reclaim.bytes = self.reclaim.bytes.saturating_sub(held);

        // 1. List what is still worth listing; one durable commit.
        let mut listed: Vec<(CompactedFile, Vec<bool>)> = Vec::new();
        let mut old_files: Vec<u64> = Vec::with_capacity(ready.len());
        for compaction in ready {
            old_files.push(compaction.old_file);
            for out in compaction.outputs {
                let unchanged: Vec<bool> = out
                    .moved
                    .iter()
                    .map(|m| self.lookup(&m.key) == Some(m.from))
                    .collect();
                if !unchanged.contains(&true) {
                    // Every survivor changed since: nothing would point at
                    // the copy, so it is never listed.
                    discard_output(shard_dir, out.entry.file_id);
                    continue;
                }
                match manifest.add_file(out.entry.clone()) {
                    Ok(()) => listed.push((out, unchanged)),
                    Err(e) => {
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
        }
        if !listed.is_empty()
            && let Err(e) = manifest.commit()
        {
            for (out, _) in &listed {
                manifest.remove_file(out.entry.file_id, PageType::KvLeaf);
                discard_output(shard_dir, out.entry.file_id);
            }
            tracing::error!(
                err = %e,
                files = listed.len(),
                "cold reclaim: manifest commit failed; the compacted files are not adopted and \
                 their keys stay in the files they were read from"
            );
            return report;
        }
        report.files_listed = listed.len();

        // 2. Re-point the survivors that did not move; the rest leave a dead
        //    slot in a now-listed file.
        for (out, unchanged) in listed {
            for (m, same) in out.moved.into_iter().zip(unchanged) {
                if same {
                    self.insert(m.key, m.to);
                    report.keys_moved += 1;
                } else {
                    self.note_dead_slot(m.to.file_id, m.key, m.to.ttl_ms);
                }
            }
        }

        // 3. Unlink the compacted files nothing references now.
        old_files.retain(|id| !self.file_refs.contains_key(id));
        let queued = |ci: &ColdIndex| {
            old_files
                .iter()
                .filter(|id| ci.pending_unlink.contains(id))
                .count()
        };
        let before = queued(self);
        match self.unlink_now(&old_files, shard_dir, Some(manifest)) {
            Ok(bytes) => {
                report.files_unlinked = before - queued(self);
                report.bytes_unlinked = bytes;
            }
            Err(e) => tracing::error!(
                err = %e,
                "cold reclaim: manifest commit after unlinking compacted files failed; the \
                 orphan sweep retries"
            ),
        }
        report
    }
}
