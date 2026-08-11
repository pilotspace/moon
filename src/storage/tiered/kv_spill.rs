//! KV spill-to-disk: serialize evicted entries to KvLeafPage DataFiles.
//!
//! When `disk_offload_enabled`, eviction writes entries to `.mpf` files
//! instead of permanently deleting them.

use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use tracing::warn;

use super::kv_serde;
use crate::persistence::kv_page::{
    KvLeafPage, KvOverflowPage, PageFull, ValueType, build_overflow_chain, entry_flags,
    write_datafile, write_datafile_mixed,
};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::{PAGE_4K, PageType};
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;

/// Outcome of building a spill page set: a finalized leaf page, the overflow
/// chain (empty unless the value didn't fit), and the total page count.
///
/// Both the synchronous (`spill_to_datafile`) and asynchronous
/// (`SpillThread::write_spill_file`) paths construct identical leaf/overflow
/// layouts; this helper is the single source of truth for that layout.
pub struct KvSpillPages {
    pub leaf: KvLeafPage,
    pub overflow: Vec<crate::persistence::kv_page::KvOverflowPage>,
    pub total_pages: u32,
}

/// Build the leaf + overflow page set for a spilled KV entry.
///
/// Returns `Ok(KvSpillPages)` on success. Returns `Err(io::ErrorKind::InvalidData)`
/// if the key itself is too large to fit in a leaf page even alongside an
/// overflow pointer (an irrecoverable layout failure for that key).
pub fn build_kv_spill_pages(
    key: &[u8],
    value_bytes: &[u8],
    value_type: ValueType,
    flags: u8,
    ttl_ms: Option<u64>,
    file_id: u64,
) -> io::Result<KvSpillPages> {
    let mut leaf = KvLeafPage::new(0, file_id);

    let (overflow, total_pages) = match leaf.insert(key, value_bytes, value_type, flags, ttl_ms) {
        Ok(_) => (Vec::new(), 1u32),
        Err(PageFull) => {
            // Build the overflow chain and reinsert the key with an overflow pointer.
            let chain = build_overflow_chain(value_bytes, file_id, 1);
            let chain_len = chain.len() as u32;
            let overflow_ptr = 1u32.to_le_bytes();
            let overflow_flags = flags | entry_flags::OVERFLOW;
            match leaf.insert(key, &overflow_ptr, value_type, overflow_flags, ttl_ms) {
                Ok(_) => {}
                Err(PageFull) => {
                    warn!(
                        key_len = key.len(),
                        "kv_spill: key too large for leaf page even with overflow pointer"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key too large for leaf page",
                    ));
                }
            }
            (chain, 1 + chain_len)
        }
    };

    leaf.finalize();

    Ok(KvSpillPages {
        leaf,
        overflow,
        total_pages,
    })
}

/// Write a previously-built `KvSpillPages` to `{shard_dir}/data/heap-{file_id:06}.mpf`.
///
/// Returns the byte size of the written file. The caller is responsible for
/// updating the manifest / cold index after this returns.
pub fn write_kv_spill_pages(
    shard_dir: &Path,
    file_id: u64,
    pages: &KvSpillPages,
) -> io::Result<u64> {
    let data_dir = shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;
    let file_path = data_dir.join(format!("heap-{file_id:06}.mpf"));

    if pages.overflow.is_empty() {
        write_datafile(&file_path, &[&pages.leaf])?;
    } else {
        write_datafile_mixed(&file_path, &pages.leaf, &pages.overflow)?;
    }
    // Fsync the directory so the new file's directory entry survives a crash —
    // the manifest (which IS dir-fsynced) must never reference a vanished file.
    crate::persistence::fsync::fsync_directory(&data_dir)?;

    Ok((pages.total_pages as u64) * (PAGE_4K as u64))
}

/// On-disk [`ValueType`] tag for a hot value.
///
/// Re-exported from the shared value codec (W1 unification) — used by every
/// spill entry point and by the sync eviction path to populate
/// `ColdLocation::value_type` (#364).
pub use crate::storage::value_codec::value_type_of;

/// Spill a single KV entry to its own DataFile on disk.
///
/// Creates a `.mpf` file at `{shard_dir}/data/heap-{file_id:06}.mpf` (leaf
/// page + overflow chain for oversized values), writes the entry, and
/// registers the file in the shard manifest with a blocking durable commit.
///
/// **No longer a production eviction path (W2).** Every eviction spill —
/// sync and async — now routes through `spill_thread::flush_buffer` batches
/// (shared multi-entry files, one manifest commit per file). This helper
/// remains as the single-entry primitive: test fixtures use it to place one
/// key cold deterministically (same `build_kv_spill_pages` +
/// `write_kv_spill_pages` layout the batch salvage path emits, so the format
/// stays production-exercised via `spill_single_entry`).
///
/// `db_index` is the logical database the entry was evicted FROM — it is
/// stamped into the manifest `FileEntry` so `rebuild_from_manifest_per_db`
/// re-attaches the key to the right database after a crash (#139). Passing 0
/// for a db>0 victim silently corrupts recovery attribution.
pub fn spill_to_datafile(
    shard_dir: &Path,
    file_id: u64,
    key: &[u8],
    entry: &Entry,
    db_index: usize,
    manifest: &mut ShardManifest,
    cold_index: Option<&mut super::cold_index::ColdIndex>,
) -> io::Result<()> {
    // Determine value type and extract bytes. For collections, serialize via
    // kv_serde; for strings, borrow directly.
    let collection_buf: Vec<u8>;
    let val_ref = entry.as_redis_value();
    let (value_type, value_bytes): (ValueType, &[u8]) = match val_ref {
        RedisValueRef::String(s) => (ValueType::String, s),
        ref other => {
            collection_buf = kv_serde::serialize_collection(other).unwrap_or_default();
            (value_type_of(other), collection_buf.as_slice())
        }
    };

    // Determine flags and TTL
    let mut flags: u8 = 0;
    let ttl_ms = if entry.has_expiry() {
        flags |= entry_flags::HAS_TTL;
        Some(entry.expires_at_ms())
    } else {
        None
    };

    // Build leaf + overflow via the shared helper. A "key too large" failure
    // is non-fatal here (legacy behavior) — log and skip the spill.
    let pages = match build_kv_spill_pages(key, value_bytes, value_type, flags, ttl_ms, file_id) {
        Ok(p) => p,
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            warn!(key = %String::from_utf8_lossy(key), "kv_spill: skipping oversized key");
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    let byte_size = write_kv_spill_pages(shard_dir, file_id, &pages)?;

    // Register in manifest
    manifest.add_file(FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12, // 4KB = 2^12
        page_count: pages.total_pages,
        byte_size,
        created_lsn: 0,
        db_index: db_index as u64,
        max_key_hash: 0,
        last_modified_lsn: 0,
    });
    manifest.commit()?;

    // Update cold index with the spilled key's disk location. `ttl_ms` is
    // threaded through so the proactive TTL sweep (R1, H-2) can judge expiry
    // from the in-RAM index alone, without reading this file back.
    if let Some(ci) = cold_index {
        ci.insert(
            Bytes::copy_from_slice(key),
            super::cold_index::ColdLocation {
                file_id,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms,
                value_type,
            },
        );
    }

    Ok(())
}

// ── Multi-page batch spill ───────────────────────────────────────────────────

/// Maximum raw value size (in bytes) for an entry to be eligible for inline
/// batching.  Entries whose serialized value exceeds this threshold are spilled
/// via the existing single-file path (`build_kv_spill_pages`) which handles
/// overflow chains correctly.
///
/// The 4KB leaf page has ~3916B of usable payload after all headers + one slot
/// (PAGE_4K=4096 − MoonPage header 64B − KV header 16B − slot 4B).  After LZ4
/// compression (minimum 256B value → may not shrink) plus key overhead, the
/// safe inline threshold is 3500B.  Using the raw value length is conservative
/// but correct: the caller pre-screens before building the entry, so the batch
/// builder never sees truly oversized values.
pub const INLINE_MAX_VALUE_BYTES: usize = 3500;

/// One entry to include in a spill batch.
pub struct SpillEntry {
    pub key: bytes::Bytes,
    pub value_bytes: bytes::Bytes,
    pub value_type: ValueType,
    pub flags: u8,
    pub ttl_ms: Option<u64>,
}

/// One physical 4KB page within a spill batch file, in final on-disk order.
///
/// A batch file interleaves ordinary `KvLeafPage`s (inline entries, or an
/// overflow-pointer stub for an oversized one) with the `KvOverflowPage`
/// chain of any oversized entry — the same "leaf immediately followed by its
/// own overflow chain" shape [`build_kv_spill_pages`]'s single-entry path
/// already writes, just repeated for every oversized entry that lands in
/// this batch instead of each getting its own dedicated file. This is what
/// makes batching effective regardless of value size (task: spill-file
/// batching, v0.8 close-out) — previously any entry whose value exceeded
/// [`INLINE_MAX_VALUE_BYTES`] was routed around the batch builder entirely to
/// a dedicated single-entry file, so a workload of large values (e.g. 10KB)
/// degenerated to ~1 file per key even though `flush_buffer` was correctly
/// grouping up to `FLUSH_ENTRY_CAP` requests per flush.
pub enum BatchSlot {
    Leaf(KvLeafPage),
    Overflow(KvOverflowPage),
}

impl BatchSlot {
    /// Raw 4KB page bytes, ready to write at this slot's absolute file
    /// position (its index within `BatchPages::pages`).
    fn as_bytes(&self) -> &[u8; PAGE_4K] {
        match self {
            BatchSlot::Leaf(l) => l.as_bytes(),
            BatchSlot::Overflow(o) => o.as_bytes(),
        }
    }
}

/// Result of building a multi-page spill batch.
///
/// `pages` holds every physical page (leaf or overflow) in final file order —
/// a page's index within this `Vec` IS its file-absolute `page_idx`, matching
/// how both the writer (this module) and the independent recovery scan
/// (`ColdIndex::rebuild_from_manifest`, which walks raw `chunks_exact(PAGE_4K)`
/// and skips any chunk that doesn't parse as a `KvLeafPage`) derive it.
/// `locations[i]` is the `(page_idx, slot_idx)` for `entries[i]` — always the
/// LEAF page holding that entry's slot (an overflow-pointer stub for
/// oversized entries, the real value inline otherwise); the read path
/// (`cold_read::read_cold_entry`) already follows the stub's overflow
/// pointer transparently.
pub struct BatchPages {
    pub pages: Vec<BatchSlot>,
    /// Parallel to the *accepted* entries slice: (file-absolute page_idx, slot_idx).
    pub locations: Vec<(u32, u16)>,
}

/// Build a multi-page spill batch from a slice of entries, packing BOTH
/// inline (small) and oversized (overflow-chained) entries into ONE file.
///
/// Entries with `value_bytes.len() <= INLINE_MAX_VALUE_BYTES` are greedily
/// packed into shared `KvLeafPage`s exactly like the pre-existing inline
/// packer: insert into the currently-open leaf, and on `Err(PageFull)`
/// finalize + seal that leaf and retry on a fresh one.
///
/// Entries above the threshold get their own dedicated leaf (sealing
/// whatever inline leaf was open first) holding a 4-byte overflow-pointer
/// stub, immediately followed by that entry's own overflow-page chain — the
/// exact single-entry layout `build_kv_spill_pages` uses, just placed inline
/// in the shared file's page stream instead of a dedicated file. The pointer
/// value (the file-absolute page index of the first overflow page) is known
/// up front because the dedicated leaf's own `page_idx` is fixed the moment
/// it is created (`pages.len()` at that point) — no two-pass patch-up is
/// needed. A subsequent inline entry opens a brand new leaf; oversized
/// entries never share a leaf with inline neighbors.
///
/// Returns `Err(io::ErrorKind::InvalidData)` only in the defensive case where
/// a single entry (its key, or key+overflow-pointer) does not fit even a
/// freshly-created empty leaf — the caller (`flush_buffer`) falls back to the
/// single-file path (`spill_single_entry`) for the whole buffer in that case.
pub fn build_kv_spill_batch(entries: &[SpillEntry], file_id: u64) -> io::Result<BatchPages> {
    let mut pages: Vec<BatchSlot> = Vec::new();
    let mut locations: Vec<(u32, u16)> = Vec::with_capacity(entries.len());

    // The leaf currently being filled with inline entries, plus the
    // file-absolute page index it will occupy once finalized and pushed.
    // `None` right after an oversized entry: it seals whatever inline leaf
    // was open and the next inline entry starts a brand new one.
    let mut current_leaf: Option<(KvLeafPage, u32)> = None;

    for entry in entries {
        if entry.value_bytes.len() <= INLINE_MAX_VALUE_BYTES {
            loop {
                let (leaf, page_idx) = current_leaf.get_or_insert_with(|| {
                    let idx = pages.len() as u32;
                    (KvLeafPage::new(idx as u64, file_id), idx)
                });
                let page_idx = *page_idx;
                let leaf_was_empty = leaf.slot_count() == 0;
                match leaf.insert(
                    &entry.key,
                    &entry.value_bytes,
                    entry.value_type,
                    entry.flags,
                    entry.ttl_ms,
                ) {
                    Ok(slot_idx) => {
                        locations.push((page_idx, slot_idx));
                        break;
                    }
                    Err(PageFull) => {
                        if leaf_was_empty {
                            // Value is too large for a fresh leaf even after
                            // LZ4 — a genuine failure, not a full-page retry.
                            warn!(
                                key_len = entry.key.len(),
                                value_len = entry.value_bytes.len(),
                                "kv_spill batch: entry too large for a fresh inline leaf, skipping"
                            );
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "entry too large for inline leaf page",
                            ));
                        }
                        // Current leaf is full (had prior entries): finalize
                        // + seal it, then retry this SAME entry on a fresh one.
                        if let Some((mut full_leaf, _)) = current_leaf.take() {
                            full_leaf.finalize();
                            pages.push(BatchSlot::Leaf(full_leaf));
                        }
                    }
                }
            }
        } else {
            // Oversized: seal any open inline leaf first — this entry's
            // dedicated leaf + overflow chain must not share a page with
            // inline neighbors (their page_idx is only known once THIS leaf
            // is pushed).
            if let Some((mut open_leaf, _)) = current_leaf.take() {
                open_leaf.finalize();
                pages.push(BatchSlot::Leaf(open_leaf));
            }

            let leaf_page_idx = pages.len() as u32;
            let overflow_start = leaf_page_idx + 1;
            let mut leaf = KvLeafPage::new(leaf_page_idx as u64, file_id);
            let overflow_ptr = overflow_start.to_le_bytes();
            let overflow_flags = entry.flags | entry_flags::OVERFLOW;

            match leaf.insert(
                &entry.key,
                &overflow_ptr,
                entry.value_type,
                overflow_flags,
                entry.ttl_ms,
            ) {
                Ok(slot_idx) => {
                    leaf.finalize();
                    pages.push(BatchSlot::Leaf(leaf));
                    let chain =
                        build_overflow_chain(&entry.value_bytes, file_id, overflow_start as u64);
                    for page in chain {
                        pages.push(BatchSlot::Overflow(page));
                    }
                    locations.push((leaf_page_idx, slot_idx));
                }
                Err(PageFull) => {
                    warn!(
                        key_len = entry.key.len(),
                        "kv_spill batch: key too large for a leaf even with an overflow pointer, skipping"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key too large for leaf page even with overflow pointer",
                    ));
                }
            }
        }
    }

    // Flush any still-open inline leaf.
    if let Some((mut leaf, _)) = current_leaf.take() {
        leaf.finalize();
        pages.push(BatchSlot::Leaf(leaf));
    }

    Ok(BatchPages { pages, locations })
}

/// Write a `BatchPages` to `{shard_dir}/data/heap-{file_id:06}.mpf` atomically.
///
/// Pages are written in `batch.pages` order (their index IS their
/// file-absolute `page_idx` — see `BatchPages` doc). Writes to a `.tmp` file,
/// fsyncs, then renames — so a crash during write leaves no partial file
/// visible at the canonical path; a torn `.tmp` left behind by a kill-9
/// mid-write is reclaimed by the crash-orphan sweep
/// (`classify_orphan_heap_files`), which already recognizes `heap-*.tmp`.
///
/// Returns the total byte size written.
///
/// Hand-rolls the temp+fsync+rename sequence instead of calling
/// `persistence::atomic::atomic_write_durable` (which has the same crash
/// contract) because that helper's signature takes one pre-materialized
/// `&[u8]`: concatenating `batch.pages` into a single contiguous buffer first
/// would undo the whole point of `BATCH_BYTES_CAP` (`spill_thread.rs`) —
/// streaming each already-built 4KB page straight into the temp file via
/// `write_all` in a loop is exactly what keeps this write's own working set
/// at one page at a time, on top of (not doubling) the capped `batch.pages`
/// already resident from `build_kv_spill_batch`.
pub fn write_kv_spill_batch(shard_dir: &Path, file_id: u64, batch: &BatchPages) -> io::Result<u64> {
    use std::io::Write as _;

    let data_dir = shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    let final_path = data_dir.join(format!("heap-{file_id:06}.mpf"));
    let tmp_path = data_dir.join(format!("heap-{file_id:06}.tmp"));

    {
        let mut file = std::fs::File::create(&tmp_path)?;
        for page in &batch.pages {
            file.write_all(page.as_bytes())?;
        }
        file.sync_all()?;
    }

    std::fs::rename(&tmp_path, &final_path)?;
    // Fsync the directory so the rename itself survives a crash — without it
    // the manifest can point at a file whose directory entry was lost.
    crate::persistence::fsync::fsync_directory(&data_dir)?;

    let total_pages = batch.pages.len() as u64;
    Ok(total_pages * PAGE_4K as u64)
}

/// Startup sweep of crash-orphaned heap files in `{shard_dir}/data`.
///
/// Removes `heap-*.mpf` files whose `file_id` is not registered in the
/// manifest (spill wrote the file, crash before manifest commit — invisible
/// to the cold index, would otherwise leak disk forever) and all
/// `heap-*.tmp` leftovers from interrupted atomic-rename batch writes.
///
/// Only call this AFTER the manifest has been opened successfully: with no
/// readable manifest every heap file would look orphaned, and deleting data
/// on a corrupt-manifest signal would be destructive.
///
/// Returns the number of files removed. I/O errors are logged and skipped —
/// a sweep failure must never abort recovery.
///
/// Runs classification + deletion in one synchronous pass — kept for tests
/// and any caller that genuinely wants blocking behavior. The startup path
/// (task #55) instead calls [`classify_orphan_heap_files`] synchronously
/// (cheap: metadata-only) and defers [`remove_orphan_heap_file`] per entry
/// to a background sweep so `remove_file` I/O never blocks readiness.
pub fn sweep_orphan_heap_files(shard_dir: &Path, manifest: &ShardManifest) -> usize {
    let candidates = classify_orphan_heap_files(shard_dir, manifest);
    let removed = candidates.len();
    for path in candidates {
        remove_orphan_heap_file(&path);
    }
    removed
}

/// Classification half of the crash-orphan sweep (task #55).
///
/// Scans `{shard_dir}/data` and returns the paths of `heap-*.mpf` files whose
/// `file_id` is not registered in `manifest`, plus any `heap-*.tmp` leftovers
/// — WITHOUT deleting anything. `read_dir` + a `HashSet` membership test are
/// pure in-memory/metadata work (no `remove_file` syscalls), so this stays
/// fast even at hundreds of thousands of spilled files; the multi-minute
/// startup stall measured in production (G2 bench: ~40s/shard at ~59K files)
/// is the deletion I/O, not the classification.
///
/// Splitting classification from deletion lets the caller run classification
/// synchronously during startup recovery — correct, because it observes the
/// exact manifest state recovery just rebuilt, before the shard has served a
/// single command, so no spill can have raced ahead of it — while deferring
/// the actual `remove_file` calls to a background sweep that runs only after
/// the shard is already serving traffic. The file-id namespace is monotonic
/// and this snapshot is taken before any new spill can occur on this shard,
/// so a path returned here stays correct to delete arbitrarily later:
/// nothing that gets registered afterward can retroactively alias one of
/// these on-disk file ids.
///
/// Only call this AFTER the manifest has been opened successfully — see
/// [`sweep_orphan_heap_files`]'s doc for why.
pub fn classify_orphan_heap_files(shard_dir: &Path, manifest: &ShardManifest) -> Vec<PathBuf> {
    let data_dir = shard_dir.join("data");
    let Ok(read_dir) = std::fs::read_dir(&data_dir) else {
        return Vec::new();
    };
    let registered: std::collections::HashSet<u64> =
        manifest.files().iter().map(|e| e.file_id).collect();

    let mut orphans = Vec::new();
    for dir_entry in read_dir.flatten() {
        let path = dir_entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let orphan = if let Some(id_str) = name
            .strip_prefix("heap-")
            .and_then(|rest| rest.strip_suffix(".mpf"))
        {
            match id_str.parse::<u64>() {
                Ok(file_id) => !registered.contains(&file_id),
                Err(_) => false, // not our naming scheme — leave it alone
            }
        } else {
            // Interrupted batch write: tmp files are always safe to remove.
            name.strip_prefix("heap-")
                .and_then(|rest| rest.strip_suffix(".tmp"))
                .is_some()
        };
        if orphan {
            orphans.push(path);
        }
    }
    orphans
}

/// Delete one file already classified as a crash orphan by
/// [`classify_orphan_heap_files`].
///
/// I/O errors are logged and skipped — a sweep failure must never abort
/// recovery or crash the background reclaim task. A `NotFound` error is not
/// even logged: harmless if something else already reclaimed the path (e.g.
/// a re-run of the sweep, or the classify→delete window overlapping a manual
/// cleanup).
pub fn remove_orphan_heap_file(path: &Path) {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("<unknown>");
    match std::fs::remove_file(path) {
        Ok(()) => {
            tracing::info!("cold-tier sweep: removed crash-orphaned {}", name);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("cold-tier sweep: failed to remove {}: {}", name, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::read_datafile;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::compact_value::CompactValue;
    use crate::storage::entry::{Entry, RedisValue, current_time_ms};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::collections::VecDeque;

    #[test]
    fn test_spill_string_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let entry = Entry::new_string(Bytes::from_static(b"hello world"));
        spill_to_datafile(shard_dir, 1, b"mykey", &entry, 0, &mut manifest, None).unwrap();

        // Verify file was created
        let file_path = shard_dir.join("data/heap-000001.mpf");
        assert!(file_path.exists());

        // Read back and verify
        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);

        let kv_entry = pages[0].get(0).unwrap();
        assert_eq!(kv_entry.key, b"mykey");
        assert_eq!(kv_entry.value, b"hello world");
        assert_eq!(kv_entry.value_type, ValueType::String);
        assert_eq!(kv_entry.ttl_ms, None);

        // Verify manifest was updated
        assert_eq!(manifest.files().len(), 1);
        assert_eq!(manifest.files()[0].file_id, 1);
    }

    #[test]
    fn test_sweep_orphan_heap_files() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        // Registered spill: file 1 in manifest, must survive the sweep.
        let entry = Entry::new_string(Bytes::from_static(b"registered"));
        spill_to_datafile(shard_dir, 1, b"livekey", &entry, 0, &mut manifest, None).unwrap();

        // Crash orphan: heap file on disk but never registered in the manifest
        // (spill wrote the file, crash before manifest commit).
        let data_dir = shard_dir.join("data");
        std::fs::write(data_dir.join("heap-000099.mpf"), [0u8; PAGE_4K]).unwrap();
        // Crash leftover: interrupted atomic-rename batch write.
        std::fs::write(data_dir.join("heap-000050.tmp"), b"partial").unwrap();
        // Unrelated file: must not be touched.
        std::fs::write(data_dir.join("notes.txt"), b"keep me").unwrap();

        let removed = sweep_orphan_heap_files(shard_dir, &manifest);

        assert_eq!(removed, 2, "orphan .mpf + stale .tmp must both be removed");
        assert!(
            data_dir.join("heap-000001.mpf").exists(),
            "registered file must survive"
        );
        assert!(
            !data_dir.join("heap-000099.mpf").exists(),
            "orphan must be unlinked"
        );
        assert!(
            !data_dir.join("heap-000050.tmp").exists(),
            "stale tmp must be unlinked"
        );
        assert!(
            data_dir.join("notes.txt").exists(),
            "unrelated files must survive"
        );
    }

    /// Task #55: `classify_orphan_heap_files` must be a pure, deletion-free
    /// scan — the whole point of splitting it out of `sweep_orphan_heap_files`
    /// is that recovery can run classification synchronously (cheap) while
    /// deferring the actual `remove_file` I/O (measured ~40s/shard at ~59K
    /// files in production) to a background task. If this ever regresses to
    /// deleting inline, callers relying on "classify is safe to run on the
    /// startup critical path" silently reintroduce task #55's readiness
    /// stall.
    #[test]
    fn test_classify_orphan_heap_files_does_not_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        // Registered spill: must be classified as NOT orphaned.
        let entry = Entry::new_string(Bytes::from_static(b"registered"));
        spill_to_datafile(shard_dir, 1, b"livekey", &entry, 0, &mut manifest, None).unwrap();

        let data_dir = shard_dir.join("data");
        // A larger batch of unregistered files, standing in for a
        // production-scale crash-orphan backlog.
        const N: usize = 500;
        for i in 0..N {
            std::fs::write(
                data_dir.join(format!("heap-{:06}.mpf", 100_000 + i)),
                [0u8; 16],
            )
            .unwrap();
        }
        std::fs::write(data_dir.join("heap-000050.tmp"), b"partial").unwrap();

        let candidates = classify_orphan_heap_files(shard_dir, &manifest);

        // Classification found every orphan...
        assert_eq!(
            candidates.len(),
            N + 1,
            "must classify every orphan/tmp file"
        );
        // ...but deleted NONE of them.
        for path in &candidates {
            assert!(
                path.exists(),
                "classify_orphan_heap_files must not delete {:?} — deletion is deferred",
                path
            );
        }
        // The registered file was correctly excluded and untouched.
        assert!(data_dir.join("heap-000001.mpf").exists());
        assert!(
            !candidates.contains(&data_dir.join("heap-000001.mpf")),
            "registered file must not be classified as orphan"
        );

        // Now prove the deferred half actually reclaims them.
        for path in &candidates {
            remove_orphan_heap_file(path);
        }
        for path in &candidates {
            assert!(
                !path.exists(),
                "remove_orphan_heap_file must delete {:?}",
                path
            );
        }
        assert!(
            data_dir.join("heap-000001.mpf").exists(),
            "registered file must still survive after the deferred sweep runs"
        );
    }

    #[test]
    fn test_spill_with_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let mut entry = Entry::new_string(Bytes::from_static(b"expiring"));
        let future_ms = current_time_ms() + 60_000;
        entry.set_expires_at_ms(future_ms);

        spill_to_datafile(shard_dir, 2, b"ttl_key", &entry, 0, &mut manifest, None).unwrap();

        let file_path = shard_dir.join("data/heap-000002.mpf");
        let pages = read_datafile(&file_path).unwrap();
        let kv_entry = pages[0].get(0).unwrap();

        assert_eq!(kv_entry.key, b"ttl_key");
        assert_eq!(kv_entry.value, b"expiring");
        // TTL should be present (stored as absolute ms, derived from seconds)
        assert!(kv_entry.ttl_ms.is_some());
        let stored_ttl = kv_entry.ttl_ms.unwrap();
        assert!(stored_ttl > 0);
    }

    #[test]
    fn test_spill_oversized_uses_overflow() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        // Create an entry that won't fit in a 4KB page even after LZ4.
        // Use a simple hash-like sequence that LZ4 cannot compress.
        let mut big_value = vec![0u8; 4000];
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for b in big_value.iter_mut() {
            // xorshift64
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        let entry = Entry::new_string(Bytes::from(big_value));

        spill_to_datafile(shard_dir, 3, b"big_key", &entry, 0, &mut manifest, None).unwrap();

        // File SHOULD now exist with overflow pages
        let file_path = shard_dir.join("data/heap-000003.mpf");
        assert!(
            file_path.exists(),
            "oversized entry should use overflow pages"
        );

        // Manifest should have an entry with page_count > 1
        assert_eq!(manifest.files().len(), 1);
        assert!(
            manifest.files()[0].page_count > 1,
            "should have overflow pages"
        );

        // Verify the leaf page has OVERFLOW flag
        let file_data = std::fs::read(&file_path).unwrap();
        let mut leaf_buf = [0u8; PAGE_4K];
        leaf_buf.copy_from_slice(&file_data[..PAGE_4K]);
        let leaf = crate::persistence::kv_page::KvLeafPage::from_bytes(leaf_buf).unwrap();
        let kv_entry = leaf.get(0).unwrap();
        assert_ne!(
            kv_entry.flags & entry_flags::OVERFLOW,
            0,
            "OVERFLOW flag should be set"
        );
    }

    #[test]
    fn test_spill_hash_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));

        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::Hash(map));

        spill_to_datafile(shard_dir, 10, b"hash_key", &entry, 0, &mut manifest, None).unwrap();

        let file_path = shard_dir.join("data/heap-000010.mpf");
        assert!(file_path.exists(), "DataFile should exist for hash entry");

        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);

        let kv_entry = pages[0].get(0).unwrap();
        assert_eq!(kv_entry.key, b"hash_key");
        assert_eq!(kv_entry.value_type, ValueType::Hash);

        // Verify deserialization
        let deserialized = kv_serde::deserialize_collection(&kv_entry.value, ValueType::Hash)
            .expect("should deserialize hash");
        match deserialized {
            RedisValue::Hash(result_map) => {
                assert_eq!(result_map.len(), 2);
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"f1")).unwrap(),
                    &Bytes::from_static(b"v1")
                );
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"f2")).unwrap(),
                    &Bytes::from_static(b"v2")
                );
            }
            _ => panic!("expected Hash"),
        }
    }

    #[test]
    fn test_spill_list_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"elem1"));
        list.push_back(Bytes::from_static(b"elem2"));
        list.push_back(Bytes::from_static(b"elem3"));

        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::List(list));

        spill_to_datafile(shard_dir, 11, b"list_key", &entry, 0, &mut manifest, None).unwrap();

        let file_path = shard_dir.join("data/heap-000011.mpf");
        assert!(file_path.exists(), "DataFile should exist for list entry");

        let pages = read_datafile(&file_path).unwrap();
        let kv_entry = pages[0].get(0).unwrap();
        assert_eq!(kv_entry.key, b"list_key");
        assert_eq!(kv_entry.value_type, ValueType::List);

        let deserialized = kv_serde::deserialize_collection(&kv_entry.value, ValueType::List)
            .expect("should deserialize list");
        match deserialized {
            RedisValue::List(result_list) => {
                assert_eq!(result_list.len(), 3);
                assert_eq!(result_list[0], Bytes::from_static(b"elem1"));
                assert_eq!(result_list[1], Bytes::from_static(b"elem2"));
                assert_eq!(result_list[2], Bytes::from_static(b"elem3"));
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn test_spill_overflow_string_roundtrip() {
        use crate::storage::tiered::cold_index::ColdIndex;
        use crate::storage::tiered::cold_read::cold_read_through;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        // 6KB of incompressible data (xorshift PRNG)
        let mut big_value = vec![0u8; 6000];
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for b in big_value.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        let entry = Entry::new_string(Bytes::from(big_value.clone()));

        spill_to_datafile(
            shard_dir,
            50,
            b"overflow_key",
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        // Verify file is multi-page
        let file_path = shard_dir.join("data/heap-000050.mpf");
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert!(
            file_size > PAGE_4K as u64,
            "file should have overflow pages"
        );

        // Read back via cold_read_through
        let result = cold_read_through(&cold_index, shard_dir, b"overflow_key", 0);
        assert!(result.is_some(), "should read overflow entry");
        let (value, _ttl) = result.unwrap();
        match value {
            RedisValue::String(data) => {
                assert_eq!(data.as_ref(), big_value.as_slice());
            }
            _ => panic!("expected String"),
        }
    }

    // ── Multi-page batch tests (TDD: written before implementation) ──────────

    /// Helper: build N entries with distinct keys/values small enough to fit
    /// inline (≤200 bytes each), forcing page overflow by sheer count.
    fn make_inline_entries(n: usize) -> Vec<SpillEntry> {
        (0..n)
            .map(|i| SpillEntry {
                key: bytes::Bytes::from(format!("batch_key_{i:04}")),
                value_bytes: bytes::Bytes::from(format!(
                    "batch_value_{i:04}_padding_to_200_bytes_{:0>150}",
                    i
                )),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
            })
            .collect()
    }

    /// Helper: build N entries whose values are large enough (well above
    /// `INLINE_MAX_VALUE_BYTES`) and incompressible enough that they always
    /// take the overflow-chain path — the exact shape of the G2 workload
    /// (260K keys × 10KB values) that exposed the one-file-per-key
    /// degeneration. `value_len` defaults to 10240 (10KB) when callers want
    /// to match that scenario directly; distinct per-entry xorshift seeds
    /// keep LZ4 from finding cross-entry redundancy.
    fn make_oversized_entries(n: usize, value_len: usize) -> Vec<SpillEntry> {
        (0..n)
            .map(|i| {
                let mut value = vec![0u8; value_len];
                let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE ^ (i as u64).wrapping_mul(0x9E37_79B9);
                for b in value.iter_mut() {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    *b = state as u8;
                }
                SpillEntry {
                    key: bytes::Bytes::from(format!("oversized_key_{i:05}")),
                    value_bytes: bytes::Bytes::from(value),
                    value_type: ValueType::String,
                    flags: 0,
                    ttl_ms: None,
                }
            })
            .collect()
    }

    /// A KvLeafPage holds roughly 7-10 entries of ~200 B each.  Generating 50
    /// entries guarantees ≥ 2 leaf pages.
    #[test]
    fn test_build_kv_spill_batch_multi_page() {
        const N: usize = 50;
        let entries = make_inline_entries(N);
        let file_id = 42u64;

        let batch =
            build_kv_spill_batch(&entries, file_id).expect("build_kv_spill_batch should succeed");

        // Must have spanned at least 2 leaf pages.
        assert!(
            batch.pages.len() >= 2,
            "expected ≥2 leaf pages, got {}",
            batch.pages.len()
        );
        // One location per entry.
        assert_eq!(batch.locations.len(), N);

        // page_idx values must be monotonically non-decreasing and within range.
        let max_page = batch.pages.len() as u32 - 1;
        for (i, &(page_idx, slot_idx)) in batch.locations.iter().enumerate() {
            assert!(
                page_idx <= max_page,
                "entry {i}: page_idx {page_idx} out of range (max {max_page})"
            );
            let _ = slot_idx; // just assert no panic
        }
    }

    /// write_kv_spill_batch must produce an atomic file and
    /// read_cold_entry_at must recover every entry by (page_idx, slot_idx).
    #[test]
    fn test_write_and_read_batch_multi_page() {
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;

        const N: usize = 50;
        let entries = make_inline_entries(N);
        let file_id = 77u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        assert!(batch.pages.len() >= 2, "test requires ≥2 leaf pages");

        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();
        assert!(byte_size > 0);

        // The file must exist at the canonical path (not the .tmp).
        let file_path = shard_dir
            .join("data")
            .join(format!("heap-{file_id:06}.mpf"));
        assert!(
            file_path.exists(),
            "batch file should exist at canonical path"
        );
        let tmp_path = shard_dir
            .join("data")
            .join(format!("heap-{file_id:06}.tmp"));
        assert!(!tmp_path.exists(), ".tmp file should be renamed away");

        // Round-trip: every entry must be readable by its location.
        for (i, (&(page_idx, slot_idx), entry)) in
            batch.locations.iter().zip(entries.iter()).enumerate()
        {
            let loc = ColdLocation {
                file_id,
                page_idx,
                slot_idx,
                ttl_ms: None,
                value_type: ValueType::String,
            };
            let result = read_cold_entry_at(shard_dir, loc, 0);
            assert!(
                result.is_some(),
                "entry {i} (key={}) not readable at page_idx={page_idx} slot_idx={slot_idx}",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => {
                    assert_eq!(
                        data.as_ref(),
                        entry.value_bytes.as_ref(),
                        "entry {i}: value mismatch"
                    );
                }
                _ => panic!("entry {i}: expected String"),
            }
        }
    }

    /// An entry at page_idx=3 slot=2 (deep in the batch) resolves correctly.
    #[test]
    fn test_batch_deep_page_slot_resolves() {
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;

        // Generate enough entries to reach page_idx ≥ 3.
        const N: usize = 100;
        let entries = make_inline_entries(N);
        let file_id = 88u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        // Find the first entry with page_idx >= 3 and slot_idx >= 1.
        let target = batch
            .locations
            .iter()
            .zip(entries.iter())
            .enumerate()
            .find(|&(_, (&(page_idx, slot_idx), _))| page_idx >= 3 && slot_idx >= 1);

        if let Some((i, (&(page_idx, slot_idx), entry))) = target {
            let loc = ColdLocation {
                file_id,
                page_idx,
                slot_idx,
                ttl_ms: None,
                value_type: ValueType::String,
            };
            let result = read_cold_entry_at(shard_dir, loc, 0);
            assert!(
                result.is_some(),
                "deep entry {i} (page={page_idx} slot={slot_idx}) not readable"
            );
            let (value, _) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => {
                    assert_eq!(data.as_ref(), entry.value_bytes.as_ref());
                }
                _ => panic!("expected String"),
            }
        } else {
            // Fewer than 100 entries didn't reach page 3 — bump N if this fires.
            panic!(
                "test needs more entries to reach page_idx≥3 slot≥1; got {} pages",
                batch.pages.len()
            );
        }
    }

    /// #139 recovery attribution: two spill files tagged with different
    /// `FileEntry::db_index` values must rebuild into SEPARATE per-db cold
    /// indexes, each holding exactly its own file's keys — the db0-only
    /// attach used to make every SELECT >0 spilled key unreachable after
    /// restart.
    #[test]
    fn test_rebuild_per_db_attributes_files_to_their_databases() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;
        use crate::storage::tiered::cold_index::ColdIndex;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        // One batch file per db, registered exactly as apply_spill_completions
        // would (db attribution at file granularity).
        for (db, file_id, prefix) in [(0u64, 201u64, "d0:"), (3u64, 202u64, "d3:")] {
            let entries: Vec<SpillEntry> = (0..10)
                .map(|i| SpillEntry {
                    key: Bytes::from(format!("{prefix}{i}")),
                    value_bytes: Bytes::from(format!("v{i}")),
                    value_type: ValueType::String,
                    flags: 0,
                    ttl_ms: None,
                })
                .collect();
            let batch = build_kv_spill_batch(&entries, file_id).unwrap();
            let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();
            manifest.add_file(FileEntry {
                file_id,
                file_type: PageType::KvLeaf as u8,
                status: FileStatus::Active,
                tier: StorageTier::Hot,
                page_size_log2: 12,
                page_count: batch.pages.len() as u32,
                byte_size,
                created_lsn: 0,
                db_index: db,
                max_key_hash: 0,
                last_modified_lsn: 0,
            });
        }
        manifest.commit().unwrap();

        let mut per_db = ColdIndex::rebuild_from_manifest_per_db(shard_dir, &manifest);
        per_db.sort_by_key(|(db, _)| *db);
        let dbs: Vec<usize> = per_db.iter().map(|(db, _)| *db).collect();
        assert_eq!(dbs, vec![0, 3], "one index per db present in the manifest");
        for (db, index) in &per_db {
            assert_eq!(index.len(), 10, "db {db}: every entry recovered");
            let prefix = if *db == 0 { "d0:" } else { "d3:" };
            for i in 0..10 {
                assert!(
                    index.lookup(format!("{prefix}{i}").as_bytes()).is_some(),
                    "db {db}: missing its own key {prefix}{i}"
                );
            }
            let other = if *db == 0 { "d3:0" } else { "d0:0" };
            assert!(
                index.lookup(other.as_bytes()).is_none(),
                "db {db}: must not contain the other db's keys"
            );
        }

        // The merged wrapper still sees everything (single-db callers).
        let merged = ColdIndex::rebuild_from_manifest(shard_dir, &manifest);
        assert_eq!(merged.len(), 20);
    }

    /// RECOVERY-PATH test: prove `ColdIndex::rebuild_from_manifest` reconstructs
    /// the SAME (page_idx, slot_idx) mapping the builder produced.
    ///
    /// The other batch tests read back via the *builder's* returned `locations`.
    /// On crash recovery the cold_index is thrown away and rebuilt by SCANNING
    /// the heap file (`chunks_exact(PAGE_4K).enumerate()` + `slot_count()`) — a
    /// completely independent mapping. If that scan disagrees with the builder
    /// (off-by-one page index, slot ordering, overflow-page miscount), every
    /// cold key returns nil after restart even though the live path is green.
    /// This test exercises the recovery mapping end-to-end, fully in-process.
    #[test]
    fn test_rebuild_from_manifest_roundtrip() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;
        use crate::storage::tiered::cold_index::ColdIndex;
        use crate::storage::tiered::cold_read::cold_read_through;

        // Enough entries to span several leaf pages (the multi-page case is the
        // whole point — single-page would never exercise the page_idx scan).
        const N: usize = 100;
        let entries = make_inline_entries(N);
        let file_id = 123u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        // 1. Build + write the batch file (the live spill path).
        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        assert!(
            batch.pages.len() >= 3,
            "test requires ≥3 leaf pages to exercise page_idx>0 in the rebuild scan"
        );
        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        // 2. Register the file in a manifest exactly as apply_spill_completions does.
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.pages.len() as u32,
            byte_size,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        });
        manifest.commit().unwrap();

        // 3. Rebuild the cold index FROM THE MANIFEST (the recovery path under test).
        //    Note: this throws away `batch.locations` and recomputes everything.
        let rebuilt = ColdIndex::rebuild_from_manifest(shard_dir, &manifest);
        assert_eq!(
            rebuilt.len(),
            N,
            "rebuild must recover every entry (got {} of {N})",
            rebuilt.len()
        );

        // 4. Every key must read back its exact value VIA THE REBUILT INDEX —
        //    not the builder's locations. This is what a real restart does.
        for entry in &entries {
            let result = cold_read_through(&rebuilt, shard_dir, &entry.key, 0);
            assert!(
                result.is_some(),
                "rebuilt index: key {} returned nil (page/slot mapping mismatch)",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => assert_eq!(
                    data.as_ref(),
                    entry.value_bytes.as_ref(),
                    "rebuilt index: key {} resolved to the WRONG value (slot/page swap)",
                    String::from_utf8_lossy(&entry.key)
                ),
                other => panic!("expected String for {:?}, got {other:?}", entry.key),
            }
        }
    }

    // ── v0.8 spill-file batching: oversized entries must share ONE file ──────

    /// RED baseline this test proves fixed: 300 entries with 10KB
    /// (well-above-`INLINE_MAX_VALUE_BYTES`, incompressible) values — the
    /// exact shape of the G2 acceptance workload (260K × 10KB keys) that
    /// produced ~1 heap-*.mpf file per key in production. Before the v0.8
    /// batching fix, `flush_buffer` routed every such entry to its own
    /// dedicated single-entry file regardless of how many requests it
    /// buffered together; `build_kv_spill_batch` now gives each oversized
    /// entry a dedicated leaf + overflow chain INSIDE one shared file.
    #[test]
    fn test_build_kv_spill_batch_oversized_entries_share_one_file() {
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;

        const N: usize = 300;
        let entries = make_oversized_entries(N, 10 * 1024);
        let file_id = 900u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch =
            build_kv_spill_batch(&entries, file_id).expect("oversized batch build must succeed");
        assert_eq!(
            batch.locations.len(),
            N,
            "every oversized entry must get a location"
        );

        write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        // The whole point: ONE file on disk for all N entries, not N files.
        let data_dir = shard_dir.join("data");
        let mpf_count = std::fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == std::ffi::OsStr::new("mpf"))
            })
            .count();
        assert_eq!(
            mpf_count, 1,
            "batching {N} oversized entries must produce exactly one .mpf file, got {mpf_count}"
        );

        // Every entry must resolve back to its exact value through the
        // overflow-pointer stub the builder placed for it.
        for (i, (&(page_idx, slot_idx), entry)) in
            batch.locations.iter().zip(entries.iter()).enumerate()
        {
            let loc = ColdLocation {
                file_id,
                page_idx,
                slot_idx,
                ttl_ms: None,
                value_type: ValueType::String,
            };
            let result = read_cold_entry_at(shard_dir, loc, 0);
            assert!(
                result.is_some(),
                "oversized entry {i} not readable at page_idx={page_idx} slot_idx={slot_idx}"
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => {
                    assert_eq!(
                        data.as_ref(),
                        entry.value_bytes.as_ref(),
                        "oversized entry {i}: value mismatch"
                    );
                }
                _ => panic!("entry {i}: expected String"),
            }
        }
    }

    /// A single batch mixing small (inline) and large (overflow-chained)
    /// entries must resolve every key correctly, with inline entries packed
    /// several-per-leaf and each oversized entry on its own dedicated leaf —
    /// proving the two packing strategies compose within one file rather
    /// than requiring separate files per size class.
    #[test]
    fn test_build_kv_spill_batch_mixed_inline_and_oversized() {
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;

        let mut entries = make_inline_entries(20);
        entries.extend(make_oversized_entries(20, 8000));
        // Interleave so an oversized entry can land in the middle of a run
        // of inline entries, exercising the "seal the open leaf, dedicate a
        // fresh one, then resume inline packing" transition in both
        // directions.
        let mut interleaved = Vec::with_capacity(entries.len());
        let (inline_part, oversized_part) = entries.split_at(20);
        for i in 0..20 {
            interleaved.push(clone_spill_entry(&inline_part[i]));
            interleaved.push(clone_spill_entry(&oversized_part[i]));
        }

        let file_id = 901u64;
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch =
            build_kv_spill_batch(&interleaved, file_id).expect("mixed batch build must succeed");
        assert_eq!(batch.locations.len(), interleaved.len());

        write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        for (i, (&(page_idx, slot_idx), entry)) in
            batch.locations.iter().zip(interleaved.iter()).enumerate()
        {
            let loc = ColdLocation {
                file_id,
                page_idx,
                slot_idx,
                ttl_ms: None,
                value_type: ValueType::String,
            };
            let result = read_cold_entry_at(shard_dir, loc, 0);
            assert!(
                result.is_some(),
                "mixed-batch entry {i} (key={}) not readable",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => {
                    assert_eq!(
                        data.as_ref(),
                        entry.value_bytes.as_ref(),
                        "mixed-batch entry {i}: value mismatch"
                    );
                }
                _ => panic!("entry {i}: expected String"),
            }
        }
    }

    /// Recovery-path counterpart to `test_rebuild_from_manifest_roundtrip`,
    /// but for a batch of OVERSIZED (overflow-chained) entries — the exact
    /// shape the v0.8 fix newly allows to batch together. Proves
    /// `ColdIndex::rebuild_from_manifest`'s independent
    /// `chunks_exact(PAGE_4K)` scan agrees with the builder's own
    /// `locations` for entries whose leaf holds an overflow-pointer stub,
    /// not the value itself — a crash right after this file's manifest
    /// commit must still resolve every key correctly on restart.
    #[test]
    fn test_rebuild_from_manifest_oversized_batch_roundtrip() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;
        use crate::storage::tiered::cold_index::ColdIndex;
        use crate::storage::tiered::cold_read::cold_read_through;

        const N: usize = 40;
        let entries = make_oversized_entries(N, 6000);
        let file_id = 902u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.pages.len() as u32,
            byte_size,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        });
        manifest.commit().unwrap();

        let rebuilt = ColdIndex::rebuild_from_manifest(shard_dir, &manifest);
        assert_eq!(
            rebuilt.len(),
            N,
            "rebuild must recover every oversized entry (got {} of {N})",
            rebuilt.len()
        );

        for entry in &entries {
            let result = cold_read_through(&rebuilt, shard_dir, &entry.key, 0);
            assert!(
                result.is_some(),
                "rebuilt index: oversized key {} returned nil",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => assert_eq!(
                    data.as_ref(),
                    entry.value_bytes.as_ref(),
                    "rebuilt index: oversized key {} resolved to the WRONG value",
                    String::from_utf8_lossy(&entry.key)
                ),
                other => panic!("expected String for {:?}, got {other:?}", entry.key),
            }
        }
    }

    /// review FIX 1 gap: neither `test_build_kv_spill_batch_mixed_inline_and_oversized`
    /// (uses the BUILDER's own known `batch.locations`, never exercises the
    /// manifest scan) nor `test_rebuild_from_manifest_oversized_batch_roundtrip`
    /// (all-oversized, no inline entries in the file) proves that
    /// `ColdIndex::rebuild_from_manifest`'s independent
    /// `chunks_exact(PAGE_4K)` scan (`cold_index.rs`) correctly locates
    /// INLINE entries interleaved with OVERFLOW entries in the SAME batch
    /// file — i.e. the scan, not the builder's own bookkeeping, has to
    /// re-derive page indices for a file shape only this fix's batching can
    /// produce. This is exactly the gap the real-server integration test
    /// (`tests/crash_recovery_spill_batch_kill9.rs`) caught.
    #[test]
    fn test_rebuild_from_manifest_mixed_inline_and_oversized_roundtrip() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;
        use crate::storage::tiered::cold_index::ColdIndex;
        use crate::storage::tiered::cold_read::cold_read_through;

        let mut entries = make_inline_entries(20);
        entries.extend(make_oversized_entries(20, 8000));
        let mut interleaved = Vec::with_capacity(entries.len());
        let (inline_part, oversized_part) = entries.split_at(20);
        for i in 0..20 {
            interleaved.push(clone_spill_entry(&inline_part[i]));
            interleaved.push(clone_spill_entry(&oversized_part[i]));
        }

        let file_id = 903u64;
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch = build_kv_spill_batch(&interleaved, file_id).expect("mixed batch build");
        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.pages.len() as u32,
            byte_size,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        });
        manifest.commit().unwrap();

        let rebuilt = ColdIndex::rebuild_from_manifest(shard_dir, &manifest);
        assert_eq!(
            rebuilt.len(),
            interleaved.len(),
            "rebuild must recover every entry (got {} of {})",
            rebuilt.len(),
            interleaved.len()
        );

        for entry in &interleaved {
            let result = cold_read_through(&rebuilt, shard_dir, &entry.key, 0);
            assert!(
                result.is_some(),
                "rebuilt index: key {} returned nil",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => assert_eq!(
                    data.as_ref(),
                    entry.value_bytes.as_ref(),
                    "rebuilt index: key {} resolved to the WRONG value ({} bytes, expected {})",
                    String::from_utf8_lossy(&entry.key),
                    data.len(),
                    entry.value_bytes.len(),
                ),
                other => panic!("expected String for {:?}, got {other:?}", entry.key),
            }
        }
    }

    /// `SpillEntry` has no `Clone` derive (deliberately — production code
    /// never needs to duplicate one); tests that need to interleave entries
    /// from two pre-built vectors clone field-by-field instead.
    fn clone_spill_entry(e: &SpillEntry) -> SpillEntry {
        SpillEntry {
            key: e.key.clone(),
            value_bytes: e.value_bytes.clone(),
            value_type: e.value_type,
            flags: e.flags,
            ttl_ms: e.ttl_ms,
        }
    }
}
