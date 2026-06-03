//! In-memory cold index tracking KV entries spilled to disk DataFiles.
//!
//! Maps key bytes to (file_id, slot_idx) for O(1) cold lookup.
//! Populated at spill time, rebuilt from heap DataFiles during recovery.

use std::collections::HashMap;
use std::path::Path;

use bytes::Bytes;

/// Statistics returned by [`ColdIndex::orphan_sweep`].
///
/// An orphan is a cold entry whose key has been re-written to the hot
/// in-memory DashTable. The cold copy is stale and safe to delete.
#[derive(Debug, Default, Clone, Copy)]
pub struct SweepStats {
    /// Number of cold index entries removed (one per orphaned key).
    pub entries_reclaimed: usize,
    /// Sum of DataFile sizes deleted, in bytes.
    pub bytes_reclaimed: u64,
}

/// Location of a cold KV entry on disk.
///
/// Multi-page spill files store many KV entries across several KvLeafPages.
/// `page_idx` is the FILE-ABSOLUTE 4 KB chunk index (0-based); `slot_idx`
/// is the slot within that page.  For the legacy single-page path, both are
/// always 0.
#[derive(Debug, Clone, Copy)]
pub struct ColdLocation {
    /// Manifest file_id of the heap DataFile.
    pub file_id: u64,
    /// File-absolute 4KB page index within the DataFile (0 = first page).
    pub page_idx: u32,
    /// Slot index within the KvLeafPage at `page_idx`.
    pub slot_idx: u16,
}

/// In-memory index from key to cold disk location.
///
/// NOT on the hot path -- only consulted when DashTable lookup misses
/// and disk-offload is enabled.
#[derive(Debug)]
pub struct ColdIndex {
    map: HashMap<Bytes, ColdLocation>,
    /// Reverse liveness: `file_id` -> count of live `map` entries pointing at it.
    ///
    /// A batched spill file (`heap-NNNNNN.mpf`) holds up to `FLUSH_ENTRY_CAP`
    /// (256) KVs across its pages, so it is safe to unlink ONLY when this count
    /// reaches 0 (no co-located live key still references it). Deleting a file
    /// on a single orphan key silently orphans its co-located keys — observed
    /// empirically as cold read-through collapsing 200/200 -> 88/200 once the
    /// orphan sweep runs.
    file_refs: HashMap<u64, u32>,
    /// `file_id`s that dropped to zero live refs (via an `insert` overwrite or a
    /// `remove`) and are awaiting unlink. Drained off the hot path by the orphan
    /// sweep ([`Self::drain_pending_unlink`]). Pushed only on a zero-ref
    /// transition (rare), so it does not allocate on the common insert path.
    pending_unlink: Vec<u64>,
}

impl ColdIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            file_refs: HashMap::new(),
            pending_unlink: Vec::new(),
        }
    }

    /// Increment a file's live-ref count.
    #[inline]
    fn ref_inc(&mut self, file_id: u64) {
        *self.file_refs.entry(file_id).or_insert(0) += 1;
    }

    /// Decrement a file's live-ref count. Returns `true` when it reaches zero —
    /// the file no longer backs any live cold entry and may be unlinked.
    #[inline]
    fn ref_dec(&mut self, file_id: u64) -> bool {
        if let Some(c) = self.file_refs.get_mut(&file_id) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.file_refs.remove(&file_id);
                return true;
            }
        }
        false
    }

    /// Record a spilled key's disk location.
    ///
    /// Maintains the reverse [`Self::file_refs`] liveness index. When this call
    /// overwrites an existing entry whose key moves to a *different* file (the
    /// re-eviction case), the old file loses its referrer; if that was its last
    /// referrer the old `file_id` is queued for unlink — the hot∩cold sweep can
    /// never see such a file because no key references it anymore.
    pub fn insert(&mut self, key: Bytes, location: ColdLocation) {
        let new_file = location.file_id;
        if let Some(old) = self.map.insert(key, location) {
            if old.file_id != new_file {
                if self.ref_dec(old.file_id) {
                    self.pending_unlink.push(old.file_id);
                }
                self.ref_inc(new_file);
            }
            // Same file_id (different slot/page): live-ref count is unchanged.
        } else {
            self.ref_inc(new_file);
        }
    }

    /// Remove a key from the cold index (e.g., when promoted back to RAM).
    ///
    /// Decrements the backing file's live-ref count; if this removes the file's
    /// last referrer, the `file_id` is queued for unlink by the next sweep.
    pub fn remove(&mut self, key: &[u8]) {
        if let Some(old) = self.map.remove(key) {
            if self.ref_dec(old.file_id) {
                self.pending_unlink.push(old.file_id);
            }
        }
    }

    /// Look up a key's cold location.
    pub fn lookup(&self, key: &[u8]) -> Option<ColdLocation> {
        self.map.get(key).copied()
    }

    /// Merge another ColdIndex into this one (used during recovery).
    ///
    /// Routes through [`Self::insert`] so the reverse [`Self::file_refs`]
    /// liveness index is rebuilt for the merged entries (a raw `map.extend`
    /// would leave the ref counts inconsistent and break safe reclamation).
    pub fn merge(&mut self, other: ColdIndex) {
        for (key, location) in other.map {
            self.insert(key, location);
        }
    }

    /// Number of entries tracked.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether any zero-ref files are queued for unlink by the next sweep.
    ///
    /// Files orphaned by `insert` overwrite (re-eviction) or `remove`
    /// (promotion) carry no hot∩cold key, so the sweep trigger must consult
    /// this in addition to the orphan-key set — otherwise those files are never
    /// reclaimed on ticks where no key is hot-shadowed.
    pub fn has_pending_unlink(&self) -> bool {
        !self.pending_unlink.is_empty()
    }

    /// Iterate over all cold entries as `(key, location)` pairs.
    ///
    /// Used by the orphan sweeper to walk all entries without taking ownership.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &ColdLocation)> {
        self.map.iter()
    }

    /// Sweep cold entries that are shadowed by a live hot key.
    ///
    /// # Orphan definition
    ///
    /// A cold entry is an **orphan** when `db.is_hot(key)` returns `true` —
    /// the key was re-written to the in-memory DashTable after being spilled.
    /// The cold DataFile is now stale and wastes disk space.
    ///
    /// We deliberately do **NOT** treat "absent from hot tier" as an orphan.
    /// A cold-only entry (present in cold index, absent from DashTable) is the
    /// normal live state for a spilled key — deleting it would destroy user data.
    ///
    /// # Concurrency safety
    ///
    /// The caller must hold the shard write lock for the duration of this call.
    /// - Spill runs under the same write lock, so a spill-in-progress cannot
    ///   race with sweep — the entry won't appear in `cold_index` until spill
    ///   commits, and by then the write lock is held by the sweeper.
    /// - Cold reads (read path) take the shard read lock; the write lock blocks
    ///   them, so no reader can observe a partial deletion.
    ///
    /// # Parameters
    ///
    /// - `db`: read-only reference to the current shard Database (for `is_hot`).
    /// - `shard_dir`: the shard's storage directory (used to locate DataFiles).
    /// - `manifest`: optional shard manifest to tombstone orphaned file entries.
    ///   If `None`, file deletion still proceeds but no manifest update is made
    ///   (useful when disk-offload is enabled without a manifest).
    ///
    /// # Returns
    ///
    /// `Ok(SweepStats)` with counts and bytes reclaimed, or an `io::Error` if
    /// manifest commit fails (individual file-deletion errors are logged and
    /// counted as reclaimed if the index entry is removed regardless).
    /// Sweep cold entries that are shadowed by a live hot key.
    ///
    /// # Orphan definition
    ///
    /// A cold entry is an **orphan** when `db.is_hot(key)` returns `true` —
    /// the key was re-written to the in-memory DashTable after being spilled.
    /// The cold DataFile is now stale and wastes disk space.
    ///
    /// We deliberately do **NOT** treat "absent from hot tier" as an orphan.
    /// A cold-only entry (present in cold index, absent from DashTable) is the
    /// normal live state for a spilled key — deleting it would destroy user data.
    ///
    /// # Concurrency safety
    ///
    /// The caller must hold the shard write lock for the duration of this call.
    /// Spill, read-promotion, and eviction all run under the same write lock,
    /// preventing TOCTOU races with the sweep.
    ///
    /// # Two-phase design
    ///
    /// When `ColdIndex` is a field of `Database` the caller cannot borrow both
    /// `&mut cold_index` and `&db` at the same time. In that case, use the
    /// two-phase pattern with [`sweep_known_orphans`]:
    ///
    /// 1. Collect orphan keys: `ci.iter().filter(|(k,_)| db.is_hot(k)).map(…).collect()`
    /// 2. Delete: `ci.sweep_known_orphans(keys, shard_dir, manifest)`
    ///
    /// This method is the one-shot API for tests where `ColdIndex` is held
    /// separately from `Database`.
    pub fn orphan_sweep(
        &mut self,
        db: &crate::storage::db::Database,
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<SweepStats> {
        // Phase 1: identify orphans — immutable borrow of self.map only.
        let orphan_keys: Vec<Bytes> = self
            .map
            .keys()
            .filter(|key| db.is_hot(key))
            .cloned()
            .collect();

        // Phase 2: delete (immutable db ref not held anymore).
        self.sweep_known_orphans(orphan_keys, shard_dir, manifest)
    }

    /// Delete a pre-identified set of orphan keys from the cold index and disk.
    ///
    /// This is the second phase of the two-phase orphan sweep used when
    /// `ColdIndex` and `Database` share ownership (the caller cannot pass both
    /// `&mut self` and `&Database` simultaneously because the cold index is a
    /// field of Database). The caller computes `orphan_keys` externally:
    ///
    /// ```ignore
    /// // Phase 1: collect while we can borrow db immutably
    /// let orphan_keys: Vec<Bytes> = guard
    ///     .cold_index.as_ref().unwrap()
    ///     .iter()
    ///     .filter(|(k, _)| guard.is_hot(k))
    ///     .map(|(k, _)| k.clone())
    ///     .collect();
    /// // Phase 2: delete (only cold_index is &mut, no db borrow needed)
    /// guard.cold_index.as_mut().unwrap()
    ///     .sweep_known_orphans(orphan_keys, shard_dir, manifest)
    /// ```
    ///
    /// This two-phase approach is also used by unit tests that keep the
    /// `ColdIndex` separate from `Database`.
    pub fn sweep_known_orphans(
        &mut self,
        orphan_keys: Vec<Bytes>,
        shard_dir: &Path,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<SweepStats> {
        let mut stats = SweepStats::default();

        // Phase 1: remove orphan ENTRIES and decrement their files' ref counts.
        // A file becomes an unlink candidate only when its LAST live ref is
        // removed — NEVER on a single orphan key while co-located keys in the
        // same batched `.mpf` still reference it (the data-loss bug this fixes).
        for key in &orphan_keys {
            if let Some(old) = self.map.remove(key.as_ref()) {
                stats.entries_reclaimed += 1;
                if self.ref_dec(old.file_id) {
                    self.pending_unlink.push(old.file_id);
                }
            }
        }

        // Phase 2: unlink files that now have zero live refs (off the hot path).
        stats.bytes_reclaimed = self.drain_pending_unlink(shard_dir, manifest)?;

        if stats.entries_reclaimed > 0 || stats.bytes_reclaimed > 0 {
            tracing::info!(
                entries = stats.entries_reclaimed,
                bytes = stats.bytes_reclaimed,
                "orphan_sweep: completed",
            );
        }

        Ok(stats)
    }

    /// Unlink every `pending_unlink` file that still has zero live refs,
    /// tombstone its manifest entry, and commit once. Returns bytes reclaimed.
    ///
    /// A file is queued only on a zero-ref transition, but a later `insert`
    /// could re-reference the same `file_id` before the drain runs (file ids are
    /// minted monotonically per process, so this is defensive); such files are
    /// skipped. The `.mpf` is unlinked BEFORE the manifest commit, so disk is
    /// freed even when the commit fails (e.g. manifest root overflow at >70
    /// entries) — the commit error is surfaced only after best-effort
    /// reclamation, and a file whose unlink itself errors is re-queued for a
    /// later sweep rather than leaked.
    fn drain_pending_unlink(
        &mut self,
        shard_dir: &Path,
        mut manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<u64> {
        if self.pending_unlink.is_empty() {
            return Ok(0);
        }
        let data_dir = shard_dir.join("data");
        let mut queued = std::mem::take(&mut self.pending_unlink);
        queued.sort_unstable();
        queued.dedup();

        let mut bytes_reclaimed: u64 = 0;
        let mut manifest_dirty = false;
        for file_id in queued {
            // A live ref re-appeared after queueing -> keep the file.
            if self.file_refs.contains_key(&file_id) {
                continue;
            }
            let file_path = data_dir.join(format!("heap-{:06}.mpf", file_id));
            let file_bytes = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);

            // Delete the DataFile (idempotent — missing = already gone).
            match std::fs::remove_file(&file_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        file = %file_path.display(),
                        err = %e,
                        "orphan_sweep: failed to delete cold DataFile — will retry",
                    );
                    // Re-queue rather than leak; a later sweep retries.
                    self.pending_unlink.push(file_id);
                    continue;
                }
            }

            // Tombstone manifest entry so GC / recovery doesn't re-index it.
            if let Some(ref mut m) = manifest.as_deref_mut() {
                m.remove_file(file_id);
                manifest_dirty = true;
            }

            bytes_reclaimed = bytes_reclaimed.saturating_add(file_bytes);
            crate::command::info_reclamation::record_cold_orphan_reclaim(file_bytes);

            tracing::debug!(
                file_id,
                bytes = file_bytes,
                "orphan_sweep: reclaimed zero-ref cold DataFile",
            );
        }

        // Single manifest commit for all tombstones in this drain.
        if manifest_dirty {
            if let Some(m) = manifest {
                if let Err(e) = m.commit() {
                    tracing::error!(err = %e, "orphan_sweep: manifest commit failed");
                    return Err(e);
                }
            }
        }

        Ok(bytes_reclaimed)
    }

    /// Rebuild the cold index from all heap DataFiles in a shard directory.
    ///
    /// Scans manifest for KvLeaf entries, reads each DataFile, and populates
    /// the index. Called during v3 recovery.
    pub fn rebuild_from_manifest(
        shard_dir: &Path,
        manifest: &crate::persistence::manifest::ShardManifest,
    ) -> Self {
        use crate::persistence::manifest::FileStatus;
        use crate::persistence::page::{PAGE_4K, PageType};

        let mut index = Self::new();
        let data_dir = shard_dir.join("data");

        for entry in manifest.files() {
            if entry.status == FileStatus::Active && entry.file_type == PageType::KvLeaf as u8 {
                let heap_path = data_dir.join(format!("heap-{:06}.mpf", entry.file_id));
                // Read raw bytes and iterate by absolute chunk index.
                // `read_datafile` skips overflow pages (returns only KvLeaf pages),
                // so its enumerate index ≠ file-absolute page index in multi-page files.
                // We must use the raw chunk index to produce a correct `page_idx`.
                let raw = match std::fs::read(&heap_path) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                for (page_idx, chunk) in raw.chunks_exact(PAGE_4K).enumerate() {
                    let mut buf = [0u8; PAGE_4K];
                    buf.copy_from_slice(chunk);
                    if let Some(page) = crate::persistence::kv_page::KvLeafPage::from_bytes(buf) {
                        for slot_idx in 0..page.slot_count() {
                            if let Some(kv) = page.get(slot_idx) {
                                index.insert(
                                    Bytes::from(kv.key),
                                    ColdLocation {
                                        file_id: entry.file_id,
                                        page_idx: page_idx as u32,
                                        slot_idx,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cold_index_insert_lookup_remove() {
        let mut idx = ColdIndex::new();
        let loc = ColdLocation {
            file_id: 1,
            page_idx: 0,
            slot_idx: 0,
        };
        idx.insert(Bytes::from_static(b"key1"), loc);
        assert_eq!(idx.len(), 1);
        let found = idx.lookup(b"key1").unwrap();
        assert_eq!(found.file_id, 1);
        assert_eq!(found.page_idx, 0);
        assert_eq!(found.slot_idx, 0);
        idx.remove(b"key1");
        assert!(idx.lookup(b"key1").is_none());
    }

    /// Create a shard dir with a `data/` subdir and a dummy heap-NNNNNN.mpf
    /// file standing in for a batched multi-KV spill file.
    fn make_shard_with_heap(file_ids: &[u64]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        for &fid in file_ids {
            let p = data_dir.join(format!("heap-{:06}.mpf", fid));
            std::fs::write(&p, vec![0xABu8; 4096]).unwrap();
        }
        tmp
    }

    fn heap_path(shard_dir: &Path, file_id: u64) -> std::path::PathBuf {
        shard_dir
            .join("data")
            .join(format!("heap-{:06}.mpf", file_id))
    }

    /// REGRESSION (batch-file shared-deletion data loss): sweeping ONE orphan
    /// key must NOT delete its `.mpf` while a co-located live key still
    /// references the same file. Under spill batching a single file holds up
    /// to 256 KVs; deleting it on one orphan key silently orphans the rest
    /// (observed empirically as cold read-through 200/200 -> 88/200).
    #[test]
    fn test_sweep_retains_file_with_colocated_live_key() {
        let tmp = make_shard_with_heap(&[5]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        // Two keys co-located in the SAME batched file (file_id = 5).
        ci.insert(
            Bytes::from_static(b"k_orphan"),
            ColdLocation {
                file_id: 5,
                page_idx: 0,
                slot_idx: 0,
            },
        );
        ci.insert(
            Bytes::from_static(b"k_live"),
            ColdLocation {
                file_id: 5,
                page_idx: 0,
                slot_idx: 1,
            },
        );

        // Sweep ONLY the orphan key.
        ci.sweep_known_orphans(vec![Bytes::from_static(b"k_orphan")], shard_dir, None)
            .unwrap();

        // The co-located live key must remain resolvable AND its file present.
        assert!(
            ci.lookup(b"k_live").is_some(),
            "co-located live key dropped from cold index",
        );
        assert!(
            heap_path(shard_dir, 5).exists(),
            "DATA LOSS: file holding a live co-located key was deleted",
        );
        // The orphan entry itself is gone.
        assert!(ci.lookup(b"k_orphan").is_none(), "orphan entry not removed");
    }

    /// A batched file is unlinked only once its LAST live ref is removed.
    #[test]
    fn test_sweep_deletes_file_only_when_last_ref_removed() {
        let tmp = make_shard_with_heap(&[7]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"k1"),
            ColdLocation {
                file_id: 7,
                page_idx: 0,
                slot_idx: 0,
            },
        );
        ci.insert(
            Bytes::from_static(b"k2"),
            ColdLocation {
                file_id: 7,
                page_idx: 0,
                slot_idx: 1,
            },
        );

        // Sweep k1: one ref remains (k2) -> file MUST survive.
        ci.sweep_known_orphans(vec![Bytes::from_static(b"k1")], shard_dir, None)
            .unwrap();
        assert!(
            heap_path(shard_dir, 7).exists(),
            "file deleted while k2 still references it",
        );

        // Sweep k2: last ref removed -> file now reclaimed.
        ci.sweep_known_orphans(vec![Bytes::from_static(b"k2")], shard_dir, None)
            .unwrap();
        assert!(
            !heap_path(shard_dir, 7).exists(),
            "file not reclaimed after its last ref was swept",
        );
    }

    /// Re-eviction churn: `insert` overwriting a key's location (old file_id ->
    /// new file_id) drops the old file to zero refs. The hot∩cold sweep can
    /// never see this file (no key references it anymore), so the index must
    /// enqueue it for reclamation and a subsequent sweep must unlink it.
    #[test]
    fn test_overwrite_reclaims_orphaned_old_file() {
        let tmp = make_shard_with_heap(&[10, 11]);
        let shard_dir = tmp.path();

        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"k"),
            ColdLocation {
                file_id: 10,
                page_idx: 0,
                slot_idx: 0,
            },
        );
        // Key re-spilled to a NEW file (re-eviction) -> file 10 orphaned.
        ci.insert(
            Bytes::from_static(b"k"),
            ColdLocation {
                file_id: 11,
                page_idx: 0,
                slot_idx: 0,
            },
        );

        // A sweep with NO orphan keys must still drain the pending unlink.
        ci.sweep_known_orphans(vec![], shard_dir, None).unwrap();

        assert!(
            !heap_path(shard_dir, 10).exists(),
            "orphaned old file (10) not reclaimed after overwrite",
        );
        assert!(
            heap_path(shard_dir, 11).exists(),
            "live file (11) wrongly deleted",
        );
        assert_eq!(ci.lookup(b"k").map(|l| l.file_id), Some(11));
    }
}
