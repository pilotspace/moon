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
#[derive(Debug, Clone, Copy)]
pub struct ColdLocation {
    /// Manifest file_id of the heap DataFile.
    pub file_id: u64,
    /// Slot index within the KvLeafPage (currently single-page files).
    pub slot_idx: u16,
}

/// In-memory index from key to cold disk location.
///
/// NOT on the hot path -- only consulted when DashTable lookup misses
/// and disk-offload is enabled.
#[derive(Debug)]
pub struct ColdIndex {
    map: HashMap<Bytes, ColdLocation>,
}

impl ColdIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Record a spilled key's disk location.
    pub fn insert(&mut self, key: Bytes, location: ColdLocation) {
        self.map.insert(key, location);
    }

    /// Remove a key from the cold index (e.g., when promoted back to RAM).
    pub fn remove(&mut self, key: &[u8]) {
        self.map.remove(key);
    }

    /// Look up a key's cold location.
    pub fn lookup(&self, key: &[u8]) -> Option<ColdLocation> {
        self.map.get(key).copied()
    }

    /// Merge another ColdIndex into this one (used during recovery).
    pub fn merge(&mut self, other: ColdIndex) {
        self.map.extend(other.map);
    }

    /// Number of entries tracked.
    pub fn len(&self) -> usize {
        self.map.len()
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
        mut manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    ) -> std::io::Result<SweepStats> {
        let mut stats = SweepStats::default();
        let data_dir = shard_dir.join("data");

        for key in &orphan_keys {
            let loc = match self.map.get(key.as_ref()) {
                Some(l) => *l,
                None => continue, // already removed by a concurrent path
            };

            let file_path = data_dir.join(format!("heap-{:06}.mpf", loc.file_id));

            // Determine file size before deletion for byte accounting.
            let file_bytes = std::fs::metadata(&file_path)
                .map(|m| m.len())
                .unwrap_or(0);

            // Delete the DataFile (idempotent — missing = already gone).
            match std::fs::remove_file(&file_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        key = %String::from_utf8_lossy(key),
                        file = %file_path.display(),
                        err = %e,
                        "orphan_sweep: failed to delete cold DataFile — skipping",
                    );
                    continue;
                }
            }

            // Tombstone manifest entry so GC / recovery doesn't re-index it.
            if let Some(ref mut m) = manifest.as_deref_mut() {
                m.remove_file(loc.file_id);
            }

            // Remove from in-memory cold index.
            self.map.remove(key.as_ref());

            stats.entries_reclaimed += 1;
            stats.bytes_reclaimed = stats.bytes_reclaimed.saturating_add(file_bytes);

            crate::command::info_reclamation::record_cold_orphan_reclaim(file_bytes);

            tracing::debug!(
                key = %String::from_utf8_lossy(key),
                file_id = loc.file_id,
                bytes = file_bytes,
                "orphan_sweep: reclaimed cold-tier orphan",
            );
        }

        // Single manifest commit for all tombstones in this batch.
        if stats.entries_reclaimed > 0 {
            if let Some(m) = manifest {
                if let Err(e) = m.commit() {
                    tracing::error!(err = %e, "orphan_sweep: manifest commit failed");
                    return Err(e);
                }
            }
            tracing::info!(
                entries = stats.entries_reclaimed,
                bytes = stats.bytes_reclaimed,
                "orphan_sweep: completed",
            );
        }

        Ok(stats)
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
        use crate::persistence::page::PageType;

        let mut index = Self::new();
        let data_dir = shard_dir.join("data");

        for entry in manifest.files() {
            if entry.status == FileStatus::Active && entry.file_type == PageType::KvLeaf as u8 {
                let heap_path = data_dir.join(format!("heap-{:06}.mpf", entry.file_id));
                if let Ok(pages) = crate::persistence::kv_page::read_datafile(&heap_path) {
                    for page in &pages {
                        for slot_idx in 0..page.slot_count() {
                            if let Some(kv) = page.get(slot_idx) {
                                index.insert(
                                    Bytes::from(kv.key),
                                    ColdLocation {
                                        file_id: entry.file_id,
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
            slot_idx: 0,
        };
        idx.insert(Bytes::from_static(b"key1"), loc);
        assert_eq!(idx.len(), 1);
        let found = idx.lookup(b"key1").unwrap();
        assert_eq!(found.file_id, 1);
        assert_eq!(found.slot_idx, 0);
        idx.remove(b"key1");
        assert!(idx.lookup(b"key1").is_none());
    }
}
