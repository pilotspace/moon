//! In-memory cold index tracking KV entries spilled to disk DataFiles.
//!
//! Maps key bytes to (file_id, slot_idx) for O(1) cold lookup.
//! Populated at spill time, rebuilt from heap DataFiles during recovery.

use std::collections::HashMap;
use std::path::Path;

use bytes::Bytes;

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
