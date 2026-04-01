//! Per-shard VectorStore -- owns all vector indexes for one shard.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as PubSubRegistry).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::vector::filter::PayloadIndex;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::mvcc::manager::TransactionManager;
use crate::vector::segment::compaction;
use crate::vector::segment::{SegmentHolder, SegmentList};
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::DistanceMetric;

/// Metadata describing a vector index (from FT.CREATE).
pub struct IndexMeta {
    /// Index name (e.g., "idx").
    pub name: Bytes,
    /// Original (unpadded) dimension.
    pub dimension: u32,
    /// Padded dimension (next power of 2).
    pub padded_dimension: u32,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// HNSW M parameter (max neighbors per layer).
    pub hnsw_m: u32,
    /// HNSW ef_construction parameter.
    pub hnsw_ef_construction: u32,
    /// HNSW ef_runtime (search beam width). 0 = auto: max(k*15, 200).
    /// Higher = better recall, lower QPS. Range: 10-4096.
    pub hnsw_ef_runtime: u32,
    /// Minimum vectors in mutable segment before auto-compaction triggers.
    /// Lower = more frequent compaction (smaller HNSW graphs, more segments).
    /// Higher = fewer compactions (larger graphs, better recall). Range: 100-100000.
    pub compact_threshold: u32,
    /// The HASH field name that contains the vector blob (e.g., "vec").
    pub source_field: Bytes,
    /// Key prefixes to auto-index (from PREFIX clause).
    pub key_prefixes: Vec<Bytes>,
    /// Quantization algorithm. Default: TurboQuant4.
    pub quantization: QuantizationConfig,
    /// Build mode: Light (fast, less memory) or Exact (higher recall).
    pub build_mode: crate::vector::turbo_quant::collection::BuildMode,
}

/// A single vector index: meta + segments + scratch + collection config.
pub struct VectorIndex {
    pub meta: IndexMeta,
    pub segments: SegmentHolder,
    pub scratch: SearchScratch,
    pub collection: Arc<CollectionMetadata>,
    pub payload_index: PayloadIndex,
}

/// Default minimum vector count to trigger compaction before search.
/// Overridden by IndexMeta.compact_threshold when set via FT.CREATE.
const DEFAULT_COMPACT_THRESHOLD: usize = 1000;

impl VectorIndex {
    /// Compact the mutable segment into an immutable HNSW segment if beneficial.
    ///
    /// Triggered lazily on first search when the mutable segment exceeds the
    /// threshold and no immutable segments exist yet. After compaction, searches
    /// use HNSW (O(log n)) instead of brute force (O(n)).
    ///
    /// This is a blocking operation (builds HNSW graph). For production, this
    /// should be moved to a background task with async notification.
    pub fn try_compact(&mut self) {
        let mutable_len;
        {
            let snapshot = self.segments.load();
            mutable_len = snapshot.mutable.len();
        } // drop snapshot guard before freeze/compact

        let threshold = if self.meta.compact_threshold > 0 {
            self.meta.compact_threshold as usize
        } else {
            DEFAULT_COMPACT_THRESHOLD
        };
        if mutable_len < threshold {
            return;
        }

        let frozen = self.segments.load().mutable.freeze();
        // Use a deterministic seed based on collection ID for reproducibility
        let seed = self
            .collection
            .collection_id
            .wrapping_mul(6364136223846793005);

        match compaction::compact(&frozen, &self.collection, seed, None) {
            Ok(immutable) => {
                // Resize scratch to match new graph size
                let num_nodes = immutable.graph().num_nodes();
                let padded = self.collection.padded_dimension;
                self.scratch = SearchScratch::new(num_nodes, padded);

                // Swap: empty mutable + append new immutable to existing list
                let old = self.segments.load();
                let mut imm_list = old.immutable.clone();
                imm_list.push(Arc::new(immutable));
                let new_list = SegmentList {
                    mutable: Arc::new(crate::vector::segment::mutable::MutableSegment::new(
                        self.meta.dimension,
                        self.collection.clone(),
                    )),
                    immutable: imm_list,
                    ivf: old.ivf.clone(),
                };
                self.segments.swap(new_list);
            }
            Err(_e) => {
                // Compaction failed (recall too low, etc.) — fall back to brute force
            }
        }
    }
}

/// Per-shard store of all vector indexes. Directly owned by shard thread.
pub struct VectorStore {
    indexes: HashMap<Bytes, VectorIndex>,
    /// Monotonically increasing collection ID counter.
    next_collection_id: u64,
    /// Per-shard MVCC transaction manager.
    txn_manager: TransactionManager,
    /// Segments recovered from persistence, awaiting FT.CREATE to claim them.
    /// Key: collection_id. Populated during crash recovery.
    pending_segments: HashMap<u64, crate::vector::persistence::recovery::RecoveredCollection>,
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            next_collection_id: 1,
            txn_manager: TransactionManager::new(),
            pending_segments: HashMap::new(),
        }
    }

    /// Read-only access to the transaction manager.
    #[inline]
    pub fn txn_manager(&self) -> &TransactionManager {
        &self.txn_manager
    }

    /// Mutable access to the transaction manager.
    #[inline]
    pub fn txn_manager_mut(&mut self) -> &mut TransactionManager {
        &mut self.txn_manager
    }

    /// Attach recovered segments from persistence. Called by shard restore.
    ///
    /// Stores recovered collections in pending_segments, keyed by collection_id.
    /// They will be attached to indexes when FT.CREATE runs (or immediately if
    /// the index already exists).
    pub fn attach_recovered(
        &mut self,
        recovered: crate::vector::persistence::recovery::RecoveredState,
    ) {
        for (collection_id, collection) in recovered.collections {
            self.pending_segments.insert(collection_id, collection);
        }
    }

    /// Number of pending (unattached) recovered collections.
    #[allow(dead_code)]
    pub fn pending_count(&self) -> usize {
        self.pending_segments.len()
    }

    /// Create a new index. Returns Err(&str) if index already exists.
    pub fn create_index(&mut self, meta: IndexMeta) -> Result<(), &'static str> {
        if self.indexes.contains_key(&meta.name) {
            return Err("Index already exists");
        }
        let collection_id = self.next_collection_id;
        self.next_collection_id += 1;

        let padded = padded_dimension(meta.dimension);
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            collection_id,
            meta.dimension,
            meta.metric,
            meta.quantization,
            collection_id, // use collection_id as seed for determinism
            meta.build_mode,
        ));
        let segments = SegmentHolder::new(meta.dimension, collection.clone());
        let scratch = SearchScratch::new(0, padded);

        let name = meta.name.clone();
        self.indexes.insert(
            name.clone(),
            VectorIndex {
                meta,
                segments,
                scratch,
                collection,
                payload_index: PayloadIndex::new(),
            },
        );

        // Check if recovered segments exist for this collection_id
        if let Some(recovered) = self.pending_segments.remove(&collection_id) {
            if let Some(index) = self.indexes.get(&name) {
                let mut immutable_arcs: Vec<
                    Arc<crate::vector::segment::immutable::ImmutableSegment>,
                > = Vec::with_capacity(recovered.immutable.len());
                for (imm, _meta) in recovered.immutable {
                    immutable_arcs.push(Arc::new(imm));
                }
                let new_list = crate::vector::segment::SegmentList {
                    mutable: Arc::new(recovered.mutable),
                    immutable: immutable_arcs,
                    ivf: Vec::new(),
                };
                index.segments.swap(new_list);
            }
        }

        Ok(())
    }

    /// Drop an index by name. Returns true if it existed.
    pub fn drop_index(&mut self, name: &[u8]) -> bool {
        self.indexes.remove(name).is_some()
    }

    /// Get index reference by name.
    pub fn get_index(&self, name: &[u8]) -> Option<&VectorIndex> {
        self.indexes.get(name)
    }

    /// Get mutable index reference by name.
    pub fn get_index_mut(&mut self, name: &[u8]) -> Option<&mut VectorIndex> {
        self.indexes.get_mut(name)
    }

    /// List all index names.
    pub fn index_names(&self) -> Vec<&Bytes> {
        self.indexes.keys().collect()
    }

    /// Find indexes whose key_prefixes match the given key.
    /// Returns refs to matching VectorIndex entries.
    pub fn find_matching_indexes(&self, key: &[u8]) -> Vec<&VectorIndex> {
        self.indexes
            .values()
            .filter(|idx| idx.meta.key_prefixes.iter().any(|p| key.starts_with(p)))
            .collect()
    }

    /// Find matching index names for auto-indexing.
    /// Caller must collect names first to avoid borrow issues.
    pub fn find_matching_index_names(&self, key: &[u8]) -> Vec<Bytes> {
        self.indexes
            .iter()
            .filter_map(|(name, idx)| {
                if idx.meta.key_prefixes.iter().any(|p| key.starts_with(p)) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Mark vectors as deleted for a key that was removed (DEL/HDEL/UNLINK).
    ///
    /// Finds all indexes whose key_prefixes match the key, computes the key_hash,
    /// and marks matching entries as deleted in the mutable segment. This prevents
    /// stale vectors from appearing in search results.
    ///
    /// NOTE: Vec allocation for matching_names is acceptable -- this only fires
    /// when a deleted key matches an index prefix (rare per-operation).
    pub fn mark_deleted_for_key(&mut self, key: &[u8]) {
        let matching_names = self.find_matching_index_names(key);
        if matching_names.is_empty() {
            return;
        }
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        for idx_name in matching_names {
            if let Some(idx) = self.indexes.get(&idx_name) {
                let snap = idx.segments.load();
                snap.mutable.mark_deleted_by_key_hash(key_hash, 1);
            }
        }
    }

    /// Number of indexes.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(name: &str, dim: u32, prefixes: &[&str]) -> IndexMeta {
        IndexMeta {
            name: Bytes::from(name.to_owned()),
            dimension: dim,
            padded_dimension: padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: prefixes
                .iter()
                .map(|p| Bytes::from(p.to_string()))
                .collect(),
            quantization: QuantizationConfig::TurboQuant4,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
        }
    }

    fn make_meta_quant(name: &str, dim: u32, quant: QuantizationConfig) -> IndexMeta {
        IndexMeta {
            name: Bytes::from(name.to_owned()),
            dimension: dim,
            padded_dimension: padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: vec![Bytes::from_static(b"doc:")],
            quantization: quant,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
        }
    }

    #[test]
    fn test_new_is_empty() {
        let store = VectorStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_create_index() {
        let mut store = VectorStore::new();
        let meta = make_meta("idx", 128, &["doc:"]);
        assert!(store.create_index(meta).is_ok());
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        // Duplicate should fail
        let meta2 = make_meta("idx", 128, &["doc:"]);
        assert!(store.create_index(meta2).is_err());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_drop_index() {
        let mut store = VectorStore::new();
        let meta = make_meta("idx", 128, &["doc:"]);
        store.create_index(meta).unwrap();

        assert!(store.drop_index(b"idx"));
        assert!(store.is_empty());

        // Drop non-existent
        assert!(!store.drop_index(b"idx"));
        assert!(!store.drop_index(b"nonexistent"));
    }

    #[test]
    fn test_find_matching_indexes() {
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("idx1", 64, &["user:"]))
            .unwrap();
        store
            .create_index(make_meta("idx2", 64, &["product:"]))
            .unwrap();
        store
            .create_index(make_meta("idx3", 64, &["user:", "item:"]))
            .unwrap();

        let matches = store.find_matching_indexes(b"user:123");
        assert_eq!(matches.len(), 2);

        let matches = store.find_matching_indexes(b"product:456");
        assert_eq!(matches.len(), 1);

        let matches = store.find_matching_indexes(b"item:789");
        assert_eq!(matches.len(), 1);

        let matches = store.find_matching_indexes(b"order:000");
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_get_index() {
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("myidx", 256, &["doc:"]))
            .unwrap();

        let idx = store.get_index(b"myidx").unwrap();
        assert_eq!(idx.meta.dimension, 256);
        assert_eq!(idx.meta.hnsw_m, 16);

        assert!(store.get_index(b"nonexistent").is_none());
    }

    // -- MVCC tests (Phase 65-02) --

    #[test]
    fn test_vector_store_has_txn_manager() {
        let store = VectorStore::new();
        // txn_manager accessible, starts with 0 active
        assert_eq!(store.txn_manager().active_count(), 0);
        assert_eq!(store.txn_manager().committed_count(), 0);
    }

    #[test]
    fn test_vector_store_txn_manager_mut() {
        let mut store = VectorStore::new();
        let txn = store.txn_manager_mut().begin();
        assert_eq!(txn.txn_id, 1);
        assert_eq!(store.txn_manager().active_count(), 1);
    }

    // -- Multi-bit quantization tests (Phase 72-02) --

    #[test]
    fn test_create_index_with_tq2_has_4_centroids() {
        let mut store = VectorStore::new();
        let meta = make_meta_quant("idx_tq2", 128, QuantizationConfig::TurboQuant2);
        store.create_index(meta).unwrap();

        let idx = store.get_index(b"idx_tq2").unwrap();
        assert_eq!(idx.collection.codebook.len(), 4);
        assert_eq!(idx.collection.codebook_boundaries.len(), 3);
        assert_eq!(idx.collection.quantization, QuantizationConfig::TurboQuant2);
    }

    #[test]
    fn test_create_index_with_tq1_has_2_centroids() {
        let mut store = VectorStore::new();
        let meta = make_meta_quant("idx_tq1", 128, QuantizationConfig::TurboQuant1);
        store.create_index(meta).unwrap();

        let idx = store.get_index(b"idx_tq1").unwrap();
        assert_eq!(idx.collection.codebook.len(), 2);
        assert_eq!(idx.collection.quantization, QuantizationConfig::TurboQuant1);
    }

    #[test]
    fn test_create_index_default_tq4() {
        let mut store = VectorStore::new();
        let meta = make_meta("idx_default", 128, &["doc:"]);
        store.create_index(meta).unwrap();

        let idx = store.get_index(b"idx_default").unwrap();
        assert_eq!(idx.collection.codebook.len(), 16);
        assert_eq!(idx.collection.quantization, QuantizationConfig::TurboQuant4);
    }
}
