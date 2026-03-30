//! Per-shard VectorStore -- owns all vector indexes for one shard.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as PubSubRegistry).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::vector::hnsw::search::SearchScratch;
use crate::vector::segment::SegmentHolder;
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
    /// The HASH field name that contains the vector blob (e.g., "vec").
    pub source_field: Bytes,
    /// Key prefixes to auto-index (from PREFIX clause).
    pub key_prefixes: Vec<Bytes>,
}

/// A single vector index: meta + segments + scratch + collection config.
pub struct VectorIndex {
    pub meta: IndexMeta,
    pub segments: SegmentHolder,
    pub scratch: SearchScratch,
    pub collection: Arc<CollectionMetadata>,
}

/// Per-shard store of all vector indexes. Directly owned by shard thread.
pub struct VectorStore {
    indexes: HashMap<Bytes, VectorIndex>,
    /// Monotonically increasing collection ID counter.
    next_collection_id: u64,
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            next_collection_id: 1,
        }
    }

    /// Create a new index. Returns Err(&str) if index already exists.
    pub fn create_index(&mut self, meta: IndexMeta) -> Result<(), &'static str> {
        if self.indexes.contains_key(&meta.name) {
            return Err("Index already exists");
        }
        let collection_id = self.next_collection_id;
        self.next_collection_id += 1;

        let padded = padded_dimension(meta.dimension);
        let collection = Arc::new(CollectionMetadata::new(
            collection_id,
            meta.dimension,
            meta.metric,
            QuantizationConfig::Sq8,
            collection_id, // use collection_id as seed for determinism
        ));
        let segments = SegmentHolder::new(meta.dimension);
        let scratch = SearchScratch::new(0, padded);

        let name = meta.name.clone();
        self.indexes.insert(name, VectorIndex {
            meta,
            segments,
            scratch,
            collection,
        });
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
        self.indexes.values().filter(|idx| {
            idx.meta.key_prefixes.iter().any(|p| key.starts_with(p))
        }).collect()
    }

    /// Find matching index names for auto-indexing.
    /// Caller must collect names first to avoid borrow issues.
    pub fn find_matching_index_names(&self, key: &[u8]) -> Vec<Bytes> {
        self.indexes.iter().filter_map(|(name, idx)| {
            if idx.meta.key_prefixes.iter().any(|p| key.starts_with(p)) {
                Some(name.clone())
            } else {
                None
            }
        }).collect()
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
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: prefixes.iter().map(|p| Bytes::from(p.to_string())).collect(),
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
        store.create_index(make_meta("idx1", 64, &["user:"])).unwrap();
        store.create_index(make_meta("idx2", 64, &["product:"])).unwrap();
        store.create_index(make_meta("idx3", 64, &["user:", "item:"])).unwrap();

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
        store.create_index(make_meta("myidx", 256, &["doc:"])).unwrap();

        let idx = store.get_index(b"myidx").unwrap();
        assert_eq!(idx.meta.dimension, 256);
        assert_eq!(idx.meta.hnsw_m, 16);

        assert!(store.get_index(b"nonexistent").is_none());
    }
}
