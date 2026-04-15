//! SparseStore — wraps InvertedIndex with key_hash mapping for vector search.
//!
//! Provides insert + search for sparse vectors (e.g., SPLADE outputs).
//! Maps between internal doc_ids and external key_hash identifiers.

use std::collections::HashMap;

use crate::vector::sparse::inverted::InvertedIndex;
use crate::vector::types::{SearchResult, VectorId};

/// Per-field sparse vector store backed by an inverted index.
pub struct SparseStore {
    index: InvertedIndex,
    /// Maximum dimension index (e.g., 30000 for SPLADE vocabulary).
    max_dim: u32,
    /// doc_id -> key_hash (for search result mapping).
    doc_to_key_hash: HashMap<u32, u64>,
    /// key_hash -> doc_id (for upsert detection).
    key_hash_to_doc: HashMap<u64, u32>,
    /// Next document ID to assign.
    next_doc_id: u32,
}

impl SparseStore {
    /// Create an empty sparse store with the given maximum dimension.
    pub fn new(max_dim: u32) -> Self {
        Self {
            index: InvertedIndex::new(),
            max_dim,
            doc_to_key_hash: HashMap::new(),
            key_hash_to_doc: HashMap::new(),
            next_doc_id: 0,
        }
    }

    /// Insert a sparse vector for a key_hash.
    ///
    /// If the key_hash already exists, the old document is removed first (upsert).
    /// Each pair is (dimension_index, weight). All dimension indices must be < max_dim.
    ///
    /// Returns the assigned doc_id on success, or an error if any dimension is out of range.
    pub fn insert(&mut self, key_hash: u64, pairs: &[(u32, f32)]) -> Result<u32, &'static str> {
        // Validate dimensions
        for &(dim, _) in pairs {
            if dim >= self.max_dim {
                return Err("sparse dimension index exceeds max_dim");
            }
        }

        // Upsert: remove old doc if key_hash already exists
        if let Some(&old_doc_id) = self.key_hash_to_doc.get(&key_hash) {
            self.index.remove_doc(old_doc_id);
            self.doc_to_key_hash.remove(&old_doc_id);
        }

        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        // Insert into inverted index
        for &(dim, weight) in pairs {
            self.index.insert(dim, doc_id, weight);
        }

        // Track mappings
        self.doc_to_key_hash.insert(doc_id, key_hash);
        self.key_hash_to_doc.insert(key_hash, doc_id);

        Ok(doc_id)
    }

    /// Search for top-k sparse vectors by dot-product similarity.
    ///
    /// Returns results with `distance` = raw dot-product score (higher = more similar,
    /// matching InnerProduct metric convention).
    pub fn search(&self, query_pairs: &[(u32, f32)], top_k: usize) -> Vec<SearchResult> {
        let scored = self.index.dot_product_search(query_pairs, top_k);

        scored
            .into_iter()
            .filter_map(|(doc_id, score)| {
                let key_hash = *self.doc_to_key_hash.get(&doc_id)?;
                Some(SearchResult::with_key_hash(
                    score,
                    VectorId(doc_id),
                    key_hash,
                ))
            })
            .collect()
    }

    /// Number of documents in the store.
    pub fn len(&self) -> usize {
        self.key_hash_to_doc.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.key_hash_to_doc.is_empty()
    }

    /// Maximum dimension index for this store.
    pub fn max_dim(&self) -> u32 {
        self.max_dim
    }
}
