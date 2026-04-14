//! Generic inverted index with parallel Vec posting lists.
//!
//! Core reusable data structure for both sparse vector search and
//! full-text indexing (HYBR-09). Keyed by u32 dimension/term IDs,
//! stores parallel doc_id + weight vectors for efficient dot-product
//! accumulation.

use std::collections::HashMap;

/// A posting list for a single dimension/term: parallel doc_id + weight vectors.
pub struct PostingList {
    /// Document IDs, sorted ascending for merge efficiency.
    pub doc_ids: Vec<u32>,
    /// Weights parallel to `doc_ids`.
    pub weights: Vec<f32>,
}

impl PostingList {
    fn new() -> Self {
        Self {
            doc_ids: Vec::new(),
            weights: Vec::new(),
        }
    }

    /// Insert a document with weight, maintaining sorted doc_id order.
    fn insert(&mut self, doc_id: u32, weight: f32) {
        match self.doc_ids.binary_search(&doc_id) {
            Ok(pos) => {
                // Update existing entry
                self.weights[pos] = weight;
            }
            Err(pos) => {
                // Insert at sorted position
                self.doc_ids.insert(pos, doc_id);
                self.weights.insert(pos, weight);
            }
        }
    }

    /// Remove a document from this posting list.
    fn remove(&mut self, doc_id: u32) -> bool {
        if let Ok(pos) = self.doc_ids.binary_search(&doc_id) {
            self.doc_ids.remove(pos);
            self.weights.remove(pos);
            true
        } else {
            false
        }
    }
}

/// Generic inverted index mapping dimension/term IDs to posting lists.
///
/// Designed to be shared between sparse vector search and full-text
/// indexing. The dimension ID is u32 to support both vocabulary indices
/// (SPLADE: up to ~30K) and term IDs (full-text: arbitrary).
pub struct InvertedIndex {
    postings: HashMap<u32, PostingList>,
    /// Set of all unique document IDs in the index.
    doc_ids: std::collections::HashSet<u32>,
}

impl InvertedIndex {
    /// Create an empty inverted index.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_ids: std::collections::HashSet::new(),
        }
    }

    /// Insert a (doc_id, weight) entry for a given dimension/term.
    pub fn insert(&mut self, dim_id: u32, doc_id: u32, weight: f32) {
        self.doc_ids.insert(doc_id);
        self.postings
            .entry(dim_id)
            .or_insert_with(PostingList::new)
            .insert(doc_id, weight);
    }

    /// Get the posting list for a dimension/term (for external consumers like TextIndex).
    pub fn get_posting(&self, dim_id: u32) -> Option<&PostingList> {
        self.postings.get(&dim_id)
    }

    /// Dot-product search: accumulate scores per doc across query dimensions,
    /// return top-k sorted by descending score.
    ///
    /// Uses a HashMap accumulator. Acceptable since search is not per-key hot path.
    pub fn dot_product_search(&self, query: &[(u32, f32)], top_k: usize) -> Vec<(u32, f32)> {
        if top_k == 0 {
            return Vec::new();
        }

        let mut scores: HashMap<u32, f32> = HashMap::new();

        for &(dim_id, query_weight) in query {
            if let Some(posting) = self.postings.get(&dim_id) {
                for (i, &doc_id) in posting.doc_ids.iter().enumerate() {
                    *scores.entry(doc_id).or_insert(0.0) += query_weight * posting.weights[i];
                }
            }
        }

        // Collect and sort by descending score, break ties by ascending doc_id
        let mut scored: Vec<(u32, f32)> = scores.into_iter().collect();
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(top_k);
        scored
    }

    /// Number of unique documents in the index.
    pub fn len(&self) -> u32 {
        self.doc_ids.len() as u32
    }

    /// Returns true if the index contains no documents.
    pub fn is_empty(&self) -> bool {
        self.doc_ids.is_empty()
    }

    /// Remove a document from all posting lists.
    pub fn remove_doc(&mut self, doc_id: u32) {
        if !self.doc_ids.remove(&doc_id) {
            return;
        }
        // Remove from all posting lists. Retain only non-empty lists.
        self.postings.retain(|_, posting| {
            posting.remove(doc_id);
            !posting.doc_ids.is_empty()
        });
    }
}
