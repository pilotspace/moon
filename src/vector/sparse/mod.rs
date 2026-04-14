//! Sparse vector storage with inverted posting lists.
//!
//! Shared infrastructure for both sparse vector search (SPLADE/BM25)
//! and full-text indexing. The `InvertedIndex` is the core reusable
//! primitive; `SparseStore` wraps it with key_hash mapping for the
//! vector search layer.

pub mod inverted;
pub mod store;

pub use inverted::InvertedIndex;
pub use store::SparseStore;

#[cfg(test)]
mod tests {
    use super::*;

    // === InvertedIndex tests ===

    #[test]
    fn test_inverted_index_insert_and_len() {
        let mut idx = InvertedIndex::new();
        assert_eq!(idx.len(), 0);
        idx.insert(0, 1, 0.5);
        idx.insert(0, 2, 0.8);
        idx.insert(1, 1, 0.3);
        assert_eq!(idx.len(), 2); // 2 unique doc_ids
    }

    #[test]
    fn test_inverted_index_get_posting() {
        let mut idx = InvertedIndex::new();
        idx.insert(10, 1, 0.5);
        idx.insert(10, 2, 0.8);
        let posting = idx.get_posting(10).unwrap();
        assert_eq!(posting.doc_ids.len(), 2);
        assert_eq!(posting.weights.len(), 2);
        assert!(idx.get_posting(99).is_none());
    }

    #[test]
    fn test_inverted_index_dot_product_search() {
        let mut idx = InvertedIndex::new();
        // doc 1: dim0=0.5, dim1=0.3
        idx.insert(0, 1, 0.5);
        idx.insert(1, 1, 0.3);
        // doc 2: dim0=0.8, dim1=0.0 (absent)
        idx.insert(0, 2, 0.8);
        // doc 3: dim1=0.9
        idx.insert(1, 3, 0.9);

        // query: dim0=1.0, dim1=2.0
        let query = [(0, 1.0f32), (1, 2.0f32)];
        let results = idx.dot_product_search(&query, 10);
        // doc 1: 0.5*1.0 + 0.3*2.0 = 1.1
        // doc 2: 0.8*1.0 = 0.8
        // doc 3: 0.9*2.0 = 1.8
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 3); // highest score
        assert!((results[0].1 - 1.8).abs() < 1e-6);
        assert_eq!(results[1].0, 1);
        assert!((results[1].1 - 1.1).abs() < 1e-6);
        assert_eq!(results[2].0, 2);
        assert!((results[2].1 - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_inverted_index_empty_search() {
        let idx = InvertedIndex::new();
        let results = idx.dot_product_search(&[(0, 1.0)], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_inverted_index_no_overlap() {
        let mut idx = InvertedIndex::new();
        idx.insert(0, 1, 0.5);
        // query on dimension 99 which has no postings
        let results = idx.dot_product_search(&[(99, 1.0)], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_inverted_index_remove_doc() {
        let mut idx = InvertedIndex::new();
        idx.insert(0, 1, 0.5);
        idx.insert(1, 1, 0.3);
        idx.insert(0, 2, 0.8);
        assert_eq!(idx.len(), 2);
        idx.remove_doc(1);
        assert_eq!(idx.len(), 1);
        let results = idx.dot_product_search(&[(0, 1.0)], 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn test_inverted_index_top_k_limit() {
        let mut idx = InvertedIndex::new();
        for doc_id in 0..100 {
            idx.insert(0, doc_id, doc_id as f32);
        }
        let results = idx.dot_product_search(&[(0, 1.0)], 5);
        assert_eq!(results.len(), 5);
        // Highest scores first
        assert_eq!(results[0].0, 99);
    }

    // === SparseStore tests ===

    #[test]
    fn test_sparse_store_insert_and_search() {
        let mut store = SparseStore::new(30000);
        let pairs = [(0, 0.5f32), (1, 0.3)];
        let doc_id = store.insert(1001, &pairs).unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(store.len(), 1);

        let query = [(0, 1.0f32), (1, 2.0)];
        let results = store.search(&query, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key_hash, 1001);
        // score = 0.5*1.0 + 0.3*2.0 = 1.1
        assert!((results[0].distance - 1.1).abs() < 1e-6);
    }

    #[test]
    fn test_sparse_store_empty_search() {
        let store = SparseStore::new(30000);
        let results = store.search(&[(0, 1.0)], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_sparse_store_upsert() {
        let mut store = SparseStore::new(30000);
        store.insert(1001, &[(0, 0.5)]).unwrap();
        // Upsert with new value
        store.insert(1001, &[(0, 0.9)]).unwrap();
        assert_eq!(store.len(), 1);
        let results = store.search(&[(0, 1.0)], 10);
        assert_eq!(results.len(), 1);
        assert!((results[0].distance - 0.9).abs() < 1e-6);
    }

    #[test]
    fn test_sparse_store_dim_validation() {
        let mut store = SparseStore::new(100);
        // Valid: dim 99 < max_dim 100
        assert!(store.insert(1, &[(99, 0.5)]).is_ok());
        // Invalid: dim 100 >= max_dim 100
        assert!(store.insert(2, &[(100, 0.5)]).is_err());
    }

    #[test]
    fn test_sparse_store_multiple_docs() {
        let mut store = SparseStore::new(30000);
        store.insert(100, &[(0, 0.5), (1, 0.3)]).unwrap();
        store.insert(200, &[(0, 0.8)]).unwrap();
        store.insert(300, &[(1, 0.9)]).unwrap();

        let results = store.search(&[(0, 1.0), (1, 2.0)], 10);
        assert_eq!(results.len(), 3);
        // doc 300: 0.9*2.0 = 1.8 (highest)
        assert_eq!(results[0].key_hash, 300);
        // doc 100: 0.5*1.0 + 0.3*2.0 = 1.1
        assert_eq!(results[1].key_hash, 100);
        // doc 200: 0.8*1.0 = 0.8
        assert_eq!(results[2].key_hash, 200);
    }
}
