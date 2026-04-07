//! Core newtypes for the vector search engine.
//!
//! These types prevent mixing up IDs, metrics, and results at compile time.

/// Internal vector identifier. Sequential per shard, supports 4B vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VectorId(pub u32);

/// Distance metric for similarity computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DistanceMetric {
    /// Euclidean (L2 squared) distance. Lower = more similar.
    L2 = 0,
    /// Cosine similarity. Higher = more similar.
    Cosine = 1,
    /// Inner (dot) product. Higher = more similar.
    InnerProduct = 2,
}

/// A single search result: (distance, vector ID, key_hash).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchResult {
    /// Distance or similarity score.
    pub distance: f32,
    /// Internal vector ID (global_id after segment remap).
    pub id: VectorId,
    /// xxh64 hash of the original Redis HASH key. Used to look up the
    /// original key string via `VectorIndex.key_hash_to_key` so FT.SEARCH
    /// returns `doc:N` instead of `vec:<internal_id>`.
    /// Default 0 means "unknown" — caller falls back to `vec:<id>` form.
    pub key_hash: u64,
}

impl SearchResult {
    #[inline]
    pub fn new(distance: f32, id: VectorId) -> Self {
        Self { distance, id, key_hash: 0 }
    }

    #[inline]
    pub fn with_key_hash(distance: f32, id: VectorId, key_hash: u64) -> Self {
        Self { distance, id, key_hash }
    }
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by distance (lower first), break ties by ID.
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_id_newtype() {
        let a = VectorId(42);
        let b = VectorId(42);
        let c = VectorId(99);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a < c);
    }

    #[test]
    fn test_distance_metric_repr() {
        assert_eq!(DistanceMetric::L2 as u8, 0);
        assert_eq!(DistanceMetric::Cosine as u8, 1);
        assert_eq!(DistanceMetric::InnerProduct as u8, 2);
    }

    #[test]
    fn test_search_result_ordering() {
        let a = SearchResult::new(0.5, VectorId(1));
        let b = SearchResult::new(0.8, VectorId(2));
        let c = SearchResult::new(0.5, VectorId(3));
        assert!(a < b); // lower distance first
        assert!(a < c); // same distance, lower ID first
    }
}
