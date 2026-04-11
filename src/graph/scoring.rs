//! Graph traversal scoring functions.
//!
//! All scorers operate on edge metadata (timestamp, weight/distance) and produce
//! a `f64` score. Designed for zero-allocation per-edge evaluation -- all state
//! is captured at construction time. Uses shard-cached timestamps (TRAV-08).

use crate::graph::types::{EdgeKey, NodeKey};

/// A scored edge result from traversal.
#[derive(Debug, Clone)]
pub struct EdgeScore {
    /// The neighbor node reached via this edge.
    pub node: NodeKey,
    /// The edge used to reach the neighbor.
    pub edge: EdgeKey,
    /// Computed score (higher = better for ranking, lower = better for cost).
    pub score: f64,
}

impl EdgeScore {
    /// Create a new scored edge.
    pub fn new(node: NodeKey, edge: EdgeKey, score: f64) -> Self {
        Self { node, edge, score }
    }
}

// ---------------------------------------------------------------------------
// Temporal Decay Scorer (TRAV-05)
// ---------------------------------------------------------------------------

/// Temporal decay: `score(edge) = 1.0 / (1.0 + lambda * (now - edge.timestamp))`
///
/// `now` is the shard-cached timestamp (epoch millis or LSN), captured once at
/// query start -- no `Instant::now()` syscall per edge (TRAV-08).
///
/// Higher score = more recent edge. Lambda controls decay rate:
/// - lambda=0.001: slow decay (1-hour-old edge scores ~0.78)
/// - lambda=0.01: fast decay (1-hour-old edge scores ~0.027)
pub struct TemporalDecayScorer {
    pub lambda: f64,
    pub now: u64,
}

impl TemporalDecayScorer {
    /// Create a scorer with the given decay rate and cached timestamp.
    pub fn new(lambda: f64, now: u64) -> Self {
        Self { lambda, now }
    }

    /// Score an edge by its timestamp.
    #[inline]
    pub fn score(&self, timestamp: u64) -> f64 {
        let age = self.now.saturating_sub(timestamp) as f64;
        1.0 / (1.0 + self.lambda * age)
    }
}

// ---------------------------------------------------------------------------
// Distance Scorer (TRAV-06)
// ---------------------------------------------------------------------------

/// Distance scorer: `score(edge) = 1.0 / (1.0 + edge.distance)`
///
/// Higher score = shorter distance. Distance is the edge weight field.
pub struct DistanceScorer;

impl DistanceScorer {
    /// Score an edge by its weight/distance.
    #[inline]
    pub fn score(weight: f64) -> f64 {
        1.0 / (1.0 + weight.abs())
    }
}

// ---------------------------------------------------------------------------
// Composite Scorer (TRAV-07)
// ---------------------------------------------------------------------------

/// Composite scorer combining temporal decay + distance + optional vector similarity.
///
/// `score = time_weight * temporal_score + distance_weight * distance_score + vector_weight * vector_score`
///
/// All weights should sum to 1.0 for normalized output, but this is not enforced.
pub struct CompositeScorer {
    pub time_weight: f64,
    pub distance_weight: f64,
    pub vector_weight: f64,
    temporal: TemporalDecayScorer,
}

impl CompositeScorer {
    /// Create a composite scorer.
    ///
    /// `lambda` controls temporal decay rate. `now` is the shard-cached timestamp.
    pub fn new(
        time_weight: f64,
        distance_weight: f64,
        vector_weight: f64,
        lambda: f64,
        now: u64,
    ) -> Self {
        Self {
            time_weight,
            distance_weight,
            vector_weight,
            temporal: TemporalDecayScorer::new(lambda, now),
        }
    }

    /// Score an edge given its timestamp, weight, and optional vector similarity.
    ///
    /// `vector_sim` should be in [0, 1] range (cosine similarity normalized).
    #[inline]
    pub fn score(&self, timestamp: u64, weight: f64, vector_sim: f64) -> f64 {
        let temporal_score = self.temporal.score(timestamp);
        let distance_score = DistanceScorer::score(weight);
        self.time_weight * temporal_score
            + self.distance_weight * distance_score
            + self.vector_weight * vector_sim
    }
}

// ---------------------------------------------------------------------------
// Weighted Cost Function (TRAV-03)
// ---------------------------------------------------------------------------

/// Weighted traversal cost function for Dijkstra / DFS pruning.
///
/// `cost(edge) = time_weight * (now - edge.timestamp) + distance_weight * edge.distance`
///
/// Lower cost = better path. This is an additive cost function suitable for
/// shortest-path algorithms.
pub struct WeightedCostFn {
    pub time_weight: f64,
    pub distance_weight: f64,
    pub now: u64,
}

impl WeightedCostFn {
    /// Create a weighted cost function with shard-cached timestamp.
    pub fn new(time_weight: f64, distance_weight: f64, now: u64) -> Self {
        Self {
            time_weight,
            distance_weight,
            now,
        }
    }

    /// Compute the cost of traversing an edge.
    #[inline]
    pub fn cost(&self, timestamp: u64, weight: f64) -> f64 {
        let age = self.now.saturating_sub(timestamp) as f64;
        self.time_weight * age + self.distance_weight * weight.abs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::KeyData;

    fn dummy_node_key() -> NodeKey {
        KeyData::from_ffi(1).into()
    }

    fn dummy_edge_key() -> EdgeKey {
        KeyData::from_ffi(1).into()
    }

    // --- TemporalDecayScorer tests ---

    #[test]
    fn test_temporal_decay_recent_edge() {
        let scorer = TemporalDecayScorer::new(0.001, 1000);
        // Edge created at t=1000 (age=0): score should be 1.0
        let score = scorer.score(1000);
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_temporal_decay_old_edge() {
        let scorer = TemporalDecayScorer::new(0.001, 1000);
        // Edge created at t=0 (age=1000): score = 1/(1+0.001*1000) = 1/2 = 0.5
        let score = scorer.score(0);
        assert!((score - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_temporal_decay_monotonic() {
        let scorer = TemporalDecayScorer::new(0.01, 100);
        let s1 = scorer.score(90); // age=10
        let s2 = scorer.score(50); // age=50
        let s3 = scorer.score(0); // age=100
        assert!(s1 > s2);
        assert!(s2 > s3);
    }

    #[test]
    fn test_temporal_decay_future_timestamp_saturates() {
        let scorer = TemporalDecayScorer::new(0.001, 100);
        // Timestamp in the future: saturating_sub gives 0, score = 1.0
        let score = scorer.score(200);
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_temporal_decay_zero_lambda() {
        let scorer = TemporalDecayScorer::new(0.0, 1000);
        // With zero lambda, all edges score 1.0
        assert!((scorer.score(0) - 1.0).abs() < f64::EPSILON);
        assert!((scorer.score(500) - 1.0).abs() < f64::EPSILON);
    }

    // --- DistanceScorer tests ---

    #[test]
    fn test_distance_zero_weight() {
        assert!((DistanceScorer::score(0.0) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_distance_unit_weight() {
        assert!((DistanceScorer::score(1.0) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_distance_negative_weight_uses_abs() {
        assert!((DistanceScorer::score(-1.0) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_distance_monotonic() {
        let s1 = DistanceScorer::score(1.0);
        let s2 = DistanceScorer::score(5.0);
        let s3 = DistanceScorer::score(100.0);
        assert!(s1 > s2);
        assert!(s2 > s3);
    }

    // --- CompositeScorer tests ---

    #[test]
    fn test_composite_all_weights_zero_except_time() {
        let scorer = CompositeScorer::new(1.0, 0.0, 0.0, 0.001, 1000);
        let score = scorer.score(1000, 5.0, 0.9);
        // Only temporal matters, edge at t=1000 -> temporal=1.0
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_composite_all_weights_zero_except_distance() {
        let scorer = CompositeScorer::new(0.0, 1.0, 0.0, 0.001, 1000);
        let score = scorer.score(0, 1.0, 0.9);
        // Only distance matters, weight=1.0 -> distance_score=0.5
        assert!((score - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_composite_all_weights_zero_except_vector() {
        let scorer = CompositeScorer::new(0.0, 0.0, 1.0, 0.001, 1000);
        let score = scorer.score(0, 5.0, 0.85);
        assert!((score - 0.85).abs() < f64::EPSILON);
    }

    #[test]
    fn test_composite_monotonically_decreasing_ranked_list() {
        // TRAV-07: composite scorer produces monotonically decreasing ranked list
        let scorer = CompositeScorer::new(0.4, 0.3, 0.3, 0.01, 100);
        let mut scores: Vec<f64> = vec![
            scorer.score(95, 0.5, 0.9),  // recent, close, similar
            scorer.score(80, 1.0, 0.7),  // older, farther, less similar
            scorer.score(50, 3.0, 0.4),  // old, far, dissimilar
            scorer.score(10, 10.0, 0.1), // ancient, very far, barely similar
        ];
        // Verify originally descending
        for w in scores.windows(2) {
            assert!(w[0] > w[1], "Expected descending: {} > {}", w[0], w[1]);
        }
        // Sort descending and verify order preserved
        let original = scores.clone();
        scores.sort_by(|a, b| b.partial_cmp(a).unwrap_or(core::cmp::Ordering::Equal));
        assert_eq!(original, scores);
    }

    // --- WeightedCostFn tests ---

    #[test]
    fn test_weighted_cost_zero_age() {
        let cost_fn = WeightedCostFn::new(1.0, 1.0, 100);
        // Edge at t=100 with weight=5.0: cost = 0 + 5.0 = 5.0
        let cost = cost_fn.cost(100, 5.0);
        assert!((cost - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_weighted_cost_mixed() {
        let cost_fn = WeightedCostFn::new(0.5, 2.0, 100);
        // Edge at t=50 with weight=3.0: cost = 0.5*50 + 2.0*3.0 = 25 + 6 = 31.0
        let cost = cost_fn.cost(50, 3.0);
        assert!((cost - 31.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_weighted_cost_distance_only() {
        let cost_fn = WeightedCostFn::new(0.0, 1.0, 100);
        let cost = cost_fn.cost(0, 7.0);
        assert!((cost - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_weighted_cost_time_only() {
        let cost_fn = WeightedCostFn::new(1.0, 0.0, 100);
        // age = 100 - 30 = 70
        let cost = cost_fn.cost(30, 999.0);
        assert!((cost - 70.0).abs() < f64::EPSILON);
    }

    // --- EdgeScore tests ---

    #[test]
    fn test_edge_score_construction() {
        let es = EdgeScore::new(dummy_node_key(), dummy_edge_key(), 0.75);
        assert!((es.score - 0.75).abs() < f64::EPSILON);
    }
}
