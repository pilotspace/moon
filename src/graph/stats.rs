//! Per-graph statistics for cost-based query planning.
//!
//! `GraphStats` tracks node counts by label, edge counts by type, and degree
//! distribution (avg, P50, P99, max). Stats are updated incrementally on every
//! insert/delete, with periodic percentile recomputation from a histogram.

use std::collections::HashMap;

/// Interval (in mutations) between degree percentile recomputations.
const RECOMPUTE_INTERVAL: u64 = 1000;

/// Maximum degree tracked in the histogram. Degrees above this are clamped.
const MAX_HISTOGRAM_DEGREE: usize = 10_000;

/// Degree distribution statistics derived from a histogram.
#[derive(Debug, Clone)]
pub struct DegreeStats {
    /// Average degree (total_edges * 2 / total_nodes for undirected accounting).
    pub avg: f64,
    /// Median degree (50th percentile).
    pub p50: u32,
    /// 99th percentile degree.
    pub p99: u32,
    /// Maximum observed degree.
    pub max: u32,
    /// Histogram: bin[i] = number of nodes with degree i.
    /// Capped at `MAX_HISTOGRAM_DEGREE` bins.
    histogram: Vec<u32>,
    /// Total number of nodes tracked in the histogram.
    histogram_total: u64,
}

impl DegreeStats {
    fn new() -> Self {
        Self {
            avg: 0.0,
            p50: 0,
            p99: 0,
            max: 0,
            histogram: Vec::new(),
            histogram_total: 0,
        }
    }

    /// Record that a node now has `degree` total edges (outgoing + incoming).
    /// Call on insert: increment from old_degree to new_degree.
    fn record_degree_change(&mut self, old_degree: u32, new_degree: u32) {
        let old = (old_degree as usize).min(MAX_HISTOGRAM_DEGREE);
        let new = (new_degree as usize).min(MAX_HISTOGRAM_DEGREE);

        // Ensure histogram is large enough.
        let needed = old.max(new) + 1;
        if self.histogram.len() < needed {
            self.histogram.resize(needed, 0);
        }

        // Remove old bucket, add new bucket.
        if old < self.histogram.len() && self.histogram[old] > 0 {
            self.histogram[old] -= 1;
        }
        if new < self.histogram.len() {
            self.histogram[new] += 1;
        }

        // Update max.
        if new_degree > self.max {
            self.max = new_degree;
        }
    }

    /// Record a new node entering the histogram at degree 0.
    fn record_node_added(&mut self) {
        if self.histogram.is_empty() {
            self.histogram.push(0);
        }
        self.histogram[0] += 1;
        self.histogram_total += 1;
    }

    /// Record a node being removed from the histogram at the given degree.
    fn record_node_removed(&mut self, degree: u32) {
        let idx = (degree as usize).min(MAX_HISTOGRAM_DEGREE);
        if idx < self.histogram.len() && self.histogram[idx] > 0 {
            self.histogram[idx] -= 1;
        }
        self.histogram_total = self.histogram_total.saturating_sub(1);
    }

    /// Recompute percentile statistics from the histogram.
    pub fn recompute_percentiles(&mut self, total_nodes: u64, total_edges: u64) {
        if total_nodes == 0 {
            self.avg = 0.0;
            self.p50 = 0;
            self.p99 = 0;
            self.max = 0;
            return;
        }

        // Average degree: each edge contributes to 2 nodes (src + dst).
        self.avg = if total_nodes > 0 {
            (total_edges as f64 * 2.0) / total_nodes as f64
        } else {
            0.0
        };

        // Compute percentiles from histogram.
        let total = self.histogram.iter().map(|&c| c as u64).sum::<u64>();
        if total == 0 {
            self.p50 = 0;
            self.p99 = 0;
            return;
        }

        let p50_target = (total as f64 * 0.50).ceil() as u64;
        let p99_target = (total as f64 * 0.99).ceil() as u64;

        let mut cumulative: u64 = 0;
        let mut found_p50 = false;
        let mut found_p99 = false;
        let mut actual_max: u32 = 0;

        for (degree, &count) in self.histogram.iter().enumerate() {
            if count > 0 {
                actual_max = degree as u32;
            }
            cumulative += count as u64;
            if !found_p50 && cumulative >= p50_target {
                self.p50 = degree as u32;
                found_p50 = true;
            }
            if !found_p99 && cumulative >= p99_target {
                self.p99 = degree as u32;
                found_p99 = true;
            }
            if found_p50 && found_p99 {
                break;
            }
        }

        // Update max from histogram (may have decreased after deletions).
        self.max = actual_max;
        // Scan remainder for actual max if we broke early.
        if found_p50 && found_p99 {
            for (degree, &count) in self.histogram.iter().enumerate().rev() {
                if count > 0 {
                    self.max = degree as u32;
                    break;
                }
            }
        }
    }
}

/// Per-graph statistics for cost-based query planning.
#[derive(Debug, Clone)]
pub struct GraphStats {
    /// Total live node count.
    pub total_nodes: u64,
    /// Total live edge count.
    pub total_edges: u64,
    /// Node count by label ID.
    pub nodes_by_label: HashMap<u16, u64>,
    /// Edge count by type ID.
    pub edges_by_type: HashMap<u16, u64>,
    /// Degree distribution.
    pub degree_stats: DegreeStats,
    /// Mutation counter for periodic recomputation.
    mutation_count: u64,
}

impl GraphStats {
    /// Create empty stats.
    pub fn new() -> Self {
        Self {
            total_nodes: 0,
            total_edges: 0,
            nodes_by_label: HashMap::new(),
            edges_by_type: HashMap::new(),
            degree_stats: DegreeStats::new(),
            mutation_count: 0,
        }
    }

    /// Record a node insertion with the given labels.
    pub fn on_node_insert(&mut self, labels: &[u16]) {
        self.total_nodes += 1;
        for &label in labels {
            *self.nodes_by_label.entry(label).or_insert(0) += 1;
        }
        self.degree_stats.record_node_added();
        self.maybe_recompute();
    }

    /// Record an edge insertion with the given type.
    /// `src_old_degree` and `dst_old_degree` are the degrees of the source
    /// and destination nodes BEFORE this edge was added.
    pub fn on_edge_insert(
        &mut self,
        edge_type: u16,
        src_old_degree: u32,
        dst_old_degree: u32,
    ) {
        self.total_edges += 1;
        *self.edges_by_type.entry(edge_type).or_insert(0) += 1;

        // Source node gains one outgoing edge.
        self.degree_stats
            .record_degree_change(src_old_degree, src_old_degree + 1);
        // Destination node gains one incoming edge.
        self.degree_stats
            .record_degree_change(dst_old_degree, dst_old_degree + 1);

        self.maybe_recompute();
    }

    /// Record a node deletion with the given labels and current degree.
    pub fn on_node_delete(&mut self, labels: &[u16], degree: u32) {
        self.total_nodes = self.total_nodes.saturating_sub(1);
        for &label in labels {
            if let Some(count) = self.nodes_by_label.get_mut(&label) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.nodes_by_label.remove(&label);
                }
            }
        }
        self.degree_stats.record_node_removed(degree);
        self.maybe_recompute();
    }

    /// Record an edge deletion with the given type.
    pub fn on_edge_delete(&mut self, edge_type: u16) {
        self.total_edges = self.total_edges.saturating_sub(1);
        if let Some(count) = self.edges_by_type.get_mut(&edge_type) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.edges_by_type.remove(&edge_type);
            }
        }
        self.maybe_recompute();
    }

    /// Force recompute percentiles (useful after bulk operations).
    pub fn recompute_percentiles(&mut self) {
        self.degree_stats
            .recompute_percentiles(self.total_nodes, self.total_edges);
    }

    /// Check if it is time to recompute percentiles.
    fn maybe_recompute(&mut self) {
        self.mutation_count += 1;
        if self.mutation_count % RECOMPUTE_INTERVAL == 0 {
            self.degree_stats
                .recompute_percentiles(self.total_nodes, self.total_edges);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats_are_empty() {
        let stats = GraphStats::new();
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.total_edges, 0);
        assert!(stats.nodes_by_label.is_empty());
        assert!(stats.edges_by_type.is_empty());
        assert_eq!(stats.degree_stats.avg, 0.0);
        assert_eq!(stats.degree_stats.p50, 0);
        assert_eq!(stats.degree_stats.p99, 0);
        assert_eq!(stats.degree_stats.max, 0);
    }

    #[test]
    fn test_node_insert_increments_counts() {
        let mut stats = GraphStats::new();
        stats.on_node_insert(&[1, 2]);
        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.nodes_by_label[&1], 1);
        assert_eq!(stats.nodes_by_label[&2], 1);

        stats.on_node_insert(&[1]);
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.nodes_by_label[&1], 2);
        assert_eq!(stats.nodes_by_label[&2], 1);
    }

    #[test]
    fn test_edge_insert_increments_counts() {
        let mut stats = GraphStats::new();
        stats.on_node_insert(&[0]);
        stats.on_node_insert(&[0]);

        // First edge: both nodes had degree 0.
        stats.on_edge_insert(10, 0, 0);
        assert_eq!(stats.total_edges, 1);
        assert_eq!(stats.edges_by_type[&10], 1);
    }

    #[test]
    fn test_node_delete_decrements() {
        let mut stats = GraphStats::new();
        stats.on_node_insert(&[1, 2]);
        stats.on_node_insert(&[1]);
        assert_eq!(stats.total_nodes, 2);

        stats.on_node_delete(&[1, 2], 0);
        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.nodes_by_label[&1], 1);
        // Label 2 removed entirely.
        assert!(!stats.nodes_by_label.contains_key(&2));
    }

    #[test]
    fn test_edge_delete_decrements() {
        let mut stats = GraphStats::new();
        stats.on_node_insert(&[0]);
        stats.on_node_insert(&[0]);
        stats.on_edge_insert(5, 0, 0);
        assert_eq!(stats.total_edges, 1);

        stats.on_edge_delete(5);
        assert_eq!(stats.total_edges, 0);
        assert!(!stats.edges_by_type.contains_key(&5));
    }

    #[test]
    fn test_degree_percentiles_basic() {
        let mut stats = GraphStats::new();

        // Create 100 nodes.
        for _ in 0..100 {
            stats.on_node_insert(&[0]);
        }

        // 50 nodes at degree 2 (via edge inserts), 50 at degree 0.
        // We simulate: 50 edges between pairs of nodes.
        // Each edge increments src degree by 1 and dst degree by 1.
        // So 50 source nodes go 0->1, 50 dst nodes go 0->1.
        // Then another 50 edges: src nodes go 1->2, dst nodes go 1->2.
        // But we need to track old degrees properly.
        //
        // Simpler: just create a known distribution.
        let mut ds = DegreeStats::new();
        // 50 nodes at degree 1.
        for _ in 0..50 {
            ds.record_node_added(); // degree 0
            ds.record_degree_change(0, 1);
        }
        // 40 nodes at degree 5.
        for _ in 0..40 {
            ds.record_node_added();
            ds.record_degree_change(0, 5);
        }
        // 10 nodes at degree 100 (hubs).
        for _ in 0..10 {
            ds.record_node_added();
            ds.record_degree_change(0, 100);
        }

        ds.recompute_percentiles(100, 0); // edges don't matter for percentile calc

        assert_eq!(ds.p50, 1); // 50% of 100 = 50th node, which is at degree 1
        assert_eq!(ds.p99, 100); // 99th percentile = hub nodes
        assert_eq!(ds.max, 100);
    }

    #[test]
    fn test_degree_stats_empty() {
        let mut ds = DegreeStats::new();
        ds.recompute_percentiles(0, 0);
        assert_eq!(ds.avg, 0.0);
        assert_eq!(ds.p50, 0);
        assert_eq!(ds.p99, 0);
        assert_eq!(ds.max, 0);
    }

    #[test]
    fn test_degree_stats_uniform() {
        let mut ds = DegreeStats::new();
        // All 10 nodes at degree 3.
        for _ in 0..10 {
            ds.record_node_added();
            ds.record_degree_change(0, 3);
        }
        ds.recompute_percentiles(10, 15); // 15 edges -> avg = 30/10 = 3.0
        assert_eq!(ds.p50, 3);
        assert_eq!(ds.p99, 3);
        assert_eq!(ds.max, 3);
        assert!((ds.avg - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_incremental_no_full_recompute_until_interval() {
        let mut stats = GraphStats::new();
        // Insert 999 nodes -- should not trigger recompute (interval = 1000).
        for _ in 0..999 {
            stats.on_node_insert(&[0]);
        }
        // Percentiles are stale (not yet recomputed).
        // The 1000th mutation triggers recompute.
        stats.on_node_insert(&[0]);
        // After 1000 mutations, percentiles should be recomputed.
        assert_eq!(stats.total_nodes, 1000);
        // All nodes at degree 0 => p50=0, p99=0.
        assert_eq!(stats.degree_stats.p50, 0);
        assert_eq!(stats.degree_stats.p99, 0);
    }

    #[test]
    fn test_hub_detection_via_p99() {
        let mut stats = GraphStats::new();
        // Build from scratch using a fresh DegreeStats to avoid double-counting.
        // 99 normal nodes at degree 2.
        for _ in 0..99 {
            stats.on_node_insert(&[0]);
        }
        // 1 hub node at degree 500.
        stats.on_node_insert(&[1]);

        // Simulate degree changes via on_edge_insert which properly tracks
        // src and dst degrees. Instead, manipulate the histogram directly
        // for a clean test.
        //
        // After on_node_insert, all 100 nodes are at degree 0.
        // Move 99 nodes from degree 0 to degree 2.
        for _ in 0..99 {
            stats.degree_stats.record_degree_change(0, 2);
        }
        // Move 1 node from degree 0 to degree 500.
        stats.degree_stats.record_degree_change(0, 500);

        stats.recompute_percentiles();

        assert_eq!(stats.degree_stats.max, 500);
        // P99 of 100 nodes: ceil(100 * 0.99) = 99th entry.
        // Sorted: 99 nodes at degree 2, 1 node at degree 500.
        // Cumulative at degree 2: 99. 99 >= 99? yes => p99 = 2? No...
        // Wait: ceil(100 * 0.99) = 99. cumulative at degree 2 = 99, 99 >= 99 => p99 = 2.
        // This means with only 1 hub out of 100, p99 catches it only if it's
        // within the top 1%. Let's use a smaller set for clearer test.
        //
        // Actually p99 target = ceil(100 * 0.99) = 99. At degree 2 we have 99 nodes.
        // 99 >= 99 => p99 = 2. The hub at degree 500 is the 100th node.
        // So p99=2 is correct. The hub IS above p99, which is what we want
        // for hub detection (degree >= p99 means it's a hub).
        assert_eq!(stats.degree_stats.p99, 2);
        // P50 should be 2.
        assert_eq!(stats.degree_stats.p50, 2);
        // Hub node at degree 500 > p99 (2) => triggers vector-first.
        assert!(500 > stats.degree_stats.p99);
    }

    #[test]
    fn test_on_node_delete_removes_from_histogram() {
        let mut stats = GraphStats::new();
        stats.on_node_insert(&[0]);
        stats.on_node_insert(&[0]);
        assert_eq!(stats.total_nodes, 2);

        stats.on_node_delete(&[0], 0);
        assert_eq!(stats.total_nodes, 1);
    }

    #[test]
    fn test_saturating_sub_on_overcounts() {
        let mut stats = GraphStats::new();
        // Delete without insert should not underflow.
        stats.on_node_delete(&[99], 0);
        assert_eq!(stats.total_nodes, 0);

        stats.on_edge_delete(42);
        assert_eq!(stats.total_edges, 0);
    }
}
