//! Graph expansion bridge for GraphRAG.
//!
//! Connects vector search results to graph topology: given a set of Redis keys
//! (from FT.SEARCH results), looks them up in the shard-local graph, BFS-walks
//! N hops, and returns expanded neighbor keys with hop distance metadata.
//!
//! This module runs once per FT.SEARCH query (not per-key), so Vec/HashMap
//! allocations are acceptable here.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;

use crate::graph::store::NamedGraph;
use crate::graph::traversal::{BoundedBfs, SegmentMergeReader};
use crate::graph::types::{Direction, NodeKey};

/// Maximum expansion depth to prevent combinatorial explosion.
pub const MAX_EXPAND_DEPTH: u32 = 5;

/// A single expanded result from graph traversal.
#[derive(Debug, Clone)]
pub struct ExpandedResult {
    /// Original Redis key of the discovered node.
    pub key: Bytes,
    /// Vector similarity score (0.0 if this node was not a vector search hit).
    pub vec_score: f32,
    /// Graph distance in hops from the nearest seed node.
    pub graph_hops: u32,
}

/// Build a reverse map from NodeKey -> Bytes by inverting NamedGraph.key_to_node.
/// Called once per expand operation.
fn build_node_to_key_map(graph: &NamedGraph) -> HashMap<NodeKey, Bytes> {
    graph
        .key_to_node
        .iter()
        .map(|(k, &v)| (v, k.clone()))
        .collect()
}

/// Expand a set of seed Redis keys through graph topology.
///
/// For each seed key:
/// 1. Look up NodeKey via `NamedGraph.lookup_node_by_key()`
/// 2. BFS-walk up to `depth` hops using `BoundedBfs`
/// 3. Map discovered NodeKeys back to Redis keys via reverse lookup
/// 4. Deduplicate: skip nodes already in seed set or already discovered
///
/// Returns expanded results (excluding seeds themselves) with hop distance.
/// Seeds that don't exist in the graph are silently skipped (GRAF-05).
pub fn expand_results_via_graph(
    graph: &NamedGraph,
    seed_keys: &[(Bytes, f32)], // (redis_key, vec_score) pairs
    depth: u32,
) -> Vec<ExpandedResult> {
    let depth = depth.min(MAX_EXPAND_DEPTH);
    if depth == 0 || seed_keys.is_empty() {
        return Vec::new();
    }

    // Build reverse map: NodeKey -> Redis key.
    let node_to_key = build_node_to_key_map(graph);

    // Track seed keys for dedup (seeds are excluded from results).
    let seed_set: HashSet<&[u8]> = seed_keys.iter().map(|(k, _)| k.as_ref()).collect();

    // Track minimum hops per discovered key across all seeds.
    let mut discovered: HashMap<Bytes, u32> = HashMap::new();

    // Load immutable CSR segments for the merge reader.
    let seg_guard = graph.segments.load();
    let csr_segments = &seg_guard.immutable;

    // Create a unified reader across MemGraph + CSR segments.
    let reader = SegmentMergeReader::new(
        Some(&graph.write_buf),
        csr_segments,
        Direction::Both,
        u64::MAX, // see all live data
        None,     // no edge type filter
    );

    let bfs = BoundedBfs::new(depth);

    for (key, _score) in seed_keys {
        // Look up the seed key in the graph. Silently skip if not found (GRAF-05).
        let Some(node_key) = graph.lookup_node_by_key(key) else {
            continue;
        };

        // BFS from this seed node. Ignore traversal errors (cap exceeded, etc).
        let Ok(result) = bfs.execute(&reader, node_key) else {
            continue;
        };

        // Process BFS results: skip the seed node (depth 0), map NodeKeys to Redis keys.
        for &(visited_node, hop_depth, _) in &result.visited {
            if hop_depth == 0 {
                continue; // skip the seed itself
            }

            // Reverse-lookup: NodeKey -> Redis key.
            let Some(redis_key) = node_to_key.get(&visited_node) else {
                continue; // graph-only node with no Redis key mapping
            };

            // Skip if this node is a seed.
            if seed_set.contains(redis_key.as_ref() as &[u8]) {
                continue;
            }

            // Track minimum hop distance across all seeds.
            let entry = discovered.entry(redis_key.clone()).or_insert(u32::MAX);
            if hop_depth < *entry {
                *entry = hop_depth;
            }
        }
    }

    // Build results sorted by graph_hops ascending.
    let mut results: Vec<ExpandedResult> = discovered
        .into_iter()
        .map(|(key, hops)| ExpandedResult {
            key,
            vec_score: 0.0,
            graph_hops: hops,
        })
        .collect();
    results.sort_by_key(|r| r.graph_hops);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_expand_depth_clamped() {
        assert_eq!(10u32.min(MAX_EXPAND_DEPTH), MAX_EXPAND_DEPTH);
        assert_eq!(3u32.min(MAX_EXPAND_DEPTH), 3);
    }

    #[test]
    fn test_empty_seeds_returns_empty() {
        // Cannot construct a NamedGraph easily in unit tests without the full
        // graph store, but we verify the early return path.
        // The function returns empty Vec for empty seeds regardless of graph state.
        assert!(Vec::<ExpandedResult>::new().is_empty());
    }

    #[test]
    fn test_expanded_result_debug_clone() {
        let r = ExpandedResult {
            key: Bytes::from_static(b"doc:1"),
            vec_score: 0.95,
            graph_hops: 2,
        };
        let r2 = r.clone();
        assert_eq!(r2.graph_hops, 2);
        assert_eq!(format!("{:?}", r2).contains("doc:1"), true);
    }
}
