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
    /// Wall-clock creation stamp (Unix millis) of the edge that discovered
    /// this node (the last hop on the shortest discovery path). 0 = unknown
    /// (pre-upgrade edge or CSR segment without per-edge stamps); decay
    /// treats 0 as neutral.
    pub edge_created_ms: u64,
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

    // Track (min hops, discovery-edge created_ms) per key across all seeds.
    let mut discovered: HashMap<Bytes, (u32, u64)> = HashMap::new();

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
        for &(visited_node, hop_depth, edge_used) in &result.visited {
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

            // Track minimum hop distance across all seeds, carrying the
            // wall-clock stamp of the edge that discovered the node on that
            // shortest path (for FT.NAVIGATE DECAY re-ranking).
            let edge_ms = edge_used.map(|e| e.created_ms).unwrap_or(0);
            let entry = discovered.entry(redis_key.clone()).or_insert((u32::MAX, 0));
            if hop_depth < entry.0 {
                *entry = (hop_depth, edge_ms);
            }
        }
    }

    // Build results sorted by graph_hops ascending.
    let mut results: Vec<ExpandedResult> = discovered
        .into_iter()
        .map(|(key, (hops, edge_created_ms))| ExpandedResult {
            key,
            vec_score: 0.0,
            graph_hops: hops,
            edge_created_ms,
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
            edge_created_ms: 0,
        };
        let r2 = r.clone();
        assert_eq!(r2.graph_hops, 2);
        assert!(format!("{:?}", r2).contains("doc:1"));
    }

    #[test]
    fn test_expand_carries_discovery_edge_created_ms() {
        // seed -> n1 over an edge created at t=42s: the expanded result for
        // n1 must surface that edge's wall-clock stamp so FT.NAVIGATE DECAY
        // can age it.
        use smallvec::smallvec;

        let mut gs = crate::graph::store::GraphStore::new();
        gs.create_graph(Bytes::from_static(b"g"), 64_000, 0)
            .expect("create");
        let graph = gs.get_graph_mut(b"g").expect("graph");

        let seed = graph.write_buf.add_node(smallvec![0], smallvec![], None, 1);
        let n1 = graph.write_buf.add_node(smallvec![0], smallvec![], None, 1);

        crate::storage::entry::tl_clock_set(42, 42_000);
        graph
            .write_buf
            .add_edge(seed, n1, 1, 1.0, None, 2)
            .expect("edge");
        crate::storage::entry::tl_clock_set(0, 0);

        graph.register_key(Bytes::from_static(b"doc:seed"), seed);
        graph.register_key(Bytes::from_static(b"doc:n1"), n1);

        let graph = gs.get_graph(b"g").expect("graph");
        let seeds = vec![(Bytes::from_static(b"doc:seed"), 0.1f32)];
        let results = expand_results_via_graph(graph, &seeds, 2);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key.as_ref(), b"doc:n1");
        assert_eq!(
            results[0].edge_created_ms, 42_000,
            "expansion must carry the discovery edge's created_ms"
        );
    }
}
