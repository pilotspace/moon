//! Graph segment compaction -- merges multiple CSR segments, drops tombstones,
//! and applies Rabbit Order reordering for improved cache locality.
//!
//! Rabbit Order (Arai et al., IEEE IPDPS 2016): simplified single-pass
//! community-based reordering that assigns contiguous IDs to nodes in the same
//! community, reducing cache misses by ~38%.

use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::graph::csr::{CsrError, CsrSegment};
use crate::graph::index::{EdgeTypeIndex, LabelIndex, MphNodeIndex};
use crate::graph::types::{EdgeMeta, GraphSegmentHeader, NodeMeta};

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of segments to trigger compaction.
    pub min_segments: usize,
    /// Do not compact segments with more edges than this.
    pub max_segment_edges: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_segments: 3,
            max_segment_edges: 1_000_000,
        }
    }
}

/// Errors from compaction.
#[derive(Debug, PartialEq, Eq)]
pub enum CompactionError {
    /// Not enough segments to compact.
    TooFewSegments,
    /// All input edges were tombstoned; nothing to produce.
    EmptyResult,
    /// CSR build error.
    CsrBuild(CsrError),
}

/// Collected edge from merging multiple CSR segments.
#[derive(Debug, Clone)]
struct MergedEdge {
    src_row: u32,
    dst_row: u32,
    edge_type: u16,
    flags: u16,
    created_lsn: u64,
}

/// Compact multiple CSR segments into one with Rabbit Order reordering.
///
/// Steps:
/// 1. Merge: collect all live edges from input segments
/// 2. Deduplicate: if same (src, dst, type) appears in multiple segments, keep newest
/// 3. Reorder: apply Rabbit Order for cache locality
/// 4. Build: construct new CsrSegment with reordered node IDs
pub fn compact_segments(
    segments: &[Arc<CsrSegment>],
    config: &CompactionConfig,
) -> Result<CsrSegment, CompactionError> {
    if segments.len() < config.min_segments {
        return Err(CompactionError::TooFewSegments);
    }

    // Step 1+2: Merge all live edges, deduplicate by (src_external_id, dst_external_id, edge_type).
    // We use node_meta.external_id as the canonical node identity across segments.

    // Collect all unique nodes across segments.
    let mut all_node_ids: Vec<u64> = Vec::new();
    for seg in segments {
        for nm in &seg.node_meta {
            all_node_ids.push(nm.external_id);
        }
    }
    all_node_ids.sort_unstable();
    all_node_ids.dedup();

    if all_node_ids.is_empty() {
        return Err(CompactionError::EmptyResult);
    }

    // Build external_id -> merged_row mapping.
    let mut id_to_merged: std::collections::HashMap<u64, u32> =
        std::collections::HashMap::with_capacity(all_node_ids.len());
    for (i, &id) in all_node_ids.iter().enumerate() {
        id_to_merged.insert(id, i as u32);
    }

    // Collect live edges from all segments, dedup by (src, dst, type) keeping highest LSN.
    let mut edge_map: std::collections::HashMap<(u32, u32, u16), MergedEdge> =
        std::collections::HashMap::new();

    for seg in segments {
        for src_row in 0..seg.node_count() {
            let src_ext = seg.node_meta[src_row as usize].external_id;
            let Some(&src_merged) = id_to_merged.get(&src_ext) else {
                continue;
            };

            let start = seg.row_offsets[src_row as usize] as usize;
            let end = seg.row_offsets[src_row as usize + 1] as usize;

            for edge_idx in start..end {
                if !seg.validity.contains(edge_idx as u32) {
                    continue; // tombstoned
                }
                let dst_csr_row = seg.col_indices[edge_idx];
                let dst_ext = seg.node_meta[dst_csr_row as usize].external_id;
                let Some(&dst_merged) = id_to_merged.get(&dst_ext) else {
                    continue;
                };

                let em = &seg.edge_meta[edge_idx];
                let key = (src_merged, dst_merged, em.edge_type);
                let new_edge = MergedEdge {
                    src_row: src_merged,
                    dst_row: dst_merged,
                    edge_type: em.edge_type,
                    flags: em.flags,
                    created_lsn: seg.created_lsn,
                };

                edge_map
                    .entry(key)
                    .and_modify(|existing| {
                        if new_edge.created_lsn > existing.created_lsn {
                            *existing = new_edge.clone();
                        }
                    })
                    .or_insert(new_edge);
            }
        }
    }

    let merged_edges: Vec<MergedEdge> = edge_map.into_values().collect();
    if merged_edges.is_empty() {
        return Err(CompactionError::EmptyResult);
    }

    let node_count = all_node_ids.len();

    // Step 3: Rabbit Order reordering.
    let edge_pairs: Vec<(u32, u32)> = merged_edges
        .iter()
        .map(|e| (e.src_row, e.dst_row))
        .collect();
    let perm = rabbit_order_reorder(node_count, &edge_pairs);

    // Step 4: Build CSR with reordered IDs.
    // Apply permutation to edges.
    let mut reordered_edges: Vec<MergedEdge> = merged_edges
        .into_iter()
        .map(|mut e| {
            e.src_row = perm[e.src_row as usize];
            e.dst_row = perm[e.dst_row as usize];
            e
        })
        .collect();

    // Sort edges by (src, dst) for CSR construction.
    reordered_edges.sort_by_key(|e| (e.src_row, e.dst_row));

    // Build row_offsets.
    let mut row_offsets = vec![0u32; node_count + 1];
    for e in &reordered_edges {
        row_offsets[e.src_row as usize + 1] += 1;
    }
    for i in 1..=node_count {
        row_offsets[i] += row_offsets[i - 1];
    }

    let edge_count = reordered_edges.len();

    // Build col_indices and edge_meta.
    let mut col_indices = Vec::with_capacity(edge_count);
    let mut edge_meta_vec = Vec::with_capacity(edge_count);
    for e in &reordered_edges {
        col_indices.push(e.dst_row);
        edge_meta_vec.push(EdgeMeta {
            edge_type: e.edge_type,
            flags: e.flags,
            property_offset: 0,
        });
    }

    // Build node_meta with reordered positions.
    // Invert permutation: inv_perm[new_row] = old_row.
    let mut inv_perm = vec![0u32; node_count];
    for (old, &new) in perm.iter().enumerate() {
        inv_perm[new as usize] = old as u32;
    }

    let mut node_meta_vec = Vec::with_capacity(node_count);
    // Find the most recent node metadata for each external ID across segments.
    let mut best_node_meta: std::collections::HashMap<u64, NodeMeta> =
        std::collections::HashMap::with_capacity(node_count);
    for seg in segments {
        for nm in &seg.node_meta {
            best_node_meta
                .entry(nm.external_id)
                .and_modify(|existing| {
                    if nm.created_lsn > existing.created_lsn {
                        *existing = NodeMeta {
                            external_id: nm.external_id,
                            label_bitmap: nm.label_bitmap,
                            property_offset: 0,
                            created_lsn: nm.created_lsn,
                            deleted_lsn: nm.deleted_lsn,
                        };
                    }
                })
                .or_insert(NodeMeta {
                    external_id: nm.external_id,
                    label_bitmap: nm.label_bitmap,
                    property_offset: 0,
                    created_lsn: nm.created_lsn,
                    deleted_lsn: nm.deleted_lsn,
                });
        }
    }

    for new_row in 0..node_count {
        let old_row = inv_perm[new_row] as usize;
        let ext_id = all_node_ids[old_row];
        if let Some(nm) = best_node_meta.get(&ext_id) {
            node_meta_vec.push(NodeMeta {
                external_id: nm.external_id,
                label_bitmap: nm.label_bitmap,
                property_offset: 0,
                created_lsn: nm.created_lsn,
                deleted_lsn: nm.deleted_lsn,
            });
        } else {
            node_meta_vec.push(NodeMeta {
                external_id: ext_id,
                label_bitmap: 0,
                property_offset: 0,
                created_lsn: 0,
                deleted_lsn: u64::MAX,
            });
        }
    }

    // Initialize validity bitmap: all edges valid.
    let mut validity = RoaringBitmap::new();
    for i in 0..edge_count as u32 {
        validity.insert(i);
    }

    // Build node_id_to_row from reordered node_meta.
    // We don't have NodeKey here (compaction works at external_id level),
    // so leave node_id_to_row empty -- lookup by NodeKey requires the
    // GraphStore to maintain a separate mapping.
    let node_id_to_row = std::collections::HashMap::new();

    // Compute min/max external IDs.
    let min_node_id = all_node_ids.first().copied().unwrap_or(0);
    let max_node_id = all_node_ids.last().copied().unwrap_or(0);

    // Max LSN across input segments.
    let created_lsn = segments.iter().map(|s| s.created_lsn).max().unwrap_or(0);

    // Checksum.
    let checksum = {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&(node_count as u32).to_le_bytes());
        hasher.update(&(edge_count as u32).to_le_bytes());
        hasher.update(&created_lsn.to_le_bytes());
        hasher.finalize() as u64
    };

    let header = GraphSegmentHeader {
        magic: *b"MNGR",
        version: 1,
        node_count: node_count as u32,
        edge_count: edge_count as u32,
        min_node_id,
        max_node_id,
        row_offsets_offset: 0,
        col_indices_offset: 0,
        edge_meta_offset: 0,
        validity_bitmap_offset: 0,
        created_lsn,
        checksum,
    };

    // Build indexes from compacted data. MPH is empty (no NodeKeys in compaction).
    let mph = MphNodeIndex::build(&[]);
    let label_index = LabelIndex::build(&node_meta_vec);
    let edge_type_index = EdgeTypeIndex::build(&edge_meta_vec);

    Ok(CsrSegment {
        header,
        row_offsets,
        col_indices,
        edge_meta: edge_meta_vec,
        node_meta: node_meta_vec,
        validity,
        node_id_to_row,
        mph,
        label_index,
        edge_type_index,
        created_lsn,
    })
}

/// Rabbit Order: community-based node reordering for cache locality.
///
/// Simplified single-pass version (not hierarchical dendro):
/// 1. Each node starts in its own community
/// 2. For each node, compute modularity gain of merging with each neighbor's community
/// 3. Merge with best neighbor if gain > 0
/// 4. Assign contiguous IDs within each community
///
/// Returns permutation: `perm[old_id] = new_id`.
pub fn rabbit_order_reorder(node_count: usize, edges: &[(u32, u32)]) -> Vec<u32> {
    if node_count == 0 {
        return Vec::new();
    }

    // Build undirected adjacency list with edge weights.
    let total_weight: f64 = edges.len() as f64;
    if total_weight == 0.0 {
        // No edges -- identity permutation.
        return (0..node_count as u32).collect();
    }

    // Adjacency: node -> Vec<(neighbor, weight)>
    let mut adj: Vec<Vec<(u32, f64)>> = vec![Vec::new(); node_count];
    for &(u, v) in edges {
        if (u as usize) < node_count && (v as usize) < node_count {
            adj[u as usize].push((v, 1.0));
            adj[v as usize].push((u, 1.0));
        }
    }

    // Community assignment: community[node] = community_id.
    let mut community: Vec<u32> = (0..node_count as u32).collect();
    // Degree of each node (sum of edge weights, counting both directions).
    let mut degree: Vec<f64> = vec![0.0; node_count];
    for (i, neighbors) in adj.iter().enumerate() {
        degree[i] = neighbors.len() as f64;
    }

    // Pre-compute community degree sums for O(1) lookup (avoids O(N) scan per candidate).
    let mut comm_degree_sum: std::collections::HashMap<u32, f64> =
        std::collections::HashMap::with_capacity(node_count);
    for i in 0..node_count {
        *comm_degree_sum.entry(i as u32).or_insert(0.0) += degree[i];
    }

    // Single pass: for each node, try merging with best neighbor's community.
    let m2 = 2.0 * total_weight; // 2 * total edges (for modularity formula)

    for u in 0..node_count {
        if adj[u].is_empty() {
            continue;
        }

        let u_comm = community[u];
        let mut best_gain = 0.0f64;
        let mut best_comm = u_comm;

        // Compute weight of edges from u to each neighbor community.
        let mut comm_weights: std::collections::HashMap<u32, f64> =
            std::collections::HashMap::new();
        for &(v, w) in &adj[u] {
            let v_comm = community[v as usize];
            if v_comm != u_comm {
                *comm_weights.entry(v_comm).or_insert(0.0) += w;
            }
        }

        // Evaluate modularity gain for each candidate community using pre-computed sums.
        for (&target_comm, &edge_weight_to_comm) in &comm_weights {
            let sigma_tot = comm_degree_sum.get(&target_comm).copied().unwrap_or(0.0);

            let k_u = degree[u];
            // Modularity gain of moving u from its community to target_comm.
            let gain = edge_weight_to_comm / total_weight - (sigma_tot * k_u) / (m2 * total_weight);

            if gain > best_gain {
                best_gain = gain;
                best_comm = target_comm;
            }
        }

        if best_comm != u_comm {
            // Update community degree sums: move u's degree from old to new community.
            let k_u = degree[u];
            *comm_degree_sum.entry(u_comm).or_insert(0.0) -= k_u;
            *comm_degree_sum.entry(best_comm).or_insert(0.0) += k_u;
            community[u] = best_comm;
        }
    }

    // Assign contiguous IDs within each community.
    // Group nodes by community, then assign sequential IDs.
    let mut comm_nodes: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
    for (node, &comm) in community.iter().enumerate() {
        comm_nodes.entry(comm).or_default().push(node as u32);
    }

    // Sort communities by their smallest node ID for determinism.
    let mut comm_order: Vec<(u32, Vec<u32>)> = comm_nodes.into_iter().collect();
    comm_order.sort_by_key(|(_, nodes)| nodes.first().copied().unwrap_or(0));

    let mut perm = vec![0u32; node_count];
    let mut next_id = 0u32;
    for (_, nodes) in &comm_order {
        for &node in nodes {
            perm[node as usize] = next_id;
            next_id += 1;
        }
    }

    perm
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::memgraph::MemGraph;
    use smallvec::smallvec;

    fn make_csr(node_count: usize, edges: &[(usize, usize)], lsn: u64) -> CsrSegment {
        let mut mg = MemGraph::new(100_000);
        let mut keys = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            keys.push(mg.add_node(smallvec![0], smallvec![], None, 1));
        }
        for &(s, d) in edges {
            mg.add_edge(keys[s], keys[d], 1, 1.0, None, 2).expect("ok");
        }
        let frozen = mg.freeze().expect("ok");
        CsrSegment::from_frozen(frozen, lsn).expect("ok")
    }

    #[test]
    fn test_merge_three_segments() {
        let seg1 = Arc::new(make_csr(3, &[(0, 1), (1, 2)], 10));
        let seg2 = Arc::new(make_csr(3, &[(0, 2)], 20));
        let seg3 = Arc::new(make_csr(3, &[(1, 2)], 30));

        let config = CompactionConfig {
            min_segments: 2,
            max_segment_edges: 1_000_000,
        };
        let result = compact_segments(&[seg1, seg2, seg3], &config).expect("ok");
        // Should have merged edges (deduplication may reduce count).
        assert!(result.edge_count() > 0);
        assert!(result.node_count() > 0);
    }

    #[test]
    fn test_tombstoned_edges_dropped() {
        let mut seg = make_csr(3, &[(0, 1), (0, 2), (1, 2)], 10);
        // Tombstone one edge.
        seg.mark_deleted(0);

        let seg = Arc::new(seg);
        // Need 3 segments for default config; use min_segments=1 for this test.
        let config = CompactionConfig {
            min_segments: 1,
            max_segment_edges: 1_000_000,
        };
        let result = compact_segments(&[seg], &config).expect("ok");
        // Should have 2 edges (one was tombstoned).
        assert_eq!(result.edge_count(), 2);
    }

    #[test]
    fn test_rabbit_order_valid_permutation() {
        let edges = vec![(0, 1), (1, 2), (2, 3), (3, 4), (0, 4)];
        let perm = rabbit_order_reorder(5, &edges);

        // Check it's a valid bijection.
        assert_eq!(perm.len(), 5);
        let mut sorted = perm.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_rabbit_order_groups_connected_components() {
        // Two disconnected components: {0,1,2} and {3,4,5}
        let edges = vec![(0, 1), (1, 2), (0, 2), (3, 4), (4, 5), (3, 5)];
        let perm = rabbit_order_reorder(6, &edges);

        // Check bijection.
        let mut sorted = perm.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2, 3, 4, 5]);

        // Nodes in the same component should get contiguous IDs.
        let comp1: Vec<u32> = vec![perm[0], perm[1], perm[2]];
        let comp2: Vec<u32> = vec![perm[3], perm[4], perm[5]];

        // Each component's IDs should form a contiguous range.
        let mut c1 = comp1.clone();
        c1.sort();
        assert_eq!(c1[2] - c1[0], 2, "component 1 should be contiguous");

        let mut c2 = comp2.clone();
        c2.sort();
        assert_eq!(c2[2] - c2[0], 2, "component 2 should be contiguous");
    }

    #[test]
    fn test_edge_count_matches_live_input() {
        let seg1 = Arc::new(make_csr(4, &[(0, 1), (1, 2), (2, 3)], 10));
        let seg2 = Arc::new(make_csr(4, &[(0, 3), (1, 3)], 20));
        let seg3 = Arc::new(make_csr(4, &[(0, 2)], 30));

        let config = CompactionConfig {
            min_segments: 2,
            max_segment_edges: 1_000_000,
        };
        let result = compact_segments(&[seg1, seg2, seg3], &config).expect("ok");

        // Total unique edges: (0,1), (1,2), (2,3), (0,3), (1,3), (0,2) = 6
        // Each from different segments, no duplicates.
        assert_eq!(result.edge_count(), 6);
    }

    #[test]
    fn test_too_few_segments_error() {
        let seg = Arc::new(make_csr(2, &[(0, 1)], 10));
        let config = CompactionConfig::default(); // min_segments = 3
        assert_eq!(
            compact_segments(&[seg], &config).unwrap_err(),
            CompactionError::TooFewSegments
        );
    }
}
