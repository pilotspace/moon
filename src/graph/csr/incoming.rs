//! Derived reverse (incoming) adjacency index for CSR segments.
//!
//! The on-disk CSR stores only OUTGOING adjacency (`row_offsets` + `col_indices`).
//! `Direction::Incoming`/`Both` traversal of a compacted graph needs each node's
//! predecessors. Rather than persist a second adjacency (a segment-format change),
//! this index is DERIVED from the existing forward arrays and built lazily, in
//! memory, at most once per segment (cached in a `OnceLock` on the owning segment).
//!
//! Layout mirrors CSR: `in_offsets[d]..in_offsets[d+1]` is the slice of `in_pairs`
//! holding node `d`'s incoming edges, each as `(src_row, edge_idx)`. Storing the
//! forward `edge_idx` lets the caller recover `edge_meta`/`edge_created_ms`/validity
//! by index (so edge-type filter, tombstones, and decay stamp are preserved with no
//! new semantics); storing `src_row` explicitly avoids inverting `row_offsets` at
//! query time (graph-incoming-edges A2).

/// Reverse adjacency: for each CSR row, the incoming edges that target it.
#[derive(Debug, Clone)]
pub struct IncomingIndex {
    /// Length = node_count + 1. `in_offsets[d]..in_offsets[d+1]` indexes `in_pairs`
    /// for destination row `d`.
    pub in_offsets: Vec<u32>,
    /// Length = edge_count. `(src_row, edge_idx)` pairs grouped by destination row.
    /// `edge_idx` is the position in the forward `col_indices`/`edge_meta` arrays.
    pub in_pairs: Vec<(u32, u32)>,
}

impl IncomingIndex {
    /// Build the reverse index from a segment's forward CSR arrays in O(V + E)
    /// via a counting sort keyed on destination row.
    ///
    /// `row_offsets` has length `node_count + 1`; `col_indices[e]` is the
    /// destination row of forward edge `e`. The source row of edge `e` is the
    /// row whose `row_offsets` range contains `e`.
    pub fn build(row_offsets: &[u32], col_indices: &[u32], node_count: usize) -> Self {
        // Count incoming edges per destination, then prefix-sum into offsets.
        let mut in_offsets = vec![0u32; node_count + 1];
        for &dst in col_indices {
            // Defensive: a malformed dst beyond node_count is ignored rather than
            // panicking (parser/loader is trusted, but never crash on bad data).
            if (dst as usize) < node_count {
                in_offsets[dst as usize + 1] += 1;
            }
        }
        for i in 0..node_count {
            in_offsets[i + 1] += in_offsets[i];
        }

        // Scatter each forward edge into its destination's slot.
        let mut cursor = in_offsets.clone();
        let mut in_pairs = vec![(0u32, 0u32); col_indices.len()];
        for src_row in 0..node_count {
            let start = row_offsets[src_row] as usize;
            let end = row_offsets[src_row + 1] as usize;
            for (e, &dst) in col_indices[start..end].iter().enumerate() {
                let edge_idx = (start + e) as u32;
                if (dst as usize) < node_count {
                    let pos = cursor[dst as usize] as usize;
                    in_pairs[pos] = (src_row as u32, edge_idx);
                    cursor[dst as usize] += 1;
                }
            }
        }

        IncomingIndex {
            in_offsets,
            in_pairs,
        }
    }

    /// The `(src_row, edge_idx)` pairs of edges incoming to destination row `dst`.
    /// Empty slice if `dst` is out of range.
    #[inline]
    pub fn incoming(&self, dst: u32) -> &[(u32, u32)] {
        let d = dst as usize;
        if d + 1 >= self.in_offsets.len() {
            return &[];
        }
        let start = self.in_offsets[d] as usize;
        let end = self.in_offsets[d + 1] as usize;
        &self.in_pairs[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_reverse_index_small() {
        // Forward CSR for: 0->2, 1->2, 2->3 (rows 0,1,2,3).
        // row_offsets: [0,1,2,3,3]; col_indices: [2, 2, 3].
        let row_offsets = vec![0u32, 1, 2, 3, 3];
        let col_indices = vec![2u32, 2, 3];
        let idx = IncomingIndex::build(&row_offsets, &col_indices, 4);

        // Node 2's predecessors are rows 0 and 1.
        let into_2: Vec<u32> = idx.incoming(2).iter().map(|&(s, _)| s).collect();
        assert_eq!(into_2, vec![0, 1]);
        // Node 3's predecessor is row 2.
        let into_3: Vec<u32> = idx.incoming(3).iter().map(|&(s, _)| s).collect();
        assert_eq!(into_3, vec![2]);
        // Nodes 0 and 1 have no predecessors.
        assert!(idx.incoming(0).is_empty());
        assert!(idx.incoming(1).is_empty());
        // Out-of-range dst is empty, not a panic.
        assert!(idx.incoming(99).is_empty());
    }

    #[test]
    fn test_edge_idx_recovers_forward_position() {
        // col_indices index must point back at the right forward edge.
        let row_offsets = vec![0u32, 2, 2]; // row 0 has 2 out-edges, row 1 none
        let col_indices = vec![1u32, 0]; // edge0: 0->1, edge1: 0->0
        let idx = IncomingIndex::build(&row_offsets, &col_indices, 2);
        // Into node 1: forward edge 0 (0->1).
        assert_eq!(idx.incoming(1), &[(0, 0)]);
        // Into node 0: forward edge 1 (0->0).
        assert_eq!(idx.incoming(0), &[(0, 1)]);
    }
}
