//! HNSW graph data structure with contiguous layer-0 storage, BFS reorder,
//! and dual prefetch for cache-optimized traversal.

use crate::vector::aligned_buffer::AlignedBuffer;
use smallvec::SmallVec;

/// Sentinel value for unused neighbor slots.
pub const SENTINEL: u32 = u32::MAX;

/// Default connectivity parameter.
pub const DEFAULT_M: u8 = 16;

/// Default layer-0 connectivity (2 * M).
pub const DEFAULT_M0: u8 = 32;

/// Immutable HNSW graph with BFS-reordered layer 0 for cache-friendly traversal.
///
/// Layer 0 neighbors are stored in a flat `AlignedBuffer<u32>` indexed by BFS position.
/// Upper layer neighbors use `SmallVec` indexed by original node ID.
pub struct HnswGraph {
    /// Total number of nodes in the graph.
    num_nodes: u32,
    /// Max neighbors per node on upper layers.
    m: u8,
    /// Max neighbors per node on layer 0 (= 2 * m).
    m0: u8,
    /// Entry point node ID (in BFS-reordered space after reorder, original space before).
    entry_point: u32,
    /// Maximum level in the graph.
    max_level: u8,

    /// Layer 0 neighbors: flat contiguous array.
    /// Layout: node i's neighbors at offset [i * m0 .. (i+1) * m0].
    /// Unused slots filled with SENTINEL (u32::MAX).
    /// After BFS reorder, index i corresponds to BFS position i.
    layer0_neighbors: AlignedBuffer<u32>,

    /// BFS reorder mapping: bfs_order[original_id] = bfs_position.
    bfs_order: Vec<u32>,
    /// Inverse: bfs_inverse[bfs_position] = original_id.
    bfs_inverse: Vec<u32>,

    /// Upper layers: Vec indexed by original node ID.
    /// Only nodes with level > 0 have non-empty SmallVecs.
    /// Contains neighbors for levels 1..=max_level.
    /// Layout: upper_layers[node_id] stores all upper-layer neighbors concatenated,
    /// with each level having `m` slots. Level l starts at offset (l-1)*m.
    upper_layers: Vec<SmallVec<[u32; 32]>>,

    /// Node levels: levels[original_id] = level for that node.
    levels: Vec<u8>,

    /// Bytes per TQ code (padded_dim / 2 + 4 for norm as f32).
    bytes_per_code: u32,
}

impl HnswGraph {
    /// Create from raw parts (called by HnswBuilder::build).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        num_nodes: u32,
        m: u8,
        m0: u8,
        entry_point: u32,
        max_level: u8,
        layer0_neighbors: AlignedBuffer<u32>,
        bfs_order: Vec<u32>,
        bfs_inverse: Vec<u32>,
        upper_layers: Vec<SmallVec<[u32; 32]>>,
        levels: Vec<u8>,
        bytes_per_code: u32,
    ) -> Self {
        Self {
            num_nodes,
            m,
            m0,
            entry_point,
            max_level,
            layer0_neighbors,
            bfs_order,
            bfs_inverse,
            upper_layers,
            levels,
            bytes_per_code,
        }
    }

    #[inline]
    pub fn num_nodes(&self) -> u32 {
        self.num_nodes
    }

    #[inline]
    pub fn entry_point(&self) -> u32 {
        self.entry_point
    }

    #[inline]
    pub fn max_level(&self) -> u8 {
        self.max_level
    }

    #[inline]
    pub fn m(&self) -> u8 {
        self.m
    }

    #[inline]
    pub fn m0(&self) -> u8 {
        self.m0
    }

    /// Get layer-0 neighbors for a BFS-reordered node position.
    /// Returns a slice of m0 u32s (may contain SENTINEL for unfilled slots).
    #[inline]
    pub fn neighbors_l0(&self, bfs_pos: u32) -> &[u32] {
        let start = bfs_pos as usize * self.m0 as usize;
        &self.layer0_neighbors.as_slice()[start..start + self.m0 as usize]
    }

    /// Get upper-layer neighbors for a node at a specific level.
    /// `node_id` is in ORIGINAL space (upper layers not BFS-reordered).
    /// Returns slice of m u32s (may contain SENTINEL).
    #[inline]
    pub fn neighbors_upper(&self, node_id: u32, level: usize) -> &[u32] {
        let sv = &self.upper_layers[node_id as usize];
        if sv.is_empty() {
            return &[];
        }
        let start = (level - 1) * self.m as usize;
        let end = start + self.m as usize;
        if end > sv.len() {
            return &[];
        }
        &sv[start..end]
    }

    /// Get the TQ code bytes for a node from the vector data buffer.
    /// `bfs_pos` is in BFS-reordered space.
    /// `vectors_tq` is the flat buffer of all TQ codes laid out in BFS order.
    #[inline]
    pub fn tq_code<'a>(&self, bfs_pos: u32, vectors_tq: &'a [u8]) -> &'a [u8] {
        let offset = bfs_pos as usize * self.bytes_per_code as usize;
        &vectors_tq[offset..offset + self.bytes_per_code as usize]
    }

    /// Get the norm (last 4 bytes of the TQ code slot) for a node.
    #[inline]
    pub fn tq_norm(&self, bfs_pos: u32, vectors_tq: &[u8]) -> f32 {
        let offset = bfs_pos as usize * self.bytes_per_code as usize;
        let norm_offset = offset + self.bytes_per_code as usize - 4;
        f32::from_le_bytes([
            vectors_tq[norm_offset],
            vectors_tq[norm_offset + 1],
            vectors_tq[norm_offset + 2],
            vectors_tq[norm_offset + 3],
        ])
    }

    /// Map original node ID to BFS position.
    #[inline]
    pub fn to_bfs(&self, original_id: u32) -> u32 {
        self.bfs_order[original_id as usize]
    }

    /// Map BFS position back to original node ID.
    #[inline]
    pub fn to_original(&self, bfs_pos: u32) -> u32 {
        self.bfs_inverse[bfs_pos as usize]
    }

    /// Dual prefetch: neighbor list + vector data for a BFS-positioned node.
    /// Prefetches 2 cache lines of neighbors (128 bytes = 32 u32s at M0=32)
    /// and 3 cache lines of TQ code data (~192 bytes covers 512-byte TQ code start).
    #[inline(always)]
    pub fn prefetch_node(&self, bfs_pos: u32, vectors_tq: &[u8]) {
        let neighbor_offset = bfs_pos as usize * self.m0 as usize;
        let vector_offset = bfs_pos as usize * self.bytes_per_code as usize;

        #[cfg(target_arch = "x86_64")]
        {
            use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let nptr = self.layer0_neighbors.as_ptr();
            let vptr = vectors_tq.as_ptr();
            // SAFETY: prefetch is an architectural hint on x86_64. Out-of-bounds
            // prefetch addresses do not fault -- the CPU silently ignores them.
            // No memory is read or written; only the cache hierarchy is hinted.
            unsafe {
                _mm_prefetch(nptr.add(neighbor_offset) as *const i8, _MM_HINT_T0);
                _mm_prefetch(nptr.add(neighbor_offset + 16) as *const i8, _MM_HINT_T0);
                _mm_prefetch(vptr.add(vector_offset) as *const i8, _MM_HINT_T0);
                _mm_prefetch(vptr.add(vector_offset + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(vptr.add(vector_offset + 128) as *const i8, _MM_HINT_T0);
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            // No-op on AArch64 for now (PRFM requires nightly intrinsics).
            let _ = (neighbor_offset, vector_offset);
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let _ = (neighbor_offset, vector_offset);
        }
    }
}

/// Perform BFS traversal from entry_point on layer 0 and return
/// (bfs_order, bfs_inverse) mappings.
///
/// bfs_order[original_id] = bfs_position
/// bfs_inverse[bfs_position] = original_id
///
/// Nodes unreachable from entry_point get positions after all reachable nodes.
pub(crate) fn bfs_reorder(
    num_nodes: u32,
    m0: u8,
    entry_point: u32,
    layer0_flat: &[u32],
) -> (Vec<u32>, Vec<u32>) {
    let n = num_nodes as usize;
    let mut bfs_order = vec![u32::MAX; n]; // original -> bfs_pos
    let mut bfs_inverse = Vec::with_capacity(n); // bfs_pos -> original

    // BFS from entry_point
    let mut queue = std::collections::VecDeque::with_capacity(n);
    queue.push_back(entry_point);
    bfs_order[entry_point as usize] = 0;
    bfs_inverse.push(entry_point);

    while let Some(current) = queue.pop_front() {
        let start = current as usize * m0 as usize;
        let neighbors = &layer0_flat[start..start + m0 as usize];
        for &nb in neighbors {
            if nb == SENTINEL {
                break;
            }
            if bfs_order[nb as usize] == u32::MAX {
                let pos = bfs_inverse.len() as u32;
                bfs_order[nb as usize] = pos;
                bfs_inverse.push(nb);
                queue.push_back(nb);
            }
        }
    }

    // Handle unreachable nodes (shouldn't happen in a well-built HNSW, but safety)
    for id in 0..n {
        if bfs_order[id] == u32::MAX {
            let pos = bfs_inverse.len() as u32;
            bfs_order[id] = pos;
            bfs_inverse.push(id as u32);
        }
    }

    debug_assert_eq!(bfs_inverse.len(), n);
    (bfs_order, bfs_inverse)
}

/// Rearrange a flat layer-0 neighbor array from original order to BFS order.
/// Also remaps neighbor IDs from original space to BFS space.
pub(crate) fn rearrange_layer0(
    num_nodes: u32,
    m0: u8,
    original_flat: &[u32],
    bfs_order: &[u32],
    bfs_inverse: &[u32],
) -> AlignedBuffer<u32> {
    let n = num_nodes as usize;
    let stride = m0 as usize;
    let mut result = AlignedBuffer::<u32>::new(n * stride);
    let out = result.as_mut_slice();

    // Fill with sentinel
    for slot in out.iter_mut() {
        *slot = SENTINEL;
    }

    // For each BFS position, copy the original node's neighbors (remapped to BFS space)
    for bfs_pos in 0..n {
        let orig_id = bfs_inverse[bfs_pos] as usize;
        let src_start = orig_id * stride;
        let dst_start = bfs_pos * stride;

        for j in 0..stride {
            let nb = original_flat[src_start + j];
            if nb == SENTINEL {
                break;
            }
            out[dst_start + j] = bfs_order[nb as usize];
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small 5-node graph for testing BFS reorder.
    /// Graph structure (layer 0, m0=4):
    ///   0 -> [1, 2, SENTINEL, SENTINEL]
    ///   1 -> [0, 3, SENTINEL, SENTINEL]
    ///   2 -> [0, 4, SENTINEL, SENTINEL]
    ///   3 -> [1, 4, SENTINEL, SENTINEL]
    ///   4 -> [2, 3, SENTINEL, SENTINEL]
    fn make_test_graph() -> (u32, u8, Vec<u32>) {
        let m0: u8 = 4;
        let num_nodes: u32 = 5;
        let s = SENTINEL;
        let flat = vec![
            1, 2, s, s, // node 0
            0, 3, s, s, // node 1
            0, 4, s, s, // node 2
            1, 4, s, s, // node 3
            2, 3, s, s, // node 4
        ];
        (num_nodes, m0, flat)
    }

    #[test]
    fn test_bfs_reorder_produces_valid_permutation() {
        let (num_nodes, m0, flat) = make_test_graph();
        let (bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);

        // Every node should appear exactly once in bfs_inverse
        assert_eq!(bfs_inverse.len(), num_nodes as usize);
        let mut sorted = bfs_inverse.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2, 3, 4]);

        // bfs_order and bfs_inverse should be consistent
        for (orig, &bfs_pos) in bfs_order.iter().enumerate() {
            assert_eq!(bfs_inverse[bfs_pos as usize], orig as u32);
        }

        // Entry point should be at BFS position 0
        assert_eq!(bfs_order[0], 0);
    }

    #[test]
    fn test_bfs_reorder_known_order() {
        let (num_nodes, m0, flat) = make_test_graph();
        let (bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);

        // BFS from 0: visit 0, then neighbors 1,2, then 1's neighbor 3, then 2's neighbor 4
        // (4 is already reached via 2, so order is 0,1,2,3,4)
        assert_eq!(bfs_inverse[0], 0); // first visited
        assert_eq!(bfs_inverse[1], 1); // neighbor of 0
        assert_eq!(bfs_inverse[2], 2); // neighbor of 0
        assert_eq!(bfs_inverse[3], 3); // neighbor of 1
        assert_eq!(bfs_inverse[4], 4); // neighbor of 2 (or 3)
    }

    #[test]
    fn test_rearrange_layer0_remaps_ids() {
        let (num_nodes, m0, flat) = make_test_graph();
        let (bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);
        let result = rearrange_layer0(num_nodes, m0, &flat, &bfs_order, &bfs_inverse);

        let stride = m0 as usize;
        // Check BFS position 0 (was originally node 0, neighbors were 1,2)
        let n0 = &result.as_slice()[0..stride];
        assert_eq!(n0[0], bfs_order[1]); // neighbor 1 remapped
        assert_eq!(n0[1], bfs_order[2]); // neighbor 2 remapped
        assert_eq!(n0[2], SENTINEL);
        assert_eq!(n0[3], SENTINEL);
    }

    #[test]
    fn test_neighbors_l0_returns_correct_slice() {
        let m0: u8 = 4;
        let s = SENTINEL;
        let flat_data = vec![10u32, 20, s, s, 30, 40, 50, s];
        let layer0 = AlignedBuffer::from_vec(flat_data);

        let graph = HnswGraph::new(
            2, 16, m0, 0, 0, layer0,
            vec![0, 1], vec![0, 1],
            vec![SmallVec::new(), SmallVec::new()],
            vec![0, 0], 8,
        );

        let n0 = graph.neighbors_l0(0);
        assert_eq!(n0, &[10, 20, s, s]);

        let n1 = graph.neighbors_l0(1);
        assert_eq!(n1, &[30, 40, 50, s]);
    }

    #[test]
    fn test_neighbors_upper_returns_correct_slice() {
        let m: u8 = 2;
        let s = SENTINEL;
        // Node 0 has level 2, so upper_layers[0] has 2 levels * 2 slots = 4 entries
        let mut sv = SmallVec::new();
        sv.extend_from_slice(&[10, 20, 30, s]); // level 1: [10,20], level 2: [30, SENTINEL]

        let graph = HnswGraph::new(
            1, m, 4, 0, 2,
            AlignedBuffer::new(4),
            vec![0], vec![0],
            vec![sv],
            vec![2], 8,
        );

        let l1 = graph.neighbors_upper(0, 1);
        assert_eq!(l1, &[10, 20]);

        let l2 = graph.neighbors_upper(0, 2);
        assert_eq!(l2, &[30, s]);
    }

    #[test]
    fn test_neighbors_upper_empty_for_level0_node() {
        let graph = HnswGraph::new(
            1, 16, 32, 0, 0,
            AlignedBuffer::new(32),
            vec![0], vec![0],
            vec![SmallVec::new()],
            vec![0], 8,
        );

        let n = graph.neighbors_upper(0, 1);
        assert!(n.is_empty());
    }

    #[test]
    fn test_tq_code_returns_correct_slice() {
        let bytes_per_code: u32 = 8;
        let vectors_tq: Vec<u8> = (0..24).collect(); // 3 codes of 8 bytes each

        let graph = HnswGraph::new(
            3, 16, 32, 0, 0,
            AlignedBuffer::new(96),
            vec![0, 1, 2], vec![0, 1, 2],
            vec![SmallVec::new(); 3],
            vec![0; 3], bytes_per_code,
        );

        assert_eq!(graph.tq_code(0, &vectors_tq), &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(graph.tq_code(1, &vectors_tq), &[8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(graph.tq_code(2, &vectors_tq), &[16, 17, 18, 19, 20, 21, 22, 23]);
    }

    #[test]
    fn test_tq_norm_reads_last_4_bytes() {
        let bytes_per_code: u32 = 8;
        let norm_val: f32 = 3.14;
        let norm_bytes = norm_val.to_le_bytes();
        let mut vectors_tq = vec![0u8; 8];
        vectors_tq[4] = norm_bytes[0];
        vectors_tq[5] = norm_bytes[1];
        vectors_tq[6] = norm_bytes[2];
        vectors_tq[7] = norm_bytes[3];

        let graph = HnswGraph::new(
            1, 16, 32, 0, 0,
            AlignedBuffer::new(32),
            vec![0], vec![0],
            vec![SmallVec::new()],
            vec![0], bytes_per_code,
        );

        let got = graph.tq_norm(0, &vectors_tq);
        assert!((got - norm_val).abs() < 1e-6);
    }

    #[test]
    fn test_prefetch_node_no_panic() {
        let m0: u8 = 4;
        let layer0 = AlignedBuffer::<u32>::new(4);
        let vectors_tq = vec![0u8; 16];

        let graph = HnswGraph::new(
            1, 16, m0, 0, 0, layer0,
            vec![0], vec![0],
            vec![SmallVec::new()],
            vec![0], 16,
        );

        // Should compile and not panic
        graph.prefetch_node(0, &vectors_tq);
    }

    #[test]
    fn test_to_bfs_and_to_original_roundtrip() {
        let (num_nodes, m0, flat) = make_test_graph();
        let (bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);

        let graph = HnswGraph::new(
            num_nodes, 16, m0, bfs_order[0], 0,
            rearrange_layer0(num_nodes, m0, &flat, &bfs_order, &bfs_inverse),
            bfs_order, bfs_inverse,
            vec![SmallVec::new(); num_nodes as usize],
            vec![0; num_nodes as usize], 8,
        );

        for orig in 0..num_nodes {
            let bfs = graph.to_bfs(orig);
            let back = graph.to_original(bfs);
            assert_eq!(back, orig);
        }
    }

    #[test]
    fn test_hnsw_graph_new_constructs_without_panic() {
        let graph = HnswGraph::new(
            0, DEFAULT_M, DEFAULT_M0, 0, 0,
            AlignedBuffer::new(0),
            Vec::new(), Vec::new(),
            Vec::new(), Vec::new(), 8,
        );
        assert_eq!(graph.num_nodes(), 0);
        assert_eq!(graph.entry_point(), 0);
        assert_eq!(graph.max_level(), 0);
    }

    #[test]
    fn test_bfs_reorder_unreachable_nodes() {
        // Disconnected graph: nodes 0-1 connected, nodes 2-3 disconnected
        let m0: u8 = 2;
        let s = SENTINEL;
        let flat = vec![
            1, s, // node 0
            0, s, // node 1
            s, s, // node 2 (disconnected)
            s, s, // node 3 (disconnected)
        ];
        let (bfs_order, bfs_inverse) = bfs_reorder(4, m0, 0, &flat);

        // All 4 nodes should be assigned positions
        assert_eq!(bfs_inverse.len(), 4);
        // Nodes 0,1 should be first (reachable)
        assert_eq!(bfs_order[0], 0);
        assert_eq!(bfs_order[1], 1);
        // Nodes 2,3 should be after (unreachable, appended in ID order)
        assert!(bfs_order[2] >= 2);
        assert!(bfs_order[3] >= 2);
    }
}
