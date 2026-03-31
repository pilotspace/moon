//! HNSW graph data structure with contiguous layer-0 storage, BFS reorder,
//! CSR upper-layer storage, and dual prefetch for cache-optimized traversal.

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
/// Upper layer neighbors use CSR (Compressed Sparse Row) format for memory efficiency.
///
/// ## CSR Upper Layer Storage
///
/// For each (node_id, level) pair, neighbors are in:
///   `upper_neighbors[upper_offsets[idx]..upper_offsets[idx+1]]`
/// where `idx = upper_index[node_id] + (level - 1)`.
///
/// Nodes with level=0 have `upper_index[node_id] == SENTINEL` (no entry).
///
/// Memory comparison for 1M nodes (2% at L1, 0.04% at L2, M=16):
/// - SmallVec: 1M * 136 bytes = 136 MB (every node allocates inline storage)
/// - CSR: 1M * 4 (index) + ~20K * 4 (offsets) + ~320K * 4 (neighbors) = ~5.4 MB
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

    /// CSR upper-layer index: node_id -> start row in upper_offsets, or SENTINEL.
    /// Length: num_nodes.
    upper_index: Vec<u32>,
    /// CSR row pointers: upper_offsets[row..row+1] delimits neighbors in upper_neighbors.
    /// Length: total_upper_rows + 1.
    upper_offsets: Vec<u32>,
    /// CSR column values: actual neighbor IDs (no SENTINEL padding).
    upper_neighbors: Vec<u32>,

    /// Node levels: levels[original_id] = level for that node.
    /// Used during search to determine which layers a node participates in.
    #[allow(dead_code)]
    levels: Vec<u8>,

    /// Bytes per TQ code (padded_dim / 2 + 4 for norm as f32).
    bytes_per_code: u32,
}

impl HnswGraph {
    /// Create from raw parts (called by HnswBuilder::build).
    ///
    /// Accepts SmallVec upper layers from the builder and converts to CSR internally.
    /// This keeps the builder simple (SmallVec during construction) while the immutable
    /// graph benefits from CSR's compact storage.
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
        let (upper_index, upper_offsets, upper_neighbors) = build_upper_csr(&upper_layers, m);

        Self {
            num_nodes,
            m,
            m0,
            entry_point,
            max_level,
            layer0_neighbors,
            bfs_order,
            bfs_inverse,
            upper_index,
            upper_offsets,
            upper_neighbors,
            levels,
            bytes_per_code,
        }
    }

    /// Create from pre-built CSR arrays (used by deserialization).
    #[allow(clippy::too_many_arguments)]
    fn from_csr(
        num_nodes: u32,
        m: u8,
        m0: u8,
        entry_point: u32,
        max_level: u8,
        layer0_neighbors: AlignedBuffer<u32>,
        bfs_order: Vec<u32>,
        bfs_inverse: Vec<u32>,
        upper_index: Vec<u32>,
        upper_offsets: Vec<u32>,
        upper_neighbors: Vec<u32>,
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
            upper_index,
            upper_offsets,
            upper_neighbors,
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

    /// Bytes per TQ code slot (padded_dim/2 + 4 for norm).
    #[inline]
    pub fn bytes_per_code(&self) -> u32 {
        self.bytes_per_code
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
    /// Returns a slice of neighbor IDs (no SENTINEL padding, variable length).
    #[inline]
    pub fn neighbors_upper(&self, node_id: u32, level: usize) -> &[u32] {
        let idx_start = self.upper_index[node_id as usize];
        if idx_start == SENTINEL {
            return &[];
        }
        let row = idx_start as usize + (level - 1);
        if row + 1 >= self.upper_offsets.len() {
            return &[];
        }
        let start = self.upper_offsets[row] as usize;
        let end = self.upper_offsets[row + 1] as usize;
        &self.upper_neighbors[start..end]
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

    /// Serialize the graph to a byte buffer.
    ///
    /// Format v2 (all LE):
    ///   num_nodes: u32, m: u8, m0: u8, entry_point: u32, max_level: u8,
    ///   bytes_per_code: u32,
    ///   layer0_len: u32, layer0_neighbors: [u32; layer0_len],
    ///   bfs_order: [u32; num_nodes], bfs_inverse: [u32; num_nodes],
    ///   levels: [u8; num_nodes],
    ///   upper_index: [u32; num_nodes],
    ///   upper_offsets_len: u32, upper_offsets: [u32; upper_offsets_len],
    ///   upper_neighbors_len: u32, upper_neighbors: [u32; upper_neighbors_len]
    pub fn to_bytes(&self) -> Vec<u8> {
        let n = self.num_nodes as usize;
        let layer0_len = self.layer0_neighbors.len();
        let capacity = 4
            + 1
            + 1
            + 4
            + 1
            + 4
            + 4
            + layer0_len * 4
            + n * 4 * 2
            + n
            + n * 4
            + 4
            + self.upper_offsets.len() * 4
            + 4
            + self.upper_neighbors.len() * 4;
        let mut buf = Vec::with_capacity(capacity);

        buf.extend_from_slice(&self.num_nodes.to_le_bytes());
        buf.push(self.m);
        buf.push(self.m0);
        buf.extend_from_slice(&self.entry_point.to_le_bytes());
        buf.push(self.max_level);
        buf.extend_from_slice(&self.bytes_per_code.to_le_bytes());

        // Layer 0
        buf.extend_from_slice(&(layer0_len as u32).to_le_bytes());
        for &v in self.layer0_neighbors.as_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // BFS order and inverse
        for &v in &self.bfs_order {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        for &v in &self.bfs_inverse {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // Levels
        buf.extend_from_slice(&self.levels);

        // CSR upper layers
        for &v in &self.upper_index {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(self.upper_offsets.len() as u32).to_le_bytes());
        for &v in &self.upper_offsets {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(self.upper_neighbors.len() as u32).to_le_bytes());
        for &v in &self.upper_neighbors {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        buf
    }

    /// Deserialize from bytes. Returns `Err` on truncation or format mismatch.
    pub fn from_bytes(data: &[u8]) -> Result<Self, &'static str> {
        let mut pos = 0;

        let ensure = |pos: usize, need: usize| -> Result<(), &'static str> {
            if pos + need > data.len() {
                Err("truncated graph data")
            } else {
                Ok(())
            }
        };

        let read_u8 = |pos: &mut usize| -> Result<u8, &'static str> {
            ensure(*pos, 1)?;
            let v = data[*pos];
            *pos += 1;
            Ok(v)
        };

        let read_u32 = |pos: &mut usize| -> Result<u32, &'static str> {
            ensure(*pos, 4)?;
            let v =
                u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(v)
        };

        let num_nodes = read_u32(&mut pos)?;
        let m = read_u8(&mut pos)?;
        let m0 = read_u8(&mut pos)?;
        let entry_point = read_u32(&mut pos)?;
        let max_level = read_u8(&mut pos)?;
        let bytes_per_code = read_u32(&mut pos)?;

        let n = num_nodes as usize;

        // Layer 0
        let layer0_len = read_u32(&mut pos)? as usize;
        ensure(pos, layer0_len * 4)?;
        let mut layer0_vec = Vec::with_capacity(layer0_len);
        for _ in 0..layer0_len {
            layer0_vec.push(read_u32(&mut pos)?);
        }
        let layer0_neighbors = AlignedBuffer::from_vec(layer0_vec);

        // BFS order
        ensure(pos, n * 4)?;
        let mut bfs_order = Vec::with_capacity(n);
        for _ in 0..n {
            bfs_order.push(read_u32(&mut pos)?);
        }

        // BFS inverse
        ensure(pos, n * 4)?;
        let mut bfs_inverse = Vec::with_capacity(n);
        for _ in 0..n {
            bfs_inverse.push(read_u32(&mut pos)?);
        }

        // Levels
        ensure(pos, n)?;
        let levels = data[pos..pos + n].to_vec();
        pos += n;

        // CSR upper layers
        ensure(pos, n * 4)?;
        let mut upper_index = Vec::with_capacity(n);
        for _ in 0..n {
            upper_index.push(read_u32(&mut pos)?);
        }

        let offsets_len = read_u32(&mut pos)? as usize;
        ensure(pos, offsets_len * 4)?;
        let mut upper_offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            upper_offsets.push(read_u32(&mut pos)?);
        }

        let neighbors_len = read_u32(&mut pos)? as usize;
        ensure(pos, neighbors_len * 4)?;
        let mut upper_neighbors = Vec::with_capacity(neighbors_len);
        for _ in 0..neighbors_len {
            upper_neighbors.push(read_u32(&mut pos)?);
        }

        Ok(Self::from_csr(
            num_nodes,
            m,
            m0,
            entry_point,
            max_level,
            layer0_neighbors,
            bfs_order,
            bfs_inverse,
            upper_index,
            upper_offsets,
            upper_neighbors,
            levels,
            bytes_per_code,
        ))
    }

    /// Dual prefetch: neighbor list + vector data for a BFS-positioned node.
    /// Prefetches 2 cache lines of neighbors (128 bytes = 32 u32s at M0=32)
    /// and 3 cache lines of TQ code data (~192 bytes covers 512-byte TQ code start).
    #[inline(always)]
    pub fn prefetch_node(&self, bfs_pos: u32, _vectors_tq: &[u8]) {
        let neighbor_offset = bfs_pos as usize * self.m0 as usize;
        let vector_offset = bfs_pos as usize * self.bytes_per_code as usize;

        #[cfg(target_arch = "x86_64")]
        {
            use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let nptr = self.layer0_neighbors.as_ptr();
            let vptr = _vectors_tq.as_ptr();
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

/// Convert SmallVec upper layers to CSR format.
///
/// Input: `upper_layers[node_id]` = SmallVec with `level * m` entries
///   (each level has m slots, SENTINEL-padded).
///
/// Output: (upper_index, upper_offsets, upper_neighbors) where:
/// - `upper_index[node_id]` = starting row in offsets, or SENTINEL if level=0
/// - `upper_offsets[row]..upper_offsets[row+1]` = neighbor range in upper_neighbors
/// - `upper_neighbors` = packed neighbor IDs (SENTINELs stripped)
fn build_upper_csr(upper_layers: &[SmallVec<[u32; 32]>], m: u8) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    let n = upper_layers.len();
    let mut upper_index = vec![SENTINEL; n];
    let mut upper_offsets: Vec<u32> = Vec::new();
    let mut upper_neighbors: Vec<u32> = Vec::new();

    let m_usize = m as usize;

    for (node_id, sv) in upper_layers.iter().enumerate() {
        if sv.is_empty() {
            continue;
        }
        // Number of upper levels for this node
        let num_levels = sv.len() / m_usize;
        upper_index[node_id] = upper_offsets.len() as u32;

        for level_idx in 0..num_levels {
            upper_offsets.push(upper_neighbors.len() as u32);
            let start = level_idx * m_usize;
            let end = start + m_usize;
            // Copy non-SENTINEL neighbors
            for &nb in &sv[start..end] {
                if nb == SENTINEL {
                    break;
                }
                upper_neighbors.push(nb);
            }
        }
    }
    // Final sentinel offset (marks end of last row)
    upper_offsets.push(upper_neighbors.len() as u32);

    (upper_index, upper_offsets, upper_neighbors)
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
        let (_bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);

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
            2,
            16,
            m0,
            0,
            0,
            layer0,
            vec![0, 1],
            vec![0, 1],
            vec![SmallVec::new(), SmallVec::new()],
            vec![0, 0],
            8,
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
            1,
            m,
            4,
            0,
            2,
            AlignedBuffer::new(4),
            vec![0],
            vec![0],
            vec![sv],
            vec![2],
            8,
        );

        // CSR strips sentinels, so level 1 has [10, 20] and level 2 has [30]
        let l1 = graph.neighbors_upper(0, 1);
        assert_eq!(l1, &[10, 20]);

        let l2 = graph.neighbors_upper(0, 2);
        assert_eq!(l2, &[30]);
    }

    #[test]
    fn test_neighbors_upper_empty_for_level0_node() {
        let graph = HnswGraph::new(
            1,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(32),
            vec![0],
            vec![0],
            vec![SmallVec::new()],
            vec![0],
            8,
        );

        let n = graph.neighbors_upper(0, 1);
        assert!(n.is_empty());
    }

    #[test]
    fn test_tq_code_returns_correct_slice() {
        let bytes_per_code: u32 = 8;
        let vectors_tq: Vec<u8> = (0..24).collect(); // 3 codes of 8 bytes each

        let graph = HnswGraph::new(
            3,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(96),
            vec![0, 1, 2],
            vec![0, 1, 2],
            vec![SmallVec::new(); 3],
            vec![0; 3],
            bytes_per_code,
        );

        assert_eq!(graph.tq_code(0, &vectors_tq), &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            graph.tq_code(1, &vectors_tq),
            &[8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            graph.tq_code(2, &vectors_tq),
            &[16, 17, 18, 19, 20, 21, 22, 23]
        );
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
            1,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(32),
            vec![0],
            vec![0],
            vec![SmallVec::new()],
            vec![0],
            bytes_per_code,
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
            1,
            16,
            m0,
            0,
            0,
            layer0,
            vec![0],
            vec![0],
            vec![SmallVec::new()],
            vec![0],
            16,
        );

        // Should compile and not panic
        graph.prefetch_node(0, &vectors_tq);
    }

    #[test]
    fn test_to_bfs_and_to_original_roundtrip() {
        let (num_nodes, m0, flat) = make_test_graph();
        let (bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);

        let graph = HnswGraph::new(
            num_nodes,
            16,
            m0,
            bfs_order[0],
            0,
            rearrange_layer0(num_nodes, m0, &flat, &bfs_order, &bfs_inverse),
            bfs_order,
            bfs_inverse,
            vec![SmallVec::new(); num_nodes as usize],
            vec![0; num_nodes as usize],
            8,
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
            0,
            DEFAULT_M,
            DEFAULT_M0,
            0,
            0,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            8,
        );
        assert_eq!(graph.num_nodes(), 0);
        assert_eq!(graph.entry_point(), 0);
        assert_eq!(graph.max_level(), 0);
    }

    #[test]
    fn test_graph_serialization_roundtrip() {
        let (num_nodes, m0, flat) = make_test_graph();
        let m: u8 = 16;
        let (bfs_order, bfs_inverse) = bfs_reorder(num_nodes, m0, 0, &flat);
        let layer0 = rearrange_layer0(num_nodes, m0, &flat, &bfs_order, &bfs_inverse);

        // Build upper layers for node 0 (level 1)
        // With m=16, each level has m=16 slots. Node 0 has level 1.
        let mut upper = vec![SmallVec::new(); num_nodes as usize];
        let mut sv: SmallVec<[u32; 32]> = SmallVec::new();
        // Level 1: m=16 slots
        for i in 0..m as u32 {
            sv.push(if i < 3 { i + 1 } else { SENTINEL });
        }
        upper[0] = sv;

        let levels = vec![1, 0, 0, 0, 0];

        let graph = HnswGraph::new(
            num_nodes,
            m,
            m0,
            bfs_order[0],
            1,
            layer0,
            bfs_order,
            bfs_inverse,
            upper,
            levels,
            36,
        );

        let bytes = graph.to_bytes();
        let restored = HnswGraph::from_bytes(&bytes).unwrap();

        assert_eq!(restored.num_nodes(), graph.num_nodes());
        assert_eq!(restored.m(), graph.m());
        assert_eq!(restored.m0(), graph.m0());
        assert_eq!(restored.entry_point(), graph.entry_point());
        assert_eq!(restored.max_level(), graph.max_level());

        // Check layer 0 neighbors match
        for i in 0..num_nodes {
            assert_eq!(restored.neighbors_l0(i), graph.neighbors_l0(i));
        }

        // Check BFS mappings
        for i in 0..num_nodes {
            assert_eq!(restored.to_bfs(i), graph.to_bfs(i));
            assert_eq!(restored.to_original(i), graph.to_original(i));
        }

        // Check upper layers for node 0 at level 1 -- CSR strips sentinels
        let l1 = restored.neighbors_upper(0, 1);
        assert_eq!(l1.len(), 3); // only 3 non-sentinel neighbors
        assert_eq!(l1[0], 1);
        assert_eq!(l1[1], 2);
        assert_eq!(l1[2], 3);
    }

    #[test]
    fn test_graph_serialization_empty() {
        let graph = HnswGraph::new(
            0,
            DEFAULT_M,
            DEFAULT_M0,
            0,
            0,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            8,
        );
        let bytes = graph.to_bytes();
        let restored = HnswGraph::from_bytes(&bytes).unwrap();
        assert_eq!(restored.num_nodes(), 0);
    }

    #[test]
    fn test_graph_from_bytes_rejects_truncated() {
        let graph = HnswGraph::new(
            5,
            16,
            4,
            0,
            0,
            AlignedBuffer::new(20),
            vec![0, 1, 2, 3, 4],
            vec![0, 1, 2, 3, 4],
            vec![SmallVec::new(); 5],
            vec![0; 5],
            8,
        );
        let bytes = graph.to_bytes();
        // Truncate to half
        assert!(HnswGraph::from_bytes(&bytes[..bytes.len() / 2]).is_err());
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

    // ── CSR-specific tests ─────────────────────────────────────────────

    #[test]
    fn test_csr_5_node_graph_same_neighbors() {
        // 5-node graph: node 0 at level 2, node 1 at level 1, rest at level 0.
        let m: u8 = 4;
        let s = SENTINEL;
        let mut upper = vec![SmallVec::new(); 5];

        // Node 0, level 2: 2 levels * 4 slots = 8 entries
        let mut sv0 = SmallVec::new();
        // Level 1: neighbors [1, 2, S, S]
        sv0.extend_from_slice(&[1, 2, s, s]);
        // Level 2: neighbors [3, S, S, S]
        sv0.extend_from_slice(&[3, s, s, s]);
        upper[0] = sv0;

        // Node 1, level 1: 1 level * 4 slots = 4 entries
        let mut sv1 = SmallVec::new();
        // Level 1: neighbors [0, 4, S, S]
        sv1.extend_from_slice(&[0, 4, s, s]);
        upper[1] = sv1;

        let graph = HnswGraph::new(
            5,
            m,
            8,
            0,
            2,
            AlignedBuffer::new(40),
            vec![0, 1, 2, 3, 4],
            vec![0, 1, 2, 3, 4],
            upper,
            vec![2, 1, 0, 0, 0],
            8,
        );

        // Node 0, level 1: [1, 2] (sentinels stripped)
        assert_eq!(graph.neighbors_upper(0, 1), &[1, 2]);
        // Node 0, level 2: [3]
        assert_eq!(graph.neighbors_upper(0, 2), &[3]);
        // Node 1, level 1: [0, 4]
        assert_eq!(graph.neighbors_upper(1, 1), &[0, 4]);
        // Node 2 (level 0): empty
        assert!(graph.neighbors_upper(2, 1).is_empty());
        // Node 3 (level 0): empty
        assert!(graph.neighbors_upper(3, 1).is_empty());
        // Node 4 (level 0): empty
        assert!(graph.neighbors_upper(4, 1).is_empty());
    }

    #[test]
    fn test_csr_serialization_roundtrip() {
        let m: u8 = 4;
        let s = SENTINEL;
        let mut upper = vec![SmallVec::new(); 3];
        let mut sv = SmallVec::new();
        sv.extend_from_slice(&[1, 2, s, s]); // level 1
        upper[0] = sv;

        let graph = HnswGraph::new(
            3,
            m,
            8,
            0,
            1,
            AlignedBuffer::new(24),
            vec![0, 1, 2],
            vec![0, 1, 2],
            upper,
            vec![1, 0, 0],
            8,
        );

        let bytes = graph.to_bytes();
        let restored = HnswGraph::from_bytes(&bytes).unwrap();

        // Verify CSR structure preserved
        assert_eq!(restored.neighbors_upper(0, 1), &[1, 2]);
        assert!(restored.neighbors_upper(1, 1).is_empty());
        assert!(restored.neighbors_upper(2, 1).is_empty());
    }

    #[test]
    fn test_csr_memory_estimate() {
        // For 1M nodes with 2% at level 1 and 0.04% at level 2, M=16:
        // upper_index: 1M * 4 = 4 MB
        // upper_offsets: ~20,400 rows * 4 = ~82 KB
        // upper_neighbors: ~20K nodes * 16 avg neighbors = 320K * 4 = ~1.3 MB
        // Total: ~5.4 MB vs 136 MB with SmallVec

        let n = 1_000_000usize;
        let m: u8 = 16;
        let s = SENTINEL;

        // Simulate: 2% nodes at level 1, 0.04% at level 2
        let mut upper = vec![SmallVec::new(); n];
        let mut level1_count = 0u32;
        let mut level2_count = 0u32;

        for i in 0..n {
            if i % 2500 == 0 && level2_count < 400 {
                // Level 2 node: 2 levels * m slots
                let mut sv = SmallVec::with_capacity(2 * m as usize);
                for j in 0..m as u32 {
                    sv.push(if j < 8 {
                        (i as u32 + j + 1) % n as u32
                    } else {
                        s
                    });
                }
                for j in 0..m as u32 {
                    sv.push(if j < 4 {
                        (i as u32 + j + 100) % n as u32
                    } else {
                        s
                    });
                }
                upper[i] = sv;
                level2_count += 1;
            } else if i % 50 == 0 && level1_count < 20_000 {
                // Level 1 node: 1 level * m slots
                let mut sv = SmallVec::with_capacity(m as usize);
                for j in 0..m as u32 {
                    sv.push(if j < 10 {
                        (i as u32 + j + 1) % n as u32
                    } else {
                        s
                    });
                }
                upper[i] = sv;
                level1_count += 1;
            }
        }

        let (index, offsets, neighbors) = build_upper_csr(&upper, m);

        // CSR memory: index + offsets + neighbors (all Vec<u32>)
        let csr_bytes = index.len() * 4 + offsets.len() * 4 + neighbors.len() * 4;
        // Average per node
        let avg_per_node = csr_bytes / n;

        // SmallVec baseline: every node pays 136 bytes (size_of::<SmallVec<[u32; 32]>>)
        // Even empty SmallVec on stack is 136 bytes due to inline storage
        let smallvec_bytes = n * std::mem::size_of::<SmallVec<[u32; 32]>>();

        assert!(
            csr_bytes < 10_000_000, // < 10 MB
            "CSR memory {} bytes ({} avg/node) exceeds 10 MB",
            csr_bytes,
            avg_per_node
        );
        assert!(
            csr_bytes < smallvec_bytes / 10,
            "CSR ({} MB) should be at least 10x smaller than SmallVec ({} MB)",
            csr_bytes / 1_000_000,
            smallvec_bytes / 1_000_000
        );
    }

    #[test]
    fn test_csr_empty_upper_layers_return_empty() {
        // All nodes at level 0 -- every neighbor_upper should be empty
        let n = 10u32;
        let graph = HnswGraph::new(
            n,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(n as usize * 32),
            (0..n).collect(),
            (0..n).collect(),
            vec![SmallVec::new(); n as usize],
            vec![0; n as usize],
            8,
        );

        for i in 0..n {
            assert!(graph.neighbors_upper(i, 1).is_empty());
        }
    }

    #[test]
    fn test_build_upper_csr_strips_sentinels() {
        // Verify that CSR strips SENTINEL padding from neighbor lists
        let m: u8 = 4;
        let s = SENTINEL;
        let mut upper = vec![SmallVec::new(); 2];
        let mut sv = SmallVec::new();
        sv.extend_from_slice(&[10, s, s, s]); // only 1 actual neighbor
        upper[0] = sv;

        let (index, offsets, neighbors) = build_upper_csr(&upper, m);
        assert_ne!(index[0], SENTINEL);
        assert_eq!(index[1], SENTINEL);
        // Only 1 neighbor stored, not 4
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], 10);
        // Offsets: [0, 1] (one row with 1 element)
        let row = index[0] as usize;
        assert_eq!(offsets[row], 0);
        assert_eq!(offsets[row + 1], 1);
    }
}
