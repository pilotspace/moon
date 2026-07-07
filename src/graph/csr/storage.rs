//! CsrStorage — unified access to heap or mmap CSR segments.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use super::mmap::MmapCsrSegment;
use super::{CsrError, CsrSegment, IncomingIndex};
use crate::graph::index::{EdgeTypeIndex, LabelIndex, MphNodeIndex};
use crate::graph::types::{EdgeMeta, GraphSegmentHeader, NodeKey, NodeMeta};

/// Unified CSR access — either heap-owned (from_bytes) or memory-mapped (from_mmap_file).
#[derive(Debug)]
pub enum CsrStorage {
    Heap(CsrSegment),
    Mmap(MmapCsrSegment),
}

impl CsrStorage {
    /// Load a CSR segment from a file. Tries mmap first, falls back to heap on failure.
    pub fn from_file(path: &Path) -> Result<Self, CsrError> {
        match MmapCsrSegment::from_mmap_file(path) {
            Ok(mmap_seg) => Ok(CsrStorage::Mmap(mmap_seg)),
            Err(_) => CsrSegment::from_file(path).map(CsrStorage::Heap),
        }
    }

    /// Row offsets slice.
    pub fn row_offsets(&self) -> &[u32] {
        match self {
            CsrStorage::Heap(s) => &s.row_offsets,
            CsrStorage::Mmap(s) => s.row_offsets(),
        }
    }

    /// Column indices slice.
    pub fn col_indices(&self) -> &[u32] {
        match self {
            CsrStorage::Heap(s) => &s.col_indices,
            CsrStorage::Mmap(s) => s.col_indices(),
        }
    }

    /// Edge metadata slice.
    pub fn edge_meta(&self) -> &[EdgeMeta] {
        match self {
            CsrStorage::Heap(s) => &s.edge_meta,
            CsrStorage::Mmap(s) => s.edge_meta(),
        }
    }

    /// Node metadata slice.
    pub fn node_meta(&self) -> &[NodeMeta] {
        match self {
            CsrStorage::Heap(s) => &s.node_meta,
            CsrStorage::Mmap(s) => s.node_meta(),
        }
    }

    /// Per-edge wall-clock creation stamps, parallel to col_indices.
    /// Empty for segments loaded from version < 3 files (stamp unknown).
    pub fn edge_created_ms(&self) -> &[u64] {
        match self {
            CsrStorage::Heap(s) => &s.edge_created_ms,
            CsrStorage::Mmap(s) => s.edge_created_ms(),
        }
    }

    /// Outgoing neighbor row indices for a CSR row.
    pub fn neighbors_out(&self, row: u32) -> &[u32] {
        match self {
            CsrStorage::Heap(s) => s.neighbors_out(row),
            CsrStorage::Mmap(s) => s.neighbors_out(row),
        }
    }

    /// Look up CSR row index for a NodeKey.
    pub fn lookup_node(&self, key: NodeKey) -> Option<u32> {
        match self {
            CsrStorage::Heap(s) => s.lookup_node(key),
            CsrStorage::Mmap(s) => s.lookup_node(key),
        }
    }

    /// Node count from header.
    pub fn node_count(&self) -> u32 {
        match self {
            CsrStorage::Heap(s) => s.node_count(),
            CsrStorage::Mmap(s) => s.node_count(),
        }
    }

    /// Edge count from header.
    pub fn edge_count(&self) -> u32 {
        match self {
            CsrStorage::Heap(s) => s.edge_count(),
            CsrStorage::Mmap(s) => s.edge_count(),
        }
    }

    /// Check if an edge is still valid.
    pub fn is_valid(&self, edge_idx: u32) -> bool {
        match self {
            CsrStorage::Heap(s) => s.is_valid(edge_idx),
            CsrStorage::Mmap(s) => s.is_valid(edge_idx),
        }
    }

    /// Mark an edge as deleted.
    pub fn mark_deleted(&mut self, edge_idx: u32) {
        match self {
            CsrStorage::Heap(s) => s.mark_deleted(edge_idx),
            CsrStorage::Mmap(s) => s.mark_deleted(edge_idx),
        }
    }

    /// Hint sequential access pattern (effective only for Mmap variant on Linux).
    pub fn madvise_sequential(&self) {
        match self {
            CsrStorage::Heap(_) => {}
            CsrStorage::Mmap(s) => s.madvise_sequential(),
        }
    }

    /// Hint random access pattern (effective only for Mmap variant on Linux).
    pub fn madvise_random(&self) {
        match self {
            CsrStorage::Heap(_) => {}
            CsrStorage::Mmap(s) => s.madvise_random(),
        }
    }

    /// Resident bytes used by this CSR segment.
    ///
    /// Follows the `RawF16Store` precedent (PR #232): mmap-backed sections
    /// are kernel page cache (reclaimable under memory pressure) and count
    /// as 0; heap-owned sections count in full. Previously this summed
    /// `row_offsets`/`col_indices`/`edge_meta`/`node_meta` unconditionally
    /// for BOTH variants (over-counting `Mmap` segments, whose backing
    /// storage is NOT heap-resident) and omitted the v5 node/edge property
    /// blobs, the lazily-built `SegmentPropertyIndexes`, and the lazily-built
    /// `GraphHnsw` bridge entirely — all invisible to the shard's elastic
    /// memory budget / eviction pipeline (`NamedGraph::resident_bytes` ->
    /// `src/shard/persistence_tick.rs`).
    ///
    /// `props_index` and `hnsw_bridge` are ALWAYS heap-owned once built,
    /// regardless of whether the underlying segment is `Heap` or `Mmap` —
    /// they are derived structures built on top of the (possibly mmap'd)
    /// source data, never persisted/mapped themselves.
    pub fn resident_bytes(&self) -> usize {
        let (core, props) = match self {
            CsrStorage::Heap(s) => {
                let core = std::mem::size_of_val(s.row_offsets.as_slice())
                    + std::mem::size_of_val(s.col_indices.as_slice())
                    + std::mem::size_of_val(s.edge_meta.as_slice())
                    + std::mem::size_of_val(s.node_meta.as_slice());
                (core, s.node_props.len() + s.edge_props.len())
            }
            CsrStorage::Mmap(_) => (0, 0),
        };
        let indexes = self
            .props_index_if_built()
            .map_or(0, |ix| ix.resident_bytes());
        let hnsw = self
            .hnsw_bridge_if_built()
            .map_or(0, |b| b.resident_bytes());
        core + props + indexes + hnsw
    }

    /// Borrow the property index ONLY if it has already been built (does
    /// not trigger a build) — `resident_bytes` must be a cheap read, never
    /// a lazy-init trigger.
    fn props_index_if_built(&self) -> Option<&crate::graph::index::SegmentPropertyIndexes> {
        match self {
            CsrStorage::Heap(s) => s.props_index.get(),
            CsrStorage::Mmap(s) => s.props_index.get(),
        }
    }

    /// Borrow the HNSW bridge ONLY if it has already been built AND
    /// succeeded (`Some(GraphHnsw)`, not the cached "too few vectors" `None`).
    fn hnsw_bridge_if_built(&self) -> Option<&crate::graph::hnsw_bridge::GraphHnsw> {
        match self {
            CsrStorage::Heap(s) => s.hnsw_bridge.get().and_then(|b| b.as_ref()),
            CsrStorage::Mmap(s) => s.hnsw_bridge.get().and_then(|b| b.as_ref()),
        }
    }

    /// Created LSN for this segment.
    pub fn created_lsn(&self) -> u64 {
        match self {
            CsrStorage::Heap(s) => s.created_lsn,
            CsrStorage::Mmap(s) => s.created_lsn,
        }
    }

    /// Access the validity bitmap.
    pub fn validity(&self) -> &RoaringBitmap {
        match self {
            CsrStorage::Heap(s) => &s.validity,
            CsrStorage::Mmap(s) => &s.validity,
        }
    }

    /// Access the header.
    pub fn header(&self) -> &GraphSegmentHeader {
        match self {
            CsrStorage::Heap(s) => &s.header,
            CsrStorage::Mmap(s) => &s.header,
        }
    }

    /// Access the MPH index.
    pub fn mph(&self) -> &MphNodeIndex {
        match self {
            CsrStorage::Heap(s) => &s.mph,
            CsrStorage::Mmap(s) => &s.mph,
        }
    }

    /// Access the label index.
    pub fn label_index(&self) -> &LabelIndex {
        match self {
            CsrStorage::Heap(s) => &s.label_index,
            CsrStorage::Mmap(s) => &s.label_index,
        }
    }

    /// Access the sparse node label-overflow map (CSR row -> labels with id >= 32).
    /// Empty for version <= 3 segments and graphs with no labels >= 32.
    pub fn label_overflow(&self) -> &HashMap<u32, SmallVec<[u16; 4]>> {
        match self {
            CsrStorage::Heap(s) => &s.label_overflow,
            CsrStorage::Mmap(s) => &s.label_overflow,
        }
    }

    /// Access the edge type index.
    pub fn edge_type_index(&self) -> &EdgeTypeIndex {
        match self {
            CsrStorage::Heap(s) => &s.edge_type_index,
            CsrStorage::Mmap(s) => &s.edge_type_index,
        }
    }

    /// Access the node_id_to_row map.
    pub fn node_id_to_row(&self) -> &HashMap<NodeKey, u32> {
        match self {
            CsrStorage::Heap(s) => &s.node_id_to_row,
            CsrStorage::Mmap(s) => &s.node_id_to_row,
        }
    }

    /// Iterator over valid outgoing neighbor edges for a CSR row.
    ///
    /// Note: materializes into Vec due to enum dispatch. For hot paths,
    /// prefer [`for_each_neighbor_edge`] which avoids the allocation.
    pub fn neighbor_edges(&self, row: u32) -> Vec<(u32, EdgeMeta)> {
        let mut out = Vec::new();
        self.for_each_neighbor_edge(row, |col, meta| out.push((col, meta)));
        out
    }

    /// Zero-allocation neighbor edge iteration via callback.
    ///
    /// Calls `f(col_idx, edge_meta)` for each valid outgoing edge from `row`.
    /// Eliminates the Vec allocation in [`neighbor_edges`] for BFS/DFS hot loops.
    #[inline]
    pub fn for_each_neighbor_edge(&self, row: u32, mut f: impl FnMut(u32, EdgeMeta)) {
        let r = row as usize;
        let ro = self.row_offsets();
        let (start, end) = if r < self.node_count() as usize {
            (ro[r] as usize, ro[r + 1] as usize)
        } else {
            (0, 0)
        };
        let ci = self.col_indices();
        let em = self.edge_meta();
        let validity = self.validity();
        for idx in start..end {
            if validity.contains(idx as u32) {
                f(ci[idx], em[idx]);
            }
        }
    }

    /// Like [`for_each_neighbor_edge`], additionally yielding the edge's
    /// wall-clock creation stamp (Unix millis) for temporal-decay scoring.
    /// Yields 0 (= unknown, decay-neutral) for segments without per-edge
    /// stamps (version < 3 files).
    #[inline]
    pub fn for_each_neighbor_edge_ms(&self, row: u32, mut f: impl FnMut(u32, EdgeMeta, u64)) {
        let r = row as usize;
        let ro = self.row_offsets();
        let (start, end) = if r < self.node_count() as usize {
            (ro[r] as usize, ro[r + 1] as usize)
        } else {
            (0, 0)
        };
        let ci = self.col_indices();
        let em = self.edge_meta();
        let ecms = self.edge_created_ms();
        let validity = self.validity();
        for idx in start..end {
            if validity.contains(idx as u32) {
                // Checked access: ecms is empty for pre-v3 segments.
                let ms = ecms.get(idx).copied().unwrap_or(0);
                f(ci[idx], em[idx], ms);
            }
        }
    }

    /// Reverse of [`for_each_neighbor_edge_ms`]: for each LIVE edge INCOMING to
    /// `dst_row`, invoke `f(src_row, edge_meta, created_ms)`. The source row +
    /// per-edge attributes are recovered by forward edge index, so the edge-type
    /// filter, validity tombstones, and decay stamp are identical to the outgoing
    /// path. Lazily builds and caches the derived [`IncomingIndex`] on first call.
    /// (v3-2 graph-incoming-edges.)
    #[inline]
    pub fn for_each_incoming_edge_ms(&self, dst_row: u32, mut f: impl FnMut(u32, EdgeMeta, u64)) {
        let index = self.incoming_index();
        let em = self.edge_meta();
        let ecms = self.edge_created_ms();
        let validity = self.validity();
        for &(src_row, edge_idx) in index.incoming(dst_row) {
            if validity.contains(edge_idx) {
                let e = edge_idx as usize;
                let ms = ecms.get(e).copied().unwrap_or(0);
                f(src_row, em[e], ms);
            }
        }
    }

    /// Get-or-build this segment's derived reverse (incoming) adjacency index.
    /// Built once from the forward `row_offsets` + `col_indices`; cached in the
    /// segment's `OnceLock` (interior mutability — the index is read-only state).
    fn incoming_index(&self) -> &IncomingIndex {
        match self {
            CsrStorage::Heap(s) => s.incoming.get_or_init(|| {
                let nc = s.row_offsets.len().saturating_sub(1);
                IncomingIndex::build(&s.row_offsets, &s.col_indices, nc)
            }),
            CsrStorage::Mmap(s) => s.incoming.get_or_init(|| {
                let ro = s.row_offsets();
                let nc = ro.len().saturating_sub(1);
                IncomingIndex::build(ro, s.col_indices(), nc)
            }),
        }
    }

    /// Get-or-build this segment's lazy property index over CSR rows.
    /// Built once from node_meta + the v5 property blob; cached in the
    /// segment's `OnceLock` (interior mutability — read-only derived state,
    /// same pattern as `incoming_index`).
    pub fn property_index(&self) -> &crate::graph::index::SegmentPropertyIndexes {
        match self {
            CsrStorage::Heap(s) => s.props_index.get_or_init(|| {
                crate::graph::index::SegmentPropertyIndexes::build(&s.node_meta, &s.node_props)
            }),
            CsrStorage::Mmap(s) => s.props_index.get_or_init(|| {
                crate::graph::index::SegmentPropertyIndexes::build(
                    s.node_meta(),
                    s.node_props_blob(),
                )
            }),
        }
    }

    /// This segment's HNSW bridge over v5 node embeddings (hybrid
    /// HnswPreFilter), if already built. The build itself runs on a
    /// detached background thread (W2-5): the first caller kicks it off and
    /// gets `None` — every caller already treats `None` as "score exactly",
    /// so queries brute-force until the bridge installs instead of stalling
    /// the shard event loop for the full HNSW construction. The negative
    /// answer (too few embeddings) is cached the same way. Built at most
    /// once per segment.
    pub fn hnsw_bridge(self: &Arc<Self>) -> Option<&crate::graph::hnsw_bridge::GraphHnsw> {
        let (cell, building) = match &**self {
            CsrStorage::Heap(s) => (&s.hnsw_bridge, &s.hnsw_building),
            CsrStorage::Mmap(s) => (&s.hnsw_bridge, &s.hnsw_building),
        };
        if let Some(cached) = cell.get() {
            return cached.as_ref();
        }
        if !building.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let seg = Arc::clone(self);
            std::thread::spawn(move || {
                // A panicking build must still converge the cell (to None),
                // or the segment would re-arm `building` on no future call
                // and answer exact-only forever without a cached decision.
                let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::graph::hnsw_bridge::GraphHnsw::build(
                        &seg,
                        crate::graph::hnsw_bridge::BRIDGE_MIN_VECTORS,
                    )
                }))
                .unwrap_or_else(|_| {
                    tracing::warn!("graph hnsw bridge build panicked; segment stays exact-scored");
                    None
                });
                let cell = match &*seg {
                    CsrStorage::Heap(s) => &s.hnsw_bridge,
                    CsrStorage::Mmap(s) => &s.hnsw_bridge,
                };
                let _ = cell.set(built);
            });
        }
        None
    }

    /// Test hook: build the bridge regardless of the production minimum, so
    /// small fixtures can exercise the HnswPreFilter path.
    #[cfg(test)]
    pub fn hnsw_bridge_for_test(&self) -> Option<&crate::graph::hnsw_bridge::GraphHnsw> {
        let cell = match self {
            CsrStorage::Heap(s) => &s.hnsw_bridge,
            CsrStorage::Mmap(s) => &s.hnsw_bridge,
        };
        cell.get_or_init(|| crate::graph::hnsw_bridge::GraphHnsw::build(self, 1))
            .as_ref()
    }

    /// Node property blob (empty for pre-v5 segments).
    pub fn node_props_blob(&self) -> &[u8] {
        match self {
            CsrStorage::Heap(s) => &s.node_props,
            CsrStorage::Mmap(s) => s.node_props_blob(),
        }
    }

    /// Edge property blob (empty for pre-v5 segments).
    pub fn edge_props_blob(&self) -> &[u8] {
        match self {
            CsrStorage::Heap(s) => &s.edge_props,
            CsrStorage::Mmap(s) => s.edge_props_blob(),
        }
    }

    /// Node properties for a CSR row (empty for none / pre-v5 segments).
    pub fn node_properties(&self, row: u32) -> crate::graph::types::PropertyMap {
        match self {
            CsrStorage::Heap(s) => s.node_properties(row),
            CsrStorage::Mmap(s) => s.node_properties(row),
        }
    }

    /// Single node property lookup without materializing the map.
    pub fn node_property(&self, row: u32, key: u16) -> Option<crate::graph::types::PropertyValue> {
        match self {
            CsrStorage::Heap(s) => s.node_property(row, key),
            CsrStorage::Mmap(s) => s.node_property(row, key),
        }
    }

    /// Node embedding for a CSR row (None for none / pre-v5 segments).
    pub fn node_embedding(&self, row: u32) -> Option<Vec<f32>> {
        match self {
            CsrStorage::Heap(s) => s.node_embedding(row),
            CsrStorage::Mmap(s) => s.node_embedding(row),
        }
    }

    /// Edge weight by edge index (1.0 default / pre-v5 segments).
    pub fn edge_weight(&self, edge_idx: u32) -> f64 {
        match self {
            CsrStorage::Heap(s) => s.edge_weight(edge_idx),
            CsrStorage::Mmap(s) => s.edge_weight(edge_idx),
        }
    }

    /// Edge properties by edge index (empty for none / pre-v5 segments).
    pub fn edge_properties(&self, edge_idx: u32) -> crate::graph::types::PropertyMap {
        match self {
            CsrStorage::Heap(s) => s.edge_properties(edge_idx),
            CsrStorage::Mmap(s) => s.edge_properties(edge_idx),
        }
    }

    /// Write the segment to a file (only supported for Heap variant).
    /// For Mmap variant, the file already exists on disk.
    pub fn write_to_file(&self, path: &Path) -> Result<(), CsrError> {
        match self {
            CsrStorage::Heap(s) => s.write_to_file(path),
            CsrStorage::Mmap(_) => Ok(()), // Already on disk
        }
    }

    /// Serialize to bytes (only meaningful for Heap variant).
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            CsrStorage::Heap(s) => s.to_bytes(),
            CsrStorage::Mmap(_) => Vec::new(), // Not applicable
        }
    }

    /// Returns valid outgoing neighbor row indices filtered by node label.
    pub fn neighbors_by_label(&self, row: u32, label: u16) -> Vec<u32> {
        match self {
            CsrStorage::Heap(s) => s.neighbors_by_label(row, label),
            CsrStorage::Mmap(s) => {
                let r = row as usize;
                if r >= s.header.node_count as usize {
                    return Vec::new();
                }
                let ro = s.row_offsets();
                let start = ro[r] as usize;
                let end = ro[r + 1] as usize;
                let label_bm = match s.label_index.nodes_with_label(label) {
                    Some(bm) => bm,
                    None => return Vec::new(),
                };
                let ci = s.col_indices();
                let mut result = Vec::new();
                for idx in start..end {
                    if s.validity.contains(idx as u32) {
                        let dst_row = ci[idx];
                        if label_bm.contains(dst_row) {
                            result.push(dst_row);
                        }
                    }
                }
                result
            }
        }
    }

    /// Returns valid outgoing neighbor edges filtered by edge type.
    pub fn edges_by_type(&self, row: u32, edge_type: u16) -> Vec<(u32, EdgeMeta)> {
        match self {
            CsrStorage::Heap(s) => s
                .edges_by_type(row, edge_type)
                .into_iter()
                .map(|(col, em)| (col, *em))
                .collect(),
            CsrStorage::Mmap(s) => {
                let r = row as usize;
                if r >= s.header.node_count as usize {
                    return Vec::new();
                }
                let ro = s.row_offsets();
                let start = ro[r] as usize;
                let end = ro[r + 1] as usize;
                let type_bm = match s.edge_type_index.edges_of_type(edge_type) {
                    Some(bm) => bm,
                    None => return Vec::new(),
                };
                let ci = s.col_indices();
                let em = s.edge_meta();
                let mut result = Vec::new();
                for idx in start..end {
                    let idx32 = idx as u32;
                    if s.validity.contains(idx32) && type_bm.contains(idx32) {
                        result.push((ci[idx], em[idx]));
                    }
                }
                result
            }
        }
    }
}

impl From<CsrSegment> for CsrStorage {
    fn from(seg: CsrSegment) -> Self {
        CsrStorage::Heap(seg)
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use super::*;
    use crate::graph::memgraph::MemGraph;
    use crate::graph::types::PropertyValue;

    /// Deterministic pseudo-random embedding (no Math.random in tests).
    fn embedding(i: u64, dim: usize) -> Vec<f32> {
        let mut state = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((state >> 33) as f32 / (1u64 << 31) as f32) - 0.5
            })
            .collect()
    }

    /// A heap-backed segment with real per-node properties AND embeddings,
    /// so both `property_index()` and `hnsw_bridge_for_test()` have
    /// something non-trivial to build (an all-empty fixture would build
    /// indexes that are themselves empty, masking a broken resident_bytes
    /// count with a true-by-accident 0 -> 0 "no growth" result).
    fn heap_segment_with_props_and_embeddings(n: usize, dim: usize) -> CsrStorage {
        let mut g = MemGraph::new(1_000_000);
        let mut keys = Vec::with_capacity(n);
        for i in 0..n {
            let props = smallvec![(0u16, PropertyValue::Int(i as i64))];
            keys.push(g.add_node(smallvec![0u16], props, Some(embedding(i as u64, dim)), 1));
        }
        for i in 0..n.saturating_sub(1) {
            g.add_edge(keys[i], keys[i + 1], 1, 1.0, None, 2)
                .expect("edge");
        }
        let frozen = g.freeze().expect("freeze");
        let seg = CsrSegment::from_frozen(frozen, 10).expect("csr");
        CsrStorage::Heap(seg)
    }

    #[test]
    fn test_resident_bytes_grows_after_index_and_bridge_build() {
        let storage = heap_segment_with_props_and_embeddings(64, 8);

        let baseline = storage.resident_bytes();

        // Force the property index to build (get_or_init, not a call that
        // would already have been triggered by anything above).
        let _ = storage.property_index();
        let after_props_index = storage.resident_bytes();
        assert!(
            after_props_index > baseline,
            "resident_bytes must grow once the lazily-built SegmentPropertyIndexes \
             is materialized: baseline={baseline}, after={after_props_index}"
        );

        // Force the HNSW bridge to build too (test hook bypasses the
        // production BRIDGE_MIN_VECTORS gate).
        assert!(
            storage.hnsw_bridge_for_test().is_some(),
            "fixture has embeddings on every node; bridge build must succeed"
        );
        let after_bridge = storage.resident_bytes();
        assert!(
            after_bridge > after_props_index,
            "resident_bytes must grow again once the lazily-built GraphHnsw bridge \
             is materialized: after_props_index={after_props_index}, after_bridge={after_bridge}"
        );
    }

    #[test]
    fn test_resident_bytes_mmap_zeroes_core_arrays_heap_counts_them() {
        // Heap segment: row/col/edge_meta/node_meta count in full (these are
        // genuinely heap-owned Vec allocations).
        let heap = heap_segment_with_props_and_embeddings(16, 4);
        let heap_core = std::mem::size_of_val(heap.row_offsets())
            + std::mem::size_of_val(heap.col_indices())
            + std::mem::size_of_val(heap.edge_meta())
            + std::mem::size_of_val(heap.node_meta());
        assert!(
            heap.resident_bytes() >= heap_core,
            "Heap variant must count its row/col/edge_meta/node_meta arrays in full"
        );
    }
}
