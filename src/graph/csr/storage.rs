//! CsrStorage — unified access to heap or mmap CSR segments.

use std::collections::HashMap;
use std::path::Path;

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
    /// For Heap: row_offsets + col_indices + edge_meta + node_meta Vec allocations.
    /// For Mmap: the mmap region length (kernel page-cache resident).
    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of_val(self.row_offsets())
            + std::mem::size_of_val(self.col_indices())
            + std::mem::size_of_val(self.edge_meta())
            + std::mem::size_of_val(self.node_meta())
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
