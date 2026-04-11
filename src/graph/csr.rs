//! CSR (Compressed Sparse Row) immutable graph segment.
//!
//! Built from a `FrozenMemGraph`. Neighbor iteration is a contiguous memory scan
//! from `col_indices[row_offsets[v]]` to `col_indices[row_offsets[v+1]]`.
//! Edge deletions use a Roaring validity bitmap without modifying CSR arrays.

use std::collections::HashMap;
use std::path::Path;

use memmap2::Mmap;
use roaring::RoaringBitmap;
use slotmap::Key;

use crate::graph::index::{EdgeTypeIndex, LabelIndex, MphNodeIndex};
use crate::graph::memgraph::FrozenMemGraph;
use crate::graph::types::{EdgeMeta, GraphSegmentHeader, NodeKey, NodeMeta};

/// Errors from CSR construction or deserialization.
#[derive(Debug, PartialEq, Eq)]
pub enum CsrError {
    /// Input graph has zero nodes.
    EmptyGraph,
    /// Edge references a node that does not exist in the frozen set.
    InvalidNodeRef,
    /// CRC32 checksum mismatch on load -- data is corrupted.
    ChecksumMismatch { expected: u64, actual: u64 },
    /// Data too short or structurally invalid.
    InvalidData(String),
    /// I/O error description (not std::io::Error to keep PartialEq).
    IoError(String),
}

/// Immutable CSR graph segment.
#[derive(Debug)]
pub struct CsrSegment {
    pub header: GraphSegmentHeader,
    /// Length = node_count + 1. row_offsets[i] is the start index in col_indices for node i.
    pub row_offsets: Vec<u32>,
    /// Length = edge_count. Target node CSR-row indices.
    pub col_indices: Vec<u32>,
    /// Parallel to col_indices. Per-edge metadata.
    pub edge_meta: Vec<EdgeMeta>,
    /// Parallel to rows (length = node_count). Per-node metadata.
    pub node_meta: Vec<NodeMeta>,
    /// Validity bitmap: bit set = edge is live. One bit per edge.
    pub validity: RoaringBitmap,
    /// Node key to CSR row index mapping -- kept for backward compatibility.
    /// Prefer `mph` for O(1) lookup with ~3 bits/key overhead.
    pub node_id_to_row: HashMap<NodeKey, u32>,
    /// Minimal perfect hash index: NodeKey -> CSR row (~3 bits/key).
    pub mph: MphNodeIndex,
    /// Per-label Roaring bitmap index for O(1) label filtering.
    pub label_index: LabelIndex,
    /// Per-edge-type Roaring bitmap index for O(1) edge type filtering.
    pub edge_type_index: EdgeTypeIndex,
    /// LSN at which this segment was created.
    pub created_lsn: u64,
}

impl CsrSegment {
    /// Build a CSR segment from a frozen MemGraph snapshot.
    ///
    /// Steps:
    /// 1. Assign dense row indices to nodes (sorted by NodeKey for determinism)
    /// 2. For each node, collect outgoing edges sorted by destination
    /// 3. Build row_offsets prefix sum
    /// 4. Populate col_indices, edge_meta, node_meta
    /// 5. Initialize validity bitmap with all edges valid
    /// 6. Compute CRC32 checksum for header
    pub fn from_frozen(frozen: FrozenMemGraph, lsn: u64) -> Result<Self, CsrError> {
        if frozen.nodes.is_empty() {
            return Err(CsrError::EmptyGraph);
        }

        let node_count = frozen.nodes.len();

        // Sort nodes by key for deterministic row assignment.
        let mut sorted_nodes = frozen.nodes;
        sorted_nodes.sort_by_key(|(k, _)| *k);

        // Build key->row mapping.
        let mut node_id_to_row: HashMap<NodeKey, u32> = HashMap::with_capacity(node_count);
        for (row, (key, _)) in sorted_nodes.iter().enumerate() {
            node_id_to_row.insert(*key, row as u32);
        }

        // Build per-node outgoing edge lists.
        // edges_by_src[row] = Vec<(dst_row, edge)>
        let mut edges_by_src: Vec<Vec<(u32, &crate::graph::types::MutableEdge)>> =
            vec![Vec::new(); node_count];

        for (_, edge) in &frozen.edges {
            let Some(&src_row) = node_id_to_row.get(&edge.src) else {
                return Err(CsrError::InvalidNodeRef);
            };
            let Some(&dst_row) = node_id_to_row.get(&edge.dst) else {
                return Err(CsrError::InvalidNodeRef);
            };
            edges_by_src[src_row as usize].push((dst_row, edge));
        }

        // Sort each node's edges by destination for cache-friendly traversal.
        for edges in &mut edges_by_src {
            edges.sort_by_key(|(dst, _)| *dst);
        }

        // Build row_offsets prefix sum.
        let mut row_offsets = Vec::with_capacity(node_count + 1);
        let mut offset: u32 = 0;
        for edges in &edges_by_src {
            row_offsets.push(offset);
            offset += edges.len() as u32;
        }
        row_offsets.push(offset);
        let edge_count = offset as usize;

        // Build col_indices and edge_meta.
        let mut col_indices = Vec::with_capacity(edge_count);
        let mut edge_meta = Vec::with_capacity(edge_count);
        for edges in &edges_by_src {
            for &(dst_row, edge) in edges {
                col_indices.push(dst_row);
                edge_meta.push(EdgeMeta {
                    edge_type: edge.edge_type,
                    flags: 0,
                    property_offset: 0,
                });
            }
        }

        // Build node_meta.
        let mut node_meta = Vec::with_capacity(node_count);
        let mut min_node_id = u64::MAX;
        let mut max_node_id = 0u64;
        for (key, node) in &sorted_nodes {
            let id_bits = key.data().as_ffi();
            if id_bits < min_node_id {
                min_node_id = id_bits;
            }
            if id_bits > max_node_id {
                max_node_id = id_bits;
            }
            // Build label bitmap from labels SmallVec.
            let mut label_bitmap: u32 = 0;
            for &label in &node.labels {
                if label < 32 {
                    label_bitmap |= 1 << label;
                }
            }
            node_meta.push(NodeMeta {
                external_id: id_bits,
                label_bitmap,
                property_offset: 0,
                created_lsn: node.created_lsn,
                deleted_lsn: node.deleted_lsn,
            });
        }

        // Initialize validity bitmap: all edges valid.
        let mut validity = RoaringBitmap::new();
        for i in 0..edge_count as u32 {
            validity.insert(i);
        }

        // Compute CRC32 checksum of key header fields.
        let checksum = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&(node_count as u32).to_le_bytes());
            hasher.update(&(edge_count as u32).to_le_bytes());
            hasher.update(&lsn.to_le_bytes());
            hasher.finalize() as u64
        };

        let header = GraphSegmentHeader {
            magic: *b"MNGR",
            version: 1,
            node_count: node_count as u32,
            edge_count: edge_count as u32,
            min_node_id,
            max_node_id,
            row_offsets_offset: 0, // populated during serialization
            col_indices_offset: 0,
            edge_meta_offset: 0,
            validity_bitmap_offset: 0,
            created_lsn: lsn,
            checksum,
        };

        // Build indexes (Phase 116).
        let sorted_keys: Vec<NodeKey> = sorted_nodes.iter().map(|(k, _)| *k).collect();
        let mph = MphNodeIndex::build(&sorted_keys);
        let label_index = LabelIndex::build(&node_meta);
        let edge_type_index = EdgeTypeIndex::build(&edge_meta);

        Ok(Self {
            header,
            row_offsets,
            col_indices,
            edge_meta,
            node_meta,
            validity,
            node_id_to_row,
            mph,
            label_index,
            edge_type_index,
            created_lsn: lsn,
        })
    }

    /// Returns the slice of outgoing neighbor row indices for the given CSR row.
    /// Returns an empty slice if `row` is out of bounds.
    pub fn neighbors_out(&self, row: u32) -> &[u32] {
        let r = row as usize;
        if r >= self.header.node_count as usize {
            return &[];
        }
        let start = self.row_offsets[r] as usize;
        let end = self.row_offsets[r + 1] as usize;
        &self.col_indices[start..end]
    }

    /// Mark an edge as deleted in the validity bitmap.
    pub fn mark_deleted(&mut self, edge_idx: u32) {
        self.validity.remove(edge_idx);
    }

    /// Check if an edge is still valid (not deleted).
    pub fn is_valid(&self, edge_idx: u32) -> bool {
        self.validity.contains(edge_idx)
    }

    /// Node count from header.
    pub fn node_count(&self) -> u32 {
        self.header.node_count
    }

    /// Edge count from header.
    pub fn edge_count(&self) -> u32 {
        self.header.edge_count
    }

    /// Look up CSR row index for a NodeKey.
    ///
    /// Uses boomphf MPH (O(1), ~3 bits/key) with false-positive rejection.
    /// Falls back to HashMap if MPH returns None (should not happen for valid keys).
    pub fn lookup_node(&self, key: NodeKey) -> Option<u32> {
        self.mph
            .lookup(key)
            .or_else(|| self.node_id_to_row.get(&key).copied())
    }

    /// Returns valid outgoing neighbor row indices filtered by node label.
    ///
    /// Uses Roaring bitmap intersection: edges whose destination node carries
    /// the given label are returned without scanning all neighbors.
    pub fn neighbors_by_label(&self, row: u32, label: u16) -> Vec<u32> {
        let r = row as usize;
        if r >= self.header.node_count as usize {
            return Vec::new();
        }
        let start = self.row_offsets[r] as usize;
        let end = self.row_offsets[r + 1] as usize;
        let label_bm = match self.label_index.nodes_with_label(label) {
            Some(bm) => bm,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        for idx in start..end {
            if self.validity.contains(idx as u32) {
                let dst_row = self.col_indices[idx];
                if label_bm.contains(dst_row) {
                    result.push(dst_row);
                }
            }
        }
        result
    }

    /// Returns valid outgoing neighbor edges filtered by edge type.
    ///
    /// Uses per-edge-type Roaring bitmap for O(1) membership test per edge.
    pub fn edges_by_type(&self, row: u32, edge_type: u16) -> Vec<(u32, &EdgeMeta)> {
        let r = row as usize;
        if r >= self.header.node_count as usize {
            return Vec::new();
        }
        let start = self.row_offsets[r] as usize;
        let end = self.row_offsets[r + 1] as usize;
        let type_bm = match self.edge_type_index.edges_of_type(edge_type) {
            Some(bm) => bm,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        for idx in start..end {
            let idx32 = idx as u32;
            if self.validity.contains(idx32) && type_bm.contains(idx32) {
                result.push((self.col_indices[idx], &self.edge_meta[idx]));
            }
        }
        result
    }

    /// Iterator over valid outgoing neighbor edges for a CSR row.
    /// Yields (col_index, &EdgeMeta) pairs, skipping invalid edges via validity bitmap.
    /// Returns an empty iterator if `row` is out of bounds.
    pub fn neighbor_edges(&self, row: u32) -> impl Iterator<Item = (u32, &EdgeMeta)> {
        let r = row as usize;
        let (start, end) = if r < self.header.node_count as usize {
            (
                self.row_offsets[r] as usize,
                self.row_offsets[r + 1] as usize,
            )
        } else {
            (0, 0)
        };
        let validity = &self.validity;
        (start..end).filter_map(move |idx| {
            if validity.contains(idx as u32) {
                Some((self.col_indices[idx], &self.edge_meta[idx]))
            } else {
                None
            }
        })
    }

    /// Serialize the CSR segment to a contiguous byte buffer.
    /// Layout: header (128B) | row_offsets | col_indices | edge_meta | node_meta
    pub fn to_bytes(&self) -> Vec<u8> {
        let header_size = core::mem::size_of::<GraphSegmentHeader>();
        let ro_size = self.row_offsets.len() * 4;
        let ci_size = self.col_indices.len() * 4;
        let em_size = self.edge_meta.len() * core::mem::size_of::<EdgeMeta>();
        let nm_size = self.node_meta.len() * core::mem::size_of::<NodeMeta>();

        let total = header_size + ro_size + ci_size + em_size + nm_size;
        let mut buf = Vec::with_capacity(total);

        // Write header with computed offsets.
        let ro_offset = header_size as u64;
        let ci_offset = ro_offset + ro_size as u64;
        let em_offset = ci_offset + ci_size as u64;
        // validity_bitmap_offset: not written inline (Roaring needs separate serialization)
        let _nm_offset = em_offset + em_size as u64;

        // Write magic, version, counts.
        buf.extend_from_slice(&self.header.magic);
        buf.extend_from_slice(&self.header.version.to_le_bytes());
        buf.extend_from_slice(&self.header.node_count.to_le_bytes());
        buf.extend_from_slice(&self.header.edge_count.to_le_bytes());
        buf.extend_from_slice(&self.header.min_node_id.to_le_bytes());
        buf.extend_from_slice(&self.header.max_node_id.to_le_bytes());
        buf.extend_from_slice(&ro_offset.to_le_bytes());
        buf.extend_from_slice(&ci_offset.to_le_bytes());
        buf.extend_from_slice(&em_offset.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // validity_bitmap_offset placeholder
        buf.extend_from_slice(&self.header.created_lsn.to_le_bytes());
        buf.extend_from_slice(&self.header.checksum.to_le_bytes());

        // Pad header to 128 bytes.
        while buf.len() < header_size {
            buf.push(0);
        }

        // Write row_offsets.
        for &v in &self.row_offsets {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // Write col_indices.
        for &v in &self.col_indices {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // Write edge_meta.
        for em in &self.edge_meta {
            buf.extend_from_slice(&em.edge_type.to_le_bytes());
            buf.extend_from_slice(&em.flags.to_le_bytes());
            buf.extend_from_slice(&em.property_offset.to_le_bytes());
        }

        // Write node_meta.
        for nm in &self.node_meta {
            buf.extend_from_slice(&nm.external_id.to_le_bytes());
            buf.extend_from_slice(&nm.label_bitmap.to_le_bytes());
            buf.extend_from_slice(&nm.property_offset.to_le_bytes());
            buf.extend_from_slice(&nm.created_lsn.to_le_bytes());
            buf.extend_from_slice(&nm.deleted_lsn.to_le_bytes());
        }

        buf
    }

    /// Write CSR segment bytes to a file.
    pub fn write_to_file(&self, path: &Path) -> Result<(), CsrError> {
        let bytes = self.to_bytes();
        std::fs::write(path, &bytes).map_err(|e| CsrError::IoError(e.to_string()))
    }

    /// Reconstruct a CsrSegment from serialized bytes (as produced by `to_bytes()`).
    ///
    /// Validates the CRC32 checksum. Returns `CsrError::ChecksumMismatch` on corruption.
    pub fn from_bytes(data: &[u8]) -> Result<Self, CsrError> {
        let header_size = core::mem::size_of::<GraphSegmentHeader>(); // 128
        if data.len() < header_size {
            return Err(CsrError::InvalidData("data shorter than header".to_owned()));
        }

        // Parse header fields.
        let magic: [u8; 4] = data[0..4]
            .try_into()
            .map_err(|_| CsrError::InvalidData("bad magic".to_owned()))?;
        if magic != *b"MNGR" {
            return Err(CsrError::InvalidData(format!("bad magic: {:?}", magic)));
        }

        let version = u32::from_le_bytes(read4(data, 4)?);
        let node_count = u32::from_le_bytes(read4(data, 8)?);
        let edge_count = u32::from_le_bytes(read4(data, 12)?);
        let min_node_id = u64::from_le_bytes(read8(data, 16)?);
        let max_node_id = u64::from_le_bytes(read8(data, 24)?);
        let _ro_offset = u64::from_le_bytes(read8(data, 32)?);
        let _ci_offset = u64::from_le_bytes(read8(data, 40)?);
        let _em_offset = u64::from_le_bytes(read8(data, 48)?);
        let _vb_offset = u64::from_le_bytes(read8(data, 56)?);
        let created_lsn = u64::from_le_bytes(read8(data, 64)?);
        let stored_checksum = u64::from_le_bytes(read8(data, 72)?);

        // Validate CRC32.
        let computed_checksum = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&node_count.to_le_bytes());
            hasher.update(&edge_count.to_le_bytes());
            hasher.update(&created_lsn.to_le_bytes());
            hasher.finalize() as u64
        };
        if stored_checksum != computed_checksum {
            return Err(CsrError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        let nc = node_count as usize;
        let ec = edge_count as usize;
        let em_elem_size = core::mem::size_of::<EdgeMeta>(); // 8
        let nm_elem_size = core::mem::size_of::<NodeMeta>(); // 32

        let expected_len =
            header_size + (nc + 1) * 4 + ec * 4 + ec * em_elem_size + nc * nm_elem_size;
        if data.len() < expected_len {
            return Err(CsrError::InvalidData(format!(
                "data too short: {} < {}",
                data.len(),
                expected_len
            )));
        }

        // Parse row_offsets.
        let mut pos = header_size;
        let mut row_offsets = Vec::with_capacity(nc + 1);
        for _ in 0..=nc {
            row_offsets.push(u32::from_le_bytes(read4(data, pos)?));
            pos += 4;
        }

        // Parse col_indices.
        let mut col_indices = Vec::with_capacity(ec);
        for _ in 0..ec {
            col_indices.push(u32::from_le_bytes(read4(data, pos)?));
            pos += 4;
        }

        // Parse edge_meta.
        let mut edge_meta = Vec::with_capacity(ec);
        for _ in 0..ec {
            let edge_type = u16::from_le_bytes(read2(data, pos)?);
            let flags = u16::from_le_bytes(read2(data, pos + 2)?);
            let property_offset = u32::from_le_bytes(read4(data, pos + 4)?);
            edge_meta.push(EdgeMeta {
                edge_type,
                flags,
                property_offset,
            });
            pos += em_elem_size;
        }

        // Parse node_meta.
        let mut node_meta = Vec::with_capacity(nc);
        for _ in 0..nc {
            let external_id = u64::from_le_bytes(read8(data, pos)?);
            let label_bitmap = u32::from_le_bytes(read4(data, pos + 8)?);
            let property_offset = u32::from_le_bytes(read4(data, pos + 12)?);
            let nm_created_lsn = u64::from_le_bytes(read8(data, pos + 16)?);
            let deleted_lsn = u64::from_le_bytes(read8(data, pos + 24)?);
            node_meta.push(NodeMeta {
                external_id,
                label_bitmap,
                property_offset,
                created_lsn: nm_created_lsn,
                deleted_lsn,
            });
            pos += nm_elem_size;
        }

        // Rebuild validity bitmap: all edges valid (fresh load).
        let mut validity = RoaringBitmap::new();
        for i in 0..ec as u32 {
            validity.insert(i);
        }

        // Rebuild node_id_to_row from node_meta external_id.
        // The external_id is the raw u64 from NodeKey::data().as_ffi().
        // We need to reconstruct NodeKey from the u64 -- use KeyData::from_ffi.
        let mut node_id_to_row: HashMap<NodeKey, u32> = HashMap::with_capacity(nc);
        let mut sorted_keys = Vec::with_capacity(nc);
        for (row, nm) in node_meta.iter().enumerate() {
            let key_data = slotmap::KeyData::from_ffi(nm.external_id);
            let nk = NodeKey::from(key_data);
            node_id_to_row.insert(nk, row as u32);
            sorted_keys.push(nk);
        }

        // Rebuild indexes.
        let mph = MphNodeIndex::build(&sorted_keys);
        let label_index = LabelIndex::build(&node_meta);
        let edge_type_index = EdgeTypeIndex::build(&edge_meta);

        let header = GraphSegmentHeader {
            magic,
            version,
            node_count,
            edge_count,
            min_node_id,
            max_node_id,
            row_offsets_offset: _ro_offset,
            col_indices_offset: _ci_offset,
            edge_meta_offset: _em_offset,
            validity_bitmap_offset: _vb_offset,
            created_lsn,
            checksum: stored_checksum,
        };

        Ok(Self {
            header,
            row_offsets,
            col_indices,
            edge_meta,
            node_meta,
            validity,
            node_id_to_row,
            mph,
            label_index,
            edge_type_index,
            created_lsn,
        })
    }

    /// Load a CsrSegment from a file, validating its CRC32 checksum.
    pub fn from_file(path: &Path) -> Result<Self, CsrError> {
        let data = std::fs::read(path).map_err(|e| CsrError::IoError(e.to_string()))?;
        Self::from_bytes(&data)
    }
}

// ---------------------------------------------------------------------------
// MmapCsrSegment — zero-copy memory-mapped CSR segment
// ---------------------------------------------------------------------------

/// Memory-mapped CSR segment. Owns the `Mmap` handle; CSR arrays are
/// zero-copy slices into the mapped region. Heap usage is limited to
/// metadata (Roaring bitmap, indexes, HashMap) — the large arrays
/// (`row_offsets`, `col_indices`, `edge_meta`, `node_meta`) live in
/// the kernel page cache.
pub struct MmapCsrSegment {
    /// The memory-mapped file data. Must be kept alive for slice validity.
    _mmap: Mmap,
    pub header: GraphSegmentHeader,
    /// Pointer into mmap: length = node_count + 1.
    row_offsets_ptr: *const u32,
    row_offsets_len: usize,
    /// Pointer into mmap: length = edge_count.
    col_indices_ptr: *const u32,
    col_indices_len: usize,
    /// Pointer into mmap: length = edge_count.
    edge_meta_ptr: *const EdgeMeta,
    edge_meta_len: usize,
    /// Pointer into mmap: length = node_count.
    node_meta_ptr: *const NodeMeta,
    node_meta_len: usize,
    /// Validity bitmap (heap-allocated, mutable).
    pub validity: RoaringBitmap,
    pub node_id_to_row: HashMap<NodeKey, u32>,
    pub mph: MphNodeIndex,
    pub label_index: LabelIndex,
    pub edge_type_index: EdgeTypeIndex,
    pub created_lsn: u64,
}

// SAFETY: MmapCsrSegment contains raw pointers into an immutable Mmap region.
// The Mmap handle (_mmap) is Send+Sync itself, and the pointed-to data is
// immutable (read-only mapping). All other fields (RoaringBitmap, HashMap,
// MphNodeIndex, etc.) are Send+Sync. No mutable aliasing is possible because
// the pointers only produce shared references via accessor methods.
unsafe impl Send for MmapCsrSegment {}
// SAFETY: Same reasoning — the mmap'd region is immutable, raw pointers only
// yield &[T] references, and mutation is limited to the validity bitmap which
// requires &mut self.
unsafe impl Sync for MmapCsrSegment {}

impl MmapCsrSegment {
    /// Load a CSR segment via memory-mapped I/O.
    ///
    /// The file is mapped read-only. CSR arrays are zero-copy slices into the
    /// mapped region. Validates magic bytes and CRC32 checksum.
    pub fn from_mmap_file(path: &Path) -> Result<Self, CsrError> {
        let file =
            std::fs::File::open(path).map_err(|e| CsrError::IoError(e.to_string()))?;

        // SAFETY: The file is opened read-only. The Mmap handle keeps the
        // mapping alive for the lifetime of this struct. We validate all
        // offsets and lengths before constructing slices.
        let mmap = unsafe { Mmap::map(&file) }
            .map_err(|e| CsrError::IoError(e.to_string()))?;

        let header_size = core::mem::size_of::<GraphSegmentHeader>(); // 128
        if mmap.len() < header_size {
            return Err(CsrError::InvalidData(
                "mmap'd data shorter than header".to_owned(),
            ));
        }

        // Parse header fields from the mmap'd region.
        let data: &[u8] = &mmap;
        let magic: [u8; 4] = data[0..4]
            .try_into()
            .map_err(|_| CsrError::InvalidData("bad magic".to_owned()))?;
        if magic != *b"MNGR" {
            return Err(CsrError::InvalidData(format!("bad magic: {magic:?}")));
        }

        let version = u32::from_le_bytes(read4(data, 4)?);
        let node_count = u32::from_le_bytes(read4(data, 8)?);
        let edge_count = u32::from_le_bytes(read4(data, 12)?);
        let min_node_id = u64::from_le_bytes(read8(data, 16)?);
        let max_node_id = u64::from_le_bytes(read8(data, 24)?);
        let ro_offset = u64::from_le_bytes(read8(data, 32)?);
        let ci_offset = u64::from_le_bytes(read8(data, 40)?);
        let em_offset = u64::from_le_bytes(read8(data, 48)?);
        let vb_offset = u64::from_le_bytes(read8(data, 56)?);
        let created_lsn = u64::from_le_bytes(read8(data, 64)?);
        let stored_checksum = u64::from_le_bytes(read8(data, 72)?);

        // Validate CRC32.
        let computed_checksum = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&node_count.to_le_bytes());
            hasher.update(&edge_count.to_le_bytes());
            hasher.update(&created_lsn.to_le_bytes());
            hasher.finalize() as u64
        };
        if stored_checksum != computed_checksum {
            return Err(CsrError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        let nc = node_count as usize;
        let ec = edge_count as usize;
        let em_elem_size = core::mem::size_of::<EdgeMeta>(); // 8
        let nm_elem_size = core::mem::size_of::<NodeMeta>(); // 32

        let expected_len =
            header_size + (nc + 1) * 4 + ec * 4 + ec * em_elem_size + nc * nm_elem_size;
        if mmap.len() < expected_len {
            return Err(CsrError::InvalidData(format!(
                "mmap'd data too short: {} < {expected_len}",
                mmap.len(),
            )));
        }

        // Compute array pointers from the mmap base.
        let base = mmap.as_ptr();

        // SAFETY: header is 128 bytes (64-aligned). u32 requires 4-byte alignment.
        // 128 % 4 == 0, so row_offsets_ptr is correctly aligned. We verified that
        // the mmap region is large enough to hold all arrays. The types are
        // #[repr(C)] with known sizes validated by const assertions in types.rs.
        // The Mmap is immutable and lives as long as self (_mmap field).
        let ro_ptr = unsafe { base.add(header_size) } as *const u32;
        let ro_len = nc + 1;

        let ci_start = header_size + ro_len * 4;
        let ci_ptr = unsafe { base.add(ci_start) } as *const u32;
        let ci_len = ec;

        let em_start = ci_start + ec * 4;
        // SAFETY: EdgeMeta is #[repr(C, align(8))], size 8. We verify alignment
        // at runtime and fall back to heap loading if it's violated.
        let em_ptr = unsafe { base.add(em_start) } as *const EdgeMeta;
        if (em_ptr as usize) % core::mem::align_of::<EdgeMeta>() != 0 {
            return Err(CsrError::InvalidData(
                "edge_meta alignment violated in mmap".to_owned(),
            ));
        }
        let em_len = ec;

        let nm_start = em_start + ec * em_elem_size;
        let nm_ptr = unsafe { base.add(nm_start) } as *const NodeMeta;
        if (nm_ptr as usize) % core::mem::align_of::<NodeMeta>() != 0 {
            return Err(CsrError::InvalidData(
                "node_meta alignment violated in mmap".to_owned(),
            ));
        }
        let nm_len = nc;

        // Rebuild indexes from mmap'd slices.
        // SAFETY: pointers were validated above, mmap is alive for this scope.
        let node_meta_slice =
            unsafe { core::slice::from_raw_parts(nm_ptr, nm_len) };
        let edge_meta_slice =
            unsafe { core::slice::from_raw_parts(em_ptr, em_len) };

        let mut node_id_to_row: HashMap<NodeKey, u32> = HashMap::with_capacity(nc);
        let mut sorted_keys = Vec::with_capacity(nc);
        for (row, nm) in node_meta_slice.iter().enumerate() {
            let key_data = slotmap::KeyData::from_ffi(nm.external_id);
            let nk = NodeKey::from(key_data);
            node_id_to_row.insert(nk, row as u32);
            sorted_keys.push(nk);
        }

        let mph = MphNodeIndex::build(&sorted_keys);
        let label_index = LabelIndex::build(node_meta_slice);
        let edge_type_index = EdgeTypeIndex::build(edge_meta_slice);

        // Initialize validity bitmap: all edges valid.
        let mut validity = RoaringBitmap::new();
        for i in 0..ec as u32 {
            validity.insert(i);
        }

        let header = GraphSegmentHeader {
            magic,
            version,
            node_count,
            edge_count,
            min_node_id,
            max_node_id,
            row_offsets_offset: ro_offset,
            col_indices_offset: ci_offset,
            edge_meta_offset: em_offset,
            validity_bitmap_offset: vb_offset,
            created_lsn,
            checksum: stored_checksum,
        };

        Ok(Self {
            _mmap: mmap,
            header,
            row_offsets_ptr: ro_ptr,
            row_offsets_len: ro_len,
            col_indices_ptr: ci_ptr,
            col_indices_len: ci_len,
            edge_meta_ptr: em_ptr,
            edge_meta_len: em_len,
            node_meta_ptr: nm_ptr,
            node_meta_len: nm_len,
            validity,
            node_id_to_row,
            mph,
            label_index,
            edge_type_index,
            created_lsn,
        })
    }

    /// Row offsets array (borrowed from mmap).
    pub fn row_offsets(&self) -> &[u32] {
        // SAFETY: _mmap is alive as long as self. The pointer and length were
        // validated in from_mmap_file. The data is immutable.
        unsafe { core::slice::from_raw_parts(self.row_offsets_ptr, self.row_offsets_len) }
    }

    /// Column indices array (borrowed from mmap).
    pub fn col_indices(&self) -> &[u32] {
        // SAFETY: same as row_offsets — pointer validated, mmap alive.
        unsafe { core::slice::from_raw_parts(self.col_indices_ptr, self.col_indices_len) }
    }

    /// Edge metadata array (borrowed from mmap).
    pub fn edge_meta(&self) -> &[EdgeMeta] {
        // SAFETY: same as row_offsets — pointer validated, mmap alive.
        unsafe { core::slice::from_raw_parts(self.edge_meta_ptr, self.edge_meta_len) }
    }

    /// Node metadata array (borrowed from mmap).
    pub fn node_meta(&self) -> &[NodeMeta] {
        // SAFETY: same as row_offsets — pointer validated, mmap alive.
        unsafe { core::slice::from_raw_parts(self.node_meta_ptr, self.node_meta_len) }
    }

    /// Outgoing neighbor row indices for a CSR row.
    pub fn neighbors_out(&self, row: u32) -> &[u32] {
        let r = row as usize;
        if r >= self.header.node_count as usize {
            return &[];
        }
        let ro = self.row_offsets();
        let start = ro[r] as usize;
        let end = ro[r + 1] as usize;
        &self.col_indices()[start..end]
    }

    /// Look up CSR row index for a NodeKey.
    pub fn lookup_node(&self, key: NodeKey) -> Option<u32> {
        self.mph
            .lookup(key)
            .or_else(|| self.node_id_to_row.get(&key).copied())
    }

    /// Node count from header.
    pub fn node_count(&self) -> u32 {
        self.header.node_count
    }

    /// Edge count from header.
    pub fn edge_count(&self) -> u32 {
        self.header.edge_count
    }

    /// Check if an edge is still valid (not deleted).
    pub fn is_valid(&self, edge_idx: u32) -> bool {
        self.validity.contains(edge_idx)
    }

    /// Mark an edge as deleted in the validity bitmap.
    pub fn mark_deleted(&mut self, edge_idx: u32) {
        self.validity.remove(edge_idx);
    }

    /// Iterator over valid outgoing neighbor edges for a CSR row.
    pub fn neighbor_edges(&self, row: u32) -> impl Iterator<Item = (u32, &EdgeMeta)> {
        let r = row as usize;
        let (start, end) = if r < self.header.node_count as usize {
            let ro = self.row_offsets();
            (ro[r] as usize, ro[r + 1] as usize)
        } else {
            (0, 0)
        };
        let ci = self.col_indices();
        let em = self.edge_meta();
        let validity = &self.validity;
        (start..end).filter_map(move |idx| {
            if validity.contains(idx as u32) {
                Some((ci[idx], &em[idx]))
            } else {
                None
            }
        })
    }

    /// Hint the OS about sequential access pattern for the mmap'd region.
    #[cfg(target_os = "linux")]
    pub fn madvise_sequential(&self) {
        // SAFETY: _mmap points to a valid mapped region of len() bytes.
        unsafe {
            libc::madvise(
                self._mmap.as_ptr() as *mut libc::c_void,
                self._mmap.len(),
                libc::MADV_SEQUENTIAL,
            );
        }
    }

    /// Hint the OS about random access pattern for the mmap'd region.
    #[cfg(target_os = "linux")]
    pub fn madvise_random(&self) {
        // SAFETY: _mmap points to a valid mapped region of len() bytes.
        unsafe {
            libc::madvise(
                self._mmap.as_ptr() as *mut libc::c_void,
                self._mmap.len(),
                libc::MADV_RANDOM,
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn madvise_sequential(&self) {}

    #[cfg(not(target_os = "linux"))]
    pub fn madvise_random(&self) {}
}

impl core::fmt::Debug for MmapCsrSegment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MmapCsrSegment")
            .field("node_count", &self.header.node_count)
            .field("edge_count", &self.header.edge_count)
            .field("created_lsn", &self.created_lsn)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// CsrStorage — unified access to heap or mmap CSR segments
// ---------------------------------------------------------------------------

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
    pub fn neighbor_edges(&self, row: u32) -> Vec<(u32, EdgeMeta)> {
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
        (start..end)
            .filter_map(|idx| {
                if validity.contains(idx as u32) {
                    Some((ci[idx], em[idx]))
                } else {
                    None
                }
            })
            .collect()
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

/// Read 2 bytes from `data` at `offset`.
fn read2(data: &[u8], offset: usize) -> Result<[u8; 2], CsrError> {
    data.get(offset..offset + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| CsrError::InvalidData(format!("read2 out of bounds at {offset}")))
}

/// Read 4 bytes from `data` at `offset`.
fn read4(data: &[u8], offset: usize) -> Result<[u8; 4], CsrError> {
    data.get(offset..offset + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| CsrError::InvalidData(format!("read4 out of bounds at {offset}")))
}

/// Read 8 bytes from `data` at `offset`.
fn read8(data: &[u8], offset: usize) -> Result<[u8; 8], CsrError> {
    data.get(offset..offset + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| CsrError::InvalidData(format!("read8 out of bounds at {offset}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::memgraph::MemGraph;
    use smallvec::smallvec;

    fn build_small_graph() -> FrozenMemGraph {
        let mut g = MemGraph::new(100);
        let mut nodes = Vec::new();
        for i in 0..5u16 {
            nodes.push(g.add_node(smallvec![i], smallvec![], None, 1));
        }
        // Create 10 edges: star pattern from node 0, plus some cross-edges.
        for i in 1..5 {
            g.add_edge(nodes[0], nodes[i], 1, 1.0, None, 2).expect("ok");
        }
        for i in 1..4 {
            g.add_edge(nodes[i], nodes[i + 1], 2, 0.5, None, 2)
                .expect("ok");
        }
        // 3 more edges to reach 10
        g.add_edge(nodes[4], nodes[1], 3, 2.0, None, 2).expect("ok");
        g.add_edge(nodes[2], nodes[4], 3, 1.5, None, 2).expect("ok");
        g.add_edge(nodes[3], nodes[1], 3, 0.8, None, 2).expect("ok");

        g.freeze().expect("freeze ok")
    }

    #[test]
    fn test_csr_from_frozen_basic() {
        let frozen = build_small_graph();
        assert_eq!(frozen.nodes.len(), 5);
        assert_eq!(frozen.edges.len(), 10);

        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");
        assert_eq!(csr.node_count(), 5);
        assert_eq!(csr.edge_count(), 10);
        assert_eq!(csr.header.magic, *b"MNGR");
        assert_eq!(csr.header.version, 1);
    }

    #[test]
    fn test_row_offsets_prefix_sum() {
        let frozen = build_small_graph();
        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        // row_offsets should be monotonically non-decreasing, last = edge_count.
        for i in 0..csr.row_offsets.len() - 1 {
            assert!(csr.row_offsets[i] <= csr.row_offsets[i + 1]);
        }
        assert_eq!(
            *csr.row_offsets.last().expect("non-empty"),
            csr.edge_count()
        );
    }

    #[test]
    fn test_neighbors_returns_correct_targets() {
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        // Node 0 has 4 outgoing edges (star center).
        let row0 = csr.lookup_node(node_keys[0]).expect("row exists");
        let neighbors = csr.neighbors_out(row0);
        assert_eq!(neighbors.len(), 4);
    }

    #[test]
    fn test_mark_deleted_and_neighbor_edges() {
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let mut csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        let row0 = csr.lookup_node(node_keys[0]).expect("row exists");
        let all_edges: Vec<_> = csr.neighbor_edges(row0).collect();
        assert_eq!(all_edges.len(), 4);

        // Delete first edge of node 0.
        let first_edge_idx = csr.row_offsets[row0 as usize];
        csr.mark_deleted(first_edge_idx);
        assert!(!csr.is_valid(first_edge_idx));

        let valid_edges: Vec<_> = csr.neighbor_edges(row0).collect();
        assert_eq!(valid_edges.len(), 3);
    }

    #[test]
    fn test_to_bytes_roundtrip_header() {
        let frozen = build_small_graph();
        let csr = CsrSegment::from_frozen(frozen, 42).expect("csr ok");
        let bytes = csr.to_bytes();

        // Read back header fields.
        assert_eq!(&bytes[0..4], b"MNGR");
        let version = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
        assert_eq!(version, 1);
        let nc = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        assert_eq!(nc, 5);
        let ec = u32::from_le_bytes(bytes[12..16].try_into().expect("4 bytes"));
        assert_eq!(ec, 10);
    }

    #[test]
    fn test_deterministic_output() {
        // Build the same graph twice, verify CSR arrays are identical.
        let frozen1 = build_small_graph();
        let frozen2 = build_small_graph();

        let csr1 = CsrSegment::from_frozen(frozen1, 100).expect("csr ok");
        let csr2 = CsrSegment::from_frozen(frozen2, 100).expect("csr ok");

        assert_eq!(csr1.row_offsets, csr2.row_offsets);
        assert_eq!(csr1.col_indices, csr2.col_indices);
    }

    #[test]
    fn test_empty_graph_error() {
        let frozen = FrozenMemGraph {
            nodes: vec![],
            edges: vec![],
        };
        assert_eq!(
            CsrSegment::from_frozen(frozen, 1).unwrap_err(),
            CsrError::EmptyGraph
        );
    }

    // --- Phase 116: Index integration tests ---

    #[test]
    fn test_indexes_built_during_from_frozen() {
        let frozen = build_small_graph();
        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        // LabelIndex: 5 nodes with labels 0..4, each has exactly one label.
        assert_eq!(csr.label_index.label_count(), 5);
        for label in 0..5u16 {
            let bm = csr
                .label_index
                .nodes_with_label(label)
                .expect("label exists");
            assert_eq!(bm.len(), 1);
        }

        // EdgeTypeIndex: 3 edge types (1, 2, 3).
        assert_eq!(csr.edge_type_index.type_count(), 3);
        // Type 1: 4 star edges from node 0.
        let bm = csr.edge_type_index.edges_of_type(1).expect("type 1 exists");
        assert_eq!(bm.len(), 4);
        // Type 2: 3 chain edges.
        let bm = csr.edge_type_index.edges_of_type(2).expect("type 2 exists");
        assert_eq!(bm.len(), 3);
        // Type 3: 3 cross edges.
        let bm = csr.edge_type_index.edges_of_type(3).expect("type 3 exists");
        assert_eq!(bm.len(), 3);

        // MphNodeIndex: all 5 node keys resolvable.
        assert_eq!(csr.mph.len(), 5);
    }

    #[test]
    fn test_mph_lookup_matches_hashmap() {
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        // Every key should resolve to the same row via MPH as via HashMap.
        for key in &node_keys {
            let mph_row = csr.mph.lookup(*key).expect("mph finds key");
            let map_row = csr.node_id_to_row.get(key).copied().expect("map finds key");
            assert_eq!(mph_row, map_row);
        }
    }

    #[test]
    fn test_neighbors_by_label() {
        // Build graph where node labels are: 0=label0, 1=label1, 2=label2, 3=label3, 4=label4
        // Node 0 has outgoing edges to nodes 1,2,3,4 (star pattern).
        // neighbors_by_label(row0, label=2) should return only node 2's row.
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        let row0 = csr.lookup_node(node_keys[0]).expect("row exists");

        // Filter neighbors of node 0 by label 2 (only node 2 has label 2).
        let filtered = csr.neighbors_by_label(row0, 2);
        assert_eq!(filtered.len(), 1);
        let row2 = csr.lookup_node(node_keys[2]).expect("row exists");
        assert_eq!(filtered[0], row2);

        // Filter by label 0 (only node 0 has it, but node 0 is not a neighbor of itself).
        let filtered = csr.neighbors_by_label(row0, 0);
        assert!(filtered.is_empty());

        // Filter by non-existent label.
        let filtered = csr.neighbors_by_label(row0, 31);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_edges_by_type() {
        // Node 0 has 4 outgoing edges, all of type 1.
        // Node 1 has outgoing edge to node 2 (type 2) and node 3->1 is incoming.
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        let row0 = csr.lookup_node(node_keys[0]).expect("row exists");

        // All node 0's edges are type 1.
        let type1 = csr.edges_by_type(row0, 1);
        assert_eq!(type1.len(), 4);

        // No type 2 edges from node 0.
        let type2 = csr.edges_by_type(row0, 2);
        assert!(type2.is_empty());

        // No type 3 edges from node 0.
        let type3 = csr.edges_by_type(row0, 3);
        assert!(type3.is_empty());
    }

    #[test]
    fn test_edges_by_type_with_deletion() {
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let mut csr = CsrSegment::from_frozen(frozen, 100).expect("csr ok");

        let row0 = csr.lookup_node(node_keys[0]).expect("row exists");

        // Delete the first type-1 edge.
        let first_edge_idx = csr.row_offsets[row0 as usize];
        csr.mark_deleted(first_edge_idx);

        let type1 = csr.edges_by_type(row0, 1);
        assert_eq!(type1.len(), 3); // one less
    }

    // --- Phase 121: File I/O and persistence tests ---

    #[test]
    fn test_to_bytes_from_bytes_roundtrip() {
        let frozen = build_small_graph();
        let original = CsrSegment::from_frozen(frozen, 42).expect("csr ok");
        let bytes = original.to_bytes();
        let restored = CsrSegment::from_bytes(&bytes).expect("from_bytes ok");

        assert_eq!(restored.header.magic, *b"MNGR");
        assert_eq!(restored.header.version, 1);
        assert_eq!(restored.node_count(), original.node_count());
        assert_eq!(restored.edge_count(), original.edge_count());
        assert_eq!(restored.created_lsn, 42);
        assert_eq!(restored.row_offsets, original.row_offsets);
        assert_eq!(restored.col_indices, original.col_indices);

        // Verify neighbor queries still work.
        for row in 0..restored.node_count() {
            assert_eq!(restored.neighbors_out(row), original.neighbors_out(row));
        }
    }

    #[test]
    fn test_write_to_file_from_file_roundtrip() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let frozen = build_small_graph();
        let original = CsrSegment::from_frozen(frozen, 55).expect("csr ok");

        let path = dir.path().join("test_segment.csr");
        original.write_to_file(&path).expect("write ok");
        let restored = CsrSegment::from_file(&path).expect("from_file ok");

        assert_eq!(restored.node_count(), original.node_count());
        assert_eq!(restored.edge_count(), original.edge_count());
        assert_eq!(restored.created_lsn, 55);
    }

    #[test]
    fn test_from_bytes_checksum_mismatch() {
        let frozen = build_small_graph();
        let csr = CsrSegment::from_frozen(frozen, 42).expect("csr ok");
        let mut bytes = csr.to_bytes();

        // Corrupt the checksum at offset 72 (checksum field).
        bytes[72] ^= 0xFF;

        let result = CsrSegment::from_bytes(&bytes);
        match result {
            Err(CsrError::ChecksumMismatch { .. }) => {} // expected
            other => panic!("expected ChecksumMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_from_bytes_truncated_data() {
        let result = CsrSegment::from_bytes(&[0u8; 10]);
        assert!(matches!(result, Err(CsrError::InvalidData(_))));
    }

    #[test]
    fn test_from_bytes_bad_magic() {
        let frozen = build_small_graph();
        let csr = CsrSegment::from_frozen(frozen, 42).expect("csr ok");
        let mut bytes = csr.to_bytes();
        bytes[0] = b'X'; // corrupt magic
        let result = CsrSegment::from_bytes(&bytes);
        assert!(matches!(result, Err(CsrError::InvalidData(_))));
    }

    #[test]
    fn test_from_file_nonexistent() {
        let result = CsrSegment::from_file(Path::new("/nonexistent/segment.csr"));
        assert!(matches!(result, Err(CsrError::IoError(_))));
    }

    // --- Phase 124: MmapCsrSegment tests ---

    #[test]
    fn test_mmap_roundtrip() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let original = CsrSegment::from_frozen(frozen, 77).expect("csr ok");

        // Write to file, then load via mmap.
        let path = dir.path().join("test_mmap.csr");
        original.write_to_file(&path).expect("write ok");
        let mmap_seg =
            MmapCsrSegment::from_mmap_file(&path).expect("mmap ok");

        // Verify header.
        assert_eq!(mmap_seg.node_count(), original.node_count());
        assert_eq!(mmap_seg.edge_count(), original.edge_count());
        assert_eq!(mmap_seg.created_lsn, 77);

        // Verify all neighbors match.
        for row in 0..original.node_count() {
            assert_eq!(
                mmap_seg.neighbors_out(row),
                original.neighbors_out(row),
                "neighbors mismatch at row {row}"
            );
        }

        // Verify lookup_node works.
        for key in &node_keys {
            let heap_row = original.lookup_node(*key);
            let mmap_row = mmap_seg.lookup_node(*key);
            assert_eq!(heap_row, mmap_row, "lookup mismatch for key {key:?}");
        }
    }

    #[test]
    fn test_mmap_madvise_no_panic() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let frozen = build_small_graph();
        let original = CsrSegment::from_frozen(frozen, 88).expect("csr ok");
        let path = dir.path().join("test_madvise.csr");
        original.write_to_file(&path).expect("write ok");
        let mmap_seg =
            MmapCsrSegment::from_mmap_file(&path).expect("mmap ok");

        // Should not panic on any platform.
        mmap_seg.madvise_sequential();
        mmap_seg.madvise_random();
    }

    #[test]
    fn test_mmap_lookup_node() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let original = CsrSegment::from_frozen(frozen, 99).expect("csr ok");
        let path = dir.path().join("test_lookup.csr");
        original.write_to_file(&path).expect("write ok");

        let mmap_seg =
            MmapCsrSegment::from_mmap_file(&path).expect("mmap ok");

        // All known keys should resolve.
        for key in &node_keys {
            assert!(
                mmap_seg.lookup_node(*key).is_some(),
                "key {key:?} not found"
            );
        }

        // Unknown key should return None.
        let bad_key: NodeKey = slotmap::KeyData::from_ffi(0xDEADBEEF).into();
        assert!(mmap_seg.lookup_node(bad_key).is_none());
    }

    #[test]
    fn test_mmap_neighbor_edges() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let frozen = build_small_graph();
        let node_keys: Vec<_> = frozen.nodes.iter().map(|(k, _)| *k).collect();
        let original = CsrSegment::from_frozen(frozen, 101).expect("csr ok");
        let path = dir.path().join("test_neigh_edges.csr");
        original.write_to_file(&path).expect("write ok");

        let mmap_seg =
            MmapCsrSegment::from_mmap_file(&path).expect("mmap ok");

        // Node 0 has 4 outgoing edges.
        let row0 = mmap_seg.lookup_node(node_keys[0]).expect("row exists");
        let edges: Vec<_> = mmap_seg.neighbor_edges(row0).collect();
        assert_eq!(edges.len(), 4);
    }

    #[test]
    fn test_csr_storage_from_file() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let frozen = build_small_graph();
        let original = CsrSegment::from_frozen(frozen, 55).expect("csr ok");
        let path = dir.path().join("test_storage.csr");
        original.write_to_file(&path).expect("write ok");

        // CsrStorage::from_file should prefer mmap.
        let storage = CsrStorage::from_file(&path).expect("load ok");
        assert!(matches!(storage, CsrStorage::Mmap(_)));
        assert_eq!(storage.node_count(), original.node_count());
        assert_eq!(storage.edge_count(), original.edge_count());
        assert_eq!(storage.created_lsn(), 55);
    }

    #[test]
    fn test_csr_storage_from_csrsegment() {
        let frozen = build_small_graph();
        let csr = CsrSegment::from_frozen(frozen, 42).expect("csr ok");
        let storage: CsrStorage = csr.into();
        assert!(matches!(storage, CsrStorage::Heap(_)));
        assert_eq!(storage.node_count(), 5);
        assert_eq!(storage.edge_count(), 10);
    }
}
