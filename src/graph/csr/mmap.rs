//! Memory-mapped CSR segment — zero-copy access via `mmap`.

use std::collections::HashMap;
use std::path::Path;

use memmap2::Mmap;
use roaring::RoaringBitmap;

use super::{CsrError, compute_csr_checksum, read4, read8};
use crate::graph::index::{EdgeTypeIndex, LabelIndex, MphNodeIndex};
use crate::graph::types::{EdgeMeta, GraphSegmentHeader, NodeKey, NodeMeta};

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
        let file = std::fs::File::open(path).map_err(|e| CsrError::IoError(e.to_string()))?;

        // SAFETY: The file is opened read-only. The Mmap handle keeps the
        // mapping alive for the lifetime of this struct. We validate all
        // offsets and lengths before constructing slices.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| CsrError::IoError(e.to_string()))?;

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

        let nc = node_count as usize;
        let ec = edge_count as usize;
        let em_elem_size = core::mem::size_of::<EdgeMeta>(); // 8
        let nm_elem_size = core::mem::size_of::<NodeMeta>(); // 32

        // Use checked arithmetic to prevent integer overflow from attacker-controlled
        // node_count/edge_count values bypassing the size validation.
        let expected_len = (|| -> Option<usize> {
            let ro_size = nc.checked_add(1)?.checked_mul(4)?;
            let ci_size = ec.checked_mul(4)?;
            let em_size = ec.checked_mul(em_elem_size)?;
            let nm_size = nc.checked_mul(nm_elem_size)?;
            header_size
                .checked_add(ro_size)?
                .checked_add(ci_size)?
                .checked_add(em_size)?
                .checked_add(nm_size)
        })()
        .ok_or_else(|| CsrError::InvalidData("size overflow in CSR header fields".to_owned()))?;
        if mmap.len() < expected_len {
            return Err(CsrError::InvalidData(format!(
                "mmap'd data too short: {} < {expected_len}",
                mmap.len(),
            )));
        }

        // Validate CRC32 over the entire payload (not just header fields).
        let computed_checksum = compute_csr_checksum(data) as u64;
        if stored_checksum != computed_checksum {
            return Err(CsrError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        // Compute array pointers from the mmap base.
        let base = mmap.as_ptr();

        // SAFETY: header is 128 bytes (64-aligned). u32 requires 4-byte alignment.
        // 128 % 4 == 0, so row_offsets_ptr is correctly aligned. We verified that
        // the mmap region is large enough to hold all arrays. The types are
        // #[repr(C)] with known sizes validated by const assertions in types.rs.
        // The Mmap is immutable and lives as long as self (_mmap field).
        // SAFETY: base + header_size is in-bounds (validated above), result is u32-aligned.
        let ro_ptr = unsafe { base.add(header_size) } as *const u32;
        let ro_len = nc + 1;

        let ci_start = header_size + ro_len * 4;
        // SAFETY: base + ci_start is in-bounds (validated above), result is u32-aligned.
        let ci_ptr = unsafe { base.add(ci_start) } as *const u32;
        let ci_len = ec;

        let em_start = ci_start + ec * 4;
        // SAFETY: EdgeMeta is #[repr(C, align(8))], size 8. We verify alignment
        // at runtime and fall back to heap loading if it's violated.
        let em_ptr = unsafe { base.add(em_start) } as *const EdgeMeta;
        if !(em_ptr as usize).is_multiple_of(core::mem::align_of::<EdgeMeta>()) {
            return Err(CsrError::InvalidData(
                "edge_meta alignment violated in mmap".to_owned(),
            ));
        }
        let em_len = ec;

        let nm_start = em_start + ec * em_elem_size;
        // SAFETY: base + nm_start is in-bounds (validated above), alignment checked below.
        let nm_ptr = unsafe { base.add(nm_start) } as *const NodeMeta;
        if !(nm_ptr as usize).is_multiple_of(core::mem::align_of::<NodeMeta>()) {
            return Err(CsrError::InvalidData(
                "node_meta alignment violated in mmap".to_owned(),
            ));
        }
        let nm_len = nc;

        // Validate row_offsets: every entry must be <= edge_count and
        // the array must be monotonically non-decreasing. Without this,
        // corrupted CSR files cause panics via out-of-bounds slice indexing
        // in neighbors_out() and neighbor_edges().
        {
            // SAFETY: ro_ptr was computed from a validated mmap region of sufficient size.
            let ro_slice = unsafe { core::slice::from_raw_parts(ro_ptr, ro_len) };
            for i in 0..ro_len {
                if ro_slice[i] > edge_count {
                    return Err(CsrError::InvalidData(format!(
                        "row_offsets[{i}] = {} exceeds edge_count = {edge_count}",
                        ro_slice[i]
                    )));
                }
                if i > 0 && ro_slice[i] < ro_slice[i - 1] {
                    return Err(CsrError::InvalidData(format!(
                        "row_offsets[{i}] = {} < row_offsets[{}] = {} (not monotonic)",
                        ro_slice[i],
                        i - 1,
                        ro_slice[i - 1]
                    )));
                }
            }
        }

        // Rebuild indexes from mmap'd slices.
        // SAFETY: pointers were validated above, mmap is alive for this scope.
        let node_meta_slice = unsafe { core::slice::from_raw_parts(nm_ptr, nm_len) };
        // SAFETY: em_ptr validated above (alignment checked, mmap alive for scope).
        let edge_meta_slice = unsafe { core::slice::from_raw_parts(em_ptr, em_len) };

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
