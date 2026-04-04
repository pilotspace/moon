//! Co-located Vamana page format for DiskANN cold tier.
//!
//! Each 4KB page holds one graph node: header + node_id + degree + vector +
//! neighbors + CRC32C. One SSD read = one graph hop + one exact distance
//! computation. Per design section 7.4 (Vamana mode).

use crate::persistence::page::{
    MoonPageHeader, PageType, MOONPAGE_HEADER_SIZE, PAGE_4K,
};
use crate::vector::diskann::vamana::VamanaGraph;
use std::io;
use std::path::Path;

/// Sentinel value for unused neighbor slots.
const NEIGHBOR_SENTINEL: u32 = u32::MAX;

/// Offset where node payload starts (after MoonPageHeader).
const NODE_PAYLOAD_OFFSET: usize = MOONPAGE_HEADER_SIZE; // 64

/// A parsed Vamana node read from a page.
pub struct VamanaNode {
    pub node_id: u32,
    pub vector: Vec<f32>,
    pub neighbors: Vec<u32>,
}

/// Compute the total payload size for a Vamana page.
///
/// Layout after header: node_id(4) + degree(2) + reserved(2) + vector(dim*4) + neighbors(max_degree*4) + crc(4)
#[inline]
fn payload_size(dim: usize, max_degree: u32) -> usize {
    4 + 2 + 2 + dim * 4 + max_degree as usize * 4 + 4
}

/// Assert that a Vamana node fits within a 4KB page.
#[inline]
fn assert_fits_4k(dim: usize, max_degree: u32) {
    let total = MOONPAGE_HEADER_SIZE + payload_size(dim, max_degree);
    assert!(
        total <= PAGE_4K,
        "Vamana node too large for 4KB page: {total} > {PAGE_4K} (dim={dim}, R={max_degree})"
    );
}

/// Write a single Vamana node into a 4KB page buffer.
///
/// The page layout is:
/// ```text
/// [MoonPageHeader, 64 bytes, type=VecGraph]
/// node_id: u32 (4 bytes)
/// degree: u16 (2 bytes)
/// reserved: u16 (2 bytes)
/// vector: [f32 x dim]
/// neighbors: [u32 x max_degree]  (unused slots = SENTINEL)
/// crc32c: u32 (4 bytes)
/// ```
pub fn write_vamana_page(
    buf: &mut [u8; PAGE_4K],
    page_id: u64,
    file_id: u64,
    node_id: u32,
    vector: &[f32],
    neighbors: &[u32],
    max_degree: u32,
) {
    let dim = vector.len();
    assert_fits_4k(dim, max_degree);
    assert!(
        neighbors.len() <= max_degree as usize,
        "neighbor count {} exceeds max_degree {}",
        neighbors.len(),
        max_degree,
    );

    // Zero the buffer
    buf.fill(0);

    let psize = payload_size(dim, max_degree);
    let mut hdr = MoonPageHeader::new(PageType::VecGraph, page_id, file_id);
    hdr.payload_bytes = psize as u32;
    hdr.entry_count = 1;
    hdr.write_to(buf);

    let mut off = NODE_PAYLOAD_OFFSET;

    // node_id
    buf[off..off + 4].copy_from_slice(&node_id.to_le_bytes());
    off += 4;

    // degree
    buf[off..off + 2].copy_from_slice(&(neighbors.len() as u16).to_le_bytes());
    off += 2;

    // reserved
    off += 2;

    // vector
    for &v in vector {
        buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
        off += 4;
    }

    // neighbors (pad with sentinel)
    for i in 0..max_degree as usize {
        let nbr = if i < neighbors.len() {
            neighbors[i]
        } else {
            NEIGHBOR_SENTINEL
        };
        buf[off..off + 4].copy_from_slice(&nbr.to_le_bytes());
        off += 4;
    }

    // CRC32C is embedded in the MoonPageHeader checksum field
    MoonPageHeader::compute_checksum(buf);
}

/// Read and validate a Vamana node from a 4KB page buffer.
///
/// Returns `None` if the header is invalid, page type is wrong, or CRC fails.
pub fn read_vamana_node(buf: &[u8; PAGE_4K], dim: usize) -> Option<VamanaNode> {
    let hdr = MoonPageHeader::read_from(buf)?;
    if hdr.page_type != PageType::VecGraph {
        return None;
    }

    // Verify CRC
    if !MoonPageHeader::verify_checksum(buf) {
        return None;
    }

    let mut off = NODE_PAYLOAD_OFFSET;

    // node_id
    let node_id = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
    off += 4;

    // degree
    let degree = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
    off += 2;

    // reserved
    off += 2;

    // vector
    let mut vector = Vec::with_capacity(dim);
    for _ in 0..dim {
        let v = f32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        vector.push(v);
        off += 4;
    }

    // neighbors (only read `degree` valid entries)
    let mut neighbors = Vec::with_capacity(degree);
    for i in 0..degree {
        let _ = i;
        let nbr = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        if nbr != NEIGHBOR_SENTINEL {
            neighbors.push(nbr);
        }
        off += 4;
    }

    Some(VamanaNode {
        node_id,
        vector,
        neighbors,
    })
}

/// Write an entire Vamana graph to a multi-page file (one 4KB page per node).
pub fn write_vamana_mpf(
    path: &Path,
    graph: &VamanaGraph,
    vectors: &[f32],
    dim: usize,
) -> io::Result<()> {
    use std::io::Write;

    assert_fits_4k(dim, graph.max_degree());

    let mut file = std::fs::File::create(path)?;
    let mut page = [0u8; PAGE_4K];

    for node_id in 0..graph.num_nodes() {
        let vec_slice = &vectors[node_id as usize * dim..(node_id as usize + 1) * dim];
        let neighbors = graph.neighbors(node_id);

        write_vamana_page(
            &mut page,
            node_id as u64, // page_id = node index
            0,              // file_id
            node_id,
            vec_slice,
            neighbors,
            graph.max_degree(),
        );

        file.write_all(&page)?;
    }

    file.sync_all()?;
    Ok(())
}

/// Read a single Vamana node from a multi-page file by node index.
pub fn read_vamana_node_at(
    path: &Path,
    node_index: u32,
    dim: usize,
) -> io::Result<Option<VamanaNode>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)?;
    let offset = node_index as u64 * PAGE_4K as u64;
    file.seek(SeekFrom::Start(offset))?;

    let mut page = [0u8; PAGE_4K];
    file.read_exact(&mut page)?;

    Ok(read_vamana_node(&page, dim))
}

/// Read a Vamana node from an already-open file descriptor via pread.
///
/// Same as `read_vamana_node_at` but uses an existing File handle,
/// avoiding open/close syscalls per graph hop. `FileExt::read_at`
/// (pread) is thread-safe and does not move the file cursor.
#[cfg(unix)]
pub fn read_vamana_node_with_fd(
    file: &std::fs::File,
    node_index: u32,
    dim: usize,
) -> io::Result<Option<VamanaNode>> {
    use std::os::unix::fs::FileExt;

    let offset = node_index as u64 * PAGE_4K as u64;
    let mut page = [0u8; PAGE_4K];
    file.read_at(&mut page, offset)?;

    Ok(read_vamana_node(&page, dim))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_roundtrip() {
        let dim = 128;
        let max_degree = 32;
        let node_id = 42;
        let vector: Vec<f32> = (0..dim).map(|i| i as f32 * 0.1).collect();
        let neighbors: Vec<u32> = vec![1, 5, 10, 20];

        let mut page = [0u8; PAGE_4K];
        write_vamana_page(&mut page, 0, 0, node_id, &vector, &neighbors, max_degree);

        let node = read_vamana_node(&page, dim).expect("should parse");
        assert_eq!(node.node_id, node_id);
        assert_eq!(node.vector, vector);
        assert_eq!(node.neighbors, neighbors);
    }

    #[test]
    fn test_crc_detects_corruption() {
        let dim = 64;
        let max_degree = 16;
        let vector: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        let neighbors: Vec<u32> = vec![0, 1, 2];

        let mut page = [0u8; PAGE_4K];
        write_vamana_page(&mut page, 0, 0, 0, &vector, &neighbors, max_degree);

        // Corrupt a byte in the vector region
        page[NODE_PAYLOAD_OFFSET + 20] ^= 0xFF;

        assert!(read_vamana_node(&page, dim).is_none(), "corrupted CRC should reject");
    }

    #[test]
    fn test_768d_r96_fits_4k() {
        // Per design: 64 + 8 + 3072 + 384 + 4 = 3532 <= 4096
        let total = MOONPAGE_HEADER_SIZE + payload_size(768, 96);
        assert_eq!(total, 3532);
        assert!(total <= PAGE_4K);

        // Also verify via assert_fits_4k (should not panic)
        assert_fits_4k(768, 96);
    }

    #[test]
    fn test_mpf_write_read_roundtrip() {
        let dim = 32;
        let n = 10;
        let r = 8;
        let vectors: Vec<f32> = (0..n * dim)
            .map(|i| (i as f32) * 0.01)
            .collect();

        let graph = crate::vector::diskann::vamana::VamanaGraph::build(&vectors, dim, r, r);

        let dir = std::env::temp_dir().join("moon_test_vamana_mpf");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.mpf");

        write_vamana_mpf(&path, &graph, &vectors, dim).expect("write should succeed");

        // Read back each node
        for node_id in 0..n as u32 {
            let node = read_vamana_node_at(&path, node_id, dim)
                .expect("read should succeed")
                .expect("node should parse");
            assert_eq!(node.node_id, node_id);
            let expected_vec = &vectors[node_id as usize * dim..(node_id as usize + 1) * dim];
            assert_eq!(node.vector, expected_vec);
            assert_eq!(node.neighbors, graph.neighbors(node_id));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_max_neighbors_filled() {
        let dim = 8;
        let max_degree = 4;
        let vector = vec![1.0_f32; dim];
        let neighbors = vec![0, 1, 2, 3]; // full degree

        let mut page = [0u8; PAGE_4K];
        write_vamana_page(&mut page, 0, 0, 99, &vector, &neighbors, max_degree);

        let node = read_vamana_node(&page, dim).expect("should parse");
        assert_eq!(node.neighbors, neighbors);
    }
}
