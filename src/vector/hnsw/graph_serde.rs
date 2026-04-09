//! Serialization and deserialization for HnswGraph.
//!
//! Provides both raw (v2) and delta+VByte compressed (v1) formats.

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::neighbor_codec;

use super::graph::{HnswGraph, SENTINEL};

impl HnswGraph {
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
        let n = self.num_nodes() as usize;
        let layer0_len = self.layer0_neighbors_slice().len();
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
            + self.upper_offsets_slice().len() * 4
            + 4
            + self.upper_neighbors_slice().len() * 4;
        let mut buf = Vec::with_capacity(capacity);

        buf.extend_from_slice(&self.num_nodes().to_le_bytes());
        buf.push(self.m());
        buf.push(self.m0());
        buf.extend_from_slice(&self.entry_point().to_le_bytes());
        buf.push(self.max_level());
        buf.extend_from_slice(&self.bytes_per_code().to_le_bytes());

        // Layer 0
        buf.extend_from_slice(&(layer0_len as u32).to_le_bytes());
        for &v in self.layer0_neighbors_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // BFS order and inverse
        for &v in self.bfs_order_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        for &v in self.bfs_inverse_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // Levels
        buf.extend_from_slice(self.levels_slice());

        // CSR upper layers
        for &v in self.upper_index_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(self.upper_offsets_slice().len() as u32).to_le_bytes());
        for &v in self.upper_offsets_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(self.upper_neighbors_slice().len() as u32).to_le_bytes());
        for &v in self.upper_neighbors_slice() {
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

    /// Serialize the graph with delta + VByte compression on layer-0 neighbors.
    ///
    /// Compressed format v1 (all LE unless noted):
    ///   num_nodes: u32, m: u8, m0: u8, entry_point: u32, max_level: u8,
    ///   bytes_per_code: u32,
    ///   version_tag: u8 (0x01 = compressed),
    ///   For each of num_nodes layer-0 neighbor lists:
    ///     blob_len: u16 LE, blob: [u8; blob_len]  (delta+VByte encoded)
    ///   bfs_order: [u32; num_nodes], bfs_inverse: [u32; num_nodes],
    ///   levels: [u8; num_nodes],
    ///   upper_index: [u32; num_nodes],
    ///   upper_offsets_len: u32, upper_offsets: [u32; upper_offsets_len],
    ///   upper_neighbors_len: u32, upper_neighbors: [u32; upper_neighbors_len]
    ///
    /// Callers in the warm transition path should use this instead of `to_bytes()`
    /// to reduce on-disk footprint. The in-memory graph remains uncompressed.
    pub fn to_bytes_compressed(&self) -> Vec<u8> {
        let n = self.num_nodes() as usize;
        // Estimate: header ~16 bytes + compressed layer0 (much smaller than raw)
        // + BFS/levels/CSR same as uncompressed
        let mut buf = Vec::with_capacity(
            16 + n * 8 // rough estimate for compressed layer0
            + n * 4 * 2  // bfs_order + bfs_inverse
            + n          // levels
            + n * 4      // upper_index
            + 4 + self.upper_offsets_slice().len() * 4
            + 4 + self.upper_neighbors_slice().len() * 4,
        );

        // Header (same as to_bytes)
        buf.extend_from_slice(&self.num_nodes().to_le_bytes());
        buf.push(self.m());
        buf.push(self.m0());
        buf.extend_from_slice(&self.entry_point().to_le_bytes());
        buf.push(self.max_level());
        buf.extend_from_slice(&self.bytes_per_code().to_le_bytes());

        // Version tag: 0x01 = compressed format
        buf.push(0x01);

        // Layer 0: delta + VByte encoded per node
        for i in 0..n {
            let neighbors = self.neighbors_l0(i as u32);
            let encoded = neighbor_codec::encode_neighbors(neighbors);
            let blob_len = encoded.len() as u16;
            buf.extend_from_slice(&blob_len.to_le_bytes());
            buf.extend_from_slice(&encoded);
        }

        // BFS order and inverse
        for &v in self.bfs_order_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        for &v in self.bfs_inverse_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        // Levels
        buf.extend_from_slice(self.levels_slice());

        // CSR upper layers
        for &v in self.upper_index_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(self.upper_offsets_slice().len() as u32).to_le_bytes());
        for &v in self.upper_offsets_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(self.upper_neighbors_slice().len() as u32).to_le_bytes());
        for &v in self.upper_neighbors_slice() {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        buf
    }

    /// Deserialize from compressed format. Returns `Err` on truncation or format mismatch.
    pub fn from_bytes_compressed(data: &[u8]) -> Result<Self, &'static str> {
        let mut pos = 0;

        let ensure = |pos: usize, need: usize| -> Result<(), &'static str> {
            if pos + need > data.len() {
                Err("truncated compressed graph data")
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

        let read_u16 = |pos: &mut usize| -> Result<u16, &'static str> {
            ensure(*pos, 2)?;
            let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
            *pos += 2;
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

        // Version tag
        let version = read_u8(&mut pos)?;
        if version != 0x01 {
            return Err("unsupported compressed graph version");
        }

        let n = num_nodes as usize;
        let m0_usize = m0 as usize;

        // Layer 0: decode each node's compressed neighbors, pad with SENTINEL
        let total_slots = n * m0_usize;
        let mut layer0_vec = vec![SENTINEL; total_slots];
        for i in 0..n {
            let blob_len = read_u16(&mut pos)? as usize;
            ensure(pos, blob_len)?;
            let blob = &data[pos..pos + blob_len];
            pos += blob_len;
            let neighbors = neighbor_codec::decode_neighbors(blob);
            let dst_start = i * m0_usize;
            let copy_len = neighbors.len().min(m0_usize);
            layer0_vec[dst_start..dst_start + copy_len].copy_from_slice(&neighbors[..copy_len]);
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
}
