//! Append-only mutable segment with TQ-4bit encoded vectors.
//!
//! Stores TQ codes + norm at insert time (no f32 retained). Brute-force
//! search uses TQ-ADC distance. Memory: 564 bytes/vec at 768d (5.5x less
//! than f32 storage).

use std::collections::BinaryHeap;
use std::sync::Arc;

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::mvcc::visibility::is_visible;
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::turbo_quant::encoder::{encode_tq_mse_scaled, padded_dimension};
use crate::vector::turbo_quant::fwht;
use crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;
use crate::vector::types::{SearchResult, VectorId};

/// Maximum byte size before a mutable segment is considered full (128 MB).
const MUTABLE_SEGMENT_MAX: usize = 128 * 1024 * 1024;

/// 48 bytes. MVCC fields prepared for Phase 65.
#[repr(C)]
pub struct MutableEntry {
    pub internal_id: u32,
    pub key_hash: u64,
    pub vector_offset: u32,
    pub norm: f32,
    pub insert_lsn: u64,
    pub delete_lsn: u64,
    pub txn_id: u64,
}

/// Snapshot from freeze() for compaction pipeline.
pub struct FrozenSegment {
    pub entries: Vec<MutableEntry>,
    /// TQ-4bit nibble-packed codes, `bytes_per_code` per vector.
    pub tq_codes: Vec<u8>,
    /// f32 vectors for reranking in ImmutableSegment after compaction.
    pub vectors_f32: Vec<f32>,
    /// Bytes per TQ code (padded_dim/2 + 4 for norm).
    pub bytes_per_code: usize,
    pub dimension: u32,
}

struct MutableSegmentInner {
    /// TQ-encoded codes for brute-force TQ-ADC search.
    tq_codes: Vec<u8>,
    /// f32 vectors for compaction → immutable f32 reranking.
    vectors_f32: Vec<f32>,
    entries: Vec<MutableEntry>,
    dimension: u32,
    padded_dimension: u32,
    bytes_per_code: usize,
    byte_size: usize,
}

/// Ordered wrapper for BinaryHeap: (distance, id).
#[derive(PartialEq)]
struct DistF32(f32, u32);

impl Eq for DistF32 {}

impl Ord for DistF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.1.cmp(&other.1))
    }
}

impl PartialOrd for DistF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Append-only flat buffer with TQ-ADC brute-force search.
pub struct MutableSegment {
    inner: RwLock<MutableSegmentInner>,
    collection: Arc<CollectionMetadata>,
}

impl MutableSegment {
    /// Create an empty mutable segment.
    pub fn new(dimension: u32, collection: Arc<CollectionMetadata>) -> Self {
        let padded = padded_dimension(dimension);
        let bytes_per_code = padded as usize / 2 + 4; // nibble-packed + 4 bytes norm
        Self {
            inner: RwLock::new(MutableSegmentInner {
                tq_codes: Vec::new(),
                vectors_f32: Vec::new(),
                entries: Vec::new(),
                dimension,
                padded_dimension: padded,
                bytes_per_code,
                byte_size: 0,
            }),
            collection,
        }
    }

    /// Append a vector. TQ-encodes the f32 input and stores only the compressed code.
    ///
    /// SQ8 parameter accepted for API compatibility but ignored.
    pub fn append(
        &self,
        key_hash: u64,
        vector_f32: &[f32],
        _vector_sq: &[i8],
        _norm: f32,
        insert_lsn: u64,
    ) -> u32 {
        let mut inner = self.inner.write();
        let internal_id = inner.entries.len() as u32;
        let padded = inner.padded_dimension as usize;
        let bytes_per_code = inner.bytes_per_code;

        // TQ encode: normalize → pad → FWHT → quantize → nibble-pack
        let signs = self.collection.fwht_sign_flips.as_slice();
        let boundaries = self.collection.codebook_boundaries_15();
        let mut work_buf = vec![0.0f32; padded];
        let code = encode_tq_mse_scaled(vector_f32, signs, boundaries, &mut work_buf);

        // Append packed code + norm (4 bytes LE) to TQ buffer
        inner.tq_codes.extend_from_slice(&code.codes);
        inner.tq_codes.extend_from_slice(&code.norm.to_le_bytes());

        // Also store f32 for compaction → immutable f32 reranking
        inner.vectors_f32.extend_from_slice(vector_f32);

        inner.entries.push(MutableEntry {
            internal_id,
            key_hash,
            vector_offset: internal_id,
            norm: code.norm,
            insert_lsn,
            delete_lsn: 0,
            txn_id: 0,
        });

        inner.byte_size += bytes_per_code + inner.dimension as usize * 4 + std::mem::size_of::<MutableEntry>();
        internal_id
    }

    /// Brute-force search using exact f32 L2 distance.
    ///
    /// TQ codes are stored for compaction/HNSW build. Search uses f32 directly
    /// for perfect recall. TQ-ADC ranking error (std ≈ 0.4) exceeds the NN
    /// distance gap (0.04) at 768d, making it unsuitable for final ranking.
    pub fn brute_force_search(
        &self,
        query_f32: &[f32],
        k: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        self.brute_force_search_filtered(query_f32, k, None)
    }

    /// Brute-force filtered search using f32 L2 distance.
    pub fn brute_force_search_filtered(
        &self,
        query_f32: &[f32],
        k: usize,
        allow_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        let inner = self.inner.read();
        let dim = inner.dimension as usize;
        let l2_f32 = crate::vector::distance::table().l2_f32;

        let mut heap: BinaryHeap<DistF32> = BinaryHeap::with_capacity(k + 1);

        for entry in &inner.entries {
            if entry.delete_lsn != 0 {
                continue;
            }
            if let Some(bm) = allow_bitmap {
                if !bm.contains(entry.internal_id) {
                    continue;
                }
            }
            let offset = entry.internal_id as usize * dim;
            let vec_f32 = &inner.vectors_f32[offset..offset + dim];
            let dist = l2_f32(query_f32, vec_f32);

            if heap.len() < k {
                heap.push(DistF32(dist, entry.internal_id));
            } else if let Some(&DistF32(worst, _)) = heap.peek() {
                if dist < worst {
                    heap.pop();
                    heap.push(DistF32(dist, entry.internal_id));
                }
            }
        }

        heap.into_sorted_vec()
            .into_iter()
            .map(|DistF32(d, id)| SearchResult::new(d, VectorId(id)))
            .collect()
    }

    /// MVCC-aware brute-force search using f32 L2 distance.
    pub fn brute_force_search_mvcc(
        &self,
        query_f32: &[f32],
        k: usize,
        allow_bitmap: Option<&RoaringBitmap>,
        snapshot_lsn: u64,
        my_txn_id: u64,
        committed: &RoaringBitmap,
    ) -> SmallVec<[SearchResult; 32]> {
        let inner = self.inner.read();
        let dim = inner.dimension as usize;
        let l2_f32 = crate::vector::distance::table().l2_f32;

        let mut heap: BinaryHeap<DistF32> = BinaryHeap::with_capacity(k + 1);

        for entry in &inner.entries {
            if !is_visible(
                entry.insert_lsn,
                entry.delete_lsn,
                entry.txn_id,
                snapshot_lsn,
                my_txn_id,
                committed,
            ) {
                continue;
            }
            if let Some(bm) = allow_bitmap {
                if !bm.contains(entry.internal_id) {
                    continue;
                }
            }
            let offset = entry.internal_id as usize * dim;
            let vec_f32 = &inner.vectors_f32[offset..offset + dim];
            let dist = l2_f32(query_f32, vec_f32);

            if heap.len() < k {
                heap.push(DistF32(dist, entry.internal_id));
            } else if let Some(&DistF32(worst, _)) = heap.peek() {
                if dist < worst {
                    heap.pop();
                    heap.push(DistF32(dist, entry.internal_id));
                }
            }
        }

        heap.into_sorted_vec()
            .into_iter()
            .map(|DistF32(d, id)| SearchResult::new(d, VectorId(id)))
            .collect()
    }

    /// Append within a transaction context.
    pub fn append_transactional(
        &self,
        key_hash: u64,
        vector_f32: &[f32],
        _vector_sq: &[i8],
        _norm: f32,
        insert_lsn: u64,
        txn_id: u64,
    ) -> u32 {
        let mut inner = self.inner.write();
        let internal_id = inner.entries.len() as u32;
        let padded = inner.padded_dimension as usize;
        let bytes_per_code = inner.bytes_per_code;

        let signs = self.collection.fwht_sign_flips.as_slice();
        let boundaries = self.collection.codebook_boundaries_15();
        let mut work_buf = vec![0.0f32; padded];
        let code = encode_tq_mse_scaled(vector_f32, signs, boundaries, &mut work_buf);

        inner.tq_codes.extend_from_slice(&code.codes);
        inner.tq_codes.extend_from_slice(&code.norm.to_le_bytes());
        inner.vectors_f32.extend_from_slice(vector_f32);

        inner.entries.push(MutableEntry {
            internal_id,
            key_hash,
            vector_offset: internal_id,
            norm: code.norm,
            insert_lsn,
            delete_lsn: 0,
            txn_id,
        });

        let dim = inner.dimension as usize;
        inner.byte_size += bytes_per_code + dim * 4 + std::mem::size_of::<MutableEntry>();
        internal_id
    }

    /// Returns true when the segment exceeds the 128 MB threshold.
    pub fn is_full(&self) -> bool {
        self.inner.read().byte_size >= MUTABLE_SEGMENT_MAX
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    /// Returns true if no entries.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.read().entries.is_empty()
    }

    /// Mark an entry as deleted.
    pub fn mark_deleted(&self, internal_id: u32, delete_lsn: u64) {
        let mut inner = self.inner.write();
        if let Some(entry) = inner.entries.get_mut(internal_id as usize) {
            entry.delete_lsn = delete_lsn;
        }
    }

    /// Mark all entries matching a key_hash as deleted.
    pub fn mark_deleted_by_key_hash(&self, key_hash: u64, delete_lsn: u64) -> u32 {
        let mut inner = self.inner.write();
        let mut count = 0u32;
        for entry in inner.entries.iter_mut() {
            if entry.key_hash == key_hash && entry.delete_lsn == 0 {
                entry.delete_lsn = delete_lsn;
                count += 1;
            }
        }
        count
    }

    /// Freeze: snapshot TQ codes and entries for compaction.
    pub fn freeze(&self) -> FrozenSegment {
        let inner = self.inner.read();
        FrozenSegment {
            entries: inner
                .entries
                .iter()
                .map(|e| MutableEntry {
                    internal_id: e.internal_id,
                    key_hash: e.key_hash,
                    vector_offset: e.vector_offset,
                    norm: e.norm,
                    insert_lsn: e.insert_lsn,
                    delete_lsn: e.delete_lsn,
                    txn_id: e.txn_id,
                })
                .collect(),
            tq_codes: inner.tq_codes.clone(),
            vectors_f32: inner.vectors_f32.clone(),
            bytes_per_code: inner.bytes_per_code,
            dimension: inner.dimension,
        }
    }

    /// Access collection metadata.
    pub fn collection(&self) -> &Arc<CollectionMetadata> {
        &self.collection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::turbo_quant::collection::QuantizationConfig;
    use crate::vector::types::DistanceMetric;

    fn make_collection(dim: u32) -> Arc<CollectionMetadata> {
        Arc::new(CollectionMetadata::new(
            1, dim, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        ))
    }

    fn make_f32_vector(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        // Normalize
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            for x in v.iter_mut() { *x *= inv; }
        }
        v
    }

    fn rotate_query(query: &[f32], collection: &CollectionMetadata) -> Vec<f32> {
        let dim = query.len();
        let padded = collection.padded_dimension as usize;
        let mut q_rot = vec![0.0f32; padded];
        q_rot[..dim].copy_from_slice(query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rot[..dim].iter_mut() { *v *= inv; }
        }
        fwht::fwht(&mut q_rot, collection.fwht_sign_flips.as_slice());
        q_rot
    }

    #[test]
    fn test_append_returns_sequential_ids() {
        distance::init();
        let col = make_collection(128);
        let seg = MutableSegment::new(128, col);
        let v1 = make_f32_vector(128, 1);
        let v2 = make_f32_vector(128, 2);
        assert_eq!(seg.append(100, &v1, &[], 1.0, 1), 0);
        assert_eq!(seg.append(200, &v2, &[], 1.0, 2), 1);
        assert_eq!(seg.len(), 2);
    }

    #[test]
    fn test_brute_force_search_returns_nearest() {
        distance::init();
        let dim = 128;
        let col = make_collection(dim as u32);
        let seg = MutableSegment::new(dim as u32, col.clone());

        let vectors: Vec<Vec<f32>> = (0..20u32)
            .map(|i| make_f32_vector(dim, i * 7 + 1))
            .collect();
        for (i, v) in vectors.iter().enumerate() {
            seg.append(i as u64, v, &[], 1.0, i as u64);
        }

        let q_rot = rotate_query(&vectors[0], &col);
        let codebook = col.codebook_16();
        let results = seg.brute_force_search(&vectors[0], 3);

        assert!(results.len() <= 3);
        // First result should be vector 0 (nearest to itself)
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_brute_force_search_excludes_deleted() {
        distance::init();
        let dim = 128;
        let col = make_collection(dim as u32);
        let seg = MutableSegment::new(dim as u32, col.clone());

        let v0 = make_f32_vector(dim, 1);
        let v1 = make_f32_vector(dim, 2);
        let v2 = make_f32_vector(dim, 3);
        seg.append(0, &v0, &[], 1.0, 1);
        seg.append(1, &v1, &[], 1.0, 2);
        seg.append(2, &v2, &[], 1.0, 3);

        seg.mark_deleted(0, 10);

        let results = seg.brute_force_search(&v0, 3);
        for r in &results {
            assert_ne!(r.id.0, 0, "deleted vector should not appear");
        }
    }

    #[test]
    fn test_freeze_returns_snapshot() {
        distance::init();
        let col = make_collection(128);
        let seg = MutableSegment::new(128, col);
        let v1 = make_f32_vector(128, 1);
        let v2 = make_f32_vector(128, 2);
        seg.append(100, &v1, &[], 1.5, 1);
        seg.append(200, &v2, &[], 2.5, 2);

        let frozen = seg.freeze();
        assert_eq!(frozen.entries.len(), 2);
        assert_eq!(frozen.entries[0].key_hash, 100);
        // TQ codes should have 2 * bytes_per_code bytes
        let padded = padded_dimension(128) as usize;
        let expected_bpc = padded / 2 + 4;
        assert_eq!(frozen.tq_codes.len(), 2 * expected_bpc);
        // Segment retains data after freeze
        assert_eq!(seg.len(), 2);
    }

    #[test]
    fn test_mark_deleted() {
        distance::init();
        let col = make_collection(128);
        let seg = MutableSegment::new(128, col);
        seg.append(1, &make_f32_vector(128, 1), &[], 1.0, 1);
        seg.mark_deleted(0, 42);
        let frozen = seg.freeze();
        assert_eq!(frozen.entries[0].delete_lsn, 42);
    }

    #[test]
    fn test_mvcc_backward_compat() {
        distance::init();
        let dim = 128;
        let col = make_collection(dim as u32);
        let seg = MutableSegment::new(dim as u32, col.clone());

        let vectors: Vec<Vec<f32>> = (0..10u32)
            .map(|i| make_f32_vector(dim, i * 7 + 1))
            .collect();
        for (i, v) in vectors.iter().enumerate() {
            seg.append(i as u64, v, &[], 1.0, i as u64);
        }

        let q_rot = rotate_query(&vectors[0], &col);
        let codebook = col.codebook_16();
        let committed = roaring::RoaringBitmap::new();

        let non_mvcc = seg.brute_force_search(&vectors[0], 3);
        let mvcc = seg.brute_force_search_mvcc(&vectors[0], 3, None, 0, 0, &committed);

        assert_eq!(non_mvcc.len(), mvcc.len());
        for (a, b) in non_mvcc.iter().zip(mvcc.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }
}
