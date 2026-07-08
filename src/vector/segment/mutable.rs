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

use crate::vector::distance;
use crate::vector::distance::fastscan;
use crate::vector::mvcc::visibility::is_visible;
use crate::vector::segment::ivf::BLOCK_SIZE as FASTSCAN_BLOCK;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::{
    encode_tq_mse_a2, encode_tq_mse_scaled, encode_tq_mse_scaled_with_signs, padded_dimension,
};
use crate::vector::turbo_quant::fwht;
use crate::vector::turbo_quant::sq8::{
    SQ8_INT8_QMAX, SQ8_PARAMS_BYTES, encode_sq8_into, sq8_int8_dot_to_f32, sq8_l2_from_stats,
    sq8_params, sq8_quantize_query_scalar, sq8_query_stats,
};
use crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;
use crate::vector::types::{DistanceMetric, SearchResult, VectorId};

/// Maximum byte size before a mutable segment is considered full (128 MB).
const MUTABLE_SEGMENT_MAX: usize = 128 * 1024 * 1024;

/// SQ8 ADC query preparation, heap-free on the per-query hot path.
///
/// L2 borrows the caller's query directly (encode side stores raw values,
/// no normalization needed). Unit-sphere metrics (Cosine + InnerProduct)
/// copy into an inline buffer (stack for dim ≤ 512) and normalize to match
/// the encode-side normalization.
// The 2KB inline variant is deliberate: a stack buffer that keeps the
// per-query SQ8 hot path heap-free. The enum lives only as a short-lived
// stack local during search; boxing it would reintroduce the allocation.
#[allow(clippy::large_enum_variant)]
enum Sq8Query<'a> {
    Borrowed(&'a [f32]),
    Normalized(SmallVec<[f32; 512]>),
}

impl<'a> Sq8Query<'a> {
    #[inline]
    fn prepare(query_f32: &'a [f32], metric: DistanceMetric) -> Self {
        if metric == DistanceMetric::L2 {
            return Sq8Query::Borrowed(query_f32);
        }
        let mut q: SmallVec<[f32; 512]> = SmallVec::from_slice(query_f32);
        let n: f32 = q.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n > 0.0 {
            let inv = 1.0 / n;
            for v in q.iter_mut() {
                *v *= inv;
            }
        }
        Sq8Query::Normalized(q)
    }

    #[inline]
    fn as_slice(&self) -> &[f32] {
        match self {
            Sq8Query::Borrowed(s) => s,
            Sq8Query::Normalized(q) => q,
        }
    }
}

/// Per-query state for the chunked MVCC brute-force scan (QP-4): the prepared
/// query buffer and the top-k heap survive across yield chunks so neither is
/// redone per chunk. Build via `MutableSegment::prepare_brute_force_query`.
pub struct BruteForceQuery {
    /// SQ8: (possibly normalized) owned query copy; TQ-ADC: FWHT-rotated padded
    /// query; TQ-prod: empty (the caller's `TqProdQueryState` carries the prep).
    prepared: Vec<f32>,
    /// Whether the TQ-ADC distance path applies (resolved once at prepare).
    use_tq_adc: bool,
    /// SQ8 (HQ-2): per-query ADC constants `(Σq_i, Σq_i²)` over the PREPARED
    /// (possibly normalized) query — computed once here, combined per
    /// candidate via `sq8_l2_from_stats`. Zero for non-SQ8 collections.
    sq8_q_sum: f32,
    sq8_q_sumsq: f32,
    /// Task #13: int8-quantized copy of `prepared`, populated only when a
    /// SIMD int8 ADC kernel is installed for this CPU/build (empty
    /// otherwise — `brute_force_scan_mvcc_chunk` falls back to the f32
    /// `sq8_stats` path when `sq8_i8_stats` dispatch is `None`).
    sq8_qi8: Vec<i8>,
    sq8_q_scale: f32,
    sq8_sum_qi8: i32,
    /// FastScan pre-filter (TQ-ADC path only): quantized u8 LUT built once at
    /// prepare (`padded_dim * 16` entries). Empty when the segment is too
    /// small or the path doesn't apply — the chunk scan then runs the plain
    /// per-candidate loop.
    fs_lut: Vec<u8>,
    /// 1/scale from `build_quantized_lut` (f32 distance reconstruction).
    fs_inv_scale: f32,
    /// Bias from `build_quantized_lut` (sum of per-coordinate minima).
    fs_bias: f32,
    /// Sound quantization error bound (`padded_dim / scale`, pre-norm²):
    /// `|reconstructed - exact| <= fs_eps`, so `reconstructed - fs_eps` is a
    /// true lower bound and the filter can never drop a real top-k candidate.
    fs_eps: f32,
    /// Shared top-k accumulator across all chunks.
    heap: BinaryHeap<DistF32>,
}

impl BruteForceQuery {
    /// Drain the accumulated global top-k, ascending by distance.
    pub fn into_results(self) -> SmallVec<[SearchResult; 32]> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|DistF32(d, id, kh)| SearchResult::with_key_hash(d, VectorId(id), kh))
            .collect()
    }
}

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
    /// QJL sign bits per vector (ceil(dim/8) bytes each), contiguous.
    pub qjl_signs: Vec<u8>,
    /// Residual norms (one f32 per vector).
    pub residual_norms: Vec<f32>,
    /// Raw f32 vectors for exact pairwise distance during HNSW build.
    /// Layout: dim floats per vector, contiguous. Dropped after compaction.
    pub raw_f32: Vec<f32>,
    /// f16 originals for the exact-rerank sidecar (HQ-1). Present in BOTH
    /// build modes; dim halves per vector, mutable-internal-id order.
    pub raw_f16: Vec<u16>,
    /// Sub-centroid sign bits per vector (ceil(padded_dim/8) bytes each).
    /// Computed at insert time from pre-quantization FWHT values.
    pub sub_centroid_signs: Vec<u8>,
    /// Bytes per sub-centroid sign vector.
    pub sub_sign_bytes_per_vec: usize,
    /// Bytes per TQ code (padded_dim/2 + 4 for norm).
    pub bytes_per_code: usize,
    /// Bytes per QJL sign vector (ceil(dim/8)).
    pub qjl_bytes_per_vec: usize,
    /// Base offset for computing global vector IDs: global_id = base + internal_id.
    pub global_id_base: u32,
    pub dimension: u32,
}

struct MutableSegmentInner {
    /// TQ-encoded codes for HNSW TQ-ADC traversal.
    tq_codes: Vec<u8>,
    /// FastScan shadow of the nibble-packed TQ codes in FAISS-interleaved
    /// 32-vector blocks (`block[d * 32 + lane]`, zero-padded lanes). Written
    /// at append time for scalar-codebook TQ4 collections (empty for SQ8/A2);
    /// lets the MVCC brute-force scan batch 32 candidates per SIMD LUT pass.
    /// Costs padded_dim/2 bytes per vector on top of `tq_codes`.
    fs_blocks: Vec<u8>,
    /// QJL sign bits per vector — for TurboQuant_prod unbiased IP scoring.
    /// Zero-filled at insert time; recomputed from raw_f32 during freeze().
    qjl_signs: Vec<u8>,
    /// Residual norms per vector — ||x - decode(TQ(x))||.
    /// Zero at insert time; recomputed during freeze().
    residual_norms: Vec<f32>,
    /// Raw f32 vectors retained for deferred QJL encoding at freeze time.
    /// Layout: dim floats per vector, contiguous.
    raw_f32: Vec<f32>,
    /// f16 copies of the original vectors (HQ-1 exact-rerank sidecar source).
    /// Layout: dim halves per vector, contiguous — retained in BOTH build
    /// modes (unlike raw_f32, Exact-only) at 2·dim B/vector so compaction can
    /// hand the immutable segment its rerank sidecar by permutation alone.
    raw_f16: Vec<u16>,
    /// Sub-centroid sign bits computed at insert time.
    sub_centroid_signs: Vec<u8>,
    sub_sign_bytes_per_vec: usize,
    entries: Vec<MutableEntry>,
    /// Base offset for global vector IDs. When a mutable segment is replaced
    /// after compaction, the new segment starts at base_id = previous max global ID.
    /// global_id(entry) = base_id + entry.internal_id
    global_id_base: u32,
    dimension: u32,
    padded_dimension: u32,
    bytes_per_code: usize,
    qjl_bytes_per_vec: usize,
    byte_size: usize,
}

/// Ordered wrapper for BinaryHeap: (distance, id, key_hash).
/// key_hash is carried so FT.SEARCH can return the original Redis key.
#[derive(PartialEq)]
struct DistF32(f32, u32, u64);

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

/// Encode one vector into a self-contained SQ8 slot: `dim` u8 affine codes
/// followed by an 8-byte `(min, scale)` f32 trailer (`dim + SQ8_PARAMS_BYTES`).
///
/// Cosine and InnerProduct are unit-sphere metrics in this engine (the TQ path
/// normalizes the query unconditionally — there is no true dot-product ranking
/// anywhere), so both normalize the vector before quantizing; only L2 encodes the
/// raw vector so its squared-L2 ADC ranks by true Euclidean distance.
///
/// Returns `(slot_bytes, raw_norm)` — `raw_norm` is the pre-normalization L2 norm,
/// stored on the entry. Single source of truth shared by `append()` and
/// `append_transactional()`: encoding them separately is exactly how the two paths
/// drifted into a stride mismatch.
fn encode_sq8_slot(vector_f32: &[f32], metric: DistanceMetric, dim: usize) -> (Vec<u8>, f32) {
    let raw_norm: f32 = vector_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mut codes = vec![0u8; dim + SQ8_PARAMS_BYTES];
    let (min, scale) = if metric != DistanceMetric::L2 && raw_norm > 0.0 {
        let inv = 1.0 / raw_norm;
        let mut unit = vec![0.0f32; dim];
        for (u, &x) in unit.iter_mut().zip(vector_f32) {
            *u = x * inv;
        }
        encode_sq8_into(&unit, &mut codes[..dim])
    } else {
        encode_sq8_into(vector_f32, &mut codes[..dim])
    };
    codes[dim..dim + 4].copy_from_slice(&min.to_le_bytes());
    codes[dim + 4..dim + 8].copy_from_slice(&scale.to_le_bytes());
    (codes, raw_norm)
}

/// Minimum entry count before the FastScan pre-filter engages. Below this,
/// the ~2-8µs per-query LUT build costs more than the plain scan saves.
const FASTSCAN_MIN_ENTRIES: usize = 64;

/// Mirror one vector's nibble-packed TQ code into the FastScan shadow blocks.
///
/// `internal_id` must be the next sequential id (append-only invariant): a new
/// zero-filled block is grown whenever a block boundary is crossed, so lanes
/// past the current count stay zero (FastScan scans full blocks; out-of-range
/// lanes are skipped by position, never by content).
fn push_fastscan_shadow(fs_blocks: &mut Vec<u8>, internal_id: usize, code: &[u8]) {
    let dim_half = code.len();
    let block_bytes = dim_half * FASTSCAN_BLOCK;
    if internal_id % FASTSCAN_BLOCK == 0 {
        fs_blocks.resize(fs_blocks.len() + block_bytes, 0);
    }
    let block_base = (internal_id / FASTSCAN_BLOCK) * block_bytes;
    let lane = internal_id % FASTSCAN_BLOCK;
    for (d, &b) in code.iter().enumerate() {
        fs_blocks[block_base + d * FASTSCAN_BLOCK + lane] = b;
    }
}

impl MutableSegmentInner {
    /// Single shadow-maintenance gate shared by every TQ-code writer
    /// (`append` + `append_transactional`): mirrors the nibble code into
    /// `fs_blocks` for scalar-codebook TQ4 and accounts the extra bytes.
    /// Keeping this in ONE place is what guarantees `fs_blocks` can never
    /// desync from `tq_codes` when a new insertion path is added.
    fn maintain_fastscan_shadow(&mut self, internal_id: u32, code: &[u8], is_a2: bool) {
        if !is_a2 && code.len() * 2 == self.padded_dimension as usize {
            push_fastscan_shadow(&mut self.fs_blocks, internal_id as usize, code);
            self.byte_size += code.len();
        }
    }

    /// O(1) structural check that the shadow covers exactly the blocks needed
    /// for `entries.len()` vectors. Used as the FastScan gate: a mismatch
    /// (e.g. a future writer that misses the shadow) falls back to the plain
    /// scan instead of scanning stale/garbage lanes.
    #[inline]
    fn fastscan_shadow_consistent(&self, code_len: usize) -> bool {
        let expected = self.entries.len().div_ceil(FASTSCAN_BLOCK) * code_len * FASTSCAN_BLOCK;
        !self.fs_blocks.is_empty() && self.fs_blocks.len() == expected
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
        // SQ8 stores `dim` u8 codes + an 8-byte (min, scale) trailer, sized by the
        // true dimension (no FWHT padding). All other quantizers use padded packed
        // codes + a 4-byte norm trailer.
        let bytes_per_code = if collection.quantization == QuantizationConfig::Sq8 {
            dimension as usize + SQ8_PARAMS_BYTES
        } else {
            collection.code_bytes_per_vector() + 4 // packed codes + 4 bytes norm
        };
        let m = collection.qjl_num_projections.max(1);
        let qjl_bytes_per_vec = m * ((dimension as usize + 7) / 8);
        let sub_sign_bytes_per_vec = (padded as usize + 7) / 8;
        Self {
            inner: RwLock::new(MutableSegmentInner {
                tq_codes: Vec::new(),
                fs_blocks: Vec::new(),
                qjl_signs: Vec::new(),
                residual_norms: Vec::new(),
                raw_f32: Vec::new(),
                raw_f16: Vec::new(),
                sub_centroid_signs: Vec::new(),
                sub_sign_bytes_per_vec,
                entries: Vec::new(),
                global_id_base: 0,
                dimension,
                padded_dimension: padded,
                bytes_per_code,
                qjl_bytes_per_vec,
                byte_size: 0,
            }),
            collection,
        }
    }

    /// Append a vector. TQ-encodes at insert time; QJL deferred to freeze().
    ///
    /// Fast path: only FWHT + quantize + nibble pack (O(d log d)).
    /// QJL encoding (O(M×d²)) is deferred to freeze() when the segment compacts.
    /// Mutable brute-force search uses TQ-MSE-only distance (no QJL correction).
    pub fn append(&self, key_hash: u64, vector_f32: &[f32], insert_lsn: u64) -> u32 {
        let mut inner = self.inner.write();
        let internal_id = inner.entries.len() as u32;
        let dim = inner.dimension as usize;
        let padded = inner.padded_dimension as usize;
        let bytes_per_code = inner.bytes_per_code;

        // SQ8 path: per-vector affine scalar quantization. No FWHT, no codebook,
        // no QJL/sub-centroid side arrays. Slot = `dim` u8 codes + (min, scale) f32.
        // For Cosine, normalize first so squared-L2 ADC ranking == cosine ranking
        // (mirrors how TQ operates on unit vectors); L2/IP encode the raw vector.
        if self.collection.quantization == QuantizationConfig::Sq8 {
            let (codes, raw_norm) = encode_sq8_slot(vector_f32, self.collection.metric, dim);
            inner.tq_codes.extend_from_slice(&codes);

            // Keep the sub-centroid sign buffer offset-consistent (unused for SQ8).
            let sub_bpv = inner.sub_sign_bytes_per_vec;
            inner
                .sub_centroid_signs
                .extend(std::iter::repeat_n(0u8, sub_bpv));

            let is_exact = self.collection.build_mode
                == crate::vector::turbo_quant::collection::BuildMode::Exact;
            crate::vector::f16::encode_f16_slice(vector_f32, &mut inner.raw_f16);
            let mut extra_bytes = dim * 2; // f16 sidecar source
            if is_exact {
                let qjl_bpv = inner.qjl_bytes_per_vec;
                let new_qjl_len = inner.qjl_signs.len() + qjl_bpv;
                inner.qjl_signs.resize(new_qjl_len, 0u8);
                inner.residual_norms.push(0.0);
                inner.raw_f32.extend_from_slice(vector_f32);
                extra_bytes += qjl_bpv + 4 + dim * 4;
            }

            inner.entries.push(MutableEntry {
                internal_id,
                key_hash,
                vector_offset: internal_id,
                norm: raw_norm,
                insert_lsn,
                delete_lsn: 0,
                txn_id: 0,
            });
            inner.byte_size += bytes_per_code + extra_bytes + std::mem::size_of::<MutableEntry>();
            return internal_id;
        }

        // Step 1: TQ-MSE encode (fast: O(d log d) via FWHT)
        // For scalar TQ4: also compute sub-centroid signs at encode time.
        // These signs double effective quantization resolution during HNSW search
        // (32-level LUT instead of 16), improving recall by ~3-5% at zero memory cost
        // in the search path (signs are stored alongside TQ codes).
        let fwht_signs = self.collection.fwht_sign_flips.as_slice();
        let mut work_buf = vec![0.0f32; padded];
        let is_scalar_tq4 = self.collection.quantization == QuantizationConfig::TurboQuant4;
        let (code, sub_signs) = if self.collection.quantization == QuantizationConfig::TurboQuant4A2
        {
            let a2_cb = crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
                self.collection.padded_dimension,
            );
            (
                encode_tq_mse_a2(vector_f32, fwht_signs, &a2_cb, &mut work_buf),
                None,
            )
        } else if is_scalar_tq4 {
            let boundaries = self.collection.codebook_boundaries_15();
            let centroids = self.collection.codebook_16();
            let with_signs = encode_tq_mse_scaled_with_signs(
                vector_f32,
                fwht_signs,
                boundaries,
                centroids,
                &mut work_buf,
            );
            (with_signs.code, Some(with_signs.signs))
        } else {
            let boundaries = self.collection.codebook_boundaries_15();
            (
                encode_tq_mse_scaled(vector_f32, fwht_signs, boundaries, &mut work_buf),
                None,
            )
        };

        // Append packed code + norm to TQ buffer
        inner.tq_codes.extend_from_slice(&code.codes);
        inner.tq_codes.extend_from_slice(&code.norm.to_le_bytes());

        // FastScan shadow: scalar-codebook TQ4 only (A2 never scans via ADC).
        let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
        inner.maintain_fastscan_shadow(internal_id, &code.codes, is_a2);

        // Append sub-centroid signs (Light mode TQ4 only)
        if let Some(signs) = sub_signs {
            inner.sub_centroid_signs.extend_from_slice(&signs);
        } else {
            // Zero-fill for non-TQ4 paths (A2, multi-bit)
            let sub_bpv = inner.sub_sign_bytes_per_vec;
            inner
                .sub_centroid_signs
                .extend(std::iter::repeat_n(0u8, sub_bpv));
        }

        // Exact mode: retain raw f32 + zero-fill QJL (recomputed at freeze).
        // Light mode: skip both — saves 1,536 B/vec + avoids O(M×d²) at freeze.
        let is_exact =
            self.collection.build_mode == crate::vector::turbo_quant::collection::BuildMode::Exact;
        crate::vector::f16::encode_f16_slice(vector_f32, &mut inner.raw_f16);
        let mut extra_bytes = dim * 2; // f16 sidecar source
        if is_exact {
            let qjl_bpv = inner.qjl_bytes_per_vec;
            let new_qjl_len = inner.qjl_signs.len() + qjl_bpv;
            inner.qjl_signs.resize(new_qjl_len, 0u8);
            inner.residual_norms.push(0.0);
            inner.raw_f32.extend_from_slice(vector_f32);
            extra_bytes += qjl_bpv + 4 + dim * 4;
        }

        inner.entries.push(MutableEntry {
            internal_id,
            key_hash,
            vector_offset: internal_id,
            norm: code.norm,
            insert_lsn,
            delete_lsn: 0,
            txn_id: 0,
        });

        inner.byte_size += bytes_per_code + extra_bytes + std::mem::size_of::<MutableEntry>();
        internal_id
    }

    /// Brute-force search on mutable segment.
    ///
    /// Light mode: TQ-ADC scoring (fast, no QJL overhead).
    /// Exact mode: TurboQuant_prod unbiased L2 (higher accuracy).
    pub fn brute_force_search(
        &self,
        query_f32: &[f32],
        query_state: Option<&crate::vector::turbo_quant::inner_product::TqProdQueryState>,
        k: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        self.brute_force_search_filtered(query_f32, query_state, k, None)
    }

    /// Brute-force filtered search. Routes to TQ-ADC or TQ_prod based on build_mode.
    pub fn brute_force_search_filtered(
        &self,
        query_f32: &[f32],
        query_state: Option<&crate::vector::turbo_quant::inner_product::TqProdQueryState>,
        k: usize,
        allow_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        let inner = self.inner.read();
        let dim = inner.dimension as usize;
        let padded = inner.padded_dimension as usize;
        let bytes_per_code = inner.bytes_per_code;

        // SQ8: per-vector affine decode + true squared-L2 ADC (no FWHT, no codebook).
        // Query is normalized for unit-sphere metrics (Cosine + InnerProduct) to
        // match the encode-side normalization.
        if self.collection.quantization == QuantizationConfig::Sq8 {
            // Heap-free query prep: borrow for L2, inline-normalize otherwise.
            let q = Sq8Query::prepare(query_f32, self.collection.metric);
            let q = q.as_slice();
            // HQ-2: resolve the SIMD-dispatched ADC stats kernel and the
            // per-query constants (Σq_i, Σq_i²) ONCE, before the candidate
            // loop below — never per candidate.
            let sq8_stats_fn = distance::table().sq8_stats;
            let (q_sum, q_sumsq) = sq8_query_stats(q);
            // Task #13: int8 symmetric ADC — quantize once per search when a
            // SIMD int8 kernel is installed for this CPU/build; else fall
            // back to the f32 `sq8_stats_fn` path above.
            let sq8_i8_stats_fn = distance::table().sq8_i8_stats;
            let (sq8_qi8, sq8_q_scale, sq8_sum_qi8): (Vec<i8>, f32, i32) =
                if sq8_i8_stats_fn.is_some() {
                    let mut qi8 = vec![0i8; dim];
                    let (scale, sum) = sq8_quantize_query_scalar(q, SQ8_INT8_QMAX, &mut qi8);
                    (qi8, scale, sum)
                } else {
                    (Vec::new(), 0.0, 0)
                };
            let mut heap: BinaryHeap<DistF32> = BinaryHeap::with_capacity(k + 1);
            for entry in &inner.entries {
                if entry.delete_lsn != 0 {
                    continue;
                }
                if let Some(bm) = allow_bitmap {
                    let gid = inner.global_id_base + entry.internal_id;
                    if !bm.contains(gid) {
                        continue;
                    }
                }
                let id = entry.internal_id as usize;
                let off = id * bytes_per_code;
                let slot = &inner.tq_codes[off..off + bytes_per_code];
                let (min, scale) = sq8_params(slot, dim);
                let (dot_qc, sum_c, sumsq_c) = if let Some(i8_stats) = sq8_i8_stats_fn {
                    let (dot_int, sum_c_int, sumsq_c_int) =
                        i8_stats(&sq8_qi8, &slot[..dim], sq8_sum_qi8);
                    (
                        sq8_int8_dot_to_f32(sq8_q_scale, dot_int),
                        sum_c_int as f32,
                        sumsq_c_int as f32,
                    )
                } else {
                    sq8_stats_fn(q, &slot[..dim])
                };
                let dist =
                    sq8_l2_from_stats(dim, min, scale, q_sum, q_sumsq, dot_qc, sum_c, sumsq_c);
                let global_id = inner.global_id_base + entry.internal_id;
                if heap.len() < k {
                    heap.push(DistF32(dist, global_id, entry.key_hash));
                } else if let Some(&DistF32(worst, _, _)) = heap.peek() {
                    if dist < worst {
                        heap.pop();
                        heap.push(DistF32(dist, global_id, entry.key_hash));
                    }
                }
            }
            return heap
                .into_sorted_vec()
                .into_iter()
                .map(|DistF32(d, id, kh)| SearchResult::with_key_hash(d, VectorId(id), kh))
                .collect();
        }

        let code_len = bytes_per_code - 4;
        // A2 collections don't have a scalar codebook; TQ-ADC not applicable.
        let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
        // Placeholder codebook for A2 (unused in L2 fallback path).
        let a2_placeholder = [0.0f32; 16];
        let centroids: &[f32; 16] = if is_a2 {
            &a2_placeholder
        } else {
            self.collection.codebook_16()
        };

        let mut heap: BinaryHeap<DistF32> = BinaryHeap::with_capacity(k + 1);

        // Distance strategy:
        // - Scalar TQ4 (Light mode or no query_state): TQ-ADC with rotated query
        // - Scalar TQ4 (Exact mode with query_state): TurboQuant_prod scoring
        // - A2 TQ4A2: decoded-vector symmetric L2 (no scalar ADC available)
        let use_tq_adc = !is_a2
            && (query_state.is_none()
                || self.collection.build_mode
                    == crate::vector::turbo_quant::collection::BuildMode::Light);
        let use_a2_decoded_l2 = is_a2;

        // Prepare FWHT-rotated query for TQ-ADC or A2 decoded-L2 path
        let q_rotated: Vec<f32> = if use_tq_adc || use_a2_decoded_l2 {
            let mut buf = vec![0.0f32; padded];
            buf[..dim].copy_from_slice(query_f32);
            let norm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                let inv = 1.0 / norm;
                for v in buf[..dim].iter_mut() {
                    *v *= inv;
                }
            }
            fwht::fwht(&mut buf, self.collection.fwht_sign_flips.as_slice());
            buf
        } else {
            Vec::new()
        };

        // Pre-build A2 codebook for decoded-L2 path
        let a2_cb_for_search = if use_a2_decoded_l2 {
            Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
                self.collection.padded_dimension,
            ))
        } else {
            None
        };

        for entry in &inner.entries {
            if entry.delete_lsn != 0 {
                continue;
            }
            if let Some(bm) = allow_bitmap {
                let gid = inner.global_id_base + entry.internal_id;
                if !bm.contains(gid) {
                    continue;
                }
            }
            let id = entry.internal_id as usize;
            let tq_offset = id * bytes_per_code;
            let tq_code = &inner.tq_codes[tq_offset..tq_offset + code_len];

            let dist = if use_a2_decoded_l2 {
                // A2: decode nibble pairs to f32, compute symmetric L2 vs rotated query
                let cb = a2_cb_for_search.as_ref();
                if let Some(a2cb) = cb {
                    let mut decoded = Vec::with_capacity(padded);
                    for &byte in tq_code {
                        let (x0, y0) = a2cb.decode_pair(byte & 0x0F);
                        let (x1, y1) = a2cb.decode_pair(byte >> 4);
                        decoded.push(x0);
                        decoded.push(y0);
                        decoded.push(x1);
                        decoded.push(y1);
                    }
                    decoded.truncate(padded);
                    // L2 between decoded centroid vector and rotated query, scaled by norm²
                    let norm_sq = entry.norm * entry.norm;
                    let mut sum = 0.0f32;
                    for j in 0..padded.min(decoded.len()).min(q_rotated.len()) {
                        let d = q_rotated[j] - decoded[j];
                        sum += d * d;
                    }
                    sum * norm_sq
                } else {
                    f32::MAX
                }
            } else if use_tq_adc {
                tq_l2_adc_scaled(&q_rotated, tq_code, entry.norm, centroids)
            } else if let Some(qs) = query_state {
                let qjl_bpv = inner.qjl_bytes_per_vec;
                let qjl_offset = id * qjl_bpv;
                let qjl_signs = &inner.qjl_signs[qjl_offset..qjl_offset + qjl_bpv];
                let residual_norm = inner.residual_norms[id];
                let single_qjl_bpv = (dim + 7) / 8;
                crate::vector::turbo_quant::inner_product::score_l2_prod(
                    qs,
                    tq_code,
                    entry.norm,
                    qjl_signs,
                    residual_norm,
                    centroids,
                    dim,
                    single_qjl_bpv,
                )
            } else {
                f32::MAX // unreachable: non-A2, non-ADC, no query_state
            };

            let global_id = inner.global_id_base + entry.internal_id;
            let key_hash = entry.key_hash;
            if heap.len() < k {
                heap.push(DistF32(dist, global_id, key_hash));
            } else if let Some(&DistF32(worst, _, _)) = heap.peek() {
                if dist < worst {
                    heap.pop();
                    heap.push(DistF32(dist, global_id, key_hash));
                }
            }
        }

        heap.into_sorted_vec()
            .into_iter()
            .map(|DistF32(d, id, kh)| SearchResult::with_key_hash(d, VectorId(id), kh))
            .collect()
    }

    /// MVCC-aware brute-force search using TurboQuant_prod L2 distance.
    ///
    /// Scans the half-open entry range `[start, end)` (clamped to the current
    /// entry count). The mutable segment is APPEND-ONLY (entries only `push`;
    /// deletes set `delete_lsn` in place — never removal/reorder), so a caller
    /// that captured `end = len` at search start can scan `[0, end)` across
    /// cooperative yields and remain isolation-correct: appends committed during
    /// a yield land at indices ≥ `end` (invisible to this scan), and deletes set
    /// `delete_lsn > snapshot_lsn` (still visible to this snapshot via
    /// `is_visible`). Full-scan callers pass `0..len`. (ft-search-off-eventloop)
    ///
    /// Chunked callers (the yielding path) should instead call
    /// [`Self::prepare_brute_force_query`] once and
    /// [`Self::brute_force_scan_mvcc_chunk`] per chunk, so the query
    /// rotation/normalization and the top-k heap are NOT redone per chunk (QP-4).
    #[allow(clippy::too_many_arguments)]
    pub fn brute_force_search_mvcc(
        &self,
        query_f32: &[f32],
        query_state: Option<&crate::vector::turbo_quant::inner_product::TqProdQueryState>,
        k: usize,
        allow_bitmap: Option<&RoaringBitmap>,
        snapshot_lsn: u64,
        my_txn_id: u64,
        committed: &roaring::RoaringTreemap,
        start: usize,
        end: usize,
    ) -> SmallVec<[SearchResult; 32]> {
        let mut q = self.prepare_brute_force_query(query_f32, query_state.is_some(), k);
        self.brute_force_scan_mvcc_chunk(
            &mut q,
            query_state,
            k,
            allow_bitmap,
            snapshot_lsn,
            my_txn_id,
            committed,
            start,
            end,
        );
        q.into_results()
    }

    /// One-time per-query setup for the chunked MVCC brute-force scan (QP-4):
    /// prepares the (possibly normalized/FWHT-rotated) query buffer and the
    /// shared top-k heap that persist across yield chunks. A single per-query
    /// allocation at capture — never per-chunk (G-HOTPATH SAFETY-NET clause).
    pub fn prepare_brute_force_query(
        &self,
        query_f32: &[f32],
        have_query_state: bool,
        k: usize,
    ) -> BruteForceQuery {
        let dim = query_f32.len();
        let padded = self.collection.padded_dimension as usize;
        let prepared: Vec<f32>;
        let use_tq_adc: bool;
        if self.collection.quantization == QuantizationConfig::Sq8 {
            // Mirrors `Sq8Query::prepare`, owned: copy for L2, normalize otherwise.
            use_tq_adc = false;
            let mut q = query_f32.to_vec();
            if self.collection.metric != DistanceMetric::L2 {
                let n: f32 = q.iter().map(|x| x * x).sum::<f32>().sqrt();
                if n > 0.0 {
                    let inv = 1.0 / n;
                    for v in q.iter_mut() {
                        *v *= inv;
                    }
                }
            }
            prepared = q;
        } else {
            let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
            use_tq_adc = !is_a2
                && (!have_query_state
                    || self.collection.build_mode
                        == crate::vector::turbo_quant::collection::BuildMode::Light);
            if use_tq_adc {
                let mut buf = vec![0.0f32; padded];
                buf[..dim].copy_from_slice(query_f32);
                let norm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    let inv = 1.0 / norm;
                    for v in buf[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut buf, self.collection.fwht_sign_flips.as_slice());
                prepared = buf;
            } else {
                prepared = Vec::new();
            }
        }
        // HQ-2: per-query ADC constants over the prepared (normalized) query —
        // the same buffer the chunk scan feeds the stats kernel.
        let (sq8_q_sum, sq8_q_sumsq) = if self.collection.quantization == QuantizationConfig::Sq8 {
            sq8_query_stats(&prepared)
        } else {
            (0.0, 0.0)
        };
        // Task #13: int8 symmetric ADC — quantize once here (persists across
        // chunks via `BruteForceQuery`, mirroring `sq8_q_sum`/`sq8_q_sumsq`
        // above) only when a SIMD int8 kernel is installed for this
        // CPU/build; empty otherwise (chunk scan falls back to f32 stats).
        let (sq8_qi8, sq8_q_scale, sq8_sum_qi8): (Vec<i8>, f32, i32) =
            if self.collection.quantization == QuantizationConfig::Sq8
                && distance::table().sq8_i8_stats.is_some()
            {
                let mut qi8 = vec![0i8; prepared.len()];
                let (scale, sum) = sq8_quantize_query_scalar(&prepared, SQ8_INT8_QMAX, &mut qi8);
                (qi8, scale, sum)
            } else {
                (Vec::new(), 0.0, 0)
            };
        // FastScan pre-filter LUT (TQ-ADC path only): one per-query build,
        // gated on segment size so tiny segments skip the fixed LUT cost.
        // `use_tq_adc` already excludes SQ8 and A2 (the paths without a
        // scalar 16-centroid codebook / interleaved shadow).
        let (fs_lut, fs_inv_scale, fs_bias, fs_eps) =
            if use_tq_adc && self.inner.read().entries.len() >= FASTSCAN_MIN_ENTRIES {
                let centroids = self.collection.codebook_16();
                let mut lut = vec![0u8; prepared.len() * 16];
                let params = fastscan::build_quantized_lut(&prepared, centroids, &mut lut);
                // Round-to-nearest quantization: each of the padded_dim LUT entries
                // carries at most 0.5 quanta of error, so 0.5·padded_dim/scale is a
                // sound accumulated bound (kernel saturation only *under*-estimates,
                // which routes the candidate to the exact rescore anyway). The 1e-3
                // headroom covers f32 accumulation order differences.
                let eps = 0.5 * prepared.len() as f32 / params.scale * 1.001;
                (lut, 1.0 / params.scale, params.bias, eps)
            } else {
                (Vec::new(), 0.0, 0.0, 0.0)
            };

        BruteForceQuery {
            prepared,
            use_tq_adc,
            sq8_q_sum,
            sq8_q_sumsq,
            sq8_qi8,
            sq8_q_scale,
            sq8_sum_qi8,
            fs_lut,
            fs_inv_scale,
            fs_bias,
            fs_eps,
            heap: BinaryHeap::with_capacity(k + 1),
        }
    }

    /// Scan one `[start, end)` chunk, accumulating into the query's shared
    /// top-k heap. Isolation contract identical to
    /// [`Self::brute_force_search_mvcc`]; the inner read lock is taken per
    /// chunk so cooperative yields between chunks never hold it.
    #[allow(clippy::too_many_arguments)]
    pub fn brute_force_scan_mvcc_chunk(
        &self,
        q: &mut BruteForceQuery,
        query_state: Option<&crate::vector::turbo_quant::inner_product::TqProdQueryState>,
        k: usize,
        allow_bitmap: Option<&RoaringBitmap>,
        snapshot_lsn: u64,
        my_txn_id: u64,
        committed: &roaring::RoaringTreemap,
        start: usize,
        end: usize,
    ) {
        let inner = self.inner.read();
        let hi = end.min(inner.entries.len());
        let lo = start.min(hi);
        let dim = inner.dimension as usize;
        let bytes_per_code = inner.bytes_per_code;

        // SQ8: per-vector affine decode + true squared-L2 ADC (MVCC-visible scan).
        if self.collection.quantization == QuantizationConfig::Sq8 {
            let q_slice = q.prepared.as_slice();
            // HQ-2: SIMD-dispatched stats kernel resolved once per chunk (a fn
            // pointer read), per-query constants carried in `BruteForceQuery`.
            let sq8_stats_fn = distance::table().sq8_stats;
            let (q_sum, q_sumsq) = (q.sq8_q_sum, q.sq8_q_sumsq);
            // Task #13: int8 symmetric ADC — resolved once per chunk like
            // `sq8_stats_fn`; `sq8_i8_stats_fn` is `Some` only when
            // `q.sq8_qi8` was actually populated in `prepare_brute_force_query`.
            let sq8_i8_stats_fn = distance::table().sq8_i8_stats;
            let sq8_qi8 = q.sq8_qi8.as_slice();
            let sq8_q_scale = q.sq8_q_scale;
            let sq8_sum_qi8 = q.sq8_sum_qi8;
            let heap = &mut q.heap;
            for entry in &inner.entries[lo..hi] {
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
                    let gid = inner.global_id_base + entry.internal_id;
                    if !bm.contains(gid) {
                        continue;
                    }
                }
                let id = entry.internal_id as usize;
                let off = id * bytes_per_code;
                let slot = &inner.tq_codes[off..off + bytes_per_code];
                let (min, scale) = sq8_params(slot, dim);
                let (dot_qc, sum_c, sumsq_c) = if let Some(i8_stats) = sq8_i8_stats_fn {
                    let (dot_int, sum_c_int, sumsq_c_int) =
                        i8_stats(sq8_qi8, &slot[..dim], sq8_sum_qi8);
                    (
                        sq8_int8_dot_to_f32(sq8_q_scale, dot_int),
                        sum_c_int as f32,
                        sumsq_c_int as f32,
                    )
                } else {
                    sq8_stats_fn(q_slice, &slot[..dim])
                };
                let dist =
                    sq8_l2_from_stats(dim, min, scale, q_sum, q_sumsq, dot_qc, sum_c, sumsq_c);
                let global_id = inner.global_id_base + entry.internal_id;
                if heap.len() < k {
                    heap.push(DistF32(dist, global_id, entry.key_hash));
                } else if let Some(&DistF32(worst, _, _)) = heap.peek() {
                    if dist < worst {
                        heap.pop();
                        heap.push(DistF32(dist, global_id, entry.key_hash));
                    }
                }
            }
            return;
        }

        let code_len = bytes_per_code - 4;
        let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
        let a2_placeholder = [0.0f32; 16];
        let centroids: &[f32; 16] = if is_a2 {
            &a2_placeholder
        } else {
            self.collection.codebook_16()
        };

        let use_tq_adc = q.use_tq_adc;
        let q_rotated = q.prepared.as_slice();

        // FastScan-filtered scan: batch 32 candidates per SIMD LUT pass, then
        // exact-rescore only candidates whose sound lower bound
        // `(approx - eps) * norm²` beats the current heap worst. Every pushed
        // distance is still the exact `tq_l2_adc_scaled` value, so results are
        // identical to the plain loop below — the filter only skips candidates
        // that provably cannot enter the top-k.
        // The shadow-consistency gate is the safety net: if a future writer
        // ever misses `maintain_fastscan_shadow`, the O(1) length check fails
        // and the scan falls back to the plain loop instead of reading
        // stale/garbage lanes. Note: a `[lo,hi)` chunk that splits a 32-block
        // re-scans that block's SIMD pass on the next chunk (out-of-range
        // lanes are skipped by position) — the default yield chunking is
        // 32-aligned, so this costs at most one duplicate block per
        // operator-tuned unaligned boundary.
        let use_fastscan = use_tq_adc
            && !q.fs_lut.is_empty()
            && code_len * 2 == q.prepared.len()
            && inner.fastscan_shadow_consistent(code_len);
        if use_fastscan {
            let scan = fastscan::fastscan_dispatch().scan_block;
            let fs_lut = q.fs_lut.as_slice();
            let (inv_scale, bias, eps) = (q.fs_inv_scale, q.fs_bias, q.fs_eps);
            let heap = &mut q.heap;
            let block_bytes = code_len * FASTSCAN_BLOCK;
            let mut block_dists = [0u16; 32];

            let mut block = lo / FASTSCAN_BLOCK;
            while block * FASTSCAN_BLOCK < hi {
                let vec_start = block * FASTSCAN_BLOCK;
                let base = block * block_bytes;
                scan(
                    &inner.fs_blocks[base..base + block_bytes],
                    fs_lut,
                    code_len,
                    &mut block_dists,
                );

                let lane_lo = lo.saturating_sub(vec_start);
                let lane_hi = (hi - vec_start).min(FASTSCAN_BLOCK);
                for lane in lane_lo..lane_hi {
                    let entry = &inner.entries[vec_start + lane];
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
                        let gid = inner.global_id_base + entry.internal_id;
                        if !bm.contains(gid) {
                            continue;
                        }
                    }
                    let norm_sq = entry.norm * entry.norm;
                    if heap.len() >= k {
                        let lower = (block_dists[lane] as f32 * inv_scale + bias - eps) * norm_sq;
                        if let Some(&DistF32(worst, _, _)) = heap.peek() {
                            if lower >= worst {
                                continue;
                            }
                        }
                    }

                    let id = entry.internal_id as usize;
                    let tq_offset = id * bytes_per_code;
                    let tq_code = &inner.tq_codes[tq_offset..tq_offset + code_len];
                    let dist = tq_l2_adc_scaled(q_rotated, tq_code, entry.norm, centroids);

                    let global_id = inner.global_id_base + entry.internal_id;
                    if heap.len() < k {
                        heap.push(DistF32(dist, global_id, entry.key_hash));
                    } else if let Some(&DistF32(worst, _, _)) = heap.peek() {
                        if dist < worst {
                            heap.pop();
                            heap.push(DistF32(dist, global_id, entry.key_hash));
                        }
                    }
                }
                block += 1;
            }
            return;
        }

        let heap = &mut q.heap;

        for entry in &inner.entries[lo..hi] {
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
                let gid = inner.global_id_base + entry.internal_id;
                if !bm.contains(gid) {
                    continue;
                }
            }
            let id = entry.internal_id as usize;
            let tq_offset = id * bytes_per_code;
            let tq_code = &inner.tq_codes[tq_offset..tq_offset + code_len];

            let dist = if use_tq_adc {
                tq_l2_adc_scaled(q_rotated, tq_code, entry.norm, centroids)
            } else {
                let qs = query_state.unwrap();
                let qjl_bpv = inner.qjl_bytes_per_vec;
                let qjl_offset = id * qjl_bpv;
                let qjl_signs = &inner.qjl_signs[qjl_offset..qjl_offset + qjl_bpv];
                let residual_norm = inner.residual_norms[id];
                let single_qjl_bpv = (dim + 7) / 8;
                crate::vector::turbo_quant::inner_product::score_l2_prod(
                    qs,
                    tq_code,
                    entry.norm,
                    qjl_signs,
                    residual_norm,
                    centroids,
                    dim,
                    single_qjl_bpv,
                )
            };

            let global_id = inner.global_id_base + entry.internal_id;
            let key_hash = entry.key_hash;
            if heap.len() < k {
                heap.push(DistF32(dist, global_id, key_hash));
            } else if let Some(&DistF32(worst, _, _)) = heap.peek() {
                if dist < worst {
                    heap.pop();
                    heap.push(DistF32(dist, global_id, key_hash));
                }
            }
        }
    }

    /// Append within a transaction context.
    pub fn append_transactional(
        &self,
        key_hash: u64,
        vector_f32: &[f32],
        insert_lsn: u64,
        txn_id: u64,
    ) -> u32 {
        // Delegate to append() logic with txn_id override
        let mut inner = self.inner.write();
        let internal_id = inner.entries.len() as u32;
        let dim = inner.dimension as usize;
        let padded = inner.padded_dimension as usize;
        let bytes_per_code = inner.bytes_per_code;

        // SQ8 uses a `dim + 8` affine slot, not the TQ `padded/2 + 4` layout. Without
        // this branch the TQ encoder below writes the wrong stride into `tq_codes`,
        // corrupting every transactionally-inserted or WAL-recovered SQ8 vector
        // (recovery.rs and the txn insert path both call this). Mirrors append()'s
        // SQ8 branch exactly (shared `encode_sq8_slot`), differing only in `txn_id`.
        if self.collection.quantization == QuantizationConfig::Sq8 {
            let (codes, raw_norm) = encode_sq8_slot(vector_f32, self.collection.metric, dim);
            inner.tq_codes.extend_from_slice(&codes);

            // Keep the sub-centroid sign buffer offset-consistent (unused for SQ8).
            let sub_bpv = inner.sub_sign_bytes_per_vec;
            inner
                .sub_centroid_signs
                .extend(std::iter::repeat_n(0u8, sub_bpv));

            let is_exact = self.collection.build_mode
                == crate::vector::turbo_quant::collection::BuildMode::Exact;
            crate::vector::f16::encode_f16_slice(vector_f32, &mut inner.raw_f16);
            let mut extra_bytes = dim * 2; // f16 sidecar source
            if is_exact {
                let qjl_bpv = inner.qjl_bytes_per_vec;
                let new_qjl_len = inner.qjl_signs.len() + qjl_bpv;
                inner.qjl_signs.resize(new_qjl_len, 0u8);
                inner.residual_norms.push(0.0);
                inner.raw_f32.extend_from_slice(vector_f32);
                extra_bytes += qjl_bpv + 4 + dim * 4;
            }

            inner.entries.push(MutableEntry {
                internal_id,
                key_hash,
                vector_offset: internal_id,
                norm: raw_norm,
                insert_lsn,
                delete_lsn: 0,
                txn_id,
            });
            inner.byte_size += bytes_per_code + extra_bytes + std::mem::size_of::<MutableEntry>();
            return internal_id;
        }

        let signs = self.collection.fwht_sign_flips.as_slice();
        let mut work_buf = vec![0.0f32; padded];
        let code = if self.collection.quantization == QuantizationConfig::TurboQuant4A2 {
            let a2_cb = crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
                self.collection.padded_dimension,
            );
            encode_tq_mse_a2(vector_f32, signs, &a2_cb, &mut work_buf)
        } else {
            let boundaries = self.collection.codebook_boundaries_15();
            encode_tq_mse_scaled(vector_f32, signs, boundaries, &mut work_buf)
        };

        inner.tq_codes.extend_from_slice(&code.codes);
        inner.tq_codes.extend_from_slice(&code.norm.to_le_bytes());

        // FastScan shadow: scalar-codebook TQ4 only (shared gate with append()).
        let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
        inner.maintain_fastscan_shadow(internal_id, &code.codes, is_a2);

        let is_exact =
            self.collection.build_mode == crate::vector::turbo_quant::collection::BuildMode::Exact;
        crate::vector::f16::encode_f16_slice(vector_f32, &mut inner.raw_f16);
        let mut extra_bytes = dim * 2; // f16 sidecar source
        if is_exact {
            let qjl_bpv = inner.qjl_bytes_per_vec;
            let new_qjl_len = inner.qjl_signs.len() + qjl_bpv;
            inner.qjl_signs.resize(new_qjl_len, 0u8);
            inner.residual_norms.push(0.0);
            inner.raw_f32.extend_from_slice(vector_f32);
            extra_bytes += qjl_bpv + 4 + dim * 4;
        }

        inner.entries.push(MutableEntry {
            internal_id,
            key_hash,
            vector_offset: internal_id,
            norm: code.norm,
            insert_lsn,
            delete_lsn: 0,
            txn_id,
        });

        inner.byte_size += bytes_per_code + extra_bytes + std::mem::size_of::<MutableEntry>();
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

    /// Resident bytes used by in-memory buffers (TQ codes, QJL signs,
    /// residual norms, raw f32 vectors, sub-centroid signs, entries).
    /// O(1) -- reads the running `byte_size` counter.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.inner.read().byte_size
    }

    /// Iterate live (non-deleted) entries, calling `f(key_hash, f32_vector, norm)` for each.
    /// Used by `force_compact` to merge multiple segments into one.
    /// Requires the mutable segment to retain `raw_f32` (BuildMode::Light or higher).
    pub fn iter_live<F>(&self, mut f: F)
    where
        F: FnMut(u64, &[f32], f32),
    {
        let inner = self.inner.read();
        let dim = inner.dimension as usize;
        if inner.raw_f32.len() < inner.entries.len() * dim {
            // raw_f32 not retained — skip (caller must handle this case separately).
            return;
        }
        for (i, entry) in inner.entries.iter().enumerate() {
            if entry.delete_lsn != 0 {
                continue;
            }
            let start = i * dim;
            let end = start + dim;
            f(entry.key_hash, &inner.raw_f32[start..end], entry.norm);
        }
    }

    /// Mark an entry as deleted.
    pub fn mark_deleted(&self, internal_id: u32, delete_lsn: u64) {
        let mut inner = self.inner.write();
        if let Some(entry) = inner.entries.get_mut(internal_id as usize) {
            entry.delete_lsn = delete_lsn;
        }
    }

    /// Tombstone the entry at `internal_id` iff its key_hash matches (VEC-1
    /// HSET-update fast path). The key check makes the O(1) index lookup safe
    /// even if the caller's `key_hash → global_id` mapping is stale (e.g.
    /// remapped by a concurrent compaction install) — on mismatch the caller
    /// falls back to the O(n) `mark_deleted_by_key_hash` scan.
    /// Returns `true` if an entry was tombstoned.
    pub fn mark_deleted_if_key(&self, internal_id: u32, key_hash: u64, delete_lsn: u64) -> bool {
        let mut inner = self.inner.write();
        if let Some(entry) = inner.entries.get_mut(internal_id as usize) {
            if entry.key_hash == key_hash && entry.delete_lsn == 0 {
                entry.delete_lsn = delete_lsn;
                return true;
            }
        }
        false
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

    /// Visit each entry in `[0..window_len)` calling `f(key_hash, delete_lsn)`.
    ///
    /// Used by background-compaction reconciliation to detect post-freeze
    /// deletions and overwrites in the window without exposing `inner`.
    pub fn for_each_window_entry<F>(&self, window_len: usize, mut f: F)
    where
        F: FnMut(u64, u64),
    {
        let inner = self.inner.read();
        let limit = window_len.min(inner.entries.len());
        for entry in inner.entries[..limit].iter() {
            f(entry.key_hash, entry.delete_lsn);
        }
    }

    /// Visit each entry in `[frozen_len..end)` calling `f(key_hash)`.
    ///
    /// Used by background-compaction reconciliation to detect key-hashes that
    /// were re-inserted in the tail window after the freeze point.  Combined
    /// with [`for_each_window_entry`] this lets `snap_and_reconcile` tombstone
    /// both deleted-then-gone entries *and* overwritten entries (same key_hash
    /// appears again in the tail) before wrapping the new ImmutableSegment.
    pub fn for_each_tail_entry<F>(&self, frozen_len: usize, mut f: F)
    where
        F: FnMut(u64),
    {
        let inner = self.inner.read();
        let start = frozen_len.min(inner.entries.len());
        for entry in inner.entries[start..].iter() {
            f(entry.key_hash);
        }
    }

    /// Tombstone entries matching `key_hash` whose `insert_lsn > threshold_lsn`.
    ///
    /// Designed for TXN.ABORT: the aborting transaction should only roll back
    /// rows it inserted, NOT rows committed by earlier transactions that
    /// happen to share the same Redis key (same xxh64 `key_hash`). The
    /// `threshold_lsn` is the aborting transaction's `snapshot_lsn` — any
    /// entry with `insert_lsn > threshold_lsn` must have been appended inside
    /// the transaction (MVCC invariant: LSN allocated monotonically).
    ///
    /// Each matching entry's `delete_lsn` is set to its own `insert_lsn`,
    /// which makes the entry invisible at every snapshot `>= insert_lsn`
    /// (per `crate::vector::mvcc::visibility::is_visible`: the delete is
    /// "at" the insert moment, i.e., the entry never existed from any
    /// reader's perspective). Returns the number of entries tombstoned.
    ///
    /// Preserves earlier-committed entries with the same `key_hash` (e.g.,
    /// a row inserted at T1, then mutated inside an aborted txn at T2 — the
    /// T1 row stays visible after the T2 rollback).
    pub fn mark_deleted_by_key_hash_after_lsn(&self, key_hash: u64, threshold_lsn: u64) -> u32 {
        let mut inner = self.inner.write();
        let mut count = 0u32;
        for entry in inner.entries.iter_mut() {
            if entry.key_hash == key_hash
                && entry.delete_lsn == 0
                && entry.insert_lsn > threshold_lsn
            {
                entry.delete_lsn = entry.insert_lsn;
                count += 1;
            }
        }
        count
    }

    /// Set the global ID base offset. Called when replacing a compacted mutable segment
    /// with a new empty one — the new segment's IDs start from where the old one left off.
    pub fn set_global_id_base(&self, base: u32) {
        self.inner.write().global_id_base = base;
    }

    /// Get the next global ID that would be assigned (base + current count).
    pub fn next_global_id(&self) -> u32 {
        let inner = self.inner.read();
        inner.global_id_base + inner.entries.len() as u32
    }

    /// Get the global ID base.
    pub fn global_id_base(&self) -> u32 {
        self.inner.read().global_id_base
    }

    /// Freeze: snapshot TQ codes and entries for compaction.
    pub fn freeze(&self) -> FrozenSegment {
        self.freeze_prefix(usize::MAX)
    }

    /// Freeze only the first `n` entries (clamped to len) for a **bounded**
    /// compaction build. Bulk loads compact into several threshold-sized
    /// segments instead of one giant graph — bounding build memory/latency and
    /// giving the intra-query search pool independent segments to fan out
    /// over. The frozen window is the prefix `[0, n)`, so entry
    /// `vector_offset`s (absolute from 0) stay valid; the tail survives via
    /// `clone_suffix(n)` at install, exactly like a mid-build append.
    pub fn freeze_prefix(&self, n: usize) -> FrozenSegment {
        let inner = self.inner.read();
        let n = n.min(inner.entries.len());
        let dim = inner.dimension as usize;
        // SQ8 has no QJL/residual side data; its codes are not TQ-decodable, so
        // the recompute paths (which assume TQ layout) must be skipped entirely.
        let exact_tq = self.collection.build_mode
            == crate::vector::turbo_quant::collection::BuildMode::Exact
            && self.collection.quantization != QuantizationConfig::Sq8;
        // Recompute is whole-buffer; truncate to the frozen window afterwards.
        let mut qjl_signs = if exact_tq {
            self.recompute_qjl_signs(&inner)
        } else {
            Vec::new()
        };
        qjl_signs.truncate(n * inner.qjl_bytes_per_vec);
        let mut residual_norms = if exact_tq {
            self.recompute_residual_norms(&inner)
        } else {
            Vec::new()
        };
        residual_norms.truncate(n);
        FrozenSegment {
            entries: inner.entries[..n]
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
            tq_codes: inner.tq_codes[..n * inner.bytes_per_code].to_vec(),
            qjl_signs,
            residual_norms,
            // empty in Light mode (nothing was appended)
            raw_f32: if inner.raw_f32.is_empty() {
                Vec::new()
            } else {
                inner.raw_f32[..n * dim].to_vec()
            },
            raw_f16: if inner.raw_f16.is_empty() {
                Vec::new()
            } else {
                inner.raw_f16[..n * dim].to_vec()
            },
            sub_centroid_signs: if inner.sub_centroid_signs.is_empty() {
                Vec::new()
            } else {
                inner.sub_centroid_signs[..n * inner.sub_sign_bytes_per_vec].to_vec()
            },
            sub_sign_bytes_per_vec: inner.sub_sign_bytes_per_vec,
            bytes_per_code: inner.bytes_per_code,
            qjl_bytes_per_vec: inner.qjl_bytes_per_vec,
            global_id_base: inner.global_id_base,
            dimension: inner.dimension,
        }
    }

    /// Copy the tail `[start..len)` of this segment into a fresh `MutableSegment`.
    ///
    /// Used by background-compaction install to preserve vectors that arrived
    /// **while** the worker was building the HNSW graph. The returned segment is
    /// ready to accept new appends immediately after install.
    ///
    /// ## Byte-copy semantics
    ///
    /// TQ codes and sub-centroid signs are copied verbatim — no re-encoding.
    /// In Light mode `raw_f32`, `qjl_signs`, and `residual_norms` are empty and
    /// are skipped. In Exact mode they are copied slice-by-slice.
    ///
    /// Entries whose `delete_lsn != 0` in the window are copied as-is (deleted).
    /// The brute-force path in the new segment already skips `delete_lsn != 0`
    /// entries, so they are invisible to search without filtering here.
    ///
    /// ## Global IDs
    ///
    /// `global_id_base` of the returned segment is set to
    /// `old_base + start`, preserving the global ID space.
    pub fn clone_suffix(&self, start: usize) -> Arc<MutableSegment> {
        let inner = self.inner.read();
        let n = inner.entries.len();
        let start = start.min(n); // clamp — never panic on stale index
        let count = n - start;

        let bpc = inner.bytes_per_code;
        let sub_bpv = inner.sub_sign_bytes_per_vec;
        let qjl_bpv = inner.qjl_bytes_per_vec;
        let dim = inner.dimension as usize;

        // ── TQ codes ────────────────────────────────────────────────────────
        let tq_start = start * bpc;
        let tq_codes = inner.tq_codes[tq_start..].to_vec();

        // ── Sub-centroid signs (always present, even if zero-filled) ─────────
        let sub_start = start * sub_bpv;
        let sub_centroid_signs = inner.sub_centroid_signs[sub_start..].to_vec();

        // ── Exact-mode optional fields ───────────────────────────────────────
        let qjl_signs = if inner.qjl_signs.is_empty() {
            Vec::new()
        } else {
            let qs = start * qjl_bpv;
            inner.qjl_signs[qs..].to_vec()
        };
        let residual_norms = if inner.residual_norms.is_empty() {
            Vec::new()
        } else {
            inner.residual_norms[start..].to_vec()
        };
        let raw_f32 = if inner.raw_f32.is_empty() {
            Vec::new()
        } else {
            let rs = start * dim;
            inner.raw_f32[rs..].to_vec()
        };
        let raw_f16 = if inner.raw_f16.is_empty() {
            Vec::new()
        } else {
            let rs = start * dim;
            inner.raw_f16[rs..].to_vec()
        };

        // ── Entries: rebase internal_id and vector_offset to 0-based ─────────
        let entries: Vec<MutableEntry> = inner.entries[start..]
            .iter()
            .enumerate()
            .map(|(i, e)| MutableEntry {
                internal_id: i as u32,
                key_hash: e.key_hash,
                vector_offset: i as u32,
                norm: e.norm,
                insert_lsn: e.insert_lsn,
                delete_lsn: e.delete_lsn,
                txn_id: e.txn_id,
            })
            .collect();

        // ── FastScan shadow: rebuild (lanes rebase along with internal ids) ──
        let fs_blocks = if inner.fs_blocks.is_empty() {
            Vec::new()
        } else {
            let code_len = bpc - 4;
            let mut fs = Vec::new();
            for i in 0..count {
                push_fastscan_shadow(&mut fs, i, &tq_codes[i * bpc..i * bpc + code_len]);
            }
            fs
        };

        let byte_size = count * (bpc + std::mem::size_of::<MutableEntry>())
            + fs_blocks.len()
            + count * sub_bpv
            + (if !qjl_signs.is_empty() {
                count * qjl_bpv
            } else {
                0
            })
            + (if !residual_norms.is_empty() {
                count * 4
            } else {
                0
            })
            + (if !raw_f32.is_empty() {
                count * dim * 4
            } else {
                0
            })
            + (if !raw_f16.is_empty() {
                count * dim * 2
            } else {
                0
            });

        let new_inner = MutableSegmentInner {
            tq_codes,
            fs_blocks,
            qjl_signs,
            residual_norms,
            raw_f32,
            raw_f16,
            sub_centroid_signs,
            sub_sign_bytes_per_vec: sub_bpv,
            entries,
            global_id_base: inner.global_id_base + start as u32,
            dimension: inner.dimension,
            padded_dimension: inner.padded_dimension,
            bytes_per_code: bpc,
            qjl_bytes_per_vec: qjl_bpv,
            byte_size,
        };

        Arc::new(MutableSegment {
            inner: parking_lot::RwLock::new(new_inner),
            collection: self.collection.clone(),
        })
    }

    /// Recompute QJL signs from retained raw f32 vectors.
    ///
    /// Called during freeze() to produce correct QJL signs for the immutable segment.
    /// Cost: O(N × M × d²) — amortized, runs once per compaction cycle.
    fn recompute_qjl_signs(&self, inner: &MutableSegmentInner) -> Vec<u8> {
        let dim = inner.dimension as usize;
        let padded = inner.padded_dimension as usize;
        let signs = self.collection.fwht_sign_flips.as_slice();
        let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
        let a2_cb = if is_a2 {
            Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
                self.collection.padded_dimension,
            ))
        } else {
            None
        };
        let centroids_opt: Option<&[f32; 16]> = if !is_a2 {
            Some(self.collection.codebook_16())
        } else {
            None
        };
        let bytes_per_code = inner.bytes_per_code;

        let mut qjl_signs = Vec::new();
        let mut work_buf = vec![0.0f32; padded];

        for (i, entry) in inner.entries.iter().enumerate() {
            let raw = &inner.raw_f32[i * dim..(i + 1) * dim];

            // Decode TQ to get residual
            let offset = entry.internal_id as usize * bytes_per_code;
            let code_end = offset + bytes_per_code - 4;
            let code_slice = &inner.tq_codes[offset..code_end];
            let norm_bytes = &inner.tq_codes[code_end..offset + bytes_per_code];
            let norm =
                f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

            let tq_code = crate::vector::turbo_quant::encoder::TqCode {
                codes: code_slice.to_vec(),
                norm,
            };
            let decoded = match (is_a2, a2_cb.as_ref(), centroids_opt) {
                (true, Some(cb), _) => crate::vector::turbo_quant::encoder::decode_tq_mse_a2(
                    &tq_code,
                    signs,
                    cb,
                    dim,
                    &mut work_buf,
                ),
                (false, _, Some(c)) => crate::vector::turbo_quant::encoder::decode_tq_mse_scaled(
                    &tq_code,
                    signs,
                    c,
                    dim,
                    &mut work_buf,
                ),
                _ => vec![0.0f32; dim], // fallback: zero vector (should not happen)
            };

            // Compute residual
            let mut residual = Vec::with_capacity(dim);
            for j in 0..dim {
                residual.push(raw[j] - decoded[j]);
            }

            // QJL encode residual for each projection matrix
            for matrix in &self.collection.qjl_matrices {
                let qs = crate::vector::turbo_quant::qjl::qjl_encode(matrix, &residual, dim);
                qjl_signs.extend_from_slice(&qs);
            }
            if self.collection.qjl_matrices.is_empty() {
                let qjl_bpv = inner.qjl_bytes_per_vec;
                qjl_signs.extend(std::iter::repeat_n(0u8, qjl_bpv));
            }
        }
        qjl_signs
    }

    /// Recompute residual norms from retained raw f32 vectors.
    fn recompute_residual_norms(&self, inner: &MutableSegmentInner) -> Vec<f32> {
        let dim = inner.dimension as usize;
        let padded = inner.padded_dimension as usize;
        let signs = self.collection.fwht_sign_flips.as_slice();
        let is_a2 = self.collection.quantization == QuantizationConfig::TurboQuant4A2;
        let a2_cb = if is_a2 {
            Some(crate::vector::turbo_quant::a2_lattice::A2Codebook::new(
                self.collection.padded_dimension,
            ))
        } else {
            None
        };
        let centroids_opt: Option<&[f32; 16]> = if !is_a2 {
            Some(self.collection.codebook_16())
        } else {
            None
        };
        let bytes_per_code = inner.bytes_per_code;

        let mut norms = Vec::with_capacity(inner.entries.len());
        let mut work_buf = vec![0.0f32; padded];

        for (i, entry) in inner.entries.iter().enumerate() {
            let raw = &inner.raw_f32[i * dim..(i + 1) * dim];
            let offset = entry.internal_id as usize * bytes_per_code;
            let code_end = offset + bytes_per_code - 4;
            let code_slice = &inner.tq_codes[offset..code_end];
            let norm_bytes = &inner.tq_codes[code_end..offset + bytes_per_code];
            let norm =
                f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);

            let tq_code = crate::vector::turbo_quant::encoder::TqCode {
                codes: code_slice.to_vec(),
                norm,
            };
            let decoded = match (is_a2, a2_cb.as_ref(), centroids_opt) {
                (true, Some(cb), _) => crate::vector::turbo_quant::encoder::decode_tq_mse_a2(
                    &tq_code,
                    signs,
                    cb,
                    dim,
                    &mut work_buf,
                ),
                (false, _, Some(c)) => crate::vector::turbo_quant::encoder::decode_tq_mse_scaled(
                    &tq_code,
                    signs,
                    c,
                    dim,
                    &mut work_buf,
                ),
                _ => vec![0.0f32; dim],
            };

            let mut r_norm_sq = 0.0f32;
            for j in 0..dim {
                let r = raw[j] - decoded[j];
                r_norm_sq += r * r;
            }
            norms.push(r_norm_sq.sqrt());
        }
        norms
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
        // Use Exact mode in tests to preserve TQ_prod scoring compatibility
        Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
            crate::vector::turbo_quant::collection::BuildMode::Exact,
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
            for x in v.iter_mut() {
                *x *= inv;
            }
        }
        v
    }

    fn make_query_state(
        query: &[f32],
        col: &CollectionMetadata,
    ) -> crate::vector::turbo_quant::inner_product::TqProdQueryState {
        crate::vector::turbo_quant::inner_product::prepare_query_prod(
            query,
            &col.qjl_matrices,
            col.fwht_sign_flips.as_slice(),
            col.padded_dimension as usize,
        )
    }

    fn rotate_query(query: &[f32], collection: &CollectionMetadata) -> Vec<f32> {
        let dim = query.len();
        let padded = collection.padded_dimension as usize;
        let mut q_rot = vec![0.0f32; padded];
        q_rot[..dim].copy_from_slice(query);
        let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if q_norm > 0.0 {
            let inv = 1.0 / q_norm;
            for v in q_rot[..dim].iter_mut() {
                *v *= inv;
            }
        }
        fwht::fwht(&mut q_rot, collection.fwht_sign_flips.as_slice());
        q_rot
    }

    #[test]
    fn test_fastscan_shadow_matches_vector_major_codes() {
        distance::init();
        let dim = 32u32;
        let col = make_collection(dim);
        let seg = MutableSegment::new(dim, col);

        // 70 vectors: 2 full blocks + 1 partial (6 lanes).
        for i in 0..70u32 {
            let v = make_f32_vector(dim as usize, 1000 + i);
            seg.append(i as u64, &v, i as u64 + 1);
        }

        let inner = seg.inner.read();
        let code_len = inner.bytes_per_code - 4;
        assert_eq!(code_len * 2, inner.padded_dimension as usize);
        let block_bytes = code_len * FASTSCAN_BLOCK;
        assert_eq!(
            inner.fs_blocks.len(),
            3 * block_bytes,
            "3 blocks for 70 entries"
        );

        for v in 0..70usize {
            let code =
                &inner.tq_codes[v * inner.bytes_per_code..v * inner.bytes_per_code + code_len];
            let block_base = (v / FASTSCAN_BLOCK) * block_bytes;
            let lane = v % FASTSCAN_BLOCK;
            for (d, &byte) in code.iter().enumerate() {
                assert_eq!(
                    inner.fs_blocks[block_base + d * FASTSCAN_BLOCK + lane],
                    byte,
                    "shadow mismatch at vector {v}, sub-dim {d}"
                );
            }
        }
        // Padding lanes of the partial block must be zero.
        let last_base = 2 * block_bytes;
        for d in 0..code_len {
            for lane in 6..FASTSCAN_BLOCK {
                assert_eq!(
                    inner.fs_blocks[last_base + d * FASTSCAN_BLOCK + lane],
                    0,
                    "padding lane {lane} must stay zero"
                );
            }
        }
    }

    #[test]
    fn test_fastscan_filtered_scan_identical_to_plain_scan() {
        distance::init();
        let dim = 32u32;
        let col = make_collection(dim);
        let seg = MutableSegment::new(dim, col);

        let n = 500u32;
        for i in 0..n {
            let v = make_f32_vector(dim as usize, 7000 + i);
            seg.append(i as u64, &v, i as u64 + 1);
        }

        let committed = roaring::RoaringTreemap::new();
        let query = make_f32_vector(dim as usize, 42);
        let k = 10;
        // Snapshot excludes the last 100 entries (insert_lsn > 400) and a
        // bitmap filter drops every third id — exercises both skip paths
        // inside the FastScan block loop.
        let snapshot_lsn = 400u64;
        let mut bitmap = RoaringBitmap::new();
        for i in 0..n {
            if i % 3 != 0 {
                bitmap.insert(i);
            }
        }

        // FastScan-filtered path (default: LUT built at prepare).
        let mut q_fs = seg.prepare_brute_force_query(&query, false, k);
        assert!(
            !q_fs.fs_lut.is_empty(),
            "FastScan LUT must engage above FASTSCAN_MIN_ENTRIES"
        );
        seg.brute_force_scan_mvcc_chunk(
            &mut q_fs,
            None,
            k,
            Some(&bitmap),
            snapshot_lsn,
            0,
            &committed,
            0,
            n as usize,
        );
        let fs_results = q_fs.into_results();

        // Plain path: identical query with the LUT cleared.
        let mut q_plain = seg.prepare_brute_force_query(&query, false, k);
        q_plain.fs_lut.clear();
        seg.brute_force_scan_mvcc_chunk(
            &mut q_plain,
            None,
            k,
            Some(&bitmap),
            snapshot_lsn,
            0,
            &committed,
            0,
            n as usize,
        );
        let plain_results = q_plain.into_results();

        assert_eq!(fs_results.len(), plain_results.len());
        assert_eq!(
            fs_results.len(),
            k,
            "enough visible entries for a full top-k"
        );
        for (a, b) in fs_results.iter().zip(plain_results.iter()) {
            assert_eq!(a.id, b.id, "FastScan filter changed the result set");
            assert_eq!(a.distance, b.distance, "distances must be bit-identical");
        }

        // Chunked scan (yield-path shape) must also match: same query fed in
        // 64-entry chunks.
        let mut q_chunked = seg.prepare_brute_force_query(&query, false, k);
        let mut start = 0usize;
        while start < n as usize {
            let end = (start + 64).min(n as usize);
            seg.brute_force_scan_mvcc_chunk(
                &mut q_chunked,
                None,
                k,
                Some(&bitmap),
                snapshot_lsn,
                0,
                &committed,
                start,
                end,
            );
            start = end;
        }
        let chunked_results = q_chunked.into_results();
        for (a, b) in chunked_results.iter().zip(plain_results.iter()) {
            assert_eq!(a.id, b.id, "chunked FastScan diverged");
            assert_eq!(a.distance, b.distance);
        }
    }

    #[test]
    fn test_append_returns_sequential_ids() {
        distance::init();
        let col = make_collection(128);
        let seg = MutableSegment::new(128, col);
        let v1 = make_f32_vector(128, 1);
        let v2 = make_f32_vector(128, 2);
        assert_eq!(seg.append(100, &v1, 1), 0);
        assert_eq!(seg.append(200, &v2, 2), 1);
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
            seg.append(i as u64, v, i as u64);
        }

        let _q_rot = rotate_query(&vectors[0], &col);
        let _codebook = col.codebook_16();
        let _qs = make_query_state(&vectors[0], &col);
        let results = seg.brute_force_search(&vectors[0], None, 3);

        assert!(results.len() <= 3);
        // First result should be vector 0 (nearest to itself)
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_sq8_append_and_brute_force_exact_match() {
        use crate::vector::turbo_quant::collection::BuildMode;
        distance::init();
        let dim = 64usize;
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
            BuildMode::Light,
        ));
        let seg = MutableSegment::new(dim as u32, collection);
        let db: Vec<Vec<f32>> = (0..50u32).map(|i| make_f32_vector(dim, 100 + i)).collect();
        for (i, v) in db.iter().enumerate() {
            seg.append(i as u64, v, 1);
        }

        // Query == db[7]; SQ8 must rank that vector first (exact-match invariant
        // that the broken codebook fallback violated, returning ml:0).
        let res = seg.brute_force_search(&db[7], None, 10);
        assert!(!res.is_empty(), "SQ8 brute force returned no results");
        assert_eq!(
            res[0].id.0, 7,
            "SQ8 nearest != exact match (got {})",
            res[0].id.0
        );

        // recall@10 vs exact-f32 L2 (== Cosine on unit vectors) must be near-perfect:
        // 8-bit fidelity barely perturbs ranking.
        let mut idx: Vec<usize> = (0..db.len()).collect();
        idx.sort_by(|&a, &b| {
            let da: f32 = db[7]
                .iter()
                .zip(&db[a])
                .map(|(x, y)| (x - y) * (x - y))
                .sum();
            let dbb: f32 = db[7]
                .iter()
                .zip(&db[b])
                .map(|(x, y)| (x - y) * (x - y))
                .sum();
            da.total_cmp(&dbb)
        });
        let exact: std::collections::HashSet<u32> =
            idx.into_iter().take(10).map(|i| i as u32).collect();
        let got: std::collections::HashSet<u32> = res.iter().map(|r| r.id.0).collect();
        let hits = exact.intersection(&got).count();
        assert!(hits >= 9, "SQ8 recall@10 too low: {hits}/10");
    }

    #[test]
    fn test_sq8_cosine_nonnormalized_ranking() {
        // Advisor must-fix: a Cosine index must rank by ANGLE even when inputs are
        // NOT unit-normalized. SQ8 normalizes at encode + query time; without that
        // the per-vector ||x||^2 term corrupts the squared-L2 ordering. The recall
        // harness pre-normalizes embeddings, so only a test like this catches it.
        use crate::vector::turbo_quant::collection::BuildMode;
        distance::init();
        let dim = 48usize;
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            7,
            BuildMode::Light,
        ));
        let seg = MutableSegment::new(dim as u32, collection);
        // Non-normalized db with widely varying magnitudes.
        let mut db: Vec<Vec<f32>> = Vec::new();
        for i in 0..40u32 {
            let mut v = make_f32_vector(dim, 500 + i); // unit direction
            let scale = 0.1 + (i as f32) * 0.5; // blow up + vary magnitude per vector
            for x in v.iter_mut() {
                *x *= scale;
            }
            seg.append(i as u64, &v, 1);
            db.push(v);
        }
        // Query == db[12] scaled by a different factor: identical direction (cos = 1),
        // very different magnitude. Cosine-nearest must still be 12.
        let mut q = db[12].clone();
        for x in q.iter_mut() {
            *x *= 4.2;
        }
        let res = seg.brute_force_search(&q, None, 5);
        assert_eq!(
            res[0].id.0, 12,
            "Cosine SQ8 misranked non-normalized input: got {}",
            res[0].id.0
        );
    }

    #[test]
    fn test_sq8_append_transactional_stride_and_exact_match() {
        // Regression (PR #166 review, Finding 3): append_transactional() must produce
        // the SAME dim+8 SQ8 slot as append(). Before the fix it ran the TQ encoder
        // (padded/2 + 4 layout), so every transactionally-inserted or WAL-recovered
        // SQ8 vector (recovery.rs:176 + the txn insert path) corrupted the tq_codes
        // stride. A stride mismatch scrambles the exact-match invariant below.
        use crate::vector::turbo_quant::collection::BuildMode;
        distance::init();
        let dim = 64usize;
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim as u32,
            DistanceMetric::Cosine,
            QuantizationConfig::Sq8,
            42,
            BuildMode::Light,
        ));
        let seg = MutableSegment::new(dim as u32, collection);
        let db: Vec<Vec<f32>> = (0..50u32).map(|i| make_f32_vector(dim, 100 + i)).collect();
        for (i, v) in db.iter().enumerate() {
            seg.append_transactional(i as u64, v, 1, 7);
        }

        // Direct stride check: n slots of exactly dim + SQ8_PARAMS_BYTES bytes.
        {
            let inner = seg.inner.read();
            assert_eq!(
                inner.tq_codes.len(),
                db.len() * (dim + SQ8_PARAMS_BYTES),
                "append_transactional wrote wrong SQ8 slot stride"
            );
        }

        // Exact-match: query == db[7] must rank db[7] first (broken under a stride
        // mismatch or the TQ empty-codebook fallback).
        let res = seg.brute_force_search(&db[7], None, 10);
        assert!(
            !res.is_empty(),
            "SQ8 transactional brute force returned no results"
        );
        assert_eq!(
            res[0].id.0, 7,
            "SQ8 transactional nearest != exact match (got {})",
            res[0].id.0
        );
    }

    #[test]
    fn test_sq8_inner_product_ranks_by_angle() {
        // PR #166 review (Findings 1 & 4): InnerProduct is a unit-sphere metric in
        // this engine — the TQ search path normalizes the query unconditionally, so
        // there is no true dot-product ranking anywhere; Cosine and IP are
        // equivalent. SQ8 must therefore normalize for IP exactly as for Cosine
        // (rank by ANGLE), not by raw squared-L2. Before the fix SQ8 only checked
        // `== Cosine`, so an IP index ranked non-normalized inputs by magnitude.
        use crate::vector::turbo_quant::collection::BuildMode;
        distance::init();
        let dim = 48usize;
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim as u32,
            DistanceMetric::InnerProduct,
            QuantizationConfig::Sq8,
            7,
            BuildMode::Light,
        ));
        let seg = MutableSegment::new(dim as u32, collection);
        // Non-normalized db, magnitudes varying widely across vectors.
        let mut db: Vec<Vec<f32>> = Vec::new();
        for i in 0..40u32 {
            let mut v = make_f32_vector(dim, 500 + i);
            let scale = 0.1 + (i as f32) * 0.5;
            for x in v.iter_mut() {
                *x *= scale;
            }
            seg.append(i as u64, &v, 1);
            db.push(v);
        }
        // Query == db[12] at a very different magnitude (same direction). With the
        // fix (normalize for IP) the angle-nearest is 12; ranking by raw L2 instead
        // would pick a high-magnitude vector.
        let mut q = db[12].clone();
        for x in q.iter_mut() {
            *x *= 4.2;
        }
        let res = seg.brute_force_search(&q, None, 5);
        assert_eq!(
            res[0].id.0, 12,
            "SQ8 InnerProduct misranked by magnitude: got {}",
            res[0].id.0
        );
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
        seg.append(0, &v0, 1);
        seg.append(1, &v1, 2);
        seg.append(2, &v2, 3);

        seg.mark_deleted(0, 10);

        let results = seg.brute_force_search(&v0, None, 3);
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
        seg.append(100, &v1, 1);
        seg.append(200, &v2, 2);

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
        seg.append(1, &make_f32_vector(128, 1), 1);
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
            seg.append(i as u64, v, i as u64);
        }

        let _q_rot = rotate_query(&vectors[0], &col);
        let _codebook = col.codebook_16();
        let committed = roaring::RoaringTreemap::new();
        let qs = make_query_state(&vectors[0], &col);

        let non_mvcc = seg.brute_force_search(&vectors[0], Some(&qs), 3);
        let mvcc = seg.brute_force_search_mvcc(
            &vectors[0],
            Some(&qs),
            3,
            None,
            0,
            0,
            &committed,
            0,
            usize::MAX,
        );

        assert_eq!(non_mvcc.len(), mvcc.len());
        for (a, b) in non_mvcc.iter().zip(mvcc.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }
}
