//! BUILD_MODE EXACT: QJL sign bits + residual norms for a freshly compacted
//! segment, computed on the compaction worker (moon#1192).
//!
//! HEAD computed these in `MutableSegment::freeze_prefix` — on the SHARD
//! thread, synchronously inside `begin_background_compact`, for the WHOLE
//! mutable buffer (then truncated to the frozen window), with a scalar d×d
//! matvec per projection: ~1 s of shard stall per 1K vectors at 384d. Here
//! they are derived from the frozen snapshot's retained `raw_f32`, for the
//! frozen LIVE entries only, in BFS order, with the SIMD `dot_f32` kernel
//! (`qjl::qjl_encode_into`).
//!
//! HEAD also indexed its QJL buffer by LIVE position while it was laid out
//! by mutable internal id, so every row after the first dead entry of the
//! window carried another vector's signs; computing per live entry removes
//! that misalignment.

use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::segment::mutable::{FrozenSegment, MutableEntry};
use crate::vector::turbo_quant::a2_lattice::A2Codebook;
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};

/// BFS-ordered QJL data for one compacted segment.
pub(super) struct ExactQjl {
    /// `n * bytes_per_vec` sign bytes (`M` projections of `ceil(dim/8)`
    /// bytes each, per entry). Empty when not applicable.
    pub signs: Vec<u8>,
    /// One residual norm per entry. Empty when not applicable.
    pub residual_norms: Vec<f32>,
    /// Sign bytes per entry.
    pub bytes_per_vec: usize,
}

/// Compute QJL signs + residual norms for an EXACT scalar-TQ / TQ4A2 build
/// with retained raw vectors; empty buffers otherwise (LIGHT, SQ8, raw-less
/// rebuilds — the immutable segment's TurboQuant_prod rerank fallback
/// no-ops on an empty buffer, exactly as for a segment reloaded from disk).
///
/// `live_entries[i]` is the frozen entry the graph's original id `i` maps
/// to; `tq_bfs` holds the codes in BFS order.
pub(super) fn exact_qjl_bfs(
    collection: &CollectionMetadata,
    frozen: &FrozenSegment,
    live_entries: &[&MutableEntry],
    graph: &HnswGraph,
    tq_bfs: &[u8],
) -> ExactQjl {
    let dim = frozen.dimension as usize;
    let n = live_entries.len();
    let single = dim.div_ceil(8);
    let applies = collection.build_mode == BuildMode::Exact
        && collection.quantization != QuantizationConfig::Sq8
        && !collection.qjl_matrices.is_empty()
        && frozen.raw_f32.len() >= frozen.entries.len() * dim
        && dim > 0;
    if !applies {
        return ExactQjl {
            signs: Vec::new(),
            residual_norms: Vec::new(),
            bytes_per_vec: 0,
        };
    }
    let m = collection.qjl_matrices.len();
    let bytes_per_vec = m * single;
    let padded = collection.padded_dimension as usize;
    let bytes_per_code = frozen.bytes_per_code;
    let code_len = bytes_per_code - 4;
    let flips = collection.fwht_sign_flips.as_slice();
    let is_a2 = collection.quantization == QuantizationConfig::TurboQuant4A2;
    let a2_cb = is_a2.then(|| A2Codebook::new(collection.padded_dimension));
    let codebook = if is_a2 {
        None
    } else {
        collection.try_codebook_16()
    };
    if !is_a2 && codebook.is_none() {
        return ExactQjl {
            signs: Vec::new(),
            residual_norms: Vec::new(),
            bytes_per_vec: 0,
        };
    }

    let mut signs = vec![0u8; n * bytes_per_vec];
    let mut residual_norms = vec![0.0f32; n];
    let mut work = vec![0.0f32; padded];
    let mut residual = vec![0.0f32; dim];
    for bfs_pos in 0..n {
        let orig = graph.to_original(bfs_pos as u32) as usize;
        let internal = live_entries[orig].internal_id as usize;
        let raw = &frozen.raw_f32[internal * dim..(internal + 1) * dim];
        let slot = &tq_bfs[bfs_pos * bytes_per_code..(bfs_pos + 1) * bytes_per_code];
        let code = &slot[..code_len];
        let norm = f32::from_le_bytes([
            slot[code_len],
            slot[code_len + 1],
            slot[code_len + 2],
            slot[code_len + 3],
        ]);

        // Decode TQ → rotated centroids → inverse FWHT → scale by norm (the
        // same arithmetic as `decode_tq_mse_scaled` / `decode_tq_mse_a2`).
        if let Some(cb) = a2_cb.as_ref() {
            for (i, &byte) in code.iter().enumerate() {
                let (x0, y0) = cb.decode_pair(byte & 0x0F);
                let (x1, y1) = cb.decode_pair(byte >> 4);
                for (k, v) in [x0, y0, x1, y1].into_iter().enumerate() {
                    if let Some(w) = work.get_mut(i * 4 + k) {
                        *w = v;
                    }
                }
            }
        } else if let Some(cb) = codebook {
            for (i, &byte) in code.iter().enumerate() {
                work[i * 2] = cb[(byte & 0x0F) as usize];
                work[i * 2 + 1] = cb[(byte >> 4) as usize];
            }
        }
        crate::vector::turbo_quant::fwht::inverse_fwht(&mut work[..padded], flips);

        let mut r_norm_sq = 0.0f32;
        for j in 0..dim {
            let r = raw[j] - work[j] * norm;
            residual[j] = r;
            r_norm_sq += r * r;
        }
        residual_norms[bfs_pos] = r_norm_sq.sqrt();

        let row = &mut signs[bfs_pos * bytes_per_vec..(bfs_pos + 1) * bytes_per_vec];
        for (p, matrix) in collection.qjl_matrices.iter().enumerate() {
            crate::vector::turbo_quant::qjl::qjl_encode_into(
                matrix,
                &residual,
                dim,
                &mut row[p * single..(p + 1) * single],
            );
        }
    }
    ExactQjl {
        signs,
        residual_norms,
        bytes_per_vec,
    }
}
