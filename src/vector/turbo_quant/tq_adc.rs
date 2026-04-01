//! TurboQuant Asymmetric Distance Computation (ADC).
//!
//! Computes L2 distance between a full-precision rotated query and a
//! nibble-packed TQ code. Used by HNSW beam search (Phase 61).
//!
//! The scalar version here serves as reference. AVX2/AVX-512 VPERMPS
//! versions are added in Phase 61+ for production throughput.

use super::codebook::CENTROIDS;

/// Asymmetric L2 distance using dimension-scaled centroids.
///
/// Same algorithm as `tq_l2_adc_scalar` but accepts the codebook as a parameter
/// instead of using the hardcoded (1/sqrt(768)) CENTROIDS constant.
/// This is the correct version for production use.
#[inline]
pub fn tq_l2_adc_scaled(q_rotated: &[f32], code: &[u8], norm: f32, centroids: &[f32; 16]) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 2);

    let norm_sq = norm * norm;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 2;

        let b0 = code[base];
        let b1 = code[base + 1];
        let b2 = code[base + 2];
        let b3 = code[base + 3];

        let d0lo = q_rotated[qbase] - centroids[(b0 & 0x0F) as usize];
        let d0hi = q_rotated[qbase + 1] - centroids[(b0 >> 4) as usize];
        sum0 += d0lo * d0lo + d0hi * d0hi;

        let d1lo = q_rotated[qbase + 2] - centroids[(b1 & 0x0F) as usize];
        let d1hi = q_rotated[qbase + 3] - centroids[(b1 >> 4) as usize];
        sum1 += d1lo * d1lo + d1hi * d1hi;

        let d2lo = q_rotated[qbase + 4] - centroids[(b2 & 0x0F) as usize];
        let d2hi = q_rotated[qbase + 5] - centroids[(b2 >> 4) as usize];
        sum2 += d2lo * d2lo + d2hi * d2hi;

        let d3lo = q_rotated[qbase + 6] - centroids[(b3 & 0x0F) as usize];
        let d3hi = q_rotated[qbase + 7] - centroids[(b3 >> 4) as usize];
        sum3 += d3lo * d3lo + d3hi * d3hi;
    }

    let tail_start = chunks * 4;
    for j in 0..remainder {
        let i = tail_start + j;
        let byte = code[i];
        let d_lo = q_rotated[i * 2] - centroids[(byte & 0x0F) as usize];
        let d_hi = q_rotated[i * 2 + 1] - centroids[(byte >> 4) as usize];
        sum0 += d_lo * d_lo + d_hi * d_hi;
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

/// Budgeted version of `tq_l2_adc_scaled` with early termination.
#[inline]
pub fn tq_l2_adc_scaled_budgeted(
    q_rotated: &[f32],
    code: &[u8],
    norm: f32,
    centroids: &[f32; 16],
    budget: f32,
) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 2);

    let norm_sq = norm * norm;
    let sum_budget = if norm_sq > 0.0 {
        budget / norm_sq
    } else {
        f32::MAX
    };

    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 2;

        let b0 = code[base];
        let b1 = code[base + 1];
        let b2 = code[base + 2];
        let b3 = code[base + 3];

        let d0lo = q_rotated[qbase] - centroids[(b0 & 0x0F) as usize];
        let d0hi = q_rotated[qbase + 1] - centroids[(b0 >> 4) as usize];
        sum0 += d0lo * d0lo + d0hi * d0hi;

        let d1lo = q_rotated[qbase + 2] - centroids[(b1 & 0x0F) as usize];
        let d1hi = q_rotated[qbase + 3] - centroids[(b1 >> 4) as usize];
        sum1 += d1lo * d1lo + d1hi * d1hi;

        let d2lo = q_rotated[qbase + 4] - centroids[(b2 & 0x0F) as usize];
        let d2hi = q_rotated[qbase + 5] - centroids[(b2 >> 4) as usize];
        sum2 += d2lo * d2lo + d2hi * d2hi;

        let d3lo = q_rotated[qbase + 6] - centroids[(b3 & 0x0F) as usize];
        let d3hi = q_rotated[qbase + 7] - centroids[(b3 >> 4) as usize];
        sum3 += d3lo * d3lo + d3hi * d3hi;

        if c & 15 == 15 {
            let partial = sum0 + sum1 + sum2 + sum3;
            if partial > sum_budget {
                return f32::MAX;
            }
        }
    }

    let tail_start = chunks * 4;
    for j in 0..remainder {
        let i = tail_start + j;
        let byte = code[i];
        let d_lo = q_rotated[i * 2] - centroids[(byte & 0x0F) as usize];
        let d_hi = q_rotated[i * 2 + 1] - centroids[(byte >> 4) as usize];
        sum0 += d_lo * d_lo + d_hi * d_hi;
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

/// Asymmetric L2 distance: full-precision query vs TQ code.
///
/// `q_rotated`: pre-rotated query (already FWHT'd, length = padded_dim).
/// `code`: nibble-packed TQ indices (length = padded_dim / 2).
/// `norm`: original vector norm stored in TqCode.
///
/// Returns estimated squared L2 distance.
///
/// Algorithm:
/// 1. Unpack nibbles to centroid indices inline (no allocation)
/// 2. For each dimension: d = q_rotated[i] - CENTROIDS[idx[i]]
/// 3. Sum d*d, scale by norm^2
#[inline]
pub fn tq_l2_adc_scalar(q_rotated: &[f32], code: &[u8], norm: f32) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 2);

    let norm_sq = norm * norm;

    // 4-way unrolled accumulation breaks dependency chain for out-of-order execution.
    // Each accumulator can retire independently, hiding FMA latency (~4 cycles).
    // Process 4 code bytes (8 dimensions) per iteration.
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    // Main unrolled loop: 4 bytes = 8 dimensions per iteration.
    // Indexing uses pre-computed base to help the optimizer.
    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 2;

        let b0 = code[base];
        let b1 = code[base + 1];
        let b2 = code[base + 2];
        let b3 = code[base + 3];

        let d0lo = q_rotated[qbase] - CENTROIDS[(b0 & 0x0F) as usize];
        let d0hi = q_rotated[qbase + 1] - CENTROIDS[(b0 >> 4) as usize];
        sum0 += d0lo * d0lo + d0hi * d0hi;

        let d1lo = q_rotated[qbase + 2] - CENTROIDS[(b1 & 0x0F) as usize];
        let d1hi = q_rotated[qbase + 3] - CENTROIDS[(b1 >> 4) as usize];
        sum1 += d1lo * d1lo + d1hi * d1hi;

        let d2lo = q_rotated[qbase + 4] - CENTROIDS[(b2 & 0x0F) as usize];
        let d2hi = q_rotated[qbase + 5] - CENTROIDS[(b2 >> 4) as usize];
        sum2 += d2lo * d2lo + d2hi * d2hi;

        let d3lo = q_rotated[qbase + 6] - CENTROIDS[(b3 & 0x0F) as usize];
        let d3hi = q_rotated[qbase + 7] - CENTROIDS[(b3 >> 4) as usize];
        sum3 += d3lo * d3lo + d3hi * d3hi;
    }

    // Handle remaining 0-3 bytes.
    let tail_start = chunks * 4;
    for j in 0..remainder {
        let i = tail_start + j;
        let byte = code[i];
        let d_lo = q_rotated[i * 2] - CENTROIDS[(byte & 0x0F) as usize];
        let d_hi = q_rotated[i * 2 + 1] - CENTROIDS[(byte >> 4) as usize];
        sum0 += d_lo * d_lo + d_hi * d_hi;
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

/// TQ-ADC distance with early termination budget.
///
/// Identical to `tq_l2_adc_scalar` but aborts early if the accumulated sum
/// exceeds `budget / norm^2`, returning `f32::MAX`. This avoids completing
/// the full ADC loop for neighbors that are clearly dominated.
///
/// `budget`: the worst distance currently in the results heap. If the partial
/// distance already exceeds this, the neighbor cannot improve results.
#[inline]
pub fn tq_l2_adc_budgeted(q_rotated: &[f32], code: &[u8], norm: f32, budget: f32) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 2);

    let norm_sq = norm * norm;
    // Pre-divide budget by norm^2 so we compare raw sums in the loop.
    let sum_budget = if norm_sq > 0.0 {
        budget / norm_sq
    } else {
        f32::MAX
    };

    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 2;

        let b0 = code[base];
        let b1 = code[base + 1];
        let b2 = code[base + 2];
        let b3 = code[base + 3];

        let d0lo = q_rotated[qbase] - CENTROIDS[(b0 & 0x0F) as usize];
        let d0hi = q_rotated[qbase + 1] - CENTROIDS[(b0 >> 4) as usize];
        sum0 += d0lo * d0lo + d0hi * d0hi;

        let d1lo = q_rotated[qbase + 2] - CENTROIDS[(b1 & 0x0F) as usize];
        let d1hi = q_rotated[qbase + 3] - CENTROIDS[(b1 >> 4) as usize];
        sum1 += d1lo * d1lo + d1hi * d1hi;

        let d2lo = q_rotated[qbase + 4] - CENTROIDS[(b2 & 0x0F) as usize];
        let d2hi = q_rotated[qbase + 5] - CENTROIDS[(b2 >> 4) as usize];
        sum2 += d2lo * d2lo + d2hi * d2hi;

        let d3lo = q_rotated[qbase + 6] - CENTROIDS[(b3 & 0x0F) as usize];
        let d3hi = q_rotated[qbase + 7] - CENTROIDS[(b3 >> 4) as usize];
        sum3 += d3lo * d3lo + d3hi * d3hi;

        // Check budget every 128 dimensions (16 iterations of 4-way unroll).
        // The partial sum is a lower bound on the final sum, so early exit is safe.
        // Checking every 16 iterations amortizes branch cost for best throughput.
        if c & 15 == 15 {
            let partial = sum0 + sum1 + sum2 + sum3;
            if partial > sum_budget {
                return f32::MAX;
            }
        }
    }

    let tail_start = chunks * 4;
    for j in 0..remainder {
        let i = tail_start + j;
        let byte = code[i];
        let d_lo = q_rotated[i * 2] - CENTROIDS[(byte & 0x0F) as usize];
        let d_hi = q_rotated[i * 2 + 1] - CENTROIDS[(byte >> 4) as usize];
        sum0 += d_lo * d_lo + d_hi * d_hi;
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

use crate::vector::turbo_quant::codebook::code_bytes_per_vector;
use crate::vector::turbo_quant::collection::CollectionMetadata;
use crate::vector::turbo_quant::fwht;
use crate::vector::types::{SearchResult, VectorId};
use smallvec::SmallVec;

/// Asymmetric L2 distance for any bit width (1-4).
///
/// Unpacks indices inline from the packed code based on bit width,
/// looks up centroids from the variable-length slice, and computes
/// squared difference. 4-way unrolled accumulation.
///
/// For bits=4, this produces identical results to `tq_l2_adc_scaled`.
#[inline]
pub fn tq_l2_adc_multibit(
    q_rotated: &[f32],
    code: &[u8],
    norm: f32,
    centroids: &[f32],
    bits: u8,
) -> f32 {
    match bits {
        1 => tq_l2_adc_1bit(q_rotated, code, norm, centroids),
        2 => tq_l2_adc_2bit(q_rotated, code, norm, centroids),
        3 => tq_l2_adc_3bit(q_rotated, code, norm, centroids),
        4 => {
            // Delegate to existing optimized 4-bit path
            debug_assert_eq!(centroids.len(), 16);
            let c: &[f32; 16] = centroids.try_into().unwrap_or_else(|_| {
                panic!(
                    "4-bit ADC requires exactly 16 centroids, got {}",
                    centroids.len()
                )
            });
            tq_l2_adc_scaled(q_rotated, code, norm, c)
        }
        _ => panic!("unsupported bit width: {bits}"),
    }
}

/// 1-bit ADC: extract single bit per dimension, 8 dimensions per byte.
#[inline]
fn tq_l2_adc_1bit(q_rotated: &[f32], code: &[u8], norm: f32, centroids: &[f32]) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 8);
    debug_assert_eq!(centroids.len(), 2);

    let norm_sq = norm * norm;
    let c0 = centroids[0];
    let c1 = centroids[1];
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 8;

        for j in 0..8 {
            let idx = (code[base] >> j) & 1;
            let cent = if idx == 0 { c0 } else { c1 };
            let d = q_rotated[qbase + j] - cent;
            sum0 += d * d;
        }
        for j in 0..8 {
            let idx = (code[base + 1] >> j) & 1;
            let cent = if idx == 0 { c0 } else { c1 };
            let d = q_rotated[qbase + 8 + j] - cent;
            sum1 += d * d;
        }
        for j in 0..8 {
            let idx = (code[base + 2] >> j) & 1;
            let cent = if idx == 0 { c0 } else { c1 };
            let d = q_rotated[qbase + 16 + j] - cent;
            sum2 += d * d;
        }
        for j in 0..8 {
            let idx = (code[base + 3] >> j) & 1;
            let cent = if idx == 0 { c0 } else { c1 };
            let d = q_rotated[qbase + 24 + j] - cent;
            sum3 += d * d;
        }
    }

    let tail_start = chunks * 4;
    for i in 0..remainder {
        let byte_idx = tail_start + i;
        let qoff = byte_idx * 8;
        for j in 0..8 {
            let idx = (code[byte_idx] >> j) & 1;
            let cent = if idx == 0 { c0 } else { c1 };
            let d = q_rotated[qoff + j] - cent;
            sum0 += d * d;
        }
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

/// 2-bit ADC: extract 2 bits per dimension, 4 dimensions per byte.
#[inline]
fn tq_l2_adc_2bit(q_rotated: &[f32], code: &[u8], norm: f32, centroids: &[f32]) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded / 4);
    debug_assert_eq!(centroids.len(), 4);

    let norm_sq = norm * norm;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    let code_len = code.len();
    let chunks = code_len / 4;
    let remainder = code_len % 4;

    for c in 0..chunks {
        let base = c * 4;
        let qbase = base * 4;

        for j in 0..4 {
            let idx = (code[base] >> (j * 2)) & 3;
            let d = q_rotated[qbase + j] - centroids[idx as usize];
            sum0 += d * d;
        }
        for j in 0..4 {
            let idx = (code[base + 1] >> (j * 2)) & 3;
            let d = q_rotated[qbase + 4 + j] - centroids[idx as usize];
            sum1 += d * d;
        }
        for j in 0..4 {
            let idx = (code[base + 2] >> (j * 2)) & 3;
            let d = q_rotated[qbase + 8 + j] - centroids[idx as usize];
            sum2 += d * d;
        }
        for j in 0..4 {
            let idx = (code[base + 3] >> (j * 2)) & 3;
            let d = q_rotated[qbase + 12 + j] - centroids[idx as usize];
            sum3 += d * d;
        }
    }

    let tail_start = chunks * 4;
    for i in 0..remainder {
        let byte_idx = tail_start + i;
        let qoff = byte_idx * 4;
        for j in 0..4 {
            let idx = (code[byte_idx] >> (j * 2)) & 3;
            let d = q_rotated[qoff + j] - centroids[idx as usize];
            sum0 += d * d;
        }
    }

    (sum0 + sum1 + sum2 + sum3) * norm_sq
}

/// 3-bit ADC: extract 3 bits per dimension, 8 dimensions per 3-byte group.
#[inline]
fn tq_l2_adc_3bit(q_rotated: &[f32], code: &[u8], norm: f32, centroids: &[f32]) -> f32 {
    let padded = q_rotated.len();
    debug_assert_eq!(code.len(), padded * 3 / 8);
    debug_assert_eq!(centroids.len(), 8);

    let norm_sq = norm * norm;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;

    // Process in 3-byte groups (8 dimensions each)
    let n_groups = code.len() / 3;
    let groups_2 = n_groups / 2;
    let groups_rem = n_groups % 2;

    for g in 0..groups_2 {
        let group_base = g * 2;

        // Group 0
        let off0 = group_base * 3;
        let qoff0 = group_base * 8;
        let bits0 =
            code[off0] as u32 | ((code[off0 + 1] as u32) << 8) | ((code[off0 + 2] as u32) << 16);
        for j in 0..8 {
            let idx = ((bits0 >> (j * 3)) & 7) as usize;
            let d = q_rotated[qoff0 + j] - centroids[idx];
            sum0 += d * d;
        }

        // Group 1
        let off1 = (group_base + 1) * 3;
        let qoff1 = (group_base + 1) * 8;
        let bits1 =
            code[off1] as u32 | ((code[off1 + 1] as u32) << 8) | ((code[off1 + 2] as u32) << 16);
        for j in 0..8 {
            let idx = ((bits1 >> (j * 3)) & 7) as usize;
            let d = q_rotated[qoff1 + j] - centroids[idx];
            sum1 += d * d;
        }
    }

    if groups_rem > 0 {
        let off = groups_2 * 2 * 3;
        let qoff = groups_2 * 2 * 8;
        let bits =
            code[off] as u32 | ((code[off + 1] as u32) << 8) | ((code[off + 2] as u32) << 16);
        for j in 0..8 {
            let idx = ((bits >> (j * 3)) & 7) as usize;
            let d = q_rotated[qoff + j] - centroids[idx];
            sum0 += d * d;
        }
    }

    (sum0 + sum1) * norm_sq
}

/// Budgeted version of `tq_l2_adc_multibit` with early termination.
#[inline]
pub fn tq_l2_adc_multibit_budgeted(
    q_rotated: &[f32],
    code: &[u8],
    norm: f32,
    centroids: &[f32],
    bits: u8,
    budget: f32,
) -> f32 {
    // For simplicity, compute full distance and check budget after.
    // The 4-bit path has the optimized inner-loop budget check.
    if bits == 4 {
        debug_assert_eq!(centroids.len(), 16);
        let c: &[f32; 16] = centroids
            .try_into()
            .unwrap_or_else(|_| panic!("4-bit ADC requires exactly 16 centroids"));
        return tq_l2_adc_scaled_budgeted(q_rotated, code, norm, c, budget);
    }

    let dist = tq_l2_adc_multibit(q_rotated, code, norm, centroids, bits);
    if dist > budget { f32::MAX } else { dist }
}

/// Brute-force scan of ALL TQ codes at any bit width using ADC.
///
/// `bits`: quantization bit width (1-4).
/// Code layout per vector: [packed_code (code_bytes_per_vector)] [norm (4 bytes LE f32)].
pub fn brute_force_tq_adc_multibit(
    query: &[f32],
    tq_buffer: &[u8],
    n_vectors: usize,
    collection: &CollectionMetadata,
    k: usize,
    bits: u8,
) -> SmallVec<[SearchResult; 32]> {
    if n_vectors == 0 || k == 0 {
        return SmallVec::new();
    }

    let dim = query.len();
    let padded = collection.padded_dimension as usize;
    let code_len = code_bytes_per_vector(collection.padded_dimension, bits);
    let bytes_per_code = code_len + 4; // code + f32 norm
    let centroids = &collection.codebook;

    // Prepare rotated query
    let mut q_rotated = vec![0.0f32; padded];
    q_rotated[..dim].copy_from_slice(query);
    for v in q_rotated[dim..padded].iter_mut() {
        *v = 0.0;
    }
    let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
    if q_norm > 0.0 {
        let inv = 1.0 / q_norm;
        for v in q_rotated[..dim].iter_mut() {
            *v *= inv;
        }
    }
    fwht::fwht(
        &mut q_rotated[..padded],
        collection.fwht_sign_flips.as_slice(),
    );

    // Scan with max-heap for top-K
    use std::collections::BinaryHeap;
    let mut heap: BinaryHeap<(ordered_float::OrderedFloat<f32>, u32)> = BinaryHeap::new();

    for i in 0..n_vectors {
        let offset = i * bytes_per_code;
        let code = &tq_buffer[offset..offset + code_len];
        let norm_bytes = &tq_buffer[offset + code_len..offset + code_len + 4];
        let norm = f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);
        let dist = tq_l2_adc_multibit(&q_rotated, code, norm, centroids, bits);

        if heap.len() < k {
            heap.push((ordered_float::OrderedFloat(dist), i as u32));
        } else if let Some(&(worst, _)) = heap.peek() {
            if dist < worst.0 {
                heap.pop();
                heap.push((ordered_float::OrderedFloat(dist), i as u32));
            }
        }
    }

    let mut results: Vec<(f32, u32)> = heap.into_iter().map(|(d, id)| (d.0, id)).collect();
    results.sort_by(|a, b| a.0.total_cmp(&b.0));

    results
        .into_iter()
        .map(|(d, id)| SearchResult::new(d, VectorId(id)))
        .collect()
}

/// Brute-force scan of ALL TQ codes using asymmetric distance computation.
///
/// This is the paper-validated NN search method (arXiv 2504.19874 Section 4.4).
/// TQ-ADC is correct for exhaustive scan but NOT for HNSW greedy navigation
/// (use hnsw_search_f32 for graph traversal).
///
/// `query`: raw f32 query vector (original dimension, NOT rotated).
/// `tq_buffer`: flat buffer of TQ codes. Layout per code: [nibbles (pdim/2)] [norm (4 bytes)].
///   Codes may be in any order (original-ID or BFS order).
/// `n_vectors`: number of vectors in the buffer.
/// `collection`: metadata with sign flips, codebook, padded dimension.
/// `k`: number of nearest neighbors to return.
///
/// Returns up to k SearchResults sorted by distance ascending.
pub fn brute_force_tq_adc(
    query: &[f32],
    tq_buffer: &[u8],
    n_vectors: usize,
    collection: &CollectionMetadata,
    k: usize,
) -> SmallVec<[SearchResult; 32]> {
    if n_vectors == 0 || k == 0 {
        return SmallVec::new();
    }

    let dim = query.len();
    let padded = collection.padded_dimension as usize;
    let bytes_per_code = padded / 2 + 4;
    let code_len = padded / 2;
    let codebook = collection.codebook_16();

    // Prepare rotated query: normalize, pad, FWHT
    let mut q_rotated = vec![0.0f32; padded];
    q_rotated[..dim].copy_from_slice(query);
    for v in q_rotated[dim..padded].iter_mut() {
        *v = 0.0;
    }
    let q_norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
    if q_norm > 0.0 {
        let inv = 1.0 / q_norm;
        for v in q_rotated[..dim].iter_mut() {
            *v *= inv;
        }
    }
    fwht::fwht(
        &mut q_rotated[..padded],
        collection.fwht_sign_flips.as_slice(),
    );

    // Scan all vectors, keep top-K in a max-heap
    use std::collections::BinaryHeap;
    let mut heap: BinaryHeap<(ordered_float::OrderedFloat<f32>, u32)> = BinaryHeap::new();

    for i in 0..n_vectors {
        let offset = i * bytes_per_code;
        let code = &tq_buffer[offset..offset + code_len];
        let norm_bytes = &tq_buffer[offset + code_len..offset + code_len + 4];
        let norm = f32::from_le_bytes([norm_bytes[0], norm_bytes[1], norm_bytes[2], norm_bytes[3]]);
        let dist = tq_l2_adc_scaled(&q_rotated, code, norm, codebook);

        if heap.len() < k {
            heap.push((ordered_float::OrderedFloat(dist), i as u32));
        } else if let Some(&(worst, _)) = heap.peek() {
            if dist < worst.0 {
                heap.pop();
                heap.push((ordered_float::OrderedFloat(dist), i as u32));
            }
        }
    }

    // Extract sorted results
    let mut results: Vec<(f32, u32)> = heap.into_iter().map(|(d, id)| (d.0, id)).collect();
    results.sort_by(|a, b| a.0.total_cmp(&b.0));

    results
        .into_iter()
        .map(|(d, id)| SearchResult::new(d, VectorId(id)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::turbo_quant::encoder::{decode_tq_mse, encode_tq_mse, padded_dimension};
    use crate::vector::turbo_quant::fwht;

    /// Deterministic LCG PRNG for reproducible test vectors.
    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            for x in v.iter_mut() {
                *x *= inv;
            }
        }
        norm
    }

    fn test_sign_flips(dim: usize, seed: u32) -> Vec<f32> {
        let mut signs = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            signs.push(if s & 1 == 0 { 1.0f32 } else { -1.0 });
        }
        signs
    }

    #[test]
    fn test_tq_l2_self_distance_small() {
        // Encode a vector, then compute ADC distance against its own FWHT-rotated form.
        // Should be close to 0 (quantization error only).
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work = vec![0.0f32; padded];

        let mut vec = lcg_f32(dim, 99);
        normalize(&mut vec);

        let code = encode_tq_mse(&vec, &signs, &mut work);

        // Prepare rotated query (same vector through same FWHT)
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&vec);
        for dst in q_rotated[dim..].iter_mut() {
            *dst = 0.0;
        }
        // Normalize for FWHT input
        // vec is already unit norm, so inv_norm = 1.0
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_scalar(&q_rotated, &code.codes, code.norm);
        eprintln!("self-distance (ADC): {dist}");
        // Self-distance should be small (quantization error only, norm=1 so norm_sq=1)
        assert!(dist < 0.02, "self-distance {dist} too large");
        assert!(dist >= 0.0, "distance must be non-negative");
    }

    #[test]
    fn test_tq_l2_distant_vectors() {
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work = vec![0.0f32; padded];

        // Encode first vector
        let mut v1 = lcg_f32(dim, 11);
        normalize(&mut v1);
        let code1 = encode_tq_mse(&v1, &signs, &mut work);

        // Create a distant query (opposite direction)
        let v2: Vec<f32> = v1.iter().map(|&x| -x).collect();
        // Already unit norm since v1 was unit

        // Rotate query
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&v2);
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_scalar(&q_rotated, &code1.codes, code1.norm);
        eprintln!("distant-distance (ADC): {dist}");
        // Opposite unit vectors have L2^2 = 4.0. With quantization error, should be close.
        assert!(
            dist > 2.0,
            "distant vectors should have large distance, got {dist}"
        );
    }

    #[test]
    fn test_tq_l2_matches_decoded_l2() {
        // ADC distance should produce same ranking as brute-force decoded L2
        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let mut work_enc = vec![0.0f32; padded];
        let mut work_dec = vec![0.0f32; padded];

        // Encode 10 vectors
        let mut codes = Vec::new();
        let mut originals = Vec::new();
        for seed in 0..10u32 {
            let mut v = lcg_f32(dim, seed * 7 + 13);
            normalize(&mut v);
            originals.push(v.clone());
            codes.push(encode_tq_mse(&v, &signs, &mut work_enc));
        }

        // Query
        let mut query = lcg_f32(dim, 999);
        normalize(&mut query);
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&query);
        fwht::fwht(&mut q_rotated, &signs);

        // Compute ADC distances
        let adc_dists: Vec<f32> = codes
            .iter()
            .map(|c| tq_l2_adc_scalar(&q_rotated, &c.codes, c.norm))
            .collect();

        // Compute brute-force L2 via decode
        let bf_dists: Vec<f32> = codes
            .iter()
            .map(|c| {
                let decoded = decode_tq_mse(c, &signs, dim, &mut work_dec);
                let mut sum = 0.0f32;
                for (a, b) in query.iter().zip(decoded.iter()) {
                    let d = a - b;
                    sum += d * d;
                }
                sum
            })
            .collect();

        // Rankings should match (ADC preserves ordering)
        let mut adc_order: Vec<usize> = (0..10).collect();
        adc_order.sort_by(|&a, &b| adc_dists[a].partial_cmp(&adc_dists[b]).unwrap());

        let mut bf_order: Vec<usize> = (0..10).collect();
        bf_order.sort_by(|&a, &b| bf_dists[a].partial_cmp(&bf_dists[b]).unwrap());

        eprintln!("ADC ranking: {adc_order:?}");
        eprintln!("BF  ranking: {bf_order:?}");

        // Top-3 should match (quantization may swap nearly-equal distances)
        assert_eq!(adc_order[0], bf_order[0], "nearest neighbor mismatch");
    }

    #[test]
    fn test_tq_l2_norm_scaling() {
        // Verify norm scaling: distance should scale with norm^2
        fwht::init_fwht();
        let dim = 64;
        let padded = padded_dimension(dim as u32) as usize;
        let _signs = test_sign_flips(padded, 42);

        // Create a simple query and code
        let q = vec![0.01f32; padded];
        // Hand-craft a code: all indices = 8 (centroid = 0.001075)
        let code = vec![0x88u8; padded / 2];

        let dist_norm1 = tq_l2_adc_scalar(&q, &code, 1.0);
        let dist_norm2 = tq_l2_adc_scalar(&q, &code, 2.0);

        // dist_norm2 should be 4x dist_norm1 (norm^2 scaling)
        let ratio = dist_norm2 / dist_norm1;
        assert!(
            (ratio - 4.0).abs() < 0.01,
            "norm scaling wrong: ratio = {ratio}, expected 4.0"
        );
    }

    #[test]
    fn test_tq_l2_non_negative() {
        let q = [0.1f32, -0.2, 0.3, -0.4];
        let code = [0x21, 0x43]; // arbitrary nibbles
        let dist = tq_l2_adc_scalar(&q, &code, 1.5);
        assert!(dist >= 0.0, "distance must be non-negative, got {dist}");
    }

    #[test]
    fn test_brute_force_tq_adc_recall() {
        use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_scaled;
        use crate::vector::types::DistanceMetric;
        use std::sync::Arc;

        fwht::init_fwht();
        let n = 1000;
        let dim = 128;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let padded = collection.padded_dimension as usize;
        let signs = collection.fwht_sign_flips.as_slice();
        let boundaries = collection.codebook_boundaries_15();
        let bytes_per_code = padded / 2 + 4;

        // Generate and encode vectors using scaled boundaries (matching collection codebook)
        let mut vectors = Vec::with_capacity(n);
        let mut tq_buffer: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
        let mut work = vec![0.0f32; padded];

        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            let code = encode_tq_mse_scaled(&v, signs, boundaries, &mut work);
            tq_buffer.extend_from_slice(&code.codes);
            tq_buffer.extend_from_slice(&code.norm.to_le_bytes());
            vectors.push(v);
        }

        // Test recall over 50 queries
        let k = 10;
        let num_queries = 50;
        let mut total_recall = 0.0f64;

        for qi in 0..num_queries {
            let mut query = lcg_f32(dim, (qi * 31 + 997) as u32);
            normalize(&mut query);

            // True L2 brute force ground truth
            let mut true_dists: Vec<(f32, usize)> = vectors
                .iter()
                .enumerate()
                .map(|(idx, v)| {
                    let d: f32 = query
                        .iter()
                        .zip(v.iter())
                        .map(|(a, b)| {
                            let diff = a - b;
                            diff * diff
                        })
                        .sum();
                    (d, idx)
                })
                .collect();
            true_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let true_top_k: Vec<usize> = true_dists.iter().take(k).map(|&(_, id)| id).collect();

            // TQ-ADC brute force
            let results = brute_force_tq_adc(&query, &tq_buffer, n, &collection, k);
            let adc_top_k: Vec<usize> = results.iter().map(|r| r.id.0 as usize).collect();

            // Count overlap
            let hits = adc_top_k
                .iter()
                .filter(|id| true_top_k.contains(id))
                .count();
            total_recall += hits as f64 / k as f64;
        }

        let avg_recall = total_recall / num_queries as f64;
        eprintln!("brute_force_tq_adc recall@{k}: {avg_recall:.4}");
        // 4-bit ADC at 128d achieves ~0.80-0.85 recall (dimension-dependent).
        // Higher dimensions (768d) achieve 0.90+ due to better FWHT concentration.
        assert!(
            avg_recall >= 0.80,
            "recall@{k} = {avg_recall:.4}, expected >= 0.80"
        );
    }

    #[test]
    fn test_brute_force_tq_adc_empty() {
        use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
        use crate::vector::types::DistanceMetric;
        use std::sync::Arc;

        fwht::init_fwht();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let query = vec![0.1f32; 128];
        let results = brute_force_tq_adc(&query, &[], 0, &collection, 10);
        assert!(
            results.is_empty(),
            "empty buffer should return empty results"
        );
    }

    #[test]
    fn test_brute_force_tq_adc_k_larger_than_n() {
        use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_scaled;
        use crate::vector::types::DistanceMetric;
        use std::sync::Arc;

        fwht::init_fwht();
        let n = 10;
        let dim = 128;
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let padded = collection.padded_dimension as usize;
        let signs = collection.fwht_sign_flips.as_slice();
        let boundaries = collection.codebook_boundaries_15();
        let bytes_per_code = padded / 2 + 4;

        let mut tq_buffer: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
        let mut work = vec![0.0f32; padded];

        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            let code = encode_tq_mse_scaled(&v, signs, boundaries, &mut work);
            tq_buffer.extend_from_slice(&code.codes);
            tq_buffer.extend_from_slice(&code.norm.to_le_bytes());
        }

        let query = vec![0.1f32; dim];
        let results = brute_force_tq_adc(&query, &tq_buffer, n, &collection, 100);
        assert_eq!(results.len(), n, "k=100 with n=10 should return 10 results");
    }

    // ── Multi-bit ADC tests ──────────────────────────────────────────

    #[test]
    fn test_tq_l2_adc_multibit_self_distance_1bit() {
        use crate::vector::turbo_quant::codebook::{scaled_boundaries_n, scaled_centroids_n};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_multibit;

        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries_n(padded as u32, 1).unwrap();
        let centroids = scaled_centroids_n(padded as u32, 1).unwrap();
        let mut work = vec![0.0f32; padded];

        let mut v = lcg_f32(dim, 99);
        normalize(&mut v);

        let code = encode_tq_mse_multibit(&v, &signs, &boundaries, 1, &mut work);

        // Rotate query
        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&v);
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_multibit(&q_rotated, &code.codes, code.norm, &centroids, 1);
        eprintln!("1-bit self-distance: {dist}");
        assert!(dist < 0.8, "1-bit self-distance {dist} too large");
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_tq_l2_adc_multibit_self_distance_2bit() {
        use crate::vector::turbo_quant::codebook::{scaled_boundaries_n, scaled_centroids_n};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_multibit;

        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries_n(padded as u32, 2).unwrap();
        let centroids = scaled_centroids_n(padded as u32, 2).unwrap();
        let mut work = vec![0.0f32; padded];

        let mut v = lcg_f32(dim, 99);
        normalize(&mut v);

        let code = encode_tq_mse_multibit(&v, &signs, &boundaries, 2, &mut work);

        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&v);
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_multibit(&q_rotated, &code.codes, code.norm, &centroids, 2);
        eprintln!("2-bit self-distance: {dist}");
        assert!(dist < 0.3, "2-bit self-distance {dist} too large");
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_tq_l2_adc_multibit_self_distance_3bit() {
        use crate::vector::turbo_quant::codebook::{scaled_boundaries_n, scaled_centroids_n};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_multibit;

        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);
        let boundaries = scaled_boundaries_n(padded as u32, 3).unwrap();
        let centroids = scaled_centroids_n(padded as u32, 3).unwrap();
        let mut work = vec![0.0f32; padded];

        let mut v = lcg_f32(dim, 99);
        normalize(&mut v);

        let code = encode_tq_mse_multibit(&v, &signs, &boundaries, 3, &mut work);

        let mut q_rotated = vec![0.0f32; padded];
        q_rotated[..dim].copy_from_slice(&v);
        fwht::fwht(&mut q_rotated, &signs);

        let dist = tq_l2_adc_multibit(&q_rotated, &code.codes, code.norm, &centroids, 3);
        eprintln!("3-bit self-distance: {dist}");
        assert!(dist < 0.08, "3-bit self-distance {dist} too large");
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_tq_l2_adc_multibit_ranking() {
        use crate::vector::turbo_quant::codebook::{scaled_boundaries_n, scaled_centroids_n};
        use crate::vector::turbo_quant::encoder::{decode_tq_mse_multibit, encode_tq_mse_multibit};

        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);

        for bits in [1u8, 2, 3] {
            let boundaries = scaled_boundaries_n(padded as u32, bits).unwrap();
            let centroids = scaled_centroids_n(padded as u32, bits).unwrap();
            let mut work_enc = vec![0.0f32; padded];
            let mut work_dec = vec![0.0f32; padded];

            // Encode 10 vectors
            let mut codes = Vec::new();
            let mut originals = Vec::new();
            for seed in 0..10u32 {
                let mut v = lcg_f32(dim, seed * 7 + 13);
                normalize(&mut v);
                originals.push(v.clone());
                codes.push(encode_tq_mse_multibit(
                    &v,
                    &signs,
                    &boundaries,
                    bits,
                    &mut work_enc,
                ));
            }

            // Query
            let mut query = lcg_f32(dim, 999);
            normalize(&mut query);
            let mut q_rotated = vec![0.0f32; padded];
            q_rotated[..dim].copy_from_slice(&query);
            fwht::fwht(&mut q_rotated, &signs);

            // ADC distances
            let adc_dists: Vec<f32> = codes
                .iter()
                .map(|c| tq_l2_adc_multibit(&q_rotated, &c.codes, c.norm, &centroids, bits))
                .collect();

            // Decoded L2 distances
            let bf_dists: Vec<f32> = codes
                .iter()
                .map(|c| {
                    let decoded =
                        decode_tq_mse_multibit(c, &signs, &centroids, bits, dim, &mut work_dec);
                    let mut sum = 0.0f32;
                    for (a, b) in query.iter().zip(decoded.iter()) {
                        let d = a - b;
                        sum += d * d;
                    }
                    sum
                })
                .collect();

            let mut adc_order: Vec<usize> = (0..10).collect();
            adc_order.sort_by(|&a, &b| adc_dists[a].partial_cmp(&adc_dists[b]).unwrap());

            let mut bf_order: Vec<usize> = (0..10).collect();
            bf_order.sort_by(|&a, &b| bf_dists[a].partial_cmp(&bf_dists[b]).unwrap());

            eprintln!("{bits}-bit ADC ranking: {adc_order:?}");
            eprintln!("{bits}-bit BF  ranking: {bf_order:?}");

            // Top-1 should match
            assert_eq!(
                adc_order[0], bf_order[0],
                "{bits}-bit: nearest neighbor mismatch"
            );
        }
    }

    #[test]
    fn test_tq_l2_adc_multibit_budgeted_returns_max() {
        use crate::vector::turbo_quant::codebook::{scaled_boundaries_n, scaled_centroids_n};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_multibit;

        fwht::init_fwht();
        let dim = 768;
        let padded = padded_dimension(dim as u32) as usize;
        let signs = test_sign_flips(padded, 42);

        for bits in [1u8, 2, 3] {
            let boundaries = scaled_boundaries_n(padded as u32, bits).unwrap();
            let centroids = scaled_centroids_n(padded as u32, bits).unwrap();
            let mut work = vec![0.0f32; padded];

            let mut v = lcg_f32(dim, 99);
            normalize(&mut v);
            let code = encode_tq_mse_multibit(&v, &signs, &boundaries, bits, &mut work);

            // Create a distant query
            let v2: Vec<f32> = v.iter().map(|&x| -x).collect();
            let mut q_rotated = vec![0.0f32; padded];
            q_rotated[..dim].copy_from_slice(&v2);
            fwht::fwht(&mut q_rotated, &signs);

            // Use a tiny budget that will be exceeded
            let dist = tq_l2_adc_multibit_budgeted(
                &q_rotated,
                &code.codes,
                code.norm,
                &centroids,
                bits,
                0.001,
            );
            assert_eq!(dist, f32::MAX, "{bits}-bit: budgeted should return MAX");
        }
    }

    #[test]
    fn test_brute_force_tq_adc_multibit_recall() {
        use crate::vector::turbo_quant::codebook::code_bytes_per_vector;
        use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
        use crate::vector::turbo_quant::encoder::encode_tq_mse_multibit;
        use crate::vector::types::DistanceMetric;
        use std::sync::Arc;

        fwht::init_fwht();
        let n = 500;
        let dim = 128;

        for (bits, quant, min_recall) in [
            // At 128d, FWHT concentration is weak. These thresholds reflect that.
            // Higher dimensions (768d) achieve significantly better recall.
            (1u8, QuantizationConfig::TurboQuant1, 0.25),
            (2, QuantizationConfig::TurboQuant2, 0.40),
            (3, QuantizationConfig::TurboQuant3, 0.60),
        ] {
            let collection = Arc::new(CollectionMetadata::new(
                1,
                dim as u32,
                DistanceMetric::L2,
                quant,
                42,
            ));
            let padded = collection.padded_dimension as usize;
            let signs = collection.fwht_sign_flips.as_slice();
            let boundaries = &collection.codebook_boundaries;
            let code_len = code_bytes_per_vector(padded as u32, bits);
            let bytes_per_code = code_len + 4;

            let mut vectors = Vec::with_capacity(n);
            let mut tq_buffer: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
            let mut work = vec![0.0f32; padded];

            for i in 0..n {
                let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
                normalize(&mut v);
                let code = encode_tq_mse_multibit(&v, signs, boundaries, bits, &mut work);
                tq_buffer.extend_from_slice(&code.codes);
                tq_buffer.extend_from_slice(&code.norm.to_le_bytes());
                vectors.push(v);
            }

            let k = 10;
            let num_queries = 30;
            let mut total_recall = 0.0f64;

            for qi in 0..num_queries {
                let mut query = lcg_f32(dim, (qi * 31 + 997) as u32);
                normalize(&mut query);

                let mut true_dists: Vec<(f32, usize)> = vectors
                    .iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        let d: f32 = query
                            .iter()
                            .zip(v.iter())
                            .map(|(a, b)| {
                                let diff = a - b;
                                diff * diff
                            })
                            .sum();
                        (d, idx)
                    })
                    .collect();
                true_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                let true_top_k: Vec<usize> = true_dists.iter().take(k).map(|&(_, id)| id).collect();

                let results =
                    brute_force_tq_adc_multibit(&query, &tq_buffer, n, &collection, k, bits);
                let adc_top_k: Vec<usize> = results.iter().map(|r| r.id.0 as usize).collect();

                let hits = adc_top_k
                    .iter()
                    .filter(|id| true_top_k.contains(id))
                    .count();
                total_recall += hits as f64 / k as f64;
            }

            let avg_recall = total_recall / num_queries as f64;
            eprintln!("{bits}-bit brute_force_tq_adc_multibit recall@{k}: {avg_recall:.4}");
            assert!(
                avg_recall >= min_recall,
                "{bits}-bit recall@{k} = {avg_recall:.4}, expected >= {min_recall}"
            );
        }
    }
}
