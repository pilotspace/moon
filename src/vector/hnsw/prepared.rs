//! Per-query TQ search state, built once and shared by every graph segment a
//! query visits (moon#1196).
//!
//! All immutable segments of one index share one `CollectionMetadata` (same
//! FWHT sign flips, codebook, sub-centroid table), yet each per-segment
//! `hnsw_search_filtered` call used to re-normalize and FWHT-rotate the query
//! and rebuild its ADC LUT through a scalar `push` loop — `padded × 32`
//! floats in sub-centroid mode (64 KB at 384d, 128 KB at 768d) — and every
//! exact rerank allocated its own unit-query `Vec`. With the default compact
//! threshold, double-digit segment counts are normal, and the pool fans the
//! same query out to worker threads that each redid the work.
//!
//! [`PreparedTqQuery`] does it once per query: the rotated query and its
//! norm eagerly, the 16- and 32-level LUTs lazily (a WARM segment or a
//! pre-v2 reload needs the 16-level one, a fresh HOT segment the 32-level
//! one — a query touching only one kind builds only that one), and the unit
//! query for the unit-sphere exact rerank. It is `Sync` (`OnceLock`) so the
//! search pool's jobs share one `Arc` of it.
//!
//! **Identity.** Every buffer is produced by the SAME routines the
//! non-prepared path uses ([`rotate_query_into`], [`fill_lut16`],
//! [`fill_lut32`], [`unit_query_into`]), so a prepared search is
//! bit-identical to an unprepared one. A segment whose collection does not
//! [`PreparedTqQuery::matches`] (different sign flips/codebook) ignores the
//! prepared state and builds its own.

use std::sync::{Arc, OnceLock};

use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::fwht;
use crate::vector::types::DistanceMetric;

#[cfg(test)]
thread_local! {
    /// Test-only counter of ADC LUT builds on this thread (prepared or
    /// per-segment) — lets tests pin "one LUT per query, not per segment".
    pub(crate) static LUT_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    /// Test-only counter of [`PreparedTqQuery`] constructions on this thread
    /// — each one heap-allocates the rotated query (+ the unit query, + the
    /// LUTs it builds lazily), so this pins when a query pays for them.
    pub(crate) static PREPARED_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[inline]
fn note_lut_build() {
    #[cfg(test)]
    LUT_BUILDS.with(|c| c.set(c.get() + 1));
}

/// Copy `query` into `out` (padded), zero the padding, normalize to unit
/// length and apply the collection's randomized FWHT. Returns the query norm
/// BEFORE normalization. `out.len()` must be the collection's padded dim.
///
/// This is verbatim the rotation `hnsw_search_filtered` has always done
/// in-line; both paths call it so they cannot drift.
pub(crate) fn rotate_query_into(query: &[f32], sign_flips: &[f32], out: &mut [f32]) -> f32 {
    let q_norm = normalize_query_into(query, out);
    fwht::fwht(out, sign_flips);
    q_norm
}

/// The first half of [`rotate_query_into`] (copy, zero-pad, normalize) —
/// the SQ8 beam's query prep, which is axis-aligned and never rotated.
pub(crate) fn normalize_query_into(query: &[f32], out: &mut [f32]) -> f32 {
    let dim = query.len();
    let padded = out.len();
    out[..dim].copy_from_slice(query);
    for v in out[dim..padded].iter_mut() {
        *v = 0.0;
    }
    // Compute query norm BEFORE normalization (needed for distance correction)
    let mut q_norm_sq = 0.0f32;
    for &v in &out[..dim] {
        q_norm_sq += v * v;
    }
    let q_norm = q_norm_sq.sqrt();
    // Normalize query to unit length (TQ operates on unit sphere)
    if q_norm > 0.0 {
        let inv = 1.0 / q_norm;
        for v in out[..dim].iter_mut() {
            *v *= inv;
        }
    }
    q_norm
}

/// 16-entry-per-coordinate ADC LUT: `out[j*16 + c] = (q_rot[j] - cb[c])²`.
/// A fixed-width slice fill the compiler vectorizes (was a scalar `push`
/// loop); same arithmetic per entry, so the same bits.
pub(crate) fn fill_lut16(q_rot: &[f32], codebook: &[f32; 16], out: &mut [f32]) {
    note_lut_build();
    for (row, &q) in out.chunks_exact_mut(16).zip(q_rot.iter()) {
        for (o, &c) in row.iter_mut().zip(codebook.iter()) {
            let d = q - c;
            *o = d * d;
        }
    }
}

/// 32-entry sub-centroid LUT: `out[j*32 + e] = (q_rot[j] - table[e])²`.
pub(crate) fn fill_lut32(q_rot: &[f32], table: &[f32], out: &mut [f32]) {
    note_lut_build();
    let Some(table) = table.get(..32) else {
        return;
    };
    for (row, &q) in out.chunks_exact_mut(32).zip(q_rot.iter()) {
        for (o, &c) in row.iter_mut().zip(table.iter()) {
            let d = q - c;
            *o = d * d;
        }
    }
}

/// Unit-normalize `query` into `out` (cleared first) for the unit-sphere
/// exact-rerank / exact-beam distances; a zero query is copied as-is. The
/// formula every rerank site used in-line.
pub(crate) fn unit_query_into<E: Extend<f32>>(query: &[f32], out: &mut E) {
    let norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        let inv = 1.0 / norm;
        out.extend(query.iter().map(|x| x * inv));
    } else {
        out.extend(query.iter().copied());
    }
}

/// Squared RMS magnitude, in f16 subnormal steps (2^-24 ≈ 5.96e-8), below
/// which an L2 row's f16 encoding is coarser than the TQ ADC estimate.
///
/// f16 rounding error is uniform in ±½ step: RMS ≈ step/√12 ≈ 1.7e-8 per
/// component, whatever the component's size. TQ4's error is relative (about
/// 0.1 per rotated coordinate) — it quantizes the unit direction and keeps
/// the norm. The two meet at a component RMS of ≈ 1.7e-7 ≈ 3 steps, which is
/// where a recall sweep (32d and 128d, 5e-8 … 1) crosses over: at 1e-7 the
/// f16 rerank answered R@10 0.767 against ADC's 0.848, at 3e-7 0.917 against
/// 0.848 (moon#1242).
const MIN_L2_RERANK_RMS_STEPS_SQ: u64 = 9;

/// Whether an L2 row's f16 encoding is too coarse to improve on the ADC
/// estimate: every component is subnormal (zero exponent bits) and the row's
/// RMS is below 3 subnormal steps ([`MIN_L2_RERANK_RMS_STEPS_SQ`]).
///
/// Exact and cheap: integer arithmetic on the stored bits (the same answer on
/// every SIMD tier), and it returns at the first component with a nonzero
/// exponent — component 0 for a row at any ordinary scale. Only rows that are
/// wholly subnormal pay a full pass.
#[inline]
pub(crate) fn f16_row_below_rerank_precision(row: &[u16]) -> bool {
    let mut steps_sq: u64 = 0;
    for &h in row {
        if h & 0x7C00 != 0 {
            return false; // a normal, infinite or NaN component
        }
        let m = u64::from(h & 0x03FF);
        steps_sq += m * m;
    }
    steps_sq < MIN_L2_RERANK_RMS_STEPS_SQ.saturating_mul(row.len() as u64)
}

/// The exact-rerank distance of one f16 row (HQ-1), in the convention every
/// rerank shares so cross-segment merges stay consistent: true squared L2 for
/// L2 (`q` is the raw query), `2 − 2·cos` for the unit-sphere metrics (`q` is
/// the unit query, see [`unit_query_into`]; f16 rounding is clamped into
/// `[-1, 1]`). `None` — the caller keeps its ADC estimate:
/// - for a zero row under a unit-sphere metric (its normalized form is
///   undefined);
/// - for a non-finite result: a component beyond the f16 range (±65,504) was
///   stored as ±inf, and an `inf` / NaN distance would erase that row's rank
///   (moon#1226);
/// - for an L2 row deep in f16's subnormal range
///   ([`f16_row_below_rerank_precision`]): its f16 form keeps a bit or two
///   per component while the ADC estimate is scale-invariant, so the "exact"
///   distance would lose recall (moon#1242). Unit-sphere metrics are not
///   affected (measured: the rerank stays well above ADC at every scale), and
///   their ADC estimate is not on the `2 − 2·cos` scale, so it is never mixed
///   in for them on this account.
#[inline]
pub(crate) fn exact_f16_distance(
    kernels: &crate::vector::distance::DistanceTable,
    q: &[f32],
    row: &[u16],
    is_l2: bool,
) -> Option<f32> {
    let d = if is_l2 {
        if f16_row_below_rerank_precision(row) {
            return None;
        }
        (kernels.f16_l2)(q, row)
    } else {
        let (dot, xsq) = (kernels.f16_dot_normsq)(q, row);
        if xsq <= 0.0 {
            return None;
        }
        let cos = (dot / xsq.sqrt()).clamp(-1.0, 1.0);
        2.0 - 2.0 * cos
    };
    d.is_finite().then_some(d)
}

/// See the module docs.
pub struct PreparedTqQuery {
    collection: Arc<CollectionMetadata>,
    dim: usize,
    q_rotated: Vec<f32>,
    q_norm: f32,
    /// Unit query for the unit-sphere metrics; empty for L2.
    q_unit: Vec<f32>,
    lut16: OnceLock<Vec<f32>>,
    lut32: OnceLock<Option<Vec<f32>>>,
}

impl PreparedTqQuery {
    /// Prepare `query` for `collection`. `None` for SQ8 (axis-aligned ADC,
    /// no rotation/LUT) or a dimension mismatch (the per-segment path then
    /// reproduces whatever it does today).
    pub fn new(query: &[f32], collection: &Arc<CollectionMetadata>) -> Option<Self> {
        if collection.quantization == QuantizationConfig::Sq8
            || query.len() != collection.dimension as usize
        {
            return None;
        }
        let padded = collection.padded_dimension as usize;
        if padded < query.len() || collection.fwht_sign_flips.len() != padded {
            return None;
        }
        #[cfg(test)]
        PREPARED_BUILDS.with(|c| c.set(c.get() + 1));
        let mut q_rotated = vec![0.0f32; padded];
        let q_norm =
            rotate_query_into(query, collection.fwht_sign_flips.as_slice(), &mut q_rotated);
        let mut q_unit = Vec::new();
        if collection.metric != DistanceMetric::L2 {
            q_unit.reserve_exact(query.len());
            unit_query_into(query, &mut q_unit);
        }
        Some(Self {
            collection: Arc::clone(collection),
            dim: query.len(),
            q_rotated,
            q_norm,
            q_unit,
            lut16: OnceLock::new(),
            lut32: OnceLock::new(),
        })
    }

    /// Whether this state is valid for a segment built against `other`:
    /// the same `Arc`, or a collection with the same id, dimension, padded
    /// dimension, quantization and metadata checksum (which covers the sign
    /// flips and the codebook — everything the rotation and LUTs depend on).
    pub fn matches(&self, other: &CollectionMetadata) -> bool {
        let mine = &*self.collection;
        std::ptr::eq(mine, other)
            || (mine.metadata_checksum == other.metadata_checksum
                && mine.collection_id == other.collection_id
                && mine.dimension == other.dimension
                && mine.padded_dimension == other.padded_dimension
                && mine.quantization == other.quantization
                && mine.metric == other.metric)
    }

    /// Original (unpadded) query dimension.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// FWHT-rotated unit query (padded dimension).
    pub fn q_rotated(&self) -> &[f32] {
        &self.q_rotated
    }

    /// Query norm before normalization.
    pub fn q_norm(&self) -> f32 {
        self.q_norm
    }

    /// Unit query for the unit-sphere metrics (empty for L2).
    pub fn q_unit(&self) -> &[f32] {
        &self.q_unit
    }

    /// The 16-level ADC LUT (built on first use).
    pub fn lut16(&self) -> &[f32] {
        self.lut16.get_or_init(|| {
            let mut lut = vec![0.0f32; self.q_rotated.len() * 16];
            fill_lut16(&self.q_rotated, self.collection.codebook_16(), &mut lut);
            lut
        })
    }

    /// The 32-level sub-centroid LUT (built on first use); `None` when the
    /// collection has no sub-centroid table.
    pub fn lut32(&self) -> Option<&[f32]> {
        self.lut32
            .get_or_init(|| {
                let table = self.collection.sub_centroid_table.as_ref()?;
                let mut lut = vec![0.0f32; self.q_rotated.len() * 32];
                fill_lut32(&self.q_rotated, &table.table, &mut lut);
                Some(lut)
            })
            .as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::{exact_f16_distance, f16_row_below_rerank_precision};
    use crate::vector::f16::f32_to_f16;

    #[test]
    fn rows_below_three_subnormal_steps_rms_are_below_rerank_precision() {
        let dim = 32;
        assert!(f16_row_below_rerank_precision(&vec![0u16; dim]), "zero row");
        // Every component 2 steps (either sign): RMS 2 steps.
        let two: Vec<u16> = (0..dim).map(|i| 0x0002 | ((i as u16 & 1) << 15)).collect();
        assert!(f16_row_below_rerank_precision(&two));
        // RMS exactly 3 steps is kept exact.
        assert!(!f16_row_below_rerank_precision(&vec![0x0003u16; dim]));
        // One large subnormal component lifts the RMS over the bound.
        let mut spike = vec![0x0001u16; dim];
        spike[7] = 0x0011; // 17 steps: 31 + 289 = 320 >= 9 * 32
        assert!(!f16_row_below_rerank_precision(&spike));
        // Any normal (or infinite) component: the row is at an ordinary scale.
        let mut normal = vec![0u16; dim];
        normal[dim - 1] = 0x0400; // 2^-14, the smallest normal
        assert!(!f16_row_below_rerank_precision(&normal));
        let mut inf = vec![0u16; dim];
        inf[0] = 0x7C00;
        assert!(!f16_row_below_rerank_precision(&inf));
        let ordinary: Vec<u16> = (0..dim).map(|i| f32_to_f16(0.01 * i as f32)).collect();
        assert!(!f16_row_below_rerank_precision(&ordinary));
        // Components of ~1e-7 (1-2 steps) are below it; ~1e-6 (17 steps) are not.
        let e7: Vec<u16> = (0..dim)
            .map(|i| f32_to_f16(1e-7 * (1 + i % 2) as f32))
            .collect();
        assert!(f16_row_below_rerank_precision(&e7));
        let e6: Vec<u16> = (0..dim).map(|_| f32_to_f16(1e-6)).collect();
        assert!(!f16_row_below_rerank_precision(&e6));
    }

    #[test]
    fn only_l2_keeps_the_adc_estimate_for_a_row_below_rerank_precision() {
        crate::vector::distance::init();
        let kernels = crate::vector::distance::table();
        let dim = 32;
        let tiny: Vec<u16> = (0..dim).map(|_| f32_to_f16(1e-7)).collect();
        let q: Vec<f32> = (0..dim).map(|i| 1e-7 * (i % 3) as f32).collect();
        assert_eq!(exact_f16_distance(kernels, &q, &tiny, true), None, "L2");
        let mut q_unit = Vec::new();
        super::unit_query_into(&q, &mut q_unit);
        assert!(
            exact_f16_distance(kernels, &q_unit, &tiny, false).is_some(),
            "unit-sphere metrics rerank it"
        );
        let ordinary: Vec<u16> = (0..dim).map(|i| f32_to_f16(0.5 + i as f32)).collect();
        assert!(exact_f16_distance(kernels, &q, &ordinary, true).is_some());
    }
}
