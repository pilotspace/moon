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

/// The exact-rerank distance of one f16 row (HQ-1), in the convention every
/// rerank shares so cross-segment merges stay consistent: true squared L2 for
/// L2 (`q` is the raw query), `2 − 2·cos` for the unit-sphere metrics (`q` is
/// the unit query, see [`unit_query_into`]; f16 rounding is clamped into
/// `[-1, 1]`). `None` for a zero row under a unit-sphere metric — its
/// normalized form is undefined and the caller keeps its ADC estimate.
#[inline]
pub(crate) fn exact_f16_distance(
    kernels: &crate::vector::distance::DistanceTable,
    q: &[f32],
    row: &[u16],
    is_l2: bool,
) -> Option<f32> {
    if is_l2 {
        return Some((kernels.f16_l2)(q, row));
    }
    let (dot, xsq) = (kernels.f16_dot_normsq)(q, row);
    (xsq > 0.0).then(|| {
        let cos = (dot / xsq.sqrt()).clamp(-1.0, 1.0);
        2.0 - 2.0 * cos
    })
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
