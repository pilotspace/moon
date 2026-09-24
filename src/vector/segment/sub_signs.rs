//! Sub-centroid sign bits (the 32-level TQ-ADC refinement), shared by every
//! producer so they cannot drift apart (moon#1193).
//!
//! For each padded coordinate `j` of a TQ-encoded vector the sign bit records
//! whether the vector's ACTUAL rotated unit coordinate lies at or above the
//! centroid its 4-bit code selected (upper sub-bin) or below it. The HNSW
//! beam then scores with a 32-entry LUT (`code * 2 + sign`) instead of the
//! 16-entry one.
//!
//! Producers:
//! - compaction (`compact_path::compact`): from the retained raw `f32`
//!   vectors (EXACT build mode) — [`SubSignEncoder::encode_f32`];
//! - GraphUnion merge: a source segment that carries no signs (reloaded from
//!   a pre-moon#1193 segment directory) has them recomputed from its f16
//!   exact-rerank sidecar — [`SubSignEncoder::encode_f16`] — instead of
//!   zero-filling, which biased every such row toward the lower sub-bin;
//! - segment persistence writes them to `sub_signs.bin` so a restart keeps
//!   the 32-level LUT.

use crate::vector::turbo_quant::a2_lattice::A2Codebook;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};

/// Bytes of sign bits per vector for a padded dimension.
#[inline]
pub(crate) fn sub_sign_bytes_per_vec(padded_dim: usize) -> usize {
    padded_dim.div_ceil(8)
}

/// Stateful encoder: owns the rotation work buffer so a whole segment is
/// encoded with one allocation.
pub(crate) struct SubSignEncoder<'a> {
    signs: &'a [f32],
    dim: usize,
    padded: usize,
    code_len: usize,
    codebook: Option<&'a [f32; 16]>,
    a2_cb: Option<A2Codebook>,
    work: Vec<f32>,
}

impl<'a> SubSignEncoder<'a> {
    /// `None` for SQ8 (no codebook, no sub-centroid refinement — its segments
    /// carry a zero-filled buffer the search never reads).
    pub(crate) fn new(collection: &'a CollectionMetadata) -> Option<Self> {
        let dim = collection.dimension as usize;
        let padded = collection.padded_dimension as usize;
        let (codebook, a2_cb, code_len) = match collection.quantization {
            QuantizationConfig::Sq8 => return None,
            QuantizationConfig::TurboQuant4A2 => (
                None,
                Some(A2Codebook::new(collection.padded_dimension)),
                collection.code_bytes_per_vector(),
            ),
            _ => (
                Some(collection.try_codebook_16()?),
                None,
                collection.code_bytes_per_vector(),
            ),
        };
        Some(Self {
            signs: collection.fwht_sign_flips.as_slice(),
            dim,
            padded,
            code_len,
            codebook,
            a2_cb,
            work: vec![0.0; padded],
        })
    }

    /// Sign bytes per vector this encoder produces.
    pub(crate) fn bytes_per_vec(&self) -> usize {
        sub_sign_bytes_per_vec(self.padded)
    }

    /// Code bytes (without the norm trailer) this encoder expects.
    pub(crate) fn code_len(&self) -> usize {
        self.code_len
    }

    /// Signs for one vector given its raw `f32` values. `out` must be
    /// zeroed and `bytes_per_vec()` long.
    pub(crate) fn encode_f32(&mut self, raw: &[f32], code: &[u8], out: &mut [u8]) {
        let dim = self.dim;
        // Normalize + pad + FWHT to get the actual rotated coordinates.
        let norm_sq: f32 = raw.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            for (dst, &src) in self.work[..dim].iter_mut().zip(raw.iter()) {
                *dst = src * inv;
            }
        } else {
            for v in self.work[..dim].iter_mut() {
                *v = 0.0;
            }
        }
        self.finish(code, out);
    }

    /// Signs for one vector given its f16 exact-rerank sidecar row. Same as
    /// [`Self::encode_f32`] on the decoded values; a coordinate whose rotated
    /// value lies within f16 rounding of its centroid may differ from the
    /// f32-derived bit (it is then equally close to either sub-bin).
    pub(crate) fn encode_f16(&mut self, raw: &[u16], code: &[u8], out: &mut [u8]) {
        let dim = self.dim;
        for (dst, &h) in self.work[..dim].iter_mut().zip(raw.iter()) {
            *dst = crate::vector::f16::f16_to_f32(h);
        }
        let norm_sq: f32 = self.work[..dim].iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            for v in self.work[..dim].iter_mut() {
                *v *= inv;
            }
        } else {
            for v in self.work[..dim].iter_mut() {
                *v = 0.0;
            }
        }
        self.finish(code, out);
    }

    fn finish(&mut self, code_slice: &[u8], out: &mut [u8]) {
        let (dim, padded) = (self.dim, self.padded);
        for v in self.work[dim..padded].iter_mut() {
            *v = 0.0;
        }
        crate::vector::turbo_quant::fwht::fwht(&mut self.work[..padded], self.signs);
        let work = &self.work;
        if let Some(cb) = self.a2_cb.as_ref() {
            // A2: each nibble is a pair index, decode via A2Codebook.
            for (j, &byte) in code_slice.iter().enumerate() {
                let qi = j * 4; // each byte = 2 pairs = 4 coordinates
                let (x0, y0) = cb.decode_pair(byte & 0x0F);
                let (x1, y1) = cb.decode_pair(byte >> 4);
                if qi < padded && work[qi] >= x0 {
                    out[qi / 8] |= 1 << (qi % 8);
                }
                if qi + 1 < padded && work[qi + 1] >= y0 {
                    out[(qi + 1) / 8] |= 1 << ((qi + 1) % 8);
                }
                if qi + 2 < padded && work[qi + 2] >= x1 {
                    out[(qi + 2) / 8] |= 1 << ((qi + 2) % 8);
                }
                if qi + 3 < padded && work[qi + 3] >= y1 {
                    out[(qi + 3) / 8] |= 1 << ((qi + 3) % 8);
                }
            }
        } else if let Some(codebook) = self.codebook {
            // Scalar TQ: each nibble is a single-coordinate index.
            for (j, &byte) in code_slice.iter().enumerate() {
                let qi = j * 2;
                if work[qi] >= codebook[(byte & 0x0F) as usize] {
                    out[qi / 8] |= 1 << (qi % 8);
                }
                if work[qi + 1] >= codebook[(byte >> 4) as usize] {
                    out[(qi + 1) / 8] |= 1 << ((qi + 1) % 8);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::turbo_quant::encoder::encode_tq_mse_scaled_with_signs;
    use crate::vector::types::DistanceMetric;

    fn vec_of(dim: usize, seed: u32) -> Vec<f32> {
        let mut s = seed;
        (0..dim)
            .map(|_| {
                s = s.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (s as f32) / (u32::MAX as f32) * 2.0 - 1.0
            })
            .collect()
    }

    #[test]
    fn encoder_matches_insert_time_signs() {
        // The mutable segment computes signs at insert time with
        // `encode_tq_mse_scaled_with_signs`; the shared encoder (used by
        // compaction and merge) must agree bit for bit from raw f32.
        crate::vector::distance::init();
        crate::vector::turbo_quant::fwht::init_fwht();
        for dim in [6usize, 100, 384, 768] {
            let col = CollectionMetadata::new(
                3,
                dim as u32,
                DistanceMetric::L2,
                QuantizationConfig::TurboQuant4,
                11,
            );
            let mut enc = SubSignEncoder::new(&col).expect("TQ4");
            let mut work = vec![0.0f32; col.padded_dimension as usize];
            for seed in 0..32u32 {
                let v = vec_of(dim, seed * 7 + 1);
                let with = encode_tq_mse_scaled_with_signs(
                    &v,
                    col.fwht_sign_flips.as_slice(),
                    col.codebook_boundaries_15(),
                    col.codebook_16(),
                    &mut work,
                );
                let mut out = vec![0u8; enc.bytes_per_vec()];
                enc.encode_f32(&v, &with.code.codes, &mut out);
                assert_eq!(out, with.signs, "dim={dim} seed={seed}");
                // f16 path: equal except for coordinates within f16 rounding
                // of their centroid — on these fixtures, at most a handful.
                let mut halves = Vec::new();
                crate::vector::f16::encode_f16_slice(&v, &mut halves);
                let mut out16 = vec![0u8; enc.bytes_per_vec()];
                enc.encode_f16(&halves, &with.code.codes, &mut out16);
                let flipped: u32 = out16
                    .iter()
                    .zip(out.iter())
                    .map(|(a, b)| (a ^ b).count_ones())
                    .sum();
                assert!(
                    flipped as usize <= col.padded_dimension as usize / 64 + 1,
                    "dim={dim}: {flipped} f16-vs-f32 sign flips"
                );
            }
        }
    }

    #[test]
    fn sq8_has_no_encoder() {
        let col = CollectionMetadata::new(3, 16, DistanceMetric::L2, QuantizationConfig::Sq8, 1);
        assert!(SubSignEncoder::new(&col).is_none());
    }
}
