//! CollectionMetadata -- immutable per-collection configuration.
//!
//! Write-once at collection creation. FWHT sign flips and codebook
//! are materialized (stored as actual values, not PRNG seeds) to
//! prevent PRNG implementation drift across Rust versions.

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::types::DistanceMetric;
use super::codebook::{CODEBOOK_VERSION, scaled_centroids_n, scaled_boundaries_n, code_bytes_per_vector};
use super::encoder::padded_dimension;
use super::sub_centroid::SubCentroidTable;

/// Quantization algorithm selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QuantizationConfig {
    Sq8 = 0,
    TurboQuant4 = 1,
    TurboQuantProd4 = 2,
    TurboQuant1 = 3,
    TurboQuant2 = 4,
    TurboQuant3 = 5,
}

impl QuantizationConfig {
    /// Number of bits per coordinate for this quantization variant.
    #[inline]
    pub fn bits(&self) -> u8 {
        match self {
            Self::TurboQuant1 => 1,
            Self::TurboQuant2 => 2,
            Self::TurboQuant3 => 3,
            Self::TurboQuant4 | Self::TurboQuantProd4 => 4,
            Self::Sq8 => 8,
        }
    }

    /// Returns true for any TurboQuant variant (1/2/3/4-bit).
    #[inline]
    pub fn is_turbo_quant(&self) -> bool {
        matches!(self, Self::TurboQuant1 | Self::TurboQuant2 | Self::TurboQuant3 | Self::TurboQuant4 | Self::TurboQuantProd4)
    }

    /// Number of centroids for this quantization variant: 2^bits.
    #[inline]
    pub fn n_centroids(&self) -> usize {
        1 << self.bits()
    }
}

/// Immutable per-collection configuration with integrity checksum.
///
/// Created once when a collection is defined. The FWHT sign flips are
/// materialized as explicit +/-1.0 values, never regenerated from a seed.
/// The `metadata_checksum` field (XXHash64) is computed at creation and
/// verified at load and search init.
pub struct CollectionMetadata {
    pub collection_id: u64,
    pub created_at_lsn: u64,
    pub dimension: u32,
    pub padded_dimension: u32,
    pub metric: DistanceMetric,
    pub quantization: QuantizationConfig,

    /// Materialized +-1.0 sign flips for randomized FWHT.
    /// Length = padded_dimension. NEVER regenerated from seed.
    pub fwht_sign_flips: AlignedBuffer<f32>,

    pub codebook_version: u8,
    pub codebook: Vec<f32>,
    pub codebook_boundaries: Vec<f32>,

    /// XXHash64 of all fields above. Verified at load and search init.
    pub metadata_checksum: u64,

    /// QJL dense Gaussian projection matrices for unbiased inner product estimation.
    ///
    /// The QJL unbiasedness proof requires rows sᵢ ~ N(0, I) so that
    /// (sᵢᵀx, sᵢᵀy) is jointly Gaussian. SRHT violates this assumption
    /// and introduces bias. Dense Gaussian is mathematically correct.
    ///
    /// M independent d×d matrices. Memory: M × d² × 4 bytes.
    /// M=4 at 768d = 9 MB shared. M=8 for 95%+ recall = 18 MB.
    pub qjl_matrices: Vec<Vec<f32>>,
    /// Number of QJL projections (M). Higher M = lower variance = better recall.
    /// M=4: ~91% recall. M=8: ~95% recall.
    pub qjl_num_projections: usize,

    /// Sub-centroid table for sign-bit refinement (from turboquant_search).
    /// Doubles effective quantization resolution from 2^b to 2^(b+1) levels.
    /// Used as Tier 2 reranker — better recall than TQ-ADC, no QJL overhead.
    pub sub_centroid_table: Option<SubCentroidTable>,
}

/// Errors related to collection metadata integrity.
#[derive(Debug)]
pub enum CollectionMetadataError {
    ChecksumMismatch { expected: u64, actual: u64 },
}

impl std::fmt::Display for CollectionMetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChecksumMismatch { expected, actual } =>
                write!(f, "metadata checksum mismatch: expected {expected:#x}, got {actual:#x}"),
        }
    }
}

impl CollectionMetadata {
    /// Create new metadata with materialized sign flips.
    ///
    /// `seed` controls sign flip generation (deterministic for testing).
    /// Sign flips are materialized: stored as +/-1.0 f32, not as seed.
    /// After generation the seed is discarded -- flips are the source of truth.
    pub fn new(
        collection_id: u64,
        dimension: u32,
        metric: DistanceMetric,
        quantization: QuantizationConfig,
        seed: u64,
    ) -> Self {
        let padded = padded_dimension(dimension);

        // Generate materialized sign flips using LCG PRNG.
        // After generation the seed is discarded -- flips are the source of truth.
        let mut sign_flips = AlignedBuffer::<f32>::new(padded as usize);
        let mut rng_state = seed;
        for val in sign_flips.as_mut_slice().iter_mut() {
            // LCG constants from Knuth MMIX
            rng_state = rng_state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            *val = if (rng_state >> 63) == 0 { 1.0 } else { -1.0 };
        }

        // Generate M dense Gaussian QJL matrices for unbiased inner product scoring.
        // Dense Gaussian required — SRHT violates joint Gaussianity for E[V·sign(U)].
        // M=4: ~91% recall, 9 MB at 768d. M=8: ~95% recall, 18 MB.
        const QJL_NUM_PROJECTIONS: usize = 8;
        let (qjl_matrices, qjl_num_projections) = if quantization.is_turbo_quant() {
            let matrices: Vec<Vec<f32>> = (0..QJL_NUM_PROJECTIONS)
                .map(|m| {
                    super::qjl::generate_qjl_matrix(
                        dimension as usize,
                        seed.wrapping_add(1 + m as u64),
                    )
                })
                .collect();
            (matrices, QJL_NUM_PROJECTIONS)
        } else {
            (Vec::new(), 0)
        };

        // Build sub-centroid table for sign-bit refinement (doubles effective resolution).
        let sub_centroid_table = if quantization.is_turbo_quant() {
            Some(SubCentroidTable::new(padded, quantization.bits()))
        } else {
            None
        };

        let mut meta = Self {
            collection_id,
            created_at_lsn: 0,
            dimension,
            padded_dimension: padded,
            metric,
            quantization,
            fwht_sign_flips: sign_flips,
            codebook_version: CODEBOOK_VERSION,
            codebook: if quantization.is_turbo_quant() {
                scaled_centroids_n(padded, quantization.bits())
            } else {
                // SQ8 doesn't use codebooks -- store empty Vec
                Vec::new()
            },
            codebook_boundaries: if quantization.is_turbo_quant() {
                scaled_boundaries_n(padded, quantization.bits())
            } else {
                Vec::new()
            },
            metadata_checksum: 0, // computed below
            qjl_matrices,
            qjl_num_projections,
            sub_centroid_table,
        };
        meta.metadata_checksum = meta.compute_checksum();
        meta
    }

    /// Compute XXHash64 over all fields except metadata_checksum itself.
    fn compute_checksum(&self) -> u64 {
        use xxhash_rust::xxh64::xxh64;
        let mut data = Vec::with_capacity(256);
        data.extend_from_slice(&self.collection_id.to_le_bytes());
        data.extend_from_slice(&self.created_at_lsn.to_le_bytes());
        data.extend_from_slice(&self.dimension.to_le_bytes());
        data.extend_from_slice(&self.padded_dimension.to_le_bytes());
        data.extend_from_slice(&[self.metric as u8]);
        data.extend_from_slice(&[self.quantization as u8]);
        data.extend_from_slice(&[self.codebook_version]);
        for &c in &self.codebook {
            data.extend_from_slice(&c.to_le_bytes());
        }
        for &b in &self.codebook_boundaries {
            data.extend_from_slice(&b.to_le_bytes());
        }
        // Include sign flips (the materialized values, not a seed)
        for &s in self.fwht_sign_flips.as_slice() {
            data.extend_from_slice(&s.to_le_bytes());
        }
        xxh64(&data, 0)
    }

    /// Packed code size in bytes per vector for this collection's quantization.
    #[inline]
    pub fn code_bytes_per_vector(&self) -> usize {
        code_bytes_per_vector(self.padded_dimension, self.quantization.bits())
    }

    /// Convenience accessor: returns the codebook boundaries as a `&[f32; 15]` reference.
    ///
    /// Panics if quantization is not 4-bit (only valid for TurboQuant4 / TurboQuantProd4).
    /// Used by legacy `encode_tq_mse_scaled` which requires fixed-size array.
    pub fn codebook_boundaries_15(&self) -> &[f32; 15] {
        assert_eq!(
            self.codebook_boundaries.len(), 15,
            "codebook_boundaries_15 requires 4-bit quantization (15 boundaries), got {}",
            self.codebook_boundaries.len()
        );
        self.codebook_boundaries[..15].try_into().unwrap()
    }

    /// Convenience accessor: returns the codebook as a `&[f32; 16]` reference.
    ///
    /// Panics if quantization is not 4-bit (only valid for TurboQuant4 / TurboQuantProd4).
    /// Used by legacy `tq_l2_adc_scaled` which requires fixed-size array.
    pub fn codebook_16(&self) -> &[f32; 16] {
        assert_eq!(
            self.codebook.len(), 16,
            "codebook_16 requires 4-bit quantization (16 centroids), got {}",
            self.codebook.len()
        );
        self.codebook[..16].try_into().unwrap()
    }

    /// Verify metadata integrity. Returns Err if checksum mismatch.
    pub fn verify_checksum(&self) -> Result<(), CollectionMetadataError> {
        let computed = self.compute_checksum();
        if computed != self.metadata_checksum {
            return Err(CollectionMetadataError::ChecksumMismatch {
                expected: self.metadata_checksum,
                actual: computed,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::turbo_quant::codebook::CODEBOOK_VERSION;

    #[test]
    fn test_new_creates_correct_padded_dimension() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert_eq!(meta.padded_dimension, 1024);
        assert_eq!(meta.dimension, 768);
    }

    #[test]
    fn test_sign_flips_length_and_values() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert_eq!(meta.fwht_sign_flips.len(), 1024);
        // Every element must be exactly +1.0 or -1.0
        for (i, &val) in meta.fwht_sign_flips.as_slice().iter().enumerate() {
            assert!(
                val == 1.0 || val == -1.0,
                "sign_flip[{i}] = {val}, expected +/-1.0"
            );
        }
        // Should have both +1 and -1 (probabilistic, but with 1024 elements and seed 42 this is certain)
        let plus_count = meta.fwht_sign_flips.as_slice().iter().filter(|&&v| v == 1.0).count();
        assert!(plus_count > 0 && plus_count < 1024, "sign flips should be mixed");
    }

    #[test]
    fn test_checksum_deterministic() {
        let meta1 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        let meta2 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert_eq!(meta1.metadata_checksum, meta2.metadata_checksum);
        assert_ne!(meta1.metadata_checksum, 0);
    }

    #[test]
    fn test_verify_checksum_ok() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert!(meta.verify_checksum().is_ok());
    }

    #[test]
    fn test_verify_checksum_detects_corruption() {
        let mut meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        // Corrupt the collection_id
        meta.collection_id = 999;
        assert!(meta.verify_checksum().is_err());

        // Corrupt dimension
        let mut meta2 = CollectionMetadata::new(
            2, 384, DistanceMetric::Cosine, QuantizationConfig::TurboQuant4, 123,
        );
        meta2.dimension = 999;
        assert!(meta2.verify_checksum().is_err());

        // Corrupt a sign flip
        let mut meta3 = CollectionMetadata::new(
            3, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 77,
        );
        meta3.fwht_sign_flips.as_mut_slice()[0] = 0.5; // invalid value
        assert!(meta3.verify_checksum().is_err());
    }

    #[test]
    fn test_codebook_version_matches() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert_eq!(meta.codebook_version, CODEBOOK_VERSION);
    }

    #[test]
    fn test_different_seeds_produce_different_flips() {
        let meta1 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        let meta2 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 99,
        );
        // Different seeds -> different sign flips -> different checksum
        assert_ne!(meta1.metadata_checksum, meta2.metadata_checksum);
    }

    #[test]
    fn test_checksum_mismatch_error_display() {
        let err = CollectionMetadataError::ChecksumMismatch {
            expected: 0xDEAD,
            actual: 0xBEEF,
        };
        let msg = format!("{err}");
        assert!(msg.contains("checksum mismatch"));
        assert!(msg.contains("0xdead"));
        assert!(msg.contains("0xbeef"));
    }

    // -- Multi-bit TurboQuant tests (Phase 72-02) --

    #[test]
    fn test_turbo_quant1_exists_and_has_correct_repr() {
        assert_eq!(QuantizationConfig::TurboQuant1 as u8, 3);
        assert_eq!(QuantizationConfig::TurboQuant2 as u8, 4);
        assert_eq!(QuantizationConfig::TurboQuant3 as u8, 5);
    }

    #[test]
    fn test_bits_helper() {
        assert_eq!(QuantizationConfig::TurboQuant1.bits(), 1);
        assert_eq!(QuantizationConfig::TurboQuant2.bits(), 2);
        assert_eq!(QuantizationConfig::TurboQuant3.bits(), 3);
        assert_eq!(QuantizationConfig::TurboQuant4.bits(), 4);
        assert_eq!(QuantizationConfig::TurboQuantProd4.bits(), 4);
        assert_eq!(QuantizationConfig::Sq8.bits(), 8);
    }

    #[test]
    fn test_is_turbo_quant() {
        assert!(QuantizationConfig::TurboQuant1.is_turbo_quant());
        assert!(QuantizationConfig::TurboQuant2.is_turbo_quant());
        assert!(QuantizationConfig::TurboQuant3.is_turbo_quant());
        assert!(QuantizationConfig::TurboQuant4.is_turbo_quant());
        assert!(QuantizationConfig::TurboQuantProd4.is_turbo_quant());
        assert!(!QuantizationConfig::Sq8.is_turbo_quant());
    }

    #[test]
    fn test_tq1_codebook_has_2_centroids_1_boundary() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant1, 42,
        );
        assert_eq!(meta.codebook.len(), 2);
        assert_eq!(meta.codebook_boundaries.len(), 1);
        assert!(meta.verify_checksum().is_ok());
    }

    #[test]
    fn test_tq2_codebook_has_4_centroids_3_boundaries() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant2, 42,
        );
        assert_eq!(meta.codebook.len(), 4);
        assert_eq!(meta.codebook_boundaries.len(), 3);
        assert!(meta.verify_checksum().is_ok());
    }

    #[test]
    fn test_tq3_codebook_has_8_centroids_7_boundaries() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant3, 42,
        );
        assert_eq!(meta.codebook.len(), 8);
        assert_eq!(meta.codebook_boundaries.len(), 7);
        assert!(meta.verify_checksum().is_ok());
    }

    #[test]
    fn test_tq4_still_has_16_centroids_15_boundaries() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert_eq!(meta.codebook.len(), 16);
        assert_eq!(meta.codebook_boundaries.len(), 15);
        assert!(meta.verify_checksum().is_ok());
    }

    #[test]
    fn test_code_bytes_per_vector() {
        let meta1 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant1, 42,
        );
        // 768 pads to 1024. 1-bit: 1024/8 = 128
        assert_eq!(meta1.code_bytes_per_vector(), 128);

        let meta2 = CollectionMetadata::new(
            2, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant2, 42,
        );
        // 2-bit: 1024/4 = 256
        assert_eq!(meta2.code_bytes_per_vector(), 256);

        let meta4 = CollectionMetadata::new(
            4, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        // 4-bit: 1024/2 = 512
        assert_eq!(meta4.code_bytes_per_vector(), 512);
    }

    #[test]
    fn test_checksum_changes_when_quantization_changes() {
        let meta1 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant1, 42,
        );
        let meta4 = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        assert_ne!(meta1.metadata_checksum, meta4.metadata_checksum);
    }

    #[test]
    fn test_codebook_16_accessor() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        let cb: &[f32; 16] = meta.codebook_16();
        assert_eq!(cb.len(), 16);
    }

    #[test]
    fn test_codebook_boundaries_15_accessor() {
        let meta = CollectionMetadata::new(
            1, 768, DistanceMetric::L2, QuantizationConfig::TurboQuant4, 42,
        );
        let bb: &[f32; 15] = meta.codebook_boundaries_15();
        assert_eq!(bb.len(), 15);
    }
}
