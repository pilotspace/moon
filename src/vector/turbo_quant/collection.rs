//! CollectionMetadata -- immutable per-collection configuration.
//!
//! Write-once at collection creation. FWHT sign flips and codebook
//! are materialized (stored as actual values, not PRNG seeds) to
//! prevent PRNG implementation drift across Rust versions.

use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::types::DistanceMetric;
use super::codebook::{CENTROIDS, CODEBOOK_VERSION, BOUNDARIES};
use super::encoder::padded_dimension;

/// Quantization algorithm selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QuantizationConfig {
    Sq8 = 0,
    TurboQuant4 = 1,
    TurboQuantProd4 = 2,
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
    pub codebook: [f32; 16],
    pub codebook_boundaries: [f32; 15],

    /// XXHash64 of all fields above. Verified at load and search init.
    pub metadata_checksum: u64,
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

        let mut meta = Self {
            collection_id,
            created_at_lsn: 0,
            dimension,
            padded_dimension: padded,
            metric,
            quantization,
            fwht_sign_flips: sign_flips,
            codebook_version: CODEBOOK_VERSION,
            codebook: CENTROIDS,
            codebook_boundaries: BOUNDARIES,
            metadata_checksum: 0, // computed below
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
}
