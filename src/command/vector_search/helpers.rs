//! Shared helper functions for vector search command handlers.

use bytes::Bytes;

use crate::vector::turbo_quant::collection::QuantizationConfig;
use crate::vector::types::DistanceMetric;

/// Convert QuantizationConfig to display bytes.
pub fn quantization_to_bytes(q: QuantizationConfig) -> Bytes {
    match q {
        QuantizationConfig::Sq8 => Bytes::from_static(b"SQ8"),
        QuantizationConfig::TurboQuant4 => Bytes::from_static(b"TurboQuant4"),
        QuantizationConfig::TurboQuantProd4 => Bytes::from_static(b"TurboQuantProd4"),
        QuantizationConfig::TurboQuant1 => Bytes::from_static(b"TurboQuant1"),
        QuantizationConfig::TurboQuant2 => Bytes::from_static(b"TurboQuant2"),
        QuantizationConfig::TurboQuant3 => Bytes::from_static(b"TurboQuant3"),
        QuantizationConfig::TurboQuant4A2 => Bytes::from_static(b"TurboQuant4A2"),
    }
}

/// Scalar-quantize f32 vector to i8 for mutable segment brute-force search.
/// Clamps to [-1.0, 1.0] range, scales to [-127, 127].
/// This is intentionally simple -- TQ encoding is used for immutable segments.
pub fn quantize_f32_to_sq(input: &[f32], output: &mut [i8]) {
    debug_assert_eq!(input.len(), output.len());
    for (i, &val) in input.iter().enumerate() {
        let clamped = val.clamp(-1.0, 1.0);
        output[i] = (clamped * 127.0) as i8;
    }
}

/// Convert DistanceMetric to display bytes.
pub fn metric_to_bytes(m: DistanceMetric) -> Bytes {
    match m {
        DistanceMetric::L2 => Bytes::from_static(b"L2"),
        DistanceMetric::Cosine => Bytes::from_static(b"COSINE"),
        DistanceMetric::InnerProduct => Bytes::from_static(b"IP"),
    }
}
