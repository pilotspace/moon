//! Minimal IEEE 754 binary16 (half-precision) conversion.
//!
//! Used by the exact-rerank sidecar (deep-review HQ-1): immutable segments
//! keep an optional f16 copy of each original vector so the top-of-beam
//! candidates can be re-scored with (near-)exact distances instead of pure
//! quantized ADC estimates. f16 halves the sidecar footprint vs f32 while its
//! ~1e-3 relative error is far below SQ8/TQ4 quantization error.
//!
//! Hand-rolled instead of the `half` crate: two total functions, no new
//! dependency on the hot path. Conversion follows IEEE 754-2019
//! round-to-nearest-even, with subnormal, overflow→infinity, and NaN
//! handling — each pinned by a unit test below.

/// Convert an f32 to IEEE 754 binary16 bits (round-to-nearest-even).
#[inline]
pub fn f32_to_f16(value: f32) -> u16 {
    let bits = value.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let exp = ((bits >> 23) & 0xFF) as i32;
    let mant = bits & 0x007F_FFFF;

    if exp == 0xFF {
        // Inf / NaN. Preserve NaN-ness with a quiet-NaN mantissa bit.
        return if mant == 0 {
            sign | 0x7C00
        } else {
            sign | 0x7E00
        };
    }

    // Re-bias: f32 bias 127 -> f16 bias 15.
    let unbiased = exp - 127;
    if unbiased > 15 {
        // Overflows f16 range -> infinity.
        return sign | 0x7C00;
    }
    if unbiased >= -14 {
        // Normal f16. Keep top 10 mantissa bits, RNE on the dropped 13.
        let exp16 = (unbiased + 15) as u32;
        let mant16 = mant >> 13;
        let rest = mant & 0x1FFF;
        let mut out = (exp16 << 10) | mant16;
        // Round up on >half, or exactly half with odd LSB (ties-to-even).
        if rest > 0x1000 || (rest == 0x1000 && (mant16 & 1) == 1) {
            out += 1; // Mantissa overflow correctly carries into the exponent.
        }
        return sign | (out as u16);
    }
    if unbiased >= -25 {
        // Subnormal f16: implicit leading 1 becomes explicit, shifted right.
        let full = mant | 0x0080_0000;
        let shift = (-14 - unbiased) as u32 + 13;
        let mant16 = full >> shift;
        let rest = full & ((1u32 << shift) - 1);
        let half = 1u32 << (shift - 1);
        let mut out = mant16;
        if rest > half || (rest == half && (mant16 & 1) == 1) {
            out += 1;
        }
        return sign | (out as u16);
    }
    // Underflows to signed zero.
    sign
}

/// Convert IEEE 754 binary16 bits to f32 (exact — every f16 is representable).
#[inline]
pub fn f16_to_f32(bits: u16) -> f32 {
    let sign = ((bits & 0x8000) as u32) << 16;
    let exp = ((bits >> 10) & 0x1F) as u32;
    let mant = (bits & 0x03FF) as u32;

    let out = if exp == 0 {
        if mant == 0 {
            sign // Signed zero.
        } else {
            // Subnormal: normalize by shifting the mantissa up.
            let lead = mant.leading_zeros() - 21; // Zeros above bit 9.
            let exp32 = 127 - 15 - lead;
            let mant32 = (mant << (lead + 1)) & 0x03FF;
            sign | (exp32 << 23) | (mant32 << 13)
        }
    } else if exp == 0x1F {
        // Inf / NaN.
        sign | 0x7F80_0000 | (mant << 13)
    } else {
        sign | ((exp + 127 - 15) << 23) | (mant << 13)
    };
    f32::from_bits(out)
}

/// Encode a full f32 slice into f16 bits, appending to `out`.
#[inline]
pub fn encode_f16_slice(src: &[f32], out: &mut Vec<u16>) {
    out.reserve(src.len());
    for &v in src {
        out.push(f32_to_f16(v));
    }
}

/// Squared L2 between an f32 query and an f16-encoded vector.
#[inline]
pub fn l2_sq_f16(query: &[f32], vec_f16: &[u16]) -> f32 {
    debug_assert_eq!(query.len(), vec_f16.len());
    let mut sum = 0.0f32;
    for (q, &h) in query.iter().zip(vec_f16.iter()) {
        let d = q - f16_to_f32(h);
        sum += d * d;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_values_roundtrip_exact() {
        // Values exactly representable in f16 must roundtrip bit-perfectly.
        for &v in &[
            0.0f32, -0.0, 1.0, -1.0, 0.5, 2.0, 65504.0, -65504.0, 0.25, 1024.0,
        ] {
            let back = f16_to_f32(f32_to_f16(v));
            assert_eq!(v.to_bits(), back.to_bits(), "value {v}");
        }
    }

    #[test]
    fn known_bit_patterns() {
        assert_eq!(f32_to_f16(1.0), 0x3C00);
        assert_eq!(f32_to_f16(-2.0), 0xC000);
        assert_eq!(f32_to_f16(65504.0), 0x7BFF); // f16::MAX
        assert_eq!(f32_to_f16(f32::INFINITY), 0x7C00);
        assert_eq!(f32_to_f16(f32::NEG_INFINITY), 0xFC00);
        assert_eq!(f32_to_f16(65520.0), 0x7C00); // Overflow -> inf.
        assert_eq!(f32_to_f16(6.10352e-5), 0x0400); // Smallest normal.
        assert_eq!(f32_to_f16(5.96046e-8), 0x0001); // Smallest subnormal.
        assert_eq!(f32_to_f16(1e-9), 0x0000); // Underflow -> zero.
    }

    #[test]
    fn nan_preserved() {
        assert!(f16_to_f32(f32_to_f16(f32::NAN)).is_nan());
    }

    #[test]
    fn round_to_nearest_even() {
        // 1.0 + 2^-11 is exactly halfway between f16(1.0) and the next f16 up;
        // ties-to-even keeps the even mantissa (1.0).
        let halfway = f32::from_bits(0x3F80_1000);
        assert_eq!(f32_to_f16(halfway), 0x3C00);
        // Just above halfway rounds up.
        let above = f32::from_bits(0x3F80_1001);
        assert_eq!(f32_to_f16(above), 0x3C01);
    }

    #[test]
    fn relative_error_bounded() {
        // RNE guarantees relative error <= 2^-11 for normal-range values.
        let mut s = 0x2545F491u32;
        for _ in 0..10_000 {
            s ^= s << 13;
            s ^= s >> 17;
            s ^= s << 5;
            let v = (s as f32 / u32::MAX as f32) * 200.0 - 100.0;
            let back = f16_to_f32(f32_to_f16(v));
            if v.abs() > 6.2e-5 {
                let rel = ((v - back) / v).abs();
                assert!(rel <= 4.9e-4, "v={v} back={back} rel={rel}");
            }
        }
    }

    #[test]
    fn l2_sq_matches_f32_within_tolerance() {
        let q: Vec<f32> = (0..64).map(|i| (i as f32) * 0.031 - 1.0).collect();
        let x: Vec<f32> = (0..64).map(|i| (i as f32) * -0.017 + 0.5).collect();
        let mut enc = Vec::new();
        encode_f16_slice(&x, &mut enc);
        let exact: f32 = q.iter().zip(x.iter()).map(|(a, b)| (a - b) * (a - b)).sum();
        let approx = l2_sq_f16(&q, &enc);
        let rel = ((exact - approx) / exact).abs();
        assert!(rel < 1e-3, "exact={exact} approx={approx} rel={rel}");
    }
}
