//! TurboQuant 4-bit quantization (arXiv 2504.19874).
//!
//! Implements the TurboQuant_MSE algorithm: normalize, pad, randomized FWHT,
//! quantize via Lloyd-Max codebook, nibble-pack. Achieves 8x compression
//! at <= 0.009 MSE distortion for unit vectors (Theorem 1).

pub mod codebook;
pub mod collection;
pub mod encoder;
pub mod fwht;
pub mod inner_product;
pub mod qjl;
pub mod sub_centroid;
pub mod tq_adc;
pub mod tq_adc_avx2;
