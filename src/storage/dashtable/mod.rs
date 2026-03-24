//! DashTable: segmented hash table with Swiss Table SIMD probing.
//!
//! A custom hash table that combines Dragonfly's DashTable macro-architecture
//! (directory -> segments -> buckets) with hashbrown's Swiss Table SIMD
//! micro-optimization (control byte groups with parallel comparison).

pub mod iter;
pub mod segment;
pub mod simd;
