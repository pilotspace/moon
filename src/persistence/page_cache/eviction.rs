//! Clock-sweep eviction algorithm for the PageCache.
//!
//! Implements PostgreSQL-style clock-sweep: a circular scan that decrements
//! usage counts and evicts the first frame with usage=0 and refcount=0.

/// Placeholder — implemented in Task 2.
pub struct ClockSweep;
