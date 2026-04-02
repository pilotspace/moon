//! Global atomic counters for vector engine monitoring.
//!
//! Follows the same pattern as `persistence.rs` (SAVE_IN_PROGRESS, LAST_SAVE_TIME):
//! all counters use `Ordering::Relaxed` because INFO is advisory, not transactional.
//!
//! No allocations in any metric function -- pure atomic operations only.
//! These are called from hot paths (FT.SEARCH).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// -- MoonStore v2 flags --

/// Whether disk offload (tiered storage) is enabled. Set once at startup.
pub static MOONSTORE_DISK_OFFLOAD_ENABLED: AtomicBool = AtomicBool::new(false);

// -- Counters --

/// Number of active vector indexes (incremented on FT.CREATE, decremented on FT.DROPINDEX).
pub static VECTOR_INDEXES: AtomicU64 = AtomicU64::new(0);

/// Total vectors inserted across all indexes.
pub static VECTOR_TOTAL_VECTORS: AtomicU64 = AtomicU64::new(0);

/// Approximate total memory usage of vector data in bytes.
pub static VECTOR_MEMORY_BYTES: AtomicU64 = AtomicU64::new(0);

/// Total number of FT.SEARCH operations executed.
pub static VECTOR_SEARCH_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Rolling last-search latency in microseconds (last-writer-wins).
pub static VECTOR_SEARCH_LATENCY_US: AtomicU64 = AtomicU64::new(0);

/// Total number of compaction operations completed.
pub static VECTOR_COMPACTION_COUNT: AtomicU64 = AtomicU64::new(0);

/// Duration of last compaction in milliseconds.
pub static VECTOR_COMPACTION_DURATION_MS: AtomicU64 = AtomicU64::new(0);

/// Approximate byte size of the active mutable segment.
pub static VECTOR_MUTABLE_SEGMENT_BYTES: AtomicU64 = AtomicU64::new(0);

// -- Helper functions (zero-allocation, pure atomics) --

/// Increment the search counter by 1.
#[inline]
pub fn increment_search() {
    VECTOR_SEARCH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Store the latest search latency in microseconds (last-writer-wins).
#[inline]
pub fn record_search_latency(us: u64) {
    VECTOR_SEARCH_LATENCY_US.store(us, Ordering::Relaxed);
}

/// Increment the active index counter (called on FT.CREATE).
#[inline]
pub fn increment_indexes() {
    VECTOR_INDEXES.fetch_add(1, Ordering::Relaxed);
}

/// Decrement the active index counter (called on FT.DROPINDEX).
/// Uses saturating subtraction to avoid wrapping from 0 to u64::MAX.
#[inline]
pub fn decrement_indexes() {
    VECTOR_INDEXES
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        })
        .ok();
}

/// Add to total vector count (called on vector insertion).
#[inline]
pub fn add_vectors(count: u64) {
    VECTOR_TOTAL_VECTORS.fetch_add(count, Ordering::Relaxed);
}

/// Update the memory usage gauge (relaxed store).
#[inline]
pub fn update_memory(bytes: u64) {
    VECTOR_MEMORY_BYTES.store(bytes, Ordering::Relaxed);
}

/// Record a compaction event: increment count, store duration.
#[inline]
pub fn record_compaction(duration_ms: u64) {
    VECTOR_COMPACTION_COUNT.fetch_add(1, Ordering::Relaxed);
    VECTOR_COMPACTION_DURATION_MS.store(duration_ms, Ordering::Relaxed);
}

/// Update the mutable segment byte size gauge.
#[inline]
pub fn update_mutable_segment_bytes(bytes: u64) {
    VECTOR_MUTABLE_SEGMENT_BYTES.store(bytes, Ordering::Relaxed);
}
