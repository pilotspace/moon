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

/// Approximate total memory usage of vector data in bytes.
pub static VECTOR_MEMORY_BYTES: AtomicU64 = AtomicU64::new(0);

/// Rolling last-search latency in microseconds (last-writer-wins).
/// Plain relaxed `store` (no RMW) — cheap enough to stay unstriped.
pub static VECTOR_SEARCH_LATENCY_US: AtomicU64 = AtomicU64::new(0);

// -- Striped hot-path counters (XC-5) --
//
// `increment_search`/`add_vectors` fire on every FT.SEARCH / vector insert from
// every shard thread. A single global `fetch_add` cache line ping-pongs across
// cores under thread-per-core load, taxing exactly the throughput the counter
// observes. Each stripe is cache-line padded; a thread picks a stripe once
// (round-robin at first use) and always RMWs its own line. Reads (INFO — cold
// path) sum all stripes.

const METRIC_STRIPES: usize = 16;

#[repr(align(64))]
struct PaddedAtomicU64(AtomicU64);

#[allow(clippy::declare_interior_mutable_const)]
const PADDED_ZERO: PaddedAtomicU64 = PaddedAtomicU64(AtomicU64::new(0));

/// Total FT.SEARCH operations, striped. Read via [`search_total`].
static VECTOR_SEARCH_TOTAL_STRIPES: [PaddedAtomicU64; METRIC_STRIPES] =
    [PADDED_ZERO; METRIC_STRIPES];

/// Total vectors inserted, striped. Read via [`total_vectors`].
static VECTOR_TOTAL_VECTORS_STRIPES: [PaddedAtomicU64; METRIC_STRIPES] =
    [PADDED_ZERO; METRIC_STRIPES];

/// Stripe slot for the calling thread: assigned round-robin on first use,
/// cached in a thread-local thereafter (one TLS read per metric bump).
#[inline]
fn stripe_index() -> usize {
    use std::sync::atomic::AtomicUsize;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SLOT: usize = NEXT.fetch_add(1, Ordering::Relaxed) % METRIC_STRIPES;
    }
    SLOT.with(|s| *s)
}

/// Sum of all search-counter stripes (cold path: INFO/stats only).
pub fn search_total() -> u64 {
    VECTOR_SEARCH_TOTAL_STRIPES
        .iter()
        .map(|s| s.0.load(Ordering::Relaxed))
        .sum()
}

/// Sum of all vector-count stripes (cold path: INFO/stats only).
pub fn total_vectors() -> u64 {
    VECTOR_TOTAL_VECTORS_STRIPES
        .iter()
        .map(|s| s.0.load(Ordering::Relaxed))
        .sum()
}

/// Total number of compaction operations completed.
pub static VECTOR_COMPACTION_COUNT: AtomicU64 = AtomicU64::new(0);

/// Duration of last compaction in milliseconds.
pub static VECTOR_COMPACTION_DURATION_MS: AtomicU64 = AtomicU64::new(0);

/// Approximate byte size of the active mutable segment.
pub static VECTOR_MUTABLE_SEGMENT_BYTES: AtomicU64 = AtomicU64::new(0);

// -- Helper functions (zero-allocation, pure atomics) --

/// Increment the search counter by 1 (striped: RMW on this thread's own line).
#[inline]
pub fn increment_search() {
    VECTOR_SEARCH_TOTAL_STRIPES[stripe_index()]
        .0
        .fetch_add(1, Ordering::Relaxed);
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

/// Add to total vector count (striped: RMW on this thread's own line).
#[inline]
pub fn add_vectors(count: u64) {
    VECTOR_TOTAL_VECTORS_STRIPES[stripe_index()]
        .0
        .fetch_add(count, Ordering::Relaxed);
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
