//! Global reclamation accounting atomics.
//!
//! These counters are updated by the warm-segment budget enforcer and exposed
//! via INFO/metrics. They use `Relaxed` ordering throughout — exact counts are
//! not required for observability; approximate values suffice for dashboards and
//! alerts. No lock-ordering constraints exist because these are write-only from
//! the shard event loop and read-only from the metrics scrape path.

use std::sync::atomic::{AtomicU64, Ordering};

/// Current estimated resident bytes held by warm-tier `WarmSearchSegment`
/// instances (owned `codes_data` + `HnswGraph` + `global_ids`).
///
/// Updated by `MmapBudget::record_access` and decremented by
/// `MmapBudget::enforce_budget` as segments are evicted. This counter
/// represents the sum across ALL shard budgets registered via
/// `MmapBudget::current_resident_bytes`.
///
/// Note: because warm segments copy mmap data into owned `Vec<u8>` at
/// construction, this tracks heap memory rather than mapped virtual address
/// space. The name "MMAP_WARM" is preserved for P10 INFO compatibility.
pub static RECL_MMAP_WARM_BYTES: AtomicU64 = AtomicU64::new(0);

/// Monotonically increasing counter of warm-segment evictions performed by
/// the budget enforcer. Each unit represents one `WarmSearchSegment` Arc
/// dropped from the `SegmentList.warm` list. The segment's on-disk files
/// remain intact; future searches reload it via `from_files`.
pub static RECL_MMAP_BUDGET_EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Increment the warm-resident-bytes counter by `delta`.
#[inline]
pub fn add_warm_resident(delta: u64) {
    RECL_MMAP_WARM_BYTES.fetch_add(delta, Ordering::Relaxed);
}

/// Decrement the warm-resident-bytes counter by `delta` (saturating at 0).
#[inline]
pub fn sub_warm_resident(delta: u64) {
    // Saturating subtract: counter must never wrap below zero.
    RECL_MMAP_WARM_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        Some(cur.saturating_sub(delta))
    }).ok();
}

/// Increment the eviction counter by `count`.
#[inline]
pub fn add_budget_evictions(count: u64) {
    RECL_MMAP_BUDGET_EVICTIONS_TOTAL.fetch_add(count, Ordering::Relaxed);
}

/// Read the current warm-resident-bytes value.
#[inline]
pub fn warm_resident_bytes() -> u64 {
    RECL_MMAP_WARM_BYTES.load(Ordering::Relaxed)
}

/// Read the cumulative budget-eviction counter.
#[inline]
pub fn budget_evictions_total() -> u64 {
    RECL_MMAP_BUDGET_EVICTIONS_TOTAL.load(Ordering::Relaxed)
}
