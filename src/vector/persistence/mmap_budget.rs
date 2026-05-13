//! Warm-segment resident-bytes budget with LRU eviction.
//!
//! # Problem
//!
//! Each `WarmSearchSegment` owns an in-memory copy of its codes and HNSW graph
//! (extracted from .mpf files at construction time). Under sustained ingest,
//! warm-segment count grows without bound: 1 K writes/s × 1000-vector compact
//! threshold = 86 K segments per day. At ~50 MB per segment that is ~4 TB of
//! heap pressure, triggering OOM-killer in cgroup-constrained containers.
//!
//! # Solution
//!
//! `MmapBudget` is a per-shard LRU tracker over `WarmSearchSegment` instances.
//! On every search the caller records an access (bumps LRU position). The
//! budget enforcer is called periodically (from the warm-check timer) and
//! removes the least-recently-accessed `WarmSearchSegment` Arcs from the
//! `SegmentList.warm` list until the tracked resident bytes fall below the
//! configured budget. The on-disk .mpf files remain intact; the next search
//! against an evicted segment causes `from_files` to reload it transparently.
//!
//! This mirrors `MADV_DONTNEED` semantics at the segment level: we release the
//! resident pages (owned Vec<u8>) to the OS and lazily fault them back on
//! access. The difference is that "faulting back" here is an explicit
//! `from_files` call rather than a kernel page-fault.
//!
//! # MADV_DONTNEED relationship
//!
//! `WarmSearchSegment::from_files` currently copies data out of the mmap into
//! owned `Vec<u8>` before returning — no live mmap persists in the search path.
//! A raw `MADV_DONTNEED` helper is provided in
//! [`crate::vector::persistence::sealed_mmap`] for future paths that retain
//! a persistent mapping. The budget mechanism here does not need to call it,
//! but the SAFETY reasoning is the same: sealed read-only files page back in
//! without data loss.
//!
//! # macOS note
//!
//! `madvise(MADV_DONTNEED)` exists on macOS but has advisory-only semantics —
//! the kernel may choose not to free the pages. The budget enforcement here
//! works on both platforms because it drops owned heap memory, not mapped
//! pages.

use std::collections::HashMap;

use crate::vector::segment::SegmentList;

/// Statistics returned by `enforce_budget`.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnforceStats {
    /// Number of warm segments evicted (Arcs dropped from `SegmentList.warm`).
    pub segments_evicted: u64,
    /// Resident bytes freed by eviction.
    pub bytes_freed: u64,
    /// Resident bytes remaining after enforcement.
    pub bytes_after: u64,
}

/// Per-segment accounting entry.
#[derive(Debug)]
struct SegmentEntry {
    /// Estimated resident bytes for this segment.
    resident_bytes: u64,
}

/// Per-shard warm-segment budget tracker.
///
/// LRU ordering is taken directly from `WarmSearchSegment::last_access_micros()`
/// which is bumped atomically on every search call. The budget struct only needs
/// to track resident bytes; it does not maintain its own clock.
///
/// `register_segment` is called by `enforce_segment_holder_budget` on each tick
/// for all warm segments currently in the list; it is idempotent (byte-delta
/// only on re-registration, no global-atomic drift).
///
/// This struct is `!Send` — it is owned exclusively by the shard event loop.
/// No interior mutability or locking is needed.
pub struct MmapBudget {
    /// Per-segment byte accounting, keyed by segment_id.
    entries: HashMap<u64, SegmentEntry>,
    /// Budget in bytes. 0 means enforcement is disabled.
    max_resident_bytes: u64,
    /// Current total of resident bytes across all tracked segments.
    total_resident_bytes: u64,
}

impl MmapBudget {
    /// Create a new budget tracker with the given limit.
    ///
    /// Set `max_resident_bytes = 0` to disable enforcement entirely.
    pub fn new(max_resident_bytes: u64) -> Self {
        Self {
            entries: HashMap::new(),
            max_resident_bytes,
            total_resident_bytes: 0,
        }
    }

    /// Register (or re-register) a warm segment's resident-byte count.
    ///
    /// On re-registration the global `RECL_MMAP_WARM_BYTES` atomic is adjusted
    /// by the signed delta rather than unconditionally adding the full size,
    /// preventing metric drift when this is called every tick.
    pub fn register_segment(&mut self, segment_id: u64, resident_bytes: u64) {
        if let Some(old) = self.entries.get_mut(&segment_id) {
            // Delta update: adjust totals by the difference only.
            let old_bytes = old.resident_bytes;
            if old_bytes != resident_bytes {
                self.total_resident_bytes = self.total_resident_bytes
                    .saturating_sub(old_bytes)
                    .saturating_add(resident_bytes);
                if resident_bytes >= old_bytes {
                    crate::admin::recl_atomics::add_warm_resident(resident_bytes - old_bytes);
                } else {
                    crate::admin::recl_atomics::sub_warm_resident(old_bytes - resident_bytes);
                }
                old.resident_bytes = resident_bytes;
            }
            // When size is unchanged, nothing to do (no atomic churn).
        } else {
            // New entry.
            self.entries.insert(segment_id, SegmentEntry { resident_bytes });
            self.total_resident_bytes =
                self.total_resident_bytes.saturating_add(resident_bytes);
            crate::admin::recl_atomics::add_warm_resident(resident_bytes);
        }
    }

    /// No-op: LRU ordering is read directly from `WarmSearchSegment::last_access_micros()`.
    ///
    /// Retained for API compatibility in case callers want to override access
    /// tracking in tests without live segments.
    #[inline]
    pub fn record_access(&mut self, _segment_id: u64) {}

    /// Remove a segment from the tracker (e.g. on cold-tier transition or drop).
    ///
    /// Self-healing reconciliation in `enforce_segment_holder_budget` also
    /// handles orphan removal, but explicit calls ensure immediate cleanup.
    pub fn remove_segment(&mut self, segment_id: u64) {
        if let Some(entry) = self.entries.remove(&segment_id) {
            self.total_resident_bytes =
                self.total_resident_bytes.saturating_sub(entry.resident_bytes);
            crate::admin::recl_atomics::sub_warm_resident(entry.resident_bytes);
        }
    }

    /// Current resident bytes tracked by this budget instance.
    pub fn current_resident_bytes(&self) -> u64 {
        self.total_resident_bytes
    }

    /// Iterator over all segment IDs currently tracked by this budget.
    ///
    /// Used by `enforce_segment_holder_budget` for self-healing reconciliation:
    /// tracked IDs that are no longer in the live warm list are removed.
    pub fn tracked_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.keys().copied()
    }

    /// Enforce the budget against `segment_list`.
    ///
    /// LRU order is determined by `WarmSearchSegment::last_access_micros()`
    /// (bumped atomically on every search call). Segments with the smallest
    /// `last_access_micros` are evicted first.
    ///
    /// The most-recently-accessed segment is protected from eviction when there
    /// is more than one candidate, to avoid disrupting active load.
    ///
    /// Returns statistics for logging and the INFO output.
    pub fn enforce_budget(&mut self, segment_list: &mut SegmentList) -> EnforceStats {
        if self.max_resident_bytes == 0
            || self.total_resident_bytes <= self.max_resident_bytes
        {
            return EnforceStats {
                segments_evicted: 0,
                bytes_freed: 0,
                bytes_after: self.total_resident_bytes,
            };
        }

        // Build a list of (last_access_micros, segment_id) for all tracked warm
        // segments. Use the segment Arc's atomic timestamp for true LRU ordering.
        let mut candidates: Vec<(u64, u64)> = segment_list
            .warm
            .iter()
            .filter_map(|arc| {
                let id = arc.segment_id();
                if self.entries.contains_key(&id) {
                    Some((arc.last_access_micros(), id))
                } else {
                    None
                }
            })
            .collect();

        if candidates.is_empty() {
            return EnforceStats {
                segments_evicted: 0,
                bytes_freed: 0,
                bytes_after: self.total_resident_bytes,
            };
        }

        // Sort ascending: index 0 is oldest access (evict first).
        candidates.sort_unstable_by_key(|(ts, _)| *ts);

        // Protect the MRU segment (last in sorted order) when > 1 candidate.
        let evict_count = if candidates.len() > 1 {
            candidates.len() - 1
        } else {
            candidates.len()
        };

        let mut evict_ids: std::collections::HashSet<u64> =
            std::collections::HashSet::with_capacity(evict_count);
        let mut bytes_freed: u64 = 0;
        let mut budget_remaining = self.total_resident_bytes;

        for (_, seg_id) in candidates.iter().take(evict_count) {
            if budget_remaining <= self.max_resident_bytes {
                break;
            }
            let seg_bytes = self.entries[seg_id].resident_bytes;
            evict_ids.insert(*seg_id);
            bytes_freed = bytes_freed.saturating_add(seg_bytes);
            budget_remaining = budget_remaining.saturating_sub(seg_bytes);
        }

        if evict_ids.is_empty() {
            return EnforceStats {
                segments_evicted: 0,
                bytes_freed: 0,
                bytes_after: self.total_resident_bytes,
            };
        }

        // Remove evicted segments from segment_list.warm.
        // Arc drop triggers SegmentHandle refcount decrement; directory removal
        // only occurs if the handle is tombstoned AND refcount hits zero.
        segment_list.warm.retain(|arc| !evict_ids.contains(&arc.segment_id()));

        // Update the tracker.
        for seg_id in &evict_ids {
            if let Some(entry) = self.entries.remove(seg_id) {
                self.total_resident_bytes =
                    self.total_resident_bytes.saturating_sub(entry.resident_bytes);
                crate::admin::recl_atomics::sub_warm_resident(entry.resident_bytes);
            }
        }

        let evicted = evict_ids.len() as u64;
        crate::admin::recl_atomics::add_budget_evictions(evicted);

        tracing::debug!(
            segments_evicted = evicted,
            bytes_freed,
            bytes_after = self.total_resident_bytes,
            "MmapBudget: evicted {} warm segment(s), freed {} bytes",
            evicted,
            bytes_freed,
        );

        EnforceStats {
            segments_evicted: evicted,
            bytes_freed,
            bytes_after: self.total_resident_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal test double for SegmentList warm eviction: we need real
    // Arc<WarmSearchSegment> objects. We build them via a helper that creates
    // .mpf files + WarmSearchSegment::from_files, same as the existing warm_search
    // tests.
    use std::path::Path;
    use std::sync::Arc;

    use crate::storage::tiered::SegmentHandle;
    use crate::vector::distance;
    use crate::vector::hnsw::graph::HnswGraph;
    use crate::vector::persistence::warm_search::WarmSearchSegment;
    use crate::vector::persistence::warm_segment::{write_codes_mpf, write_graph_mpf, write_mvcc_mpf};
    use crate::vector::segment::SegmentList;
    use crate::vector::segment::mutable::MutableSegment;
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::types::DistanceMetric;

    fn make_collection() -> Arc<CollectionMetadata> {
        Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ))
    }

    fn write_minimal_segment(seg_dir: &Path, seg_id: u64) {
        std::fs::create_dir_all(seg_dir).unwrap();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph_bytes = empty_graph.to_bytes();
        write_codes_mpf(&seg_dir.join("codes.mpf"), seg_id, &[]).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), seg_id, &graph_bytes).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), seg_id, &[]).unwrap();
    }

    fn make_warm_segment(tmp: &Path, seg_id: u64) -> Arc<WarmSearchSegment> {
        distance::init();
        let seg_dir = tmp.join(format!("seg-{seg_id}"));
        write_minimal_segment(&seg_dir, seg_id);
        let handle = SegmentHandle::new(seg_id, seg_dir.clone());
        let ws = WarmSearchSegment::from_files(
            &seg_dir,
            seg_id,
            make_collection(),
            handle,
            false,
        )
        .unwrap();
        Arc::new(ws)
    }

    fn empty_segment_list(warm: Vec<Arc<WarmSearchSegment>>) -> SegmentList {
        let collection = make_collection();
        SegmentList {
            mutable: Arc::new(MutableSegment::new(128, collection)),
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm,
            cold: Vec::new(),
        }
    }

    // ── RED tests (must pass after GREEN implementation) ──────────────────

    /// Budget=0 → enforcement is disabled, no segments evicted.
    #[test]
    fn test_budget_disabled_when_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = make_warm_segment(tmp.path(), 1);

        let mut budget = MmapBudget::new(0);
        budget.register_segment(1, ws.resident_bytes() as u64);

        let mut list = empty_segment_list(vec![ws]);
        let stats = budget.enforce_budget(&mut list);

        assert_eq!(stats.segments_evicted, 0);
        assert_eq!(list.warm.len(), 1, "no segment should be evicted when budget=0");
    }

    /// Budget above total → nothing evicted.
    #[test]
    fn test_no_eviction_under_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = make_warm_segment(tmp.path(), 2);
        let bytes = ws.resident_bytes() as u64;

        let mut budget = MmapBudget::new(bytes + 1_000_000);
        budget.register_segment(2, bytes);

        let mut list = empty_segment_list(vec![ws]);
        let stats = budget.enforce_budget(&mut list);

        assert_eq!(stats.segments_evicted, 0);
        assert_eq!(list.warm.len(), 1);
    }

    /// 5 segments, small budget → LRU segments are evicted, warm list shrinks.
    #[test]
    fn test_lru_eviction_reduces_warm_list() {
        let tmp = tempfile::tempdir().unwrap();
        let mut warm = Vec::new();

        for id in 1u64..=5 {
            warm.push(make_warm_segment(tmp.path(), id));
        }

        // Each segment is tiny (~0 bytes for empty graph). Use a budget of 0 bytes
        // to force all segments to be eviction candidates.
        let mut budget = MmapBudget::new(0);
        // Register all segments; because budget=0 enforcement is disabled via
        // the early-return guard, re-test with budget=1 after registration.
        for ws in &warm {
            budget.register_segment(ws.segment_id(), ws.resident_bytes() as u64);
        }
        // Re-configure to 1 byte to force eviction of all but MRU.
        let mut budget = MmapBudget::new(1);
        let tmp2 = tempfile::tempdir().unwrap();
        let mut warm2 = Vec::new();
        for id in 1u64..=5 {
            warm2.push(make_warm_segment(tmp2.path(), id));
        }
        for ws in &warm2 {
            budget.register_segment(ws.segment_id(), 1_000_000); // fake 1 MB each
        }
        // Record access on segment 5 (MRU) — should be protected.
        budget.record_access(5);

        let mut list = empty_segment_list(warm2.clone());
        let stats = budget.enforce_budget(&mut list);

        // Some segments must have been evicted.
        assert!(
            stats.segments_evicted > 0,
            "expected at least one eviction; got {stats:?}"
        );
        // bytes_after should be <= total - bytes_freed
        assert_eq!(
            stats.bytes_after,
            budget.current_resident_bytes(),
            "bytes_after should match tracker state"
        );
        // The warm list must have shrunk.
        assert!(
            list.warm.len() < 5,
            "warm list should have shrunk; len={}", list.warm.len()
        );
    }

    /// The segment most recently touched by a search is protected from eviction.
    ///
    /// LRU ordering now comes from `WarmSearchSegment::last_access_micros()` which
    /// is bumped by `search_filtered`. We create 3 segments, let w1/w2 get the
    /// initial (construction-time) timestamp, then trigger a search on w3 to make
    /// it the MRU. A short sleep ensures clock resolution separates the timestamps.
    #[test]
    fn test_mru_segment_protected_after_search() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();

        let w1 = make_warm_segment(tmp.path(), 10);
        let w2 = make_warm_segment(tmp.path(), 20);
        // Sleep briefly so w3's construction timestamp is clearly after w1/w2.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let w3 = make_warm_segment(tmp.path(), 30);

        // Perform a search on w3 to ensure its last_access_micros is the newest.
        {
            let query = vec![0.0f32; 128];
            let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
            let _ = w3.search(&query, 1, 10, &mut scratch);
        }

        let mut budget = MmapBudget::new(1);
        budget.register_segment(10, 1_000_000);
        budget.register_segment(20, 1_000_000);
        budget.register_segment(30, 1_000_000);

        let mut list = empty_segment_list(vec![w1, w2, w3.clone()]);
        let stats = budget.enforce_budget(&mut list);

        // At least one was evicted.
        assert!(stats.segments_evicted > 0, "expected eviction; stats={stats:?}");

        // Segment 30 (most recently searched) must still be in the list.
        let remaining_ids: Vec<u64> = list.warm.iter().map(|w| w.segment_id()).collect();
        assert!(
            remaining_ids.contains(&30),
            "MRU segment 30 should not be evicted; remaining={remaining_ids:?}"
        );
    }

    /// remove_segment removes from tracker and reduces resident bytes.
    #[test]
    fn test_remove_segment_updates_tracker() {
        let tmp = tempfile::tempdir().unwrap();
        // Create a warm segment to ensure the tmpdir stays alive for the test.
        let _ws = make_warm_segment(tmp.path(), 99);

        let mut budget = MmapBudget::new(u64::MAX);
        budget.register_segment(99, 500_000);
        assert_eq!(budget.current_resident_bytes(), 500_000);

        budget.remove_segment(99);
        assert_eq!(budget.current_resident_bytes(), 0);
        assert!(!budget.entries.contains_key(&99));
    }

    /// register_segment twice updates resident bytes without double-counting.
    #[test]
    fn test_register_segment_idempotent() {
        let mut budget = MmapBudget::new(u64::MAX);
        budget.register_segment(7, 100_000);
        budget.register_segment(7, 200_000); // re-register with different size
        assert_eq!(
            budget.current_resident_bytes(),
            200_000,
            "re-register must replace old size, not add"
        );
    }
}
