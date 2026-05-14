//! `# Reclamation` INFO section — observability foundation for Wave-1 production
//! reclamation hardening (P10, v0.1.13).
//!
//! All 29 fields are plumbed here. Wave-2 agents fill in real values by
//! incrementing the public atomics exported from this module. Until then, every
//! field that lacks a real source emits `0` or `-1` as a sentinel, with a
//! `// TODO(P10→Wave2):` comment marking the wire point.
//!
//! ## Field naming
//! All fields use the `reclamation_` prefix, snake_case, one value per line in
//! standard Redis INFO format (`field:value\r\n`).
//!
//! ## Thread-safety
//! All counters are `AtomicU64` with `Relaxed` ordering — same pattern as
//! `crate::vector::metrics`. The INFO path reads them; subsystem event loops
//! write them. No lock required.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Public atomics — Wave-2 agents write these; INFO reads them.
// ---------------------------------------------------------------------------

/// Free bytes on the persistence volume (`statvfs f_bavail * f_bsize`).
/// MA12 owns this; emits 0 until wired.
pub static RECL_DISK_FREE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Total on-disk bytes used by all WAL segment files across all shards.
/// P6 (WAL recycle) will wire this during checkpoint.
pub static RECL_WAL_BYTES: AtomicU64 = AtomicU64::new(0);

/// Number of WAL segment files currently on disk across all shards.
/// P6 wires this alongside RECL_WAL_BYTES.
pub static RECL_WAL_SEGMENTS: AtomicU64 = AtomicU64::new(0);

/// 1 when the server is in a write-stall condition, 0 otherwise.
/// MA12 owns the disk-pressure bit. MA1 owns the segment-backlog bit.
/// The INFO emitter ORs both: `RECL_WRITE_STALL_ACTIVE | RECL_SEGMENT_STALL_ACTIVE`.
pub static RECL_WRITE_STALL_ACTIVE: AtomicU64 = AtomicU64::new(0);

/// 1 when immutable segment count exceeds --max-unflushed-immutable-segments, 0 otherwise.
/// MA1 sets this; the INFO `write_stall_active` field reflects its OR with RECL_WRITE_STALL_ACTIVE.
pub static RECL_SEGMENT_STALL_ACTIVE: AtomicU64 = AtomicU64::new(0);

/// Write-stall threshold stored as tenths-of-percent (e.g. 950 = 95.0%).
/// TODO(P10→Wave2): wire from config flag --disk-free-min-pct (MA12).
pub static RECL_WRITE_STALL_THRESHOLD_PCT_X10: AtomicU64 = AtomicU64::new(950);

/// Estimated bytes of segment data awaiting compaction (mutable → immutable).
/// TODO(P10→Wave2): wire from autovacuum engine.
pub static RECL_COMPACTION_PENDING_BYTES: AtomicU64 = AtomicU64::new(0);

/// Recent compaction throughput in bytes/sec (exponential moving average).
/// TODO(P10→Wave2): wire from autovacuum engine.
pub static RECL_COMPACTION_THROUGHPUT_BPS: AtomicU64 = AtomicU64::new(0);

/// P50 fan-out depth across segments during search.
/// TODO(P10→Wave2): wire from search path instrumentation.
pub static RECL_SEGMENT_FANOUT_P50: AtomicU64 = AtomicU64::new(0);

/// P99 fan-out depth across segments during search.
/// TODO(P10→Wave2): wire from search path instrumentation.
pub static RECL_SEGMENT_FANOUT_P99: AtomicU64 = AtomicU64::new(0);

/// P99 read amplification (bytes read / bytes returned).
/// TODO(P10→Wave2): wire from search path instrumentation.
pub static RECL_READ_AMP_P99: AtomicU64 = AtomicU64::new(0);

/// Maximum dead-fraction across all segments (stored as integer millipercent, 0–1000).
/// TODO(P10→Wave2): wire from segment GC / autovacuum.
pub static RECL_DEAD_FRACTION_MAX_X1000: AtomicU64 = AtomicU64::new(0);

/// Number of immutable (sealed HNSW) vector segments across all indexes.
/// TODO(P10→Wave2): wire from VectorStore tick or compaction callback.
pub static RECL_IMMUTABLE_SEGMENTS: AtomicU64 = AtomicU64::new(0);

/// Number of warm (mmap-backed) vector segments across all indexes.
/// TODO(P10→Wave2): wire from VectorStore warm-transition callback.
pub static RECL_WARM_SEGMENTS: AtomicU64 = AtomicU64::new(0);

/// Number of cold (DiskANN) vector segments across all indexes.
/// TODO(P10→Wave2): wire from VectorStore cold-transition callback.
pub static RECL_COLD_SEGMENTS: AtomicU64 = AtomicU64::new(0);

/// Number of immutable CSR graph segments across all named graphs.
/// TODO(P10→Wave2): wire from GraphStore compaction callback.
pub static RECL_GRAPH_SEGMENTS: AtomicU64 = AtomicU64::new(0);

/// Count of active (non-tombstone) entries in the shard manifest.
/// P1 (manifest GC) wires this during commit.
pub static RECL_MANIFEST_ACTIVE: AtomicU64 = AtomicU64::new(0);

/// Count of tombstone entries in the shard manifest awaiting physical removal.
/// P1 wires this alongside RECL_MANIFEST_ACTIVE.
pub static RECL_MANIFEST_TOMBSTONES: AtomicU64 = AtomicU64::new(0);

/// Bytes mapped by mmap'd warm vector segment files (OS page-cache backed).
/// TODO(P10→Wave2): wire from WarmSearchSegment on map/unmap.
pub static RECL_MMAP_WARM_BYTES: AtomicU64 = AtomicU64::new(0);

/// Number of committed transactions in the MVCC treemap (RoaringTreemap::len()).
/// MA2 wires this; emits 0 until pruning is implemented.
pub static RECL_MVCC_COMMITTED: AtomicU64 = AtomicU64::new(0);

/// Number of currently active (in-flight) MVCC transactions.
/// MA2 wires this.
pub static RECL_MVCC_ACTIVE: AtomicU64 = AtomicU64::new(0);

/// Oldest snapshot lag: `current_lsn - oldest_snapshot_lsn`.
/// MA2 wires this.
pub static RECL_MVCC_OLDEST_SNAPSHOT_LAG: AtomicU64 = AtomicU64::new(0);

/// Age of the oldest active snapshot in seconds (wall-clock delta since begin).
/// MA2 wires this.
pub static RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS: AtomicU64 = AtomicU64::new(0);

/// Cumulative count of zombie write-intents swept by the MVCC GC pass.
/// MA2 wires this.
pub static RECL_MVCC_ZOMBIES_SWEPT_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Unix timestamp of the last autovacuum run (seconds since epoch).
/// 0 = never run. TODO(P10→Wave2): wire from autovacuum scheduler.
pub static RECL_AUTOVACUUM_LAST_RUN_TS: AtomicU64 = AtomicU64::new(0);

/// Cumulative segments compacted by autovacuum across all runs.
/// TODO(P10→Wave2): wire from autovacuum engine.
pub static RECL_AUTOVACUUM_SEGMENTS_COMPACTED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Count of autovacuum runs throttled due to high server load.
/// TODO(P10→Wave2): wire from autovacuum scheduler.
pub static RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD: AtomicU64 = AtomicU64::new(0);

/// Graph plan-cache hit count (cumulative). Used to compute hit_ratio.
/// TODO(P10→Wave2): wire from PlanCache::get() on hit path.
pub static RECL_PLAN_CACHE_HITS: AtomicU64 = AtomicU64::new(0);

/// Graph plan-cache miss count (cumulative).
/// TODO(P10→Wave2): wire from PlanCache::get() on miss path.
pub static RECL_PLAN_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

/// Cumulative evictions from the graph plan cache (LRU).
/// TODO(P10→Wave2): wire from PlanCache::insert() on eviction path.
pub static RECL_PLAN_CACHE_EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// LSN below which all pending deletes are visible and safe to GC.
/// -1 = unknown (WAL not initialised or no deletes pending).
/// P8 wires this after P1+P3+MA2 are in place.
pub static RECL_DELETE_PENDING_VISIBLE_LSN: AtomicI64 = AtomicI64::new(-1);

// ---------------------------------------------------------------------------
// Section writer — zero-alloc, writes directly into caller's pre-sized buffer.
// ---------------------------------------------------------------------------

/// Append the `# Reclamation` INFO section to `buf`.
///
/// Reads all 29 fields from the module-level atomics above using `Relaxed`
/// ordering — same pattern as `crate::vector::metrics`. No allocation beyond
/// the caller's pre-sized `String`.
///
/// Sentinels for Wave-1:
/// - Unknown counters emit `0`.
/// - `delete_pending_visible_lsn` emits `-1` (no WAL or no pending deletes).
/// - Ratios stored as integer milliunits to avoid `f64` formatting overhead.
pub fn write_reclamation_section(buf: &mut String) {
    buf.push_str("# Reclamation\r\n");

    // -- Disk --
    let _ = write!(
        buf,
        "reclamation_disk_free_bytes:{}\r\n",
        RECL_DISK_FREE_BYTES.load(Ordering::Relaxed)
    );

    // -- WAL --
    let _ = write!(
        buf,
        "reclamation_wal_bytes:{}\r\n\
         reclamation_wal_segments:{}\r\n",
        RECL_WAL_BYTES.load(Ordering::Relaxed),
        RECL_WAL_SEGMENTS.load(Ordering::Relaxed)
    );

    // -- Write stall: OR of disk-pressure (MA12) and segment-backlog (MA1) bits --
    let stall_active = RECL_WRITE_STALL_ACTIVE.load(Ordering::Relaxed)
        | RECL_SEGMENT_STALL_ACTIVE.load(Ordering::Relaxed);
    // Threshold stored as tenths-of-percent (e.g. 950 → "95.0")
    let threshold_x10 = RECL_WRITE_STALL_THRESHOLD_PCT_X10.load(Ordering::Relaxed);
    let _ = write!(
        buf,
        "reclamation_write_stall_active:{}\r\n\
         reclamation_write_stall_threshold_pct:{}.{}\r\n",
        if stall_active != 0 { "true" } else { "false" },
        threshold_x10 / 10,
        threshold_x10 % 10,
    );

    // -- Compaction --
    let _ = write!(
        buf,
        "reclamation_compaction_pending_bytes:{}\r\n\
         reclamation_compaction_throughput_bps:{}\r\n",
        RECL_COMPACTION_PENDING_BYTES.load(Ordering::Relaxed),
        RECL_COMPACTION_THROUGHPUT_BPS.load(Ordering::Relaxed)
    );

    // -- Segment fanout / read amplification --
    let _ = write!(
        buf,
        "reclamation_segment_fanout_p50:{}\r\n\
         reclamation_segment_fanout_p99:{}\r\n\
         reclamation_read_amp_p99:{}\r\n",
        RECL_SEGMENT_FANOUT_P50.load(Ordering::Relaxed),
        RECL_SEGMENT_FANOUT_P99.load(Ordering::Relaxed),
        RECL_READ_AMP_P99.load(Ordering::Relaxed)
    );

    // -- Dead fraction: stored as integer millipercent (0–1000 = 0.000–1.000) --
    let dead_x1000 = RECL_DEAD_FRACTION_MAX_X1000.load(Ordering::Relaxed);
    let _ = write!(
        buf,
        "reclamation_dead_fraction_max:{}.{:03}\r\n",
        dead_x1000 / 1000,
        dead_x1000 % 1000,
    );

    // -- Vector segment tiers --
    let _ = write!(
        buf,
        "reclamation_immutable_segments:{}\r\n\
         reclamation_warm_segments:{}\r\n\
         reclamation_cold_segments:{}\r\n",
        RECL_IMMUTABLE_SEGMENTS.load(Ordering::Relaxed),
        RECL_WARM_SEGMENTS.load(Ordering::Relaxed),
        RECL_COLD_SEGMENTS.load(Ordering::Relaxed)
    );

    // -- Graph segments --
    let _ = write!(
        buf,
        "reclamation_graph_segments:{}\r\n",
        RECL_GRAPH_SEGMENTS.load(Ordering::Relaxed)
    );

    // -- Manifest --
    let _ = write!(
        buf,
        "reclamation_manifest_active:{}\r\n\
         reclamation_manifest_tombstones:{}\r\n",
        RECL_MANIFEST_ACTIVE.load(Ordering::Relaxed),
        RECL_MANIFEST_TOMBSTONES.load(Ordering::Relaxed)
    );

    // -- Mmap warm bytes --
    let _ = write!(
        buf,
        "reclamation_mmap_warm_bytes:{}\r\n",
        RECL_MMAP_WARM_BYTES.load(Ordering::Relaxed)
    );

    // -- MVCC --
    let _ = write!(
        buf,
        "reclamation_mvcc_committed:{}\r\n\
         reclamation_mvcc_active:{}\r\n\
         reclamation_mvcc_oldest_snapshot_lag:{}\r\n\
         reclamation_mvcc_oldest_snapshot_age_secs:{}\r\n\
         reclamation_mvcc_zombies_swept_total:{}\r\n",
        RECL_MVCC_COMMITTED.load(Ordering::Relaxed),
        RECL_MVCC_ACTIVE.load(Ordering::Relaxed),
        RECL_MVCC_OLDEST_SNAPSHOT_LAG.load(Ordering::Relaxed),
        RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS.load(Ordering::Relaxed),
        RECL_MVCC_ZOMBIES_SWEPT_TOTAL.load(Ordering::Relaxed)
    );

    // -- Autovacuum --
    let _ = write!(
        buf,
        "reclamation_autovacuum_last_run_ts:{}\r\n\
         reclamation_autovacuum_segments_compacted_total:{}\r\n\
         reclamation_autovacuum_throttled_due_to_load:{}\r\n",
        RECL_AUTOVACUUM_LAST_RUN_TS.load(Ordering::Relaxed),
        RECL_AUTOVACUUM_SEGMENTS_COMPACTED_TOTAL.load(Ordering::Relaxed),
        RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD.load(Ordering::Relaxed)
    );

    // -- Graph plan cache: ratio stored as integer milliunits (0–1000) --
    let hits = RECL_PLAN_CACHE_HITS.load(Ordering::Relaxed);
    let misses = RECL_PLAN_CACHE_MISSES.load(Ordering::Relaxed);
    let total = hits + misses;
    let hit_ratio_x1000 = if total == 0 {
        0u64
    } else {
        (hits * 1000) / total
    };
    let _ = write!(
        buf,
        "reclamation_plan_cache_hit_ratio:{}.{:03}\r\n\
         reclamation_plan_cache_evictions_total:{}\r\n",
        hit_ratio_x1000 / 1000,
        hit_ratio_x1000 % 1000,
        RECL_PLAN_CACHE_EVICTIONS_TOTAL.load(Ordering::Relaxed)
    );

    // -- Delete pending LSN (-1 = not yet wired) --
    let _ = write!(
        buf,
        "reclamation_delete_pending_visible_lsn:{}\r\n",
        RECL_DELETE_PENDING_VISIBLE_LSN.load(Ordering::Relaxed)
    );

    // -- Cold-tier orphan reclamation (P9) --
    let _ = write!(
        buf,
        "reclamation_cold_orphans_reclaimed_total:{}\r\n\
         reclamation_cold_orphan_bytes_reclaimed_total:{}\r\n",
        RECL_COLD_ORPHANS_RECLAIMED_TOTAL.load(Ordering::Relaxed),
        RECL_COLD_ORPHAN_BYTES_RECLAIMED_TOTAL.load(Ordering::Relaxed),
    );

    buf.push_str("\r\n");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// All 29 required field keys must appear in the reclamation section output.
    ///
    /// RED: fails until `write_reclamation_section` emits all 29 fields.
    #[test]
    fn info_reclamation_contains_all_required_fields() {
        let mut buf = String::with_capacity(2048);
        write_reclamation_section(&mut buf);

        let required_fields: &[&str] = &[
            "reclamation_disk_free_bytes:",
            "reclamation_wal_bytes:",
            "reclamation_wal_segments:",
            "reclamation_write_stall_active:",
            "reclamation_write_stall_threshold_pct:",
            "reclamation_compaction_pending_bytes:",
            "reclamation_compaction_throughput_bps:",
            "reclamation_segment_fanout_p50:",
            "reclamation_segment_fanout_p99:",
            "reclamation_read_amp_p99:",
            "reclamation_dead_fraction_max:",
            "reclamation_immutable_segments:",
            "reclamation_warm_segments:",
            "reclamation_cold_segments:",
            "reclamation_graph_segments:",
            "reclamation_manifest_active:",
            "reclamation_manifest_tombstones:",
            "reclamation_mmap_warm_bytes:",
            "reclamation_mvcc_committed:",
            "reclamation_mvcc_active:",
            "reclamation_mvcc_oldest_snapshot_lag:",
            "reclamation_mvcc_oldest_snapshot_age_secs:",
            "reclamation_mvcc_zombies_swept_total:",
            "reclamation_autovacuum_last_run_ts:",
            "reclamation_autovacuum_segments_compacted_total:",
            "reclamation_autovacuum_throttled_due_to_load:",
            "reclamation_plan_cache_hit_ratio:",
            "reclamation_plan_cache_evictions_total:",
            "reclamation_delete_pending_visible_lsn:",
        ];

        for field in required_fields {
            assert!(
                buf.contains(field),
                "missing required field {field:?} in reclamation section.\nFull output:\n{buf}"
            );
        }
    }

    /// Section must start with the `# Reclamation` header.
    #[test]
    fn info_reclamation_starts_with_header() {
        let mut buf = String::new();
        write_reclamation_section(&mut buf);
        assert!(
            buf.starts_with("# Reclamation\r\n"),
            "section must open with '# Reclamation\\r\\n'"
        );
    }

    /// Section must end with a blank separator line (`\r\n\r\n`).
    #[test]
    fn info_reclamation_ends_with_blank_line() {
        let mut buf = String::new();
        write_reclamation_section(&mut buf);
        assert!(
            buf.ends_with("\r\n\r\n"),
            "section must end with '\\r\\n\\r\\n' separator"
        );
    }

    /// Default sentinel for `write_stall_active` must be `false`.
    #[test]
    fn info_reclamation_write_stall_default_false() {
        let mut buf = String::new();
        write_reclamation_section(&mut buf);
        assert!(
            buf.contains("reclamation_write_stall_active:false\r\n"),
            "default write_stall_active must be false"
        );
    }

    /// Default `delete_pending_visible_lsn` must be `-1` (no-data sentinel).
    #[test]
    fn info_reclamation_delete_pending_lsn_default_minus_one() {
        let mut buf = String::new();
        write_reclamation_section(&mut buf);
        assert!(
            buf.contains("reclamation_delete_pending_visible_lsn:-1\r\n"),
            "delete_pending_visible_lsn must default to -1"
        );
    }

    /// Plan-cache hit ratio must be `0.000` when no queries have been processed.
    #[test]
    fn info_reclamation_plan_cache_ratio_zero_by_default() {
        let mut buf = String::new();
        write_reclamation_section(&mut buf);
        assert!(
            buf.contains("reclamation_plan_cache_hit_ratio:0.000\r\n"),
            "plan_cache_hit_ratio must be 0.000 when hits+misses == 0"
        );
    }

    /// Verify atomic wiring: setting RECL_WAL_BYTES reflects in output.
    #[test]
    fn info_reclamation_wal_bytes_wirable() {
        RECL_WAL_BYTES.store(12_345_678, Ordering::Relaxed);
        let mut buf = String::new();
        write_reclamation_section(&mut buf);
        RECL_WAL_BYTES.store(0, Ordering::Relaxed); // restore default
        assert!(
            buf.contains("reclamation_wal_bytes:12345678\r\n"),
            "RECL_WAL_BYTES store must be visible in section output"
        );
    }
}

// ---------------------------------------------------------------------------
// P9: Cold-tier orphan reclamation counters
// ---------------------------------------------------------------------------

/// Total cold-tier orphan index entries reclaimed since server start.
pub static RECL_COLD_ORPHANS_RECLAIMED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total bytes reclaimed from cold-tier orphan files since server start.
pub static RECL_COLD_ORPHAN_BYTES_RECLAIMED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Increment both orphan reclamation counters atomically.
#[inline]
pub fn record_cold_orphan_reclaim(bytes: u64) {
    RECL_COLD_ORPHANS_RECLAIMED_TOTAL.fetch_add(1, Ordering::Relaxed);
    RECL_COLD_ORPHAN_BYTES_RECLAIMED_TOTAL.fetch_add(bytes, Ordering::Relaxed);
}

/// Read current totals for the VACUUM/INFO response shape.
#[inline]
pub fn snapshot_cold_orphan_totals() -> (u64, u64) {
    (
        RECL_COLD_ORPHANS_RECLAIMED_TOTAL.load(Ordering::Relaxed),
        RECL_COLD_ORPHAN_BYTES_RECLAIMED_TOTAL.load(Ordering::Relaxed),
    )
}
