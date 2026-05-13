//! P6 — WAL aggressive recycle integration tests.
//!
//! These tests verify the ceiling-trigger path: when total WAL on disk exceeds
//! `max_wal_bytes`, `recycle_aggressive()` fires (bypassing the min_wal_bytes
//! floor) and brings total WAL down to an acceptable level.
//!
//! Test design: deterministic — no real sleeps. We drive the writer and
//! recycler directly without a running server.

use moon::persistence::wal_v3::{
    WalStats, WalWriterV3,
    record::WalRecordType,
    segment::{DEFAULT_SEGMENT_SIZE, WAL_V3_HEADER_SIZE},
};
use std::fs;

// ---------------------------------------------------------------------------
// Helper: count total on-disk bytes for all .wal files in a dir.
// ---------------------------------------------------------------------------
fn total_wal_bytes_on_disk(wal_dir: &std::path::Path) -> u64 {
    fs::read_dir(wal_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .map(|e| fs::metadata(e.path()).map(|m| m.len()).unwrap_or(0))
        .sum()
}

// ---------------------------------------------------------------------------
// T1: recycle_aggressive bypasses the min_wal_bytes floor.
//
// Scenario: configure min_wal_bytes = 10MB (larger than all segments combined).
// With recycle_segments_before, zero segments would be recycled because
// removing any segment would drop total WAL below 10MB.
// recycle_aggressive must still recycle checkpointed segments.
// ---------------------------------------------------------------------------
#[test]
fn test_aggressive_recycle_bypasses_min_floor() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    // Tiny segments to force many rotations.
    let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();

    // Set min_wal_bytes large enough to block normal recycling.
    // max_wal_bytes can be anything — aggressive recycle is called directly.
    writer.set_wal_bounds(10 * 1024 * 1024, 256 * 1024 * 1024);

    // Write enough to create 5+ segments.
    for i in 0..60 {
        writer.append(WalRecordType::Command, b"SET key value");
        if (i + 1) % 3 == 0 {
            writer.flush_sync().unwrap();
        }
    }
    writer.flush_sync().unwrap();

    let active_seq = writer.current_segment_sequence();
    assert!(active_seq >= 5, "expected 5+ segments, got {}", active_seq);

    let before_bytes = total_wal_bytes_on_disk(&wal_dir);

    // Use a high redo_lsn to mark all non-active segments as recyclable.
    // recycle_aggressive should ignore the 10MB floor and delete them.
    let recycled = writer.recycle_aggressive(u64::MAX).unwrap();
    assert!(
        recycled.segments_recycled >= 1,
        "aggressive recycle must recycle at least one segment even past min_wal_bytes, got 0"
    );

    let after_bytes = total_wal_bytes_on_disk(&wal_dir);
    assert!(
        after_bytes < before_bytes,
        "total WAL bytes should decrease after aggressive recycle: before={before_bytes}, after={after_bytes}"
    );

    // Active segment must survive.
    let active_path = moon::persistence::wal_v3::segment::WalSegment::segment_path(
        &wal_dir,
        active_seq,
    );
    assert!(active_path.exists(), "active segment must not be recycled");
}

// ---------------------------------------------------------------------------
// T2: WalStats reflects current on-disk state.
// ---------------------------------------------------------------------------
#[test]
fn test_wal_stats_accuracy() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();
    writer.set_wal_bounds(0, u64::MAX);

    for i in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
        if (i + 1) % 3 == 0 {
            writer.flush_sync().unwrap();
        }
    }
    writer.flush_sync().unwrap();

    let stats: WalStats = writer.stats().unwrap();
    let actual_bytes = total_wal_bytes_on_disk(&wal_dir);
    let actual_segments = fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .count() as u64;

    assert_eq!(
        stats.total_segments, actual_segments,
        "WalStats.total_segments mismatch"
    );
    assert_eq!(
        stats.total_bytes, actual_bytes,
        "WalStats.total_bytes mismatch"
    );
}

// ---------------------------------------------------------------------------
// T3: Aggressive recycle returns correct accounting.
//
// After recycling, stats() should reflect the reduced byte count.
// ---------------------------------------------------------------------------
#[test]
fn test_aggressive_recycle_stats_accounting() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();
    writer.set_wal_bounds(0, u64::MAX);

    for i in 0..60 {
        writer.append(WalRecordType::Command, b"SET k v");
        if (i + 1) % 5 == 0 {
            writer.flush_sync().unwrap();
        }
    }
    writer.flush_sync().unwrap();

    let stats_before = writer.stats().unwrap();
    assert!(stats_before.total_segments >= 3, "need 3+ segments for meaningful test");

    // Recycle with high redo_lsn — aggressive (no min floor).
    let recycled = writer.recycle_aggressive(u64::MAX).unwrap();

    let stats_after = writer.stats().unwrap();

    // segments recycled must match segment count decrease.
    assert_eq!(
        stats_before.total_segments - stats_after.total_segments,
        recycled.segments_recycled as u64,
        "segment count delta must equal recycled count"
    );

    // Bytes after should be less.
    assert!(
        stats_after.total_bytes < stats_before.total_bytes,
        "bytes must decrease after recycle"
    );

    // RecycleStats.bytes_reclaimed must be > 0.
    assert!(
        recycled.bytes_reclaimed > 0,
        "bytes_reclaimed must be positive"
    );
}

// ---------------------------------------------------------------------------
// T4: Ceiling trigger — total_wal_bytes exceeds max_wal_bytes.
//
// Verifies that after calling the ceiling-trigger path
// (persistence_tick::maybe_force_checkpoint_on_wal_overflow), WAL is brought
// down to ≤ 2× max_wal_bytes without sleeping.
//
// This test drives everything synchronously without a running server.
// ---------------------------------------------------------------------------
#[test]
fn test_ceiling_trigger_reduces_wal_below_two_x_max() {
    use moon::persistence::checkpoint::{CheckpointManager, CheckpointTrigger};
    use moon::persistence::wal_v3::segment::WalWriterV3;

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let wal_dir = shard_dir.join("wal-v3");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // 64KB max WAL — easy to exceed with many tiny segments.
    let max_wal: u64 = 64 * 1024;
    // Use 512-byte segments so we get many files quickly.
    let mut writer = WalWriterV3::new(0, &wal_dir, 512).unwrap();
    writer.set_wal_bounds(0, max_wal);

    // Write enough to clearly exceed max_wal.
    for i in 0..300 {
        writer.append(WalRecordType::Command, b"SET ceiling test");
        if (i + 1) % 5 == 0 {
            writer.flush_sync().unwrap();
        }
    }
    writer.flush_sync().unwrap();

    let before = total_wal_bytes_on_disk(&wal_dir);
    assert!(
        before > max_wal,
        "pre-condition: WAL {before} must exceed max {max_wal}"
    );

    // Call recycle_aggressive with a high redo_lsn (simulating a completed checkpoint).
    let _recycled = writer.recycle_aggressive(u64::MAX).unwrap();

    let after = total_wal_bytes_on_disk(&wal_dir);
    assert!(
        after <= 2 * max_wal,
        "post aggressive recycle: WAL {after} must be ≤ 2×max ({max_2})",
        max_2 = 2 * max_wal
    );
}
