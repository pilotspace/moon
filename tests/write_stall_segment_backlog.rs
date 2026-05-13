/// MA1 RED tests — write-stall on immutable segment backlog.
///
/// These tests verify that when the total immutable segment count across
/// all indexes exceeds `max_unflushed_immutable_segments`, foreground write
/// commands (SET, HSET) return `MOONERR busy: compaction backlog` while
/// read commands (GET) continue to succeed.
///
/// RED: all tests fail until MA1 is implemented.
use std::sync::atomic::Ordering;

/// Unit-level test: VectorStore::total_immutable_segment_count() counts
/// immutable segments across all indexes and field segments.
///
/// RED: fails because `total_immutable_segment_count` does not exist.
#[test]
fn test_vector_store_total_immutable_count_method_exists() {
    let store = moon::vector::store::VectorStore::new();
    // Method must exist and return 0 for an empty store.
    let count = store.total_immutable_segment_count();
    assert_eq!(
        count, 0,
        "empty VectorStore must report 0 immutable segments"
    );
}

/// Unit-level test: RECL_SEGMENT_STALL_ACTIVE is updated by the segment-stall
/// check and OR-merged into RECL_WRITE_STALL_ACTIVE semantics.
///
/// RED: fails because RECL_SEGMENT_STALL_ACTIVE does not exist.
#[test]
fn test_recl_segment_stall_active_atomic_exists() {
    use moon::command::info_reclamation::RECL_SEGMENT_STALL_ACTIVE;
    // Must be zero by default.
    assert_eq!(
        RECL_SEGMENT_STALL_ACTIVE.load(Ordering::Relaxed),
        0,
        "RECL_SEGMENT_STALL_ACTIVE must default to 0"
    );
    // Must be writable.
    RECL_SEGMENT_STALL_ACTIVE.store(1, Ordering::Relaxed);
    assert_eq!(RECL_SEGMENT_STALL_ACTIVE.load(Ordering::Relaxed), 1);
    RECL_SEGMENT_STALL_ACTIVE.store(0, Ordering::Relaxed);
}

/// Unit-level test: `is_segment_stall_active()` returns true when
/// RECL_SEGMENT_STALL_ACTIVE is non-zero, false otherwise.
///
/// RED: fails because `is_segment_stall_active` does not exist.
#[test]
fn test_is_segment_stall_active_helper() {
    use moon::command::info_reclamation::RECL_SEGMENT_STALL_ACTIVE;
    use moon::shard::segment_stall::is_segment_stall_active;

    RECL_SEGMENT_STALL_ACTIVE.store(0, Ordering::Relaxed);
    assert!(!is_segment_stall_active(), "must be false when atomic is 0");

    RECL_SEGMENT_STALL_ACTIVE.store(1, Ordering::Relaxed);
    assert!(is_segment_stall_active(), "must be true when atomic is 1");

    // Restore
    RECL_SEGMENT_STALL_ACTIVE.store(0, Ordering::Relaxed);
}
