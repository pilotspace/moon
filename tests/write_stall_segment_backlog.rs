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

/// Unit-level test: RECL_SEGMENT_STALL_ACTIVE exists, defaults to 0, is
/// writable, and drives `is_segment_stall_active()`.
///
/// ONE test (not two) on purpose: the atomic is a PROCESS-GLOBAL and the
/// test harness runs `#[test]`s in this binary on parallel threads — two
/// tests store/load-ing the same global raced (observed on CI macOS:
/// `store(1)` here read back `0` because the sibling helper test's final
/// `store(0)` interleaved). All mutation of the global lives in this single
/// test so there is nothing to race with.
///
/// RED: fails because RECL_SEGMENT_STALL_ACTIVE / is_segment_stall_active
/// do not exist.
#[test]
fn test_recl_segment_stall_active_atomic_and_helper() {
    use moon::command::info_reclamation::RECL_SEGMENT_STALL_ACTIVE;
    use moon::shard::segment_stall::is_segment_stall_active;

    // Must be zero by default (no other test in this binary touches it).
    assert_eq!(
        RECL_SEGMENT_STALL_ACTIVE.load(Ordering::Relaxed),
        0,
        "RECL_SEGMENT_STALL_ACTIVE must default to 0"
    );
    assert!(!is_segment_stall_active(), "must be false when atomic is 0");

    // Must be writable, and the helper must observe the change.
    RECL_SEGMENT_STALL_ACTIVE.store(1, Ordering::Relaxed);
    assert_eq!(RECL_SEGMENT_STALL_ACTIVE.load(Ordering::Relaxed), 1);
    assert!(is_segment_stall_active(), "must be true when atomic is 1");

    // Restore
    RECL_SEGMENT_STALL_ACTIVE.store(0, Ordering::Relaxed);
    assert!(
        !is_segment_stall_active(),
        "must be false again after restore"
    );
}
