//! MA7 — Crash-recovery test matrix for Wave-1 reclamation code.
//!
//! Crash-injection scenarios, each proven with in-process simulation.
//! No subprocess spawning — `std::panic::catch_unwind` and drop-based
//! injection suffice because every code path under test is synchronous.
//!
//! # Approach: drop-before-commit instead of process kill
//!
//! The key insight for both remaining scenarios is that durability depends on
//! an explicit commit (either `ShardManifest::commit()` or an analogous
//! mutation applied to stable storage). A `drop` of the in-memory state
//! without a prior `commit()` is formally equivalent to a SIGKILL at the
//! exact moment before the `sync_data()` fence — the OS discards the
//! unsynchronised page-cache writes and the old root stays intact.
//!
//! This approach is:
//! - Runtime-agnostic (no monoio/tokio dependency)
//! - Deterministic (no sleep/timing)
//! - Fast (<1 ms total)
//! - Suitable for `cargo test --release` CI
//!
//! # Scenarios
//!
//! 1. **Manifest GC crash** — `gc_tombstones` mutates in-memory, drop
//!    before commit. Dual-root protocol recovers pre-staging root.
//! 2. **MVCC zombie sweep crash** — intents are ephemeral (not WAL-
//!    persisted). Simulates crash mid-sweep; verifies fresh TM is clean
//!    and sweep is idempotent. Documents the intent-durability gap.
//!
//! A former "compaction crash / orphan partial segment" scenario lived here,
//! exercising the WAL-replay `recover_vector_store` function. That function
//! (and the `src/vector/persistence/recovery.rs` module it lived in) has
//! been deleted (vector-index durability B3): it only ever populated the
//! `Shard`-owned `vector_store`, which is discarded wholesale at
//! `event_loop.rs` before the live `ShardSlice.vector_store` is built — see
//! `VECTOR-DURABILITY-DESIGN.md`. Recovery authority is now the B1/B2/B3
//! manifest+segment+keymap durability layout, and its orphan-cleanup
//! coverage lives in `src/vector/persistence/manifest.rs`'s
//! `sweep_orphans_from_disk` tests (`test_orphan_sweep_correctness` et al.),
//! which the B3 loader calls on every startup — the exact gap this deleted
//! scenario used to document no longer exists on the live path.

use std::time::{Duration, Instant};

// ── Scenario 1: Mid-GC manifest crash before commit ─────────────────────────
//
// Simulates: `gc_tombstones(0, 0, future)` is called (immediate retention →
// all tombstones eligible for removal). The process crashes (simulated by
// dropping the manifest) before `commit()` is called.
//
// Expected (and verified): the dual-root atomic-swap protocol means no durable
// change occurred. `ShardManifest::open()` reads the old active root with the
// tombstone still present and the original epoch intact.

#[test]
fn crash_manifest_gc_before_commit_recovers_pre_staging_root() {
    use moon::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
    use moon::persistence::page::PageType;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("shard-0.manifest");

    // Helper: make a minimal active FileEntry
    let make_entry = |id: u64| FileEntry {
        file_id: id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12,
        page_count: 100,
        byte_size: 409_600,
        created_lsn: id,
        min_key_hash: 0,
        max_key_hash: u64::MAX,
        last_modified_lsn: id,
    };

    // Create manifest, add two files, commit to stable epoch 2.
    let mut m = ShardManifest::create(&path).unwrap();
    m.add_file(make_entry(1));
    m.add_file(make_entry(2));
    m.commit().unwrap(); // epoch 2 on Root B (active_slot = 1)

    // Tombstone file #2 and commit — epoch 3 on Root A (active_slot = 0).
    m.remove_file(2);
    m.commit().unwrap(); // epoch 3 holds the tombstone

    // Capture the pre-staging state for assertions after simulated crash.
    let pre_crash_epoch = m.epoch();
    let pre_crash_tombstones = m.tombstone_count();

    assert_eq!(pre_crash_epoch, 3, "sanity: two commits from epoch 1 → 3");
    assert_eq!(pre_crash_tombstones, 1, "sanity: one tombstone present");

    {
        // Open a second handle and run gc_tombstones with zero retention —
        // all tombstones are eligible for immediate removal.
        // Then DROP WITHOUT COMMIT — this is the crash simulation.
        let mut m2 = ShardManifest::open(&path).unwrap();
        let future = Instant::now() + Duration::from_secs(9999);
        let pruned = m2.gc_tombstones(0, 0, future);
        assert_eq!(
            pruned, 1,
            "gc_tombstones should report 1 pruned entry in-memory"
        );
        assert_eq!(
            m2.tombstone_count(),
            0,
            "tombstone removed from in-memory view",
        );
        // Drop m2 WITHOUT calling commit() — simulates SIGKILL after staging,
        // before sync_data(). Because commit() is the atomic commit point
        // (see ShardManifest::commit at src/persistence/manifest.rs:378-411),
        // no durable change has been made.
    }

    // Recover: open a fresh handle and verify the pre-staging root is intact.
    let m3 = ShardManifest::open(&path).unwrap();

    assert_eq!(
        m3.epoch(),
        pre_crash_epoch,
        "post-crash recovery: epoch must match pre-staging value (no durability without commit)",
    );
    assert_eq!(
        m3.tombstone_count(),
        pre_crash_tombstones,
        "post-crash recovery: tombstone must still be present (gc_tombstones did not commit)",
    );
    assert_eq!(
        m3.active_entry_count(),
        1,
        "post-crash recovery: one non-tombstone entry (file_id=1) must be visible",
    );
}

// ── Scenario 2: Mid-sweep MVCC zombie crash + idempotency ───────────────────
//
// Context: `sweep_zombies_mut` drains zombie write-intents in-place (mutating
// `write_intents` via `HashMap::retain`). Write-intents are NOT WAL-persisted
// — they live only in the in-memory `TransactionManager`. On process restart
// (fresh `TransactionManager::new()`), the intents map starts empty.
//
// "Crash mid-sweep" means: the retain loop has executed for k of N intents,
// then SIGKILL. On next startup the TransactionManager is fresh — all intents
// (including the ones that weren't yet swept) are gone, which is safe because:
//   - Aborted intents are invisible to committed readers.
//   - In-flight intents whose VectorUpsert WAL records exist are rolled back
//     by WAL replay in `recover_vector_store`.
//
// This test covers:
//   (a) Inject 3 real zombie intents (owner txn_id not in active/committed).
//       Assert sweep_zombies_mut returns 3 and the intents map is empty.
//       Assert a second sweep returns 0 (idempotency on same instance).
//   (b) Inject 3 zombies again, "crash" by dropping the TM without sweeping.
//       Fresh TM has 0 intents; sweep returns 0 (restart = clean slate).
//   (c) Mixed bag: 1 zombie, 1 live intent (active owner). Sweep removes only
//       the zombie; live intent survives.
//
// // BUG: write-intents are not WAL-persisted. If the server crashes between
// // a VectorUpsert WAL record being appended and the corresponding intent being
// // added to write_intents (the two are not atomic), a subsequent restart may
// // allow a second writer to claim the same point_id before the WAL-replayed
// // mutable segment entry is made visible. For pure-writer workloads this is
// // invisible, but mixed workloads can exhibit a write-write inconsistency
// // window during the WAL replay phase.
// // Source: src/vector/mvcc/manager.rs:51 (write_intents field) and
// // src/vector/persistence/wal_record.rs — no WriteIntent frame type exists.

#[test]
#[cfg(feature = "crash-injection")]
fn crash_mvcc_sweep_zombies_idempotent_and_clean_on_restart() {
    use moon::vector::mvcc::manager::TransactionManager;

    // ── (a) Real zombies are swept; sweep is idempotent on same instance ─────

    let mut mgr = TransactionManager::new();

    // Inject 3 zombie intents: owner txn_id 9001/9002/9003 are not in active
    // or committed (they never existed in this TM). These model intents whose
    // owning transactions were aborted by another mechanism (e.g. a previous
    // run's crash) without going through the normal abort() cleanup path.
    mgr.inject_zombie_for_test(1001, 9001);
    mgr.inject_zombie_for_test(1002, 9002);
    mgr.inject_zombie_for_test(1003, 9003);

    assert_eq!(mgr.write_intent_count(), 3, "3 zombie intents injected");

    // First sweep: all 3 are zombies (owner neither active nor committed nor
    // below pruned_below=0 because 9001 > 0 and not committed).
    let swept_first = mgr.sweep_zombies_mut();
    assert_eq!(
        swept_first, 3,
        "sweep_zombies_mut must remove all 3 zombies"
    );
    assert_eq!(
        mgr.write_intent_count(),
        0,
        "write_intents must be empty after sweep",
    );

    // Second sweep on same instance — idempotent, nothing left to remove.
    let swept_second = mgr.sweep_zombies_mut();
    assert_eq!(swept_second, 0, "repeated sweep is idempotent: returns 0");

    // ── (b) Simulated crash mid-sweep: fresh TM starts with 0 intents ────────
    //
    // Inject zombies then DROP WITHOUT sweeping — models a SIGKILL between the
    // inject (compaction writing a segment dir) and the sweep timer firing.
    // On restart, a fresh TransactionManager is constructed; intents are gone.

    {
        let mut mgr_pre_crash = TransactionManager::new();
        mgr_pre_crash.inject_zombie_for_test(2001, 8001);
        mgr_pre_crash.inject_zombie_for_test(2002, 8002);
        mgr_pre_crash.inject_zombie_for_test(2003, 8003);
        assert_eq!(mgr_pre_crash.write_intent_count(), 3);
        // Drop without sweeping — simulates crash.
    }

    // BUG: the 3 intents above are NOT persisted to WAL and are silently lost.
    // For aborted transactions this is safe. For in-flight writes it creates
    // a window where a second writer can claim the same point_id during replay.
    // BUG: src/vector/mvcc/manager.rs:51 + src/vector/persistence/wal_record.rs
    // (no WriteIntent frame type).

    let mut mgr_post_restart = TransactionManager::new();
    assert_eq!(
        mgr_post_restart.write_intent_count(),
        0,
        "fresh TM after simulated crash: write_intents is empty",
    );
    let swept_restart = mgr_post_restart.sweep_zombies_mut();
    assert_eq!(
        swept_restart, 0,
        "sweep on fresh TM returns 0 — there is nothing to sweep",
    );

    // ── (c) Mixed bag: zombie co-exists with live intent ─────────────────────
    //
    // Inject one zombie (fake owner) alongside one live intent (real active
    // txn). sweep_zombies_mut must remove only the zombie and leave the live
    // intent intact.

    let mut mgr_mixed = TransactionManager::new();

    // Live intent: t_live owns point 3001.
    let t_live = mgr_mixed.begin();
    mgr_mixed
        .acquire_write(3001, t_live.txn_id)
        .expect("live intent must succeed");

    // Zombie intent: fake owner 7777 for point 3002.
    mgr_mixed.inject_zombie_for_test(3002, 7777);

    assert_eq!(mgr_mixed.write_intent_count(), 2);

    let swept_mixed = mgr_mixed.sweep_zombies_mut();
    assert_eq!(swept_mixed, 1, "sweep removes only the zombie intent");
    assert_eq!(
        mgr_mixed.write_intent_count(),
        1,
        "live intent for t_live survives the sweep",
    );

    // Commit the live transaction — intent is released.
    mgr_mixed.commit(t_live.txn_id);
    assert_eq!(
        mgr_mixed.write_intent_count(),
        0,
        "commit releases the last intent",
    );
}
