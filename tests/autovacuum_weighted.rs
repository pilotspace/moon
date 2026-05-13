//! MA4 — Weighted compaction scheduling tests.
//!
//! Red/Green TDD: these tests were written BEFORE the implementation.
//!
//! Design: the autovacuum daemon replaces round-robin with a priority queue
//! ordered by `dead_bytes_rate = bytes_dead / seconds_since_last_compaction`.
//! Highest-weighted entity runs first each tick. An anti-starvation cap
//! (`min_starvation_secs`, default 300) forces any entity that has not been
//! compacted for that long to be scheduled regardless of weight.
//!
//! Test suite:
//!  1. `test_compaction_scheduler_hottest_runs_first` — 3 entities with
//!     different dead_bytes_rate; after 5 ticks the hottest ran more than cold.
//!  2. `test_compaction_scheduler_starvation_cap` — coldest entity eventually
//!     runs even when dominated by hot entity, once starvation cap expires.
//!  3. `test_compaction_scheduler_empty_is_noop` — no entities: schedule() returns None.
//!  4. `test_compaction_scheduler_single_entity` — single entity always runs.
//!  5. `test_compaction_scheduler_weight_calculation` — dead_bytes_rate formula.

use moon::shard::autovacuum::{CompactionEntity, CompactionScheduler};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// 1. Hottest entity runs first (ordering verified directly)
// ---------------------------------------------------------------------------

#[test]
fn test_compaction_scheduler_hottest_runs_first() {
    let starvation_cap = Duration::from_secs(3600); // very long — starvation won't fire

    // Use the same elapsed for all three entities so only bytes_dead differs.
    // hot:  10_000 / 1s = 10_000 rate
    // warm:  1_000 / 1s =  1_000 rate
    // cold:    100 / 1s =    100 rate
    let base = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();

    // Pop order test: build a fresh scheduler and drain all 3 pops.
    // After each pop, re-insert with Instant::now() so the popped entity
    // has rate ≈ 0 and the remaining entities are compared correctly.
    let mut scheduler = CompactionScheduler::new(starvation_cap);
    scheduler.upsert(CompactionEntity {
        id: "hot".to_string(),
        bytes_dead: 10_000,
        last_compaction: base,
    });
    scheduler.upsert(CompactionEntity {
        id: "warm".to_string(),
        bytes_dead: 1_000,
        last_compaction: base,
    });
    scheduler.upsert(CompactionEntity {
        id: "cold".to_string(),
        bytes_dead: 100,
        last_compaction: base,
    });

    // First pop must be "hot" (highest rate).
    let first = scheduler.pop_next();
    assert_eq!(
        first.as_deref(),
        Some("hot"),
        "first pop must be hottest entity"
    );

    // Mark hot as freshly compacted (rate drops to ≈ 0).
    scheduler.upsert(CompactionEntity {
        id: "hot".to_string(),
        bytes_dead: 10_000,
        last_compaction: Instant::now(),
    });

    // Second pop must be "warm" (next highest).
    let second = scheduler.pop_next();
    assert_eq!(
        second.as_deref(),
        Some("warm"),
        "second pop must be warm entity"
    );

    // Mark warm as freshly compacted.
    scheduler.upsert(CompactionEntity {
        id: "warm".to_string(),
        bytes_dead: 1_000,
        last_compaction: Instant::now(),
    });

    // Third pop must be "cold".
    let third = scheduler.pop_next();
    assert_eq!(
        third.as_deref(),
        Some("cold"),
        "third pop must be cold entity"
    );
}

// ---------------------------------------------------------------------------
// 2. Starvation cap forces cold entity to run eventually
// ---------------------------------------------------------------------------

#[test]
fn test_compaction_scheduler_starvation_cap() {
    // Short starvation cap: 1 second.
    let starvation_cap = Duration::from_millis(500);

    let mut scheduler = CompactionScheduler::new(starvation_cap);

    // hot entity: massive dead_bytes_rate so it always wins on weight alone.
    let base_hot = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    let base_cold = Instant::now()
        .checked_sub(Duration::from_millis(600))
        .unwrap(); // > starvation_cap

    scheduler.upsert(CompactionEntity {
        id: "hot".to_string(),
        bytes_dead: 1_000_000,
        last_compaction: base_hot,
    });
    scheduler.upsert(CompactionEntity {
        id: "cold".to_string(),
        bytes_dead: 1, // tiny weight
        last_compaction: base_cold,
    });

    // Pop entities until cold gets a turn. It MUST get one within a bounded
    // number of pops because the starvation cap is exceeded.
    let mut cold_ran = false;
    for _ in 0..20 {
        if let Some(id) = scheduler.pop_next() {
            if id == "cold" {
                cold_ran = true;
                break;
            }
            // Re-upsert hot with fresh timestamp.
            scheduler.upsert(CompactionEntity {
                id: "hot".to_string(),
                bytes_dead: 1_000_000,
                last_compaction: Instant::now(),
            });
        }
    }

    assert!(
        cold_ran,
        "cold entity must run once starvation cap is exceeded"
    );
}

// ---------------------------------------------------------------------------
// 3. Empty scheduler returns None
// ---------------------------------------------------------------------------

#[test]
fn test_compaction_scheduler_empty_is_noop() {
    let mut scheduler = CompactionScheduler::new(Duration::from_secs(300));
    assert!(
        scheduler.pop_next().is_none(),
        "empty scheduler must return None"
    );
}

// ---------------------------------------------------------------------------
// 4. Single entity always runs
// ---------------------------------------------------------------------------

#[test]
fn test_compaction_scheduler_single_entity() {
    let mut scheduler = CompactionScheduler::new(Duration::from_secs(300));
    scheduler.upsert(CompactionEntity {
        id: "only".to_string(),
        bytes_dead: 500,
        last_compaction: Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
    });

    let result = scheduler.pop_next();
    assert_eq!(result.as_deref(), Some("only"));
}

// ---------------------------------------------------------------------------
// 5. Weight formula: dead_bytes_rate = bytes_dead / elapsed_secs
// ---------------------------------------------------------------------------

#[test]
fn test_compaction_scheduler_weight_calculation() {
    let mut scheduler = CompactionScheduler::new(Duration::from_secs(300));

    // Entity A: 1000 bytes dead, compacted 2s ago → weight ≈ 500/s
    let two_secs_ago = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
    // Entity B: 1000 bytes dead, compacted 1s ago → weight ≈ 1000/s
    let one_sec_ago = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();

    scheduler.upsert(CompactionEntity {
        id: "A".to_string(),
        bytes_dead: 1000,
        last_compaction: two_secs_ago,
    });
    scheduler.upsert(CompactionEntity {
        id: "B".to_string(),
        bytes_dead: 1000,
        last_compaction: one_sec_ago,
    });

    // B has higher weight (same bytes_dead but less time = higher rate).
    // Wait — actually: weight = bytes_dead / elapsed_secs.
    // A: 1000 / 2 = 500.  B: 1000 / 1 = 1000.
    // So B should pop first.
    let first = scheduler.pop_next();
    assert_eq!(
        first.as_deref(),
        Some("B"),
        "entity with higher dead_bytes_rate should pop first"
    );
}
