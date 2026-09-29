//! moon#1288: the fast expiry cycle against a real shard slice.

use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::server::expire_adaptive::{self as fast, EXPIRE_FAST_SLICE_MAX};
use crate::shard::slice::{ShardSlice, init_shard, with_shard_db};
use crate::storage::Database;
use crate::storage::entry::{Entry, current_time_ms};

const EXPIRED: usize = 60_000;

fn run_slow(dbs: &std::sync::Arc<super::ShardDatabases>, is_replica: bool) {
    super::run_active_expiry(
        dbs,
        0,
        &mut None,
        &std::sync::Arc::new(parking_lot::Mutex::new(None)),
        &mut Vec::new(),
        &None,
        None,
        false,
        is_replica,
        1,
    );
}

fn run_fast(dbs: &std::sync::Arc<super::ShardDatabases>, is_replica: bool) -> bool {
    super::run_active_expiry_fast(
        dbs,
        0,
        &mut None,
        &std::sync::Arc::new(parking_lot::Mutex::new(None)),
        &mut Vec::new(),
        &None,
        None,
        false,
        is_replica,
    )
}

/// A shard whose slow cycle cannot finish latches a backlog; the fast
/// cycle then drains every expired key, one bounded slice per tick, and
/// leaves the live keys alone. Red on ce65400: there is no fast cycle —
/// the backlog shrank by one 1 ms slow cycle per 100 ms.
#[test]
fn the_fast_cycle_drains_a_backlog_in_bounded_slices() {
    std::thread::spawn(|| {
        let mut db = Database::new();
        let past = current_time_ms() - 1_000;
        let future = current_time_ms() + 3_600_000;
        for i in 0..EXPIRED {
            db.set(
                &Bytes::from(format!("gone:{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past),
            );
        }
        for i in 0..100 {
            db.set(
                &Bytes::from(format!("live:{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), future),
            );
        }
        let (dbs, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![db]]);
        init_shard(ShardSlice::new(inits.remove(0)));

        assert!(!fast::expire_backlog_pending());
        run_slow(&dbs, false);
        assert!(
            fast::expire_backlog_pending(),
            "fixture: one 1 ms slow cycle must not clear {EXPIRED} keys"
        );
        let mut ticks = 0u32;
        let t0 = Instant::now();
        while run_fast(&dbs, false) {
            ticks += 1;
            // Until the next 1 ms tick: the bucket earns its share meanwhile.
            std::thread::sleep(Duration::from_micros(200));
            assert!(t0.elapsed() < Duration::from_secs(60), "no progress");
        }
        let left = with_shard_db(0, |db| db.len());
        assert_eq!(left, 100, "every expired key removed, every live one kept");
        assert!(!fast::expire_backlog_pending());
        assert!(ticks > 1, "a backlog this size takes more than one slice");
    })
    .join()
    .expect("shard thread");
}

/// One fast tick never holds the shard much past `EXPIRE_FAST_SLICE_MAX`,
/// however much credit a late tick has banked.
#[test]
fn one_fast_tick_is_bounded_by_the_max_slice() {
    std::thread::spawn(|| {
        let mut db = Database::new();
        let past = current_time_ms() - 1_000;
        for i in 0..EXPIRED {
            db.set(
                &Bytes::from(format!("gone:{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past),
            );
        }
        let (dbs, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![db]]);
        init_shard(ShardSlice::new(inits.remove(0)));
        run_slow(&dbs, false);
        assert!(fast::expire_backlog_pending(), "fixture");
        // Bank far more than the cap.
        std::thread::sleep(Duration::from_millis(50));
        let before = with_shard_db(0, |db| db.len());
        let t = Instant::now();
        assert!(run_fast(&dbs, false), "{EXPIRED} keys outlast one slice");
        let took = t.elapsed();
        let removed = before - with_shard_db(0, |db| db.len());
        assert!(removed > 0, "the slice made progress");
        // 1 ms slice + one 64-pop overshoot; generous for debug builds.
        assert!(
            took < EXPIRE_FAST_SLICE_MAX * 10,
            "one fast tick held the shard {took:?}"
        );
    })
    .join()
    .expect("shard thread");
}

/// #71b: a replica never sweeps; the fast cycle clears the latch.
#[test]
fn a_replica_runs_no_fast_cycle() {
    std::thread::spawn(|| {
        let mut db = Database::new();
        let past = current_time_ms() - 1_000;
        for i in 0..EXPIRED {
            db.set(
                &Bytes::from(format!("gone:{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past),
            );
        }
        let (dbs, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![db]]);
        init_shard(ShardSlice::new(inits.remove(0)));
        run_slow(&dbs, false);
        assert!(fast::expire_backlog_pending(), "fixture");
        let before = with_shard_db(0, |db| db.len());
        assert!(!run_fast(&dbs, true));
        assert!(!fast::expire_backlog_pending());
        assert_eq!(with_shard_db(0, |db| db.len()), before);
    })
    .join()
    .expect("shard thread");
}
