//! moon#1221 review F2: the shard tick's drain reports whether THIS shard
//! still has lazy-free work queued — the monoio loop's idle park keys on it,
//! so the flag must be exact per shard (the process-wide pending counter
//! would keep an idle shard awake for another shard's queue).

use std::collections::HashMap;
use std::sync::mpsc;

use bytes::Bytes;

use super::{LAZY_FREE_SWEEP_TICKS, drain_lazy_free_tick};
use crate::shard::db_plane::exclusive_count;
use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
use crate::storage::compact_value::CompactValue;
use crate::storage::entry::{Entry, RedisValue};

fn huge_hash(fields: usize) -> Entry {
    let mut h = HashMap::new();
    for i in 0..fields {
        h.insert(
            Bytes::from(format!("field-{i:07}").into_bytes()),
            Bytes::from_static(b"v"),
        );
    }
    let mut e = Entry::new_string(Bytes::new());
    e.value = CompactValue::from_redis_value(RedisValue::Hash(Box::new(h)));
    e
}

#[test]
fn the_tick_reports_work_left_on_this_shard_only() {
    let (queued_tx, queued_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    // Shard A: db 1 holds a value no single 250 µs slice can free.
    let a = std::thread::spawn(move || {
        init_shard(ShardSlice::new(make_init(0, 2)));
        with_shard_db(1, |db| {
            db.set(b"big", huge_hash(200_000));
            assert!(db.unlink(b"big"));
        });
        queued_tx.send(()).expect("signal");
        go_rx.recv().expect("wait");
        let first = drain_lazy_free_tick(2);
        let mut ticks = 1u32;
        while drain_lazy_free_tick(2) {
            ticks += 1;
            assert!(ticks < 1_000_000, "the drain made no progress");
        }
        (first, with_shard_db(1, |db| db.lazy_free_len()))
    });
    queued_rx.recv().expect("shard A queued its value");
    // Shard B: nothing queued, while shard A's queue is not empty.
    let b = std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(1, 2)));
        (
            crate::storage::db::lazy_free_pending_anywhere(),
            drain_lazy_free_tick(2),
        )
    })
    .join()
    .expect("shard B");
    go_tx.send(()).expect("release shard A");
    let (first, left) = a.join().expect("shard A");
    assert!(b.0, "fixture: shard A's queue must be pending process-wide");
    assert!(
        !b.1,
        "a shard with nothing queued must report no lazy-free work, whatever \
         other shards hold"
    );
    assert!(first, "a slice that cannot finish must report work left");
    assert_eq!(
        left, 0,
        "the tick must report pending until the queue is empty"
    );
}

/// moon#1226: while another shard drains a large value, a shard with
/// nothing queued takes NO database guard on its tick — it used to take
/// the write guard of all 16 databases every 1 ms. The periodic safety
/// sweep (every `LAZY_FREE_SWEEP_TICKS`-th tick) is the one exception.
#[test]
fn an_idle_shard_takes_no_guard_while_another_shard_drains() {
    let (queued_tx, queued_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let a = std::thread::spawn(move || {
        init_shard(ShardSlice::new(make_init(0, 16)));
        with_shard_db(3, |db| {
            db.set(b"big", huge_hash(200_000));
            assert!(db.unlink(b"big"));
        });
        queued_tx.send(()).expect("signal");
        go_rx.recv().expect("wait");
        while drain_lazy_free_tick(16) {}
    });
    queued_rx.recv().expect("shard A queued its value");
    let b = std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(1, 16)));
        assert!(crate::storage::db::lazy_free_pending_anywhere(), "fixture");
        let before = exclusive_count::get();
        for _ in 1..LAZY_FREE_SWEEP_TICKS {
            assert!(!drain_lazy_free_tick(16));
        }
        let idle = exclusive_count::get() - before;
        // The sweep tick visits every database once, finds nothing.
        let before = exclusive_count::get();
        assert!(!drain_lazy_free_tick(16));
        (idle, exclusive_count::get() - before)
    })
    .join()
    .expect("shard B");
    go_tx.send(()).expect("release shard A");
    a.join().expect("shard A");
    assert_eq!(
        b.0,
        0,
        "an idle shard took {} database write guards over {} ticks while another \
         shard had lazy-free work queued (16 per tick before moon#1226)",
        b.0,
        LAZY_FREE_SWEEP_TICKS - 1
    );
    assert_eq!(b.1, 16, "the safety sweep visits each database once");
}

/// moon#1226: the first database a tick drains rotates, so a large value in
/// db 0 cannot take every tick's whole budget while db 1's queue waits.
#[test]
fn the_first_database_drained_rotates() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 2)));
        for db_i in 0..2 {
            with_shard_db(db_i, |db| {
                db.set(b"big", huge_hash(200_000));
                assert!(db.unlink(b"big"));
            });
        }
        let charged = |i| with_shard_db(i, |db| db.estimated_memory());
        let (m0, m1) = (charged(0), charged(1));
        assert!(
            drain_lazy_free_tick(2),
            "one slice cannot free 2 x 200K fields"
        );
        let (a0, a1) = (charged(0), charged(1));
        assert!(drain_lazy_free_tick(2));
        let (b0, b1) = (charged(0), charged(1));
        // One tick started at each database: each one moved once.
        let first = (m0 - a0, m1 - a1);
        let second = (a0 - b0, a1 - b1);
        assert!(
            (first.0 > 0 && second.1 > 0) || (first.1 > 0 && second.0 > 0),
            "two ticks must each start at a different database: tick 1 freed \
             {first:?} bytes (db0, db1), tick 2 {second:?}"
        );
        while drain_lazy_free_tick(2) {}
        assert_eq!(with_shard_db(0, |db| db.lazy_free_len()), 0);
        assert_eq!(with_shard_db(1, |db| db.lazy_free_len()), 0);
    })
    .join()
    .expect("shard thread");
}
