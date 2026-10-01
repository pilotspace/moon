//! moon#1295 — a large collection's epoch-start image is streamed into the
//! snapshot as its own block before an in-place write, instead of copied.
//!
//! Driven like the event loop drives it (`epoch_harness`): the stream service
//! runs every tick before the walk, also while the walk is held. Each test
//! checks the file holds every key EXACTLY once at its epoch-start value.

use bytes::Bytes;

use super::epoch_harness::{Epoch, Record, run};
use super::key_stream::{ChunkLimit, KeyCursor};
use super::*;
use crate::persistence::snapshot_cow::{self, stream};
use crate::protocol::Frame;
use crate::storage::compact_value::RedisValueRef;

/// Elements per big collection: above every listpack limit.
const N: usize = 600;
const FILLER: usize = 1_000;

/// Lower the stream threshold and the per-tick budget on this thread (unit
/// tests run in parallel threads); restored on drop.
struct Knobs;

impl Knobs {
    fn set(tick: usize) -> Self {
        stream::set_knobs_for_test(Some(100), Some(tick));
        Knobs
    }
}

impl Drop for Knobs {
    fn drop(&mut self) {
        stream::set_knobs_for_test(None, None);
        snapshot_cow::disarm();
    }
}

fn argv(parts: &[&[u8]]) -> Vec<Frame> {
    parts
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
        .collect()
}

fn fill(dbs: &mut [Database], db: usize, cmd: &[u8], key: &[u8]) {
    let mut parts: Vec<Vec<u8>> = vec![cmd.to_vec(), key.to_vec()];
    for i in 0..N {
        if cmd == b"HSET" {
            parts.push(format!("f{i:04}").into_bytes());
            parts.push(format!("v{i:04}").into_bytes());
        } else if cmd == b"ZADD" {
            parts.push(format!("{i}.5").into_bytes());
            parts.push(format!("m{i:04}").into_bytes());
        } else {
            parts.push(format!("e{i:04}").into_bytes());
        }
    }
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    assert!(!matches!(run(dbs, db, &refs), Frame::Error(_)));
}

/// Two slot-stamped databases: filler strings in both, a big hash and set in
/// db 0, a big list and sorted set in db 1.
fn fixture() -> Vec<Database> {
    let mut dbs: Vec<Database> = (0..2)
        .map(|i| {
            let mut db = Database::new();
            db.db_index = i;
            db
        })
        .collect();
    for db in 0..2 {
        for i in 0..FILLER {
            run(&mut dbs, db, &[b"SET", format!("k{i:05}").as_bytes(), b"x"]);
        }
    }
    fill(&mut dbs, 0, b"HSET", b"big:h");
    fill(&mut dbs, 0, b"SADD", b"big:s");
    fill(&mut dbs, 1, b"RPUSH", b"big:l");
    fill(&mut dbs, 1, b"ZADD", b"big:z");
    dbs
}

/// A value's contents, comparable across instances (list order kept).
fn content(e: &Entry) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = match e.value.as_redis_value() {
        RedisValueRef::Hash(m) => m.iter().map(|(f, v)| (f.to_vec(), v.to_vec())).collect(),
        RedisValueRef::List(l) => {
            return l
                .iter()
                .enumerate()
                .map(|(i, v)| (i.to_le_bytes().to_vec(), v.to_vec()))
                .collect();
        }
        RedisValueRef::Set(s) => s.iter().map(|m| (m.to_vec(), Vec::new())).collect(),
        RedisValueRef::SortedSet { members, .. }
        | RedisValueRef::SortedSetBPTree { members, .. } => members
            .iter()
            .map(|(m, s)| (m.to_vec(), s.to_le_bytes().to_vec()))
            .collect(),
        RedisValueRef::String(s) => vec![(s.to_vec(), Vec::new())],
        _ => panic!("unexpected encoding"),
    };
    out.sort();
    out
}

/// Every `(db, key) -> contents` of `dbs`.
fn keyspace(dbs: &[Database]) -> Vec<(usize, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let mut out = Vec::new();
    for (i, db) in dbs.iter().enumerate() {
        for (k, e) in db.data().iter() {
            out.push((i, k.as_bytes().to_vec(), content(e)));
        }
    }
    out.sort();
    out
}

/// The records of a file as the same shape, asserting no key repeats.
fn records_keyspace(records: &[Record]) -> Vec<(usize, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let mut out: Vec<_> = records
        .iter()
        .map(|(db, k, e)| (*db, k.to_vec(), content(e)))
        .collect();
    out.sort();
    let mut keys: Vec<(usize, Vec<u8>)> = out.iter().map(|(d, k, _)| (*d, k.clone())).collect();
    keys.dedup();
    assert_eq!(keys.len(), out.len(), "a key is in the file more than once");
    out
}

fn admit(dbs: &[Database], slot: usize, parts: &[&[u8]]) -> Option<flume::Receiver<()>> {
    stream::admit_write_in(&dbs[slot], slot, parts[0], &argv(&parts[1..]))
}

/// Held ticks until no request is queued and no stream is active.
fn stream_all(epoch: &mut Epoch, dbs: &[Database]) -> usize {
    let mut ticks = 0;
    while stream::busy() {
        epoch.held_tick(dbs);
        ticks += 1;
        assert!(ticks < 1_000_000, "the stream never finished");
    }
    ticks
}

#[test]
fn key_cursor_writes_exactly_what_write_entry_writes_in_any_chunks() {
    let dbs = fixture();
    for (db, key) in [
        (0, &b"big:h"[..]),
        (0, b"big:s"),
        (1, b"big:l"),
        (1, b"big:z"),
    ] {
        let entry = dbs[db].data().get(key).expect("fixture key");
        let mut whole = Vec::new();
        rdb::write_entry(&mut whole, key, entry).unwrap();
        for chunk in [1usize, 37, 4096, usize::MAX] {
            let mut out = Vec::new();
            let mut cursor = KeyCursor::begin(key, entry, &mut out).expect("streamable");
            let limit = ChunkLimit {
                bytes: chunk,
                time: None,
            };
            let mut calls = 0;
            while !cursor.write_some(entry, &mut out, limit) {
                calls += 1;
                assert!(cursor.matches(entry));
            }
            assert_eq!(out, whole, "{key:?} in chunks of {chunk} bytes");
            if chunk == 1 {
                assert!(calls > 1, "a 1-byte budget must take several chunks");
            }
        }
    }
}

#[test]
fn small_ttl_and_unarmed_values_are_not_streamed() {
    let _k = Knobs::set(256);
    let mut dbs = fixture();
    snapshot_cow::disarm();
    assert!(
        admit(&dbs, 0, &[b"HSET", b"big:h", b"f", b"v"]).is_none(),
        "disarmed"
    );
    run(&mut dbs, 0, &[b"HSET", b"small", b"f", b"v"]);
    run(&mut dbs, 0, &[b"EXPIRE", b"big:s", b"1000"]);
    let epoch = Epoch::begin(&dbs);
    assert!(
        admit(&dbs, 0, &[b"HSET", b"small", b"g", b"v"]).is_none(),
        "small"
    );
    assert!(
        admit(&dbs, 0, &[b"SADD", b"big:s", b"new"]).is_none(),
        "key TTL"
    );
    assert!(admit(&dbs, 0, &[b"GET", b"big:h"]).is_none(), "a read");
    assert!(
        admit(&dbs, 0, &[b"DEL", b"big:h"]).is_none(),
        "a removal moves"
    );
    assert!(!stream::busy());
    drop(epoch);
}

/// The issue's case: an in-place write to a large pending hash waits while
/// the hash streams (the walk paused), then runs with no copy; the file
/// holds the hash once, at its epoch-start value.
#[test]
fn a_waiting_write_streams_the_epoch_start_image_once_without_a_copy() {
    let _k = Knobs::set(256);
    let mut dbs = fixture();
    let start = keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let rx = admit(&dbs, 0, &[b"HSET", b"big:h", b"new", b"x"]).expect("must wait");
    assert!(rx.try_recv().is_err());
    // A second writer of the key waits too.
    let rx2 = admit(&dbs, 0, &[b"HDEL", b"big:h", b"f0001"]).expect("waits too");
    epoch.held_tick(&dbs);
    let state = epoch.state.as_ref().unwrap();
    assert!(state.key_block_open_for_test(), "streams across ticks");
    let cursor = state.cursor();
    // The walk is paused while the block is open.
    assert!(!epoch.tick(&dbs));
    assert_eq!(epoch.state.as_ref().unwrap().cursor(), cursor);
    let ticks = stream_all(&mut epoch, &dbs);
    assert!(
        ticks > 10,
        "a 256-byte budget streams over many ticks ({ticks})"
    );
    assert!(matches!(
        rx.try_recv(),
        Err(flume::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        rx2.try_recv(),
        Err(flume::TryRecvError::Disconnected)
    ));
    assert!(admit(&dbs, 0, &[b"HSET", b"big:h", b"new", b"x"]).is_none());
    let clones = snapshot_cow::pre_image_clones_for_test();
    assert_eq!(
        run(&mut dbs, 0, &[b"HSET", b"big:h", b"new", b"x"]),
        Frame::Integer(1)
    );
    assert_eq!(
        run(&mut dbs, 0, &[b"HDEL", b"big:h", b"f0001"]),
        Frame::Integer(1)
    );
    assert_eq!(
        snapshot_cow::pre_image_clones_for_test(),
        clones,
        "the write copied the hash"
    );
    let records = epoch.finish(&dbs);
    assert_eq!(records_keyspace(&records), start);
}

/// Four kinds in two databases, streamed while the walk is in db 0: the
/// key blocks select their database and the walk re-selects its own.
#[test]
fn every_kind_in_every_database_streams_back_its_epoch_start_value() {
    let _k = Knobs::set(512);
    let mut dbs = fixture();
    let start = keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    // Part of db 0 written first.
    epoch.tick_one(&dbs);
    let writes: [(usize, &[&[u8]]); 4] = [
        (1, &[b"LPUSH", b"big:l", b"head"]),
        (0, &[b"SADD", b"big:s", b"new"]),
        (1, &[b"ZADD", b"big:z", b"-1", b"new"]),
        (0, &[b"HSET", b"big:h", b"new", b"x"]),
    ];
    let pending: Vec<_> = writes
        .iter()
        .filter_map(|(db, w)| admit(&dbs, *db, w))
        .collect();
    stream_all(&mut epoch, &dbs);
    assert!(
        pending
            .iter()
            .all(|rx| matches!(rx.try_recv(), Err(flume::TryRecvError::Disconnected)))
    );
    for (db, w) in writes {
        assert!(admit(&dbs, db, w).is_none());
        assert!(!matches!(run(&mut dbs, db, w), Frame::Error(_)));
    }
    let records = epoch.finish(&dbs);
    assert_eq!(records_keyspace(&records), start);
}

/// A writer that cannot wait (a MULTI body, a script) of a key that is
/// streaming finishes the stream inline before it writes — no copy.
#[test]
fn a_writer_that_cannot_wait_finishes_the_stream_inline() {
    let _k = Knobs::set(128);
    let mut dbs = fixture();
    let start = keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let rx = admit(&dbs, 0, &[b"HSET", b"big:h", b"new", b"x"]).expect("must wait");
    epoch.held_tick(&dbs);
    assert!(epoch.state.as_ref().unwrap().key_block_open_for_test());
    let clones = snapshot_cow::pre_image_clones_for_test();
    assert_eq!(
        run(&mut dbs, 0, &[b"HSET", b"big:h", b"other", b"y"]),
        Frame::Integer(1)
    );
    assert_eq!(snapshot_cow::pre_image_clones_for_test(), clones);
    assert!(matches!(
        rx.try_recv(),
        Err(flume::TryRecvError::Disconnected)
    ));
    let records = epoch.finish(&dbs);
    assert_eq!(records_keyspace(&records), start);
}

/// A removal of a key that is streaming (eviction, a DEL in MULTI) finishes
/// the stream from the value before it takes it away.
#[test]
fn a_removal_mid_stream_finishes_the_stream_first() {
    for cmd in [&b"DEL"[..], b"UNLINK"] {
        let _k = Knobs::set(128);
        let mut dbs = fixture();
        let start = keyspace(&dbs);
        let mut epoch = Epoch::begin(&dbs);
        let _rx = admit(&dbs, 1, &[b"LPUSH", b"big:l", b"x"]).expect("must wait");
        epoch.held_tick(&dbs);
        assert_eq!(run(&mut dbs, 1, &[cmd, b"big:l"]), Frame::Integer(1));
        let records = epoch.finish(&dbs);
        assert_eq!(records_keyspace(&records), start, "{cmd:?}");
    }
}

/// A key queued but not streaming yet, written by a writer that cannot
/// wait: the request is dropped and the write copies, as before moon#1295.
#[test]
fn a_queued_key_written_by_a_writer_that_cannot_wait_is_copied() {
    let _k = Knobs::set(128);
    let mut dbs = fixture();
    let start = keyspace(&dbs);
    let epoch = Epoch::begin(&dbs);
    let rx = admit(&dbs, 0, &[b"SADD", b"big:s", b"x"]).expect("must wait");
    let clones = snapshot_cow::pre_image_clones_for_test();
    assert_eq!(
        run(&mut dbs, 0, &[b"SADD", b"big:s", b"y"]),
        Frame::Integer(1)
    );
    assert_eq!(snapshot_cow::pre_image_clones_for_test(), clones + 1);
    assert!(matches!(
        rx.try_recv(),
        Err(flume::TryRecvError::Disconnected)
    ));
    assert!(!stream::busy());
    let records = epoch.finish(&dbs);
    assert_eq!(records_keyspace(&records), start);
}

/// SWAPDB mid-stream: the stream follows the table to its new slot.
#[test]
fn swapdb_mid_stream_follows_the_table() {
    let _k = Knobs::set(128);
    let mut dbs = fixture();
    let start = keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let _rx = admit(&dbs, 1, &[b"ZADD", b"big:z", b"1", b"x"]).expect("must wait");
    epoch.held_tick(&dbs);
    assert!(epoch.state.as_ref().unwrap().key_block_open_for_test());
    snapshot_cow::note_swapdb(0, 1);
    dbs.swap(0, 1);
    dbs[0].db_index = 0;
    dbs[1].db_index = 1;
    let records = epoch.finish(&dbs);
    assert_eq!(records_keyspace(&records), start);
}

/// FLUSHDB mid-stream: the stream finishes from the detached table.
#[test]
fn flushdb_mid_stream_finishes_from_the_detached_table() {
    let _k = Knobs::set(128);
    let mut dbs = fixture();
    let start = keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let _rx = admit(&dbs, 0, &[b"HSET", b"big:h", b"a", b"b"]).expect("must wait");
    epoch.held_tick(&dbs);
    assert!(epoch.state.as_ref().unwrap().key_block_open_for_test());
    assert!(!matches!(run(&mut dbs, 0, &[b"FLUSHDB"]), Frame::Error(_)));
    let records = epoch.finish(&dbs);
    assert_eq!(records_keyspace(&records), start);
}

/// A value replaced under the stream by a path no guard saw must fail the
/// save loudly, never publish a block that does not parse.
#[test]
fn a_value_changed_under_the_stream_fails_the_save() {
    let _k = Knobs::set(128);
    let mut dbs = fixture();
    let mut epoch = Epoch::begin(&dbs);
    let _rx = admit(&dbs, 0, &[b"HSET", b"big:h", b"a", b"b"]).expect("must wait");
    epoch.held_tick(&dbs);
    dbs[0].set(b"big:h", Entry::new_string(Bytes::from_static(b"replaced")));
    epoch.held_tick(&dbs);
    assert!(epoch.state.as_ref().unwrap().aborted().is_some());
    assert!(!stream::busy());
    assert!(epoch.try_finish(&dbs).is_err());
}

/// moon#1300: a large key an open TXN holds streams its PRE-transaction
/// image from the hold, whether the hold is released mid-stream (handed
/// over by move) or before the stream started (filed as the pre-image).
#[test]
fn a_held_key_streams_its_pre_transaction_image() {
    use crate::transaction::isolation;
    for release_before_first_tick in [false, true] {
        let _k = Knobs::set(128);
        let mut dbs = fixture();
        let start = keyspace(&dbs);
        let txn = 4_242;
        isolation::txn_begin(txn);
        let key = Bytes::from_static(b"big:h");
        let pre = dbs[0].data().get(b"big:h").cloned();
        assert!(isolation::hold(0, &key, txn, || pre));
        // The transaction's own write, before the save starts.
        let owner = isolation::OwnerScope::enter(txn);
        assert_eq!(
            run(&mut dbs, 0, &[b"HSET", b"big:h", b"txn", b"1"]),
            Frame::Integer(1)
        );
        drop(owner);
        let mut epoch = Epoch::begin(&dbs);
        assert!(stream::busy(), "the held image is queued, not copied");
        if release_before_first_tick {
            isolation::txn_end(txn);
        } else {
            epoch.held_tick(&dbs);
            assert!(epoch.state.as_ref().unwrap().key_block_open_for_test());
            // The transaction writes again, mid-stream: the source is the hold.
            let owner = isolation::OwnerScope::enter(txn);
            assert_eq!(
                run(&mut dbs, 0, &[b"HSET", b"big:h", b"txn2", b"1"]),
                Frame::Integer(1)
            );
            drop(owner);
            isolation::txn_end(txn);
        }
        let records = epoch.finish(&dbs);
        assert_eq!(
            records_keyspace(&records),
            start,
            "release before the first tick: {release_before_first_tick}"
        );
    }
}
