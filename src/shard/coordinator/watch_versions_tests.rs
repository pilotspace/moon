//! moon#1183: `snapshot_versions` (WATCH) against fake owner shards.
//!
//! - The local group is read under the SHARED guard.
//! - Owners the fast path does not serve are ALL asked before any reply is
//!   awaited: m owners cost one round trip, not m in series. Proved with a
//!   barrier, not a clock — each fake owner holds its reply until every owner
//!   has received its request (or 2 s pass), and reports whether it saw all.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ringbuf::traits::{Consumer, Split};
use ringbuf::{HeapProd, HeapRb};

use crate::runtime::channel;
use crate::shard::db_plane::exclusive_count;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;

fn block_on_with_timer<F: std::future::Future>(fut: F) -> F::Output {
    #[cfg(feature = "runtime-monoio")]
    {
        monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(fut)
    }
    #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime")
            .block_on(fut)
    }
}

fn key_on(shard: usize, num_shards: usize, tag: &str) -> Bytes {
    (0..)
        .map(|i| Bytes::from(format!("{tag}:{i}")))
        .find(|k| key_to_shard(k, num_shards) == shard)
        .expect("a key for every shard")
}

#[test]
fn the_local_group_takes_no_exclusive_guard() {
    let mut db = Database::new();
    db.set_string(b"a", Bytes::from_static(b"1"));
    let (_sd, mut inits) = ShardDatabases::new(vec![vec![db]]);
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
    let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> = Rc::new(RefCell::new(Vec::new()));
    let keys = [Bytes::from_static(b"a"), Bytes::from_static(b"missing")];

    let before = exclusive_count::get();
    let versions = block_on_with_timer(super::snapshot_versions(&keys, 0, 1, 0, &dispatch_tx, &[]));
    let exclusive = exclusive_count::get() - before;

    assert_ne!(versions[0], 0, "a live key has a version");
    assert_eq!(versions[1], 0, "an absent key is version 0");
    assert_eq!(
        exclusive, 0,
        "a WATCH snapshot is a read: it must not hold the db exclusively"
    );
}

#[test]
fn every_declined_owner_is_asked_before_any_reply_is_awaited() {
    const SHARDS: usize = 4;
    let (_sd, mut inits) =
        ShardDatabases::new((0..SHARDS).map(|_| vec![Database::new()]).collect());
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));

    // Shard 0 is this thread; shards 1..=3 are fake owners.
    let arrived = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let saw_all = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut producers = Vec::new();
    let mut owners = Vec::new();
    for _ in 1..SHARDS {
        let (prod, mut cons) = HeapRb::<ShardMessage>::new(8).split();
        producers.push(prod);
        let (arrived, stop, saw_all) = (arrived.clone(), stop.clone(), saw_all.clone());
        owners.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let Some(msg) = cons.try_pop() else {
                    std::thread::sleep(Duration::from_micros(50));
                    continue;
                };
                if let ShardMessage::ReadVersions(payload) = msg {
                    arrived.fetch_add(1, Ordering::SeqCst);
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while arrived.load(Ordering::SeqCst) < SHARDS - 1 && Instant::now() < deadline {
                        std::thread::sleep(Duration::from_micros(100));
                    }
                    saw_all
                        .lock()
                        .push(arrived.load(Ordering::SeqCst) == SHARDS - 1);
                    let _ = payload.reply_tx.send(vec![7; payload.keys.len()]);
                }
            }
        }));
    }
    let dispatch_tx = Rc::new(RefCell::new(producers));
    let notifiers: Vec<Arc<channel::Notify>> = (0..SHARDS)
        .map(|_| Arc::new(channel::Notify::new()))
        .collect();
    let keys: Vec<Bytes> = (1..SHARDS).map(|s| key_on(s, SHARDS, "wv")).collect();

    let versions = block_on_with_timer(super::snapshot_versions(
        &keys,
        0,
        SHARDS,
        0,
        &dispatch_tx,
        &notifiers,
    ));
    stop.store(true, Ordering::Relaxed);
    for o in owners {
        let _ = o.join();
    }

    assert_eq!(
        versions,
        vec![7; SHARDS - 1],
        "every owner's versions came back"
    );
    assert_eq!(
        *saw_all.lock(),
        vec![true; SHARDS - 1],
        "each owner must see every owner's request before any reply is awaited \
         (sequential round trips: the first owner waits alone)"
    );
}
