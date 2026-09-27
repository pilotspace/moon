//! Adversarial review round 2 of moon#1269 (0aa15c5): DEL / UNLINK hand the
//! removed hot entry to an armed save by MOVE. These pin the interleavings
//! the commit's own tests do not name. All GREEN at ad55d3d (the review
//! found no hole here); they are regression guards.

use super::epoch_harness::{Epoch, run};
use super::*;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;

const FIELDS: usize = 5_000;

fn big_hash(dbs: &mut [Database], db: usize, key: &[u8]) {
    let mut parts: Vec<Vec<u8>> = vec![b"HSET".to_vec(), key.to_vec()];
    for i in 0..FIELDS {
        parts.push(format!("f{i:05}").into_bytes());
        parts.push(format!("v{i:05}").into_bytes());
    }
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    run(dbs, db, &refs);
}

/// `n` slot-stamped databases; db 0 has two big hashes and 2,000 fillers,
/// every other db 500 fillers and one big hash `other:big`.
fn fixture(n: usize) -> Vec<Database> {
    let mut dbs: Vec<Database> = (0..n)
        .map(|i| {
            let mut db = Database::new();
            db.db_index = i;
            db
        })
        .collect();
    big_hash(&mut dbs, 0, b"big:del");
    big_hash(&mut dbs, 0, b"big:unlink");
    for i in 0..2_000 {
        run(&mut dbs, 0, &[b"SET", format!("k{i:05}").as_bytes(), b"x"]);
    }
    for db in 1..n {
        big_hash(&mut dbs, db, b"other:big");
        for i in 0..500 {
            run(&mut dbs, db, &[b"SET", format!("o{i:05}").as_bytes(), b"y"]);
        }
    }
    dbs
}

fn hash_len_in(records: &[super::epoch_harness::Record], db: usize, key: &[u8]) -> Option<usize> {
    records
        .iter()
        .find(|(d, k, _)| *d == db && k.as_ref() == key)
        .map(|(_, _, e)| match e.value.as_redis_value() {
            crate::storage::compact_value::RedisValueRef::Hash(h) => h.len(),
            _ => panic!("not a hashtable"),
        })
}

/// A key ABSENT at the epoch start, created mid-epoch (its capture is a
/// tombstone, which is deliberately NOT entered in the first-wins dedupe
/// set) and then removed: the removal's MOVE capture is queued behind the
/// tombstone. The file must not hold the key, and `used_memory` must end
/// where an unarmed run ends (the held entry is disposed by the drain's
/// first-wins, after UNLINK already credited it).
#[test]
fn a_key_created_then_removed_mid_epoch_stays_absent_and_is_credited_once() {
    for cmd in [&b"DEL"[..], b"UNLINK"] {
        let mut control = fixture(1);
        big_hash(&mut control, 0, b"new:big");
        run(&mut control, 0, &[cmd, b"new:big"]);
        while control[0].drain_lazy_free_elements(1 << 20) > 0 {}

        let mut dbs = fixture(1);
        let epoch = Epoch::begin(&dbs);
        big_hash(&mut dbs, 0, b"new:big");
        assert_eq!(run(&mut dbs, 0, &[cmd, b"new:big"]), Frame::Integer(1));
        let records = epoch.finish(&dbs);
        while dbs[0].drain_lazy_free_elements(1 << 20) > 0 {}
        let name = String::from_utf8_lossy(cmd);
        assert!(
            hash_len_in(&records, 0, b"new:big").is_none(),
            "{name}: a key absent at the epoch start reached the file"
        );
        assert_eq!(records.len(), 2_002, "{name}");
        assert_eq!(
            dbs[0].estimated_memory(),
            control[0].estimated_memory(),
            "{name}: used_memory drifted"
        );
        assert_eq!(
            snapshot_cow::published_cow_size_for_test(),
            0,
            "{name}: cow size not back to 0 after the epoch"
        );
    }
}

/// SWAPDB mid-epoch (production keeps each slot's `db_index`, swapping the
/// CONTENTS): a removal through slot 1 — which now holds epoch database
/// 0's table — must file the moved entry under epoch database 0, and slot
/// 0's removal (epoch database 1's table) under 1.
#[test]
fn a_removal_after_a_mid_epoch_swapdb_files_under_the_epoch_database() {
    for cmd in [&b"DEL"[..], b"UNLINK"] {
        let mut dbs = fixture(2);
        let epoch = Epoch::begin(&dbs);
        snapshot_cow::note_swapdb(0, 1);
        dbs.swap(0, 1);
        // Production swaps contents and leaves each slot's stamp.
        dbs[0].db_index = 0;
        dbs[1].db_index = 1;
        let name = String::from_utf8_lossy(cmd);
        assert_eq!(run(&mut dbs, 1, &[cmd, b"big:del"]), Frame::Integer(1));
        assert_eq!(run(&mut dbs, 0, &[cmd, b"other:big"]), Frame::Integer(1));
        let records = epoch.finish(&dbs);
        assert_eq!(
            hash_len_in(&records, 0, b"big:del"),
            Some(FIELDS),
            "{name}: epoch db 0's key lost after SWAPDB"
        );
        assert_eq!(
            hash_len_in(&records, 1, b"other:big"),
            Some(FIELDS),
            "{name}: epoch db 1's key lost after SWAPDB"
        );
        assert!(hash_len_in(&records, 1, b"big:del").is_none(), "{name}");
        assert!(hash_len_in(&records, 0, b"other:big").is_none(), "{name}");
    }
}

/// `DEL k k` and `UNLINK k k`: the second occurrence finds nothing; the
/// file keeps the one epoch-start value, counted once.
#[test]
fn a_repeated_key_in_one_removal_is_captured_once() {
    let mut dbs = fixture(1);
    let epoch = Epoch::begin(&dbs);
    assert_eq!(
        run(&mut dbs, 0, &[b"DEL", b"big:del", b"big:del"]),
        Frame::Integer(1)
    );
    assert_eq!(
        run(&mut dbs, 0, &[b"UNLINK", b"big:unlink", b"big:unlink"]),
        Frame::Integer(1)
    );
    let records = epoch.finish(&dbs);
    assert_eq!(hash_len_in(&records, 0, b"big:del"), Some(FIELDS));
    assert_eq!(hash_len_in(&records, 0, b"big:unlink"), Some(FIELDS));
    assert_eq!(records.len(), 2_002);
}

/// A FLUSHDB after a held removal: the held pre-image still reaches the
/// file (the flush does not abort the save, redis keeps its child), and
/// the key written after the flush does not.
#[test]
fn a_flushdb_after_a_held_removal_keeps_the_pre_image() {
    let mut dbs = fixture(1);
    let epoch = Epoch::begin(&dbs);
    run(&mut dbs, 0, &[b"UNLINK", b"big:unlink"]);
    run(&mut dbs, 0, &[b"DEL", b"big:del"]);
    run(&mut dbs, 0, &[b"FLUSHDB"]);
    run(&mut dbs, 0, &[b"SET", b"post", b"p"]);
    let records = epoch.finish(&dbs);
    assert_eq!(hash_len_in(&records, 0, b"big:del"), Some(FIELDS));
    assert_eq!(hash_len_in(&records, 0, b"big:unlink"), Some(FIELDS));
    assert!(!records.iter().any(|(_, k, _)| k.as_ref() == b"post"));
    assert_eq!(records.len(), 2_002);
}
