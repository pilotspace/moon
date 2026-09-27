//! moon#1269 — `DEL` / `UNLINK` of a large value while a save is running.
//!
//! The dispatch hook used to deep-clone every written key's value before the
//! command ran; for a removal that copy was pure waste (the original was
//! freed a moment later) and O(elements) on the shard thread — a 5M-field
//! hash UNLINKed mid-save stalled the shard ~1 s. The removed entry is now
//! the pre-image, MOVED. These tests pin: no deep clone, the file still holds
//! the epoch-start value, first capture wins, and `used_memory` ends where an
//! unarmed run ends.

use super::epoch_harness::{Epoch, run};
use super::*;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;

const FIELDS: usize = 5_000;

fn big_hash(dbs: &mut [Database], key: &[u8]) {
    let mut parts: Vec<Vec<u8>> = vec![b"HSET".to_vec(), key.to_vec()];
    for i in 0..FIELDS {
        parts.push(format!("f{i:05}").into_bytes());
        parts.push(format!("v{i:05}").into_bytes());
    }
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    run(dbs, 0, &refs);
}

/// One slot-stamped database with two big hashes and filler keys (so the
/// walk has many segments and the hashes' ranges are still pending).
fn fixture() -> Vec<Database> {
    let mut db = Database::new();
    db.db_index = 0;
    let mut dbs = vec![db];
    big_hash(&mut dbs, b"big:del");
    big_hash(&mut dbs, b"big:unlink");
    for i in 0..2_000 {
        run(&mut dbs, 0, &[b"SET", format!("k{i:05}").as_bytes(), b"x"]);
    }
    dbs
}

fn hash_len_in(records: &[super::epoch_harness::Record], key: &[u8]) -> Option<usize> {
    records
        .iter()
        .find(|(_, k, _)| k.as_ref() == key)
        .map(|(_, _, e)| match e.value.as_redis_value() {
            crate::storage::compact_value::RedisValueRef::Hash(h) => h.len(),
            _ => panic!("not a hashtable"),
        })
}

#[test]
fn del_and_unlink_of_pending_large_values_move_them_into_the_image_without_a_clone() {
    let mut dbs = fixture();
    let epoch = Epoch::begin(&dbs);
    let clones = snapshot_cow::pre_image_clones_for_test();
    assert_eq!(run(&mut dbs, 0, &[b"DEL", b"big:del"]), Frame::Integer(1));
    assert_eq!(
        run(&mut dbs, 0, &[b"UNLINK", b"big:unlink"]),
        Frame::Integer(1)
    );
    assert_eq!(
        snapshot_cow::pre_image_clones_for_test(),
        clones,
        "DEL/UNLINK deep-cloned a value they were about to discard"
    );
    assert!(dbs[0].data().get(b"big:del").is_none());
    assert!(dbs[0].data().get(b"big:unlink").is_none());
    let records = epoch.finish(&dbs);
    assert_eq!(
        hash_len_in(&records, b"big:del"),
        Some(FIELDS),
        "epoch-start value lost"
    );
    assert_eq!(hash_len_in(&records, b"big:unlink"), Some(FIELDS));
    assert_eq!(records.len(), 2_002, "every epoch-start key exactly once");
}

/// A write earlier in the epoch captured the key first (by copy): the removal
/// must not replace that epoch-start state with its (modified) entry.
#[test]
fn a_removal_after_an_earlier_write_keeps_the_first_capture() {
    let mut dbs = fixture();
    let epoch = Epoch::begin(&dbs);
    run(&mut dbs, 0, &[b"HSET", b"big:del", b"extra", b"1"]);
    run(&mut dbs, 0, &[b"HSET", b"big:unlink", b"extra", b"1"]);
    run(&mut dbs, 0, &[b"DEL", b"big:del"]);
    run(&mut dbs, 0, &[b"UNLINK", b"big:unlink"]);
    let records = epoch.finish(&dbs);
    assert_eq!(hash_len_in(&records, b"big:del"), Some(FIELDS));
    assert_eq!(hash_len_in(&records, b"big:unlink"), Some(FIELDS));
}

/// A key the walk has already written needs no pre-image: the removal frees
/// as it always did, and the file holds the key once.
#[test]
fn a_removal_behind_the_walk_is_not_held() {
    let mut dbs = fixture();
    let mut epoch = Epoch::begin(&dbs);
    while !epoch.tick(&dbs) {}
    run(&mut dbs, 0, &[b"DEL", b"big:del"]);
    run(&mut dbs, 0, &[b"UNLINK", b"big:unlink"]);
    assert!(
        snapshot_cow::pending_for_test().is_empty(),
        "nothing captured"
    );
    let records = epoch.finish(&dbs);
    assert_eq!(hash_len_in(&records, b"big:del"), Some(FIELDS));
    assert_eq!(records.len(), 2_002);
}

/// `used_memory` after a held UNLINK/DEL equals an unarmed run's once the
/// lazy-free queue has drained: the held entry is credited exactly once.
#[test]
fn a_held_removal_credits_used_memory_exactly_once() {
    let mut control = fixture();
    run(&mut control, 0, &[b"DEL", b"big:del"]);
    run(&mut control, 0, &[b"UNLINK", b"big:unlink"]);
    while control[0].drain_lazy_free_elements(1 << 20) > 0 {}

    let mut dbs = fixture();
    let epoch = Epoch::begin(&dbs);
    run(&mut dbs, 0, &[b"DEL", b"big:del"]);
    run(&mut dbs, 0, &[b"UNLINK", b"big:unlink"]);
    assert_eq!(dbs[0].lazy_free_len(), 0, "the held value is not queued");
    let _ = epoch.finish(&dbs);
    while dbs[0].drain_lazy_free_elements(1 << 20) > 0 {}
    assert_eq!(dbs[0].estimated_memory(), control[0].estimated_memory());
}

/// A cold-only or absent key: nothing to capture, the image unaffected.
#[test]
fn removing_an_absent_key_mid_epoch_changes_nothing() {
    let mut dbs = fixture();
    let epoch = Epoch::begin(&dbs);
    assert_eq!(run(&mut dbs, 0, &[b"DEL", b"nope"]), Frame::Integer(0));
    assert_eq!(run(&mut dbs, 0, &[b"UNLINK", b"nope"]), Frame::Integer(0));
    let records = epoch.finish(&dbs);
    assert_eq!(records.len(), 2_002);
}
