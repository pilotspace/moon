//! moon#1285 / moon#1185: a `TXN.ABORT` in the middle of a save.
//!
//! A transaction's writes are live the moment they run, so a BGSAVE epoch
//! armed between a write and its abort starts from the UNCOMMITTED value.
//! The image must hold that value (it is the keyspace at the epoch instant
//! F), and the abort's compensating records — logged at or above F — must
//! replay on top of it to the aborted-to state: the fold's exactly-once
//! contract. The abort used to write through `Database` with no capture at
//! all, so the walk serialized whatever the abort had restored.
//!
//! These drive the real undo (`kv_compensation::undo_one`) against the
//! snapshot epoch harness, with NO hold table: the undo's own capture. Since
//! moon#1300 a real transaction's keys are held, and an epoch armed while
//! they are holds their PRE-transaction value from the start instead
//! (`txn_after_start_tests::a_txn_write_before_the_start_is_saved_at_its_pre_txn_value`).

use bytes::Bytes;

use super::epoch_harness::{Epoch, Record, run};
use super::*;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;
use crate::transaction::UndoLog;
use crate::transaction::kv_compensation::{self, CompensatingRecord};

const FIELDS: usize = 4_000;

/// Database 0, slot-stamped: filler (so the walk has many segments and the
/// keys under test are still pending when the abort runs), a big hash and
/// the keys the transaction touches.
fn fixture() -> Vec<Database> {
    let mut db = Database::new();
    db.db_index = 0;
    let mut dbs = vec![db];
    for i in 0..2_000 {
        run(&mut dbs, 0, &[b"SET", format!("k{i:05}").as_bytes(), b"x"]);
    }
    let mut hset: Vec<Vec<u8>> = vec![b"HSET".to_vec(), b"big".to_vec()];
    for i in 0..FIELDS {
        hset.push(format!("f{i:05}").into_bytes());
        hset.push(b"v".to_vec());
    }
    let refs: Vec<&[u8]> = hset.iter().map(Vec::as_slice).collect();
    run(&mut dbs, 0, &refs);
    run(&mut dbs, 0, &[b"SET", b"upd", b"original"]);
    run(&mut dbs, 0, &[b"SET", b"gone", b"was-here"]);
    dbs
}

/// Capture the undo record the connection handler would, then apply the
/// write through dispatch (which captures pre-images when a save is armed).
fn txn_write(dbs: &mut [Database], log: &mut UndoLog, parts: &[&[u8]]) {
    let key = Bytes::copy_from_slice(parts[1]);
    match dbs[0].get(&key).cloned() {
        None => log.record_insert(0, key),
        Some(e) if parts[0] == b"DEL" => log.record_delete(0, key, e),
        Some(e) => log.record_update(0, key, e),
    }
    run(dbs, 0, parts);
}

fn abort(dbs: &mut [Database], log: UndoLog) -> Vec<CompensatingRecord> {
    let mut out = Vec::new();
    for (db, record) in kv_compensation::first_per_key(log) {
        kv_compensation::undo_one(&mut dbs[db], db, record, &mut out);
    }
    out
}

fn record<'a>(records: &'a [Record], key: &[u8]) -> Vec<&'a Record> {
    records
        .iter()
        .filter(|(_, k, _)| k.as_ref() == key)
        .collect()
}

fn string(e: &crate::storage::entry::Entry) -> Vec<u8> {
    super::epoch_harness::string_of(e)
}

/// Load the image into a fresh database and replay `log` on top — what a
/// restart does with the base and the records at or above F.
fn restore(image: &[Record], log: &[CompensatingRecord]) -> Database {
    let mut db = Database::new();
    for (_, key, entry) in image {
        db.set(key, entry.clone());
    }
    for (_, bytes) in log {
        let mut buf = bytes::BytesMut::from(bytes.as_ref());
        let Ok(Some(Frame::Array(items))) =
            crate::protocol::parse(&mut buf, &crate::protocol::ParseConfig::default())
        else {
            panic!("malformed compensating record")
        };
        let args: Vec<Frame> = items.iter().skip(1).cloned().collect();
        let Some(Frame::BulkString(cmd)) = items.first() else {
            panic!("no command")
        };
        let mut sel = 0usize;
        let _ = crate::command::dispatch(&mut db, cmd, &args, &mut sel, 16);
    }
    db
}

fn hash_len(e: &crate::storage::entry::Entry) -> usize {
    match e.value.as_redis_value() {
        crate::storage::compact_value::RedisValueRef::Hash(h) => h.len(),
        crate::storage::compact_value::RedisValueRef::HashListpack(lp) => lp.len() / 2,
        _ => panic!("not a hash"),
    }
}

/// Writes before F, abort after F: the image is the keyspace AT F (the
/// uncommitted values), taken by move — the big hash is not deep-cloned —
/// and image + compensating records = the live aborted-to keyspace.
#[test]
fn abort_after_the_epoch_instant_keeps_the_image_point_in_time() {
    let mut dbs = fixture();
    let mut log = UndoLog::new();
    txn_write(&mut dbs, &mut log, &[b"SET", b"upd", b"aborted"]);
    txn_write(&mut dbs, &mut log, &[b"SET", b"new", b"inserted"]);
    txn_write(&mut dbs, &mut log, &[b"DEL", b"gone"]);
    txn_write(&mut dbs, &mut log, &[b"HSET", b"big", b"extra", b"1"]);

    // F: the save arms here, with the transaction's values live.
    let epoch = Epoch::begin(&dbs);
    let clones = snapshot_cow::pre_image_clones_for_test();
    let compensation = abort(&mut dbs, log);
    assert_eq!(
        snapshot_cow::pre_image_clones_for_test(),
        clones,
        "the abort deep-cloned a value it was about to replace"
    );
    let image = epoch.finish(&dbs);

    let upd = record(&image, b"upd");
    assert_eq!(upd.len(), 1, "upd exactly once");
    assert_eq!(
        string(&upd[0].2),
        b"aborted",
        "the image holds the value live at F"
    );
    let new = record(&image, b"new");
    assert_eq!(new.len(), 1);
    assert_eq!(string(&new[0].2), b"inserted");
    assert!(record(&image, b"gone").is_empty(), "absent at F");
    let big = record(&image, b"big");
    assert_eq!(big.len(), 1);
    assert_eq!(
        hash_len(&big[0].2),
        FIELDS + 1,
        "the big hash as it was at F"
    );
    assert_eq!(image.len(), 2_000 + 3, "every key live at F exactly once");

    // Exactly-once: base (image) + records at or above F = live.
    let mut restored = restore(&image, &compensation);
    for key in [&b"upd"[..], b"new", b"gone"] {
        assert_eq!(
            restored.get(key).map(string),
            dbs[0].get(key).map(string),
            "{}",
            String::from_utf8_lossy(key)
        );
    }
    assert_eq!(restored.get(b"upd").map(string), Some(b"original".to_vec()));
    assert_eq!(
        restored.get(b"gone").map(string),
        Some(b"was-here".to_vec())
    );
    assert!(restored.get(b"new").is_none());
    assert_eq!(restored.get(b"big").map(hash_len), Some(FIELDS));
    assert_eq!(dbs[0].get(b"big").map(hash_len), Some(FIELDS));
}

/// Writes AFTER F: dispatch already captured the pre-transaction values
/// (first capture wins), so the abort's own capture must not replace them.
#[test]
fn abort_of_writes_made_after_the_epoch_instant_keeps_the_first_capture() {
    let mut dbs = fixture();
    let epoch = Epoch::begin(&dbs);
    let mut log = UndoLog::new();
    txn_write(&mut dbs, &mut log, &[b"SET", b"upd", b"aborted"]);
    txn_write(&mut dbs, &mut log, &[b"SET", b"new", b"inserted"]);
    txn_write(&mut dbs, &mut log, &[b"DEL", b"gone"]);
    let compensation = abort(&mut dbs, log);
    let image = epoch.finish(&dbs);

    let upd = record(&image, b"upd");
    assert_eq!(upd.len(), 1);
    assert_eq!(string(&upd[0].2), b"original");
    assert!(record(&image, b"new").is_empty());
    assert_eq!(string(&record(&image, b"gone")[0].2), b"was-here");
    let mut restored = restore(&image, &compensation);
    assert_eq!(restored.get(b"upd").map(string), Some(b"original".to_vec()));
    assert!(restored.get(b"new").is_none());
    assert_eq!(
        restored.get(b"gone").map(string),
        Some(b"was-here".to_vec())
    );
}

/// The undo takes the replaced value out with its memory credited, like
/// `DEL`: once the save is over, `used_memory` is where an unarmed abort
/// leaves it.
#[test]
fn an_abort_during_a_save_leaves_used_memory_where_an_unarmed_one_does() {
    let run_case = |armed: bool| -> usize {
        let mut dbs = fixture();
        let mut log = UndoLog::new();
        txn_write(&mut dbs, &mut log, &[b"HSET", b"big", b"extra", b"1"]);
        txn_write(&mut dbs, &mut log, &[b"SET", b"upd", b"aborted"]);
        let epoch = armed.then(|| Epoch::begin(&dbs));
        let _ = abort(&mut dbs, log);
        if let Some(epoch) = epoch {
            let _ = epoch.finish(&dbs);
        }
        dbs[0].estimated_memory()
    };
    assert_eq!(run_case(true), run_case(false));
}

/// Record kinds the undo emits, pinned: `DEL` for an insert, `RESTORE …
/// REPLACE ABSTTL` for an update or a delete.
#[test]
fn compensation_record_shapes() {
    let mut dbs = fixture();
    let mut log = UndoLog::new();
    txn_write(&mut dbs, &mut log, &[b"SET", b"upd", b"aborted"]);
    txn_write(&mut dbs, &mut log, &[b"SET", b"new", b"inserted"]);
    let out = abort(&mut dbs, log);
    assert_eq!(out.len(), 2);
    assert!(
        out[0]
            .1
            .starts_with(b"*6\r\n$7\r\nRESTORE\r\n$3\r\nupd\r\n$1\r\n0\r\n")
    );
    assert!(out[0].1.ends_with(b"$7\r\nREPLACE\r\n$6\r\nABSTTL\r\n"));
    assert_eq!(out[1].1.as_ref(), b"*2\r\n$3\r\nDEL\r\n$3\r\nnew\r\n");
}
