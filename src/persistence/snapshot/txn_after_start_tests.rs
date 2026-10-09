//! moon#1289 R2 (review N1): why checking each shard's `TXN` holds at the
//! instant it starts its part of an automatic snapshot is enough.
//!
//! A shard with no hold at its start has no uncommitted write in memory
//! (`transaction::isolation`: the hold is taken before the write is
//! dispatched and released only after the abort's restore is applied). A
//! `TXN` write that lands AFTER the start goes through the connection leg's
//! `capture_conn_write` + `command::dispatch` — the handlers' exact sequence,
//! driven here — whose copy-on-write capture stores the key's epoch-start
//! image first; first capture wins, so neither the write nor a later abort
//! restore can reach the file. These tests pin that half of the argument.
//!
//! moon#1300 (F3) closes the other half for EVERY snapshot: a write the
//! transaction made BEFORE the start is held, and the arm files the held
//! key's pre-transaction image as its first capture
//! (`snapshot_cow::capture_held_pre_images`) —
//! [`a_txn_write_before_the_start_is_saved_at_its_pre_txn_value`].

use super::epoch_harness::{Epoch, Record, run, string_of};
use super::*;
use crate::protocol::Frame;
use crate::transaction::conn_capture::capture_conn_write;
use crate::transaction::isolation;
use crate::transaction::kv_compensation;
use crate::transaction::{CrossStoreTxn, KvWriteIntents};

/// Database 0 with enough filler for several segments, plus the keys the
/// transaction touches.
fn fixture() -> Vec<Database> {
    let mut db = Database::new();
    db.db_index = 0;
    let mut dbs = vec![db];
    for i in 0..2_000 {
        run(&mut dbs, 0, &[b"SET", format!("f{i:05}").as_bytes(), b"x"]);
    }
    run(&mut dbs, 0, &[b"SET", b"k", b"original"]);
    run(&mut dbs, 0, &[b"SET", b"gone", b"was-here"]);
    dbs
}

/// A write of the open `txn`, exactly as both runtimes' generic write legs
/// run it: undo capture + hold, dispatch, finish.
fn txn_write(
    txn: &mut CrossStoreTxn,
    intents: &mut KvWriteIntents,
    dbs: &mut [Database],
    parts: &[&[u8]],
) -> Frame {
    let args: Vec<Frame> = parts[1..]
        .iter()
        .map(|p| Frame::BulkString(bytes::Bytes::copy_from_slice(p)))
        .collect();
    let capture = match capture_conn_write(txn, intents, &mut dbs[0], 0, parts[0], &args) {
        Ok(capture) => capture,
        Err(refused) => return refused,
    };
    let reply = run(dbs, 0, parts);
    capture.finish(matches!(reply, Frame::Error(_)), txn, intents);
    reply
}

fn values(records: &[Record], key: &[u8]) -> Vec<Vec<u8>> {
    records
        .iter()
        .filter(|(_, k, _)| k.as_ref() == key)
        .map(|(_, _, e)| string_of(e))
        .collect()
}

fn on_fresh_thread(f: impl FnOnce() + Send + 'static) {
    std::thread::spawn(f).join().expect("test thread");
}

/// The shard's start check passes (nothing held), then the `TXN` begins and
/// writes every key while the walk has not reached them, and stays OPEN
/// through the publish: the file holds the pre-`TXN` keyspace.
#[test]
fn a_txn_write_after_the_start_is_saved_at_its_pre_txn_value() {
    on_fresh_thread(|| {
        let mut dbs = fixture();
        assert!(!isolation::any_held(), "the shard's start check passes");
        let mut epoch = Epoch::begin(&dbs);
        // Part of the walk done: the keys under test may be on either side.
        let _ = epoch.tick_one(&dbs);

        let mut txn = CrossStoreTxn::new(41, 0, 0);
        isolation::txn_begin(41);
        let mut intents = KvWriteIntents::new();
        let (t, i) = (&mut txn, &mut intents);
        assert_eq!(
            txn_write(t, i, &mut dbs, &[b"SET", b"k", b"aborted"]),
            Frame::SimpleString(bytes::Bytes::from_static(b"OK"))
        );
        txn_write(t, i, &mut dbs, &[b"SET", b"new", b"inserted"]);
        txn_write(t, i, &mut dbs, &[b"DEL", b"gone"]);
        for n in (0..2_000).step_by(97) {
            let key = format!("f{n:05}");
            txn_write(t, i, &mut dbs, &[b"SET", key.as_bytes(), b"txn"]);
        }
        assert!(isolation::any_held(), "the TXN holds what it wrote");

        let image = epoch.finish(&dbs);
        assert_eq!(values(&image, b"k"), vec![b"original".to_vec()]);
        assert!(
            values(&image, b"new").is_empty(),
            "an insert after the start is not saved"
        );
        assert_eq!(values(&image, b"gone"), vec![b"was-here".to_vec()]);
        for i in (0..2_000).step_by(97) {
            let key = format!("f{i:05}");
            assert_eq!(values(&image, key.as_bytes()), vec![b"x".to_vec()], "{key}");
        }
        isolation::txn_end(41);
    });
}

/// The `TXN` writes after the start and ABORTS while the walk still runs:
/// the restore is a second write of each key, and first capture wins — the
/// file still holds the epoch-start values, once each.
#[test]
fn an_abort_during_the_walk_does_not_reach_the_file_either() {
    on_fresh_thread(|| {
        let mut dbs = fixture();
        assert!(!isolation::any_held());
        let mut epoch = Epoch::begin(&dbs);
        let mut txn = CrossStoreTxn::new(42, 0, 0);
        isolation::txn_begin(42);
        let mut intents = KvWriteIntents::new();
        txn_write(
            &mut txn,
            &mut intents,
            &mut dbs,
            &[b"SET", b"k", b"aborted"],
        );
        txn_write(&mut txn, &mut intents, &mut dbs, &[b"DEL", b"gone"]);
        let _ = epoch.tick_one(&dbs);
        let mut compensation = Vec::new();
        let log = std::mem::take(&mut txn.kv_undo);
        for (db, record) in kv_compensation::first_per_key(log) {
            kv_compensation::undo_one(&mut dbs[db], db, record, &mut compensation);
        }
        isolation::txn_end(42);
        let image = epoch.finish(&dbs);
        assert_eq!(values(&image, b"k"), vec![b"original".to_vec()]);
        assert_eq!(values(&image, b"gone"), vec![b"was-here".to_vec()]);
    });
}

/// moon#1300 (F3): the `TXN` writes BEFORE the epoch starts and stays open
/// through the publish, then is aborted: the image holds the pre-`TXN`
/// keyspace — the updated key's old value, the deleted key, no inserted key
/// — and the abort's restore during the walk changes nothing in it.
#[test]
fn a_txn_write_before_the_start_is_saved_at_its_pre_txn_value() {
    on_fresh_thread(|| {
        let mut dbs = fixture();
        let mut txn = CrossStoreTxn::new(43, 0, 0);
        isolation::txn_begin(43);
        let mut intents = KvWriteIntents::new();
        let (t, i) = (&mut txn, &mut intents);
        txn_write(t, i, &mut dbs, &[b"SET", b"k", b"uncommitted"]);
        txn_write(t, i, &mut dbs, &[b"SET", b"new", b"inserted"]);
        txn_write(t, i, &mut dbs, &[b"DEL", b"gone"]);
        txn_write(t, i, &mut dbs, &[b"SET", b"f00007", b"txn"]);
        assert!(isolation::any_held());

        let mut epoch = Epoch::begin(&dbs);
        let _ = epoch.tick_one(&dbs);
        // More of the transaction, then its abort, mid-walk.
        txn_write(&mut txn, &mut intents, &mut dbs, &[b"SET", b"k", b"again"]);
        let mut compensation = Vec::new();
        let log = std::mem::take(&mut txn.kv_undo);
        for (db, record) in kv_compensation::first_per_key(log) {
            kv_compensation::undo_one(&mut dbs[db], db, record, &mut compensation);
        }
        isolation::txn_end(43);
        let image = epoch.finish(&dbs);
        assert_eq!(values(&image, b"k"), vec![b"original".to_vec()]);
        assert!(values(&image, b"new").is_empty(), "the insert is not saved");
        assert_eq!(values(&image, b"gone"), vec![b"was-here".to_vec()]);
        assert_eq!(values(&image, b"f00007"), vec![b"x".to_vec()]);
    });
}

/// moon#1300 (F3): the same with the transaction still OPEN when the image
/// is published (a crash would follow): no uncommitted value in the file.
#[test]
fn an_open_txn_before_the_start_never_reaches_the_file() {
    on_fresh_thread(|| {
        let mut dbs = fixture();
        let mut txn = CrossStoreTxn::new(44, 0, 0);
        isolation::txn_begin(44);
        let mut intents = KvWriteIntents::new();
        txn_write(
            &mut txn,
            &mut intents,
            &mut dbs,
            &[b"SET", b"k", b"uncommitted"],
        );
        txn_write(&mut txn, &mut intents, &mut dbs, &[b"DEL", b"gone"]);
        let epoch = Epoch::begin(&dbs);
        let image = epoch.finish(&dbs);
        assert_eq!(values(&image, b"k"), vec![b"original".to_vec()]);
        assert_eq!(values(&image, b"gone"), vec![b"was-here".to_vec()]);
        isolation::txn_end(44);
    });
}
