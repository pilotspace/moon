//! moon#1228 — writers that changed a key under an armed snapshot epoch
//! without capturing its pre-image first.
//!
//! Every test arms an epoch, lets it write part of the keyspace, runs the
//! writer on keys whose range is already written AND on keys whose range is
//! still pending, finishes the epoch and reads the file back as raw records
//! (duplicates visible). The file must be exactly the epoch-start keyspace:
//! nothing missing, nothing extra, nothing twice, no post-epoch value. Where
//! the writer is logged, the tests also replay the tail on top of the file
//! and require the live keyspace — the no-resurrection check.

use bytes::Bytes;

use super::epoch_harness::{Epoch, Record, diverge, run, string_keyspace};
use super::*;
use crate::command::keyspace::move_cmd;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;

fn preload(db: &mut Database, prefix: &str, n: u32) {
    for i in 0..n {
        db.set_string(
            format!("{prefix}:{i:06}").as_bytes(),
            Bytes::from(format!("{prefix}{i}")),
        );
    }
}

/// Every command run after the epoch began, in order: the tail a recovery
/// replays on top of the loaded snapshot.
type Tail = Vec<(usize, Vec<Vec<u8>>)>;

/// Apply one command the way the live paths do: `MOVE` / `COPY … DB n`
/// through the two-database cores (the connection intercepts, the MULTI
/// executors, scripts and replay all end there), anything else through
/// `command::dispatch`.
fn apply(dbs: &mut [Database], db: usize, parts: &[&[u8]]) -> Frame {
    let args: Vec<Frame> = parts[1..]
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
        .collect();
    match move_cmd::resolve_two_db(parts[0], &args, db, dbs.len()) {
        Some(Ok(op)) => {
            let dst = op.dst_db();
            move_cmd::with_two_slice_dbs(dbs, db, dst, |s, d| op.apply(s, db, d))
        }
        Some(Err(e)) => e,
        None => run(dbs, db, parts),
    }
}

/// Run a command live AND record it in the tail.
fn live(dbs: &mut [Database], tail: &mut Tail, db: usize, parts: &[&[u8]]) -> Frame {
    let reply = apply(dbs, db, parts);
    tail.push((db, parts.iter().map(|p| p.to_vec()).collect()));
    reply
}

/// Recovery with a log: load the records, replay the tail on top.
fn recover(n: usize, records: Vec<Record>, tail: &Tail) -> Vec<Database> {
    let mut dbs: Vec<Database> = (0..n).map(|_| Database::new()).collect();
    for (db, key, entry) in records {
        dbs[db].set(&key, entry);
    }
    snapshot_cow::disarm();
    for (db, parts) in tail {
        let argv: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
        apply(&mut dbs, *db, &argv);
    }
    dbs
}

/// Keys of `db` (from `prefix:0..n`) whose epoch-start state the epoch has
/// already written (`false`) or still has to write (`true`).
fn split_by_pending(epoch: &Epoch, db: usize, prefix: &str, n: u32) -> (Vec<String>, Vec<String>) {
    let state = epoch.state.as_ref().expect("epoch in flight");
    (0..n)
        .map(|i| format!("{prefix}:{i:06}"))
        .partition(|k| !state.is_key_pending(db, k.as_bytes()))
}

/// Start an epoch over `dbs` and serialize a few segments of db 0, so db 0
/// has a written and a pending range and every other db is pending.
fn epoch_partway(dbs: &[Database]) -> Epoch {
    let mut epoch = Epoch::begin(dbs);
    for _ in 0..3 {
        assert!(!epoch.tick_one(dbs));
    }
    epoch
}

/// `MOVE` in every direction the file can get wrong: out of db 0's written
/// range (the key would ALSO be serialized in db 1 — a duplicate, and with a
/// log the replayed MOVE finds the destination taken and the key is
/// resurrected in db 0), out of db 0's pending range (the key would be
/// serialized in db 1 only), and from db 1 into db 0's written range (the key
/// would be in neither).
#[test]
fn move_mid_epoch_keeps_both_databases_point_in_time() {
    let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 400);
    let expected = string_keyspace(&dbs);
    let mut epoch = epoch_partway(&dbs);
    let (written, pending) = split_by_pending(&epoch, 0, "a", 1500);
    assert!(
        written.len() >= 20 && pending.len() >= 20,
        "fixture: db 0 must be partly written ({} written, {} pending)",
        written.len(),
        pending.len()
    );
    // `b:` keys whose hash lies in db 0's WRITTEN range: moved there, they
    // leave db 1 before its turn and land where the walk has already been.
    let b_into_written: Vec<String> = (0..400u32)
        .map(|i| format!("b:{i:06}"))
        .filter(|k| {
            !epoch
                .state
                .as_ref()
                .expect("epoch")
                .is_key_pending(0, k.as_bytes())
        })
        .take(10)
        .collect();
    assert!(!b_into_written.is_empty(), "fixture: no b key hashes low");

    let mut tail = Tail::new();
    for k in written.iter().take(10).chain(pending.iter().take(10)) {
        let r = live(&mut dbs, &mut tail, 0, &[b"MOVE", k.as_bytes(), b"1"]);
        assert_eq!(r, Frame::Integer(1), "setup: MOVE {k} 1");
    }
    for k in pending.iter().skip(10).take(10) {
        let r = live(&mut dbs, &mut tail, 0, &[b"MOVE", k.as_bytes(), b"2"]);
        assert_eq!(r, Frame::Integer(1), "setup: MOVE {k} 2");
    }
    for k in &b_into_written {
        let r = live(&mut dbs, &mut tail, 1, &[b"MOVE", k.as_bytes(), b"0"]);
        assert_eq!(r, Frame::Integer(1), "setup: MOVE {k} 0");
    }
    // A round trip: the first capture of each side is the epoch-start one.
    let k = &pending[30];
    live(&mut dbs, &mut tail, 0, &[b"MOVE", k.as_bytes(), b"1"]);
    live(&mut dbs, &mut tail, 1, &[b"MOVE", k.as_bytes(), b"0"]);

    let records = epoch.finish(&dbs);
    assert_eq!(
        diverge(&expected, &records),
        Default::default(),
        "the file must hold every key once, in its epoch-start database"
    );
    let recovered = recover(3, records, &tail);
    assert_eq!(
        string_keyspace(&recovered),
        string_keyspace(&dbs),
        "file + replayed MOVEs must land on the live keyspace (no resurrection)"
    );
}

/// `COPY … DB n` writes the destination only: a copy into a pending
/// database (a key that did not exist at epoch start) and a `REPLACE` over an
/// existing destination key (a post-epoch value) must both stay out of the
/// file.
#[test]
fn copy_into_another_database_mid_epoch_keeps_the_destination_point_in_time() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 400);
    let expected = string_keyspace(&dbs);
    let mut epoch = epoch_partway(&dbs);
    let (written, pending) = split_by_pending(&epoch, 0, "a", 1500);

    let mut tail = Tail::new();
    for k in written.iter().take(10).chain(pending.iter().take(10)) {
        // A new key in db 1.
        let r = live(
            &mut dbs,
            &mut tail,
            0,
            &[b"COPY", k.as_bytes(), k.as_bytes(), b"DB", b"1"],
        );
        assert_eq!(r, Frame::Integer(1), "setup: COPY {k} DB 1");
    }
    for i in 0..20u32 {
        // An existing db 1 key overwritten.
        let src = &pending[20 + i as usize];
        let dst = format!("b:{i:06}");
        let r = live(
            &mut dbs,
            &mut tail,
            0,
            &[
                b"COPY",
                src.as_bytes(),
                dst.as_bytes(),
                b"DB",
                b"1",
                b"REPLACE",
            ],
        );
        assert_eq!(r, Frame::Integer(1), "setup: COPY {src} {dst} DB 1 REPLACE");
    }

    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(2, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// The capture sits in the cores, so the paths that reach them through
/// `TwoDbOp::apply` — both MULTI executors and a script's `redis.call` —
/// capture too, on BOTH databases (a script's own keyspec capture only ever
/// saw the source database).
#[test]
fn two_db_op_apply_captures_both_databases() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 10);
    preload(&mut dbs[1], "b", 10);
    snapshot_cow::disarm();
    snapshot_cow::arm();
    let args = [
        Frame::BulkString(Bytes::from_static(b"a:000001")),
        Frame::BulkString(Bytes::from_static(b"1")),
    ];
    let op = move_cmd::resolve_two_db(b"MOVE", &args, 0, 2)
        .expect("two-db")
        .expect("valid");
    let r = move_cmd::with_two_slice_dbs(&mut dbs, 0, 1, |s, d| op.apply(s, 0, d));
    assert_eq!(r, Frame::Integer(1));
    let args = [
        Frame::BulkString(Bytes::from_static(b"a:000002")),
        Frame::BulkString(Bytes::from_static(b"b:000003")),
        Frame::BulkString(Bytes::from_static(b"DB")),
        Frame::BulkString(Bytes::from_static(b"1")),
        Frame::BulkString(Bytes::from_static(b"REPLACE")),
    ];
    let op = move_cmd::resolve_two_db(b"COPY", &args, 0, 2)
        .expect("two-db")
        .expect("valid");
    let r = move_cmd::with_two_slice_dbs(&mut dbs, 0, 1, |s, d| op.apply(s, 0, d));
    assert_eq!(r, Frame::Integer(1));
    let mut entries: Vec<(usize, Bytes)> = snapshot_cow::pending_for_test()
        .into_iter()
        .map(|(db, k, _)| (db, k))
        .collect();
    entries.sort();
    let tombstones = snapshot_cow::pending_tombstones_for_test();
    snapshot_cow::disarm();
    assert_eq!(
        entries,
        vec![
            (0, Bytes::from_static(b"a:000001")),
            (1, Bytes::from_static(b"b:000003")),
        ],
        "MOVE captures the source key; COPY … REPLACE the destination's old value"
    );
    assert_eq!(
        tombstones,
        vec![(1, Bytes::from_static(b"a:000001"))],
        "MOVE captures the destination's absence"
    );
}

/// `WS DROP`'s key sweep (`workspace::sweep_prefix`, the one body behind the
/// monoio and tokio owner paths and the routed `WsDropCleanup` arm) deletes
/// every `{wsid}:` key of every database. Mid-epoch, every one whose range
/// the save had not written yet used to be missing from the file.
#[test]
fn workspace_drop_sweep_mid_epoch_keeps_the_file_point_in_time() {
    let prefix = "{0123456789abcdef0123456789abcdef}:";
    let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1200);
    preload(&mut dbs[0], &format!("{prefix}w"), 300);
    preload(&mut dbs[1], &format!("{prefix}x"), 200);
    preload(&mut dbs[2], "c", 100);
    let expected = string_keyspace(&dbs);
    let mut epoch = epoch_partway(&dbs);
    let (written, pending) = split_by_pending(&epoch, 0, &format!("{prefix}w"), 300);
    assert!(
        !written.is_empty() && !pending.is_empty(),
        "fixture: workspace keys on both sides of the cursor"
    );

    let mut refs: Vec<&mut Database> = dbs.iter_mut().collect();
    let deleted = crate::workspace::sweep_prefix(&mut refs, prefix.as_bytes());
    assert_eq!(deleted, 500, "setup: the sweep deletes every workspace key");

    let records = epoch.finish(&dbs);
    assert_eq!(
        diverge(&expected, &records),
        Default::default(),
        "every swept key must still be in the file with its epoch-start value"
    );
}
