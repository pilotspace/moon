//! moon#1224 — FLUSHDB / FLUSHALL / SWAPDB while a snapshot epoch is in
//! flight.
//!
//! All three replace or exchange a database's whole table under the epoch.
//! Before moon#1216's hash-space walk the serializer then indexed a segment
//! the new table did not have and panicked the shard. Now the walk cannot
//! go out of bounds, and the epoch reacts to the swap explicitly:
//!
//! - the database was already fully written: nothing to do — the file holds
//!   its epoch-start contents and the logged FLUSH/SWAPDB replays on top;
//! - FLUSHDB of a database that was EMPTY: nothing changes for the file;
//! - otherwise the file could no longer be one instant (part of the
//!   database written before the swap, the rest gone or foreign), so the
//!   snapshot is ABORTED: the BGSAVE reports failure and the previous file
//!   stays in place. This is redis's own answer to `FLUSHALL` during a
//!   `BGSAVE` (it kills the child); the next save point takes a new one.

use bytes::Bytes;

use super::epoch_harness::{Epoch, Record, diverge, run, string_keyspace};
use super::*;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;

fn preload(db: &mut Database, prefix: &str, n: u32) {
    for i in 0..n {
        db.set_string(
            format!("{prefix}:{i:06}").as_bytes(),
            Bytes::from(format!("{i}")),
        );
    }
}

/// Every command the test ran after the epoch began, in order: the tail a
/// recovery replays on top of the loaded snapshot.
type Tail = Vec<(usize, Vec<Vec<u8>>)>;

/// Run a command live AND record it in the tail.
fn live(dbs: &mut [Database], tail: &mut Tail, db: usize, parts: &[&[u8]]) {
    apply(dbs, db, parts);
    tail.push((db, parts.iter().map(|p| p.to_vec()).collect()));
}

/// Apply one command the way the live paths do: FLUSHALL clears the other
/// databases too (`flush_every_database`), SWAPDB exchanges two tables.
fn apply(dbs: &mut [Database], db: usize, parts: &[&[u8]]) {
    if parts[0].eq_ignore_ascii_case(b"SWAPDB") {
        let a: usize = std::str::from_utf8(parts[1]).unwrap().parse().unwrap();
        let b: usize = std::str::from_utf8(parts[2]).unwrap().parse().unwrap();
        snapshot_cow::note_swapdb(a, b);
        dbs.swap(a, b);
        return;
    }
    let reply = run(dbs, db, parts);
    if parts[0].eq_ignore_ascii_case(b"FLUSHALL") && !matches!(reply, Frame::Error(_)) {
        crate::command::server_admin::flush_every_database(dbs, db);
    }
}

/// Recovery: load the records, replay the tail.
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

/// The moon#1224 proof, verbatim in its steps: 2,000 keys, one segment
/// written, FLUSHDB replaces the table with a one-segment one, and the
/// serializer used to index a segment that no longer existed. No capture is
/// armed here, so nothing aborts: the walk itself must stay in bounds.
#[test]
fn flushdb_mid_epoch_does_not_crash_the_serializer() {
    let mut dbs = vec![Database::new()];
    for i in 0..2000u32 {
        dbs[0].set_string(format!("k:{i:06}").as_bytes(), Bytes::from_static(b"v"));
    }
    let segs = dbs[0].data().segment_count();
    assert!(segs > 2, "fixture needs several segments, got {segs}");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.rrdshard");
    snapshot_cow::disarm();
    let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
    assert!(!state.advance_one_segment(&dbs));
    let mut selected = 0usize;
    let _ = crate::command::dispatch(&mut dbs[0], b"FLUSHDB", &[], &mut selected, 16);
    assert_eq!(
        dbs[0].data().segment_count(),
        1,
        "FLUSHDB replaced the table"
    );
    // Post-flush writes land in the new table's single segment, whose block
    // starts below the cursor.
    for i in 0..50u32 {
        dbs[0].set_string(format!("post:{i}").as_bytes(), Bytes::from_static(b"p"));
    }
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        while !state.advance_one_segment(&dbs) {}
    }));
    assert!(r.is_ok(), "a FLUSHDB during BGSAVE panicked the serializer");
    state.finalize().unwrap();
    let records = super::epoch_harness::read_records(&path);
    let mut keys: Vec<_> = records.iter().map(|(_, k, _)| k.clone()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), records.len(), "no key written twice");
}

/// A FLUSH of the database being written, the SWAPDB of one not yet
/// written, and FLUSHALL: each aborts the epoch. The BGSAVE fails loudly,
/// nothing is published, and the previous snapshot file is untouched.
fn assert_aborts(n_dbs: usize, after_ticks: usize, cmd: &[&[u8]]) {
    let mut dbs: Vec<Database> = (0..n_dbs).map(|_| Database::new()).collect();
    for (i, db) in dbs.iter_mut().enumerate() {
        preload(db, &format!("d{i}"), 1500);
    }
    let epoch = Epoch::begin(&dbs);
    // The previous generation, already on disk.
    shard_snapshot_save(0, 0, &dbs[..1], epoch.path()).unwrap();
    let previous = std::fs::read(epoch.path()).unwrap();
    let mut epoch = epoch;
    for _ in 0..after_ticks {
        assert!(!epoch.tick_one(&dbs));
    }
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 0, cmd);
    live(&mut dbs, &mut tail, 0, &[b"SET", b"after", b"1"]);
    let path = epoch.path().to_path_buf();
    let mut epoch = epoch;
    let err = epoch
        .try_finish(&dbs)
        .expect_err("the snapshot must fail instead of publishing a mixed file");
    assert!(err.contains("aborted"), "{err}");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        previous,
        "an aborted snapshot must leave the previous file in place"
    );
}

#[test]
fn flushdb_of_the_database_in_progress_aborts_the_snapshot() {
    assert_aborts(1, 1, &[b"FLUSHDB"]);
}

#[test]
fn flushdb_async_of_the_database_in_progress_aborts_the_snapshot() {
    assert_aborts(1, 1, &[b"FLUSHDB", b"ASYNC"]);
}

/// FLUSHDB of a NON-EMPTY database the epoch has not reached yet: its
/// epoch-start contents are gone before a byte of them was written.
#[test]
fn flushdb_of_a_pending_non_empty_database_aborts_the_snapshot() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 10);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    run(&mut dbs, 1, &[b"FLUSHDB"]);
    let err = epoch
        .try_finish(&dbs)
        .expect_err("db 1 was not written yet");
    assert!(err.contains("aborted"), "{err}");
}

#[test]
fn flushall_mid_epoch_aborts_the_snapshot() {
    assert_aborts(3, 2, &[b"FLUSHALL"]);
}

#[test]
fn flushall_async_mid_epoch_aborts_the_snapshot() {
    assert_aborts(2, 0, &[b"FLUSHALL", b"ASYNC"]);
}

#[test]
fn swapdb_of_an_unfinished_database_aborts_the_snapshot() {
    assert_aborts(3, 1, &[b"SWAPDB", b"1", b"2"]);
}

/// The hook sits in the one place every SWAPDB path (coordinator local leg,
/// the SPSC arm, replica apply) exchanges tables: `ShardDbSet::swap`.
#[test]
fn shard_db_set_swap_notifies_the_armed_epoch() {
    let (shared, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
        Database::new(),
        Database::new(),
    ]]);
    let _keep = shared;
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
    snapshot_cow::disarm();
    snapshot_cow::arm_with_layout(vec![1, 1]);
    crate::shard::slice::with_shard(|s| s.databases.swap(0, 1));
    let aborted = snapshot_cow::abort_pending_for_test();
    snapshot_cow::disarm();
    assert!(
        aborted.is_some(),
        "SWAPDB under an armed epoch went unnoticed"
    );
    // Unarmed: a swap is free and leaves nothing behind.
    crate::shard::slice::with_shard(|s| s.databases.swap(0, 1));
    assert!(snapshot_cow::abort_pending_for_test().is_none());
}

/// FLUSHDB of a database the epoch already wrote: the file keeps its
/// epoch-start contents, and loading it then replaying the logged tail
/// (the FLUSHDB, later writes) lands exactly on the live keyspace.
#[test]
fn flush_of_a_finished_database_keeps_the_file_point_in_time() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 1500);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    while epoch.state.as_ref().unwrap().current_db_index() == 0 {
        assert!(!epoch.tick_one(&dbs));
    }
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 0, &[b"FLUSHDB"]);
    live(&mut dbs, &mut tail, 0, &[b"SET", b"a:000007", b"new"]);
    for i in 0..300u32 {
        live(
            &mut dbs,
            &mut tail,
            1,
            &[b"INCR", format!("b:{i:06}").as_bytes()],
        );
    }
    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(2, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// FLUSHDB of an EMPTY database the epoch has not reached changes nothing
/// the file will contain: no abort.
#[test]
fn flushdb_of_an_empty_pending_database_is_harmless() {
    let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 2, &[b"FLUSHDB"]);
    live(&mut dbs, &mut tail, 2, &[b"SET", b"c:1", b"x"]);
    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(3, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// SWAPDB of two databases the epoch already wrote: harmless.
#[test]
fn swapdb_of_two_finished_databases_is_harmless() {
    let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 700);
    preload(&mut dbs[1], "b", 700);
    preload(&mut dbs[2], "c", 700);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    while epoch.state.as_ref().unwrap().current_db_index() < 2 {
        assert!(!epoch.tick_one(&dbs));
    }
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 0, &[b"SWAPDB", b"0", b"1"]);
    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(3, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// A FLUSH the command itself refuses (bad argument) flushes nothing, so it
/// must not cost the snapshot anything either.
#[test]
fn a_refused_flush_does_not_abort() {
    let mut dbs = vec![Database::new()];
    preload(&mut dbs[0], "a", 1500);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    let reply = run(&mut dbs, 0, &[b"FLUSHDB", b"BOGUS"]);
    assert!(
        matches!(reply, Frame::Error(_)),
        "setup: FLUSHDB BOGUS must be refused"
    );
    let reply = run(&mut dbs, 0, &[b"FLUSHALL", b"SYNC", b"EXTRA"]);
    assert!(
        matches!(reply, Frame::Error(_)),
        "setup: 2 args must be refused"
    );
    let records = epoch.finish(&dbs);
    assert_eq!(diverge(&expected, &records), Default::default());
}
