//! moon#1224, moon#1228 — FLUSHDB / FLUSHALL / SWAPDB while a snapshot
//! epoch is in flight.
//!
//! All three replace or exchange a database's whole table under the epoch.
//! Before moon#1216's hash-space walk the serializer then indexed a segment
//! the new table did not have and panicked the shard (moon#1224); the fix for
//! that ABORTED the save whenever the flushed / swapped database was not
//! finished, so a workload that flushed more often than one save takes never
//! completed a BGSAVE (moon#1228). Now the epoch follows the tables:
//!
//! - a FLUSHDB of a database the epoch has not finished hands the detached
//!   table to the epoch (`Database::clear` → `snapshot_cow::note_cleared_table`),
//!   which writes the pre-flush contents — what redis's forked child writes;
//! - a SWAPDB re-points the epoch's databases at the slots their tables moved
//!   to, and captures after the swap are filed under the table's database;
//! - a database already written, or a flush of an empty one, changes nothing.
//!
//! Every case checks the file is exactly the epoch-start keyspace AND that
//! loading it and replaying the logged tail lands on the live keyspace. A
//! FLUSHALL still aborts an unfinished epoch — redis kills its RDB child
//! for it (`flushAllDataAndResetRDB`) — and so does a replica full resync
//! (its data is not this node's log); the tables either one detaches are
//! dropped at once, not frozen.

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

/// A FLUSHDB of the database being written, the SWAPDB of one not yet
/// written: the save completes with the epoch-start keyspace (it used to
/// abort — moon#1228), and file + replayed tail == live keyspace.
/// Writes before and after the change hit every database, so pre-images
/// captured before a freeze / swap and writes to a post-flush table are both
/// exercised.
fn assert_point_in_time(n_dbs: usize, after_ticks: usize, cmd: &[&[u8]]) {
    let mut dbs: Vec<Database> = (0..n_dbs).map(|_| Database::new()).collect();
    for (i, db) in dbs.iter_mut().enumerate() {
        preload(db, &format!("d{i}"), 1500);
    }
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    for _ in 0..after_ticks {
        assert!(!epoch.tick_one(&dbs));
    }
    let mut tail = Tail::new();
    for i in 0..n_dbs {
        for j in (0..1500u32).step_by(97) {
            let k = format!("d{i}:{j:06}");
            live(&mut dbs, &mut tail, i, &[b"INCR", k.as_bytes()]);
        }
    }
    live(&mut dbs, &mut tail, 0, cmd);
    for i in 0..n_dbs {
        live(&mut dbs, &mut tail, i, &[b"SET", b"after", b"1"]);
        for j in (0..1500u32).step_by(89) {
            let k = format!("d{i}:{j:06}");
            live(&mut dbs, &mut tail, i, &[b"INCR", k.as_bytes()]);
        }
    }
    let records = epoch
        .try_finish(&dbs)
        .expect("the save must complete (moon#1228: it used to abort)");
    assert_eq!(
        diverge(&expected, &records),
        Default::default(),
        "the file must be the epoch-start keyspace"
    );
    let recovered = recover(n_dbs, records, &tail);
    assert_eq!(
        string_keyspace(&recovered),
        string_keyspace(&dbs),
        "file + replayed tail must land on the live keyspace"
    );
}

#[test]
fn flushdb_of_the_database_in_progress_keeps_the_pre_flush_image() {
    assert_point_in_time(1, 1, &[b"FLUSHDB"]);
}

#[test]
fn flushdb_async_of_the_database_in_progress_keeps_the_pre_flush_image() {
    assert_point_in_time(1, 1, &[b"FLUSHDB", b"ASYNC"]);
}

/// FLUSHDB of a NON-EMPTY database the epoch has not reached yet.
#[test]
fn flushdb_of_a_pending_non_empty_database_keeps_the_pre_flush_image() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 10);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 1, &[b"FLUSHDB"]);
    live(&mut dbs, &mut tail, 1, &[b"SET", b"b:000003", b"post"]);
    let records = epoch.try_finish(&dbs).expect("db 1 was not written yet");
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(2, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// Redis parity: `flushAllDataAndResetRDB` kills an in-flight RDB child,
/// so a FLUSHALL while the epoch still has databases to write fails the
/// save (the previous file stays). The tables its clears detach are dropped
/// at once — frozen, they held a second copy of the whole dataset until the
/// walk passed each one.
fn assert_flushall_aborts(n_dbs: usize, after_ticks: usize, cmd: &[&[u8]]) {
    let mut dbs: Vec<Database> = (0..n_dbs).map(|_| Database::new()).collect();
    for (i, db) in dbs.iter_mut().enumerate() {
        preload(db, &format!("d{i}"), 1500);
    }
    let mut epoch = Epoch::begin(&dbs);
    for _ in 0..after_ticks {
        assert!(!epoch.tick_one(&dbs));
    }
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 0, cmd);
    assert!(
        dbs.iter().all(|db| db.data().is_empty()),
        "setup: {cmd:?} must clear every database"
    );
    assert_eq!(
        snapshot_cow::frozen_tables_queued_for_test(),
        0,
        "{cmd:?}: the aborted epoch kept the detached tables"
    );
    let outcome = epoch.try_finish(&dbs);
    let Err(why) = outcome else {
        panic!("{cmd:?} mid-epoch must fail the save, as redis's does");
    };
    assert!(why.contains("FLUSHALL"), "abort reason: {why}");
}

#[test]
fn flushall_mid_epoch_aborts_the_save() {
    assert_flushall_aborts(3, 2, &[b"FLUSHALL"]);
}

#[test]
fn flushall_async_mid_epoch_aborts_the_save() {
    assert_flushall_aborts(2, 0, &[b"FLUSHALL", b"ASYNC"]);
}

/// A FLUSHALL once every database is written changes nothing the file
/// holds: no abort, and file + the replayed FLUSHALL == live keyspace.
#[test]
fn flushall_after_every_database_is_written_does_not_abort() {
    let mut dbs: Vec<Database> = (0..2).map(|_| Database::new()).collect();
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 1500);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    while !epoch.tick_one(&dbs) {}
    let mut tail = Tail::new();
    live(&mut dbs, &mut tail, 1, &[b"FLUSHALL"]);
    live(&mut dbs, &mut tail, 0, &[b"SET", b"after", b"1"]);
    assert!(snapshot_cow::abort_pending_for_test().is_none());
    let records = epoch
        .try_finish(&dbs)
        .expect("a FLUSHALL after the walk must not fail the save");
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(2, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// A FLUSHALL by another name — fill a database, FLUSHDB it, database after
/// database, over and over — is bounded by the EPOCH-START dataset.
///
/// At most one table per database is frozen (a flushed slot is unmapped, so
/// later flushes drop theirs), and review 5 of moon#1228 showed that the
/// count bound is not a byte bound: the table a FLUSHDB detaches holds every
/// key inserted since the epoch began, all of them tombstoned and useless to
/// the file (8 x 40 MB of SETs + FLUSHDB held 344 MB under `--maxmemory
/// 64mb`). The drain now trims a frozen table to its epoch-start rows, so
/// after every drain the epoch holds at most the epoch-start bills of the
/// databases it has not written, and releases each as the walk passes it.
/// The walk is held (drains only) while the client fills and flushes, as
/// the real-server reproduction held it. The save still completes with the
/// epoch-start keyspace.
#[test]
fn flushdb_of_every_database_holds_at_most_the_epoch_start_tables() {
    const N_DBS: usize = 4;
    let mut dbs: Vec<Database> = (0..N_DBS).map(|_| Database::new()).collect();
    for (i, db) in dbs.iter_mut().enumerate() {
        preload(db, &format!("d{i}"), 2000);
    }
    let start_bytes: Vec<u64> = dbs.iter().map(|db| db.estimated_memory() as u64).collect();
    let start_total: u64 = start_bytes.iter().sum();
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    let mut tail = Tail::new();
    let value = vec![b'x'; 1024];
    let mut peak = 0u64;
    for round in 0..20 {
        for db in 0..N_DBS {
            // Post-epoch inserts FIRST, then the flush: the detached table
            // holds them (a flush before the inserts, as this test did
            // until review 5, detaches only the epoch-start rows).
            for j in 0..300u32 {
                let k = format!("r{round}:{j}");
                live(&mut dbs, &mut tail, db, &[b"SET", k.as_bytes(), &value]);
            }
            live(&mut dbs, &mut tail, db, &[b"FLUSHDB"]);
            epoch.drain();
            let held = epoch.state.as_ref().expect("epoch").cow_bytes();
            peak = peak.max(held);
            assert!(
                held <= start_total,
                "round {round}, db {db}: the epoch holds {held} B after a drain; the \
                 epoch-start dataset is {start_total} B"
            );
        }
    }
    assert!(peak > 0, "the flushed tables were frozen");
    // Released database by database as the walk passes it.
    let mut last = epoch.state.as_ref().expect("epoch").cow_bytes();
    loop {
        let state = epoch.state.as_ref().expect("epoch");
        let cur = state.current_db_index();
        let now = state.cow_bytes();
        let still_to_write: u64 = start_bytes.iter().skip(cur).sum();
        assert!(
            now <= last,
            "held bytes grew during the walk: {last} -> {now}"
        );
        assert!(
            now <= still_to_write,
            "db {cur}: {now} B held, but only {still_to_write} B of epoch-start tables are left"
        );
        last = now;
        if epoch.tick_one(&dbs) {
            break;
        }
    }
    let records = epoch.try_finish(&dbs).expect("the save must complete");
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(N_DBS, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// Review 5 of moon#1228: removing the post-epoch rows from a frozen table
/// does not shrink its segments. An insert flood into a small database grew
/// its table far past the epoch-start one; frozen, it keeps a skeleton for
/// every inserted row unless the drain moves the epoch-start rows into a
/// table sized for them. Both a pending database and the one in progress
/// (whose written rows go too, and whose walk then resumes mid-hash-space
/// on the fresh table): the file is the epoch-start keyspace either way.
#[test]
fn a_frozen_table_keeps_no_skeleton_of_post_epoch_rows() {
    for flushed in [1usize, 0] {
        let mut dbs = vec![Database::new(), Database::new()];
        preload(&mut dbs[0], "a", 1500);
        preload(&mut dbs[1], "b", 100);
        let start_segments = dbs[flushed].data().segment_count();
        let expected = string_keyspace(&dbs);
        let mut epoch = Epoch::begin(&dbs);
        for _ in 0..3 {
            assert!(!epoch.tick_one(&dbs));
        }
        let mut tail = Tail::new();
        for j in 0..20_000u32 {
            let k = format!("post:{j}");
            live(&mut dbs, &mut tail, flushed, &[b"SET", k.as_bytes(), b"v"]);
        }
        let grown = dbs[flushed].data().segment_count();
        assert!(
            grown > 8 * start_segments,
            "setup: {grown} segments vs {start_segments}"
        );
        live(&mut dbs, &mut tail, flushed, &[b"FLUSHDB"]);
        epoch.drain_until_trimmed(flushed);
        let frozen = epoch
            .state
            .as_ref()
            .expect("epoch")
            .frozen_segments_for_test(flushed)
            .expect("the flushed table is frozen");
        assert!(
            frozen <= 2 * start_segments.max(4),
            "db {flushed}: the frozen table kept {frozen} segments of a {grown}-segment \
             table; the epoch-start one had {start_segments}"
        );
        let records = epoch.try_finish(&dbs).expect("the save must complete");
        assert_eq!(
            diverge(&expected, &records),
            Default::default(),
            "db {flushed}: the file is not the epoch-start keyspace"
        );
        let recovered = recover(2, records, &tail);
        assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
    }
}

/// Review 6 (S1): the trim is budgeted across drains. Done whole inside one
/// drain it cost 0.88 s for a pending database of 0.5M epoch-start rows
/// grown by 1.5M (release). A table grown by 50,000 post-epoch rows: each
/// drain does at most `TRIM_BUDGET` row operations, the trim completes after
/// ceil(its work / budget) drains, and the save's file is still the
/// epoch-start keyspace with the byte bound holding once the trim is done.
#[test]
fn one_drain_trims_at_most_the_budget() {
    use crate::persistence::snapshot::frozen::TRIM_BUDGET;
    let mut dbs = vec![Database::new(), Database::new()];
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 200);
    let start_bill = dbs[1].ledger_bytes() as u64;
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    let mut tail = Tail::new();
    const POST: usize = 50_000;
    for j in 0..POST {
        let k = format!("post:{j}");
        live(&mut dbs, &mut tail, 1, &[b"SET", k.as_bytes(), b"v"]);
    }
    live(&mut dbs, &mut tail, 1, &[b"FLUSHDB"]);
    // The first drain folds the 50,000 tombstones in, freezes the table and
    // starts its trim: no more than the budget of them may be consumed.
    epoch.drain();
    let state = epoch.state.as_ref().expect("epoch");
    let done = POST - state.pending_pre_images();
    assert!(
        done <= TRIM_BUDGET && state.trim_ops_last_drain_for_test() <= TRIM_BUDGET,
        "one drain trimmed {done} rows of a {POST}-row table; the budget is {TRIM_BUDGET}"
    );
    assert_eq!(state.frozen_trimmed_for_test(1), Some(false));
    let mut ops = state.trim_ops_last_drain_for_test();
    let mut drains = 1;
    while epoch
        .state
        .as_ref()
        .expect("epoch")
        .frozen_trimmed_for_test(1)
        == Some(false)
    {
        epoch.drain();
        let state = epoch.state.as_ref().expect("epoch");
        assert!(state.trim_ops_last_drain_for_test() <= TRIM_BUDGET);
        ops += state.trim_ops_last_drain_for_test();
        drains += 1;
    }
    // Every drain but the last did (nearly) a full budget: a segment that
    // does not fit waits for the next drain.
    assert!(
        ops > POST && drains <= ops.div_ceil(TRIM_BUDGET) + 1,
        "{ops} row operations took {drains} drains of {TRIM_BUDGET}"
    );
    let held = epoch.state.as_ref().expect("epoch").cow_bytes();
    assert!(
        held <= start_bill,
        "trimmed: {held} B held, epoch-start {start_bill} B"
    );
    let records = epoch.try_finish(&dbs).expect("the save must complete");
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(2, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// Review 7, item 2: the budget counted a removed collection as one row
/// operation whatever its size, and only values of 4,096+ elements were
/// freed off the shard thread — one drain of 512 removed 1,000-field hashes
/// charged and freed 512,000 elements inline (release server, 600 x 4,000
/// fields: PING gaps of 74-85 ms). A collection now costs `1 + elements /
/// 64` operations, and every lazily freeable one (> 64 elements) is freed
/// by the helper thread.
#[test]
fn a_drain_trims_a_bounded_number_of_collection_elements() {
    use crate::persistence::snapshot::frozen::{TRIM_BUDGET, take_trim_elements_for_test};
    use crate::storage::db::LAZY_FREE_THRESHOLD;
    const HASHES: usize = 64;
    const FIELDS: usize = 1_000;
    let mut dbs = vec![Database::new(), Database::new()];
    preload(&mut dbs[0], "a", 100);
    preload(&mut dbs[1], "b", 50);
    let start_bill = dbs[1].ledger_bytes() as u64;
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let mut tail = Tail::new();
    let fields: Vec<Vec<u8>> = (0..FIELDS)
        .flat_map(|f| [format!("f{f:05}").into_bytes(), b"value".to_vec()])
        .collect();
    for h in 0..HASHES {
        let key = format!("hash:{h}");
        let mut parts: Vec<&[u8]> = vec![b"HSET", key.as_bytes()];
        parts.extend(fields.iter().map(Vec::as_slice));
        live(&mut dbs, &mut tail, 1, &parts);
    }
    live(&mut dbs, &mut tail, 1, &[b"FLUSHDB"]);
    let _ = take_trim_elements_for_test();
    let (mut charged, mut inline, mut worst) = (0, 0, 0);
    loop {
        epoch.drain();
        let (c, i) = take_trim_elements_for_test();
        (charged, inline, worst) = (charged + c, inline + i, worst.max(c));
        let state = epoch.state.as_ref().expect("epoch");
        if state.frozen_trimmed_for_test(1) != Some(false) {
            break;
        }
    }
    assert_eq!(
        charged,
        HASHES * FIELDS,
        "setup: the trim removed every hash"
    );
    assert!(
        worst <= TRIM_BUDGET * LAZY_FREE_THRESHOLD + FIELDS,
        "one drain charged {worst} collection elements (budget {TRIM_BUDGET} operations)"
    );
    assert_eq!(
        inline, 0,
        "the trim freed {inline} collection elements inline"
    );
    let held = epoch.state.as_ref().expect("epoch").cow_bytes();
    assert!(
        held <= start_bill,
        "trimmed: {held} B held, epoch-start {start_bill} B"
    );
    let records = epoch.try_finish(&dbs).expect("the save must complete");
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(2, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// Review 7, item 3: once the walk has written a database whose table a
/// FLUSHDB froze, the table was dropped inline, on the shard thread (release
/// server, 2M rows: a 34-40 ms stall as the walk left that database, 4.5 ms
/// without the FLUSHDB). It goes to the `moon-snapdrop` helper now.
#[test]
fn the_walk_releases_a_frozen_table_off_the_shard_thread() {
    use crate::persistence::snapshot::frozen::take_tables_discarded_for_test;
    let mut dbs = vec![Database::new(), Database::new()];
    preload(&mut dbs[0], "a", 100);
    preload(&mut dbs[1], "b", 2000);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
    epoch.drain_until_trimmed(1);
    let _ = take_tables_discarded_for_test();
    let records = epoch.try_finish(&dbs).expect("the save must complete");
    assert_eq!(
        take_tables_discarded_for_test(),
        1,
        "the walk dropped db 1's frozen table inline"
    );
    assert_eq!(diverge(&expected, &records), Default::default());
}

/// Review 7, item 3: an aborted save dropped every frozen table inline.
#[test]
fn an_aborted_save_releases_its_frozen_tables_off_the_shard_thread() {
    use crate::persistence::snapshot::frozen::take_tables_discarded_for_test;
    let mut dbs = vec![Database::new(), Database::new(), Database::new()];
    preload(&mut dbs[1], "b", 2000);
    preload(&mut dbs[2], "c", 2000);
    let mut epoch = Epoch::begin(&dbs);
    let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
    let _ = run(&mut dbs, 2, &[b"FLUSHDB"]);
    epoch.drain_until_trimmed(1);
    epoch.drain_until_trimmed(2);
    let _ = take_tables_discarded_for_test();
    apply(&mut dbs, 0, &[b"FLUSHALL"]);
    epoch.drain();
    assert!(epoch.state.as_ref().expect("epoch").aborted().is_some());
    assert_eq!(
        take_tables_discarded_for_test(),
        2,
        "the abort dropped the frozen tables inline"
    );
    assert!(epoch.try_finish(&dbs).is_err());
}

/// Review 6 (S1), measurement only: the trim's cost per drain and in drains,
/// against main's FLUSHDB (a drop). Run in a release build:
/// `MOON_TEST_TRIM_N=<rows> cargo test --profile release-fast --lib trim_cost -- --ignored --nocapture`.
#[test]
#[ignore = "measurement: run with --ignored in a release build"]
fn trim_cost_of_a_large_flushed_table() {
    use std::time::{Duration, Instant};
    let n: u32 = std::env::var("MOON_TEST_TRIM_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let fill = |db: &mut Database, prefix: &str, n: u32| {
        for i in 0..n {
            db.set_string(
                format!("{prefix}:{i:09}").as_bytes(),
                Bytes::from_static(b"v"),
            );
        }
    };
    let drain_all = |epoch: &mut Epoch, db: usize| -> (usize, (Duration, usize), Duration) {
        let (mut drains, mut worst, mut total) = (0, (Duration::ZERO, 0), Duration::ZERO);
        loop {
            let t = Instant::now();
            epoch.drain();
            let e = t.elapsed();
            drains += 1;
            if e > worst.0 {
                worst = (e, drains);
            }
            total += e;
            let trimmed = epoch
                .state
                .as_ref()
                .and_then(|s| s.frozen_trimmed_for_test(db));
            if trimmed != Some(false) {
                return (drains, worst, total);
            }
        }
    };
    {
        snapshot_cow::disarm();
        let mut dbs = vec![Database::new()];
        fill(&mut dbs[0], "a", n);
        let t = Instant::now();
        let _ = run(&mut dbs, 0, &[b"FLUSHDB"]);
        eprintln!(
            "[trim n={n}] FLUSHDB with no save running (a drop): {:?}",
            t.elapsed()
        );
    }
    // The database in progress, flushed with the cursor half way (step 2).
    {
        let mut dbs = vec![Database::new(), Database::new()];
        fill(&mut dbs[0], "a", n);
        let mut epoch = Epoch::begin(&dbs);
        while epoch.state.as_ref().expect("epoch").cursor() < (1u64 << 63)
            && epoch.state.as_ref().expect("epoch").current_db_index() == 0
        {
            epoch.tick(&dbs);
        }
        let _ = run(&mut dbs, 0, &[b"FLUSHDB"]);
        let (drains, worst, total) = drain_all(&mut epoch, 0);
        eprintln!(
            "[trim n={n}] in-progress db, cursor at 50%: {drains} drains, worst {worst:?}, \
             total {total:?}"
        );
        let _ = epoch.try_finish(&dbs);
    }
    // A pending database of n/2 rows grown by 1.5n (steps 1 and 3).
    {
        let mut dbs = vec![Database::new(), Database::new()];
        fill(&mut dbs[0], "a", 1000);
        fill(&mut dbs[1], "b", n / 2);
        let mut epoch = Epoch::begin(&dbs);
        for i in 0..(n + n / 2) {
            let k = format!("p:{i:09}");
            let _ = run(&mut dbs, 1, &[b"SET", k.as_bytes(), b"v"]);
            if i % 4096 == 0 {
                epoch.drain();
            }
        }
        epoch.drain();
        let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
        let (drains, worst, total) = drain_all(&mut epoch, 1);
        eprintln!(
            "[trim n={n}] pending db n/2 grown by 1.5n: {drains} drains, worst {worst:?}, \
             total {total:?}"
        );
        let _ = epoch.try_finish(&dbs);
    }
}

/// The trim runs at the drain, so a flushed table waits whole until then
/// (one tick). Two databases that GREW during the save, flushed before one
/// drain (a MULTI, a script, a pipeline), fail the save the way every flush
/// used to — the epoch never holds more than one grown table's post-epoch
/// rows, or `FREEZE_WAIT_SLACK`, beside the epoch-start dataset. The same
/// flushes with a drain between them are trimmed and the save completes.
#[test]
fn grown_tables_flushed_before_one_drain_fail_the_save() {
    for drain_between in [false, true] {
        let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
        for (i, db) in dbs.iter_mut().enumerate() {
            preload(db, &format!("d{i}"), 200);
        }
        let mut epoch = Epoch::begin(&dbs);
        assert!(!epoch.tick_one(&dbs));
        let mut tail = Tail::new();
        let value = vec![b'x'; 1 << 20];
        for db in [1, 2] {
            for j in 0..5u32 {
                let k = format!("big:{j}");
                live(&mut dbs, &mut tail, db, &[b"SET", k.as_bytes(), &value]);
            }
            live(&mut dbs, &mut tail, db, &[b"FLUSHDB"]);
            if drain_between {
                epoch.drain();
            }
        }
        let outcome = epoch.try_finish(&dbs);
        if drain_between {
            assert!(outcome.is_ok(), "drained between the flushes: {outcome:?}");
        } else {
            let Err(why) = outcome else {
                panic!("two grown tables waited whole for one drain, and the save completed");
            };
            assert!(why.contains("FLUSHDB"), "abort reason: {why}");
        }
    }
}

#[test]
fn swapdb_of_an_unfinished_database_keeps_both_images() {
    assert_point_in_time(3, 1, &[b"SWAPDB", b"1", b"2"]);
}

/// The database being written swapped with a pending one, mid-walk: the rest
/// of db 0's range is read from the slot its table moved to.
#[test]
fn swapdb_of_the_database_in_progress_keeps_both_images() {
    assert_point_in_time(2, 2, &[b"SWAPDB", b"0", b"1"]);
}

/// Review 4 nit: a frozen table is billed (INFO `current_cow_size`) at its
/// database's `used_memory`, not `estimated_memory()` — the spill-in-flight
/// payloads that figure adds are not in the table. (The flushed database is
/// one the walk has not reached, so the review-5 trim keeps every row.)
#[test]
fn a_frozen_table_is_billed_without_the_spill_in_flight_bytes() {
    let mut dbs = vec![Database::new(), Database::new()];
    preload(&mut dbs[0], "a", 1500);
    preload(&mut dbs[1], "b", 3000);
    let table_bytes = dbs[1].estimated_memory() as u64;
    dbs[1].spill_inflight_mark(
        Bytes::from_static(b"spilled"),
        crate::storage::db::PendingSpill {
            req_id: 1,
            value_type: crate::persistence::kv_page::ValueType::String,
            value_bytes: Bytes::from(vec![0u8; 1 << 20]),
            ttl_ms: None,
        },
    );
    assert!(
        dbs[1].estimated_memory() as u64 > table_bytes + (1 << 20),
        "setup: the in-flight payload is billed to the database"
    );
    let mut epoch = Epoch::begin(&dbs);
    assert!(!epoch.tick_one(&dbs));
    let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
    epoch.drain();
    let held = epoch.state.as_ref().expect("epoch").cow_bytes();
    let _ = epoch.try_finish(&dbs);
    assert_eq!(held, table_bytes, "the frozen table's bill");
}

/// Review 6 (S2, the reviewer's proof): the abort fired on ANY second freeze
/// once a grown table waited with more than `FREEZE_WAIT_SLACK` of excess,
/// even a table that did not grow at all — `SELECT 1; FLUSHDB; SELECT 2;
/// FLUSHDB` after a bulk load into db 1 failed the save, in either order
/// only when the grown one went first. ONE grown table waiting is allowed:
/// both orders must complete.
#[test]
fn an_ungrown_second_flush_does_not_fail_the_save() {
    for grown_first in [true, false] {
        let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
        for (i, db) in dbs.iter_mut().enumerate() {
            preload(db, &format!("d{i}"), 200);
        }
        let expected = string_keyspace(&dbs);
        let mut epoch = Epoch::begin(&dbs);
        assert!(!epoch.tick_one(&dbs));
        let mut tail = Tail::new();
        let value = vec![b'x'; 1 << 20];
        for j in 0..10u32 {
            let k = format!("big:{j}");
            live(&mut dbs, &mut tail, 1, &[b"SET", k.as_bytes(), &value]);
        }
        let order: [usize; 2] = if grown_first { [1, 2] } else { [2, 1] };
        for db in order {
            live(&mut dbs, &mut tail, db, &[b"FLUSHDB"]);
        }
        let outcome = epoch.try_finish(&dbs);
        let records = outcome.unwrap_or_else(|e| {
            panic!(
                "grown first = {grown_first}: only ONE grown table waited, yet the save failed: {e}"
            )
        });
        assert_eq!(diverge(&expected, &records), Default::default());
        let recovered = recover(3, records, &tail);
        assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
    }
}

/// Review 6 (S2, re-derived for the budgeted trim): a grown table holds its
/// post-epoch rows until its trim's steps 1-2 are done, which now takes
/// ceil(work / budget) drains — not one. A second GROWN flush while the first
/// grown table is still trimming fails the save once their post-epoch bytes
/// pass `FREEZE_WAIT_SLACK`; the same flush after the first trim is done
/// completes. So the epoch never holds more than one grown table's post-epoch
/// rows (or the slack) beyond the epoch-start bills.
#[test]
fn a_grown_flush_while_another_grown_table_is_trimming_fails_the_save() {
    use crate::persistence::snapshot::frozen::set_trim_budget_for_test;
    for first_trim_done in [false, true] {
        set_trim_budget_for_test(Some(16));
        let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
        for (i, db) in dbs.iter_mut().enumerate() {
            preload(db, &format!("d{i}"), 200);
        }
        let mut epoch = Epoch::begin(&dbs);
        assert!(!epoch.tick_one(&dbs));
        let mut tail = Tail::new();
        let value = vec![b'x'; 8 << 10];
        let grow = |dbs: &mut Vec<Database>, tail: &mut Tail, db: usize| {
            for j in 0..2000u32 {
                let k = format!("g{db}:{j}");
                live(dbs, tail, db, &[b"SET", k.as_bytes(), &value]);
            }
            live(dbs, tail, db, &[b"FLUSHDB"]);
        };
        grow(&mut dbs, &mut tail, 1);
        epoch.drain();
        assert_eq!(
            epoch
                .state
                .as_ref()
                .expect("epoch")
                .frozen_trimmed_for_test(1),
            Some(false),
            "setup: db 1's trim spans many drains"
        );
        if first_trim_done {
            epoch.drain_until_trimmed(1);
        }
        grow(&mut dbs, &mut tail, 2);
        let outcome = epoch.try_finish(&dbs);
        set_trim_budget_for_test(None);
        if first_trim_done {
            assert!(outcome.is_ok(), "db 1 was trimmed: {outcome:?}");
        } else {
            let Err(why) = outcome else {
                panic!(
                    "two grown tables held their post-epoch rows at once, and the save completed"
                );
            };
            assert!(why.contains("FLUSHDB"), "abort reason: {why}");
        }
    }
}

/// Review 7, item 4: the untrimmed excess the S2 rule weighs was published
/// by the drain, BEFORE the tick's walk ran. A walk that finished a
/// database released its frozen table, trim unfinished; the published
/// excess still counted it until the next drain, so a grown flush later in
/// that tick failed the save although no other grown table was held.
#[test]
fn a_table_the_walk_released_is_not_weighed_against_a_later_flush() {
    use crate::persistence::snapshot::frozen::set_trim_budget_for_test;
    // One row a drain: db 1's trim outlasts the walk over db 1.
    set_trim_budget_for_test(Some(1));
    let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    preload(&mut dbs[1], "d1", 200);
    preload(&mut dbs[2], "d2", 200);
    let expected = string_keyspace(&dbs);
    let mut epoch = Epoch::begin(&dbs);
    let mut tail = Tail::new();
    let value = vec![b'x'; 8 << 10];
    let grow = |dbs: &mut Vec<Database>, tail: &mut Tail, db: usize| {
        for j in 0..1200u32 {
            let k = format!("g{db}:{j}");
            live(dbs, tail, db, &[b"SET", k.as_bytes(), &value]);
        }
        live(dbs, tail, db, &[b"FLUSHDB"]);
    };
    grow(&mut dbs, &mut tail, 1);
    epoch.drain();
    // The walk writes db 1 from its frozen table and releases it, trimmed
    // or not; the flush of db 2 comes right after that tick's advance.
    while epoch.state.as_ref().expect("epoch").current_db_index() <= 1 {
        assert!(!epoch.tick(&dbs));
    }
    assert_eq!(
        epoch.state.as_ref().expect("epoch").untrimmed_excess(),
        0,
        "setup: db 1's table is released"
    );
    grow(&mut dbs, &mut tail, 2);
    let outcome = epoch.try_finish(&dbs);
    set_trim_budget_for_test(None);
    let records = outcome.expect("one grown table was held: the save must complete");
    assert_eq!(diverge(&expected, &records), Default::default());
    let recovered = recover(3, records, &tail);
    assert_eq!(string_keyspace(&recovered), string_keyspace(&dbs));
}

/// Review 6 (N1) and review 7: an UNLINKed large collection stays CHARGED to
/// `used_memory` until the lazy-free drain frees it (moon#1190), and a
/// FLUSHDB hands the epoch that ledger as the frozen table's bill. Review 5
/// billed the charge on top of the pre-image the trim restores, for the
/// whole save (bill 551,044 B vs rows 278,817 B). Review 6 freed the value
/// INSIDE the FLUSHDB to drop the charge — the very stall moon#1190 exists to
/// avoid (231-244 ms for a 5M-field hash, release). Now the FLUSHDB leaves
/// the value queued, the queue credits the frozen bill as it frees it, and
/// once it is done the bill is exactly what the table's rows hold.
#[test]
fn a_lazy_free_charge_is_credited_to_the_frozen_table_not_freed_inline() {
    let mut dbs = vec![Database::new(), Database::new()];
    for i in 0..50 {
        let k = format!("k{i}");
        let _ = run(&mut dbs, 1, &[b"SET", k.as_bytes(), b"v"]);
    }
    for f in 0..2000 {
        let f = format!("field-{f:06}");
        let _ = run(&mut dbs, 1, &[b"HSET", b"big", f.as_bytes(), b"some-value"]);
    }
    let start_bill = dbs[1].ledger_bytes() as u64;
    let mut epoch = Epoch::begin(&dbs);
    let _ = run(&mut dbs, 1, &[b"UNLINK", b"big"]);
    assert!(
        dbs[1].lazy_free_reclaimable(),
        "setup: the UNLINKed hash waits in the lazy-free queue, still charged"
    );
    let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
    assert_eq!(
        dbs[1].lazy_free_len(),
        1,
        "the FLUSHDB freed the UNLINKed hash inline (review 6's reclaim)"
    );
    epoch.drain_until_trimmed(1);
    // The shard tick's lazy-free drain frees it; the next drain credits the
    // frozen bill with what it freed.
    let _ = dbs[1].drain_lazy_free_elements(usize::MAX);
    assert_eq!(dbs[1].lazy_free_len(), 0);
    epoch.drain();
    let state = epoch.state.as_ref().expect("epoch");
    let (bill, rows) = state.frozen_bill_and_rows_for_test(1).expect("frozen");
    let _ = epoch.try_finish(&dbs);
    assert_eq!(
        bill, rows,
        "once the lazy free is done the frozen bill is what its rows hold (epoch-start bill \
         {start_bill} B)"
    );
    assert!(
        bill <= start_bill,
        "{bill} B frozen, epoch-start {start_bill} B"
    );
}

/// Review 7: a value whose charge moved to a frozen table's bill may still
/// be draining when that save ends. Its credits belong to that save's table
/// alone: a later save, which may freeze the same database again, must not
/// take them off its own bill (each arming has its own serial).
#[test]
fn a_value_freed_after_its_save_ended_credits_no_later_save() {
    let mut dbs = vec![Database::new(), Database::new()];
    for f in 0..2000 {
        let f = format!("field-{f:06}");
        let _ = run(&mut dbs, 1, &[b"HSET", b"big", f.as_bytes(), b"some-value"]);
    }
    let mut first = Epoch::begin(&dbs);
    let _ = run(&mut dbs, 1, &[b"UNLINK", b"big"]);
    let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
    first.try_finish(&dbs).expect("the first save completes");
    assert_eq!(dbs[1].lazy_free_len(), 1, "setup: still draining");

    for i in 0..300 {
        let k = format!("k{i}");
        let _ = run(&mut dbs, 1, &[b"SET", k.as_bytes(), b"v"]);
    }
    let mut second = Epoch::begin(&dbs);
    let _ = run(&mut dbs, 1, &[b"FLUSHDB"]);
    second.drain_until_trimmed(1);
    let _ = dbs[1].drain_lazy_free_elements(usize::MAX);
    second.drain();
    let (bill, rows) = second
        .state
        .as_ref()
        .expect("epoch")
        .frozen_bill_and_rows_for_test(1)
        .expect("frozen");
    let _ = second.try_finish(&dbs);
    assert_eq!(
        bill, rows,
        "the first save's credits reached the second save's bill"
    );
}

/// The liveness half of moon#1228: a workload that FLUSHDBs (and SWAPDBs)
/// more often than one save takes. It used to fail every BGSAVE; each tick
/// here flushes, swaps, refills and increments, and the save must still
/// complete with the epoch-start keyspace. Randomized over seeds (printed on
/// failure). (A FLUSHALL fails the save — redis parity, above — so it is
/// not part of this workload.)
#[test]
fn a_workload_flushing_faster_than_one_save_still_completes_it() {
    use super::epoch_harness::Rng;
    for seed in 1..=12u64 {
        let mut rng = Rng(seed);
        let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
        for (i, db) in dbs.iter_mut().enumerate() {
            preload(db, &format!("d{i}"), 1200);
        }
        let expected = string_keyspace(&dbs);
        let mut epoch = Epoch::begin(&dbs);
        let mut tail = Tail::new();
        let mut ticks = 0u32;
        loop {
            for _ in 0..8 {
                let db = rng.below(3) as usize;
                let k = format!("d{}:{:06}", rng.below(3), rng.below(1400));
                match rng.below(10) {
                    0 => live(&mut dbs, &mut tail, db, &[b"FLUSHDB"]),
                    1 => live(&mut dbs, &mut tail, db, &[b"FLUSHDB", b"ASYNC"]),
                    2 => {
                        let a = rng.below(3).to_string();
                        let b = rng.below(3).to_string();
                        live(
                            &mut dbs,
                            &mut tail,
                            db,
                            &[b"SWAPDB", a.as_bytes(), b.as_bytes()],
                        );
                    }
                    3 => live(&mut dbs, &mut tail, db, &[b"DEL", k.as_bytes()]),
                    4 | 5 => live(&mut dbs, &mut tail, db, &[b"SET", k.as_bytes(), b"s"]),
                    _ => live(&mut dbs, &mut tail, db, &[b"INCR", k.as_bytes()]),
                }
            }
            ticks += 1;
            if epoch.tick_one(&dbs) {
                break;
            }
            assert!(ticks < 100_000, "seed {seed}: the epoch never converged");
        }
        let records = epoch
            .try_finish(&dbs)
            .unwrap_or_else(|e| panic!("seed {seed}: the save failed: {e}"));
        assert_eq!(
            diverge(&expected, &records),
            Default::default(),
            "seed {seed}: the file is not the epoch-start keyspace"
        );
        let recovered = recover(3, records, &tail);
        assert_eq!(
            string_keyspace(&recovered),
            string_keyspace(&dbs),
            "seed {seed}: file + tail is not the live keyspace"
        );
    }
}

/// The hook sits in the one place every SWAPDB path (coordinator local leg,
/// the SPSC arm, replica apply) exchanges tables: `ShardDbSet::swap`. Under
/// an armed epoch it re-maps the slots (moon#1228) instead of aborting.
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
    let mapped = snapshot_cow::logical_of_slot_for_test();
    let aborted = snapshot_cow::abort_pending_for_test();
    snapshot_cow::disarm();
    assert_eq!(
        mapped,
        vec![Some(1), Some(0)],
        "SWAPDB under an armed epoch went unnoticed"
    );
    assert!(
        aborted.is_none(),
        "a SWAPDB must not fail the save any more"
    );
    // Unarmed: a swap is free and leaves nothing behind.
    crate::shard::slice::with_shard(|s| s.databases.swap(0, 1));
    assert!(snapshot_cow::abort_pending_for_test().is_none());
}

/// `Database::clear` on the shard's own db plane is recognised by address:
/// a FLUSHDB of an unfinished database freezes its table instead of
/// aborting, and unmaps the slot.
#[test]
fn a_flush_on_the_db_plane_freezes_the_table_of_an_unfinished_database() {
    let (shared, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
        Database::new(),
        Database::new(),
    ]]);
    let _keep = shared;
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
    crate::shard::slice::with_shard_db(1, |db| preload(db, "b", 50));
    snapshot_cow::disarm();
    snapshot_cow::arm_with_layout(vec![1, 1]);
    crate::shard::slice::with_shard_db(1, |db| db.clear());
    let mapped = snapshot_cow::logical_of_slot_for_test();
    let aborted = snapshot_cow::abort_pending_for_test();
    snapshot_cow::disarm();
    assert_eq!(mapped, vec![Some(0), None], "slot 1's table was frozen");
    assert!(
        aborted.is_none(),
        "a FLUSHDB must not fail the save any more"
    );
}

/// moon#1227 review F6: a replica FULL RESYNC replaces every database of the
/// shard outside `command::dispatch` — `replication::apply::load_snapshot`
/// clears each table and loads the master's RDB in its place — so neither
/// the FLUSH hook nor `ShardDbSet::swap` saw it, and an epoch armed on the
/// replica published a file mixing its own epoch-start data with the
/// master's. It must abort exactly like FLUSHALL (BGSAVE fails, the previous
/// file stays, the tables its clears detach are dropped at once); a resync
/// with nothing armed, or after the epoch finished writing, changes nothing.
#[test]
fn a_replica_full_resync_aborts_an_unfinished_epoch() {
    let (shared, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
        Database::new(),
        Database::new(),
    ]]);
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
    crate::shard::slice::with_shard_db(0, |db| preload(db, "replica", 100));
    let mut master = vec![Database::new(), Database::new()];
    preload(&mut master[0], "master", 10);
    let mut rdb = Vec::new();
    crate::persistence::redis_rdb::write_rdb(&master, &mut rdb);

    snapshot_cow::disarm();
    snapshot_cow::arm_with_layout(vec![1, 1]);
    let loaded = crate::replication::apply::load_snapshot(&rdb, &shared).map_err(|e| e.to_string());
    let aborted = snapshot_cow::abort_pending_for_test();
    let frozen = snapshot_cow::frozen_tables_queued_for_test();
    snapshot_cow::disarm();
    assert_eq!(loaded, Ok(10), "setup: the resync loads the master's keys");
    assert_eq!(
        frozen, 0,
        "the resync's cleared tables must be dropped at once, not held until the next drain"
    );
    assert!(
        aborted.is_some(),
        "a full resync under an armed epoch went unnoticed: the file would mix \
         the replica's epoch-start data with the master's"
    );

    // Armed, but every database already written: the file is complete and
    // point-in-time; the resync replays nothing into it. No abort.
    snapshot_cow::arm_with_layout(vec![1, 1]);
    snapshot_cow::note_progress(2, 0);
    assert!(crate::replication::apply::load_snapshot(&rdb, &shared).is_ok());
    let finished = snapshot_cow::abort_pending_for_test();
    snapshot_cow::disarm();
    assert!(finished.is_none(), "a finished epoch must not be failed");

    // Unarmed: free, leaves nothing behind.
    assert!(crate::replication::apply::load_snapshot(&rdb, &shared).is_ok());
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
