//! moon#1225: a list move must never pop an element it cannot place.
//!
//! A list key INDEXED in the cold tier whose bytes cannot be read (the
//! moon#875 fault class) used to read as ABSENT to the list write router, so:
//!
//! * `LMOVE`/`RPOPLPUSH` (and `BLMOVE`/`BRPOPLPUSH` served on the spot)
//!   treated a faulted DESTINATION as "no such list", popped the source, and
//!   the push's `-IOERR` was swallowed — the client was handed the element as
//!   moved while it existed nowhere (and the success was propagated);
//! * `LMPOP` skipped a faulted key and popped a LATER one, then answered
//!   `-IOERR` — the popped element was gone on the primary.
//!
//! Every case here spills real list values with the production
//! `spill_to_datafile`, removes the heap file under the live `Database` (the
//! fault as a disk produces it), runs the command through the real dispatch
//! (or the real blocking immediate scan) and asserts: the reply is `-IOERR`,
//! every element is still where it was, and nothing was fabricated in hot RAM.
//! Runtime-agnostic (plain `Database`), so it runs on both CI legs.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::persistence::manifest::ShardManifest;
use crate::protocol::Frame;
use crate::server::conn::blocking::immediate_scan;
use crate::storage::Database;
use crate::storage::compact_value::CompactValue;
use crate::storage::entry::{Entry, RedisValue};
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::kv_spill::spill_to_datafile;

/// The cold key whose file is removed — the faulted one.
const FAULTED: &str = "l1225:faulted";
/// A second cold key whose file stays: the readable-cold control.
const COLD_OK: &str = "l1225:cold-ok";

fn args(parts: &[&str]) -> Vec<Frame> {
    parts
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
        .collect()
}

fn run(db: &mut Database, cmd: &str, parts: &[&str]) -> Frame {
    let mut selected = 0usize;
    match crate::command::dispatch(db, cmd.as_bytes(), &args(parts), &mut selected, 16) {
        crate::command::DispatchResult::Response(f) | crate::command::DispatchResult::Quit(f) => f,
    }
}

fn is_ioerr(f: &Frame) -> bool {
    matches!(f, Frame::Error(e) if e.starts_with(b"IOERR cold tier"))
}

fn heap_path(shard_dir: &Path, file_id: u64) -> PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"))
}

fn list_value(items: &[&str]) -> RedisValue {
    RedisValue::List(
        items
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect::<VecDeque<Bytes>>(),
    )
}

/// A `Database` with an active cold tier holding `FAULTED` (file 1) and
/// `COLD_OK` (file 2) as spilled lists, plus the hot lists `hot`, `hot2`.
/// Returns the path of `FAULTED`'s heap file, still present.
fn fixture(shard_dir: &Path) -> (Database, PathBuf) {
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();
    for (file_id, key, items) in [
        (1u64, FAULTED, &["c1", "c2"][..]),
        (2u64, COLD_OK, &["k1", "k2"][..]),
    ] {
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(list_value(items));
        spill_to_datafile(
            shard_dir,
            file_id,
            key.as_bytes(),
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();
    }
    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);
    for (key, items) in [("hot", ["h1", "h2"]), ("hot2", ["x1", "x2"])] {
        let mut argv = vec![key];
        argv.extend(items);
        assert_eq!(run(&mut db, "RPUSH", &argv), Frame::Integer(2));
    }
    (db, heap_path(shard_dir, 1))
}

fn lrange(db: &mut Database, key: &str) -> Vec<Bytes> {
    match run(db, "LRANGE", &[key, "0", "-1"]) {
        Frame::Array(items) => items
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.clone(),
                other => panic!("LRANGE {key} item {other:?}"),
            })
            .collect(),
        other => panic!("LRANGE {key} answered {other:?}"),
    }
}

fn b(items: &[&str]) -> Vec<Bytes> {
    items
        .iter()
        .map(|s| Bytes::copy_from_slice(s.as_bytes()))
        .collect()
}

/// The faulted key is untouched: not fabricated hot, still indexed.
fn assert_faulted_key_untouched(db: &Database, what: &str) {
    assert!(
        !db.is_hot(FAULTED.as_bytes()),
        "{what}: nothing may be fabricated over the unreadable cold copy"
    );
    assert!(
        db.cold_index
            .as_ref()
            .unwrap()
            .lookup(FAULTED.as_bytes())
            .is_some(),
        "{what}: the index entry must stay (the next read retries)"
    );
}

/// The flag never outlives the command: an unrelated read answers normally.
fn assert_no_leaked_fault(db: &mut Database, what: &str) {
    assert_eq!(
        run(db, "LLEN", &["hot2"]),
        Frame::Integer(2),
        "{what}: the fault must not leak into the next command"
    );
}

#[test]
fn lmove_and_rpoplpush_onto_a_faulted_destination_refuse_before_popping() {
    for (cmd, argv) in [
        ("LMOVE", vec!["hot", FAULTED, "LEFT", "LEFT"]),
        ("LMOVE", vec!["hot", FAULTED, "RIGHT", "RIGHT"]),
        ("RPOPLPUSH", vec!["hot", FAULTED]),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, faulted_file) = fixture(tmp.path());
        std::fs::remove_file(&faulted_file).unwrap();
        let what = format!("{cmd} {argv:?}");

        let reply = run(&mut db, cmd, &argv);
        assert!(
            is_ioerr(&reply),
            "{what}: the destination is indexed but unreadable — the reply must be \
             -IOERR, got {reply:?} (moon#1225)"
        );
        assert_eq!(
            lrange(&mut db, "hot"),
            b(&["h1", "h2"]),
            "{what}: the source must keep the element the refused move would have lost"
        );
        assert_faulted_key_untouched(&db, &what);
        assert_no_leaked_fault(&mut db, &what);
    }
}

#[test]
fn lmove_from_a_faulted_source_refuses_and_leaves_the_destination_alone() {
    for (cmd, argv) in [
        ("LMOVE", vec![FAULTED, "hot", "LEFT", "RIGHT"]),
        ("RPOPLPUSH", vec![FAULTED, "hot"]),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, faulted_file) = fixture(tmp.path());
        std::fs::remove_file(&faulted_file).unwrap();
        let what = format!("{cmd} {argv:?}");

        let reply = run(&mut db, cmd, &argv);
        assert!(is_ioerr(&reply), "{what}: got {reply:?}");
        assert_eq!(lrange(&mut db, "hot"), b(&["h1", "h2"]), "{what}");
        assert_faulted_key_untouched(&db, &what);
        assert_no_leaked_fault(&mut db, &what);
    }
}

#[test]
fn lmpop_does_not_skip_a_faulted_key_to_pop_a_later_one() {
    for dir in ["LEFT", "RIGHT"] {
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, faulted_file) = fixture(tmp.path());
        std::fs::remove_file(&faulted_file).unwrap();

        let reply = run(&mut db, "LMPOP", &["2", FAULTED, "hot", dir, "COUNT", "2"]);
        assert!(
            is_ioerr(&reply),
            "LMPOP {dir}: the first key is indexed but unreadable — -IOERR, got {reply:?}"
        );
        assert_eq!(
            lrange(&mut db, "hot"),
            b(&["h1", "h2"]),
            "LMPOP {dir}: the later key must not be popped behind a faulted one (moon#1225)"
        );
        assert_faulted_key_untouched(&db, "LMPOP");
        assert_no_leaked_fault(&mut db, "LMPOP");
    }
}

/// `BLMOVE`/`BRPOPLPUSH` served on the spot do not go through `LMOVE`: they
/// run the blocking scan (`try_immediate_pop`), whose destination probe had
/// the same "unreadable = absent" blind spot and whose push swallowed the
/// refusal. The source-side fault used to PARK the client on a key that holds
/// data, with the fault flag left pending for the next command.
#[test]
fn blocking_moves_served_on_the_spot_refuse_a_faulted_endpoint() {
    let keys = |k: &str| vec![Bytes::copy_from_slice(k.as_bytes())];
    for (cmd, argv, src) in [
        ("BLMOVE", vec!["hot", FAULTED, "LEFT", "RIGHT", "0"], "hot"),
        ("BRPOPLPUSH", vec!["hot", FAULTED, "0"], "hot"),
        (
            "BLMOVE",
            vec![FAULTED, "hot", "LEFT", "RIGHT", "0"],
            FAULTED,
        ),
        ("BRPOPLPUSH", vec![FAULTED, "hot", "0"], FAULTED),
        ("BLPOP", vec![FAULTED, "0"], FAULTED),
        ("BLMPOP", vec!["0", "1", FAULTED, "LEFT"], FAULTED),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, faulted_file) = fixture(tmp.path());
        std::fs::remove_file(&faulted_file).unwrap();
        let what = format!("{cmd} {argv:?}");

        let reply = immediate_scan(cmd.as_bytes(), &args(&argv), &keys(src), &mut db, 0, 1);
        assert!(
            matches!(&reply, Some(f) if is_ioerr(f)),
            "{what}: a faulted endpoint must answer -IOERR on the spot (not move, not \
             park), got {reply:?}"
        );
        assert_eq!(
            lrange(&mut db, "hot"),
            b(&["h1", "h2"]),
            "{what}: nothing may leave the readable list"
        );
        assert_faulted_key_untouched(&db, &what);
        assert_no_leaked_fault(&mut db, &what);
    }
}

/// The control: a READABLE cold destination is not refused — the move
/// promotes it and lands the element — and once the faulted file is back the
/// same command succeeds. The refusal is about the fault, not about coldness.
#[test]
fn readable_cold_endpoints_still_move_and_the_fault_heals() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut db, faulted_file) = fixture(tmp.path());

    assert_eq!(
        run(&mut db, "LMOVE", &["hot", COLD_OK, "LEFT", "RIGHT"]),
        Frame::BulkString(Bytes::from_static(b"h1"))
    );
    assert_eq!(lrange(&mut db, COLD_OK), b(&["k1", "k2", "h1"]));
    assert_eq!(lrange(&mut db, "hot"), b(&["h2"]));

    let parked = tmp.path().join("parked.mpf");
    std::fs::rename(&faulted_file, &parked).unwrap();
    assert!(is_ioerr(&run(&mut db, "RPOPLPUSH", &["hot", FAULTED])));
    assert_eq!(lrange(&mut db, "hot"), b(&["h2"]));
    std::fs::rename(&parked, &faulted_file).unwrap();

    assert_eq!(
        run(&mut db, "RPOPLPUSH", &["hot", FAULTED]),
        Frame::BulkString(Bytes::from_static(b"h2"))
    );
    assert_eq!(lrange(&mut db, FAULTED), b(&["h2", "c1", "c2"]));
    assert_eq!(
        run(&mut db, "EXISTS", &["hot"]),
        Frame::Integer(0),
        "the drained source is removed"
    );
}
