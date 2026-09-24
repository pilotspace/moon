//! SMOVE onto an unreadable cold destination (moon#1225's sibling path).
//!
//! `SMOVE` probes the destination with `get_set`, which answered "absent"
//! for a key INDEXED in the cold tier whose bytes cannot be read; the member
//! was then taken out of the source and the destination's create refused
//! with `-IOERR` — the member existed nowhere. The source must keep it.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::manifest::ShardManifest;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_value::CompactValue;
use crate::storage::entry::{Entry, RedisValue, SetValue};
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::kv_spill::spill_to_datafile;

fn run(db: &mut Database, cmd: &str, parts: &[&str]) -> Frame {
    let args: Vec<Frame> = parts
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
        .collect();
    let mut selected = 0usize;
    match crate::command::dispatch(db, cmd.as_bytes(), &args, &mut selected, 16) {
        crate::command::DispatchResult::Response(f) | crate::command::DispatchResult::Quit(f) => f,
    }
}

/// A database whose cold tier holds the set `cold` (file 7) and whose hot
/// plane holds the set `hot` = {a, b}.
fn fixture(shard_dir: &Path) -> Database {
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    let mut cold_index = ColdIndex::new();
    let mut set = SetValue::new();
    set.insert(Bytes::from_static(b"c1"));
    let mut entry = Entry::new_string(Bytes::new());
    entry.value = CompactValue::from_redis_value(RedisValue::Set(Box::new(set)));
    spill_to_datafile(
        shard_dir,
        7,
        b"cold",
        &entry,
        0,
        &mut manifest,
        Some(&mut cold_index),
    )
    .unwrap();
    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = Some(cold_index);
    assert_eq!(run(&mut db, "SADD", &["hot", "a", "b"]), Frame::Integer(2));
    db
}

#[test]
fn smove_onto_a_faulted_destination_keeps_the_member_in_the_source() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = fixture(tmp.path());
    let heap = tmp.path().join("data").join("heap-000007.mpf");
    let parked = tmp.path().join("parked.mpf");
    std::fs::rename(&heap, &parked).unwrap();

    let reply = run(&mut db, "SMOVE", &["hot", "cold", "a"]);
    assert!(
        matches!(&reply, Frame::Error(e) if e.starts_with(b"IOERR cold tier")),
        "SMOVE onto an indexed-but-unreadable set must answer -IOERR, got {reply:?}"
    );
    assert_eq!(
        run(&mut db, "SISMEMBER", &["hot", "a"]),
        Frame::Integer(1),
        "the refused SMOVE must leave the member in the source (moon#1225 sibling)"
    );
    assert_eq!(run(&mut db, "SCARD", &["hot"]), Frame::Integer(2));
    assert!(!db.is_hot(b"cold"), "nothing fabricated over the cold copy");

    // Control: readable again, the same move goes through.
    std::fs::rename(&parked, &heap).unwrap();
    assert_eq!(
        run(&mut db, "SMOVE", &["hot", "cold", "a"]),
        Frame::Integer(1)
    );
    assert_eq!(run(&mut db, "SCARD", &["cold"]), Frame::Integer(2));
    assert_eq!(run(&mut db, "SISMEMBER", &["hot", "a"]), Frame::Integer(0));
}
