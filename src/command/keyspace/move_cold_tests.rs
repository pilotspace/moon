//! moon#1254: `MOVE` / `COPY … DB n` of a key that lives in the cold tier.
//!
//! The source key is spilled to a real spill file (the production batch
//! writer, indexed by the production rebuild) or held in the in-flight plane,
//! exactly as eviction leaves it. `MOVE` used to start with `src.remove(key)`,
//! which drops the cold entry and the in-flight record but returns only a HOT
//! entry — so the key vanished from both databases and the reply was `:0`.

use std::path::Path;

use bytes::Bytes;

use super::{copy_core, move_core};
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::PendingSpill;
use crate::storage::entry::Entry;
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

/// A database whose `kvs` live ONLY in spill file `file_id` (listed in a
/// manifest), indexed by the production rebuild — a cold-only key.
fn cold_db(shard_dir: &Path, file_id: u64, kvs: &[(&str, &str)]) -> Database {
    let entries: Vec<SpillEntry> = kvs
        .iter()
        .map(|(k, v)| SpillEntry {
            key: Bytes::copy_from_slice(k.as_bytes()),
            value_bytes: Bytes::copy_from_slice(v.as_bytes()),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
        })
        .collect();
    let batch = build_kv_spill_batch(&entries, file_id).expect("build batch");
    let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).expect("write batch");
    let mut manifest =
        ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("create manifest");
    manifest
        .add_file(FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.pages.len() as u32,
            byte_size,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        })
        .expect("unique file id");
    manifest.commit().expect("commit manifest");
    let rebuilt = ColdIndex::rebuild_from_manifest_per_db(shard_dir, &manifest);
    let mut db = Database::new();
    db.cold_shard_dir = Some(shard_dir.to_path_buf());
    db.cold_index = rebuilt
        .per_db
        .into_iter()
        .find(|(d, _)| *d == 0)
        .map(|(_, ci)| ci);
    assert!(
        kvs.iter().all(|(k, _)| !db.is_hot(k.as_bytes())
            && db
                .cold_index
                .as_ref()
                .and_then(|ci| ci.lookup(k.as_bytes()))
                .is_some()),
        "fixture: every key is cold-only"
    );
    db
}

/// A database whose `key` is IN FLIGHT: its hot copy is gone, its payload is
/// queued for a spill that has not completed (the async spill path).
fn in_flight_db(key: &'static [u8], value: &'static [u8]) -> Database {
    let mut db = Database::new();
    db.spill_inflight_mark(
        Bytes::from_static(key),
        PendingSpill {
            req_id: 7,
            value_type: ValueType::String,
            value_bytes: Bytes::from_static(value),
            ttl_ms: None,
        },
    );
    assert!(!db.is_hot(key) && db.exists(key), "fixture: in flight");
    db
}

fn string_of(db: &mut Database, key: &[u8]) -> Option<Vec<u8>> {
    db.get(key)
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| b.to_vec())
}

fn is_ioerr(frame: &Frame) -> bool {
    matches!(frame, Frame::Error(e) if e.starts_with(b"IOERR"))
}

#[test]
fn move_of_a_spilled_key_moves_its_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut src = cold_db(dir.path(), 5, &[("k", "cold-v"), ("n", "neighbour")]);
    let mut dst = Database::new();
    let reply = move_core(&mut src, 0, &mut dst, 1, b"k");
    let in_dst = string_of(&mut dst, b"k");
    let in_src = src.exists(b"k");
    assert_eq!(
        (reply, in_dst.as_deref(), in_src),
        (Frame::Integer(1), Some(&b"cold-v"[..]), false),
        "MOVE of a spilled key: (reply, value in dst, still in src)"
    );
    assert!(
        src.cold_index.as_ref().unwrap().lookup(b"k").is_none(),
        "the source's cold entry went with the key"
    );
    assert_eq!(
        string_of(&mut src, b"n").as_deref(),
        Some(&b"neighbour"[..])
    );
}

#[test]
fn move_of_an_in_flight_key_moves_its_value() {
    let mut src = in_flight_db(b"k", b"inflight-v");
    let mut dst = Database::new();
    let reply = move_core(&mut src, 0, &mut dst, 1, b"k");
    let in_dst = string_of(&mut dst, b"k");
    let in_src = src.exists(b"k");
    assert_eq!(
        (reply, in_dst.as_deref(), in_src),
        (Frame::Integer(1), Some(&b"inflight-v"[..]), false),
        "MOVE of an in-flight key: (reply, value in dst, still in src)"
    );
    assert!(
        !src.spill_inflight_is_newest(b"k", 7),
        "the in-flight record is retired, so its completion cannot publish \
         the key back into the source"
    );
}

/// A MOVE onto an existing destination key is refused before the source is
/// touched: the cold key is neither promoted nor dropped.
#[test]
fn move_of_a_spilled_key_onto_a_collision_leaves_it_cold() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut src = cold_db(dir.path(), 5, &[("k", "cold-v")]);
    let mut dst = Database::new();
    dst.set(b"k", Entry::new_string(Bytes::from_static(b"dst-v")));
    assert_eq!(move_core(&mut src, 0, &mut dst, 1, b"k"), Frame::Integer(0));
    assert!(!src.is_hot(b"k"), "a refused MOVE does not promote");
    assert!(src.cold_index.as_ref().unwrap().lookup(b"k").is_some());
    assert_eq!(string_of(&mut src, b"k").as_deref(), Some(&b"cold-v"[..]));
    assert_eq!(string_of(&mut dst, b"k").as_deref(), Some(&b"dst-v"[..]));
}

/// A cold key whose bytes cannot be read: `-IOERR`, never a silent `:0`, and
/// the index entry stays (a transient fault must not lose the key).
#[test]
fn move_of_an_unreadable_cold_key_answers_ioerr_and_keeps_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut src = cold_db(dir.path(), 5, &[("k", "cold-v")]);
    std::fs::remove_file(dir.path().join("data").join("heap-000005.mpf")).expect("rm file");
    let mut dst = Database::new();
    let reply = move_core(&mut src, 0, &mut dst, 1, b"k");
    assert!(is_ioerr(&reply), "MOVE answered {reply:?}");
    assert!(src.cold_index.as_ref().unwrap().lookup(b"k").is_some());
    assert!(!dst.exists(b"k"));
    assert!(
        src.take_cold_fault().is_none(),
        "the fault was answered by this MOVE, not left for the next command"
    );
}

#[test]
fn copy_db_of_a_spilled_key_copies_its_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut src = cold_db(dir.path(), 5, &[("k", "cold-v")]);
    let mut dst = Database::new();
    let reply = copy_core(&mut src, 0, &mut dst, 1, b"k", b"k2", false);
    assert_eq!(reply, Frame::Integer(1));
    assert_eq!(string_of(&mut dst, b"k2").as_deref(), Some(&b"cold-v"[..]));
    assert_eq!(string_of(&mut src, b"k").as_deref(), Some(&b"cold-v"[..]));
}

#[test]
fn copy_db_of_an_in_flight_key_copies_its_value() {
    let mut src = in_flight_db(b"k", b"inflight-v");
    let mut dst = Database::new();
    let reply = copy_core(&mut src, 0, &mut dst, 1, b"k", b"k", false);
    assert_eq!(reply, Frame::Integer(1));
    assert_eq!(
        string_of(&mut dst, b"k").as_deref(),
        Some(&b"inflight-v"[..])
    );
    assert_eq!(
        string_of(&mut src, b"k").as_deref(),
        Some(&b"inflight-v"[..])
    );
}

#[test]
fn copy_db_of_an_unreadable_cold_key_answers_ioerr() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut src = cold_db(dir.path(), 5, &[("k", "cold-v")]);
    std::fs::remove_file(dir.path().join("data").join("heap-000005.mpf")).expect("rm file");
    let mut dst = Database::new();
    let reply = copy_core(&mut src, 0, &mut dst, 1, b"k", b"k", false);
    assert!(is_ioerr(&reply), "COPY … DB answered {reply:?}");
    assert!(!dst.exists(b"k"));
    assert!(src.cold_index.as_ref().unwrap().lookup(b"k").is_some());
    assert!(
        src.take_cold_fault().is_none(),
        "the fault was answered here"
    );
}
