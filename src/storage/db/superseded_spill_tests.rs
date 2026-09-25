//! moon#1253: a key whose spill a write retired while it was still in flight
//! must stay dead across an AOF rewrite that folds BEFORE that spill's
//! completion is applied, then a crash.
//!
//! The crash image. `k1` and `k3` were evicted into spill request 7. A `DEL
//! k1` (or a FLUSH) retired `k1`'s in-flight record, and a rewrite folded
//! next: `k1` was in no plane and in no ledger, so the new generation's head
//! carried no `DEL k1`, and the rewrite discarded the old generation's. Then
//! request 7's completion landed. File 7, holding `k1`'s slot, was listed for
//! `k3`, and `k1`'s ghost slot reached the dead-slot ledger, too late for the
//! head. A restart indexes every slot of file 7 and the new generation
//! authorizes it wholesale: by `MOON.COLDCUT` when 7 is below the watermark,
//! by the `MOON.SPILLED 7 k3` marker otherwise. `k1` came back. This was
//! reproduced on a real server (`tests/perf_ws15_spanning_cold_del.rs` with
//! no settle: 6-9 of 100 deleted keys back, 4 runs out of 4).
//!
//! Same harness as `storage::tiered::cold_del_rewrite_tests`: real spill files,
//! the PRODUCTION fold (`stream_fold_image` + `write_fold_image` +
//! `generation_head`, tokio `--shards 1` flat layout; the per-shard layout
//! shares `for_each_cold_delete_chunk`), and production recovery.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::aof::fold_stream::{
    fold_image_channel, stream_fold_image, write_fold_image,
};
use crate::persistence::cold_records::{generation_head, serialize_cold_cut};
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::Database;
use crate::storage::db::PendingSpill;
use crate::storage::entry::Entry;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

type Kv<'a> = (&'a str, &'a str);

/// On disk after the crash: `k2` cold in file 5 before anything here, and
/// file 7 written by request 7 with `k1`'s (now ghost) slot and `k3`'s.
const FILES: &[(u64, &[Kv<'static>])] =
    &[(5, &[("k2", "v2")]), (7, &[("k1", "v1-old"), ("k3", "v3")])];

fn spill_files(shard_dir: &Path, files: &[(u64, &[Kv<'_>])]) {
    let mut manifest =
        ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("create manifest");
    for (file_id, kvs) in files {
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
        let batch = build_kv_spill_batch(&entries, *file_id).expect("build batch");
        let byte_size = write_kv_spill_batch(shard_dir, *file_id, &batch).expect("write batch");
        manifest
            .add_file(FileEntry {
                file_id: *file_id,
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
            .expect("unique file ids");
    }
    manifest.commit().expect("commit manifest");
}

fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn recover(files: &[(u64, &[Kv<'_>])], aof: &[u8]) -> (tempfile::TempDir, Database) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("shard dir");
    spill_files(&shard_dir, files);
    let legacy = tmp.path().join("legacy");
    std::fs::create_dir_all(&legacy).expect("aof dir");
    std::fs::write(legacy.join("appendonly.aof"), aof).expect("write aof");
    let mut dbs = vec![Database::new()];
    crate::persistence::recovery::recover_shard_v3_with_fallback(
        &mut dbs,
        0,
        &shard_dir,
        &crate::persistence::replay::DispatchReplayEngine::new(),
        Some(&legacy),
        false,
    )
    .expect("recovery");
    (tmp, dbs.remove(0))
}

fn value(db: &mut Database, key: &[u8]) -> Option<Vec<u8>> {
    db.promote_cold_if_present(key, 0);
    db.get(key)
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| b.to_vec())
}

fn in_flight(db: &mut Database, key: &'static [u8], value: &'static [u8], req_id: u64) {
    db.spill_inflight_mark(
        Bytes::from_static(key),
        PendingSpill {
            req_id,
            value_type: ValueType::String,
            value_bytes: Bytes::from_static(value),
            ttl_ms: None,
        },
    );
}

/// The live server just before the write under test: `k2` cold in file 5,
/// `k1` and `k3` in flight in request 7.
fn live() -> (tempfile::TempDir, Database) {
    let mut aof = serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"SET", b"k2", b"v2"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"5", b"k2"]));
    let (dir, mut db) = recover(&FILES[..1], &aof);
    assert!(!db.is_hot(b"k2"), "fixture: k2 is cold");
    in_flight(&mut db, b"k1", b"v1-old", 7);
    in_flight(&mut db, b"k3", b"v3", 7);
    (dir, db)
}

/// The production fold of `db` at a `MOON.COLDCUT watermark`, then what the
/// new generation logged after it: request 7's completion, applied after the
/// fold, publishing `k3` (and only `k3`) into file 7.
fn fold_then_completion(db: &Database, watermark: u64) -> Vec<u8> {
    let now = crate::storage::entry::current_time_ms();
    let (sink, image) = fold_image_channel();
    stream_fold_image(&[db], now, sink);
    let mut aof = Vec::new();
    let (_, deletes) = write_fold_image(image, &mut aof, "moon#1253 test").expect("fold image");
    aof.extend_from_slice(&generation_head(watermark, &deletes, false));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"7", b"k3"]));
    aof
}

fn head_deletes(image: &[u8], key: &str) -> bool {
    let del = resp(&[b"DEL", key.as_bytes()]);
    image.windows(del.len()).any(|w| w == del.as_slice())
}

/// The reported case, with file 7 below the new generation's `MOON.COLDCUT`
/// (it was allocated before the fold): the cut authorizes it wholesale.
#[test]
fn a_key_deleted_while_its_spill_is_in_flight_stays_deleted_under_the_cut() {
    let (_live_dir, mut live) = live();
    assert!(live.remove_counting_cold(b"k1").0, "DEL k1 removes it");
    assert_eq!(
        live.spill_superseded_len(),
        1,
        "request 7 of k1 is superseded"
    );
    let image = fold_then_completion(&live, 8);
    assert!(
        head_deletes(&image, "k1"),
        "the fold must DEL k1 at the head"
    );
    let (_dir, mut db) = recover(FILES, &image);
    assert_eq!(value(&mut db, b"k1"), None, "k1 came back from file 7");
    assert_eq!(value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
    assert_eq!(value(&mut db, b"k3").as_deref(), Some(&b"v3"[..]));
}

/// File 7 at or above the watermark: authorized by `MOON.SPILLED 7 k3` in
/// the new generation instead, which authorizes the whole file too.
#[test]
fn a_key_deleted_while_its_spill_is_in_flight_stays_deleted_under_the_marker() {
    let (_live_dir, mut live) = live();
    live.remove_counting_cold(b"k1");
    let (_dir, mut db) = recover(FILES, &fold_then_completion(&live, 7));
    assert_eq!(value(&mut db, b"k1"), None, "k1 came back from file 7");
    assert_eq!(value(&mut db, b"k3").as_deref(), Some(&b"v3"[..]));
}

/// FLUSH retires every in-flight record at once: every key it flushed
/// stays flushed, the in-flight ones included.
#[test]
fn keys_flushed_while_their_spill_is_in_flight_stay_flushed() {
    let (_live_dir, mut live) = live();
    live.clear();
    assert_eq!(live.spill_superseded_len(), 2, "k1 and k3 are superseded");
    let (_dir, mut db) = recover(FILES, &fold_then_completion(&live, 8));
    for key in [&b"k1"[..], b"k2", b"k3"] {
        assert_eq!(value(&mut db, key), None, "{key:?} came back");
    }
}

/// An overwrite supersedes the spill but leaves the key ALIVE at the fold:
/// no head `DEL`, and the base's new value shadows the old slot in the
/// wholesale-authorized file.
#[test]
fn a_key_overwritten_while_its_spill_is_in_flight_keeps_the_new_value() {
    let (_live_dir, mut live) = live();
    live.set(b"k1", Entry::new_string(Bytes::from_static(b"new")));
    assert_eq!(live.spill_superseded_len(), 1);
    let image = fold_then_completion(&live, 8);
    assert!(!head_deletes(&image, "k1"), "an alive key gets no DEL");
    let (_dir, mut db) = recover(FILES, &image);
    assert_eq!(value(&mut db, b"k1").as_deref(), Some(&b"new"[..]));
}

/// A read that promotes the in-flight payload back to RAM supersedes the
/// spill too; the key is alive, so the base carries it.
#[test]
fn a_key_promoted_while_its_spill_is_in_flight_keeps_its_value() {
    let (_live_dir, mut live) = live();
    let now = crate::storage::entry::current_time_ms();
    assert!(live.promote_inflight_if_present(b"k1", now));
    assert_eq!(live.spill_superseded_len(), 1);
    let image = fold_then_completion(&live, 8);
    assert!(!head_deletes(&image, "k1"));
    let (_dir, mut db) = recover(FILES, &image);
    assert_eq!(value(&mut db, b"k1").as_deref(), Some(&b"v1-old"[..]));
}

/// The completion lands BEFORE the fold: it settles the superseded entry and
/// notes the ghost slot in the dead-slot ledger (what
/// `apply_completion_vec` does), and the ledger then carries the `DEL`.
#[test]
fn a_completion_before_the_fold_hands_the_key_to_the_ledger() {
    let (_live_dir, mut live) = live();
    live.remove_counting_cold(b"k1");
    live.spill_superseded_settle(&Bytes::from_static(b"k1"), 7);
    assert!(live.spill_superseded_is_empty());
    if let Some(ci) = live.cold_index.as_mut() {
        ci.note_dead_slot(7, Bytes::from_static(b"k1"), None);
    }
    let image = fold_then_completion(&live, 8);
    assert!(head_deletes(&image, "k1"));
    let (_dir, mut db) = recover(FILES, &image);
    assert_eq!(value(&mut db, b"k1"), None);
}

/// The superseded set is counted APART from the moon#466 pending charge
/// (`pending_spill_bytes`, which `used_memory` and admission see): a FLUSH
/// still drops that charge to 0 as in redis. Its own count goes back to 0 as
/// its entries settle; a request that was never superseded settles as a
/// no-op.
#[test]
fn superseded_entries_are_counted_apart_and_settle_to_zero() {
    let mut db = Database::new();
    in_flight(&mut db, b"k1", b"v1-old", 7);
    in_flight(&mut db, b"k3", b"v3", 7);
    assert_eq!(db.pending_spill_bytes(), (2 + 6) + (2 + 2));
    db.remove_counting_cold(b"k1");
    // A re-eviction replaces k3's record: request 7 of k3 is superseded too.
    in_flight(&mut db, b"k3", b"v3-new", 9);
    assert_eq!(db.spill_superseded_len(), 2);
    assert_eq!(
        db.pending_spill_bytes(),
        2 + 6,
        "only k3's live record is charged"
    );
    let entry = 2 + super::SPILL_SUPERSEDED_OVERHEAD;
    assert_eq!(db.spill_superseded_bytes(), 2 * entry);
    db.spill_superseded_settle(&Bytes::from_static(b"k3"), 9);
    assert_eq!(
        db.spill_superseded_len(),
        2,
        "request 9 was never superseded"
    );
    db.spill_superseded_settle(&Bytes::from_static(b"k1"), 7);
    db.spill_superseded_settle(&Bytes::from_static(b"k3"), 7);
    assert!(db.spill_superseded_is_empty());
    assert_eq!(db.spill_superseded_bytes(), 0);
    db.spill_inflight_clear(b"k3", 9);
    assert_eq!(db.pending_spill_bytes(), 0, "everything credited back");

    // FLUSH: the records become superseded, the pending charge goes to 0.
    in_flight(&mut db, b"k1", b"v1-old", 11);
    db.clear();
    assert_eq!(db.spill_superseded_len(), 1);
    assert_eq!(db.pending_spill_bytes(), 0);
    assert_eq!(db.estimated_memory(), 0, "used_memory reads 0 after FLUSH");
    assert_eq!(db.spill_superseded_bytes(), entry);
}
