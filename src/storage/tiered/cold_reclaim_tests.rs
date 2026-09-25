//! The cold-tier reclaim (moon#1215 ledger bound, PR #1233 review): compact
//! a mostly-dead spill file into a new, unlisted file; adopt it — list it,
//! re-point its survivors, unlink the old file with its ledger entries —
//! only once a fold cut after the compaction has committed.
//!
//! Unit tests drive `ColdIndex` over real spill files and a real manifest;
//! the recovery-level tests build the post-rewrite image with the
//! PRODUCTION fold and recover it with production recovery, before and after
//! adoption (the crash windows that matter).

use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::cold_index::ColdIndex;
use super::cold_reclaim::awaiting_fold;
use crate::persistence::aof::fold_stream::{
    fold_image_channel, stream_fold_image, write_fold_image,
};
use crate::persistence::cold_records::{serialize_cold_cut, write_generation_head_to};
use crate::persistence::kv_page::{ValueType, read_datafile};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::Database;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

const OLD: u64 = 5;
const NEW: u64 = 20;

fn keys(n: usize) -> Vec<(String, String)> {
    (0..n)
        .map(|i| (format!("k{i:02}"), format!("v{i:02}")))
        .collect()
}

/// Write spill file `file_id` holding `kvs` and list it in the manifest.
fn spill(shard_dir: &Path, manifest: &mut ShardManifest, file_id: u64, kvs: &[(String, String)]) {
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
        .expect("unique ids");
    manifest.commit().expect("commit");
}

fn heap(shard_dir: &Path, file_id: u64) -> PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"))
}

fn listed(manifest: &ShardManifest, file_id: u64) -> bool {
    manifest
        .files()
        .iter()
        .any(|f| f.file_id == file_id && f.status == FileStatus::Active)
}

/// A shard dir with file OLD holding 10 keys, indexed; the first `dead` of
/// them deleted (their slots in the ledger).
fn fixture(dead: usize) -> (tempfile::TempDir, PathBuf, ShardManifest, ColdIndex) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("dir");
    let mut manifest =
        ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("manifest");
    spill(&shard_dir, &mut manifest, OLD, &keys(10));
    let mut ci = ColdIndex::rebuild_from_manifest(&shard_dir, &manifest);
    for (k, _) in keys(10).iter().take(dead) {
        assert!(ci.remove(k.as_bytes()));
    }
    (tmp, shard_dir, manifest, ci)
}

#[test]
fn a_file_with_more_dead_than_live_slots_is_a_candidate_and_one_without_is_not() {
    let (_t, _dir, _m, ci) = fixture(8);
    assert_eq!(ci.reclaim_candidates(4), vec![OLD]);
    let (_t, _dir, _m, ci) = fixture(4);
    assert!(ci.reclaim_candidates(4).is_empty(), "4 dead < 6 live");
    let (_t, _dir, _m, ci) = fixture(10);
    assert!(
        ci.reclaim_candidates(4).is_empty(),
        "no live key left: the orphan sweep's file, not reclaim's"
    );
}

/// Compaction writes exactly the survivors into a new file and changes
/// nothing anyone else reads: the index, the ledger and the manifest are as
/// they were until adoption.
#[test]
fn compaction_writes_only_the_survivors_and_changes_nothing_until_adoption() {
    let (_t, dir, manifest, mut ci) = fixture(8);
    let ledger = ci.dead_slots().len();
    let mut next = NEW;
    let read = ci
        .compact_file(OLD, 0, &dir, &mut next, 0)
        .expect("compact");
    assert!(read > 0);
    assert_eq!(ci.pending_compactions(), 1);
    assert!(awaiting_fold() >= 1);
    for k in ["k08", "k09"] {
        assert_eq!(ci.lookup(k.as_bytes()).map(|l| l.file_id), Some(OLD));
    }
    assert_eq!(ci.dead_slots().len(), ledger, "the ledger is untouched");
    assert!(!listed(&manifest, NEW), "not listed before a fold commits");
    assert!(heap(&dir, OLD).exists());
    let pages = read_datafile(&heap(&dir, NEW)).expect("the compacted file");
    let mut got: Vec<(Vec<u8>, Vec<u8>)> = pages
        .iter()
        .flat_map(|p| (0..p.slot_count()).filter_map(|s| p.get(s)))
        .map(|e| (e.key, e.value))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            (b"k08".to_vec(), b"v08".to_vec()),
            (b"k09".to_vec(), b"v09".to_vec())
        ]
    );
    assert!(
        ci.reclaim_candidates(4).is_empty(),
        "a file is compacted once"
    );
}

/// Adoption waits for a fold cut after the compaction to commit; then the
/// new file is listed, the survivors move, the old file and its ledger
/// entries go, and a rebuild from disk agrees.
#[test]
fn adoption_waits_for_a_committed_fold_past_the_compaction() {
    let (_t, dir, mut manifest, mut ci) = fixture(8);
    let mut next = NEW;
    let epoch = 3;
    ci.compact_file(OLD, 0, &dir, &mut next, epoch)
        .expect("compact");
    let before = ci.dead_slot_bytes();

    for floor in [0, epoch] {
        let r = ci.adopt_compactions(floor, &dir, &mut manifest);
        assert_eq!(
            r.compactions, 0,
            "a fold at or before the compaction covers nothing"
        );
        assert_eq!(ci.pending_compactions(), 1);
    }
    let r = ci.adopt_compactions(epoch + 1, &dir, &mut manifest);
    assert_eq!(
        (
            r.compactions,
            r.files_listed,
            r.keys_moved,
            r.files_unlinked
        ),
        (1, 1, 2, 1)
    );
    assert_eq!(ci.pending_compactions(), 0);
    for k in ["k08", "k09"] {
        assert_eq!(ci.lookup(k.as_bytes()).map(|l| l.file_id), Some(NEW));
    }
    assert!(!heap(&dir, OLD).exists(), "the emptied file is unlinked");
    assert!(!ci.dead_slots().file_has_dead_slots(OLD));
    assert!(
        ci.dead_slots().is_empty(),
        "its ledger entries went with it"
    );
    assert!(ci.dead_slot_bytes() < before);

    // What a restart sees: the reopened manifest lists the new file, not the
    // old one, and the survivors come back from it.
    let reopened = ShardManifest::open(&dir.join("shard-0.manifest")).expect("reopen");
    assert!(listed(&reopened, NEW) && !listed(&reopened, OLD));
    let rebuilt = ColdIndex::rebuild_from_manifest(&dir, &reopened);
    assert_eq!(rebuilt.len(), 2);
    for k in ["k08", "k09"] {
        assert_eq!(rebuilt.lookup(k.as_bytes()).map(|l| l.file_id), Some(NEW));
    }
}

/// A survivor deleted between compaction and adoption is not moved; its copy
/// in the now-listed file is a dead slot the ledger must know.
#[test]
fn a_survivor_that_changed_before_adoption_leaves_a_dead_slot_in_the_new_file() {
    let (_t, dir, mut manifest, mut ci) = fixture(8);
    let mut next = NEW;
    ci.compact_file(OLD, 0, &dir, &mut next, 0)
        .expect("compact");
    assert!(ci.remove(b"k08"));
    let r = ci.adopt_compactions(1, &dir, &mut manifest);
    assert_eq!((r.files_listed, r.keys_moved, r.files_unlinked), (1, 1, 1));
    assert_eq!(ci.lookup(b"k09").map(|l| l.file_id), Some(NEW));
    assert!(ci.lookup(b"k08").is_none());
    let dead: Vec<Vec<u8>> = ci.dead_slots().keys().map(|k| k.to_vec()).collect();
    assert_eq!(dead, vec![b"k08".to_vec()], "k08's copy in the new file");
    assert!(ci.dead_slots().file_has_dead_slots(NEW));
}

/// Every survivor gone before adoption: the copy is never listed and its
/// file is removed.
#[test]
fn a_compaction_whose_survivors_all_changed_is_discarded() {
    let (_t, dir, mut manifest, mut ci) = fixture(8);
    let mut next = NEW;
    ci.compact_file(OLD, 0, &dir, &mut next, 0)
        .expect("compact");
    ci.clear_all();
    let r = ci.adopt_compactions(1, &dir, &mut manifest);
    assert_eq!((r.compactions, r.files_listed, r.keys_moved), (1, 0, 0));
    assert!(!listed(&manifest, NEW));
    assert!(!heap(&dir, NEW).exists(), "an unadopted copy is removed");
}

/// A file with a live slot that does not decode is left as it is and never
/// tried again: compacting part of its survivors could not free it.
#[test]
fn a_file_whose_live_slots_do_not_all_decode_is_skipped_for_good() {
    let (_t, dir, _manifest, mut ci) = fixture(8);
    // Corrupt the only page: its checksum no longer matches.
    let mut raw = std::fs::read(heap(&dir, OLD)).expect("read");
    let last = raw.len() - 1;
    raw[last] ^= 0xFF;
    std::fs::write(heap(&dir, OLD), &raw).expect("write");
    let mut next = NEW;
    assert!(ci.compact_file(OLD, 0, &dir, &mut next, 0).is_err());
    assert_eq!(ci.pending_compactions(), 0);
    assert!(!heap(&dir, NEW).exists());
    assert!(ci.reclaim_candidates(4).is_empty(), "not retried");
    assert_eq!(ci.lookup(b"k08").map(|l| l.file_id), Some(OLD));
}

// ── Recovery level: production fold + production recovery ──────────────────

fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Recover shard 0 from `shard_dir` (its manifest + heap files) and the
/// legacy flat AOF `aof`, as a restart does.
fn recover(root: &Path, shard_dir: &Path, aof: &[u8]) -> Database {
    let legacy = root.join("legacy");
    std::fs::create_dir_all(&legacy).expect("aof dir");
    std::fs::write(legacy.join("appendonly.aof"), aof).expect("aof");
    let mut dbs = vec![Database::new()];
    crate::persistence::recovery::recover_shard_v3_with_fallback(
        &mut dbs,
        0,
        shard_dir,
        &crate::persistence::replay::DispatchReplayEngine::new(),
        Some(&legacy),
        false,
    )
    .expect("recovery");
    dbs.remove(0)
}

fn value(db: &mut Database, key: &str) -> Option<String> {
    db.promote_cold_if_present(key.as_bytes(), 0);
    db.get(key.as_bytes())
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// The production fold of `db` in the tokio `--shards 1` flat layout.
fn fold(db: &Database, watermark: u64) -> Vec<u8> {
    let now = crate::storage::entry::current_time_ms();
    let (sink, image) = fold_image_channel();
    stream_fold_image(&[db], now, sink);
    let mut aof = Vec::new();
    let (_, deletes) = write_fold_image(image, &mut aof, "reclaim test").expect("fold image");
    write_generation_head_to(&mut aof, watermark, deletes, false).expect("head");
    aof
}

/// The live server: 10 keys cold in OLD, recovered from their log; the first
/// 8 then DELeted (logged), and OLD compacted into NEW (not yet adopted).
fn live_compacted() -> (tempfile::TempDir, PathBuf, Database, Vec<u8>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("dir");
    let mut manifest =
        ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("manifest");
    spill(&shard_dir, &mut manifest, OLD, &keys(10));
    drop(manifest);
    let kvs = keys(10);
    let mut log = serialize_cold_cut(1).to_vec();
    for (k, v) in &kvs {
        log.extend_from_slice(&resp(&[b"SET", k.as_bytes(), v.as_bytes()]));
    }
    let mut marker: Vec<&[u8]> = vec![b"MOON.SPILLED", b"5"];
    marker.extend(kvs.iter().map(|(k, _)| k.as_bytes()));
    log.extend_from_slice(&resp(&marker));
    let mut live = recover(tmp.path(), &shard_dir, &log);
    for (k, _) in kvs.iter().take(8) {
        assert!(live.remove_counting_cold(k.as_bytes()).0);
        log.extend_from_slice(&resp(&[b"DEL", k.as_bytes()]));
    }
    let mut next = NEW;
    live.cold_index
        .as_mut()
        .expect("cold index")
        .compact_file(OLD, 0, &shard_dir, &mut next, 0)
        .expect("compact");
    (tmp, shard_dir, live, log)
}

fn assert_restart_is_exact(db: &mut Database, when: &str) {
    for (k, v) in keys(10) {
        let want = (k.as_str() >= "k08").then_some(v);
        assert_eq!(value(db, &k), want, "{when}: {k}");
    }
}

fn cold_file_of(db: &Database, key: &[u8]) -> Option<u64> {
    db.cold_index
        .as_ref()
        .and_then(|ci| ci.lookup(key))
        .map(|l| l.file_id)
}

/// Crash after compaction, before any fold: the compacted file is unlisted,
/// the old generation's log still deletes the dead keys, and the survivors
/// come from the old file.
#[test]
fn a_crash_before_the_fold_recovers_everything_from_the_old_file() {
    let (tmp, shard_dir, _live, log) = live_compacted();
    let mut db = recover(tmp.path(), &shard_dir, &log);
    assert_eq!(cold_file_of(&db, b"k09"), Some(OLD));
    assert_restart_is_exact(&mut db, "old generation");
}

/// Crash after the fold committed, before adoption: the new generation's
/// head DELs the dead keys; the old file still serves the survivors.
#[test]
fn a_crash_between_the_fold_and_adoption_keeps_every_key_exact() {
    let (tmp, shard_dir, live, _log) = live_compacted();
    let image = fold(&live, NEW + 5);
    let mut db = recover(tmp.path(), &shard_dir, &image);
    assert_eq!(cold_file_of(&db, b"k09"), Some(OLD));
    assert_restart_is_exact(&mut db, "fold committed, not adopted");
}

/// The whole cycle: compaction, a committed fold, adoption (the old file and
/// its dead slots gone), restart — every survivor intact, every deleted key
/// still deleted, and no ledger left.
#[test]
fn after_adoption_a_restart_keeps_survivors_and_deletes_with_no_ledger_left() {
    let (tmp, shard_dir, mut live, _log) = live_compacted();
    let image = fold(&live, NEW + 5);
    let mut manifest = ShardManifest::open(&shard_dir.join("shard-0.manifest")).expect("manifest");
    let ci = live.cold_index.as_mut().expect("cold index");
    let r = ci.adopt_compactions(1, &shard_dir, &mut manifest);
    assert_eq!((r.files_listed, r.keys_moved, r.files_unlinked), (1, 2, 1));
    assert!(
        ci.dead_slots().is_empty(),
        "the reclaim freed the whole ledger"
    );
    assert!(!heap(&shard_dir, OLD).exists());
    drop(manifest);
    let mut db = recover(tmp.path(), &shard_dir, &image);
    assert_eq!(
        cold_file_of(&db, b"k09"),
        Some(NEW),
        "served from the compacted file, which is below the new cut"
    );
    assert_restart_is_exact(&mut db, "after adoption");
}
