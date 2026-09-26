//! moon#1237: SWAPDB and the cold tier.
//!
//! The replay cases run the PRODUCTION recovery (`recover_shard_v3_with_
//! fallback` over a legacy `appendonly.aof`, the tokio `--shards 1` layout)
//! against real spill files whose manifest entries carry the db they were
//! spilled from, exactly as the live server writes them. Recovery rebuilds
//! every file into its tagged db BEFORE the log replays; a replayed `SWAPDB`
//! used to move ALL of it, so a key spilled after the swap reappeared in the
//! other database.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::cold_records::serialize_cold_cut;
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::Database;
use crate::storage::db::PendingSpill;
use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

/// `(file_id, db the file was spilled from, [(key, value)])`.
type SpillFile<'a> = (u64, usize, &'a [(&'a str, &'a str)]);

fn spill_files(shard_dir: &Path, files: &[SpillFile<'_>]) {
    let mut manifest =
        ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("create manifest");
    for (file_id, db_index, kvs) in files {
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
                db_index: *db_index as u64,
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

/// Recover two databases from `files` and the log `aof`.
fn recover(files: &[SpillFile<'_>], aof: &[u8]) -> (tempfile::TempDir, Vec<Database>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("shard dir");
    spill_files(&shard_dir, files);
    let legacy = tmp.path().join("legacy");
    std::fs::create_dir_all(&legacy).expect("aof dir");
    std::fs::write(legacy.join("appendonly.aof"), aof).expect("write aof");
    let mut dbs = vec![Database::new(), Database::new()];
    crate::persistence::recovery::recover_shard_v3_with_fallback(
        &mut dbs,
        0,
        &shard_dir,
        &crate::persistence::replay::DispatchReplayEngine::new(),
        Some(&legacy),
        false,
    )
    .expect("recovery");
    (tmp, dbs)
}

fn value(db: &mut Database, key: &str) -> Option<String> {
    db.promote_cold_if_present(key.as_bytes(), 0);
    db.get(key.as_bytes())
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// Where `key` reads after recovery: `(db 0, db 1)`.
fn placement(dbs: &mut [Database], key: &str) -> (Option<String>, Option<String>) {
    let (d0, d1) = dbs.split_at_mut(1);
    (value(&mut d0[0], key), value(&mut d1[0], key))
}

fn log(cut: Option<u64>, records: &[&[&[u8]]]) -> Vec<u8> {
    let mut aof = cut
        .map(|w| serialize_cold_cut(w).to_vec())
        .unwrap_or_default();
    for r in records {
        aof.extend_from_slice(&resp(r));
    }
    aof
}

const SWAP: &[&[u8]] = &[b"SWAPDB", b"0", b"1"];

/// A key spilled in db 1 AFTER `SWAPDB 0 1` (its file is tagged 1, its
/// marker follows the swap) stays in db 1 — gated generation.
#[test]
fn a_key_spilled_after_a_replayed_swapdb_stays_in_its_db() {
    let files: &[SpillFile<'_>] = &[(7, 1, &[("k2", "v2")])];
    let aof = log(
        Some(1),
        &[
            SWAP,
            &[b"SELECT", b"1"],
            &[b"SET", b"k2", b"v2"],
            &[b"MOON.SPILLED", b"7", b"k2"],
        ],
    );
    let (_dir, mut dbs) = recover(files, &aof);
    assert_eq!(
        placement(&mut dbs, "k2"),
        (None, Some("v2".to_owned())),
        "(db 0, db 1) after the restart: the replayed SWAPDB carried a later spill across"
    );
}

/// The same with no `MOON.COLDCUT` (the tokio `--shards 1` file before its
/// first rewrite): the marker, not the cut, says when the file appeared.
#[test]
fn a_key_spilled_after_a_replayed_swapdb_stays_in_its_db_without_a_cut() {
    let files: &[SpillFile<'_>] = &[(7, 1, &[("k2", "v2")])];
    let aof = log(
        None,
        &[
            SWAP,
            &[b"SELECT", b"1"],
            &[b"SET", b"k2", b"v2"],
            &[b"MOON.SPILLED", b"7", b"k2"],
        ],
    );
    let (_dir, mut dbs) = recover(files, &aof);
    assert_eq!(placement(&mut dbs, "k2"), (None, Some("v2".to_owned())));
}

/// A key spilled BEFORE the swap (its marker precedes it) moves with it —
/// the behaviour an older log relies on, gated and ungated.
#[test]
fn a_key_spilled_before_a_replayed_swapdb_moves_with_it() {
    let files: &[SpillFile<'_>] = &[(5, 0, &[("k1", "v1")])];
    for cut in [Some(1), None] {
        let aof = log(
            cut,
            &[
                &[b"SET", b"k1", b"v1"],
                &[b"MOON.SPILLED", b"5", b"k1"],
                SWAP,
            ],
        );
        let (_dir, mut dbs) = recover(files, &aof);
        assert!(
            !dbs[1].is_hot(b"k1")
                && dbs[1]
                    .cold_index
                    .as_ref()
                    .is_some_and(|ci| ci.lookup(b"k1").is_some()),
            "cut {cut:?}: k1 is cold in db 1 (restart-as-cold kept by the move)"
        );
        assert_eq!(
            placement(&mut dbs, "k1"),
            (None, Some("v1".to_owned())),
            "cut {cut:?}"
        );
    }
}

/// A file below the generation's cut predates every record: it moves.
#[test]
fn a_file_below_the_cut_moves_with_a_replayed_swapdb() {
    let files: &[SpillFile<'_>] = &[(5, 0, &[("k1", "v1")])];
    let (_dir, mut dbs) = recover(files, &log(Some(6), &[SWAP]));
    assert_eq!(placement(&mut dbs, "k1"), (None, Some("v1".to_owned())));
}

/// Both at once, in one db each: the earlier spill moves, the later stays,
/// and neither shows up in the other database.
#[test]
fn a_replayed_swapdb_moves_the_earlier_spill_and_leaves_the_later_one() {
    let files: &[SpillFile<'_>] = &[(5, 0, &[("k1", "v1")]), (7, 1, &[("k2", "v2")])];
    for cut in [Some(1), None] {
        let aof = log(
            cut,
            &[
                &[b"SET", b"k1", b"v1"],
                &[b"MOON.SPILLED", b"5", b"k1"],
                SWAP,
                &[b"SELECT", b"1"],
                &[b"SET", b"k2", b"v2"],
                &[b"MOON.SPILLED", b"7", b"k2"],
            ],
        );
        let (_dir, mut dbs) = recover(files, &aof);
        assert_eq!(
            placement(&mut dbs, "k1"),
            (None, Some("v1".to_owned())),
            "{cut:?}"
        );
        assert_eq!(
            placement(&mut dbs, "k2"),
            (None, Some("v2".to_owned())),
            "{cut:?}"
        );
    }
}

/// One key with a copy on each side of the swap (an older log: `k` cold in
/// db 0, swapped, promoted, rewritten and spilled again in db 1): after the
/// replayed swap db 1 holds both copies, the newer as the entry.
#[test]
fn copies_of_one_key_on_both_sides_of_a_replayed_swap_keep_the_newest() {
    let files: &[SpillFile<'_>] = &[(5, 0, &[("k", "old")]), (7, 1, &[("k", "new")])];
    let aof = log(
        Some(1),
        &[
            &[b"SET", b"k", b"old"],
            &[b"MOON.SPILLED", b"5", b"k"],
            SWAP,
            &[b"SELECT", b"1"],
            &[b"SET", b"k", b"new"],
            &[b"MOON.SPILLED", b"7", b"k"],
        ],
    );
    let (_dir, mut dbs) = recover(files, &aof);
    assert_eq!(placement(&mut dbs, "k"), (None, Some("new".to_owned())));
}

// ── the live footprint predicate ────────────────────────────────────────────

fn loc(file_id: u64) -> ColdLocation {
    ColdLocation {
        file_id,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: ValueType::String,
    }
}

#[test]
fn a_database_without_cold_state_has_no_footprint() {
    let mut db = Database::new();
    assert!(!db.has_cold_footprint());
    db.cold_index = Some(ColdIndex::new());
    assert!(
        !db.has_cold_footprint(),
        "an empty cold index ties nothing to disk"
    );
}

#[test]
fn a_cold_entry_an_in_flight_spill_or_a_superseded_one_is_a_footprint() {
    let mut db = Database::new();
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"k"), loc(5));
    db.cold_index = Some(ci);
    assert!(db.has_cold_footprint(), "a live cold entry");

    let mut db = Database::new();
    db.spill_inflight_mark(
        Bytes::from_static(b"k"),
        PendingSpill {
            req_id: 9,
            value_type: ValueType::String,
            value_bytes: Bytes::from_static(b"v"),
            ttl_ms: None,
        },
    );
    assert!(db.has_cold_footprint(), "an in-flight spill");
    db.spill_inflight_forget(b"k");
    assert!(
        db.has_cold_footprint(),
        "a spill retired in flight: its completion still publishes a slot"
    );
}

/// A cold key that was deleted still has its slot on disk (its file backs a
/// neighbour, or waits for the sweep): a restart rebuilds it under the db's
/// index, so it is a footprint until the file is gone.
#[test]
fn a_deleted_cold_key_whose_file_is_still_on_disk_is_a_footprint() {
    let _ledger = crate::storage::tiered::dead_slots::force_ledger(true);
    let mut db = Database::new();
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"k"), loc(5));
    db.cold_index = Some(ci);
    assert!(db.remove_counting_cold(b"k").0);
    assert!(db.cold_index.as_ref().is_some_and(|ci| ci.len() == 0));
    assert!(
        db.has_cold_footprint(),
        "file 5 is queued for unlink, its slot recorded"
    );
}

// ── split / merge accounting ────────────────────────────────────────────────

#[test]
fn a_split_moves_entries_and_references_without_queueing_unlinks() {
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"a"), loc(5));
    ci.insert(Bytes::from_static(b"b"), loc(7));
    let before = ci.resident_bytes();
    let moved = ci.split_off_files(|f| f == 5);
    assert_eq!((ci.len(), moved.len()), (1, 1));
    assert!(ci.lookup(b"a").is_none() && moved.lookup(b"a").is_some());
    assert_eq!(
        (ci.referenced_file_count(), moved.referenced_file_count()),
        (1, 1)
    );
    assert!(!ci.has_pending_unlink(), "a move is not a zero-ref event");
    assert_eq!(ci.resident_bytes() + moved.resident_bytes(), before);
    let mut back = ColdIndex::new();
    back.merge_newer(moved);
    back.merge_newer(ci);
    assert_eq!((back.len(), back.referenced_file_count()), (2, 2));
    assert_eq!(back.resident_bytes(), before);
}

// ── the live refusal ────────────────────────────────────────────────────────

/// On the connection's own shard: clean databases swap; a cold entry in
/// either one refuses the SWAPDB before anything is logged or swapped.
#[test]
fn swapdb_is_refused_while_either_database_has_cold_data() {
    use crate::shard::slice::{ShardSlice, init_shard, with_shard_db};
    let (_shared, mut inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
        Database::new(),
        Database::new(),
    ]]);
    init_shard(ShardSlice::new(inits.remove(0)));
    assert!(
        super::swapdb_cold_refusal(0, 1, 0, 1).is_none(),
        "clean databases swap"
    );
    with_shard_db(1, |db| {
        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"k"), loc(5));
        db.cold_index = Some(ci);
    });
    let refused = super::swapdb_cold_refusal(0, 1, 0, 1);
    assert!(
        matches!(&refused, Some(crate::protocol::Frame::Error(e)) if &e[..] == super::ERR_SWAPDB_COLD),
        "SWAPDB 0 1 with a cold key in db 1: {refused:?}"
    );
    assert!(
        super::swapdb_cold_refusal(1, 0, 0, 1).is_some(),
        "either order"
    );
}
