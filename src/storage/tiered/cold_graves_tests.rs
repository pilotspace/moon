//! moon#1281 unit proofs, runtime-independent (they run on the
//! per-PR tokio leg as well as monoio):
//!
//! - a no-AOF snapshot carries the dead spill slots as a trailer after `EOF`,
//!   every reader still loads its keys, and the loader hands the graves back;
//! - the boot rebuild drops exactly the grave slots BEFORE newest-wins, so a
//!   dead newer slot cannot shadow a live older one, a key with only grave
//!   slots is absent, a file left with no live slot is queued for unlink,
//!   and the dropped slots are re-recorded for the next snapshot;
//! - every removal records its slot, whatever the path.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::persistence::snapshot::cold_graves::{self, ColdGraves, pack_slot};
use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};
use crate::storage::tiered::slot_graves;

fn entry(k: &str, v: &str) -> SpillEntry {
    SpillEntry {
        key: Bytes::copy_from_slice(k.as_bytes()),
        value_bytes: Bytes::copy_from_slice(v.as_bytes()),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
    }
}

/// Write `files` as real spill batches (the spill thread's own writers) and
/// list them Active in db 0.
fn corpus(shard_dir: &Path, files: &[(u64, Vec<SpillEntry>)]) -> ShardManifest {
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    for (file_id, entries) in files {
        let batch = build_kv_spill_batch(entries, *file_id).unwrap();
        let byte_size = write_kv_spill_batch(shard_dir, *file_id, &batch).unwrap();
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
            .unwrap();
    }
    manifest.commit().unwrap();
    manifest
}

fn db0(rebuilt: crate::storage::tiered::cold_index::ColdRebuild) -> ColdIndex {
    rebuilt
        .per_db
        .into_iter()
        .find(|(d, _)| *d == 0)
        .map(|(_, ci)| ci)
        .unwrap()
}

fn grave_at(g: &mut ColdGraves, loc: ColdLocation) {
    g.insert(loc.file_id, loc.page_idx, loc.slot_idx);
}

/// A dead NEWER slot must not shadow the live older copy: graves are applied
/// before newest-wins. Without them the newer (dead) slot wins — the
/// resurrection of an overwritten value.
#[test]
fn a_grave_on_the_newer_slot_lets_the_live_older_copy_win() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let manifest = corpus(
        dir,
        &[
            (1, vec![entry("k", "old"), entry("n1", "x")]),
            (2, vec![entry("k", "new"), entry("n2", "y")]),
        ],
    );
    let plain = db0(ColdIndex::rebuild_from_manifest_per_db(dir, &manifest));
    let newer = plain.lookup(b"k").unwrap();
    assert_eq!(
        newer.file_id, 2,
        "control: without graves the newest copy wins"
    );

    let mut graves = ColdGraves::default();
    grave_at(&mut graves, newer);
    let rebuilt =
        ColdIndex::rebuild_from_manifest_per_db_with_graves(dir, &manifest, Some(&graves));
    assert_eq!(rebuilt.report.entries_tombstoned, 1);
    assert!(!rebuilt.report.is_degraded(), "a grave is not a loss");
    let ci = db0(rebuilt);
    assert_eq!(ci.lookup(b"k").unwrap().file_id, 1, "the live older copy");
    assert!(
        ci.lookup(b"n2").is_some(),
        "the grave file's other keys stay"
    );
    assert_eq!(
        ci.slot_graves().len(),
        1,
        "the dropped slot is still on disk: the next snapshot must carry it"
    );
    assert!(!ci.has_pending_unlink(), "file 2 still backs n2");
}

/// A key whose every slot is a grave is absent after the boot, and a file
/// left with no live slot is queued (the hold then keeps it until a later
/// snapshot commits).
#[test]
fn a_key_with_only_grave_slots_is_absent_and_an_emptied_file_is_queued() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let manifest = corpus(
        dir,
        &[
            (5, vec![entry("gone:a", "1"), entry("gone:b", "2")]),
            (6, vec![entry("live", "3"), entry("gone:c", "4")]),
        ],
    );
    let plain = db0(ColdIndex::rebuild_from_manifest_per_db(dir, &manifest));
    let mut graves = ColdGraves::default();
    for k in [&b"gone:a"[..], b"gone:b", b"gone:c"] {
        grave_at(&mut graves, plain.lookup(k).unwrap());
    }
    let ci = db0(ColdIndex::rebuild_from_manifest_per_db_with_graves(
        dir,
        &manifest,
        Some(&graves),
    ));
    for k in [&b"gone:a"[..], b"gone:b", b"gone:c"] {
        assert!(
            ci.lookup(k).is_none(),
            "{:?} resurrected",
            std::str::from_utf8(k)
        );
    }
    assert!(ci.lookup(b"live").is_some());
    assert!(ci.has_pending_unlink(), "file 5 has no live slot left");
    assert_eq!(ci.slot_graves().len(), 3);
}

/// No graves (an old snapshot, or an AOF process) is exactly the old rebuild.
#[test]
fn no_graves_is_the_unchanged_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let manifest = corpus(dir, &[(1, vec![entry("a", "1"), entry("b", "2")])]);
    let with_none = ColdIndex::rebuild_from_manifest_per_db_with_graves(dir, &manifest, None);
    assert_eq!(with_none.report.entries_tombstoned, 0);
    let ci = db0(with_none);
    assert_eq!(ci.len(), 2);
    assert!(ci.slot_graves().is_empty());
}

/// Every path that takes a slot out of the index records it (the same call
/// sites as the AOF ledger), and the snapshot's encoding carries it.
#[test]
fn every_removal_records_its_slot_for_the_next_snapshot() {
    slot_graves::force_enabled(Some(true));
    let loc = |file_id, slot_idx| ColdLocation {
        file_id,
        page_idx: 0,
        slot_idx,
        ttl_ms: None,
        value_type: ValueType::String,
    };
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"del"), loc(1, 0));
    ci.insert(Bytes::from_static(b"over"), loc(1, 1));
    ci.insert(Bytes::from_static(b"flushed"), loc(2, 0));
    assert!(ci.remove(b"del"), "DEL / UNLINK / promotion");
    ci.insert(Bytes::from_static(b"over"), loc(3, 0)); // re-spill supersedes (1,1)
    ci.note_dead_slot(Bytes::from_static(b"ghost"), loc(3, 5)); // superseded spill
    ci.clear_all(); // FLUSHDB: every remaining slot
    let mut files = Vec::new();
    ci.collect_graves_into(&mut files);
    let g = cold_graves::decode(&cold_graves::encode(&files)).unwrap();
    for (f, s) in [(1, 0), (1, 1), (2, 0), (3, 0), (3, 5)] {
        assert!(g.contains(f, 0, s), "slot ({f}, 0, {s}) not recorded");
    }
    assert_eq!(g.len(), 5);
    slot_graves::force_enabled(None);
}

/// The trailer rides after `EOF`: the keys load as before, the loader returns
/// the graves, and a snapshot without one yields `None`.
#[test]
fn a_snapshot_carries_its_graves_after_eof_and_still_loads_its_keys() {
    use crate::persistence::snapshot::{
        SnapshotState, shard_snapshot_load_noting_expired, shard_snapshot_load_with_graves,
    };
    use crate::storage::db::Database;
    let tmp = tempfile::tempdir().unwrap();
    let mut src = Database::new();
    src.set_string(b"hot", Bytes::from_static(b"v"));
    let dbs = vec![src];
    let trailer = cold_graves::encode(&[(9, vec![pack_slot(0, 1), pack_slot(2, 3)])]);

    let with = tmp.path().join("with.rrdshard");
    let mut state = SnapshotState::new(0, 1, &dbs, with.clone());
    state.set_cold_graves_trailer(trailer);
    while !state.advance_one_segment(&dbs) {}
    state.finalize().unwrap();

    let without = tmp.path().join("without.rrdshard");
    crate::persistence::snapshot::shard_snapshot_save(0, 1, &dbs, &without).unwrap();

    let mut out = vec![Database::new()];
    let mut expired = Vec::new();
    let mut graves = None;
    let n = shard_snapshot_load_with_graves(&mut out, &with, &mut expired, &mut graves).unwrap();
    assert_eq!(n, 1);
    let g = graves.expect("the trailer is returned");
    assert!(g.contains(9, 0, 1) && g.contains(9, 2, 3) && g.len() == 2);

    // The reader every older binary runs stops at EOF: keys load unchanged.
    let mut out = vec![Database::new()];
    assert_eq!(
        shard_snapshot_load_noting_expired(&mut out, &with, &mut expired).unwrap(),
        1
    );
    let mut graves = Some(ColdGraves::default());
    let mut out = vec![Database::new()];
    shard_snapshot_load_with_graves(&mut out, &without, &mut expired, &mut graves).unwrap();
    assert!(graves.is_none(), "no trailer, no graves");
    let a = std::fs::read(&with).unwrap().len();
    let b = std::fs::read(&without).unwrap().len();
    assert!(a > b, "the trailer is on disk");
}
