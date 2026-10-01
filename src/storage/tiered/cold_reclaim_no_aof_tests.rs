//! The no-AOF cold reclaim (moon#1297): the snapshot is the commit point, so
//! the trailer of every snapshot that starts while a compaction waits must
//! name the compacted slots of the survivors that changed, and the next
//! snapshot's must carry them and forget the old file's.
//!
//! The recovery checks rebuild the cold index from what is durable at each
//! kill point — the manifest as reopened from disk plus the trailer of the
//! last committed snapshot — with production recovery
//! (`ColdIndex::rebuild_from_manifest_per_db_with_graves`).

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::cold_index::ColdIndex;
use super::cold_reclaim::NO_AOF_MIN_DEAD_SLOTS;
use super::slot_graves;
use super::snapshot_hold;
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::persistence::snapshot::cold_graves::{self, ColdGraves, Encoder};
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

const OLD: u64 = 5;
const NEW: u64 = 5_000;
/// Keys in the old file; the first [`DEAD`] are deleted before compaction.
const KEYS: usize = 1_600;
const DEAD: usize = 1_200;

/// Forces this test thread into the no-AOF grave mode, and back on drop.
struct NoAof;

impl NoAof {
    fn on() -> Self {
        slot_graves::force_enabled(Some(true));
        NoAof
    }
}

impl Drop for NoAof {
    fn drop(&mut self) {
        slot_graves::force_enabled(None);
    }
}

fn key(i: usize) -> String {
    format!("k{i:05}")
}

fn spill(shard_dir: &Path, manifest: &mut ShardManifest, file_id: u64, n: usize) {
    let entries: Vec<SpillEntry> = (0..n)
        .map(|i| SpillEntry {
            key: Bytes::from(key(i)),
            value_bytes: Bytes::from(format!("v{i:05}")),
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

/// File OLD with [`KEYS`] keys, the first `dead` deleted (graves at OLD).
fn fixture(dead: usize) -> (tempfile::TempDir, PathBuf, ShardManifest, ColdIndex) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("dir");
    let mut manifest =
        ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("manifest");
    spill(&shard_dir, &mut manifest, OLD, KEYS);
    let mut ci = ColdIndex::rebuild_from_manifest(&shard_dir, &manifest);
    for i in 0..dead {
        assert!(ci.remove(key(i).as_bytes()));
    }
    (tmp, shard_dir, manifest, ci)
}

/// The trailer a snapshot starting now carries (what
/// `shard::timers::note_snapshot_started` encodes without an AOF).
fn trailer(ci: &ColdIndex) -> ColdGraves {
    let mut enc = Encoder::default();
    ci.encode_graves_into(&mut enc);
    ci.encode_compaction_graves_into(&mut enc);
    cold_graves::decode(&enc.finish()).expect("decode")
}

/// Every `(file, page, slot)` of a trailer.
fn slots(g: &ColdGraves) -> BTreeSet<(u64, u64)> {
    g.to_files()
        .into_iter()
        .flat_map(|(f, s)| s.into_iter().map(move |p| (f, p)))
        .collect()
}

/// The keys a boot finds live from the durable manifest and `graves`.
fn boot(shard_dir: &Path, graves: &ColdGraves) -> HashSet<String> {
    let manifest = ShardManifest::open(&shard_dir.join("shard-0.manifest")).expect("reopen");
    let mut live = HashSet::new();
    for (_db, ci) in
        ColdIndex::rebuild_from_manifest_per_db_with_graves(shard_dir, &manifest, Some(graves))
            .per_db
    {
        live.extend(
            ci.iter()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned()),
        );
    }
    live
}

fn expected(gone: &[usize]) -> HashSet<String> {
    (DEAD..KEYS)
        .filter(|i| !gone.contains(i))
        .map(key)
        .collect()
}

#[test]
fn a_file_is_a_no_aof_candidate_only_past_the_dead_slot_floor() {
    let _m = NoAof::on();
    // 500 dead of 1600: under a third.
    let (_t, _d, _m2, mut ci) = fixture(500);
    assert!(
        ci.reclaim_candidates_no_aof(4).is_empty(),
        "500 dead, 1100 live"
    );
    // 540 dead of 1600: a third (540 * 2 >= 1060).
    let (_t, _d, _m2, mut ci) = fixture(540);
    assert_eq!(ci.reclaim_candidates_no_aof(4), vec![OLD]);
    // 1200 dead: a candidate, and 1200 >= the floor.
    let (_t, _d, _m2, mut ci) = fixture(DEAD);
    const { assert!(DEAD >= NO_AOF_MIN_DEAD_SLOTS) };
    assert_eq!(ci.reclaim_candidates_no_aof(4), vec![OLD]);
    assert!(ci.start_compaction(OLD));
    assert!(ci.reclaim_candidates_no_aof(4).is_empty(), "busy");
}

#[test]
fn a_small_mostly_dead_file_waits_for_the_floor_and_the_scan_is_memoized() {
    let _m = NoAof::on();
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&dir).expect("dir");
    let mut manifest = ShardManifest::create(&dir.join("shard-0.manifest")).expect("manifest");
    spill(&dir, &mut manifest, OLD, 100);
    let mut ci = ColdIndex::rebuild_from_manifest(&dir, &manifest);
    for i in 0..90 {
        assert!(ci.remove(key(i).as_bytes()));
    }
    assert!(
        ci.reclaim_candidates_no_aof(4).is_empty(),
        "90 dead slots are under the floor: not worth a snapshot"
    );
    assert_eq!(ci.reclaim.no_aof_idle_at_for_test(), Some(90));
    assert!(ci.reclaim_candidates_no_aof(4).is_empty());
    assert!(ci.remove(key(95).as_bytes()));
    assert_eq!(
        ci.reclaim.no_aof_idle_at_for_test(),
        Some(90),
        "the memo is only compared, the next scan refreshes it"
    );
    assert!(ci.reclaim_candidates_no_aof(4).is_empty());
    assert_eq!(ci.reclaim.no_aof_idle_at_for_test(), Some(91));
}

/// The acceptance's exactness: the adopting snapshot's trailer is the old
/// file's dead slots plus the compacted slots of the survivors that changed
/// before it started — nothing else; after the adoption the next trailer
/// has the compacted file's graves only (the old file's are forgotten with
/// it), including a survivor that changed after the adopting snapshot began.
#[test]
fn the_adopting_snapshots_trailer_is_exact_and_the_next_one_forgets_the_old_file() {
    let _m = NoAof::on();
    let (_t, dir, mut manifest, mut ci) = fixture(DEAD);
    let old_dead: BTreeSet<(u64, u64)> = slots(&trailer(&ci));
    assert_eq!(old_dead.len(), DEAD);
    assert!(old_dead.iter().all(|(f, _)| *f == OLD));

    let mut next = NEW;
    let stamp = 7;
    ci.compact_file(OLD, 0, &dir, &mut next, stamp)
        .expect("compact");
    assert!(heap(&dir, NEW).exists());
    assert!(
        ci.collect_compaction_graves().is_empty(),
        "no survivor changed"
    );

    // Survivors change between the compaction and the adopting snapshot.
    let changed_before = [DEAD, DEAD + 1, DEAD + 50];
    for &i in &changed_before {
        assert!(ci.remove(key(i).as_bytes()));
    }
    let adopting = trailer(&ci);
    let compacted = ci.collect_compaction_graves();
    assert_eq!(compacted.len(), 1);
    assert_eq!(compacted[0].0, NEW);
    assert_eq!(compacted[0].1.len(), changed_before.len());
    let mut want: BTreeSet<(u64, u64)> = old_dead.clone();
    want.extend(
        ci.slot_graves()
            .slots_of(OLD)
            .iter()
            .map(|&p| (OLD, p))
            .collect::<Vec<_>>(),
    );
    want.extend(compacted[0].1.iter().map(|&p| (NEW, p)));
    assert_eq!(
        slots(&adopting),
        want,
        "the adopting trailer: OLD's dead slots and exactly the changed survivors' NEW slots"
    );
    assert_eq!(
        slots(&adopting).len(),
        DEAD + 2 * changed_before.len(),
        "each changed survivor is a grave in both files, nothing more"
    );

    // A survivor changes while that snapshot runs (after its start).
    assert!(ci.remove(key(DEAD + 2).as_bytes()));

    // The snapshot commits (floor past the stamp): adopt.
    assert_eq!(ci.compactions_ready(stamp), 0);
    assert_eq!(ci.compactions_ready(stamp + 1), 1);
    let r = ci.adopt_compactions(stamp + 1, &dir, &mut manifest);
    assert_eq!((r.files_listed, r.files_unlinked), (1, 1));
    assert!(!heap(&dir, OLD).exists());
    assert!(ci.slot_graves().slots_of(OLD).is_empty(), "OLD forgotten");

    let after = slots(&trailer(&ci));
    assert!(after.iter().all(|(f, _)| *f == NEW), "no grave of OLD");
    assert_eq!(
        after.len(),
        changed_before.len() + 1,
        "NEW carries every survivor that changed since the compaction, once each"
    );
    // A survivor removed after the adoption is graved in NEW as usual.
    assert!(ci.remove(key(DEAD + 3).as_bytes()));
    assert_eq!(slots(&trailer(&ci)).len(), changed_before.len() + 2);
}

/// Every kill point, rebuilt from what is durable there: never a key the
/// last committed snapshot saw deleted, never a loss of one it saw live.
#[test]
fn every_kill_point_recovers_exactly_the_last_committed_snapshot() {
    let _m = NoAof::on();
    let (_t, dir, mut manifest, mut ci) = fixture(DEAD);
    let s1 = trailer(&ci); // committed before the compaction
    let mut next = NEW;
    let stamp = 1;
    ci.compact_file(OLD, 0, &dir, &mut next, stamp)
        .expect("compact");
    // Kill point: compacted (NEW unlisted). The authority is s1.
    assert_eq!(boot(&dir, &s1), expected(&[]));

    let before = [DEAD, DEAD + 7];
    for &i in &before {
        assert!(ci.remove(key(i).as_bytes()));
    }
    // The adopting snapshot s2 starts; a kill before it commits recovers s1
    // (NEW still unlisted): DEAD and DEAD+7 come back — s1's point in time.
    let s2 = trailer(&ci);
    assert_eq!(boot(&dir, &s1), expected(&[]));

    // s2 committed, NEW listed durably, nothing re-pointed (the `listed`
    // point): both files listed. Neither the deleted fillers nor the
    // survivors deleted before s2 started come back.
    let r = ci.begin_adoption(stamp + 1, &dir, &mut manifest);
    assert_eq!(r.files_listed, 1);
    assert!(heap(&dir, OLD).exists());
    assert_eq!(boot(&dir, &s2), expected(&before));

    // The rule is load-bearing: the same image with a trailer built WITHOUT
    // the compacted slots (graves only) resurrects the changed survivors.
    let mut enc = Encoder::default();
    ci.encode_graves_into(&mut enc);
    let without = cold_graves::decode(&enc.finish()).expect("decode");
    let resurrected: Vec<String> = boot(&dir, &without)
        .difference(&expected(&before))
        .cloned()
        .collect();
    assert_eq!(
        resurrected.len(),
        before.len(),
        "without the compacted graves the survivors deleted before the snapshot resurrect"
    );

    // Re-pointed and OLD unlinked (the `unlinked` point).
    let r = ci.adopt_compactions(stamp + 1, &dir, &mut manifest);
    assert_eq!(r.files_unlinked, 1);
    assert!(!heap(&dir, OLD).exists());
    assert_eq!(boot(&dir, &s2), expected(&before));

    // The next snapshot commits and is the authority.
    assert!(ci.remove(key(DEAD + 9).as_bytes()));
    let s3 = trailer(&ci);
    assert_eq!(boot(&dir, &s3), expected(&[DEAD, DEAD + 7, DEAD + 9]));
}

/// A survivor that changed after the adopting snapshot started is still
/// cold in that snapshot's view; when its output is discarded (every
/// survivor in it changed) the old file must stay listed and on disk.
#[test]
fn an_output_discarded_at_listing_keeps_the_old_file() {
    let _m = NoAof::on();
    let (_t, dir, mut manifest, mut ci) = fixture(DEAD);
    let mut next = NEW;
    ci.compact_file(OLD, 0, &dir, &mut next, 0)
        .expect("compact");
    let s = trailer(&ci);
    for i in DEAD..KEYS {
        assert!(ci.remove(key(i).as_bytes()));
    }
    let r = ci.adopt_compactions(1, &dir, &mut manifest);
    assert_eq!((r.files_listed, r.files_unlinked), (0, 0));
    assert!(
        heap(&dir, OLD).exists(),
        "held for the snapshot hold instead"
    );
    assert!(!heap(&dir, NEW).exists());
    assert_eq!(boot(&dir, &s), expected(&[]), "s saw every survivor live");
}

/// moon#1289 R2 × moon#1297: an automatic round a shard abandons because
/// it held an uncommitted `TXN` write publishes nothing, so it must commit no
/// compaction. A shard that had started its part ends it with
/// `note_snapshot_finished(false)` (`shard::snapshot_txn_guard`'s abandon);
/// one that abandons at its start runs no hook at all. Only the published
/// retry moves the floor the compaction waits for — including a compaction
/// recorded while the abandoned part ran. The counters are this test
/// thread's own (`snapshot_hold` is per shard thread).
#[test]
fn an_abandoned_snapshot_round_commits_no_compaction() {
    let _m = NoAof::on();
    let floor = || snapshot_hold::snapshot_fold_view(0).committed_floor;
    let (_t, dir, mut manifest, mut ci) = fixture(DEAD);
    let mut next = NEW;
    ci.compact_file(OLD, 0, &dir, &mut next, snapshot_hold::epoch_before_start())
        .expect("compact");

    // The round: this shard starts its part, another shard abandons it.
    snapshot_hold::note_snapshot_started();
    snapshot_hold::note_snapshot_finished(false);
    assert_eq!(ci.compactions_ready(floor()), 0, "abandoned: nothing ready");
    assert!(ci.awaits_reclaim_snapshot(floor()), "still waiting");
    let r = ci.adopt_compactions(floor(), &dir, &mut manifest);
    assert_eq!((r.compactions, r.files_listed, r.files_unlinked), (0, 0, 0));
    assert!(heap(&dir, OLD).exists(), "the old file stays");
    assert_eq!(ci.pending_compactions(), 1);

    // The retried round publishes: the compaction is committed and adopted.
    snapshot_hold::note_snapshot_started();
    snapshot_hold::note_snapshot_finished(true);
    assert_eq!(ci.compactions_ready(floor()), 1);
    let r = ci.adopt_compactions(floor(), &dir, &mut manifest);
    assert_eq!((r.files_listed, r.files_unlinked), (1, 1));
    assert!(!heap(&dir, OLD).exists());
}

/// R3 fix-e × moon#1297: `ColdIndex::remove` returns at once when the index
/// holds no entry and no older copy. A pending compaction keeps no per-key
/// state that a remove must update — "changed" is judged when a trailer is
/// encoded (`lookup` against the slot read) — so once every survivor has
/// left, a remove is still a no-op and the trailer still graves each
/// survivor's compacted slot.
#[test]
fn removes_on_an_emptied_index_leave_a_pending_compactions_graves_exact() {
    let _m = NoAof::on();
    let (_t, dir, _m2, mut ci) = fixture(DEAD);
    let mut next = NEW;
    ci.compact_file(OLD, 0, &dir, &mut next, 0)
        .expect("compact");
    for i in DEAD..KEYS {
        assert!(ci.remove(key(i).as_bytes()));
    }
    assert_eq!(ci.len(), 0, "every survivor has left");
    let before = slots(&trailer(&ci));
    for i in [0, DEAD, KEYS - 1] {
        assert!(!ci.remove(key(i).as_bytes()), "nothing to remove");
    }
    assert_eq!(slots(&trailer(&ci)), before, "the remove changed nothing");
    let compacted = ci.collect_compaction_graves();
    assert_eq!(compacted.len(), 1);
    assert_eq!(
        compacted[0].1.len(),
        KEYS - DEAD,
        "every survivor's compacted slot is a grave"
    );
}

/// A compaction abandoned without a grave changing (its job could not be
/// sent, or its spill thread died) makes its file a candidate again: the
/// memo of an earlier empty scan must not hide it.
#[test]
fn an_abandoned_compaction_is_found_again_without_a_grave_changing() {
    let _m = NoAof::on();
    let (_t, _d, _m2, mut ci) = fixture(DEAD);
    assert_eq!(ci.reclaim_candidates_no_aof(4), vec![OLD]);
    assert!(ci.start_compaction(OLD));
    assert!(ci.reclaim_candidates_no_aof(4).is_empty(), "busy: memoized");
    ci.abandon_compaction(OLD, false);
    assert_eq!(ci.reclaim_candidates_no_aof(4), vec![OLD]);
    assert!(ci.start_compaction(OLD));
    assert!(ci.reclaim_candidates_no_aof(4).is_empty());
    ci.abandon_compactions_in_flight();
    assert_eq!(ci.reclaim_candidates_no_aof(4), vec![OLD]);
}
