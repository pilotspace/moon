//! moon#1140: a gated AOF replay must not lose a key's pre-rewrite state
//! because a NEWER spill file of the key is listed in the manifest while its
//! `MOON.SPILLED` marker never reached the AOF.
//!
//! The crash image these tests build is the one a hosted-macOS CI run left
//! behind (and a 200 ms SIGKILL reproduces on any host): `BGREWRITEAOF` while
//! `k` is cold in file 5, then a post-rewrite write promotes `k`, eviction
//! spills it again into file 9, the manifest names file 9, and the process
//! dies before the writer flushed file 9's marker. The rewritten generation
//! opens with `MOON.COLDCUT 9`, so file 9 is hidden until its marker, and
//! the rebuilt index points `k` at file 9 alone: the replayed write used to
//! land on an EMPTY key, and the end-of-replay resolution kept that
//! truncated hot copy (`"!"` instead of `"hello!"`).
//!
//! Every file here is a real spill batch written by the spill thread's own
//! writers; recovery runs the production `recover_shard_v3_with_fallback`
//! over a legacy `appendonly.aof`, the layout tokio `--shards 1` recovers.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::Database;
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

fn string_entry(key: &str, value: &str) -> SpillEntry {
    SpillEntry {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value_bytes: Bytes::copy_from_slice(value.as_bytes()),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
    }
}

/// Write each `(file_id, [(key, value)])` as a real spill batch and list it
/// Active in the shard manifest recovery opens.
fn spill_files(shard_dir: &Path, manifest_path: &Path, files: &[(u64, &[(&str, &str)])]) {
    let mut manifest = ShardManifest::create(manifest_path).expect("create manifest");
    for (file_id, kvs) in files {
        let entries: Vec<SpillEntry> = kvs.iter().map(|(k, v)| string_entry(k, v)).collect();
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

/// Recover a shard whose cold tier holds `files` and whose AOF is `aof`,
/// and return the recovered database with the directory its cold files live
/// in (dropping the directory deletes them).
fn recover(files: &[(u64, &[(&str, &str)])], aof: &[u8]) -> (tempfile::TempDir, Database) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("shard dir");
    spill_files(&shard_dir, &shard_dir.join("shard-0.manifest"), files);
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
    assert!(
        !dbs[0].replay_cold_gate_active(),
        "the replay gate outlived recovery"
    );
    (tmp, dbs.remove(0))
}

/// The value of string `key` after recovery, from whichever plane holds it.
fn string_value(db: &mut Database, key: &[u8]) -> Option<Vec<u8>> {
    db.promote_cold_if_present(key, 0);
    db.get(key)
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| b.to_vec())
}

/// The generation the rewrite opened: `MOON.COLDCUT 9`, then the
/// post-rewrite `APPEND k !`. File 5 holds `k` from before the rewrite;
/// file 9 holds the respilled `hello!`.
fn rewritten_generation() -> Vec<u8> {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(9).to_vec();
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"!"]));
    aof
}

const PRE_AND_POST_CUT: &[(u64, &[(&str, &str)])] = &[
    (5, &[("k", "hello"), ("other", "o")]),
    (9, &[("k", "hello!")]),
];

/// The #1140 crash image: file 9 is in the manifest, its marker is not in
/// the AOF. Unfixed, `k` recovered as `"!"`.
#[test]
fn a_lost_spill_marker_does_not_hide_the_copy_below_the_cut() {
    let (_dir, mut db) = recover(PRE_AND_POST_CUT, &rewritten_generation());
    assert_eq!(
        string_value(&mut db, b"k").as_deref(),
        Some(&b"hello!"[..]),
        "the post-rewrite APPEND must replay onto the pre-rewrite copy in file 5"
    );
    assert_eq!(
        string_value(&mut db, b"other").as_deref(),
        Some(&b"o"[..]),
        "a key only below the cut stays readable"
    );
}

/// Control: the same image with file 9's marker present recovers the same
/// value (the marker cuts `k` over to file 9).
#[test]
fn the_marker_present_recovers_the_same_value() {
    let mut aof = rewritten_generation();
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"9", b"k"]));
    let (_dir, mut db) = recover(PRE_AND_POST_CUT, &aof);
    assert_eq!(string_value(&mut db, b"k").as_deref(), Some(&b"hello!"[..]));
}

/// A key deleted after the rewrite must not come back from the older copy:
/// the replayed DEL drops the index entry, and with it every older copy.
#[test]
fn a_replayed_delete_retires_the_older_copies_too() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(9).to_vec();
    aof.extend_from_slice(&resp(&[b"DEL", b"k"]));
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"x"]));
    let files: &[(u64, &[(&str, &str)])] = &[(5, &[("k", "hello")]), (9, &[("k", "x")])];
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(
        string_value(&mut db, b"k").as_deref(),
        Some(&b"x"[..]),
        "the pre-rewrite copy of a deleted key must stay deleted"
    );
}

/// Every copy at or past the cut stays hidden without its marker: a key
/// created after the rewrite replays from nothing, as before.
#[test]
fn copies_past_the_cut_stay_hidden_without_their_markers() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(9).to_vec();
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"a"]));
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"b"]));
    let files: &[(u64, &[(&str, &str)])] = &[(9, &[("k", "a")]), (11, &[("k", "ab")])];
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(
        string_value(&mut db, b"k").as_deref(),
        Some(&b"ab"[..]),
        "a key born after the cut must not double-apply from its own spills"
    );
}

/// The rebuild keeps every superseded copy, newest first, and the
/// generation's close releases them.
#[test]
fn the_rebuild_keeps_superseded_copies_until_replay_closes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest_path = tmp.path().join("shard.manifest");
    let files: &[(u64, &[(&str, &str)])] = &[
        (3, &[("k", "v3")]),
        (7, &[("k", "v7"), ("solo", "s")]),
        (12, &[("k", "v12")]),
    ];
    spill_files(tmp.path(), &manifest_path, files);
    let manifest = ShardManifest::open(&manifest_path).expect("open manifest");
    let mut per_db = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &manifest).per_db;
    let (_, index) = per_db.remove(0);

    assert_eq!(index.lookup(b"k").map(|l| l.file_id), Some(12));
    let older: Vec<u64> = index.older_copies(b"k").iter().map(|l| l.file_id).collect();
    assert_eq!(older, vec![7, 3], "older copies, newest first");
    assert!(index.older_copies(b"solo").is_empty());

    let mut db = Database::new();
    db.cold_index = Some(index);
    db.install_replay_cold_gate(10);
    let _ = db.finish_replay_cold_reconcile();
    let ci = db.cold_index.as_ref().expect("index");
    assert!(
        ci.older_copies(b"k").is_empty(),
        "closing the generation releases the rebuild's older copies"
    );
    assert_eq!(ci.lookup(b"k").map(|l| l.file_id), Some(12));
}

/// A file whose only copy is a superseded one must not be unlinked while a
/// gated replay can still read it. It joins the unlink queue when the
/// generation closes and the copy gives its reference back.
#[test]
fn a_file_holding_only_a_superseded_copy_is_not_unlinked_during_replay() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest_path = tmp.path().join("shard.manifest");
    // File 3 backs nothing but `k`'s oldest copy, file 7 also backs a live
    // key of its own, file 12 backs the entry `k` now points at.
    let files: &[(u64, &[(&str, &str)])] = &[
        (3, &[("k", "v3")]),
        (7, &[("k", "v7"), ("solo", "s")]),
        (12, &[("k", "v12")]),
    ];
    spill_files(tmp.path(), &manifest_path, files);
    let manifest = ShardManifest::open(&manifest_path).expect("open manifest");
    let mut per_db = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &manifest).per_db;
    let (_, index) = per_db.remove(0);

    assert_eq!(
        index.pending_unlink_len(),
        0,
        "file 3 is still reachable through the gate, so it may not be queued for unlink"
    );
    assert_eq!(index.referenced_file_count(), 3);

    let mut db = Database::new();
    db.cold_index = Some(index);
    db.install_replay_cold_gate(10);
    let _ = db.finish_replay_cold_reconcile();
    let ci = db.cold_index.as_ref().expect("index");
    assert_eq!(
        ci.pending_unlink_len(),
        1,
        "file 3 lost its last referrer when the generation closed"
    );
    assert_eq!(
        ci.referenced_file_count(),
        2,
        "file 7 still backs `solo` and file 12 still backs `k`"
    );
}

// ---------------------------------------------------------------------------
// moon#1202: what a `MOON.SPILLED` marker that never reached the AOF costs,
// in every ordering the issue asks about. The crash image is the one a marker
// dropped under AOF backpressure leaves: the spill file is in the manifest,
// the keys were published cold live (their hot copies gone), and the log
// holds every record that built them but no cut. Each test replays through
// the production recovery path above.
// ---------------------------------------------------------------------------

/// The value of `key` and whether recovery left it hot (vs cold).
fn value_and_plane(db: &mut Database, key: &[u8]) -> (Option<Vec<u8>>, bool) {
    let hot = db.is_hot(key);
    (string_value(db, key), hot)
}

/// (a) No later write: the key recovers HOT with the value the log built.
/// The file's entry is dropped at the end-of-replay resolution, so nothing
/// points at the unauthorized copy afterwards.
#[test]
fn moon1202_a_dropped_marker_with_no_later_write_recovers_hot() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"hello"]));
    let (_dir, mut db) = recover(&[(5, &[("k", "hello")])], &aof);
    assert!(
        db.cold_index
            .as_ref()
            .is_some_and(|ci| ci.lookup(b"k").is_none()),
        "hot-wins: the unauthorized cold entry is dropped"
    );
    assert_eq!(
        value_and_plane(&mut db, b"k"),
        (Some(b"hello".to_vec()), true),
        "the key recovers hot with exactly the logged value"
    );
}

/// Control for (a): with the marker the same key recovers cold.
#[test]
fn moon1202_a_control_the_marker_recovers_cold() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"hello"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"5", b"k"]));
    let (_dir, mut db) = recover(&[(5, &[("k", "hello")])], &aof);
    assert_eq!(
        value_and_plane(&mut db, b"k"),
        (Some(b"hello".to_vec()), false)
    );
}

/// (b) A later non-idempotent write: live, it promoted the cold copy
/// (`hello`) and appended. Replay lands it on the hot copy the log rebuilt —
/// neither lost nor applied twice.
#[test]
fn moon1202_b_a_later_append_is_neither_lost_nor_doubled() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"hello"]));
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"!"]));
    let (_dir, mut db) = recover(&[(5, &[("k", "hello")])], &aof);
    assert_eq!(
        value_and_plane(&mut db, b"k"),
        (Some(b"hello!".to_vec()), true)
    );
}

/// (b) A later SET and a later DEL.
#[test]
fn moon1202_b_a_later_set_or_del_wins() {
    for (tail, want) in [
        (resp(&[b"SET", b"k", b"new"]), Some(b"new".to_vec())),
        (resp(&[b"DEL", b"k"]), None),
    ] {
        let mut aof = crate::persistence::cold_records::serialize_cold_cut(1).to_vec();
        aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"hello"]));
        aof.extend_from_slice(&tail);
        let (_dir, mut db) = recover(&[(5, &[("k", "hello")])], &aof);
        assert_eq!(string_value(&mut db, b"k"), want);
    }
}

/// (b) A later write, then a re-spill whose marker DID land. The second
/// replayed write retires the stale shadow (`Database::set`'s `Updated` arm),
/// so the key recovers hot rather than cut over — either way with the
/// current value.
#[test]
fn moon1202_b_a_later_respill_with_its_marker_recovers_the_current_value() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"SET", b"k", b"hello"]));
    aof.extend_from_slice(&resp(&[b"SET", b"k", b"hello!"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"9", b"k"]));
    let files: &[(u64, &[(&str, &str)])] = &[(5, &[("k", "hello")]), (9, &[("k", "hello!")])];
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(string_value(&mut db, b"k").as_deref(), Some(&b"hello!"[..]));
}

/// (b) The key's pre-spill history is NOT in this generation: it was cold
/// below the cut (file 3), read-promoted (unlogged), re-spilled into file 5
/// with the marker dropped, then written. Live, the write promoted file 5's
/// copy. Replay: file 5 is hidden, its older copy in file 3 (same value) is
/// visible, so the write lands exactly once.
#[test]
fn moon1202_b_a_write_after_a_dropped_respill_of_a_pre_cut_key() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(4).to_vec();
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"!"]));
    let files: &[(u64, &[(&str, &str)])] = &[(3, &[("k", "v0")]), (5, &[("k", "v0")])];
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(string_value(&mut db, b"k").as_deref(), Some(&b"v0!"[..]));
}

/// (c) A rewrite committing after the drop: the base is hot-only, so the key
/// is in it not at all; the new generation's `MOON.COLDCUT` is above the
/// file, which authorizes it. The key recovers cold, and a post-rewrite write
/// replays onto that copy exactly once.
#[test]
fn moon1202_c_a_rewrite_after_the_drop_heals_it() {
    let head = crate::persistence::cold_records::serialize_cold_cut(9).to_vec();
    let (_dir, mut db) = recover(&[(5, &[("k", "hello")])], &head);
    assert_eq!(
        value_and_plane(&mut db, b"k"),
        (Some(b"hello".to_vec()), false),
        "no record in the new generation: served cold from the file below the cut"
    );

    let mut aof = head;
    aof.extend_from_slice(&resp(&[b"APPEND", b"k", b"!"]));
    let (_dir, mut db) = recover(&[(5, &[("k", "hello")])], &aof);
    assert_eq!(string_value(&mut db, b"k").as_deref(), Some(&b"hello!"[..]));
}

/// Why a dropped marker must never be RETRIED later. Same history as the
/// test above, but the key's post-spill write is a SET — its first replayed
/// write, so `set`'s `Inserted` arm leaves file 5's shadow standing — and a
/// retried marker for file 5 lands AFTER it. Replay then cuts the key back to
/// file 5's older value: the acknowledged SET is lost. The fix never logs a
/// marker anywhere but at its publish instant (fail-closed instead).
#[test]
fn moon1202_a_marker_logged_after_a_later_write_would_lose_it() {
    let mut aof = crate::persistence::cold_records::serialize_cold_cut(4).to_vec();
    aof.extend_from_slice(&resp(&[b"SET", b"k", b"acked"]));
    let files: &[(u64, &[(&str, &str)])] = &[(3, &[("k", "v0")]), (5, &[("k", "v0")])];
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(
        string_value(&mut db, b"k").as_deref(),
        Some(&b"acked"[..]),
        "control: no marker, the SET survives"
    );

    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"5", b"k"]));
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(
        string_value(&mut db, b"k").as_deref(),
        Some(&b"v0"[..]),
        "a late marker drops the acknowledged SET — the hazard a retry would create"
    );
}
