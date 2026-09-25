//! A cold key deleted BEFORE an AOF rewrite must stay deleted after a crash.
//!
//! The crash image: `k1` and `k2` were spilled together into cold file 5
//! (manifest Active). `DEL k1` removed `k1` from the in-memory cold index,
//! but file 5 still backs the live `k2`, so the orphan sweep never retires
//! it and the manifest keeps listing it Active with `k1`'s slot intact. A
//! rewrite then cut a new generation: the base is hot-only (neither key is
//! hot), and the new incr opens with `MOON.COLDCUT 6` — which authorizes
//! file 5. The `DEL` record lived in the generation the rewrite discarded.
//!
//! Recovery rebuilds the cold index from every Active file, so unless
//! something durable remembers the delete, `k1` comes back.
//!
//! The fix (moon#1215) is the cold index's dead-slot ledger plus a plain
//! `DEL` of every key the fold finds dead at its instant, written right after
//! the new generation's `MOON.COLDCUT`. So the post-rewrite image is no
//! longer "COLDCUT and nothing else": every test below builds it with the
//! PRODUCTION fold (`stream_fold_image` + `write_fold_image` +
//! `generation_head`, the tokio `--shards 1` flat-file layout) from a live
//! database the delete ran against, then recovers it with production
//! recovery. The image the unfixed fold wrote (COLDCUT only) carries no
//! record of the delete at all, so no recovery could keep `k1` deleted from
//! it; red on `ae21476` is shown by the real-server suite
//! (`tests/crash_recovery_cold_del_rewrite.rs`) and by disabling the
//! ledger's DELs, which turns every `*_stays_deleted` case below red.
//!
//! Same harness as `replay_older_copy_tests`: real spill batches, production
//! `recover_shard_v3_with_fallback` over a legacy `appendonly.aof`.

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
use crate::storage::entry::Entry;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

type Kv<'a> = (&'a str, &'a str, Option<u64>);

fn spill_files(shard_dir: &Path, manifest_path: &Path, files: &[(u64, &[Kv<'_>])]) {
    let mut manifest = ShardManifest::create(manifest_path).expect("create manifest");
    for (file_id, kvs) in files {
        let entries: Vec<SpillEntry> = kvs
            .iter()
            .map(|(k, v, ttl)| SpillEntry {
                key: Bytes::copy_from_slice(k.as_bytes()),
                value_bytes: Bytes::copy_from_slice(v.as_bytes()),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: *ttl,
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
    (tmp, dbs.remove(0))
}

fn string_value(db: &mut Database, key: &[u8]) -> Option<Vec<u8>> {
    string_value_at(db, key, 0)
}

fn string_value_at(db: &mut Database, key: &[u8], now_ms: u64) -> Option<Vec<u8>> {
    db.promote_cold_if_present(key, now_ms);
    db.get(key)
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| b.to_vec())
}

const SHARED_FILE: &[(u64, &[Kv<'static>])] = &[(5, &[("k1", "v1", None), ("k2", "v2", None)])];

/// The live server just before the rewrite: `k1` and `k2` cold in file 5,
/// recovered from the pre-rewrite log (their SETs and the spill's marker).
fn live_with_both_cold(files: &[(u64, &[Kv<'_>])]) -> (tempfile::TempDir, Database) {
    let mut aof = serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"SET", b"k1", b"v1"]));
    aof.extend_from_slice(&resp(&[b"SET", b"k2", b"v2"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"5", b"k1", b"k2"]));
    let (dir, db) = recover(files, &aof);
    assert!(
        !db.is_hot(b"k1") && !db.is_hot(b"k2"),
        "fixture: both keys cold"
    );
    (dir, db)
}

/// What the production fold of `db` publishes as the new generation in the
/// tokio `--shards 1` layout: the base image, then the head (`MOON.COLDCUT
/// <watermark>` and the moon#1215 DELs).
fn fold(db: &Database, watermark: u64) -> Vec<u8> {
    let now = crate::storage::entry::current_time_ms();
    let (sink, image) = fold_image_channel();
    stream_fold_image(&[db], now, sink);
    let mut aof = Vec::new();
    let (_, deletes) = write_fold_image(image, &mut aof, "moon#1215 test").expect("fold image");
    aof.extend_from_slice(&generation_head(watermark, &deletes, false));
    aof
}

/// Control: no rewrite. The `DEL` is still in the log and replays onto the
/// cold plane (moon#257).
#[test]
fn control_without_rewrite_the_replayed_del_keeps_k1_deleted() {
    let mut aof = serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"SET", b"k1", b"v1"]));
    aof.extend_from_slice(&resp(&[b"SET", b"k2", b"v2"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"5", b"k1", b"k2"]));
    aof.extend_from_slice(&resp(&[b"DEL", b"k1"]));
    let (_dir, mut db) = recover(SHARED_FILE, &aof);
    assert_eq!(string_value(&mut db, b"k1"), None);
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// moon#1215: DEL of a cold key whose file still backs a live neighbour,
/// then a rewrite, then a restart.
#[test]
fn a_cold_key_deleted_before_a_rewrite_stays_deleted() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    assert!(
        live.remove_counting_cold(b"k1").0,
        "DEL k1 removes a live key"
    );
    let aof = fold(&live, 6);
    let (_dir, mut db) = recover(SHARED_FILE, &aof);
    assert_eq!(
        string_value(&mut db, b"k2").as_deref(),
        Some(&b"v2"[..]),
        "the live neighbour must survive"
    );
    assert_eq!(
        string_value(&mut db, b"k1"),
        None,
        "k1 was DELeted before the rewrite; it must not come back from file 5"
    );
}

/// The delete must outlive EVERY later rewrite and restart, not just the
/// first: the restarted server re-learns the dead slot from the head's own
/// DEL (the replay removes the rebuilt entry, which records it in the
/// ledger), so its next fold writes the DEL again.
#[test]
fn a_cold_key_deleted_before_a_rewrite_stays_deleted_across_later_rewrites() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    live.remove_counting_cold(b"k1");
    let (_dir1, restarted) = recover(SHARED_FILE, &fold(&live, 6));
    assert!(
        restarted
            .cold_index
            .as_ref()
            .is_some_and(|ci| ci.dead_slots().file_has_dead_slots(5)),
        "the replayed head DEL must put k1's slot back in the ledger"
    );
    let (_dir2, mut db) = recover(SHARED_FILE, &fold(&restarted, 6));
    assert_eq!(string_value(&mut db, b"k1"), None, "second rewrite");
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// FLUSHDB of cold keys, then a rewrite before the orphan sweep retired the
/// file (the issue's FLUSHDB-inside-the-sweep-window case).
#[test]
fn cold_keys_flushed_before_a_rewrite_stay_flushed() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    live.clear();
    let (_dir, mut db) = recover(SHARED_FILE, &fold(&live, 6));
    assert_eq!(string_value(&mut db, b"k1"), None);
    assert_eq!(string_value(&mut db, b"k2"), None);
}

/// A key read back into RAM (promotion takes it out of the cold index) and
/// then deleted: the promotion left its slot behind.
#[test]
fn a_promoted_then_deleted_key_stays_deleted() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    assert_eq!(string_value(&mut live, b"k1").as_deref(), Some(&b"v1"[..]));
    assert!(live.is_hot(b"k1"), "fixture: the read promoted k1");
    live.remove_counting_cold(b"k1");
    let (_dir, mut db) = recover(SHARED_FILE, &fold(&live, 6));
    assert_eq!(string_value(&mut db, b"k1"), None);
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// The complement: a promoted key that is still alive is in the base, gets
/// no DEL, and wins over its old slot.
#[test]
fn a_promoted_live_key_keeps_its_value() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    assert_eq!(string_value(&mut live, b"k1").as_deref(), Some(&b"v1"[..]));
    let (_dir, mut db) = recover(SHARED_FILE, &fold(&live, 6));
    assert_eq!(string_value(&mut db, b"k1").as_deref(), Some(&b"v1"[..]));
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// A key overwritten by a blind SET keeps its cold entry as a shadow (the
/// `Inserted` arm of `set`); if the new value is EXPIRED at the fold, the
/// base drops it and the shadow would come back as the key's value.
#[test]
fn an_expired_overwrite_of_a_cold_key_does_not_resurrect_the_old_value() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    let mut e = Entry::new_string(Bytes::from_static(b"new"));
    e.set_expires_at_ms(1);
    live.set(b"k1", e);
    assert!(
        live.cold_index.as_ref().unwrap().lookup(b"k1").is_some(),
        "fixture: a blind SET leaves the cold shadow"
    );
    let (_dir, mut db) = recover(SHARED_FILE, &fold(&live, 6));
    assert_eq!(string_value(&mut db, b"k1"), None);
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// A key deleted and then re-created is alive at the fold: no DEL, the base
/// carries the new value, and the old slot loses to it.
#[test]
fn a_deleted_then_recreated_key_keeps_its_new_value() {
    let (_live_dir, mut live) = live_with_both_cold(SHARED_FILE);
    live.remove_counting_cold(b"k1");
    live.set(b"k1", Entry::new_string(Bytes::from_static(b"again")));
    let (_dir, mut db) = recover(SHARED_FILE, &fold(&live, 6));
    assert_eq!(string_value(&mut db, b"k1").as_deref(), Some(&b"again"[..]));
}

/// A key spilled twice (file 5, then file 7) and deleted: BOTH slots must
/// stay dead — the older one would win once the newer file is gone.
#[test]
fn a_key_with_two_dead_slots_stays_deleted() {
    const TWO: &[(u64, &[Kv<'static>])] = &[
        (5, &[("k1", "v1-old", None), ("k2", "v2", None)]),
        (7, &[("k1", "v1-new", None), ("k3", "v3", None)]),
    ];
    let mut aof = serialize_cold_cut(1).to_vec();
    aof.extend_from_slice(&resp(&[b"SET", b"k1", b"v1-old"]));
    aof.extend_from_slice(&resp(&[b"SET", b"k2", b"v2"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"5", b"k1", b"k2"]));
    aof.extend_from_slice(&resp(&[b"SET", b"k1", b"v1-new"]));
    aof.extend_from_slice(&resp(&[b"SET", b"k3", b"v3"]));
    aof.extend_from_slice(&resp(&[b"MOON.SPILLED", b"7", b"k1", b"k3"]));
    let (_live_dir, mut live) = recover(TWO, &aof);
    assert_eq!(
        string_value(&mut live, b"k1").as_deref(),
        Some(&b"v1-new"[..]),
        "fixture: the newer copy wins"
    );
    live.remove_counting_cold(b"k1");
    let (_dir, mut db) = recover(TWO, &fold(&live, 8));
    assert_eq!(string_value(&mut db, b"k1"), None);
    // Same result once file 7 (the newer copy's file) is gone: only file 5's
    // older slot is left, and it must not come back either.
    const ONLY_OLD: &[(u64, &[Kv<'static>])] =
        &[(5, &[("k1", "v1-old", None), ("k2", "v2", None)])];
    let (_dir, mut db) = recover(ONLY_OLD, &fold(&live, 8));
    assert_eq!(string_value(&mut db, b"k1"), None);
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// A key in a database other than 0: the head SELECTs it, DELs, and hands
/// the generation back on db 0.
#[test]
fn the_head_deletes_in_the_right_database_and_ends_on_db_0() {
    use crate::persistence::cold_records::ColdDeletes;
    let deletes = ColdDeletes {
        per_db: vec![(3, vec![Bytes::from_static(b"x")])],
    };
    let head = generation_head(9, &deletes, false);
    let mut want = serialize_cold_cut(9).to_vec();
    want.extend_from_slice(&resp(&[b"SELECT", b"3"]));
    want.extend_from_slice(&resp(&[b"DEL", b"x"]));
    want.extend_from_slice(&resp(&[b"SELECT", b"0"]));
    assert_eq!(head, want);
}

/// Overwrite instead of delete: `SET k1 new` promoted k1 to RAM, so the
/// hot-only base carries `k1 = new` and the end-of-replay resolution keeps
/// the hot copy over the stale cold slot.
#[test]
fn a_cold_key_overwritten_before_a_rewrite_keeps_the_new_value() {
    let mut aof = serialize_cold_cut(6).to_vec();
    // The base's SET for the hot key (the legacy rewrite's RESP base).
    aof.extend_from_slice(&resp(&[b"SET", b"k1", b"new"]));
    let (_dir, mut db) = recover(SHARED_FILE, &aof);
    assert_eq!(string_value(&mut db, b"k1").as_deref(), Some(&b"new"[..]));
}

/// TTL expiry while cold: the cold slot carries its own deadline, so an
/// expired key reads as absent after recovery whether or not its delete
/// survived the rewrite.
#[test]
fn a_cold_key_expired_before_a_rewrite_stays_expired() {
    let files: &[(u64, &[Kv<'_>])] = &[(5, &[("k1", "v1", Some(1)), ("k2", "v2", None)])];
    let aof = serialize_cold_cut(6).to_vec();
    let (_dir, mut db) = recover(files, &aof);
    assert_eq!(string_value_at(&mut db, b"k1", 10_000), None);
    assert_eq!(string_value(&mut db, b"k2").as_deref(), Some(&b"v2"[..]));
}

/// PR #1233 review (the ledger records only slots that can come back): a
/// DELeted cold key whose slot's own TTL has passed at the fold gets no head
/// `DEL` — and still reads as absent after the restart, because the rebuilt
/// slot is itself expired. Its live neighbour is untouched.
#[test]
fn a_deleted_cold_key_whose_ttl_passed_needs_no_del_and_stays_dead() {
    const WITH_TTL: &[(u64, &[Kv<'static>])] =
        &[(5, &[("k1", "v1", Some(5_000)), ("k2", "v2", None)])];
    let (_live_dir, mut live) = live_with_both_cold(WITH_TTL);
    live.remove_counting_cold(b"k1");
    assert!(
        live.cold_index
            .as_ref()
            .is_some_and(|ci| ci.lookup(b"k1").is_none() && ci.dead_slots().file_has_dead_slots(5)),
        "fixture: the DEL took k1 out of the index and recorded its slot with its TTL"
    );
    let image = fold(&live, 6);
    assert!(
        !image.windows(6).any(|w| w == b"$2\r\nk1"),
        "the fold must not write a DEL for a slot that has expired"
    );
    let (_dir, mut db) = recover(WITH_TTL, &image);
    assert_eq!(string_value_at(&mut db, b"k1", 10_000), None);
    assert_eq!(
        string_value_at(&mut db, b"k2", 10_000).as_deref(),
        Some(&b"v2"[..])
    );
}
