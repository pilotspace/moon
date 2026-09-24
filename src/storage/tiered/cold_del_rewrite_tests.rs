//! A cold key deleted BEFORE an AOF rewrite must stay deleted after a crash.
//!
//! The crash image: `k1` and `k2` were spilled together into cold file 5
//! (manifest Active). `DEL k1` removed `k1` from the in-memory cold index,
//! but file 5 still backs the live `k2`, so the orphan sweep never retires
//! it and the manifest keeps listing it Active with `k1`'s slot intact. A
//! rewrite then cut a new generation: the base is hot-only (neither key is
//! hot), and the new incr opens with `MOON.COLDCUT 6` — which authorizes
//! file 5 — and never mentions `k1` again. The `DEL` record lived in the
//! generation the rewrite discarded.
//!
//! Recovery rebuilds the cold index from every Active file, so unless
//! something durable remembers the delete, `k1` comes back.
//!
//! The model tests below all pass except the delete itself, which is RED
//! (see its doc). `tests/crash_recovery_cold_del_rewrite.rs` proves the same
//! against a real server (DEL, FLUSHDB inside the sweep window, and a clean
//! `SHUTDOWN` all resurrect; overwrite, TTL expiry and the no-rewrite control
//! do not).
//!
//! Same harness as `replay_older_copy_tests`: real spill batches, production
//! `recover_shard_v3_with_fallback` over a legacy `appendonly.aof`.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::cold_records::serialize_cold_cut;
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::storage::Database;
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

/// The post-rewrite image: the generation is `MOON.COLDCUT 6` and nothing
/// else (the hot-only base holds neither key).
///
/// RED on main (and v0.8.9): recovers `k1 = v1`. Nothing durable remembers
/// the delete once the rewrite drops the `DEL` record — `ColdIndex::remove`
/// is in-memory only, the orphan sweep never retires a file with live
/// referrers, and `rebuild_from_manifest_per_db` indexes every slot of every
/// Active file. Un-ignore with the fix.
#[test]
#[ignore = "cold DEL resurrects after an AOF rewrite; un-ignore with the fix"]
fn a_cold_key_deleted_before_a_rewrite_stays_deleted() {
    let aof = serialize_cold_cut(6).to_vec();
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
