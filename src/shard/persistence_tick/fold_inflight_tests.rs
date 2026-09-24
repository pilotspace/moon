//! moon#1223: a spill in flight at an AOF rewrite fold, whose completion
//! then does NOT publish, must still have a durable source.
//!
//! Eviction removed the key's hot copy and its value lives only in the
//! in-flight plane (file 5). The fold cuts a new base at that instant. Then
//! the completion arrives and puts the key back in RAM without a log record,
//! by one of three paths: the `MOON.SPILLED` marker is refused by a saturated
//! AOF writer (the moon#1202 withdraw), the pwrite failed, or the file id is
//! already listed (moon#893). The key is live and acknowledged; the base must
//! carry it, because the post-fold log does not and the manifest does not
//! list file 5.
//!
//! `rv_integ_withdrawn_spill_after_fold_has_a_durable_source` is the PR #1221
//! integration review's proof test, adopted: on `ae21476` it fails with
//! `live=Some("acked-value") in_base=false manifest_lists_file=false
//! in_post_fold_log=false`. Its `in_base` is now judged by LOADING the base
//! image (the value must round-trip), not by a byte-window search.

use super::*;
use crate::persistence::aof::fold_stream::{
    fold_image_channel, stream_fold_image, write_fold_image,
};
use crate::persistence::aof::{AofMessage, AofWriterPool};
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::shard::slice::{
    ShardSlice, init_shard, test_support::make_init, with_shard, with_shard_db,
};
use crate::storage::db::PendingSpill;
use crate::storage::tiered::spill_thread::{SpillCompletion, SpillCompletionEntry, SpillRequest};

const KEY: &[u8] = b"rv-integ-inflight-key";
const VALUE: &[u8] = b"acked-value";
const FILE: u64 = 5;

/// How the in-flight spill ends after the fold.
#[derive(Clone, Copy, Debug)]
enum Ending {
    /// The completion succeeds but the AOF writer refuses its marker.
    MarkerRefused,
    /// The spill thread's pwrite failed.
    PwriteFailed,
    /// The manifest already lists the file id.
    IdAlreadyListed,
}

struct Outcome {
    live: Option<Vec<u8>>,
    /// The key's value in the fold's base image, loaded back.
    base_value: Option<Vec<u8>>,
    manifest_lists_file: bool,
    in_post_fold_log: bool,
    base: Vec<u8>,
}

fn file_entry() -> FileEntry {
    FileEntry {
        file_id: FILE,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12,
        page_count: 1,
        byte_size: 4096,
        created_lsn: 0,
        db_index: 0,
        max_key_hash: 0,
        last_modified_lsn: 0,
    }
}

fn run(ending: Ending) -> Outcome {
    std::thread::spawn(move || {
        let key = bytes::Bytes::from_static(KEY);
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        if matches!(ending, Ending::IdAlreadyListed) {
            manifest.add_file(file_entry()).unwrap();
        }
        let listed_before = manifest.files().iter().any(|f| f.file_id == FILE);
        // Eviction picked the key: hot copy removed, payload in flight
        // (file 5) — exactly `evict_one_async_spill`'s end state.
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
            db.spill_inflight_mark(
                key.clone(),
                PendingSpill {
                    req_id: FILE,
                    value_type: ValueType::String,
                    value_bytes: bytes::Bytes::from_static(VALUE),
                    ttl_ms: None,
                },
            );
        });
        // BGREWRITEAOF fold at this instant (the AofFold arm's capture).
        let (sink, image) = fold_image_channel();
        let now = crate::storage::entry::current_time_ms();
        with_shard(|s| {
            s.databases
                .with_all_read(|all| stream_fold_image(all, now, sink))
        });
        let mut base = Vec::new();
        write_fold_image(image, &mut base, "moon#1223 test").unwrap();
        let mut loaded = vec![crate::storage::Database::new()];
        crate::persistence::rdb::load_from_bytes(&mut loaded, &base).unwrap();
        let base_value = loaded[0]
            .data()
            .get(KEY)
            .and_then(|e| e.value.as_bytes().map(|b| b.to_vec()));

        // The completion lands right after the fold, while the writer's
        // channel is still saturated (main's soak: 2715/3655 post-fold
        // drains found it full).
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(1);
        let pool = AofWriterPool::top_level(tx);
        assert!(pool.try_send_append(0, 1, 0, bytes::Bytes::from_static(b"filler")));
        let entry = SpillCompletionEntry {
            key: key.clone(),
            db_index: 0,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: ValueType::String,
            req_file_id: FILE,
        };
        let completion = match ending {
            Ending::MarkerRefused | Ending::IdAlreadyListed => SpillCompletion {
                file_entry: file_entry(),
                entries: vec![entry],
                success: true,
                failed_request: None,
            },
            Ending::PwriteFailed => SpillCompletion {
                file_entry: file_entry(),
                entries: Vec::new(),
                success: false,
                failed_request: Some(Box::new(SpillRequest {
                    key: key.clone(),
                    db_index: 0,
                    value_bytes: bytes::Bytes::from_static(VALUE),
                    value_type: ValueType::String,
                    flags: 0,
                    ttl_ms: None,
                    file_id: FILE,
                    shard_dir: tmp.path().to_path_buf(),
                })),
            },
        };
        let mut msink = ColdMarkerSink {
            aof_pool: Some(&pool),
            wal_writer: None,
            shard_id: 0,
            wal_kv_log: false,
        };
        let mut shard_manifest = Some(manifest);
        apply_completion_vec(vec![completion], &mut shard_manifest, &mut msink);

        let live = with_shard_db(0, |db| {
            db.data()
                .get(KEY)
                .and_then(|e| e.value.as_bytes().map(|b| b.to_vec()))
        });
        // A listing that predates the completion describes some OTHER file
        // (moon#893) and is not a source for this key.
        let manifest_lists_file = !listed_before
            && shard_manifest
                .as_ref()
                .unwrap()
                .files()
                .iter()
                .any(|f| f.file_id == FILE);
        let mut in_post_fold_log = false;
        while let Ok(msg) = rx.try_recv() {
            if let AofMessage::Append { bytes, .. } = msg
                && bytes.windows(KEY.len()).any(|w| w == KEY)
            {
                in_post_fold_log = true;
            }
        }
        Outcome {
            live,
            base_value,
            manifest_lists_file,
            in_post_fold_log,
            base,
        }
    })
    .join()
    .expect("shard thread")
}

fn assert_durable(ending: Ending) -> Outcome {
    let o = run(ending);
    eprintln!(
        "RV-INTEG 1202xFOLD ({ending:?}): live={:?} in_base={} manifest_lists_file={} \
         in_post_fold_log={}",
        o.live.as_deref().map(String::from_utf8_lossy),
        o.base_value.is_some(),
        o.manifest_lists_file,
        o.in_post_fold_log
    );
    assert_eq!(
        o.live.as_deref(),
        Some(VALUE),
        "{ending:?}: precondition: the key is back in RAM, live and acknowledged"
    );
    assert!(
        o.base_value.is_some() || o.manifest_lists_file || o.in_post_fold_log,
        "{ending:?}: the key is live in RAM but in NO durable artifact: not in the fold's base \
         image, not in the post-fold AOF generation, and its spill file is not in the \
         manifest — a restart loses it"
    );
    assert_eq!(
        o.base_value.as_deref(),
        Some(VALUE),
        "{ending:?}: the base must carry the key with the value it had at the fold"
    );
    o
}

/// The review's proof test (moon#1202 withdraw x the fold), adopted.
#[test]
fn rv_integ_withdrawn_spill_after_fold_has_a_durable_source() {
    assert_durable(Ending::MarkerRefused);
}

/// The same hole through the failed-pwrite re-insert (deep-review F1).
#[test]
fn a_failed_spill_pwrite_after_a_fold_has_a_durable_source() {
    assert_durable(Ending::PwriteFailed);
}

/// The same hole through the re-issued-file-id re-insert (moon#893).
#[test]
fn a_refused_spill_file_id_after_a_fold_has_a_durable_source() {
    assert_durable(Ending::IdAlreadyListed);
}

/// End to end through production recovery: the new generation is exactly
/// what the tokio `--shards 1` fold publishes — the base image followed by
/// the `MOON.COLDCUT` head — and nothing else (the post-fold log has no
/// record of the key, file 5 is not in the manifest). A restart must serve
/// the key.
#[test]
fn a_restart_after_the_withdraw_recovers_the_key_from_the_base() {
    let o = assert_durable(Ending::MarkerRefused);
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();
    // An empty manifest: the withdrawn file 5 is not listed.
    ShardManifest::create(&shard_dir.join("shard-0.manifest"))
        .unwrap()
        .commit()
        .unwrap();
    let legacy = tmp.path().join("legacy");
    std::fs::create_dir_all(&legacy).unwrap();
    let mut aof = o.base;
    aof.extend_from_slice(&crate::persistence::cold_records::serialize_cold_cut(
        FILE + 1,
    ));
    std::fs::write(legacy.join("appendonly.aof"), &aof).unwrap();

    let mut dbs = vec![crate::storage::Database::new()];
    crate::persistence::recovery::recover_shard_v3_with_fallback(
        &mut dbs,
        0,
        &shard_dir,
        &crate::persistence::replay::DispatchReplayEngine::new(),
        Some(&legacy),
        false,
    )
    .expect("recovery");
    let got = dbs[0]
        .data()
        .get(KEY)
        .and_then(|e| e.value.as_bytes().map(|b| b.to_vec()));
    assert_eq!(
        got.as_deref(),
        Some(VALUE),
        "the acknowledged key must survive a restart after the fold + withdraw"
    );
}
