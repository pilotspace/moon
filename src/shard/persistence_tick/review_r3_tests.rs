//! Review round 3 (moon#1265, be2a30f): the poisoned-reclaim give-up costs
//! TWO spill-thread deaths per file, and those deaths still count against the
//! spill thread's restart budget (5 respawns per 10 minutes). So THREE files
//! whose compaction panics deterministically spend 6 deaths and degrade the
//! shard's spilling for the life of the process — the MAJOR-2 outcome of
//! round 2b, reached with three bad files instead of one.
//!
//! A reclaim panic is rarely confined to one file: the source round 2b and
//! the issue name ("a corrupt spill file read by cold reclaim", moon#1240) is
//! as likely a decode bug hit by every file of a shape as a single bit-rotted
//! file, and the reclaim starts `FILES_PER_TICK` = 2 files per tick.
//!
//! Expected: a fault confined to cold reclaim (an optional duty) never
//! degrades spilling (an essential one) — e.g. a death with a reclaim culprit
//! does not count against the spill budget until that file is given up, or
//! reclaim is disabled after K culprits instead.
//!
//! Red at 4e06039 (monoio and tokio): "3 poisoned reclaim files DEGRADED ...".

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::*;
use crate::persistence::aof::{AofMessage, AofWriterPool};
use crate::persistence::cold_records::serialize_cold_cut;
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::shard::slice::{ShardSlice, init_shard, with_shard_db};
use crate::storage::Database;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};
use crate::storage::tiered::spill_thread::fault::{PanicPlan, PanicPoint};
use crate::storage::tiered::spill_thread::{Respawn, SpillThread};

/// The poisoned files' ids; new (compaction output) ids start above them.
const FILES: [u64; 3] = [5, 6, 7];
const FIRST_NEW: u64 = 20;

fn sink() -> ColdMarkerSink<'static> {
    ColdMarkerSink {
        aof_pool: None,
        wal_writer: None,
        shard_id: 0,
        wal_kv_log: false,
    }
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

fn keys_of(file_id: u64) -> Vec<(String, String)> {
    (0..10)
        .map(|i| (format!("f{file_id}k{i:02}"), format!("v{file_id}-{i:02}")))
        .collect()
}

/// `FILES.len()` listed spill files of ten keys each, the live db recovered
/// from their log (every key cold), and 8 of each file's 10 keys deleted, so
/// every file is mostly dead and a compaction candidate.
fn fixture(root: &std::path::Path, shard_dir: &std::path::Path) -> Database {
    std::fs::create_dir_all(shard_dir).expect("dir");
    let mut manifest = ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("m");
    let mut log = serialize_cold_cut(1).to_vec();
    for &file_id in &FILES {
        let kvs = keys_of(file_id);
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
        let batch = build_kv_spill_batch(&entries, file_id).expect("batch");
        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).expect("write");
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
            .expect("add");
        for (k, v) in &kvs {
            log.extend_from_slice(&resp(&[b"SET", k.as_bytes(), v.as_bytes()]));
        }
        let id = file_id.to_string();
        let mut marker: Vec<&[u8]> = vec![b"MOON.SPILLED", id.as_bytes()];
        marker.extend(kvs.iter().map(|(k, _)| k.as_bytes()));
        log.extend_from_slice(&resp(&marker));
    }
    manifest.commit().expect("commit");
    drop(manifest);
    let legacy = root.join("legacy");
    std::fs::create_dir_all(&legacy).expect("aof dir");
    std::fs::write(legacy.join("appendonly.aof"), &log).expect("aof");
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
    let mut db = dbs.remove(0);
    for &file_id in &FILES {
        for (k, _) in keys_of(file_id).iter().take(8) {
            assert!(db.remove_counting_cold(k.as_bytes()).0, "{k} was cold");
        }
    }
    db
}

#[test]
fn three_poisoned_reclaim_files_do_not_degrade_spilling() {
    std::thread::spawn(|| {
        let tmp = tempfile::tempdir().expect("tempdir");
        let shard_dir = tmp.path().join("shard-0");
        let live = fixture(tmp.path(), &shard_dir);
        let (shared, mut inits) =
            crate::shard::shared_databases::ShardDatabases::new(vec![vec![Database::new()]]);
        init_shard(ShardSlice::new(inits.remove(0)));
        with_shard_db(0, |db| *db = live);
        let mut manifest =
            Some(ShardManifest::open(&shard_dir.join("shard-0.manifest")).expect("manifest"));
        // Every reclaim WRITE panics: all three compactable files are bad.
        let st =
            SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::ReclaimWrite, 1_000)));
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level(tx);
        let runtime_config = Arc::new(parking_lot::RwLock::new(
            crate::config::RuntimeConfig::default(),
        ));
        let mut next = FIRST_NEW;

        let mut respawns = 0u32;
        let mut quiet_since = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let was_dead = st.is_dead();
            drain_and_apply(&st, &mut manifest, &mut sink(), 1, 0);
            cold_reclaim_tick::run(
                &shared,
                0,
                &runtime_config,
                &mut manifest,
                &mut next,
                Some(&shard_dir),
                Some(&pool),
                Some(&st),
                usize::MAX,
            );
            if was_dead {
                quiet_since = Instant::now();
            }
            match st.respawn_if_due(st.clock_ms()) {
                Respawn::Respawned => respawns += 1,
                Respawn::NotDue => {}
                Respawn::Failed(v) => panic!("respawn failed: {v:?}"),
            }
            if st.is_degraded() {
                break;
            }
            // Survived 3 s with a live thread: every poison was given up.
            if !st.is_dead() && quiet_since.elapsed() > Duration::from_secs(3) {
                break;
            }
            assert!(Instant::now() < deadline, "neither degraded nor settled");
            std::thread::sleep(Duration::from_millis(5));
        }
        let degraded = st.is_degraded();
        let given_up = crate::storage::tiered::cold_reclaim::files_given_up_total();
        let _ = st.shutdown();
        // Every key that was left alive is still readable, degraded or not.
        for &file_id in &FILES {
            for (k, v) in keys_of(file_id).iter().skip(8) {
                let got = with_shard_db(0, |db| {
                    db.get_cold_value(k.as_bytes(), crate::storage::entry::current_time_ms())
                        .map(|val| format!("{val:?}"))
                });
                assert!(
                    got.as_deref().is_some_and(|g| g.contains(v.as_str())),
                    "{k} lost: {got:?}"
                );
            }
        }
        assert!(
            !degraded,
            "{} poisoned reclaim files DEGRADED the shard's spilling after {respawns} respawns \
             (files given up process-wide: {given_up}): each give-up costs two deaths and every \
             death spends the spill thread's 5-respawn budget",
            FILES.len()
        );
    })
    .join()
    .unwrap();
}
