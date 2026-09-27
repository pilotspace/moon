//! moon#1265 review round 3: a spill-thread death inside a cold-reclaim job
//! is charged to the reclaim, not to spilling. A systematic reclaim bug —
//! every job panics, on every file — spends the reclaim's own death budget
//! and ends with the shard's cold reclaim DISABLED, its spilling alive and
//! every cold key readable, instead of a degraded shard or an endless crash
//! loop. (Three poisoned files, each given up at its second death, stay under
//! that budget: `review_r3_tests`.)

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
use crate::storage::tiered::spill_thread::supervisor::RestartPolicy;
use crate::storage::tiered::spill_thread::{Respawn, SpillRequest, SpillThread};

/// More mostly-dead files than the reclaim-death budget can give up (each
/// give-up costs two deaths), so only the budget can end the loop.
const FILES: std::ops::RangeInclusive<u64> = 5..=14;
const FIRST_NEW: u64 = 100;

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

/// One listed spill file of ten keys per id in [`FILES`], the live db
/// recovered from their log (every key cold), 8 of each file's keys deleted.
fn fixture(root: &std::path::Path, shard_dir: &std::path::Path) -> Database {
    std::fs::create_dir_all(shard_dir).expect("dir");
    let mut manifest = ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("m");
    let mut log = serialize_cold_cut(1).to_vec();
    for file_id in FILES {
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
    for file_id in FILES {
        for (k, _) in keys_of(file_id).iter().take(8) {
            assert!(db.remove_counting_cold(k.as_bytes()).0, "{k} was cold");
        }
    }
    db
}

fn count(f: impl Fn(&crate::storage::tiered::cold_index::ColdIndex) -> usize) -> usize {
    with_shard_db(0, |db| db.cold_index.as_ref().map_or(0, f))
}

#[test]
fn a_systematic_reclaim_bug_disables_the_reclaim_and_keeps_spilling() {
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
        // Every reclaim write panics, whatever the file: a reclaim bug.
        let st =
            SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::ReclaimWrite, 1_000)));
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level(tx);
        let runtime_config = Arc::new(parking_lot::RwLock::new(
            crate::config::RuntimeConfig::default(),
        ));
        let mut next = FIRST_NEW;
        let disabled_before = crate::storage::tiered::spill_thread::cold_reclaim_disabled_shards();
        let mut tick = |manifest: &mut Option<ShardManifest>| {
            let was_dead = st.is_dead();
            drain_and_apply(&st, manifest, &mut sink(), 1, 0);
            cold_reclaim_tick::run(
                &shared,
                0,
                &runtime_config,
                manifest,
                &mut next,
                Some(&shard_dir),
                Some(&pool),
                Some(&st),
                usize::MAX,
            );
            let respawned = match st.respawn_if_due(st.clock_ms()) {
                Respawn::Respawned => true,
                Respawn::NotDue => false,
                Respawn::Failed(v) => panic!("respawn failed: {v:?}"),
            };
            (was_dead, respawned)
        };

        let mut respawns = 0usize;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !st.reclaim_disabled() {
            assert!(!st.is_degraded(), "degraded after {respawns} respawns");
            assert!(Instant::now() < deadline, "the reclaim was never disabled");
            respawns += usize::from(tick(&mut manifest).1);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            crate::storage::tiered::spill_thread::cold_reclaim_disabled_shards() > disabled_before
        );
        // The death that disabled it is respawned like the others.
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.is_dead() {
            assert!(
                Instant::now() < deadline,
                "no respawn after the disabling death"
            );
            respawns += usize::from(tick(&mut manifest).1);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            respawns,
            RestartPolicy::DEFAULT.max_reclaim_deaths,
            "one respawn per reclaim death, the budget's worth"
        );

        // Then quiet: no job is sent, so no death, and nothing in flight,
        // although files the budget did not reach are still candidates.
        let settle = Instant::now() + Duration::from_millis(600);
        while Instant::now() < settle {
            let (was_dead, _) = tick(&mut manifest);
            assert!(!was_dead, "a death after the reclaim was disabled");
            assert_eq!(count(|ci| ci.compactions_in_flight()), 0, "a job was sent");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(count(|ci| ci.reclaim_candidates(usize::MAX).len()) > 0);
        assert!(!st.is_degraded());

        // Spilling is alive: a request is written and its watermark moves.
        // Far above every id the reclaim minted.
        let id = FIRST_NEW + 1_000_000;
        st.sender()
            .try_send(SpillRequest {
                key: Bytes::from_static(b"after"),
                db_index: 0,
                value_bytes: Bytes::from_static(b"spilled"),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
                file_id: id,
                shard_dir: shard_dir.clone(),
            })
            .expect("the request channel is open");
        let deadline = Instant::now() + Duration::from_secs(10);
        while st.done_below() <= id {
            assert!(Instant::now() < deadline, "the spill was never written");
            tick(&mut manifest);
            std::thread::sleep(Duration::from_millis(5));
        }
        let _ = st.shutdown();

        // Every key left alive still reads from its file.
        for file_id in FILES {
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
    })
    .join()
    .unwrap();
}
