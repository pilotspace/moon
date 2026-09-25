//! moon#1240: the cold reclaim's disk I/O runs off the shard thread.
//!
//! Drives the production tick (`cold_reclaim_tick::run`) on a shard thread
//! with a real spill thread and a manifest whose commits go through the
//! manifest-sync thread with an injected 300 ms fsync:
//!
//! - one tick never compacts inline: the read goes to the spill thread and
//!   nothing is written before the tick returns;
//! - the compaction's read and durable write run on the spill thread, never
//!   on the tick's thread;
//! - after a committed fold, adoption never waits for the manifest fsync: no
//!   tick takes anywhere near the injected delay, the survivors move only
//!   once the listing is durable, and a restart's view of the manifest agrees.
//!
//! Red with the fix reverted (the tick calling the inline `compact_file` and
//! `adopt_compactions`): the first tick leaves a pending compaction and its
//! output on disk, the read runs on the tick's thread, and the adoption tick
//! blocks for the injected fsync.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::cold_reclaim_tick;
use crate::persistence::aof::rewrite::FoldOutcome;
use crate::persistence::aof::{AofMessage, AofWriterPool, FoldEpoch};
use crate::persistence::cold_records::serialize_cold_cut;
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::shard::shared_databases::ShardDatabases;
use crate::shard::slice::{ShardSlice, init_shard, with_shard_db};
use crate::storage::Database;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};
use crate::storage::tiered::spill_thread::SpillThread;

const OLD: u64 = 5;
const FIRST_NEW: u64 = 20;
/// The injected manifest fsync.
const SYNC_DELAY_MS: u64 = 300;
/// A tick that did not wait for that fsync is far below it.
const TICK_BUDGET: Duration = Duration::from_millis(100);

fn keys() -> Vec<(String, String)> {
    (0..10)
        .map(|i| (format!("k{i:02}"), format!("v{i:02}")))
        .collect()
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

/// Spill file `OLD` holding the ten keys, listed; then the live db recovered
/// from their log (keys cold in `OLD`), with k00..k07 deleted (ledger).
fn fixture(root: &Path, shard_dir: &Path) -> Database {
    std::fs::create_dir_all(shard_dir).expect("dir");
    let mut manifest = ShardManifest::create(&shard_dir.join("shard-0.manifest")).expect("m");
    let kvs = keys();
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
    let batch = build_kv_spill_batch(&entries, OLD).expect("batch");
    let byte_size = write_kv_spill_batch(shard_dir, OLD, &batch).expect("write");
    manifest
        .add_file(FileEntry {
            file_id: OLD,
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
    manifest.commit().expect("commit");
    drop(manifest);
    let mut log = serialize_cold_cut(1).to_vec();
    for (k, v) in &kvs {
        log.extend_from_slice(&resp(&[b"SET", k.as_bytes(), v.as_bytes()]));
    }
    let mut marker: Vec<&[u8]> = vec![b"MOON.SPILLED", b"5"];
    marker.extend(kvs.iter().map(|(k, _)| k.as_bytes()));
    log.extend_from_slice(&resp(&marker));
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
    for (k, _) in kvs.iter().take(8) {
        assert!(db.remove_counting_cold(k.as_bytes()).0);
    }
    db
}

fn file_of(key: &str) -> Option<u64> {
    with_shard_db(0, |db| {
        db.cold_index
            .as_ref()
            .and_then(|ci| ci.lookup(key.as_bytes()))
            .map(|l| l.file_id)
    })
}

fn count(f: impl Fn(&crate::storage::tiered::cold_index::ColdIndex) -> usize) -> usize {
    with_shard_db(0, |db| db.cold_index.as_ref().map_or(0, f))
}

#[test]
fn the_reclaim_tick_never_does_the_disk_io_itself() {
    // The injected fsync delay lives on this test's own manifest, but the
    // sync agent registers in the process-wide registry other tests assert on.
    #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
    let _registry = crate::persistence::manifest::TEST_AGENT_REGISTRY_LOCK
        .lock()
        .unwrap();
    std::thread::spawn(|| {
        let tmp = tempfile::tempdir().expect("tempdir");
        let shard_dir = tmp.path().join("shard-0");
        let live = fixture(tmp.path(), &shard_dir);
        let (shared, mut inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        init_shard(ShardSlice::new(inits.remove(0)));
        with_shard_db(0, |db| *db = live);

        let mut manifest =
            Some(ShardManifest::open(&shard_dir.join("shard-0.manifest")).expect("manifest"));
        if let Some(m) = manifest.as_mut() {
            m.enable_deferred_sync(0);
        }
        let spill = SpillThread::new(0);
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level(tx);
        let runtime_config = Arc::new(parking_lot::RwLock::new(
            crate::config::RuntimeConfig::default(),
        ));
        let mut next = FIRST_NEW;
        // A ledger far over the no-maxmemory threshold: compaction is due.
        // Returns how long the tick took.
        let tick = |manifest: &mut Option<ShardManifest>, next: &mut u64| {
            let t0 = Instant::now();
            cold_reclaim_tick::run(
                &shared,
                0,
                &runtime_config,
                manifest,
                next,
                Some(&shard_dir),
                Some(&pool),
                Some(&spill),
                usize::MAX,
            );
            t0.elapsed()
        };

        // One tick: the read is handed to the spill thread; nothing is
        // compacted, written or recorded before the tick returns.
        let _ = tick(&mut manifest, &mut next);
        assert_eq!(
            count(|ci| ci.pending_compactions()),
            0,
            "the tick compacted inline"
        );
        assert_eq!(count(|ci| ci.compactions_in_flight()), 1);
        assert!(!heap(&shard_dir, FIRST_NEW).exists(), "nothing written yet");

        // The pipeline completes on later ticks.
        let deadline = Instant::now() + Duration::from_secs(10);
        while count(|ci| ci.pending_compactions()) == 0 {
            assert!(Instant::now() < deadline, "the compaction never completed");
            std::thread::sleep(Duration::from_millis(10));
            let _ = tick(&mut manifest, &mut next);
        }
        assert!(heap(&shard_dir, FIRST_NEW).exists(), "the survivors' file");
        assert!(
            !crate::storage::tiered::reclaim_io::ran_reclaim_io(std::thread::current().id()),
            "the compaction's read or write ran on the shard thread"
        );
        assert_eq!(
            file_of("k08"),
            Some(OLD),
            "nothing re-pointed before adoption"
        );

        // A fold cut after the compaction commits.
        let overflow = pool.overflow_for(0);
        let floor = overflow.advance_epoch();
        let _ = FoldOutcome::Committed { floor }.adopt(FoldEpoch::INITIAL, overflow);

        // Adoption with a slow manifest fsync: the listing is in flight, the
        // survivors stay where they are until it is durable.
        if let Some(m) = manifest.as_mut() {
            m.set_inject_sync_delay_ms(SYNC_DELAY_MS);
        }
        let mut slowest = tick(&mut manifest, &mut next);
        assert_eq!(count(|ci| ci.adoptions_in_flight()), 1);
        assert_eq!(file_of("k08"), Some(OLD), "re-pointed before durable");
        assert!(heap(&shard_dir, OLD).exists());
        let deadline = Instant::now() + Duration::from_secs(10);
        while count(|ci| ci.adoptions_in_flight()) > 0 {
            assert!(Instant::now() < deadline, "the adoption never finished");
            std::thread::sleep(Duration::from_millis(10));
            slowest = slowest.max(tick(&mut manifest, &mut next));
        }
        assert!(
            slowest < TICK_BUDGET,
            "a reclaim tick waited {slowest:?} with a {SYNC_DELAY_MS} ms manifest fsync"
        );
        for k in ["k08", "k09"] {
            assert_eq!(file_of(k), Some(FIRST_NEW), "{k} moved to the new file");
        }
        assert!(!heap(&shard_dir, OLD).exists(), "the old file is unlinked");
        if let Some(m) = manifest.as_mut() {
            m.set_inject_sync_delay_ms(0);
            m.shutdown_deferred();
        }
        let reopened = ShardManifest::open(&shard_dir.join("shard-0.manifest")).expect("reopen");
        assert!(listed(&reopened, FIRST_NEW) && !listed(&reopened, OLD));
        let _ = spill.shutdown();
    })
    .join()
    .expect("test thread");
}

/// Refs moon#1265 (review 5, N6): a spill thread that died never answers the
/// compactions it was given. They pinned the reclaim's in-flight set — and
/// with it `MAX_IN_FLIGHT` — for good, so the shard never compacted again.
/// The tick now abandons them once it sees the thread dead, and starts no
/// new compaction on it.
#[test]
fn a_dead_spill_threads_compactions_are_abandoned() {
    std::thread::spawn(|| {
        let tmp = tempfile::tempdir().expect("tempdir");
        let shard_dir = tmp.path().join("shard-0");
        let live = fixture(tmp.path(), &shard_dir);
        let (shared, mut inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        init_shard(ShardSlice::new(inits.remove(0)));
        with_shard_db(0, |db| *db = live);
        let mut manifest =
            Some(ShardManifest::open(&shard_dir.join("shard-0.manifest")).expect("manifest"));
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level(tx);
        let runtime_config = Arc::new(parking_lot::RwLock::new(
            crate::config::RuntimeConfig::default(),
        ));
        // A compaction whose job went to the thread before it died.
        with_shard_db(0, |db| {
            let ci = db.cold_index.as_mut().expect("cold index");
            assert!(ci.start_compaction(OLD));
        });
        assert_eq!(count(|ci| ci.compactions_in_flight()), 1);

        let dead = SpillThread::exited_for_test();
        let mut next = FIRST_NEW;
        cold_reclaim_tick::run(
            &shared,
            0,
            &runtime_config,
            &mut manifest,
            &mut next,
            Some(&shard_dir),
            Some(&pool),
            Some(&dead),
            usize::MAX,
        );
        assert_eq!(
            count(|ci| ci.compactions_in_flight()),
            0,
            "the dead thread's compaction still pins the in-flight set"
        );
        assert_eq!(next, FIRST_NEW, "no new compaction on a dead thread");
        assert!(heap(&shard_dir, OLD).exists() && file_of("k08") == Some(OLD));
    })
    .join()
    .expect("test thread");
}
