//! moon#1265: a spill thread that dies mid-flight, on the shard's tick path
//! (`drain_and_apply` -> `spill_supervise`), with a real spill thread whose
//! panic is injected where it hurts most — after it wrote a batch's file and
//! before it announced it.
//!
//! Invariants proven here:
//! - no in-flight key is LOST: every payload the dead thread held is back in
//!   the hot table, and every request still queued is written by the next
//!   incarnation under its own id and published;
//! - no key is RESURRECTED: a request superseded in flight (DEL, overwrite,
//!   FLUSH) that died with the thread is forgotten, its file stays unlisted
//!   (the startup orphan sweep's), and one still queued keeps its moon#1253
//!   entry until its completion settles it;
//! - no file id is minted twice (moon#1067): the requeued request keeps its
//!   id, nothing reuses the dead batch's;
//! - a reclaim write in flight at the panic loses no key, leaks no listed
//!   file, and the compaction succeeds after the respawn; one that kills the
//!   thread twice is given up, readable as it was, and spilling stays up;
//! - a crash loop degrades the shard: every payload goes back to RAM, the
//!   channel closes, and eviction takes the no-spill path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::reclaim_offload_tests::{FIRST_NEW, OLD, count, file_of, fixture, heap, listed};
use super::spill_supervise::Reconciled;
use super::*;
use crate::persistence::aof::{AofMessage, AofWriterPool};
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileStatus, ShardManifest};
use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
use crate::storage::Database;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::PendingSpill;
use crate::storage::tiered::spill_thread::fault::{PanicPlan, PanicPoint};
use crate::storage::tiered::spill_thread::{SpillRequest, SpillThread};

const V: &[u8] = b"payload";

fn sink() -> ColdMarkerSink<'static> {
    ColdMarkerSink {
        aof_pool: None,
        wal_writer: None,
        shard_id: 0,
        wal_kv_log: false,
    }
}

/// Evict `key` from db `db` as request `id`: mark it in flight (the payload
/// is its only copy) and queue the request, as `evict_one_async_spill` does.
fn evict(st: &SpillThread, db_index: usize, key: &str, id: u64, dir: &std::path::Path) {
    with_shard_db(db_index, |db| {
        db.spill_inflight_mark(
            Bytes::copy_from_slice(key.as_bytes()),
            PendingSpill {
                req_id: id,
                value_type: ValueType::String,
                value_bytes: Bytes::from_static(V),
                ttl_ms: None,
            },
        );
    });
    st.sender()
        .try_send(SpillRequest {
            key: Bytes::copy_from_slice(key.as_bytes()),
            db_index,
            value_bytes: Bytes::from_static(V),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: id,
            shard_dir: dir.to_path_buf(),
        })
        .expect("queued");
}

/// The hot value of `key` in db `db`, if hot.
fn hot(db_index: usize, key: &str) -> Option<Vec<u8>> {
    with_shard_db(db_index, |db| {
        db.data().get(key.as_bytes())?;
        match db.get(key.as_bytes())?.as_redis_value() {
            RedisValueRef::String(s) => Some(s.to_vec()),
            _ => None,
        }
    })
}

fn inflight_ids(db_index: usize) -> Vec<u64> {
    let mut ids: Vec<u64> = with_shard_db(db_index, |db| {
        db.spill_inflight_records().map(|(_, id)| id).collect()
    });
    ids.sort_unstable();
    ids
}

fn wait_dead(st: &SpillThread) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !st.is_dead() {
        assert!(Instant::now() < deadline, "the thread never died");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Tick (drain + reconcile) until `done_below` covers `id`.
fn tick_until_done(
    st: &SpillThread,
    manifest: &mut Option<ShardManifest>,
    db_count: usize,
    id: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        drain_and_apply(st, manifest, &mut sink(), db_count, 0);
        if st.done_below() > id {
            // One more drain: everything the watermark covers is queued.
            drain_and_apply(st, manifest, &mut sink(), db_count, 0);
            return;
        }
        assert!(Instant::now() < deadline, "request {id} never completed");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Wait out the backoff on the spill thread's monotonic supervision clock
/// and respawn.
fn respawn_now(st: &SpillThread) {
    use crate::storage::tiered::spill_thread::Respawn;
    let deadline = Instant::now() + Duration::from_secs(35);
    loop {
        match st.respawn_if_due(st.clock_ms()) {
            Respawn::Respawned => return,
            Respawn::NotDue => {}
            other => panic!("respawn: {other:?}"),
        }
        assert!(Instant::now() < deadline, "the backoff never ended");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_death_mid_flight_loses_nothing_and_resurrects_nothing() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 2)));
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut manifest = Some(ShardManifest::create(&dir.join("shard-0.manifest")).unwrap());
        with_shard_db(0, |db| {
            db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
        });
        let st = SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::AfterWrite, 1)));

        // Taken by the thread that dies: one live, one DELeted in flight
        // (moon#1253 superseded), one overwritten in flight.
        evict(&st, 0, "lost", 1, dir);
        evict(&st, 0, "gone", 2, dir);
        with_shard_db(0, |db| assert!(db.remove_counting_cold(b"gone").0));
        evict(&st, 0, "rewritten", 3, dir);
        with_shard_db(0, |db| {
            db.set_string(b"rewritten", Bytes::from_static(b"new"))
        });
        wait_dead(&st);
        assert!(heap(dir, 1).exists(), "the dead batch's file is on disk");

        // Queued while the thread is down: never seen by any thread.
        evict(&st, 0, "queued", 4, dir);
        evict(&st, 0, "queued-del", 5, dir);
        with_shard_db(0, |db| assert!(db.remove_counting_cold(b"queued-del").0));
        with_shard_db(0, |db| assert_eq!(db.spill_superseded_len(), 3));

        // The tick that sees the death: drain (nothing), reconcile.
        drain_and_apply(&st, &mut manifest, &mut sink(), 2, 0);
        assert_eq!(
            hot(0, "lost").as_deref(),
            Some(V),
            "lost payload back in RAM"
        );
        assert_eq!(
            hot(0, "rewritten").as_deref(),
            Some(&b"new"[..]),
            "newer write kept"
        );
        assert_eq!(hot(0, "gone"), None, "a DEL in flight stays a DEL");
        assert_eq!(
            inflight_ids(0),
            vec![4],
            "only the queued request is in flight"
        );
        with_shard_db(0, |db| {
            let left: Vec<_> = db.spill_superseded_keys_live_at(0).cloned().collect();
            assert_eq!(
                left,
                vec![Bytes::from_static(b"queued-del")],
                "rebuilt from what is queued"
            );
        });
        // Idempotent: another tick while in backoff changes nothing.
        drain_and_apply(&st, &mut manifest, &mut sink(), 2, 0);
        assert_eq!(inflight_ids(0), vec![4]);

        // Respawn: the queued requests are written under their own ids and
        // settle through the ordinary completion path.
        respawn_now(&st);
        tick_until_done(&st, &mut manifest, 2, 5);
        assert!(inflight_ids(0).is_empty());
        with_shard_db(0, |db| {
            assert!(db.spill_superseded_is_empty(), "the queued DEL settled");
            let ci = db.cold_index.as_ref().unwrap();
            assert_eq!(ci.lookup(b"queued").map(|l| l.file_id), Some(4));
            assert!(ci.lookup(b"queued-del").is_none(), "no resurrection");
            assert!(ci.lookup(b"lost").is_none() && ci.lookup(b"gone").is_none());
        });
        let m = manifest.as_ref().unwrap();
        assert!(
            listed(m, 4),
            "the requeued batch is listed under its own id"
        );
        assert!(
            !m.files().iter().any(|f| (1..=3).contains(&f.file_id)),
            "nothing of the dead batch is listed"
        );
        let orphans = crate::storage::tiered::kv_spill::classify_orphan_heap_files(dir, m);
        assert!(
            orphans.iter().any(|p| p.ends_with("heap-000001.mpf")),
            "the dead batch's file is left for the orphan sweep: {orphans:?}"
        );
        let _ = st.shutdown();
    })
    .join()
    .unwrap();
}

/// SWAPDB and FLUSHALL while the thread is down: the reconcile works on the
/// databases as they are now, not as the requests were made.
#[test]
fn swapdb_and_flushall_during_the_restart() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 2)));
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut manifest = Some(ShardManifest::create(&dir.join("shard-0.manifest")).unwrap());
        let st = SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::AfterWrite, 1)));
        evict(&st, 0, "swapped", 1, dir);
        evict(&st, 1, "flushed", 2, dir);
        wait_dead(&st);
        evict(&st, 0, "queued-flushed", 3, dir);
        // SWAPDB 0 1, then FLUSHDB of (new) db 0 = the old db 1 plus the
        // queued request's record.
        let d0 = with_shard_db(0, |db| std::mem::replace(db, Database::new()));
        let d1 = with_shard_db(1, |db| std::mem::replace(db, d0));
        with_shard_db(0, |db| *db = d1);
        with_shard_db(1, |db| {
            // The queued record moved with the swap: supersede it (FLUSH).
            db.spill_inflight_supersede_all();
        });
        with_shard_db(0, |db| db.spill_inflight_supersede_all());

        drain_and_apply(&st, &mut manifest, &mut sink(), 2, 0);
        assert_eq!(
            hot(1, "swapped"),
            None,
            "flushed after the swap: stays gone"
        );
        assert_eq!(hot(0, "flushed"), None);
        with_shard_db(0, |db| assert!(db.spill_superseded_is_empty()));
        with_shard_db(1, |db| {
            let left: Vec<_> = db.spill_superseded_keys_live_at(0).cloned().collect();
            assert_eq!(left, vec![Bytes::from_static(b"queued-flushed")]);
        });
        respawn_now(&st);
        tick_until_done(&st, &mut manifest, 2, 3);
        with_shard_db(1, |db| {
            assert!(db.spill_superseded_is_empty(), "settled after SWAPDB")
        });
        let _ = st.shutdown();
    })
    .join()
    .unwrap();
}

/// A crash loop spends the budget: the degrading reconcile puts every
/// payload — queued ones included — back in RAM, forgets every superseded
/// entry, and closes the channel; eviction then plain-drops under an
/// evicting policy and answers OOM under `noeviction`, as with no spill
/// thread at all.
#[test]
fn a_crash_loop_degrades_the_shard_to_the_no_spill_path() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut manifest = Some(ShardManifest::create(&dir.join("shard-0.manifest")).unwrap());
        // Every incarnation dies as it starts: queued requests are never
        // taken, so they are requeued across all five restarts.
        let st = SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::Start, 1_000)));
        let conn = st.sender();
        wait_dead(&st);
        evict(&st, 0, "a", 1, dir);
        evict(&st, 0, "b", 2, dir);
        with_shard_db(0, |db| assert!(db.remove_counting_cold(b"b").0));
        let mut restarts = 0;
        loop {
            wait_dead(&st);
            drain_and_apply(&st, &mut manifest, &mut sink(), 1, 0);
            if st.is_degraded() {
                break;
            }
            assert_eq!(
                inflight_ids(0),
                vec![1],
                "requeued, not lost, while restarting"
            );
            respawn_now(&st);
            restarts += 1;
            assert!(restarts <= 5, "the budget is 5 restarts per window");
        }
        assert_eq!(restarts, 5);
        assert!(conn.is_disconnected(), "degraded: the channel is closed");
        assert_eq!(hot(0, "a").as_deref(), Some(V), "back in RAM, not lost");
        assert_eq!(hot(0, "b"), None);
        with_shard_db(0, |db| {
            assert!(db.spill_inflight_is_empty() && db.spill_superseded_is_empty());
            assert_eq!(db.pending_spill_bytes(), 0, "nothing pinned");
        });

        // The no-spill path through the closed channel.
        let mut cfg = crate::config::RuntimeConfig {
            maxmemory: 1,
            appendonly: "yes".to_string(),
            ..Default::default()
        };
        let mut fid = 100u64;
        cfg.maxmemory_policy = "noeviction".to_string();
        let r = with_shard_db(0, |db| {
            crate::storage::eviction::evict_to_budget(
                db,
                &cfg,
                crate::storage::eviction::EvictionRun::async_spill(&conn, dir, &mut fid, 0, None),
            )
        });
        assert!(r.is_err(), "noeviction answers OOM");
        assert!(hot(0, "a").is_some());
        cfg.maxmemory_policy = "allkeys-lru".to_string();
        let r = with_shard_db(0, |db| {
            crate::storage::eviction::evict_to_budget(
                db,
                &cfg,
                crate::storage::eviction::EvictionRun::async_spill(&conn, dir, &mut fid, 0, None),
            )
        });
        assert!(
            r.is_ok(),
            "an evicting policy plain-drops instead of -OOM: {r:?}"
        );
        assert_eq!(hot(0, "a"), None, "dropped by the policy, not queued");
        assert_eq!(fid, 100, "no spill id minted");
        let _ = st.shutdown();
    })
    .join()
    .unwrap();
}

/// A cold-reclaim write in flight at the panic: its output is on disk,
/// unannounced. No key moves, nothing is listed, the compaction is
/// abandoned (not given up), and after the respawn it runs again under
/// fresh ids and is adopted — the dead write's output never is.
#[test]
fn a_reclaim_write_in_flight_at_the_panic_loses_no_key_and_leaks_no_listed_file() {
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
        let st = SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::ReclaimWrite, 1)));
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level(tx);
        let runtime_config = Arc::new(parking_lot::RwLock::new(
            crate::config::RuntimeConfig::default(),
        ));
        let mut next = FIRST_NEW;
        let tick = |manifest: &mut Option<ShardManifest>, next: &mut u64| {
            drain_and_apply(&st, manifest, &mut sink(), 1, 0);
            cold_reclaim_tick::run(
                &shared,
                0,
                &runtime_config,
                manifest,
                next,
                Some(&shard_dir),
                Some(&pool),
                Some(&st),
                usize::MAX,
            );
        };

        let deadline = Instant::now() + Duration::from_secs(10);
        while !st.is_dead() {
            assert!(Instant::now() < deadline, "the reclaim write never ran");
            tick(&mut manifest, &mut next);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            heap(&shard_dir, FIRST_NEW).exists(),
            "the dead write's output"
        );
        let dead_output_max = next;
        tick(&mut manifest, &mut next);
        assert_eq!(count(|ci| ci.compactions_in_flight()), 0, "abandoned");
        assert_eq!(count(|ci| ci.pending_compactions()), 0, "nothing recorded");
        for k in ["k08", "k09"] {
            assert_eq!(file_of(k), Some(OLD), "{k} still served from its file");
        }

        respawn_now(&st);
        let deadline = Instant::now() + Duration::from_secs(10);
        while count(|ci| ci.pending_compactions()) == 0 {
            assert!(Instant::now() < deadline, "the compaction never ran again");
            tick(&mut manifest, &mut next);
            std::thread::sleep(Duration::from_millis(5));
        }
        // A committed fold, then adoption.
        let overflow = pool.overflow_for(0);
        let floor = overflow.advance_epoch();
        let _ = crate::persistence::aof::rewrite::FoldOutcome::Committed { floor }
            .adopt(crate::persistence::aof::FoldEpoch::INITIAL, overflow);
        let deadline = Instant::now() + Duration::from_secs(10);
        while file_of("k08") == Some(OLD) || count(|ci| ci.adoptions_in_flight()) > 0 {
            assert!(Instant::now() < deadline, "never adopted");
            tick(&mut manifest, &mut next);
            std::thread::sleep(Duration::from_millis(5));
        }
        let new_file = file_of("k08").expect("k08 still cold");
        assert!(
            new_file >= dead_output_max,
            "fresh ids, none reissued (moon#1067)"
        );
        assert_eq!(file_of("k09"), Some(new_file));
        let m = manifest.as_ref().unwrap();
        assert!(listed(m, new_file));
        assert!(
            !m.files()
                .iter()
                .any(|f| f.file_id == FIRST_NEW && f.status == FileStatus::Active),
            "the dead write's output is never listed"
        );
        let orphans = crate::storage::tiered::kv_spill::classify_orphan_heap_files(&shard_dir, m);
        assert!(
            orphans
                .iter()
                .any(|p| p.ends_with(format!("heap-{FIRST_NEW:06}.mpf"))),
            "left for the orphan sweep: {orphans:?}"
        );
        let _ = st.shutdown();
    })
    .join()
    .unwrap();
}

/// moon#1265 review (MAJOR-2): a reclaim file whose job kills the spill
/// thread every time. The first death retries it; the second gives it up,
/// and the respawned thread then stays up — no third death, no degrade. The
/// file stays exactly as it was: listed, serving its survivors, its dead
/// slots in the ledger.
#[test]
fn a_reclaim_file_that_kills_the_thread_twice_is_given_up_and_stays_readable() {
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
        let st = SpillThread::with_fault(0, Some(PanicPlan::times(PanicPoint::ReclaimWrite, 2)));
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        let pool = AofWriterPool::top_level(tx);
        let runtime_config = Arc::new(parking_lot::RwLock::new(
            crate::config::RuntimeConfig::default(),
        ));
        let mut next = FIRST_NEW;
        let tick = |manifest: &mut Option<ShardManifest>, next: &mut u64| {
            drain_and_apply(&st, manifest, &mut sink(), 1, 0);
            cold_reclaim_tick::run(
                &shared,
                0,
                &runtime_config,
                manifest,
                next,
                Some(&shard_dir),
                Some(&pool),
                Some(&st),
                usize::MAX,
            );
        };
        let ledger = count(|ci| ci.dead_slots().len());
        let given_up = crate::storage::tiered::cold_reclaim::files_given_up_total();

        for death in 1..=2 {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !st.is_dead() {
                assert!(Instant::now() < deadline, "death {death} never came");
                tick(&mut manifest, &mut next);
                std::thread::sleep(Duration::from_millis(5));
            }
            tick(&mut manifest, &mut next);
            assert_eq!(count(|ci| ci.compactions_in_flight()), 0, "abandoned");
            let candidates = count(|ci| ci.reclaim_candidates(usize::MAX).len());
            if death == 1 {
                assert_eq!(candidates, 1, "one death is forgiven: retried");
                assert_eq!(count(|ci| ci.reclaim_suspects()), 1);
            } else {
                assert_eq!(candidates, 0, "two deaths: given up");
                assert!(
                    crate::storage::tiered::cold_reclaim::files_given_up_total() > given_up
                );
            }
            respawn_now(&st);
        }

        // The respawned thread gets no job, so it stays up.
        let settle = Instant::now() + Duration::from_millis(500);
        while Instant::now() < settle {
            tick(&mut manifest, &mut next);
            assert!(!st.is_dead(), "a third death: the file was not given up");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!st.is_degraded());
        assert_eq!(count(|ci| ci.pending_compactions()), 0);
        assert_eq!(count(|ci| ci.dead_slots().len()), ledger, "the ledger as before");
        assert!(listed(manifest.as_ref().expect("manifest"), OLD));
        for (k, v) in [("k08", "v08"), ("k09", "v09")] {
            assert_eq!(file_of(k), Some(OLD), "{k} still indexed in its file");
            let got = with_shard_db(0, |db| db.get_cold_value(k.as_bytes(), 0));
            assert!(
                matches!(&got, Some(crate::storage::entry::RedisValue::String(b)) if b.as_ref() == v.as_bytes()),
                "{k} readable from the given-up file: {got:?}"
            );
        }
        let _ = st.shutdown();
    })
    .join()
    .unwrap();
}

/// The reconcile reports what it did; a second one finds nothing to do.
#[test]
fn the_reconcile_is_idempotent() {
    std::thread::spawn(|| {
        init_shard(ShardSlice::new(make_init(0, 1)));
        let tmp = tempfile::tempdir().unwrap();
        let st = SpillThread::exited_for_test();
        evict(&st, 0, "q", 7, tmp.path());
        with_shard_db(0, |db| {
            db.spill_inflight_mark(
                Bytes::from_static(b"held"),
                PendingSpill {
                    req_id: 6,
                    value_type: ValueType::String,
                    value_bytes: Bytes::from_static(V),
                    ttl_ms: None,
                },
            );
        });
        let first = super::spill_supervise::reconcile(&st, 1, 0, true);
        assert_eq!(
            first,
            Reconciled {
                requeued: 1,
                rehydrated: 1,
                ..Reconciled::default()
            }
        );
        let second = super::spill_supervise::reconcile(&st, 1, 0, true);
        assert_eq!(
            second,
            Reconciled {
                requeued: 1,
                ..Reconciled::default()
            }
        );
        assert_eq!(inflight_ids(0), vec![7]);
        let _ = st.shutdown();
    })
    .join()
    .unwrap();
}
