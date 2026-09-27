//! Review round 2b (moon#1265): a fault confined to ONE cold-reclaim job must
//! not degrade the shard's spilling.
//!
//! moon#1240 made the spill thread also run cold-reclaim I/O, and the issue
//! names "a corrupt spill file read by cold reclaim" as a new panic source.
//! Such a fault is deterministic: the same file panics the thread every time
//! its compaction runs. The reconcile abandons the compaction but does NOT
//! give the file up ("the files are not given up, so the next incarnation
//! compacts them", `cold_reclaim_tick`), so every respawn re-submits the same
//! job, dies again, and after 5 respawns (~3.1 s of backoff) the shard is
//! DEGRADED for the life of the process: spilling stops, evicting policies
//! plain-drop keys the operator configured to spill, and cold reclaim stops.
//!
//! Expected: after a death while a compaction was in flight, that file is
//! given up (`abandon_compaction(file, true)` / the reclaim skip set) at the
//! latest on the second such death, and spilling stays up.
//!
//! Red at ad55d3d on both runtimes: the shard degrades.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::reclaim_offload_tests::{FIRST_NEW, fixture};
use super::*;
use crate::persistence::aof::{AofMessage, AofWriterPool};
use crate::persistence::manifest::ShardManifest;
use crate::shard::slice::{ShardSlice, init_shard, with_shard_db};
use crate::storage::Database;
use crate::storage::tiered::spill_thread::fault::{PanicPlan, PanicPoint};
use crate::storage::tiered::spill_thread::{Respawn, SpillThread};

fn sink() -> ColdMarkerSink<'static> {
    ColdMarkerSink {
        aof_pool: None,
        wal_writer: None,
        shard_id: 0,
        wal_kv_log: false,
    }
}

#[test]
fn a_poisoned_reclaim_job_does_not_degrade_spilling() {
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
        // The poison: every reclaim WRITE of this shard panics (the fixture
        // has exactly one compactable file, so this is "one bad file").
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
        let deadline = Instant::now() + Duration::from_secs(40);
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
            // Survived 2 s with a live thread: the poison was given up.
            if !st.is_dead() && quiet_since.elapsed() > Duration::from_secs(2) {
                break;
            }
            assert!(Instant::now() < deadline, "neither degraded nor settled");
            std::thread::sleep(Duration::from_millis(5));
        }
        let degraded = st.is_degraded();
        let _ = st.shutdown();
        assert!(
            !degraded,
            "one poisoned reclaim file DEGRADED the shard's spilling after {respawns} \
             respawns: the file was never given up, so every respawn re-ran the same \
             compaction and died again"
        );
    })
    .join()
    .unwrap();
}

/// Review round 2b (moon#1265, MINOR): the restart budget is kept on the
/// shard's cached WALL clock, and `forget_before` drops every attempt stamped
/// in the "future". A wall clock stepped back (NTP step, VM restore) therefore
/// erases the crash-loop history: a sixth death inside the same 10 minutes of
/// real time respawns instead of degrading, and every further step-back
/// grants 5 more. The supervisor is pure and clock-injected, so a monotonic
/// clock (the shard's `tick_cadence::LoopClock`) would close this.
#[test]
fn a_wall_clock_step_back_does_not_reset_the_restart_budget() {
    use crate::storage::tiered::spill_thread::supervisor::{
        RestartPolicy, RestartSupervisor, Verdict,
    };
    let mut s = RestartSupervisor::new(RestartPolicy::DEFAULT, 1_000_000);
    let mut now = 1_000_000u64;
    for _ in 0..5 {
        match s.on_death(now) {
            Verdict::RespawnAt(due) => {
                now = due;
                s.on_respawned(now);
                now += 1;
            }
            Verdict::Degrade => panic!("degraded before the budget was spent"),
        }
    }
    // Five respawns in ~3 s of real time. The wall clock now steps back by a
    // minute, and the thread dies a sixth time, a moment later in real time.
    let stepped_back = now - 60_000;
    assert_eq!(
        s.on_death(stepped_back),
        Verdict::Degrade,
        "a 60 s wall-clock step back erased 5 respawns from the 10-minute budget"
    );
}
