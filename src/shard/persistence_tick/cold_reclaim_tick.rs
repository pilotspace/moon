//! The shard tick's half of the cold-tier reclaim (moon#1215 ledger bound,
//! PR #1233 review): adopt the compactions a committed fold made safe, then,
//! while the shard's dead-slot ledger is over its share, compact a few more
//! mostly-dead spill files. The mechanism and its crash-window argument are
//! in `storage::tiered::cold_reclaim`.
//!
//! moon#1240: none of the reclaim's disk I/O runs here any more. Reads and
//! durable writes go to the shard's spill thread (`storage::tiered::reclaim_io`)
//! and come back as answers this tick applies; the adoption's manifest commit
//! is handed to the manifest-sync thread and its ack polled on a later tick.
//! What this thread does is choose, filter and re-point — in memory.
//!
//! moon#1297: without an AOF the same tick runs the pipeline with a committed
//! snapshot as the commit point (`storage::tiered::cold_reclaim::no_aof`):
//! compactions are stamped with the shard's snapshot epoch when recorded,
//! adopted once a snapshot that started after them has committed, and
//! started from the grave record's dead-slot counts instead of the ledger.

use std::path::Path;
use std::sync::Arc;

use crate::persistence::aof::AofWriterPool;
use crate::persistence::manifest::ShardManifest;
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::cold_reclaim::CulpritOutcome;
use crate::storage::tiered::cold_reclaim::test_hooks::{
    ReclaimCrashPoint, compaction_held_for_test, crash_point,
};
use crate::storage::tiered::reclaim_io::{ReclaimDone, ReclaimJob};
use crate::storage::tiered::snapshot_hold;
use crate::storage::tiered::spill_thread::SpillThread;

/// Files whose compaction starts per shard tick at most. The disk work runs
/// on the spill thread and competes with spills for the device, so this
/// paces it: 2 per 100 ms tick is the ~20 files/s the reclaim was measured
/// at (PR #1233 review) when it still ran here.
const FILES_PER_TICK: usize = 2;
/// Compactions of one shard on the spill thread at once, at most.
const MAX_IN_FLIGHT: usize = 8;
/// Compactions one database may have waiting for a fold. Their outputs are
/// unlisted files on disk until adoption; this bounds them.
const MAX_PENDING_PER_DB: usize = 256;
/// The ledger's share of the shard budget before reclaim starts.
const LEDGER_SHARE_DIVISOR: usize = 4;
/// Without `maxmemory` the ledger is still RAM; reclaim past this.
const LEDGER_CEILING_UNLIMITED: usize = 64 << 20;

// ── No-AOF starts (R2b5: the moon#1297 regression) ───────────────────────
//
// Without an AOF a compaction waits for a SNAPSHOT that started after it —
// with no save rule firing, minutes (three orphan sweeps, then a requested
// snapshot). Its record (every survivor's key and both locations, ~110 B a
// survivor) is charged to write admission with the dead-slot ledger until
// then (`ColdIndex::dead_slot_bytes`), and eviction cannot free a byte of it.
// Bounded only by a count (64 per database), a write flood at `maxmemory`
// (WS43 bench: 600 B values, `--shards 4`, 8 MB) ran ~190 compactions up to
// 4.6 MB of records in three seconds: past half a shard's budget, where
// `evict_to_budget` answers -OOM instead of draining the hot set, and every
// one of them read and rewrote a file on the spill threads the shards were
// waiting on (-22% SET throughput, +21% CPU per op at 4 vCPUs). Two rules:
//
// - RAM: a shard starts a compaction only while its records (plus an
//   estimate for those in flight) stay under `1/NO_AOF_RAM_SHARE_DIVISOR`
//   of its budget, so they can never come near the half that refuses a
//   write, and lower the eviction target by at most that share.
// - CPU: while the shard is spilling (it minted a spill file since the
//   previous tick: eviction is under way and the spill thread is busy), at
//   most one compaction starts per `NO_AOF_START_SPACING_WHILE_SPILLING`.
//   Once the writes stop, compaction runs at the full pace again.

/// A shard's no-AOF compaction records stay under this share of its budget.
const NO_AOF_RAM_SHARE_DIVISOR: usize = 16;
/// No-AOF compactions of one shard on the spill thread at once, at most.
const NO_AOF_MAX_IN_FLIGHT: usize = 2;
/// While the shard spills, one no-AOF compaction start per this long.
const NO_AOF_START_SPACING_WHILE_SPILLING: std::time::Duration = std::time::Duration::from_secs(1);

thread_local! {
    /// The shard's spill-file counter after the previous tick's reclaim
    /// minted its own ids: a higher counter at the next tick means
    /// something spilled in between.
    static SPILL_MARK: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    /// When this shard last started a no-AOF compaction.
    static LAST_NO_AOF_START: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

/// A shard's no-AOF reclaim at one tick, for [`no_aof_starts`].
#[derive(Debug, Clone, Copy)]
pub(super) struct NoAofLoad {
    /// RAM its compaction records hold (pending and being adopted).
    pub(super) record_bytes: usize,
    /// Compactions pending adoption.
    pub(super) pending: usize,
    /// Compactions with a job on the spill thread.
    pub(super) in_flight: usize,
    /// It minted a spill file since the previous tick.
    pub(super) spilling: bool,
}

/// The RAM cap of a shard's no-AOF compaction records: its share of the
/// per-shard budget, none without `maxmemory` (no write is refused there).
pub(super) fn no_aof_record_cap(maxmemory: usize, per_shard_budget: usize) -> usize {
    if maxmemory == 0 {
        usize::MAX
    } else {
        per_shard_budget / NO_AOF_RAM_SHARE_DIVISOR
    }
}

/// How many no-AOF compactions this shard may start now (see the rules
/// above): none while its records, with an average record for each one in
/// flight, reach `cap`, or while [`NO_AOF_MAX_IN_FLIGHT`] are in flight; one
/// per [`NO_AOF_START_SPACING_WHILE_SPILLING`] while it spills; otherwise up
/// to [`FILES_PER_TICK`].
pub(super) fn no_aof_starts(
    load: NoAofLoad,
    cap: usize,
    last_start: Option<std::time::Instant>,
    now: std::time::Instant,
) -> usize {
    if load.in_flight >= NO_AOF_MAX_IN_FLIGHT {
        return 0;
    }
    let per_record = load.record_bytes.checked_div(load.pending).unwrap_or(0);
    let committed = load
        .record_bytes
        .saturating_add(per_record.saturating_mul(load.in_flight));
    if committed >= cap {
        return 0;
    }
    if load.spilling {
        let spaced = last_start.is_none_or(|t| {
            now.saturating_duration_since(t) >= NO_AOF_START_SPACING_WHILE_SPILLING
        });
        return usize::from(spaced);
    }
    FILES_PER_TICK.min(NO_AOF_MAX_IN_FLIGHT - load.in_flight)
}

/// The ledger size past which this shard compacts: a quarter of its memory
/// budget, or [`LEDGER_CEILING_UNLIMITED`] with no `maxmemory`.
pub(super) fn reclaim_threshold(maxmemory: usize, per_shard_budget: usize) -> usize {
    if maxmemory == 0 {
        LEDGER_CEILING_UNLIMITED
    } else {
        per_shard_budget / LEDGER_SHARE_DIVISOR
    }
}

/// What commits a compaction on this shard.
#[derive(Clone, Copy)]
enum CommitPoint<'a> {
    /// A committed AOF fold (the ledger bound, moon#1215).
    Fold(&'a crate::persistence::aof::rewrite::RewriteOverflow),
    /// A committed snapshot: no AOF writer in this process (moon#1297).
    Snapshot,
}

/// One tick of reclaim. `ledger_bytes` is the shard's ledger as this tick
/// published it. A no-op without disk offload, or in the legacy multi-shard
/// TopLevel AOF layout, where one fold epoch spans several shards and so
/// cannot say which shard's compaction a commit covers. With an AOF writer a
/// committed fold commits a compaction and the ledger drives it; without one
/// (moon#1297) a committed snapshot commits it and the grave record drives
/// it. Without a spill thread no new compaction starts (adoption still runs).
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<crate::config::RuntimeConfig>>,
    shard_manifest: &mut Option<ShardManifest>,
    next_file_id: &mut u64,
    offload_shard_dir: Option<&Path>,
    aof_pool: Option<&Arc<AofWriterPool>>,
    spill_thread: Option<&SpillThread>,
    ledger_bytes: usize,
) {
    let (Some(shard_dir), Some(manifest)) = (offload_shard_dir, shard_manifest.as_mut()) else {
        return;
    };
    let commit_point = match aof_pool {
        Some(pool) => {
            if pool.layout() == crate::persistence::aof_manifest::AofLayout::TopLevel
                && shard_databases.num_shards() > 1
            {
                return;
            }
            CommitPoint::Fold(pool.overflow_for(shard_id))
        }
        None if snapshot_hold::applies() => CommitPoint::Snapshot,
        None => return,
    };
    let committed = match commit_point {
        CommitPoint::Fold(overflow) => overflow.committed_floor().0,
        CommitPoint::Snapshot => snapshot_hold::snapshot_fold_view(0).committed_floor,
    };
    let no_aof = matches!(commit_point, CommitPoint::Snapshot);
    let db_count = shard_databases.db_count();
    // R2b5: did anything spill since the previous tick (the counter moved
    // past what that tick's reclaim left it at)?
    let spilling = SPILL_MARK.get().is_some_and(|mark| *next_file_id > mark);
    // moon#1265: sampled before the answers are drained, so everything a
    // dead thread sent is applied below before its other jobs are abandoned.
    // "Dead" is any tick with no thread running: just died, in backoff before
    // its respawn (which runs after this tick, `spill_supervise`), or
    // degraded.
    let dead = spill_thread.is_some_and(SpillThread::is_dead);

    // 1. Apply what the spill thread finished: a read becomes a write job
    //    (survivors filtered, output ids minted and the fold epoch stamped
    //    in this one synchronous section), a write becomes a pending
    //    compaction. Without an AOF the stamp is the snapshot epoch at the
    //    record instead (moon#1297): the adopting snapshot must START after
    //    the compaction is known, so its trailer can name the compacted
    //    slots of the survivors that changed.
    if let Some(st) = spill_thread {
        let stamps = Stamps {
            plan: &|| match commit_point {
                CommitPoint::Fold(overflow) => overflow.stamp().0,
                CommitPoint::Snapshot => 0,
            },
            record: &|planned| match commit_point {
                CommitPoint::Fold(_) => planned,
                CommitPoint::Snapshot => snapshot_hold::epoch_before_start(),
            },
        };
        for done in st.drain_reclaim_done() {
            apply_done(done, st, shard_id, shard_dir, next_file_id, &stamps, no_aof);
        }
    }
    // Nothing below mints a file id: the next tick compares against this.
    SPILL_MARK.set(Some(*next_file_id));
    // moon#1265: a dead spill thread answers nothing more. Abandon what it
    // still held (its queued jobs are dropped by the shard's reconcile; a
    // write it finished unannounced is an unlisted file the startup orphan
    // sweep removes) and start nothing new until the respawn. The files are
    // not given up, so the next incarnation compacts them — except the one
    // whose job the thread was running as it died, the second time that
    // happens to it (review: a file that panics the thread deterministically
    // would otherwise crash every respawn). Such deaths do not spend the
    // spill thread's restart budget but the reclaim's own, which disables
    // the reclaim below once spent (review round 3).
    let spill_thread = if dead {
        let culprit = spill_thread.and_then(SpillThread::take_reclaim_culprit);
        for db_index in 0..db_count {
            crate::shard::slice::with_shard_db(db_index, |db| {
                let Some(ci) = db.cold_index.as_mut() else {
                    return;
                };
                match ci.abandon_compactions_after_thread_death(culprit) {
                    CulpritOutcome::NotHere => {}
                    CulpritOutcome::Suspected(file_id) => tracing::info!(
                        shard_id,
                        db = db_index,
                        file_id,
                        "cold reclaim: the spill thread died compacting this file; it is \
                         retried once, and given up if it kills the thread again (moon#1265)"
                    ),
                    CulpritOutcome::GivenUp(file_id) => tracing::warn!(
                        shard_id,
                        db = db_index,
                        file_id,
                        "cold reclaim: file given up — its compaction killed the spill thread \
                         twice. It stays as it is (its keys readable, its dead slots in the \
                         ledger) and is not compacted again until restart (INFO \
                         cold_reclaim_files_given_up, moon#1265)"
                    ),
                }
            });
        }
        None
    } else {
        spill_thread
    };

    // 2. Adoption: finish every listing whose commit is known, then list what
    //    a committed fold made safe. Neither waits for an fsync.
    for db_index in 0..db_count {
        crate::shard::slice::with_shard_db(db_index, |db| {
            let Some(ci) = db.cold_index.as_mut() else {
                return;
            };
            if ci.adoptions_in_flight() > 0 {
                let r = ci.finish_adoptions(shard_dir, manifest);
                if r.keys_moved > 0 || r.files_unlinked > 0 {
                    tracing::info!(
                        shard_id,
                        db = db_index,
                        keys_moved = r.keys_moved,
                        files_unlinked = r.files_unlinked,
                        bytes_unlinked = r.bytes_unlinked,
                        commit_point = if no_aof { "snapshot" } else { "AOF fold" },
                        "cold reclaim: adopted compacted spill files after their commit point (a committed AOF \
                         fold, or a snapshot without an AOF)"
                    );
                }
            }
            if no_aof && ci.compactions_ready(committed) > 0 {
                // moon#1297 kill point: the snapshot committed, nothing listed.
                crash_point(ReclaimCrashPoint::AdoptReady);
            }
            if ci.pending_compactions() > 0 {
                let r = ci.begin_adoption(committed, shard_dir, manifest);
                if r.compactions > 0 {
                    tracing::debug!(
                        shard_id,
                        db = db_index,
                        compactions = r.compactions,
                        files_listed = r.files_listed,
                        "cold reclaim: listing compacted spill files (commit in flight)"
                    );
                }
            }
        });
    }

    // 3. Compact while the ledger is over its share. Held spill files
    //    (moon#1231) cannot be compacted — they have no live key — so a
    //    database whose held files wait for a fold asks for one meanwhile.
    let threshold = {
        let rt = runtime_config.read();
        reclaim_threshold(rt.maxmemory, rt.maxmemory_per_shard())
    };
    let over = ledger_bytes > threshold;
    let mut load = NoAofLoad {
        record_bytes: 0,
        pending: 0,
        in_flight: 0,
        spilling,
    };
    for db_index in 0..db_count {
        crate::shard::slice::with_shard_db(db_index, |db| {
            if let Some(ci) = db.cold_index.as_mut() {
                if !no_aof {
                    ci.note_held_files_pressure(over, committed);
                }
                load.in_flight += ci.compactions_in_flight();
                load.pending += ci.pending_compactions();
                load.record_bytes += ci.reclaim_resident_bytes();
            }
        });
    }
    let in_flight = load.in_flight;
    // moon#1265 review round 3: a shard whose reclaim jobs spent the
    // reclaim-death budget starts no compaction any more (adoption of those
    // already written still runs above).
    let Some(st) = spill_thread.filter(|st| !st.reclaim_disabled()) else {
        return;
    };
    // Without an AOF there is no ledger: each database's grave record says
    // whether it has enough dead slots to compact (moon#1297).
    if !(over || no_aof) || (no_aof && compaction_held_for_test()) {
        return;
    }
    let max_pending = if no_aof {
        crate::storage::tiered::cold_reclaim::NO_AOF_MAX_PENDING_PER_DB
    } else {
        MAX_PENDING_PER_DB
    };
    let now = std::time::Instant::now();
    let mut files_left = if no_aof {
        let cap = {
            let rt = runtime_config.read();
            no_aof_record_cap(rt.maxmemory, rt.maxmemory_per_shard())
        };
        let starts = no_aof_starts(load, cap, LAST_NO_AOF_START.get(), now);
        if starts == 0 {
            tracing::debug!(
                shard_id,
                record_bytes = load.record_bytes,
                cap,
                in_flight = load.in_flight,
                spilling = load.spilling,
                "cold reclaim: no-AOF compaction start deferred (records at their budget share, \
                 jobs in flight, or the shard is spilling)"
            );
        }
        starts
    } else {
        FILES_PER_TICK.min(MAX_IN_FLIGHT.saturating_sub(in_flight))
    };
    let allowed = files_left;
    for db_index in 0..db_count {
        if files_left == 0 {
            break;
        }
        crate::shard::slice::with_shard_db(db_index, |db| {
            let Some(ci) = db.cold_index.as_mut() else {
                return;
            };
            let busy = ci.pending_compactions() + ci.compactions_in_flight();
            let room = files_left.min(max_pending.saturating_sub(busy));
            let candidates = if no_aof {
                ci.reclaim_candidates_no_aof(room)
            } else {
                ci.reclaim_candidates(room)
            };
            start_reads(ci, st, db_index, candidates, shard_dir, &mut files_left);
        });
    }
    if no_aof && files_left < allowed {
        LAST_NO_AOF_START.set(Some(now));
    }
}

/// Send a `Read` job for each of `candidates` (step 3 of [`run`]), counting
/// them off `files_left`; a full queue ends this tick's starts.
fn start_reads(
    ci: &mut ColdIndex,
    st: &SpillThread,
    db_index: usize,
    candidates: Vec<u64>,
    shard_dir: &Path,
    files_left: &mut usize,
) {
    for file_id in candidates {
        if !ci.start_compaction(file_id) {
            continue;
        }
        let job = ReclaimJob::Read {
            db_index,
            file_id,
            shard_dir: shard_dir.to_path_buf(),
        };
        if st.try_submit_reclaim(job).is_err() {
            // Queue full or thread gone: try again on a later tick.
            ci.abandon_compaction(file_id, false);
            *files_left = 0;
            return;
        }
        *files_left -= 1;
    }
}

/// The epoch a compaction is stamped with: `plan` when its output ids are
/// minted (carried by the write job), `record` maps that to the stamp it is
/// recorded under when the write comes back.
struct Stamps<'a> {
    plan: &'a dyn Fn() -> u64,
    record: &'a dyn Fn(u64) -> u64,
}

/// Apply one answer from the spill thread (step 1 of [`run`]).
fn apply_done(
    done: ReclaimDone,
    st: &SpillThread,
    shard_id: usize,
    shard_dir: &Path,
    next_file_id: &mut u64,
    stamps: &Stamps<'_>,
    no_aof: bool,
) {
    match done {
        ReclaimDone::Read {
            db_index,
            file_id,
            result,
        } => {
            let write = crate::shard::slice::with_shard_db(db_index, |db| {
                let ci = db.cold_index.as_mut()?;
                let slots = match result {
                    Ok(slots) => slots,
                    Err(why) => {
                        ci.abandon_compaction(file_id, true);
                        warn_not_compacted(shard_id, db_index, file_id, &why);
                        return None;
                    }
                };
                match ci.plan_compaction(file_id, db_index, slots, shard_dir, next_file_id) {
                    Ok(Some(plan)) => Some(ReclaimJob::Write {
                        db_index,
                        old_file: file_id,
                        // The fold epoch at the instant the output ids were
                        // minted (same synchronous section): adoption waits
                        // for a committed fold cut after them.
                        epoch: (stamps.plan)(),
                        moved: plan.moved,
                        requests: plan.requests,
                        shard_dir: shard_dir.to_path_buf(),
                    }),
                    Ok(None) => None,
                    Err(why) => {
                        warn_not_compacted(shard_id, db_index, file_id, &why);
                        None
                    }
                }
            });
            if let Some(job) = write
                && let Err(ReclaimJob::Write { old_file, .. }) = st.try_submit_reclaim(job)
            {
                // Nothing written yet: drop the compaction, retry later.
                crate::shard::slice::with_shard_db(db_index, |db| {
                    if let Some(ci) = db.cold_index.as_mut() {
                        ci.abandon_compaction(old_file, false);
                    }
                });
            }
        }
        ReclaimDone::Written {
            db_index,
            old_file,
            epoch,
            moved,
            result,
        } => {
            let epoch = (stamps.record)(epoch);
            let recorded =
                crate::shard::slice::with_shard_db(db_index, |db| match db.cold_index.as_mut() {
                    Some(ci) => ci.record_compaction(old_file, epoch, moved, result, shard_dir),
                    None => {
                        if let Ok(completions) = &result {
                            for c in completions {
                                crate::storage::tiered::reclaim_io::discard_output(
                                    shard_dir,
                                    c.file_entry.file_id,
                                );
                            }
                        }
                        Err("the database has no cold index any more".to_string())
                    }
                });
            match recorded {
                Err(why) => warn_not_compacted(shard_id, db_index, old_file, &why),
                // moon#1297 kill point: `F'` written and fsynced, unlisted.
                Ok(()) if no_aof => crash_point(ReclaimCrashPoint::Compacted),
                Ok(()) => {}
            }
        }
    }
}

fn warn_not_compacted(shard_id: usize, db_index: usize, file_id: u64, why: &str) {
    tracing::warn!(
        shard_id,
        db = db_index,
        file_id,
        why = %why,
        "cold reclaim: file not compacted"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn load(record_bytes: usize, pending: usize, in_flight: usize, spilling: bool) -> NoAofLoad {
        NoAofLoad {
            record_bytes,
            pending,
            in_flight,
            spilling,
        }
    }

    /// R2b5: the records can never reach the half of the budget where a
    /// write is refused — a shard stops starting at its sixteenth, counting
    /// an average record for each compaction still in flight.
    #[test]
    fn records_at_their_share_of_the_budget_start_nothing() {
        let now = Instant::now();
        let cap = no_aof_record_cap(8 << 20, 2 << 20);
        assert_eq!(cap, 128 << 10);
        assert_eq!(no_aof_starts(load(0, 0, 0, false), cap, None, now), 2);
        assert_eq!(no_aof_starts(load(cap - 1, 4, 0, false), cap, None, now), 2);
        assert_eq!(no_aof_starts(load(cap, 4, 0, false), cap, None, now), 0);
        // 100 KB in 4 records (25 KB each) + 1 in flight = 125 KB: one more.
        assert_eq!(
            no_aof_starts(load(100 << 10, 4, 1, false), cap, None, now),
            1
        );
        // + 2 in flight would be 150 KB: none (and 2 in flight is the limit).
        assert_eq!(
            no_aof_starts(load(100 << 10, 4, 2, false), cap, None, now),
            0
        );
        assert_eq!(
            no_aof_starts(load(112 << 10, 4, 1, false), cap, None, now),
            0
        );
        // No maxmemory: no write is ever refused, only the in-flight limit.
        assert_eq!(no_aof_record_cap(0, 0), usize::MAX);
    }

    /// R2b5: while the shard spills, one start per second; once it stops,
    /// the full pace again.
    #[test]
    fn a_spilling_shard_starts_one_compaction_per_second() {
        let cap = usize::MAX;
        let t0 = Instant::now();
        assert_eq!(no_aof_starts(load(0, 0, 0, true), cap, None, t0), 1);
        let soon = t0 + Duration::from_millis(500);
        assert_eq!(no_aof_starts(load(0, 0, 0, true), cap, Some(t0), soon), 0);
        let later = t0 + Duration::from_secs(1);
        assert_eq!(no_aof_starts(load(0, 0, 0, true), cap, Some(t0), later), 1);
        assert_eq!(no_aof_starts(load(0, 0, 0, false), cap, Some(t0), soon), 2);
    }
}
