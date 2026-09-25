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

use std::path::Path;
use std::sync::Arc;

use crate::persistence::aof::AofWriterPool;
use crate::persistence::manifest::ShardManifest;
use crate::storage::tiered::reclaim_io::{ReclaimDone, ReclaimJob};
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

/// The ledger size past which this shard compacts: a quarter of its memory
/// budget, or [`LEDGER_CEILING_UNLIMITED`] with no `maxmemory`.
pub(super) fn reclaim_threshold(maxmemory: usize, per_shard_budget: usize) -> usize {
    if maxmemory == 0 {
        LEDGER_CEILING_UNLIMITED
    } else {
        per_shard_budget / LEDGER_SHARE_DIVISOR
    }
}

/// One tick of reclaim. `ledger_bytes` is the shard's ledger as this tick
/// published it. A no-op without an AOF writer (no ledger exists then),
/// without disk offload, or in the legacy multi-shard TopLevel layout, where
/// one fold epoch spans several shards and so cannot say which shard's
/// compaction a commit covers. Without a spill thread no new compaction
/// starts (adoption still runs).
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
    let (Some(pool), Some(shard_dir), Some(manifest)) =
        (aof_pool, offload_shard_dir, shard_manifest.as_mut())
    else {
        return;
    };
    if pool.layout() == crate::persistence::aof_manifest::AofLayout::TopLevel
        && shard_databases.num_shards() > 1
    {
        return;
    }
    let overflow = pool.overflow_for(shard_id);
    let committed = overflow.committed_floor().0;
    let db_count = shard_databases.db_count();

    // 1. Apply what the spill thread finished: a read becomes a write job
    //    (survivors filtered, output ids minted and the fold epoch stamped
    //    in this one synchronous section), a write becomes a pending
    //    compaction.
    if let Some(st) = spill_thread {
        for done in st.drain_reclaim_done() {
            apply_done(done, st, shard_id, shard_dir, next_file_id, || {
                overflow.stamp().0
            });
        }
    }

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
                        "cold reclaim: adopted compacted spill files after a committed AOF fold"
                    );
                }
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
    let mut in_flight = 0usize;
    for db_index in 0..db_count {
        crate::shard::slice::with_shard_db(db_index, |db| {
            if let Some(ci) = db.cold_index.as_mut() {
                ci.note_held_files_pressure(over, committed);
                in_flight += ci.compactions_in_flight();
            }
        });
    }
    let Some(st) = spill_thread else {
        return;
    };
    if !over {
        return;
    }
    let mut files_left = FILES_PER_TICK.min(MAX_IN_FLIGHT.saturating_sub(in_flight));
    for db_index in 0..db_count {
        if files_left == 0 {
            break;
        }
        crate::shard::slice::with_shard_db(db_index, |db| {
            let Some(ci) = db.cold_index.as_mut() else {
                return;
            };
            let busy = ci.pending_compactions() + ci.compactions_in_flight();
            let room = MAX_PENDING_PER_DB.saturating_sub(busy);
            for file_id in ci.reclaim_candidates(files_left.min(room)) {
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
                    files_left = 0;
                    break;
                }
                files_left -= 1;
            }
        });
    }
}

/// Apply one answer from the spill thread (step 1 of [`run`]).
fn apply_done(
    done: ReclaimDone,
    st: &SpillThread,
    shard_id: usize,
    shard_dir: &Path,
    next_file_id: &mut u64,
    stamp: impl Fn() -> u64,
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
                        epoch: stamp(),
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
            if let Err(why) = recorded {
                warn_not_compacted(shard_id, db_index, old_file, &why);
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
