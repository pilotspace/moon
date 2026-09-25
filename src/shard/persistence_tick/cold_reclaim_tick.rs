//! The shard tick's half of the cold-tier reclaim (moon#1215 ledger bound,
//! PR #1233 review): adopt the compactions a committed fold made safe, then,
//! while the shard's dead-slot ledger is over its share, compact a few more
//! mostly-dead spill files. The mechanism and its crash-window argument are
//! in `storage::tiered::cold_reclaim`.

use std::path::Path;
use std::sync::Arc;

use crate::persistence::aof::AofWriterPool;
use crate::persistence::manifest::ShardManifest;

/// Files compacted per shard tick at most — bounds the tick's stall.
const FILES_PER_TICK: usize = 8;
/// Spill-file bytes read per shard tick at most.
const READ_BYTES_PER_TICK: u64 = 8 << 20;
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
/// compaction a commit covers.
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<crate::config::RuntimeConfig>>,
    shard_manifest: &mut Option<ShardManifest>,
    next_file_id: &mut u64,
    offload_shard_dir: Option<&Path>,
    aof_pool: Option<&Arc<AofWriterPool>>,
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

    // 1. Adopt what a committed fold made safe.
    for db_index in 0..db_count {
        let report =
            crate::shard::slice::with_shard_db(db_index, |db| match db.cold_index.as_mut() {
                Some(ci) if ci.pending_compactions() > 0 => {
                    ci.adopt_compactions(committed, shard_dir, manifest)
                }
                _ => Default::default(),
            });
        if report.compactions > 0 {
            tracing::info!(
                shard_id,
                db = db_index,
                compactions = report.compactions,
                files_listed = report.files_listed,
                keys_moved = report.keys_moved,
                files_unlinked = report.files_unlinked,
                bytes_unlinked = report.bytes_unlinked,
                "cold reclaim: adopted compacted spill files after a committed AOF fold"
            );
        }
    }

    // 2. Compact while the ledger is over its share.
    let threshold = {
        let rt = runtime_config.read();
        reclaim_threshold(rt.maxmemory, rt.maxmemory_per_shard())
    };
    if ledger_bytes <= threshold {
        return;
    }
    let epoch = overflow.stamp().0;
    let mut files_left = FILES_PER_TICK;
    let mut bytes_left = READ_BYTES_PER_TICK;
    for db_index in 0..db_count {
        if files_left == 0 || bytes_left == 0 {
            break;
        }
        crate::shard::slice::with_shard_db(db_index, |db| {
            let Some(ci) = db.cold_index.as_mut() else {
                return;
            };
            let room = MAX_PENDING_PER_DB.saturating_sub(ci.pending_compactions());
            for file_id in ci.reclaim_candidates(files_left.min(room)) {
                match ci.compact_file(file_id, db_index, shard_dir, next_file_id, epoch) {
                    Ok(read) => {
                        files_left -= 1;
                        bytes_left = bytes_left.saturating_sub(read);
                    }
                    Err(why) => tracing::warn!(
                        shard_id,
                        db = db_index,
                        file_id,
                        why = %why,
                        "cold reclaim: file not compacted (not retried by this process)"
                    ),
                }
                if files_left == 0 || bytes_left == 0 {
                    break;
                }
            }
        });
    }
}
