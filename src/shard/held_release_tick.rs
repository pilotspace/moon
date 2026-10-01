//! The shard's half of the held-file trigger (moon#1289): after each cold
//! orphan sweep, tell every database whether its held files are still waiting
//! and, without an AOF, ask for the snapshot that releases them. The rule, the
//! defaults and why they are what they are: `storage::tiered::held_release`.
//!
//! Called from the sweep's own arm on both runtimes, so it adds no timer and
//! inherits the sweep's cadence (`runtime::interval` on tokio,
//! `tick_cadence::Cadence` on monoio).
//!
//! moon#1297: the same sweep asks for the snapshot that commits the no-AOF
//! cold reclaim's compactions when they have waited as long as a held file
//! must ([`request_reclaim_snapshot`]).

use std::cell::Cell;
use std::sync::Arc;

use crate::persistence::aof::AofWriterPool;
use crate::persistence::snapshot_request::{self, SnapshotReason, SnapshotRequest};
use crate::runtime::channel::WatchSender;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::tiered::held_release;

/// Run after `timers::run_cold_orphan_sweep` on this shard.
///
/// With an AOF a stale database is only counted (`held_release::stale_databases`):
/// the auto-rewrite monitor's fold answers it. Without one this shard requests
/// a snapshot, which the process-wide gate lets through once per
/// `held_release::spacing`.
pub(crate) fn after_sweep(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    aof_pool: Option<&Arc<AofWriterPool>>,
    spill_file_id: &Cell<u64>,
    snapshot_trigger: &WatchSender<u64>,
    sweep_interval_secs: u64,
) {
    held_release::configure(sweep_interval_secs);
    // Same rule as the sweep's own view: a TopLevel layout with several
    // shards has no committed fold that covers this shard.
    let floor_covers_this_shard = aof_pool.is_none_or(|pool| {
        pool.layout() != crate::persistence::aof_manifest::AofLayout::TopLevel
            || shard_databases.num_shards() <= 1
    });
    let view = crate::shard::timers::fold_view_now(
        shard_id,
        aof_pool,
        floor_covers_this_shard,
        spill_file_id.get(),
    );
    let mut stale = false;
    for db_idx in 0..shard_databases.db_count() {
        crate::shard::slice::with_shard_db(db_idx, |db| {
            if let Some(ci) = db.cold_index.as_mut() {
                stale |= ci.note_held_sweep(view.committed_floor, floor_covers_this_shard);
            }
        });
    }
    if aof_pool.is_some() {
        return;
    }
    request_reclaim_snapshot(
        shard_databases,
        shard_id,
        snapshot_trigger,
        view.committed_floor,
    );
    if !stale {
        return;
    }
    match snapshot_request::request(
        snapshot_trigger,
        crate::command::connection::shard_count(),
        SnapshotReason::HeldColdFiles,
        held_release::spacing(),
    ) {
        SnapshotRequest::Started => tracing::info!(
            shard = shard_id,
            "held cold spill files waited for a snapshot for {} sweeps: requested one to \
             release them (moon#1289)",
            held_release::STALE_AFTER_SWEEPS
        ),
        SnapshotRequest::Refused => tracing::warn!(
            shard = shard_id,
            "held cold spill files need a snapshot to be released, and the server refused to \
             start one (moon#1289); asking again in {:?}",
            held_release::spacing()
        ),
        // A TXN is open somewhere: a snapshot now would keep its uncommitted
        // writes (moon#1300). The slot is untouched, the database stays
        // stale, and the next sweep asks again.
        SnapshotRequest::TxnOpen => tracing::debug!(
            shard = shard_id,
            "held cold spill files: snapshot deferred while a transaction is open (moon#1300)"
        ),
        SnapshotRequest::RateLimited | SnapshotRequest::Busy => {}
    }
}

/// moon#1297: without an AOF a compacted spill file is adopted only once a
/// snapshot that started after its compaction has committed. A save rule
/// usually supplies one; when compactions have waited for
/// `held_release::STALE_AFTER_SWEEPS` sweeps at one committed floor, request
/// one through the shared gate (one per `held_release::spacing` across every
/// reason, so a held-file request already served covers this too).
fn request_reclaim_snapshot(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    snapshot_trigger: &WatchSender<u64>,
    committed_floor: u64,
) {
    let mut awaiting = false;
    for db_idx in 0..shard_databases.db_count() {
        crate::shard::slice::with_shard_db(db_idx, |db| {
            if let Some(ci) = db.cold_index.as_ref() {
                awaiting |= ci.awaits_reclaim_snapshot(committed_floor);
            }
        });
    }
    if !crate::storage::tiered::cold_reclaim::note_reclaim_sweep(awaiting, committed_floor) {
        return;
    }
    match snapshot_request::request(
        snapshot_trigger,
        crate::command::connection::shard_count(),
        SnapshotReason::ColdReclaim,
        held_release::spacing(),
    ) {
        SnapshotRequest::Started => tracing::info!(
            shard = shard_id,
            "compacted cold spill files waited for a snapshot for {} sweeps: requested one to \
             adopt them (moon#1297)",
            held_release::STALE_AFTER_SWEEPS
        ),
        SnapshotRequest::Refused => tracing::warn!(
            shard = shard_id,
            "compacted cold spill files need a snapshot to be adopted, and the server refused \
             to start one (moon#1297); asking again in {:?}",
            held_release::spacing()
        ),
        // Only for a reason that waits for open transactions (moon#1300):
        // the slot is untouched and the next sweep asks again.
        SnapshotRequest::TxnOpen => tracing::debug!(
            shard = shard_id,
            "compacted cold spill files: snapshot deferred while a transaction is open (moon#1300)"
        ),
        SnapshotRequest::RateLimited | SnapshotRequest::Busy => {}
    }
}
