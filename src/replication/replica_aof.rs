//! A replica's own AOF holds the master's dataset (R2b round 2 R1, moon#1318).
//!
//! Since R2b P1 a replica's AOF is its only KV source at boot, exactly as in
//! redis. Two things made it incomplete, so a PROMOTED replica restarted with
//! almost nothing (reviewer: master 100 keys, replica synced, BGSAVE,
//! `REPLICAOF NO ONE`, `SET promoted 1`, kill -9 -> DBSIZE 1):
//!
//! 1. The full sync's dataset is loaded straight into the keyspace; nothing
//!    wrote it to the AOF. redis's `restartAOFAfterSYNC` starts an AOF
//!    rewrite right after the sync, so the new generation's base is the
//!    synced dataset. [`after_full_sync`] requests that rewrite from the
//!    auto-rewrite monitor ([`crate::persistence::aof::auto_rewrite::request_rewrite`]),
//!    which runs it at once — after a rewrite already running, whose image
//!    may predate the load — and retries it until one completes OK (the fold
//!    protocol keeps it exactly-once against the stream records appended
//!    meanwhile). The embedded server runs no monitor: [`warn_if_no_rewriter`].
//! 2. The master stream was applied with plain dispatch and never logged.
//!    redis replicas feed it into their own AOF (`propagate` /
//!    `feedAppendOnlyFile`). [`log_applied`] appends every applied KV write
//!    and every `MOON.TXN` marker, right after the apply on the shard thread
//!    (no await between them, so the record's fold stamp is the write's
//!    epoch, #455). The markers are the AOF's own transaction records
//!    (moon#1300): a master transaction the replica never saw end is rolled
//!    back by the replay as by the replica at promotion.
//!
//! Not logged: the planes that are not KV history in the AOF (`FT.*`,
//! `GRAPH.*`, `MQ.*` replication effects, `TEMPORAL.*`, `WS.*`), as the
//! master's own AOF does not carry them either; they recover from their own
//! stores. Replicas are single-shard (`replica::replica_supported`).
//!
//! A promotion that rolls back a master transaction still open asks for a
//! rewrite too ([`after_promotion_rollback`]): the AOF then holds the
//! rolled-back keyspace instead of an open block whose id a later local
//! transaction could reuse.

use std::sync::Arc;

use crate::persistence::aof::AofWriterPool;
use crate::protocol::Frame;
use crate::replication::apply::ReplCommand;

/// After a full sync loaded the master's dataset: ask for the rewrite that
/// makes it the base of a new AOF generation (redis `restartAOFAfterSYNC`).
/// The auto-rewrite monitor wakes at once, waits out a rewrite already
/// running (its image may predate the load), dispatches its own, and keeps
/// the request until a rewrite it started completes OK (R2b round 3 F-E: a
/// directly dispatched rewrite that later failed was never retried). No-op
/// without an AOF.
pub(crate) fn after_full_sync(aof_pool: Option<&Arc<AofWriterPool>>) {
    if aof_pool.is_none() {
        return;
    }
    tracing::info!(
        "replica: full sync loaded; requesting an AOF rewrite so its base is the synced dataset"
    );
    crate::persistence::aof::auto_rewrite::request_rewrite();
}

/// At `REPLICAOF` with an AOF: say so when nothing will ever rewrite it (the
/// embedded server runs no auto-rewrite monitor and cannot fold its TopLevel
/// AOF), so a promoted replica's restart cannot hold the master's dataset.
pub(crate) fn warn_if_no_rewriter(aof_pool: Option<&Arc<AofWriterPool>>) {
    if aof_pool.is_some() && !crate::persistence::aof::auto_rewrite::monitor_running() {
        tracing::warn!(
            "replica: this server runs no AOF auto-rewrite monitor (embedded mode), so the \
             synced dataset is never written to its AOF: after a promotion, a restart of this \
             node holds only what its AOF had before the sync plus the stream since"
        );
    }
}

/// A promotion rolled back `rolled` master transactions still open: ask for
/// a rewrite so the AOF holds the rolled-back keyspace.
pub(crate) fn after_promotion_rollback(rolled: usize) {
    if rolled > 0 {
        crate::persistence::aof::auto_rewrite::request_rewrite();
    }
}

/// Whether an applied stream record is KV history for the replica's AOF.
fn is_logged(cmd: &[u8], args: &[Frame]) -> bool {
    use crate::persistence::replay::pseudo::{Pseudo, classify};
    if matches!(classify(cmd, args), Some(Pseudo::Txn(_))) {
        return true;
    }
    let starts = |p: &[u8]| cmd.len() >= p.len() && cmd[..p.len()].eq_ignore_ascii_case(p);
    if [&b"FT."[..], b"GRAPH.", b"MQ.", b"TEMPORAL.", b"WS."]
        .iter()
        .any(|p| starts(p))
    {
        return false;
    }
    crate::command::metadata::is_write(cmd)
}

/// Append an applied master-stream record to the replica's own AOF. Call
/// right after `apply_local` returned `Applied`, with no await in between.
pub(crate) fn log_applied(aof_pool: Option<&Arc<AofWriterPool>>, rc: &ReplCommand) {
    let Some(pool) = aof_pool else {
        return;
    };
    let Some((cmd, args)) = crate::shard::spsc_handler::extract_command_static(&rc.command) else {
        return;
    };
    if !is_logged(cmd, args) {
        return;
    }
    let bytes = crate::persistence::aof::serialize_command_for_log(&rc.command);
    let mut budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
    if !pool.send_append_bounded_blocking(0, 0, rc.db_index, bytes, &mut budget) {
        tracing::error!(
            "replica: a master-stream record was NOT appended to this replica's AOF (writer \
             backpressure); requesting a rewrite so the AOF holds the dataset again"
        );
        crate::persistence::aof::auto_rewrite::request_rewrite();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn argv(parts: &[&str]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect()
    }

    #[test]
    fn kv_writes_and_txn_markers_are_logged_other_planes_and_reads_are_not() {
        assert!(is_logged(b"SET", &argv(&["k", "v"])));
        assert!(is_logged(b"incr", &argv(&["c"])));
        assert!(is_logged(b"DEL", &argv(&["k"])));
        assert!(is_logged(b"SWAPDB", &argv(&["0", "1"])));
        assert!(is_logged(b"MOON.TXN", &argv(&["BEGIN", "7"])));
        assert!(!is_logged(b"GET", &argv(&["k"])));
        assert!(!is_logged(b"PING", &[]));
        assert!(!is_logged(b"FT.CREATE", &argv(&["idx"])));
        assert!(!is_logged(b"GRAPH.ADDNODE", &argv(&["g", "1"])));
        assert!(!is_logged(b"WS.CREATE.APPLY", &argv(&["w"])));
    }
}
