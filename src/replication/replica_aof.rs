//! A replica's own AOF holds the master's dataset (R2b round 2 R1, moon#1318;
//! round 3 F-A, X1, F-E).
//!
//! Since R2b P1 a replica's AOF is its only KV source at boot, exactly as in
//! redis. So that a PROMOTED replica's restart keeps the master's data:
//!
//! 1. **The synced dataset.** A full sync loads it straight into the
//!    keyspace. redis's `restartAOFAfterSYNC` rewrites the AOF right after,
//!    so the new generation's base is the synced dataset. [`after_full_sync`]
//!    asks the auto-rewrite monitor for that rewrite
//!    ([`crate::persistence::aof::auto_rewrite::request_rewrite`]): the
//!    monitor wakes at once, waits out any rewrite already running (its image
//!    may predate the load), dispatches its own, and keeps the request until
//!    a rewrite IT started completes OK — a failed one is retried.
//! 2. **The applied stream.** redis replicas feed it into their own AOF
//!    (`propagate` / `feedAppendOnlyFile`). Every applied KV write and every
//!    `MOON.TXN` marker is appended: [`admit`] waits — asynchronously, the
//!    link stalls instead of the shard thread — until the writer can take
//!    the record, BEFORE the apply; [`log_applied`] enqueues it right after
//!    the apply with no await in between, so the record's fold stamp is the
//!    write's epoch (#455). redis blocks the replica rather than drop a
//!    record; so does this. A record that still cannot be enqueued (the
//!    writer is gone) marks `aof_last_write_status:err` (the pool's drop
//!    accounting) and asks for a rewrite.
//! 3. **Dead master transactions.** The markers are the AOF's own
//!    transaction records (moon#1300). When the replica stops following the
//!    master, the blocks it never saw end are rolled back in memory AND
//!    ended in this AOF with `MOON.TXN RESET` ([`log_txn_reset`], from
//!    `txn_apply::roll_back_open` / `discard_logged`), before any local
//!    write is logged — so replay rolls them back at that point, and a local
//!    transaction reusing a block's log id opens a fresh block.
//!
//! Not logged: the planes that are not KV history in the AOF (`FT.*`,
//! `GRAPH.*`, `MQ.*` replication effects, `TEMPORAL.*`, `WS.*`), as the
//! master's own AOF does not carry them either. Replicas are single-shard
//! (`replica::replica_supported`), so every record goes to writer 0.
//!
//! Residual: until the post-sync rewrite commits, the AOF's base is the
//! dataset this node held BEFORE the sync (plus the stream applied since);
//! a crash in that window that is then promoted without resyncing restarts
//! with that former dataset, not the master's. A node restarted as a replica
//! full-syncs again. The embedded server runs no auto-rewrite monitor (and
//! cannot fold its TopLevel AOF), so step 1 never happens there: it warns at
//! `REPLICAOF` ([`warn_if_no_rewriter`]).

use std::sync::Arc;

use crate::persistence::aof::AofWriterPool;
use crate::protocol::Frame;
use crate::replication::apply::ReplCommand;

/// After a full sync loaded the master's dataset: ask for the rewrite that
/// makes it the base of a new AOF generation (see the module doc). No-op
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
/// embedded server), so a promoted replica's restart cannot hold the
/// master's dataset.
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
/// a rewrite (compaction: the AOF then no longer carries the dead blocks).
pub(crate) fn after_promotion_rollback(rolled: usize) {
    if rolled > 0 {
        crate::persistence::aof::auto_rewrite::request_rewrite();
    }
}

/// How long [`log_txn_reset`] may hold the shard thread for writer room: a
/// promotion is rare, and a lost RESET would misattribute every later local
/// write, so it waits far longer than a data record's bound.
const RESET_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Append `MOON.TXN RESET` to this node's AOF, synchronously (see the module
/// doc, step 3). Called on the shard thread before the role changes.
pub(crate) fn log_txn_reset(aof_pool: Option<&Arc<AofWriterPool>>) {
    use crate::persistence::replay::pseudo::{TxnMarker, TxnRecord};
    let Some(pool) = aof_pool else {
        return;
    };
    let record = TxnRecord::new(TxnMarker::Reset);
    let bytes = bytes::Bytes::copy_from_slice(record.as_bytes());
    let mut budget = RESET_BUDGET;
    if !pool.send_append_bounded_blocking(0, 0, 0, bytes, &mut budget) {
        tracing::error!(
            "replica: MOON.TXN RESET was NOT appended to this node's AOF; requesting a rewrite \
             so the dead master transaction's records leave it"
        );
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

/// Whether `rc` will be appended by [`log_applied`].
pub(crate) fn will_log(aof_pool: Option<&Arc<AofWriterPool>>, rc: &ReplCommand) -> bool {
    aof_pool.is_some()
        && crate::shard::spsc_handler::extract_command_static(&rc.command)
            .is_some_and(|(cmd, args)| is_logged(cmd, args))
}

/// Before applying a record [`log_applied`] will append: wait until writer 0
/// can take it. Awaits (the replication link stalls, the shard keeps
/// serving); never times out while the writer lives — redis blocks the
/// replica, it never drops. `false` when the writer is gone.
pub(crate) async fn admit(pool: &AofWriterPool) -> bool {
    let mut warned = false;
    loop {
        match pool.await_append_room(0, 1).await {
            Ok(()) => return true,
            Err(crate::persistence::aof::AofAck::ChannelFull) => {
                if !warned {
                    warned = true;
                    tracing::warn!(
                        "replica: the AOF writer is behind; the replication stream waits for it \
                         (records are never dropped)"
                    );
                }
            }
            Err(_) => return false,
        }
    }
}

/// Append an applied master-stream record to the replica's own AOF. Call
/// right after `apply_local` returned `Applied`, with no await in between,
/// after [`admit`] (so the enqueue does not wait).
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
    // Non-blocking: `admit` made room. A refusal here (the writer is gone,
    // or another producer took the room) is counted by the pool as a dropped
    // append (`aof_last_write_status:err` until a fold heals it).
    if !pool.try_send_append(0, 0, rc.db_index, bytes) {
        tracing::error!(
            "replica: a master-stream record was NOT appended to this replica's AOF; \
             requesting a rewrite so the AOF holds the dataset again"
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
