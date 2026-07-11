//! `record_reason_del` — dual-plane (AOF + replication) `DEL <key>` emission
//! for master-side key removals that happen for a reason OTHER than a client
//! write command: active TTL expiry and eviction plain-drops (task #34,
//! `.planning/rfcs/plane-replication-design.md`, Wave A).
//!
//! Before this module, three classes of removal reached neither plane: the
//! key vanished from the master's own keyspace, but an attached replica kept
//! serving it forever, and a `kill -9` + restart against `--appendonly yes`
//! resurrected it from the AOF replay (the AOF never recorded the DEL, only
//! the original SET). Both gaps are closed by threading this helper's two
//! call shapes into every genuine plain-drop site:
//!
//! - [`record_reason_del`] — shard-event-loop context (background active
//!   expiry, background eviction tick). Reuses
//!   [`crate::shard::spsc_handler::wal_append_and_fanout`] verbatim — the
//!   exact mechanism the `ShardMessage::SwapDb` synthetic-command record
//!   already uses for a write with no originating client command. One call
//!   does the WAL v3 log, the replication backlog append + offset advance +
//!   deferred live fan-out, AND the AOF pool append (with its own SELECT
//!   injection, PR #282), so a background-tick DEL gets bit-for-bit the same
//!   durability and replica-delivery guarantees as an ordinary write.
//!
//! - [`record_reason_del_conn`] — connection-handler context (the inline
//!   fast-path SET eviction gate and the generic per-command write-eviction
//!   gate). These run off a per-connection monoio task, not the shard event
//!   loop, so they only have `Option<Arc<RwLock<ReplicationState>>>` (not
//!   the shard loop's pre-extracted `SharedBacklog`/`OffsetHandle`/
//!   `Vec<ReplicaFanout>`) — mirrors
//!   `handler_monoio::ft::record_local_write_db`'s mechanics (backlog
//!   append + offset advance under one read-lock + deferred
//!   `ShardMessage::ReplicaLiveFanout` self-queue push, same fused-`SELECT`
//!   rule for multi-shard masters) and adds the AOF leg that
//!   `record_local_write_db` deliberately omits (its callers already ride
//!   the generic per-command AOF gate; a synthetic eviction DEL has no
//!   client command to hang that off of).
//!
//! ⚠ Both flavors push to `shard::self_msg` (the live fan-out relay) or
//! touch shard-owned state — monoio shard threads only, same restriction as
//! every other producer in `self_msg`.
//!
//! ⚠ Callers MUST only invoke either flavor for a removal where the key is
//! truly gone — a spilled/cold-tiered entry is NOT a delete (see the
//! plain-drop vs. spill distinction in `storage::eviction`); emitting here
//! for a spill would fabricate a phantom DEL that a replica or AOF replay
//! would apply against a key the master can still serve from its cold tier.

use bytes::Bytes;

use crate::persistence::aof::{AOF_SPSC_BACKPRESSURE_BOUND, AofWriterPool, serialize_command};
use crate::protocol::Frame;
#[cfg(feature = "runtime-monoio")]
use crate::replication::state::ReplicationState;

/// Serialize `DEL <key>` as a RESP command record — the same wire form
/// `aof::serialize_command` produces for any ordinary client command, so
/// replica apply and AOF replay treat it identically to a real client DEL.
fn serialize_del(key: &[u8]) -> Bytes {
    let frame = Frame::Array(crate::framevec![
        Frame::BulkString(Bytes::from_static(b"DEL")),
        Frame::BulkString(Bytes::copy_from_slice(key)),
    ]);
    serialize_command(&frame)
}

/// Shard-event-loop-context DEL emission (active expiry, background
/// eviction tick). See module docs.
///
/// `db` is the logical db the removed key lived in — threaded through to
/// `wal_append_and_fanout` so its SELECT-on-db-change bookkeeping (and the
/// AOF writer's own db-scoped SELECT injection) stay correct for multi-db
/// deployments, exactly as every ordinary write already relies on.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_reason_del(
    key: &[u8],
    db: usize,
    wal_writer: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    shard_id: usize,
    aof_pool: Option<&std::sync::Arc<AofWriterPool>>,
    wal_kv_log: bool,
) {
    let serialized = serialize_del(key);
    // Fire-and-forget-on-loss like every other background-tick record (the
    // client that originally SET the key is long gone; there is no response
    // frame to fail loud through). A dropped AOF append here is already
    // counted + `error!`-logged inside `send_append_bounded_blocking`.
    let mut aof_budget = AOF_SPSC_BACKPRESSURE_BOUND;
    let _ = crate::shard::spsc_handler::wal_append_and_fanout(
        &serialized,
        db,
        wal_writer,
        repl_backlog,
        replica_txs,
        repl_state,
        shard_id,
        aof_pool,
        wal_kv_log,
        &mut aof_budget,
    );
}

/// Connection-handler-context DEL emission (inline fast-path SET eviction
/// gate, generic per-command write-eviction gate). See module docs.
///
/// Monoio-only: both call sites (`server::conn::blocking::try_inline_dispatch`,
/// `server::conn::handler_monoio::run_write_eviction_gate`) are
/// `#[cfg(feature = "runtime-monoio")]`-gated — master-side PSYNC is
/// monoio-only (CLAUDE.md), so a tokio-runtime build never needs this leg.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_reason_del_conn(
    repl_state: &Option<std::sync::Arc<std::sync::RwLock<ReplicationState>>>,
    shard_id: usize,
    num_shards: usize,
    aof_pool: Option<&std::sync::Arc<AofWriterPool>>,
    db: usize,
    key: &[u8],
) {
    let del_bytes = serialize_del(key);
    // Cheap first gate (one Relaxed load): skip the replication leg entirely
    // until a replica has ever begun attaching — mirrors
    // `handler_monoio::ft::replication_fanout_active`'s first check.
    if crate::replication::state::fanout_hint_active() {
        push_record_db(repl_state, shard_id, num_shards, db, del_bytes.clone());
    }
    if let Some(pool) = aof_pool {
        let mut budget = AOF_SPSC_BACKPRESSURE_BOUND;
        let _ = pool.send_append_bounded_blocking(shard_id, 0, db, del_bytes, &mut budget);
    }
}

/// Db-aware record push — mirrors `handler_monoio::ft::record_local_write_db`
/// exactly (fused `SELECT` prefix for multi-shard masters, emit-on-change
/// `SELECT` tracking for single-shard masters), parameterized on a raw
/// `repl_state` handle instead of `&ConnectionContext` so it is usable from
/// call sites that don't carry a full connection context (the inline
/// dispatch fast path only threads the individual fields it needs).
#[cfg(feature = "runtime-monoio")]
fn push_record_db(
    repl_state: &Option<std::sync::Arc<std::sync::RwLock<ReplicationState>>>,
    shard_id: usize,
    num_shards: usize,
    db: usize,
    bytes: Bytes,
) {
    if num_shards > 1 {
        let select = crate::persistence::aof::serialize_select_record(db);
        let mut combined = Vec::with_capacity(select.len() + bytes.len());
        combined.extend_from_slice(&select);
        combined.extend_from_slice(&bytes);
        push_record(repl_state, shard_id, Bytes::from(combined));
        return;
    }
    let needs_select = repl_state.as_ref().is_some_and(|rs| {
        rs.read().is_ok_and(|g| {
            g.stream_db.get(shard_id).is_some_and(|slot| {
                if slot.load(std::sync::atomic::Ordering::Relaxed) != db as i64 {
                    slot.store(db as i64, std::sync::atomic::Ordering::Relaxed);
                    true
                } else {
                    false
                }
            })
        })
    });
    if needs_select {
        push_record(
            repl_state,
            shard_id,
            crate::persistence::aof::serialize_select_record(db),
        );
    }
    push_record(repl_state, shard_id, bytes);
}

/// One backlog-append + offset-advance + deferred-fanout record push —
/// mirrors `handler_monoio::ft::record_local_write` exactly.
#[cfg(feature = "runtime-monoio")]
fn push_record(
    repl_state: &Option<std::sync::Arc<std::sync::RwLock<ReplicationState>>>,
    shard_id: usize,
    bytes: Bytes,
) {
    let mut end_offset = u64::MAX;
    if let Some(rs) = repl_state.as_ref() {
        if let Ok(g) = rs.read() {
            if let Some(slot) = g.per_shard_backlogs.get(shard_id) {
                if let Some(backlog) = slot.lock().as_mut() {
                    backlog.append(&bytes);
                }
            }
            end_offset = g.increment_shard_offset(shard_id, bytes.len() as u64);
        }
    }
    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::ReplicaLiveFanout {
        bytes,
        end_offset,
    });
}
