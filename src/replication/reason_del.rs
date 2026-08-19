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
//!
//! Wave A part 2 adds [`record_effect_write`], the connection-context
//! counterpart for a **Lua script's** write effects: `src/scripting/
//! bridge.rs`'s `redis.call`/`redis.pcall` closure has no `WRITE`
//! command-metadata flag to hang the generic per-command AOF/replication
//! gate off of (EVAL/EVALSHA/FCALL are deliberately left unflagged — see the
//! bridge module docs), so every successfully-executed inner write command
//! reached NEITHER plane before this. `record_effect_write` shares
//! `record_reason_del_conn`'s exact emission mechanics (extracted into
//! `record_bytes_conn` below) but records the verbatim `cmd + args` the
//! script issued instead of a synthetic `DEL`.
//!
//! moon#517: `record_effect_write` is the one entry point here that is NOT
//! monoio-only. Its AOF leg runs on every runtime (see
//! `record_bytes_conn`); only the replication leg stays gated. The
//! `record_reason_del*` flavors keep their whole-function gate because
//! every one of their call sites is itself monoio-only.

use bytes::Bytes;

use crate::persistence::aof::{AofWriterPool, serialize_command};
use crate::protocol::Frame;
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
/// `aof_budget` is the caller's SHARED backpressure budget for the whole
/// sweep (#454 review P2.8): callers mint ONE
/// [`crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND`] per
/// expiry cycle / eviction run and thread it through every per-key
/// emission, so a stalled AOF writer costs the shard event loop at most
/// one bound per SWEEP — not one bound per KEY (a 1000-victim OOM sweep
/// against a hung disk used to block the shard for 1000 × 500ms).
/// `wal_append_and_fanout` decrements it by time actually spent blocking;
/// once exhausted, remaining keys in the sweep fail fast into
/// [`record_reason_del_dropped`] accounting.
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
    aof_budget: &mut std::time::Duration,
) {
    // Task #34 review (defect 2): hoist the exact same no-work gate
    // `wal_append_and_fanout` checks internally to BEFORE `serialize_del`
    // allocates the RESP record. Previously every background-tick removal
    // (active expiry, eviction plain-drop) paid a `Bytes` allocation +
    // `Frame`/`framevec` build even on a standalone server with no WAL, no
    // replica, and no AOF pool wired — the single most common deployment
    // shape. Behavior when there IS work is unchanged: the same predicate
    // runs again inside `wal_append_and_fanout` (cheap, no allocation) and
    // gates nothing differently.
    if !crate::shard::spsc_handler::wal_fanout_has_work(
        wal_writer,
        replica_txs,
        aof_pool,
        wal_kv_log,
    ) {
        return;
    }
    let serialized = serialize_del(key);
    // #452.4: reason-DELs get the escalated backpressure bound — a dropped
    // record here means restart replay RESURRECTS a key clients were told is
    // gone (strictly worse than a lost client write, which at least matches
    // what the client observed on a non-durable server). There is no
    // response frame to fail loud through, so a drop past the bound is
    // counted in [`crate::persistence::aof::AOF_REASON_DEL_DROPPED`] and
    // latches `aof_last_append_status:err`. The bound itself is the
    // caller's per-sweep `aof_budget` — see the doc comment above.
    if !crate::shard::spsc_handler::wal_append_and_fanout(
        &serialized,
        db,
        wal_writer,
        repl_backlog,
        replica_txs,
        repl_state,
        shard_id,
        aof_pool,
        wal_kv_log,
        aof_budget,
    ) {
        record_reason_del_dropped(key);
    }
}

/// #452.4 fail-loud accounting for a reason-DEL that could not be enqueued
/// for persistence within [`crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND`].
fn record_reason_del_dropped(key: &[u8]) {
    crate::persistence::aof::AOF_REASON_DEL_DROPPED
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    crate::persistence::aof::AOF_LAST_APPEND_OK.store(false, std::sync::atomic::Ordering::Relaxed);
    tracing::error!(
        "reason-DEL LOST for key {:?}: AOF writer saturated past the escalated bound — \
         restart replay will RESURRECT this key unless a rewrite completes first",
        String::from_utf8_lossy(&key[..key.len().min(64)]),
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
    repl_state: &Option<std::sync::Arc<parking_lot::RwLock<ReplicationState>>>,
    shard_id: usize,
    num_shards: usize,
    aof_pool: Option<&std::sync::Arc<AofWriterPool>>,
    db: usize,
    key: &[u8],
) {
    // Task #34 review (defect 2): see `conn_has_work` doc comment. Skip the
    // `serialize_del` allocation entirely when neither leg has any work.
    if !conn_has_work(aof_pool) {
        return;
    }
    record_bytes_conn(
        repl_state,
        shard_id,
        num_shards,
        aof_pool,
        db,
        serialize_del(key),
    );
}

/// Lua-script write-effect emission (task #34 Wave A part 2). See module
/// docs. Called from `scripting::bridge::LuaEvictionCtx::emit_effect`,
/// immediately after a W-flagged `redis.call`/`redis.pcall` inner command
/// returns a non-error `Frame` — so a script that writes two keys and then
/// errors on a third still durably records the first two (effects emit as
/// they happen, not batched at script end).
///
/// `cmd_and_args` is the exact wire form the script invoked (`frames[0]` is
/// the command name, the rest its arguments) — recorded verbatim, the same
/// "replicate the command, not a derived form" approach every other write
/// path in moon uses.
///
/// Available on BOTH runtimes (moon#517). It used to be
/// `#[cfg(feature = "runtime-monoio")]`-gated like
/// [`record_reason_del_conn`], with the bridge discarding the effect
/// entirely off monoio — but that gate confused two independent legs. The
/// replication leg genuinely is monoio-only (it pushes through
/// `shard::self_msg`, and master-side PSYNC does not exist under tokio); the
/// AOF leg is a channel send to the writer pool, which the tokio connection
/// handler already performs for every ordinary write. Gating both together
/// meant a `runtime-tokio` build mutated the keyspace on `EVAL`, answered
/// OK, and wrote NOTHING to the AOF — the script's writes vanished on
/// restart while every non-script write around them survived.
/// [`record_bytes_conn`] now splits the two legs; this entry point is
/// unconditional.
pub(crate) fn record_effect_write(
    repl_state: &Option<std::sync::Arc<parking_lot::RwLock<ReplicationState>>>,
    shard_id: usize,
    num_shards: usize,
    aof_pool: Option<&std::sync::Arc<AofWriterPool>>,
    db: usize,
    cmd_and_args: &[Frame],
) {
    // Task #34 review (defect 2): see `conn_has_work` doc comment. Skip the
    // `Frame::Array`/`cmd_and_args.to_vec()`/`serialize_command` allocations
    // entirely when neither leg has any work — a script's write effects on a
    // standalone, AOF-off server previously paid a full record build for
    // every single inner command, discarded immediately after.
    if !conn_has_work(aof_pool) {
        return;
    }
    let frame = Frame::Array(crate::protocol::FrameVec::from_vec(cmd_and_args.to_vec()));
    let serialized = serialize_command(&frame);
    record_bytes_conn(repl_state, shard_id, num_shards, aof_pool, db, serialized);
}

/// Cheap pre-check (task #34 review, defect 2): `true` iff either
/// connection-context emission leg — replication (gated on the sticky
/// `fanout_hint_active` Relaxed load) or AOF (gated on `aof_pool` being
/// wired) — has any work to do. Hoisted to the top of every
/// connection-context entry point ([`record_reason_del_conn`],
/// [`record_effect_write`]) so the common "no replica ever attached AND no
/// AOF pool wired" case costs one atomic load + one `Option` check, never a
/// `Bytes`/`Frame` allocation. Mirrors the exact pair of checks
/// `record_bytes_conn` already runs per-leg — this is not a new decision,
/// only an earlier exit for the case where NEITHER leg would do anything.
#[inline]
fn conn_has_work(aof_pool: Option<&std::sync::Arc<AofWriterPool>>) -> bool {
    crate::replication::state::fanout_hint_active() || aof_pool.is_some()
}

/// Shared connection-context emission core: replication leg (gated on the
/// cheap `fanout_hint_active` Relaxed load) + AOF leg. Extracted from
/// `record_reason_del_conn` so [`record_effect_write`] rides the identical
/// mechanics for an arbitrary pre-serialized record instead of only `DEL`.
///
/// moon#517: the two legs are gated INDEPENDENTLY. The replication leg is
/// monoio-only — it hands the record to `shard::self_msg`, whose queue only
/// a monoio shard thread may touch (a tokio work-stealing task pushing there
/// would strand the record on a thread nobody drains), and master-side PSYNC
/// is monoio-only regardless. The AOF leg is runtime-agnostic: it is a send
/// on the writer pool's channel, exactly what the tokio connection handler
/// does for every ordinary write.
fn record_bytes_conn(
    repl_state: &Option<std::sync::Arc<parking_lot::RwLock<ReplicationState>>>,
    shard_id: usize,
    num_shards: usize,
    aof_pool: Option<&std::sync::Arc<AofWriterPool>>,
    db: usize,
    bytes: Bytes,
) {
    // Cheap first gate (one Relaxed load): skip the replication leg entirely
    // until a replica has ever begun attaching — mirrors
    // `handler_monoio::ft::replication_fanout_active`'s first check.
    #[cfg(feature = "runtime-monoio")]
    if crate::replication::state::fanout_hint_active() {
        push_record_db(repl_state, shard_id, num_shards, db, bytes.clone());
    }
    #[cfg(not(feature = "runtime-monoio"))]
    let _ = (repl_state, num_shards);
    if let Some(pool) = aof_pool {
        // #452.4: escalated bound + fail-loud accounting — see
        // `record_reason_del`'s comment for the resurrection rationale.
        let mut budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        if !pool.send_append_bounded_blocking(shard_id, 0, db, bytes.clone(), &mut budget) {
            record_reason_del_dropped(&bytes);
        }
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
    repl_state: &Option<std::sync::Arc<parking_lot::RwLock<ReplicationState>>>,
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
        rs.read().stream_db.get(shard_id).is_some_and(|slot| {
            if slot.load(std::sync::atomic::Ordering::Relaxed) != db as i64 {
                slot.store(db as i64, std::sync::atomic::Ordering::Relaxed);
                true
            } else {
                false
            }
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
    repl_state: &Option<std::sync::Arc<parking_lot::RwLock<ReplicationState>>>,
    shard_id: usize,
    bytes: Bytes,
) {
    let mut end_offset = u64::MAX;
    if let Some(rs) = repl_state.as_ref() {
        let g = rs.read();
        if let Some(slot) = g.per_shard_backlogs.get(shard_id) {
            if let Some(backlog) = slot.lock().as_mut() {
                backlog.append(&bytes);
            }
        }
        end_offset = g.increment_shard_offset(shard_id, bytes.len() as u64);
    }
    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::ReplicaLiveFanout {
        bytes,
        end_offset,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GREEN (defect 2 regression guard): with no WAL writer, no replica
    /// fan-out targets, no `repl_state`, and no AOF pool, `record_reason_del`
    /// must be a true no-op — in particular it must never even reach
    /// `serialize_del`/`wal_append_and_fanout`'s internals. We can't assert
    /// "zero allocations" without an allocation-counting harness (none exists
    /// in this repo), so this pins the OBSERVABLE half of the contract: the
    /// shared backlog is untouched (stays `None`) and the call returns
    /// without panicking off the event-loop thread it would normally run on.
    #[test]
    fn record_reason_del_noop_when_nothing_wired() {
        let repl_backlog: crate::replication::backlog::SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(None));
        let mut replica_txs: Vec<crate::shard::dispatch::ReplicaFanout> = Vec::new();
        let mut wal_writer: Option<crate::persistence::wal_v3::segment::WalWriterV3> = None;

        let mut aof_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        record_reason_del(
            b"gone",
            0,
            &mut wal_writer,
            &repl_backlog,
            &mut replica_txs,
            &None,
            0,
            None,
            false,
            &mut aof_budget,
        );

        assert!(
            repl_backlog.lock().is_none(),
            "no-work gate must early-return before touching the backlog"
        );
    }

    /// GREEN (defect 2 regression guard, has-work path): hoisting the gate
    /// must not break emission when there IS work. With an AOF pool wired
    /// (the "work" leg `wal_fanout_has_work` checks), the DEL record must
    /// still reach the writer channel exactly as before the hoist.
    #[test]
    fn record_reason_del_still_emits_to_aof_when_wired() {
        let (tx, rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(16);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);

        let repl_backlog: crate::replication::backlog::SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(None));
        let mut replica_txs: Vec<crate::shard::dispatch::ReplicaFanout> = Vec::new();
        let mut wal_writer: Option<crate::persistence::wal_v3::segment::WalWriterV3> = None;

        let mut aof_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        record_reason_del(
            b"gone",
            0,
            &mut wal_writer,
            &repl_backlog,
            &mut replica_txs,
            &None,
            0,
            Some(&pool),
            false,
            &mut aof_budget,
        );

        match rx.try_recv() {
            Ok(crate::persistence::aof::AofMessage::Append { bytes, .. }) => {
                let text = String::from_utf8_lossy(&bytes);
                assert!(
                    text.contains("DEL") && text.contains("gone"),
                    "expected a serialized DEL record, got {text:?}"
                );
            }
            Ok(_) => panic!("expected an AofMessage::Append record"),
            Err(e) => panic!("expected a queued AOF record, got recv error: {e:?}"),
        }
    }

    /// #452.4: a reason-DEL that cannot be enqueued within the escalated
    /// bound must be counted in `AOF_REASON_DEL_DROPPED` and latch the
    /// degraded status — never a silent `let _ =` loss (the pre-fix
    /// behavior), because restart replay resurrects the key.
    #[test]
    fn record_reason_del_drop_is_counted_and_latches_degraded() {
        // Capacity-1 channel, pre-filled and never drained: the send inside
        // record_reason_del exhausts the escalated bound (the real 500ms —
        // the bound is a const, deliberately not injectable) and reports
        // the drop.
        let (tx, rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(1);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);
        assert!(pool.try_send_append(0, 0, 0, Bytes::from_static(b"fill")));

        let repl_backlog: crate::replication::backlog::SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(None));
        let mut replica_txs: Vec<crate::shard::dispatch::ReplicaFanout> = Vec::new();
        let mut wal_writer: Option<crate::persistence::wal_v3::segment::WalWriterV3> = None;

        let before = crate::persistence::aof::AOF_REASON_DEL_DROPPED
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut aof_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        record_reason_del(
            b"resurrect-me",
            0,
            &mut wal_writer,
            &repl_backlog,
            &mut replica_txs,
            &None,
            0,
            Some(&pool),
            false,
            &mut aof_budget,
        );
        let after = crate::persistence::aof::AOF_REASON_DEL_DROPPED
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after > before,
            "dropped reason-DEL must increment AOF_REASON_DEL_DROPPED (before={before}, after={after})"
        );
        assert!(
            !crate::persistence::aof::AOF_LAST_APPEND_OK.load(std::sync::atomic::Ordering::Relaxed),
            "dropped reason-DEL must latch aof_last_append_status:err"
        );
        drop(rx);
    }

    /// #454 review P2.8: the backpressure budget is SHARED across a sweep —
    /// once one blocked emission exhausts it, later per-key calls in the
    /// same sweep must fail fast (drop-with-accounting) instead of each
    /// minting a fresh bound and re-stalling the shard event loop.
    #[test]
    fn reason_del_budget_is_shared_across_a_sweep() {
        let (tx, _rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(1);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);
        assert!(pool.try_send_append(0, 0, 0, Bytes::from_static(b"fill")));

        let repl_backlog: crate::replication::backlog::SharedBacklog =
            std::sync::Arc::new(parking_lot::Mutex::new(None));
        let mut replica_txs: Vec<crate::shard::dispatch::ReplicaFanout> = Vec::new();
        let mut wal_writer: Option<crate::persistence::wal_v3::segment::WalWriterV3> = None;

        // Small sweep budget so the test doesn't sit through the real 500ms.
        let mut sweep_budget = std::time::Duration::from_millis(20);
        record_reason_del(
            b"victim-1",
            0,
            &mut wal_writer,
            &repl_backlog,
            &mut replica_txs,
            &None,
            0,
            Some(&pool),
            false,
            &mut sweep_budget,
        );
        assert_eq!(
            sweep_budget,
            std::time::Duration::ZERO,
            "a fully-blocked emission must consume the whole shared budget"
        );
        let t0 = std::time::Instant::now();
        record_reason_del(
            b"victim-2",
            0,
            &mut wal_writer,
            &repl_backlog,
            &mut replica_txs,
            &None,
            0,
            Some(&pool),
            false,
            &mut sweep_budget,
        );
        assert!(
            t0.elapsed() < std::time::Duration::from_millis(15),
            "an exhausted sweep budget must fail fast, not re-block per key (took {:?})",
            t0.elapsed()
        );
    }

    /// `conn_has_work` (defect 2's connection-context gate): an AOF pool
    /// being wired must ALONE be sufficient to report work, independent of
    /// the sticky process-global `fanout_hint_active` flag (which this test
    /// binary otherwise never sets — see `replication::state`'s doc comment
    /// on `FANOUT_HINT`).
    #[cfg(feature = "runtime-monoio")]
    #[test]
    fn conn_has_work_true_when_aof_pool_wired() {
        let (tx, _rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(16);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);
        assert!(conn_has_work(Some(&pool)));
    }
}
