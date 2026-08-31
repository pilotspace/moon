//! VLL multi-key coordination for MGET, MSET, DEL, UNLINK, EXISTS.
//!
//! Groups keys by target shard in a BTreeMap (ascending shard-ID order for
//! deadlock prevention -- VLL pattern), dispatches to each shard, collects
//! results, and assembles the final response. Local-shard keys are executed
//! directly without SPSC overhead.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;

use crate::command::{DispatchResult, dispatch as cmd_dispatch};
use crate::framevec;
use crate::protocol::Frame;
use crate::runtime::channel;
// Coordinator uses oneshot channels (not ResponseSlotPool) for cross-thread safety.
// ResponseSlotPool's AtomicWaker doesn't work with monoio's !Send executor.
// The oneshot overhead (~80ns) is negligible on the multi-key coordination path.
use crate::shard::dispatch::{
    CROSS_SHARD_PUSH_BACKOFF, CROSS_SHARD_PUSH_MAX_RETRIES, PushOutcome, ShardMessage, key_to_shard,
};
use crate::shard::mesh::ChannelMesh;
use crate::storage::entry::CachedClock;

use super::shared_databases::ShardDatabases;
/// Coordinate a multi-key command across shards.
///
/// Routes MGET, MSET, DEL (multi), UNLINK (multi), and EXISTS (multi)
/// to the appropriate per-command coordinator.
pub async fn coordinate_multi_key(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    // Local-leg persistence context (review Finding 1): the coordinator's
    // in-process local legs for MSET/MSETNX append to the owning shard's AOF
    // via these, matching the local single-key write contract. `None` disables
    // persistence (tests / no-AOF deployments).
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    // v3-5 group commit: set to true when a local-leg append was enqueued
    // under appendfsync=always. The connection handler MUST then issue ONE
    // `fsync_barrier(my_shard)` for the batch before acking the client, and
    // overwrite this command's response with AOF_FSYNC_ERR on barrier failure.
    local_barrier_pending: &mut bool,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if cmd.eq_ignore_ascii_case(b"MGET") {
        coordinate_mget(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"MSET") {
        coordinate_mset(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"MSETNX") {
        coordinate_msetnx(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"BITOP") {
        coordinate_bitop(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"COPY") {
        coordinate_copy(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            _response_pool,
        )
        .await
    } else {
        // DEL, UNLINK, EXISTS with multiple keys
        coordinate_multi_del_or_exists(
            cmd,
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            _response_pool,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Shared legs for the BITOP / COPY coordinators
// ---------------------------------------------------------------------------

/// Run one full command on the LOCAL shard through the real dispatcher
/// (identical semantics to a remote MultiExecute leg, minus the hop).
fn run_local(
    shard_databases: &Arc<ShardDatabases>,
    db_index: usize,
    cached_clock: &CachedClock,
    cmd: &[u8],
    args: &[Frame],
) -> Frame {
    let db_count = shard_databases.db_count();
    let mut selected = db_index;
    let run = |db: &mut crate::storage::Database| {
        db.refresh_now_from_cache(cached_clock);
        match cmd_dispatch(db, cmd, args, &mut selected, db_count) {
            DispatchResult::Response(f) | DispatchResult::Quit(f) => f,
        }
    };
    crate::shard::slice::with_shard_db(db_index, run)
}

/// Maximum time to wait for a cross-shard reply after the request was
/// successfully pushed into the target shard's ring buffer (prod-hardening
/// #11). `spsc_send`'s retry budget only bounds getting the message INTO the
/// ring; once the push succeeds, if the target shard then stalls while
/// executing the command (a wedged-disk fsync during snapshot/AOF, an
/// uninterruptible D-state I/O stall, or a dead shard), `reply_rx.recv().await`
/// has no way to wake on its own and the awaiting client connection hangs
/// forever with no response and no cancel. This is a safety ceiling far above
/// any legitimate cross-shard command latency (including group-commit fsync
/// under load) — it exists only so a genuinely wedged shard surfaces an error
/// instead of an unbounded hang.
// One shared bound with the connection handlers' slot awaits (E4) — a
// single constant so the two reply paths can't drift apart.
use crate::shard::dispatch::XSHARD_REPLY_TIMEOUT;

/// Why a bounded cross-shard receive produced no reply.
///
/// The distinction is load-bearing for RETRY SAFETY, which is why it is an enum
/// and not a bool: `Closed` and `TimedOut` say different things about whether
/// the command ran. Collapsing them lets a caller report "never executed" for a
/// target that is still executing — and a client that believes a non-idempotent
/// command never ran will happily re-send it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyFailure {
    /// The target dropped the reply sender without sending. It may have dropped
    /// it *after* applying writes (e.g. a panic between apply and reply), so
    /// this is "no reply", NOT "no effect".
    Closed,
    /// [`XSHARD_REPLY_TIMEOUT`] expired. The target may still be executing, or
    /// may have already applied its writes. Execution status is UNKNOWN.
    TimedOut,
}

/// Await `reply_rx` for at most `timeout`, reporting WHICH failure occurred.
///
/// Split out from [`recv_reply_bounded_reason`] so tests can drive both arms
/// without waiting out the real 30s [`XSHARD_REPLY_TIMEOUT`], and so callers
/// that await SEVERAL receivers under ONE overall budget (the scripting
/// fan-out) can pass the remaining slice of it rather than restarting the full
/// timeout per receiver.
pub(crate) async fn recv_reply_within<T: Send + 'static>(
    reply_rx: channel::OneshotReceiver<T>,
    timeout: std::time::Duration,
) -> Result<T, ReplyFailure> {
    use crate::runtime::race::{Arm, race2};
    let recv = std::pin::pin!(reply_rx);
    #[cfg(feature = "runtime-tokio")]
    let sleep = std::pin::pin!(tokio::time::sleep(timeout));
    #[cfg(feature = "runtime-monoio")]
    let sleep = std::pin::pin!(monoio::time::sleep(timeout));
    // race2 polls the recv arm first, so a ready reply always wins the tie and
    // the timer future is dropped un-fired (cheap deregister on both runtimes).
    match race2(recv, sleep).await {
        Arm::First(Ok(v)) => Ok(v),
        Arm::First(Err(_)) => Err(ReplyFailure::Closed),
        Arm::Second(()) => Err(ReplyFailure::TimedOut),
    }
}

/// Await a cross-shard `reply_rx` with a bounded timeout (#11), preserving the
/// reason. Prefer this at any call site whose error text makes a claim about
/// whether the command executed.
pub(crate) async fn recv_reply_bounded_reason<T: Send + 'static>(
    reply_rx: channel::OneshotReceiver<T>,
) -> Result<T, ReplyFailure> {
    recv_reply_within(reply_rx, XSHARD_REPLY_TIMEOUT).await
}

/// Await a cross-shard `reply_rx` with a bounded timeout (#11).
///
/// Returns `Err(RecvError)` on EITHER a closed channel (the target shard
/// dropped the reply sender) OR expiry of [`XSHARD_REPLY_TIMEOUT`] — both mean
/// "no usable reply", so every call site's existing error handling applies
/// unchanged; the point is that neither case can hang the connection forever.
/// Use [`recv_reply_bounded_reason`] when the two need telling apart.
pub(crate) async fn recv_reply_bounded<T: Send + 'static>(
    reply_rx: channel::OneshotReceiver<T>,
) -> Result<T, channel::RecvError> {
    recv_reply_bounded_reason(reply_rx)
        .await
        .map_err(|_| channel::RecvError)
}

/// Send one full command to a REMOTE shard and await its reply.
async fn run_remote(
    target_shard: usize,
    routing_key: &Bytes,
    command: Frame,
    my_shard: usize,
    db_index: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // ChannelMesh has no self-send slot — local legs must go via run_local.
    debug_assert_ne!(target_shard, my_shard, "run_remote called for own shard");
    let (reply_tx, reply_rx) = channel::oneshot();
    let msg = ShardMessage::MultiExecute {
        db_index,
        commands: vec![(routing_key.clone(), command)],
        reply_tx,
    };
    let _ = spsc_send(dispatch_tx, my_shard, target_shard, msg, spsc_notifiers).await;
    match recv_reply_bounded(reply_rx).await {
        Ok(mut frames) if !frames.is_empty() => frames.swap_remove(0),
        _ => Frame::Error(Bytes::from_static(b"ERR cross-shard reply channel closed")),
    }
}

/// Route a whole MULTI/EXEC body to the shard that owns all of its keys and
/// await the executed reply (sharded MULTI/EXEC Phase B).
///
/// The owner runs the body atomically on its slice and persists each write to
/// ITS own AOF/WAL; PUBLISH fan-out is deferred back to the caller in the reply
/// (`exec_publishes`) so the originator keeps the normal scatter path. Returns
/// `None` if the owner's reply channel closed (the owner shard died) — the
/// caller surfaces an error rather than a false success.
pub(crate) async fn execute_txn_on_owner(
    owner: usize,
    my_shard: usize,
    db_index: usize,
    commands: Vec<Frame>,
    proto: u8,
    // WATCH tokens travel with the body: the CAS check runs on the owner.
    watched: std::collections::HashMap<Bytes, crate::server::conn::shared::WatchToken>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Option<crate::shard::dispatch::TxnExecReply> {
    debug_assert_ne!(owner, my_shard, "execute_txn_on_owner called for own shard");
    let (reply_tx, reply_rx) = channel::oneshot();
    let payload = crate::shard::dispatch::TxnExecutePayload {
        db_index,
        commands,
        reply_tx,
        proto,
        watched,
    };
    let msg = ShardMessage::TxnExecute(Box::new(payload));
    let _ = spsc_send(dispatch_tx, my_shard, owner, msg, spsc_notifiers).await;
    recv_reply_bounded(reply_rx).await.ok()
}

/// Snapshot the versions of `keys` from whichever shards own them (WATCH).
///
/// Returns versions positionally aligned with `keys`; `0` means absent, which
/// is a real token (watching a key that does not exist and seeing it created
/// IS a conflict). Local keys are read inline; remote keys are grouped per
/// owner so a WATCH of N keys costs at most one hop per shard, not per key.
///
/// A dead owner yields `0` for its keys, which is fail-SAFE in the only
/// direction that matters: `0` almost never matches a live key's version, so
/// the transaction aborts rather than committing on an unverified dependency.
pub(crate) async fn snapshot_versions(
    keys: &[Bytes],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Vec<u32> {
    let mut out = vec![0u32; keys.len()];
    // owner -> (original indices, keys)
    let mut groups: std::collections::HashMap<usize, (Vec<usize>, Vec<Bytes>)> =
        std::collections::HashMap::new();
    for (i, k) in keys.iter().enumerate() {
        let owner = key_to_shard(k, num_shards);
        let e = groups.entry(owner).or_default();
        e.0.push(i);
        e.1.push(k.clone());
    }

    for (owner, (idxs, group_keys)) in groups {
        if owner == my_shard {
            let versions = crate::shard::slice::with_shard_db(db_index, |db| {
                group_keys
                    .iter()
                    .map(|k| db.get_version(k))
                    .collect::<Vec<u32>>()
            });
            for (slot, v) in idxs.iter().zip(versions) {
                out[*slot] = v;
            }
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let payload = crate::shard::dispatch::ReadVersionsPayload {
            db_index,
            keys: group_keys,
            reply_tx,
        };
        let msg = ShardMessage::ReadVersions(Box::new(payload));
        let _ = spsc_send(dispatch_tx, my_shard, owner, msg, spsc_notifiers).await;
        if let Ok(versions) = recv_reply_bounded(reply_rx).await {
            for (slot, v) in idxs.iter().zip(versions) {
                out[*slot] = v;
            }
        }
    }
    out
}

/// Run one full command on whichever shard owns `routing_key`.
#[allow(clippy::too_many_arguments)]
async fn run_on_owner(
    routing_key: &Bytes,
    command_parts: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
) -> Frame {
    let owner = key_to_shard(routing_key, num_shards);
    if owner == my_shard {
        let (cmd, args) = match command_parts.split_first() {
            Some((Frame::BulkString(c), rest)) => (c.clone(), rest),
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid command format")),
        };
        run_local(shard_databases, db_index, cached_clock, &cmd, args)
    } else {
        let command = Frame::Array(command_parts.to_vec().into());
        run_remote(
            owner,
            routing_key,
            command,
            my_shard,
            db_index,
            dispatch_tx,
            spsc_notifiers,
        )
        .await
    }
}

/// Like [`run_on_owner`], but for WRITE commands: when the owner is the
/// connection's own shard the command executes in-process, so nothing else
/// persists it — append it to my_shard's AOF via [`persist_local_leg`]
/// (v3-5: BITOP/COPY/DEL/UNLINK carried gap). Remote owners persist on their
/// own shard via MultiExecute → wal_append_and_fanout, exactly as before.
///
/// `command_parts` MUST be replay-safe against my_shard alone (a full command
/// whose keys are all owned by my_shard, or a synthesized write like
/// `SET dest <computed>` for a scatter BITOP/COPY).
///
/// Persists only when the local execution did not error AND `persist_if`
/// says the response indicates an actual mutation (e.g. `SET ... NX` refusal
/// returns Null and wrote nothing — logging it would cost a needless barrier
/// fsync and could fail a no-op with `AOF_FSYNC_ERR`). Sets
/// `*local_barrier_pending` when the append rides group commit under
/// `appendfsync=always` (the handler owes ONE `fsync_barrier(my_shard)` per
/// batch). Returns `AOF_FSYNC_ERR` when the append never reached the writer.
// Mirrors run_on_owner's routing params + the persistence context; bundling
// them into a struct would obscure the 1:1 correspondence with run_on_owner.
#[allow(clippy::too_many_arguments)]
async fn run_on_owner_persist(
    routing_key: &Bytes,
    command_parts: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    local_barrier_pending: &mut bool,
    persist_if: impl Fn(&Frame) -> bool,
) -> Frame {
    let owner = key_to_shard(routing_key, num_shards);
    if owner != my_shard {
        let command = Frame::Array(command_parts.to_vec().into());
        return run_remote(
            owner,
            routing_key,
            command,
            my_shard,
            db_index,
            dispatch_tx,
            spsc_notifiers,
        )
        .await;
    }
    let (cmd, args) = match command_parts.split_first() {
        Some((Frame::BulkString(c), rest)) => (c.clone(), rest),
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid command format")),
    };
    let resp = run_local(shard_databases, db_index, cached_clock, &cmd, args);
    if !matches!(resp, Frame::Error(_)) && persist_if(&resp) {
        let serialized = crate::persistence::aof::serialize_command(&Frame::Array(
            command_parts.to_vec().into(),
        ));
        match persist_local_leg(aof_pool, repl_state, my_shard, db_index, serialized).await {
            Ok(needs_barrier) => *local_barrier_pending |= needs_barrier,
            Err(()) => {
                return Frame::Error(Bytes::from_static(crate::persistence::aof::AOF_FSYNC_ERR));
            }
        }
    }
    resp
}

/// Type of the replication-state handle threaded into the coordinator's local
/// persistence path (same shape `AofWriterPool::issue_append_lsn` expects).
type ReplStateRef<'a> =
    &'a Option<Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>>;

/// Persist a coordinator LOCAL-leg write to the owning shard's AOF, matching the
/// local single-key write contract (the `is_write` block in
/// `handler_monoio`/`handler_sharded`): issue an LSN off `repl_state`, then
/// group-commit-append. WAL append is external to `cmd_dispatch`, so the
/// coordinator's in-process local legs (`run_local`, `coordinate_mset` fast
/// path / local slice) MUST call this or their writes are lost on restart
/// while the remote legs (via `wal_append_and_fanout`) survive.
///
/// v3-5 group-commit routing: the append is ENQUEUED (bounded backpressure),
/// never per-write fsync-awaited. `Ok(true)` means `appendfsync=always` — the
/// connection handler MUST issue ONE `fsync_barrier(my_shard)` for the whole
/// pipeline batch before acking the client (the same contract the remote legs
/// use). The old per-write awaited fsync stacked one `fsync_timeout` per
/// coordinated command in a pipeline — the measured 2000–3000ms always-tail.
///
/// `serialized` MUST cover only keys OWNED by `my_shard`:
///   - co-located command (MSETNX; MSET fast path)  → the whole command,
///   - scattered MSET local slice                   → a synthesized MSET over
///     just the local keys (never the full scattered command — `my_shard` does
///     not own the remote keys and replay would misapply them on this shard).
///
/// Returns `Err(())` when the append never reached the writer so the caller
/// surfaces `AOF_FSYNC_ERR` instead of a false `+OK` (design-for-failure).
async fn persist_local_leg(
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    my_shard: usize,
    // task #35: the db this local leg executed in (`db_index` at every call
    // site) — threaded into the AOF pool so the writer can inject a
    // `SELECT <db>` record on a db-context change.
    db: usize,
    serialized: Bytes,
) -> Result<bool, ()> {
    let Some(pool) = aof_pool else {
        return Ok(false);
    };
    let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(
        repl_state,
        my_shard,
        serialized.len(),
    );
    match pool.send_append_group(my_shard, lsn, db, serialized).await {
        Ok(needs_barrier) => Ok(needs_barrier),
        Err(_) => Err(()),
    }
}

/// Serialize an `MSET k v ...` command over `pairs` for AOF logging of a local
/// MSET leg. Used for both the fast path (all keys local) and a scattered MSET's
/// local slice (only the local keys) — never the full scattered command.
fn serialize_local_mset(pairs: &[(Bytes, Bytes)]) -> Bytes {
    let mut parts: Vec<Frame> = Vec::with_capacity(pairs.len() * 2 + 1);
    parts.push(Frame::BulkString(Bytes::from_static(b"MSET")));
    for (k, v) in pairs {
        parts.push(Frame::BulkString(k.clone()));
        parts.push(Frame::BulkString(v.clone()));
    }
    crate::persistence::aof::serialize_command(&Frame::Array(parts.into()))
}

fn bulk(b: &Bytes) -> Frame {
    Frame::BulkString(b.clone())
}

fn bulk_static(s: &'static [u8]) -> Frame {
    Frame::BulkString(Bytes::from_static(s))
}

/// Coordinate BITOP across shards.
///
/// `BITOP <op> dest src [src ...]` — sources are gathered (local read or
/// remote GET), combined via the same `bitop_compute` the local path uses,
/// and the result is written on DEST's owning shard (SET, or DEL when the
/// combine is empty). Full Redis semantics: BITOP is string-only by spec,
/// so value transfer is exact; WRONGTYPE from any source propagates.
#[allow(clippy::too_many_arguments)]
async fn coordinate_bitop(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    local_barrier_pending: &mut bool,
    _response_pool: &(),
) -> Frame {
    // Single-shard server: straight to local dispatch — zero coordinator
    // overhead (no key vec, no owner hashing) on the 1-shard hot path.
    if num_shards == 1 {
        return run_local(shard_databases, db_index, cached_clock, b"BITOP", args);
    }
    if args.len() < 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'bitop' command",
        ));
    }
    let (Some(op), Some(dest)) = (extract_key(&args[0]), extract_key(&args[1])) else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'bitop' command",
        ));
    };
    // Redis validation order: NOT arity errors before any key is touched.
    if op.eq_ignore_ascii_case(b"NOT") && args.len() != 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR BITOP NOT requires one and only one key",
        ));
    }
    let mut src_keys: Vec<Bytes> = Vec::with_capacity(args.len() - 2);
    for arg in &args[2..] {
        match extract_key(arg) {
            Some(k) => src_keys.push(k),
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'bitop' command",
                ));
            }
        }
    }

    // Single-owner fast path: every key (dest included) on one shard —
    // forward the whole command for byte-identical local semantics.
    let dest_shard = key_to_shard(&dest, num_shards);
    if src_keys
        .iter()
        .all(|k| key_to_shard(k, num_shards) == dest_shard)
    {
        let mut parts: Vec<Frame> = Vec::with_capacity(args.len() + 1);
        parts.push(bulk_static(b"BITOP"));
        parts.extend_from_slice(args);
        // Write command: the local-owner case must persist (v3-5 carried gap).
        // Any non-error BITOP mutates dest (SET result, or DEL on empty).
        return run_on_owner_persist(
            &dest,
            &parts,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            |_| true,
        )
        .await;
    }

    // Gather sources: group by shard ascending (VLL), local legs direct,
    // remote legs as GET batches.
    let mut groups: BTreeMap<usize, Vec<(usize, Bytes)>> = BTreeMap::new();
    for (i, k) in src_keys.iter().enumerate() {
        groups
            .entry(key_to_shard(k, num_shards))
            .or_default()
            .push((i, k.clone()));
    }
    let mut sources: Vec<Option<Vec<u8>>> = vec![None; src_keys.len()];
    let mut pending: Vec<(Vec<usize>, channel::OneshotReceiver<Vec<Frame>>)> = Vec::new();
    for (shard_id, indexed) in &groups {
        if *shard_id == my_shard {
            for (idx, key) in indexed {
                let reply = run_local(
                    shard_databases,
                    db_index,
                    cached_clock,
                    b"GET",
                    &[bulk(key)],
                );
                match reply {
                    Frame::BulkString(v) => sources[*idx] = Some(v.to_vec()),
                    Frame::Error(e) => return Frame::Error(e),
                    _ => sources[*idx] = Some(Vec::new()),
                }
            }
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = indexed
                .iter()
                .map(|(_, k)| {
                    (
                        k.clone(),
                        Frame::Array(framevec![bulk_static(b"GET"), bulk(k)]),
                    )
                })
                .collect();
            let indices: Vec<usize> = indexed.iter().map(|(i, _)| *i).collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            let _ = spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending.push((indices, reply_rx));
        }
    }
    for (indices, reply_rx) in pending {
        match recv_reply_bounded(reply_rx).await {
            Ok(frames) => {
                for (idx, frame) in indices.into_iter().zip(frames) {
                    match frame {
                        Frame::BulkString(v) => sources[idx] = Some(v.to_vec()),
                        Frame::Error(e) => return Frame::Error(e),
                        _ => sources[idx] = Some(Vec::new()),
                    }
                }
            }
            Err(_) => {
                return Frame::Error(Bytes::from_static(b"ERR cross-shard reply channel closed"));
            }
        }
    }
    let gathered: Vec<Vec<u8>> = sources.into_iter().map(Option::unwrap_or_default).collect();

    match crate::command::string::bitop_compute(&op, &gathered) {
        Err(e) => e,
        Ok(None) => {
            // All sources empty/missing — dest is deleted, reply 0.
            let reply = run_on_owner_persist(
                &dest,
                &[bulk_static(b"DEL"), bulk(&dest)],
                my_shard,
                num_shards,
                db_index,
                shard_databases,
                dispatch_tx,
                spsc_notifiers,
                cached_clock,
                aof_pool,
                repl_state,
                local_barrier_pending,
                // DEL of an absent dest wrote nothing — skip the no-op record.
                |r| matches!(r, Frame::Integer(n) if *n > 0),
            )
            .await;
            if let Frame::Error(e) = reply {
                return Frame::Error(e);
            }
            Frame::Integer(0)
        }
        Ok(Some(result)) => {
            let len = result.len() as i64;
            let reply = run_on_owner_persist(
                &dest,
                &[
                    bulk_static(b"SET"),
                    bulk(&dest),
                    Frame::BulkString(Bytes::from(result)),
                ],
                my_shard,
                num_shards,
                db_index,
                shard_databases,
                dispatch_tx,
                spsc_notifiers,
                cached_clock,
                aof_pool,
                repl_state,
                local_barrier_pending,
                |_| true, // plain SET always writes on success
            )
            .await;
            if let Frame::Error(e) = reply {
                return Frame::Error(e);
            }
            Frame::Integer(len)
        }
    }
}

/// Coordinate COPY across shards.
///
/// `COPY src dst [REPLACE]` — same-shard pairs (hash tags) forward to the
/// owning shard and keep full any-type fidelity via the local copy path.
/// Cross-shard pairs transfer STRING values exactly (value + TTL, NX unless
/// REPLACE); cross-shard non-string values return an explicit error instead
/// of silently corrupting (full-fidelity transfer is DUMP/RESTORE territory,
/// tracked in the task backlog). `COPY ... DB n` never reaches this path
/// (excluded in `is_multi_key_command`; the handlers' two-db interception
/// keeps owning it).
#[allow(clippy::too_many_arguments)]
async fn coordinate_copy(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    local_barrier_pending: &mut bool,
    _response_pool: &(),
) -> Frame {
    // Single-shard server: straight to local dispatch — zero coordinator
    // overhead on the 1-shard hot path.
    if num_shards == 1 {
        return run_local(shard_databases, db_index, cached_clock, b"COPY", args);
    }
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'copy' command",
        ));
    }
    let (Some(src), Some(dst)) = (extract_key(&args[0]), extract_key(&args[1])) else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'copy' command",
        ));
    };
    let mut replace = false;
    for opt in &args[2..] {
        match extract_key(opt) {
            Some(o) if o.eq_ignore_ascii_case(b"REPLACE") => replace = true,
            _ => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        }
    }

    let src_shard = key_to_shard(&src, num_shards);
    let dst_shard = key_to_shard(&dst, num_shards);

    // Same owner: forward the whole COPY — full any-type fidelity.
    if src_shard == dst_shard {
        let mut parts: Vec<Frame> = Vec::with_capacity(args.len() + 1);
        parts.push(bulk_static(b"COPY"));
        parts.extend_from_slice(args);
        // Write command: the local-owner case must persist (v3-5 carried gap).
        return run_on_owner_persist(
            &src,
            &parts,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            // COPY :0 = refused (dst exists, no REPLACE) — nothing written.
            |r| matches!(r, Frame::Integer(1)),
        )
        .await;
    }

    // Cross-shard: read value + TTL from src's shard.
    let value = match run_on_owner(
        &src,
        &[bulk_static(b"GET"), bulk(&src)],
        my_shard,
        num_shards,
        db_index,
        shard_databases,
        dispatch_tx,
        spsc_notifiers,
        cached_clock,
    )
    .await
    {
        Frame::BulkString(v) => v,
        Frame::Error(e) if e.starts_with(b"WRONGTYPE") => {
            return Frame::Error(Bytes::from_static(
                b"ERR COPY across shards supports only string values; co-locate the keys with {hash} tags for other types",
            ));
        }
        Frame::Error(e) => return Frame::Error(e),
        _ => return Frame::Integer(0), // src missing
    };
    let ttl_ms = match run_on_owner(
        &src,
        &[bulk_static(b"PTTL"), bulk(&src)],
        my_shard,
        num_shards,
        db_index,
        shard_databases,
        dispatch_tx,
        spsc_notifiers,
        cached_clock,
    )
    .await
    {
        Frame::Integer(t) if t > 0 => Some(t),
        Frame::Integer(-2) => return Frame::Integer(0), // expired between reads
        _ => None,
    };

    // Write to dst's shard: NX unless REPLACE, then restore TTL.
    let set_parts: Vec<Frame> = if replace {
        vec![bulk_static(b"SET"), bulk(&dst), Frame::BulkString(value)]
    } else {
        vec![
            bulk_static(b"SET"),
            bulk(&dst),
            Frame::BulkString(value),
            bulk_static(b"NX"),
        ]
    };
    let set_reply = run_on_owner_persist(
        &dst,
        &set_parts,
        my_shard,
        num_shards,
        db_index,
        shard_databases,
        dispatch_tx,
        spsc_notifiers,
        cached_clock,
        aof_pool,
        repl_state,
        local_barrier_pending,
        // Null = NX refused (dst exists) — nothing was written.
        |r| matches!(r, Frame::SimpleString(_)),
    )
    .await;
    match set_reply {
        Frame::SimpleString(_) => {}
        Frame::Error(e) => return Frame::Error(e),
        // Null reply = NX refused (dst exists); anything else is unexpected.
        _ => return Frame::Integer(0),
    }
    if let Some(t) = ttl_ms {
        let mut ttl_buf = itoa::Buffer::new();
        let reply = run_on_owner_persist(
            &dst,
            &[
                bulk_static(b"PEXPIRE"),
                bulk(&dst),
                Frame::BulkString(Bytes::copy_from_slice(ttl_buf.format(t).as_bytes())),
            ],
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            aof_pool,
            repl_state,
            local_barrier_pending,
            // :0 = key vanished between SET and PEXPIRE — no TTL was set.
            |r| matches!(r, Frame::Integer(1)),
        )
        .await;
        if let Frame::Error(e) = reply {
            return Frame::Error(e);
        }
    }
    Frame::Integer(1)
}

/// Extract Bytes from a Frame argument.
fn extract_key(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// Send a ShardMessage via SPSC with **bounded** backpressure retry on a full
/// ring (R-1, design-for-failure).
///
/// Calls `notify_one()` on the target shard's notifier after a successful push
/// for immediate wake (avoids relying on the 1ms periodic timer safety net).
///
/// Retries up to [`CROSS_SHARD_PUSH_MAX_RETRIES`] with a [`CROSS_SHARD_PUSH_BACKOFF`]
/// sleep between attempts, then gives up and returns [`PushOutcome::Backpressure`].
/// This replaces the previous **unbounded** `loop { try_push; yield/sleep }`,
/// which (a) busy-spun a full core on tokio — `yield_now()` reschedules with no
/// backoff — and (b) could block graceful shutdown forever on a wedged target.
///
/// **Give-up semantics:** on `Backpressure` the message (`pending`) is dropped.
/// For a reply-carrying message (`MultiExecute`/`MultiExecuteSlotted`/…) this
/// drops the embedded reply sender, so the awaiting caller's `reply_rx.recv()`
/// resolves to `Err` and it synthesizes a per-shard error for that slice of the
/// response — the same closed-channel path callers already handle. Fire-and-
/// forget messages become best-effort: a target that stayed full for the whole
/// ~0.5s budget is effectively wedged, so dropping is the correct failure mode
/// rather than spinning forever.
///
/// The return value lets future call sites branch on the outcome explicitly;
/// existing statement-form callers (`spsc_send(...).await;`) simply discard it.
///
/// Exposed at `pub(crate)` so sibling files (e.g. `scatter_aggregate`)
/// can dispatch via the same contention-safe path.
pub(crate) async fn spsc_send(
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    my_shard: usize,
    target_shard: usize,
    msg: ShardMessage,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> PushOutcome {
    spsc_send_bounded(
        dispatch_tx,
        my_shard,
        target_shard,
        msg,
        spsc_notifiers,
        CROSS_SHARD_PUSH_MAX_RETRIES,
        CROSS_SHARD_PUSH_BACKOFF,
    )
    .await
}

/// Retry budget is parameterized so tests can drive the give-up path with a
/// tiny bound; production always calls it via [`spsc_send`] with the shared
/// [`CROSS_SHARD_PUSH_MAX_RETRIES`] / [`CROSS_SHARD_PUSH_BACKOFF`] constants.
async fn spsc_send_bounded(
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    my_shard: usize,
    target_shard: usize,
    msg: ShardMessage,
    spsc_notifiers: &[Arc<channel::Notify>],
    max_retries: u32,
    backoff: std::time::Duration,
) -> PushOutcome {
    let target_idx = ChannelMesh::target_index(my_shard, target_shard);
    let mut pending = msg;

    // Fast path: try once with no sleep. The borrow is scoped to the block so
    // it is dropped before any `.await` (a RefCell borrow must not be held
    // across a yield) and before `notify_one`.
    let push_result = {
        let mut producers = dispatch_tx.borrow_mut();
        producers[target_idx].try_push(pending)
    };
    match push_result {
        Ok(()) => {
            spsc_notifiers[target_shard].notify_one();
            return PushOutcome::Pushed;
        }
        Err(val) => pending = val,
    }

    // Brief spin before the first timed sleep: the consumer is another OS
    // thread actively draining, so a transiently-full ring often frees a slot
    // within nanoseconds — far below the 100µs backoff (which runtime timers
    // may round up to ~1ms). This preserves the old 10µs-class latency for
    // bursty-but-healthy targets without touching the wedged-target budget.
    // Bounded and tiny (≤64 iterations), so it cannot convoy siblings the way
    // an unbounded reply-side spin did (see the C2 solo-conn gate lesson).
    for _ in 0..64 {
        std::hint::spin_loop();
        let push_result = {
            let mut producers = dispatch_tx.borrow_mut();
            producers[target_idx].try_push(pending)
        };
        match push_result {
            Ok(()) => {
                spsc_notifiers[target_shard].notify_one();
                return PushOutcome::Pushed;
            }
            Err(val) => pending = val,
        }
    }

    // Bounded retry with backoff.
    for _ in 0..max_retries {
        // Back off before retrying so a full ring cannot hot-spin the core.
        // No borrow is held across this await.
        #[cfg(feature = "runtime-tokio")]
        tokio::time::sleep(backoff).await;
        #[cfg(feature = "runtime-monoio")]
        monoio::time::sleep(backoff).await;

        let push_result = {
            let mut producers = dispatch_tx.borrow_mut();
            producers[target_idx].try_push(pending)
        };
        match push_result {
            Ok(()) => {
                spsc_notifiers[target_shard].notify_one();
                return PushOutcome::Pushed;
            }
            Err(val) => pending = val,
        }
    }

    // Budget exhausted: the target ring never drained. Drop `pending` (and any
    // embedded reply sender) so awaiting callers fail loud instead of hanging.
    // Rare by construction (~0.5s of failed retries), so the warn + labeled
    // counter cannot flood.
    tracing::warn!(
        my_shard,
        target_shard,
        "cross-shard dispatch dropped after backpressure budget — target not draining"
    );
    crate::admin::metrics_setup::record_xshard_backpressure_drop(target_shard);
    PushOutcome::Backpressure
}

/// Broadcast a keyless flush (FLUSHDB/FLUSHALL) to every OTHER shard (D-2).
///
/// FLUSHDB/FLUSHALL are keyless, so `extract_primary_key` routes them
/// local-only — a normal client's flush previously cleared just the local
/// shard's selected db, silently leaving the other `num_shards - 1` shards'
/// keyspaces intact (ghost keys, stale reads, DBSIZE > 0 after FLUSHALL).
///
/// Each remote leg is shipped as a `MultiExecute` carrying the original
/// command frame, so the target shard runs it through its normal SPSC arm:
/// dispatch + per-shard AOF/WAL persistence (fail-loud `AOF_APPEND_LOST`) +
/// vector/text index clearing all apply exactly as for the local leg.
///
/// Legs run concurrently (all sends first, then all acks awaited). Like
/// SWAPDB's broadcast, this is not atomic across shards: a concurrent read
/// can observe shard A flushed while shard B is not yet — Redis-cluster-
/// relaxed semantics. On any failed leg the caller receives an explicit
/// partial-flush error naming the shard, never a silent partial `+OK`.
/// `my_shard` is the SENDER — the shard this call runs on, which `spsc_send`
/// needs to pick the right producer. `skip_shard` is the leg already flushed.
///
/// They are the same for a flush typed on a connection, and DIFFERENT for one
/// a routed script or a routed MULTI performed on the owner shard (moon#705).
/// Overloading one parameter for both silently sent from a shard the caller is
/// not running on while leaving the caller's OWN shard full — measured at
/// `--shards 4` as 5 of 12 keys surviving a routed script's `FLUSHALL`.
pub(crate) async fn coordinate_flush_broadcast(
    command: &Frame,
    my_shard: usize,
    skip_shard: usize,
    num_shards: usize,
    db_index: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Result<(), Frame> {
    let mut pending = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == skip_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::MultiExecute {
            db_index,
            // Keyless command: the routing key is unused by the flush arm.
            commands: vec![(Bytes::new(), command.clone())],
            reply_tx,
        };
        // The SPSC mesh has NO self-loop — `ChannelMesh::target_index`
        // debug-asserts `my_id != target_id`, and in release it silently
        // computes a neighbour's index instead. Before moon#705 split
        // `skip_shard` from `my_shard` this leg could not arise (the sender was
        // always the skipped shard); now it can, and it has to go through the
        // thread-local self queue the event loop drains ahead of the SPSC
        // consumers. Measured while getting this wrong: a routed script's
        // `FLUSHALL` cleared one shard twice and left the caller's own slice
        // full, so 2 of 12 keys survived a `+OK`.
        let outcome = if target == my_shard {
            crate::shard::self_msg::push(msg);
            crate::shard::dispatch::PushOutcome::Pushed
        } else {
            spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await
        };
        pending.push((target, reply_rx, outcome));
    }

    let mut failed: Option<usize> = None;
    for (target, reply_rx, outcome) in pending {
        if outcome != crate::shard::dispatch::PushOutcome::Pushed {
            failed.get_or_insert(target);
            continue;
        }
        match recv_reply_bounded(reply_rx).await {
            Ok(frames) if frames.iter().all(|f| !matches!(f, Frame::Error(_))) => {}
            _ => {
                failed.get_or_insert(target);
            }
        }
    }
    match failed {
        None => Ok(()),
        Some(target) => {
            tracing::error!(
                my_shard,
                target,
                "flush broadcast: remote leg failed — keyspace partially flushed"
            );
            Err(Frame::Error(Bytes::from(format!(
                "MOONERR FLUSH partial: shard {target} leg failed or unreachable — \
                 local shard flushed; retry the FLUSH command"
            ))))
        }
    }
}

/// Fan out the flushes a MULTI/EXEC body performed locally (c10k E2).
///
/// `execute_transaction_sharded` clears only the slice it runs on, so a queued
/// `FLUSHDB`/`FLUSHALL` empties one shard of N while `EXEC` still answers
/// `+OK`. Measured at `--shards 4`: 45 of 64 keys survived a transaction that
/// reported success. The live (non-MULTI) path has broadcast since D-2; this
/// gives the transactional path the same guarantee.
///
/// `exec_shard` is the shard that RAN the body — the owner for a routed
/// transaction, not necessarily the originator — so it is the one leg already
/// flushed and correctly skipped.
///
/// On a failed leg the corresponding element of the `EXEC` result array is
/// replaced with the partial-flush error, so a client that sees `+OK` for a
/// flush inside a transaction can rely on it, exactly as on the live path.
/// Non-atomic across shards, like every other broadcast here: a concurrent
/// reader can see shard A flushed before shard B. `MULTI` does not and cannot
/// change that in a shared-nothing engine — it bounds the report, not the
/// visibility.
/// `my_shard` is the shard this call RUNS on (the originator, always), and
/// `exec_shard` is the shard that ran the body. They differ for a ROUTED
/// transaction, and conflating them — as this did before moon#705 split the
/// two roles in `coordinate_flush_broadcast` — sends from a shard the caller
/// is not on and leaves the originator's own slice unflushed.
pub(crate) async fn broadcast_txn_flushes(
    result: &mut Frame,
    exec_flushes: &[(usize, Frame, usize)],
    my_shard: usize,
    exec_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) {
    if exec_flushes.is_empty() || num_shards <= 1 {
        return;
    }
    for (result_index, command, db_index) in exec_flushes {
        if let Err(err) = coordinate_flush_broadcast(
            command,
            my_shard,
            exec_shard,
            num_shards,
            *db_index,
            dispatch_tx,
            spsc_notifiers,
        )
        .await
            && let Frame::Array(items) = result
            && let Some(slot) = items.get_mut(*result_index)
        {
            *slot = err;
        }
    }
}

/// Coordinate MGET across shards using VLL pattern.
///
/// Groups keys by shard in a BTreeMap (ascending shard-ID order), executes
/// local keys directly, dispatches remote keys via MultiExecute batches,
/// reassembles results in original order.
async fn coordinate_mget(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'mget' command",
        ));
    }

    // Group keys by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<(usize, Bytes)>> = BTreeMap::new();
    for (i, arg) in args.iter().enumerate() {
        if let Some(key) = extract_key(arg) {
            let shard = key_to_shard(&key, num_shards);
            groups.entry(shard).or_default().push((i, key));
        }
    }

    // Fast path: all keys on local shard -- use mget directly
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        return crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            crate::command::string::mget(db, args)
        });
    }

    let total = args.len();
    let mut results: Vec<Option<Frame>> = vec![None; total];
    let mut pending_shards: Vec<(Vec<usize>, channel::OneshotReceiver<Vec<Frame>>)> = Vec::new();

    // Iterate in ascending shard-ID order (BTreeMap guarantees this)
    for (shard_id, indexed_keys) in &groups {
        let original_indices: Vec<usize> = indexed_keys.iter().map(|(i, _)| *i).collect();

        if *shard_id == my_shard {
            // Local execution: GET each key directly
            crate::shard::slice::with_shard_db(db_index, |db| {
                db.refresh_now_from_cache(cached_clock);
                for (orig_idx, key) in indexed_keys {
                    let entry = db.get(key);
                    let frame = match entry {
                        Some(e) => match e.value.as_bytes() {
                            Some(v) => Frame::BulkString(Bytes::copy_from_slice(v)),
                            None => Frame::Null,
                        },
                        None => Frame::Null,
                    };
                    results[*orig_idx] = Some(frame);
                }
            });
        } else {
            // Remote dispatch: batch of GET commands via MultiExecuteSlotted
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = indexed_keys
                .iter()
                .map(|(_, k)| {
                    let cmd = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"GET")),
                        Frame::BulkString(k.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            let _ = spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push((original_indices, reply_rx));
        }
    }

    // Await all remote results
    for (indices, reply_rx) in pending_shards {
        match recv_reply_bounded(reply_rx).await {
            Ok(frames) => {
                for (idx, frame) in indices.into_iter().zip(frames) {
                    results[idx] = Some(frame);
                }
            }
            Err(_) => {
                // Channel closed — target shard dropped without responding.
                for idx in indices {
                    results[idx] = Some(Frame::Error(Bytes::from_static(
                        b"ERR cross-shard reply channel closed",
                    )));
                }
            }
        }
    }

    // Assemble in original order
    Frame::Array(
        results
            .into_iter()
            .map(|opt| opt.unwrap_or(Frame::Null))
            .collect(),
    )
}

/// Coordinate MSET across shards using VLL pattern.
///
/// Groups key-value pairs by shard in ascending order, dispatches SET
/// sub-commands per shard. Returns OK when all complete.
async fn coordinate_mset(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    local_barrier_pending: &mut bool,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'mset' command",
        ));
    }

    // Group key-value pairs by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<(Bytes, Bytes)>> = BTreeMap::new();
    for pair in args.chunks(2) {
        let key = match extract_key(&pair[0]) {
            Some(k) => k,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ));
            }
        };
        let value = match extract_key(&pair[1]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ));
            }
        };
        let shard = key_to_shard(&key, num_shards);
        groups.entry(shard).or_default().push((key, value));
    }

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let resp = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            crate::command::string::mset(db, args)
        });
        // Local leg (review Finding 1): persist the whole MSET — every key is
        // owned by my_shard — matching the local single-key write contract.
        if let Some(pairs) = groups.get(&my_shard) {
            let serialized = serialize_local_mset(pairs);
            match persist_local_leg(aof_pool, repl_state, my_shard, db_index, serialized).await {
                Ok(needs_barrier) => *local_barrier_pending |= needs_barrier,
                Err(()) => {
                    return Frame::Error(Bytes::from_static(
                        crate::persistence::aof::AOF_FSYNC_ERR,
                    ));
                }
            }
        }
        return resp;
    }

    let mut pending_shards: Vec<channel::OneshotReceiver<Vec<Frame>>> = Vec::new();

    for (shard_id, kv_pairs) in &groups {
        if *shard_id == my_shard {
            crate::shard::slice::with_shard_db(db_index, |db| {
                db.refresh_now_from_cache(cached_clock);
                for (key, value) in kv_pairs {
                    db.set_string(key, value.clone());
                }
            });
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = kv_pairs
                .iter()
                .map(|(k, v)| {
                    let cmd = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"SET")),
                        Frame::BulkString(k.clone()),
                        Frame::BulkString(v.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            let _ = spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push(reply_rx);
        }
    }

    // Drain ALL remote acks even after a failure (every leg was already
    // dispatched, and the local leg below must still persist what this shard
    // applied) — but a timed-out, closed, or errored leg must NOT collapse
    // into OK: that would acknowledge an unconfirmed distributed write.
    let mut leg_err: Option<Frame> = None;
    for reply_rx in pending_shards {
        match recv_reply_bounded(reply_rx).await {
            Ok(frames) => {
                if leg_err.is_none()
                    && let Some(err) = frames.into_iter().find(|f| matches!(f, Frame::Error(_)))
                {
                    leg_err = Some(err);
                }
            }
            Err(_) => {
                if leg_err.is_none() {
                    leg_err = Some(Frame::Error(Bytes::from_static(
                        b"ERR cross-shard MSET leg unconfirmed (timeout or closed reply channel); write may be partially applied",
                    )));
                }
            }
        }
    }

    // Local leg (review Finding 1): persist a synthesized MSET over ONLY the
    // local keys. The remote slices persisted themselves on their owner shards
    // via MultiExecute -> wal_append_and_fanout; my_shard must not log their keys
    // (replay re-dispatches raw commands, so a full-command log here would try to
    // write keys this shard doesn't own).
    if let Some(pairs) = groups.get(&my_shard) {
        let serialized = serialize_local_mset(pairs);
        match persist_local_leg(aof_pool, repl_state, my_shard, db_index, serialized).await {
            Ok(needs_barrier) => *local_barrier_pending |= needs_barrier,
            Err(()) => {
                return Frame::Error(Bytes::from_static(crate::persistence::aof::AOF_FSYNC_ERR));
            }
        }
    }

    if let Some(err) = leg_err {
        return err;
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Coordinate MSETNX across shards.
///
/// MSETNX is atomic by contract: set every pair iff *none* of the keys already
/// exist. Moon cannot honor that atomically across shards (like MSET, cross-shard
/// writes scatter with no two-phase commit or rollback), so — by design — MSETNX
/// is rejected with a CROSSSLOT error when its keys hash to more than one shard.
/// When all keys are co-located on a single shard (including via `{hash-tag}`),
/// the whole command runs atomically on that shard's owner.
#[allow(clippy::too_many_arguments)]
async fn coordinate_msetnx(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    local_barrier_pending: &mut bool,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'msetnx' command",
        ));
    }

    // Every key must hash to the same shard; otherwise MSETNX cannot be atomic.
    let first_key = match extract_key(&args[0]) {
        Some(k) => k,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'msetnx' command",
            ));
        }
    };
    let owner = key_to_shard(&first_key, num_shards);
    for pair in args.chunks(2) {
        let key = match extract_key(&pair[0]) {
            Some(k) => k,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'msetnx' command",
                ));
            }
        };
        if key_to_shard(&key, num_shards) != owner {
            return Frame::Error(Bytes::from_static(
                b"CROSSSLOT Keys in MSETNX request don't hash to the same shard",
            ));
        }
    }

    // All keys co-located -> run the whole MSETNX atomically on the owning shard.
    // Branch on ownership explicitly (rather than via run_on_owner) so the LOCAL
    // leg can persist to my_shard's AOF on a successful write (review Finding 1);
    // the REMOTE leg persists on the owner via MultiExecute -> wal_append_and_fanout.
    let mut command_parts: Vec<Frame> = Vec::with_capacity(args.len() + 1);
    command_parts.push(Frame::BulkString(Bytes::from_static(b"MSETNX")));
    command_parts.extend_from_slice(args);
    if owner == my_shard {
        let resp = run_local(shard_databases, db_index, cached_clock, b"MSETNX", args);
        // Persist only on an actual write (:1). A :0 means some key already
        // existed and MSETNX wrote nothing — there is nothing to log.
        if matches!(resp, Frame::Integer(1)) {
            let serialized =
                crate::persistence::aof::serialize_command(&Frame::Array(command_parts.into()));
            match persist_local_leg(aof_pool, repl_state, my_shard, db_index, serialized).await {
                Ok(needs_barrier) => *local_barrier_pending |= needs_barrier,
                Err(()) => {
                    return Frame::Error(Bytes::from_static(
                        crate::persistence::aof::AOF_FSYNC_ERR,
                    ));
                }
            }
        }
        resp
    } else {
        run_remote(
            owner,
            &first_key,
            Frame::Array(command_parts.into()),
            my_shard,
            db_index,
            dispatch_tx,
            spsc_notifiers,
        )
        .await
    }
}

/// Coordinate DEL/UNLINK/EXISTS with multiple keys across shards using VLL pattern.
///
/// Groups keys by shard in ascending order (BTreeMap), dispatches sub-commands
/// per shard via MultiExecute, sums integer results.
async fn coordinate_multi_del_or_exists(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
    local_barrier_pending: &mut bool,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let cmd_upper = cmd.to_ascii_uppercase();
    // DEL/UNLINK mutate and must persist their in-process legs; EXISTS/TOUCH
    // read (TOUCH updates access time only — never AOF-logged, like Redis).
    let is_delete = cmd_upper == b"DEL" || cmd_upper == b"UNLINK";

    // Group keys by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<Frame>> = BTreeMap::new();
    for arg in args {
        if let Some(key) = extract_key(arg) {
            let shard = key_to_shard(&key, num_shards);
            groups.entry(shard).or_default().push(arg.clone());
        }
    }

    // db_count() lives on ShardDatabases — read once, share across both branches.
    let db_count = shard_databases.db_count();

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let mut selected = db_index;
        let result = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            cmd_dispatch(db, cmd, args, &mut selected, db_count)
        });
        let resp = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };
        // v3-5 carried gap: the in-process DEL/UNLINK never reached the AOF —
        // deleted keys RESURRECTED from the seed writes on restart. Persist
        // only when something was actually removed (n=0 replays identically
        // without a record).
        if is_delete && matches!(resp, Frame::Integer(n) if n > 0) {
            let mut parts: Vec<Frame> = Vec::with_capacity(args.len() + 1);
            parts.push(Frame::BulkString(Bytes::from(cmd_upper.clone())));
            parts.extend_from_slice(args);
            let serialized =
                crate::persistence::aof::serialize_command(&Frame::Array(parts.into()));
            match persist_local_leg(aof_pool, repl_state, my_shard, db_index, serialized).await {
                Ok(needs_barrier) => *local_barrier_pending |= needs_barrier,
                Err(()) => {
                    return Frame::Error(Bytes::from_static(
                        crate::persistence::aof::AOF_FSYNC_ERR,
                    ));
                }
            }
        }
        return resp;
    }

    let mut total_count: i64 = 0;
    let mut pending_shards: Vec<channel::OneshotReceiver<Vec<Frame>>> = Vec::new();

    for (shard_id, key_args) in &groups {
        if *shard_id == my_shard {
            let mut selected = db_index;
            let result = crate::shard::slice::with_shard_db(db_index, |db| {
                db.refresh_now_from_cache(cached_clock);
                cmd_dispatch(db, cmd, key_args, &mut selected, db_count)
            });
            if let DispatchResult::Response(Frame::Integer(n)) = result {
                total_count += n;
                // v3-5 carried gap: persist the local slice (synthesized over
                // ONLY the keys this shard owns — remote slices persist on
                // their owners via MultiExecute). Skip when nothing removed.
                if is_delete && n > 0 {
                    let mut parts: Vec<Frame> = Vec::with_capacity(key_args.len() + 1);
                    parts.push(Frame::BulkString(Bytes::from(cmd_upper.clone())));
                    parts.extend_from_slice(key_args);
                    let serialized =
                        crate::persistence::aof::serialize_command(&Frame::Array(parts.into()));
                    match persist_local_leg(aof_pool, repl_state, my_shard, db_index, serialized)
                        .await
                    {
                        Ok(needs_barrier) => *local_barrier_pending |= needs_barrier,
                        Err(()) => {
                            return Frame::Error(Bytes::from_static(
                                crate::persistence::aof::AOF_FSYNC_ERR,
                            ));
                        }
                    }
                }
            }
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = key_args
                .iter()
                .map(|arg| {
                    let key = extract_key(arg).unwrap_or_default();
                    let cmd_frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from(cmd_upper.clone())),
                        arg.clone(),
                    ]);
                    (key, cmd_frame)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            let _ = spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push(reply_rx);
        }
    }

    for reply_rx in pending_shards {
        match recv_reply_bounded(reply_rx).await {
            Ok(frames) => {
                for frame in frames {
                    match frame {
                        Frame::Integer(n) => total_count += n,
                        Frame::Error(_) => return frame,
                        _ => {}
                    }
                }
            }
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DEL/UNLINK",
                ));
            }
        }
    }

    Frame::Integer(total_count)
}

/// Send a script to the ONE shard that owns all of its keys, and return that
/// shard's reply verbatim (moon#508).
///
/// Unlike every other coordinator here this is not a fan-out: a script runs
/// against a single shard's database, so there is exactly one correct place
/// for it. `scripting::route_script_keys` has already established that every
/// key maps to `target`; this only moves the call there.
///
/// The reply is passed through untouched, including errors — a `NOSCRIPT` from
/// the target shard is a real answer about the target's script cache, and
/// rewriting it here would hide a cache fan-out gap.
///
/// moon#705: the second return value is the flush the routed script issued but
/// could NOT fan out from inside the owner's message loop. It is returned
/// rather than acted on here because this function does not know which shard
/// ran the body relative to the caller's own — `route_script_elsewhere` does,
/// and completes it there.
#[allow(clippy::too_many_arguments)]
pub async fn coordinate_script(
    command: std::sync::Arc<Frame>,
    target: usize,
    my_shard: usize,
    db_index: usize,
    script_acl: crate::acl::ScriptAcl,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> (Frame, Option<crate::scripting::pending_flush::PendingFlush>) {
    let (reply_tx, reply_rx) = channel::oneshot();
    let msg = ShardMessage::Execute {
        db_index,
        command,
        // moon#569: the caller's identity rides along, so the target shard
        // authorizes every `redis.call` exactly as the origin shard would.
        script_acl,
        reply_tx,
    };
    // Both failure outcomes mean the script was NEVER executed, so each can be
    // reported as a clean reject rather than an ambiguous "maybe it ran".
    match spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await {
        crate::shard::dispatch::PushOutcome::Pushed => {}
        crate::shard::dispatch::PushOutcome::Backpressure => {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR shard owning the script's keys is not draining; script not executed",
                )),
                None,
            );
        }
        crate::shard::dispatch::PushOutcome::Cancelled => {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR shutting down; script not executed",
                )),
                None,
            );
        }
    }
    // Past this point the script IS in the target's queue, so no failure here
    // can claim it did not run — only that we never heard back. Saying
    // otherwise invites a client to re-send a non-idempotent script that
    // already applied its writes.
    match recv_reply_bounded_reason(reply_rx).await {
        Ok(reply) => (reply.frame, reply.script_flush),
        Err(ReplyFailure::TimedOut) => {
            // Mirrors the handler reply paths, which already record this;
            // without it a wedged owner shard is invisible in metrics.
            crate::admin::metrics_setup::record_xshard_reply_timeout("script");
            // No flush note, and deliberately not an assumption that there was
            // none: we never heard back, so there is nothing to complete and
            // nothing that could be completed correctly. The reply already
            // says the execution status is unknown.
            (
                Frame::Error(Bytes::from_static(
                    b"ERR timeout waiting for the shard owning the script's keys; script execution status is unknown",
                )),
                None,
            )
        }
        Err(ReplyFailure::Closed) => (
            Frame::Error(Bytes::from_static(
                b"ERR cross-shard reply channel closed; script execution status is unknown",
            )),
            None,
        ),
    }
}

/// Coordinate KEYS across all shards.
///
/// Dispatches KEYS command to every shard, collects and merges results.
pub async fn coordinate_keys(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'keys' command",
        ));
    }

    let mut all_keys: Vec<Frame> = Vec::new();
    let mut pending_shards: Vec<channel::OneshotReceiver<crate::shard::dispatch::ExecReply>> =
        Vec::new();

    // Execute locally on this shard
    {
        let db_count = shard_databases.db_count();
        let mut selected = db_index;
        let result = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            cmd_dispatch(db, b"KEYS", args, &mut selected, db_count)
        });
        if let DispatchResult::Response(Frame::Array(keys)) = result {
            all_keys.extend(keys);
        }
    }

    // Dispatch to all remote shards
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = {
            let mut parts = vec![Frame::BulkString(Bytes::from_static(b"KEYS"))];
            for a in args {
                parts.push(a.clone());
            }
            Frame::Array(parts.into())
        };
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            // Never a script: this fan-out builds its own keyspace command.
            // Fail-closed anyway (moon#569).
            script_acl: crate::acl::ScriptAcl::deny(),
            reply_tx,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    // Collect remote results
    for reply_rx in pending_shards {
        match recv_reply_bounded(reply_rx).await.map(|r| r.frame) {
            Ok(Frame::Array(keys)) => all_keys.extend(keys),
            Ok(_) => {} // Non-array response (e.g., error) — skip
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during KEYS",
                ));
            }
        }
    }

    Frame::Array(all_keys.into())
}

/// Coordinate SCAN across all shards.
///
/// Cursor encoding: upper 16 bits = shard index, lower 48 bits = per-shard cursor.
/// This allows SCAN to iterate through all shards sequentially.
pub async fn coordinate_scan(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'scan' command",
        ));
    }

    // Parse the composite cursor from the first arg
    let cursor_val: i64 = match &args[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
        Frame::Integer(n) => *n,
        _ => 0,
    };
    let cursor_u64 = cursor_val as u64;

    // Decode: upper 16 bits = shard index, lower 48 bits = per-shard cursor
    let current_shard = ((cursor_u64 >> 48) & 0xFFFF) as usize;
    let shard_cursor = (cursor_u64 & 0x0000_FFFF_FFFF_FFFF) as i64;

    // Determine the target shard (may differ from my_shard)
    let target_shard_id = current_shard.min(num_shards - 1);

    // Build the SCAN command with the per-shard cursor
    let mut scan_args = vec![Frame::BulkString(Bytes::from(shard_cursor.to_string()))];
    // Forward remaining args (COUNT, MATCH, etc.)
    for a in &args[1..] {
        scan_args.push(a.clone());
    }

    // Execute SCAN on the target shard
    let scan_result = if target_shard_id == my_shard {
        let db_count = shard_databases.db_count();
        let mut selected = db_index;
        let result = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            cmd_dispatch(db, b"SCAN", &scan_args, &mut selected, db_count)
        });
        match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        }
    } else {
        // Remote dispatch
        let (reply_tx, reply_rx) = channel::oneshot();
        let mut parts = vec![Frame::BulkString(Bytes::from_static(b"SCAN"))];
        parts.extend(scan_args);
        let cmd_frame = Frame::Array(parts.into());
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            // Never a script: this fan-out builds its own keyspace command.
            // Fail-closed anyway (moon#569).
            script_acl: crate::acl::ScriptAcl::deny(),
            reply_tx,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target_shard_id, msg, spsc_notifiers).await;
        match recv_reply_bounded(reply_rx).await.map(|r| r.frame) {
            Ok(frame) => frame,
            Err(_) => Frame::Error(Bytes::from_static(
                b"ERR cross-shard reply channel closed during SCAN",
            )),
        }
    };

    // Parse the SCAN response: [cursor, [keys...]]
    match scan_result {
        Frame::Array(parts) if parts.len() == 2 => {
            let next_shard_cursor: i64 = match &parts[0] {
                Frame::BulkString(b) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                Frame::Integer(n) => *n,
                _ => 0,
            };

            let keys = parts[1].clone();

            // Compute next composite cursor
            let next_composite = if next_shard_cursor == 0 {
                // This shard is done, move to the next shard
                let next_shard = target_shard_id + 1;
                if next_shard >= num_shards {
                    // All shards done
                    0u64
                } else {
                    // Start of next shard (cursor 0)
                    (next_shard as u64) << 48
                }
            } else {
                // Continue on current shard
                ((target_shard_id as u64) << 48)
                    | (next_shard_cursor as u64 & 0x0000_FFFF_FFFF_FFFF)
            };

            Frame::Array(framevec![
                Frame::BulkString(Bytes::from(next_composite.to_string())),
                keys,
            ])
        }
        other => other,
    }
}

/// Coordinate DBSIZE across all shards.
///
/// Returns the sum of keys across all shards.
pub async fn coordinate_dbsize(
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let mut total: i64 = 0;
    let mut pending_shards: Vec<channel::OneshotReceiver<crate::shard::dispatch::ExecReply>> =
        Vec::new();

    // Local shard. `logical_len`, NOT `len`: remote legs dispatch a real
    // DBSIZE (which counts hot + cold since issue #355), so an inlined
    // resident-only local count would under-report by exactly the local
    // shard's cold plane — the aggregate must use one definition everywhere.
    {
        let local_len = crate::shard::slice::with_shard_db(db_index, |db| db.logical_len()) as i64;
        total += local_len;
    }

    // Remote shards
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"DBSIZE"))]);
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            // Never a script: this fan-out builds its own keyspace command.
            // Fail-closed anyway (moon#569).
            script_acl: crate::acl::ScriptAcl::deny(),
            reply_tx,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    for reply_rx in pending_shards {
        match recv_reply_bounded(reply_rx).await.map(|r| r.frame) {
            Ok(Frame::Integer(n)) => total += n,
            Ok(_) => {} // Non-integer response — skip
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DBSIZE",
                ));
            }
        }
    }

    Frame::Integer(total)
}

/// Coordinate `DEBUG DIGEST` across all shards.
///
/// Each shard contributes UNFINALISED per-db partials; the coordinator merges
/// them and folds the db indices once. Finalising per shard would mix each db
/// index once per shard instead of once per server and the parts could no
/// longer be combined.
///
/// The local leg runs inline rather than through the SPSC loop: a shard has no
/// self-loop (and none at all at `--shards 1`), so a fan-out that included
/// itself would silently drop its own keys.
///
/// # Failure is loud
///
/// If any shard fails to answer, this returns an ERROR rather than a digest
/// over the shards that did reply. A digest missing one shard's keys is a
/// well-formed 40-character answer that matches nothing — precisely the shape
/// that sends someone hunting data loss that never happened.
pub async fn coordinate_debug_digest(
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    let mut all: Vec<(usize, crate::command::debug_digest::Digest)> =
        crate::command::debug_digest::local_partials();

    let mut pending: Vec<channel::OneshotReceiver<crate::shard::dispatch::ExecReply>> = Vec::new();
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"DEBUG")),
            Frame::BulkString(Bytes::from_static(b"DIGEST-SHARD")),
        ]);
        let msg = ShardMessage::Execute {
            // The partials cover EVERY database, so the db this message is
            // nominally addressed to is irrelevant — but it must be a valid
            // index, and 0 always exists.
            db_index: 0,
            command: std::sync::Arc::new(cmd_frame),
            script_acl: crate::acl::ScriptAcl::deny(),
            reply_tx,
        };
        // `spsc_send` DROPS `reply_tx` when it gives up, so a discarded
        // Backpressure would surface later as "reply channel closed" — after
        // every remaining shard had been dispatched to and waited on. Stop
        // here and say what actually happened.
        if spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await
            == PushOutcome::Backpressure
        {
            return Frame::Error(Bytes::from_static(
                b"ERR cross-shard DEBUG DIGEST dispatch backpressured",
            ));
        }
        pending.push(reply_rx);
    }

    for reply_rx in pending {
        // `recv_reply_bounded_reason`, not `recv_reply_bounded`: the two
        // failures are different facts about the shard, and this error text
        // makes a claim about which one happened. A shard still grinding
        // through a large keyspace is not a shard that dropped its sender.
        match recv_reply_bounded_reason(reply_rx).await.map(|r| r.frame) {
            Ok(frame) => match crate::command::debug_digest::partials_from_frame(&frame) {
                Some(partials) => all.extend(partials),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR a shard returned a malformed DEBUG DIGEST partial",
                    ));
                }
            },
            Err(ReplyFailure::Closed) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DEBUG DIGEST",
                ));
            }
            Err(ReplyFailure::TimedOut) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard DEBUG DIGEST timed out waiting for a shard",
                ));
            }
        }
    }

    let merged = crate::command::debug_digest::merge_partials(all);
    let digest = crate::command::debug_digest::finalize_dataset(merged);
    Frame::SimpleString(Bytes::from(crate::command::debug_digest::to_hex(&digest)))
}

/// Coordinate RANDOMKEY across all shards.
///
/// Asks every shard for BOTH its key count and one random key of its own, then
/// draws a shard with probability proportional to its count.
///
/// The count is what makes the draw key-weighted rather than shard-weighted.
/// Before moon#629 RANDOMKEY was not in this coordinator at all: it answered
/// from whichever shard the connection happened to sit on, so it returned Null
/// while `DBSIZE` reported keys and could never name a key any other shard
/// owned. Picking a shard *uniformly* would only soften that — Null with
/// probability `empty_shards / N`, and an over-sample of whichever shard holds
/// fewest keys. `{hash tag}` co-location makes unequal shards the normal case,
/// not the pathological one.
///
/// Two messages per remote shard, issued back to back and awaited together, so
/// the latency is one round trip. RANDOMKEY is an introspection command; it is
/// not on any hot path.
pub async fn coordinate_randomkey(
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    use rand::RngExt;

    // (key count, that shard's own candidate), local shard first.
    let mut per_shard: Vec<(u64, Option<Bytes>)> = Vec::with_capacity(num_shards);
    per_shard.push(crate::shard::slice::with_shard_db(db_index, |db| {
        // `logical_len` counts hot + cold and `random_key` samples that same
        // union (#364), so the weight and the candidate describe one keyspace.
        // Reading both under one borrow also keeps them consistent with each
        // other: a shard cannot report `n > 0` and no candidate spuriously.
        (db.logical_len() as u64, db.random_key())
    }));

    let mut pending: Vec<(
        channel::OneshotReceiver<crate::shard::dispatch::ExecReply>,
        channel::OneshotReceiver<crate::shard::dispatch::ExecReply>,
    )> = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (size_tx, size_rx) = channel::oneshot();
        let (key_tx, key_rx) = channel::oneshot();
        for (cmd, reply_tx) in [(&b"DBSIZE"[..], size_tx), (&b"RANDOMKEY"[..], key_tx)] {
            let msg = ShardMessage::Execute {
                db_index,
                command: std::sync::Arc::new(Frame::Array(framevec![Frame::BulkString(
                    Bytes::from_static(cmd)
                )])),
                // Never a script: this fan-out builds its own keyspace
                // command. Fail-closed anyway (moon#569).
                script_acl: crate::acl::ScriptAcl::deny(),
                reply_tx,
            };
            let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        }
        pending.push((size_rx, key_rx));
    }

    for (size_rx, key_rx) in pending {
        let count = match recv_reply_bounded(size_rx).await.map(|r| r.frame) {
            Ok(Frame::Integer(n)) if n > 0 => n as u64,
            Ok(_) => 0,
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during RANDOMKEY",
                ));
            }
        };
        let candidate = match recv_reply_bounded(key_rx).await.map(|r| r.frame) {
            Ok(Frame::BulkString(key)) => Some(key),
            Ok(_) => None,
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during RANDOMKEY",
                ));
            }
        };
        per_shard.push((count, candidate));
    }

    let total: u64 = per_shard.iter().map(|(n, _)| *n).sum();
    if total > 0 {
        let mut pick = rand::rng().random_range(0..total);
        for (count, candidate) in &per_shard {
            if pick < *count {
                if let Some(key) = candidate {
                    return Frame::BulkString(key.clone());
                }
                break;
            }
            pick -= *count;
        }
    }

    // Reached when the weighted draw landed on a shard whose own reply came
    // back empty — its last key expired or was deleted between the two
    // replies — or when every count was zero but some shard answered anyway
    // (a key created in that same window). Answering Null here with a live
    // key in hand would recreate the exact defect this coordinator closes, so
    // take any candidate that exists. Null is correct only when NO shard
    // produced one.
    for (_, candidate) in &per_shard {
        if let Some(key) = candidate {
            return Frame::BulkString(key.clone());
        }
    }
    Frame::Null
}

/// Gather per-db `(keys, expires)` across ALL shards for `INFO # Keyspace`.
///
/// Element-wise sum of each shard's per-db counter vector (a key lives on
/// exactly one shard, so sums never double-count). Returns the local-only
/// vector at `num_shards == 1` without touching the SPSC mesh. A dead remote
/// reply channel degrades to the partial sum (INFO is diagnostics — prefer
/// an under-count over an error frame).
pub async fn coordinate_keyspace_info(
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Vec<(u64, u64)> {
    let mut totals: Vec<(u64, u64)> = crate::shard::slice::with_shard(|s| {
        s.databases.with_all_read(|dbs| {
            dbs.iter()
                // logical_len: hot + cold, overlap once — keeps INFO # Keyspace
                // consistent with DBSIZE under disk-offload (issue #355).
                .map(|db| (db.logical_len() as u64, db.expires_count() as u64))
                .collect()
        })
    });
    if num_shards <= 1 {
        return totals;
    }
    let mut receivers = Vec::with_capacity(num_shards - 1);
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::KeyspaceStats { reply_tx };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }
    for rx in receivers {
        if let Ok(stats) = recv_reply_bounded(rx).await {
            for (i, (k, e)) in stats.into_iter().enumerate() {
                if let Some(t) = totals.get_mut(i) {
                    t.0 += k;
                    t.1 += e;
                }
            }
        }
    }
    totals
}

/// Coordinate HOTKEYS across all shards: merge per-shard top-K sketches.
///
/// Each key lives on exactly one shard, so the merge never has to sum
/// duplicate keys — it sorts the union by sampled count and truncates.
pub async fn coordinate_hotkeys(
    count: usize,
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let mut merged: Vec<(Bytes, i64)> = Vec::new();

    // Local shard: read the sketch directly.
    {
        let local = crate::shard::slice::with_shard_db(db_index, |db| db.hot_keys().top(count));
        merged.extend(local.into_iter().map(|(k, c)| (k, c as i64)));
    }

    // Remote shards: synthetic HOTKEYS COUNT <n> executes via normal dispatch.
    let mut count_buf = itoa::Buffer::new();
    let count_bytes = Bytes::copy_from_slice(count_buf.format(count).as_bytes());
    let mut pending_shards: Vec<channel::OneshotReceiver<crate::shard::dispatch::ExecReply>> =
        Vec::new();
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"HOTKEYS")),
            Frame::BulkString(Bytes::from_static(b"COUNT")),
            Frame::BulkString(count_bytes.clone()),
        ]);
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            // Never a script: this fan-out builds its own keyspace command.
            // Fail-closed anyway (moon#569).
            script_acl: crate::acl::ScriptAcl::deny(),
            reply_tx,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    for reply_rx in pending_shards {
        match recv_reply_bounded(reply_rx).await.map(|r| r.frame) {
            Ok(Frame::Array(entries)) => {
                for entry in entries.iter() {
                    if let Frame::Array(pair) = entry
                        && let (Some(Frame::BulkString(k)), Some(Frame::Integer(c))) =
                            (pair.first(), pair.get(1))
                    {
                        merged.push((k.clone(), *c));
                    }
                }
            }
            Ok(_) => {} // Error/unexpected reply from one shard — skip its entries
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during HOTKEYS",
                ));
            }
        }
    }

    merged.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    merged.truncate(count);
    let mut out: Vec<Frame> = Vec::with_capacity(merged.len());
    for (key, sampled) in merged {
        out.push(Frame::Array(framevec![
            Frame::BulkString(key),
            Frame::Integer(sampled),
        ]));
    }
    Frame::Array(out.into())
}

/// Scatter a vector search query to all shards, collect per-shard results,
/// and merge into a global top-K response.
///
/// Used when the connection handler receives FT.SEARCH and num_shards > 1.
/// Each shard runs a local search and returns its local top-K. The coordinator
/// merges all per-shard results and returns the globally correct top-K.
///
/// For single-shard deployments, FT.SEARCH executes directly without scatter.
pub async fn scatter_vector_search(
    index_name: Bytes,
    query_blob: Bytes,
    k: usize,
    as_of_lsn: u64,
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    vector_store: &mut crate::vector::store::VectorStore,
    db_index: u8,
) -> Frame {
    let mut receivers = Vec::with_capacity(num_shards);
    let mut local_result: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Execute locally -- avoid SPSC overhead for local shard.
            // Phase 171 SCAT-01: thread as_of_lsn through the local branch so
            // the coordinator honors temporal filtering on its own shard too.
            // WS5a: db_index scopes index visibility on this shard too.
            local_result = Some(crate::command::vector_search::search_local_filtered(
                vector_store,
                &index_name,
                &query_blob,
                k,
                None,
                0,
                usize::MAX,
                None,
                as_of_lsn,
                db_index,
            ));
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg =
                ShardMessage::VectorSearch(Box::new(crate::shard::dispatch::VectorSearchPayload {
                    index_name: index_name.clone(),
                    query_blob: query_blob.clone(),
                    k,
                    as_of_lsn,
                    reply_tx,
                    db_index,
                }));
            let _ = spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    let mut shard_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_result {
        shard_responses.push(local);
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => shard_responses.push(frame),
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR shard reply channel closed during vector search scatter-gather",
                ));
            }
        }
    }

    crate::command::vector_search::merge_search_results(&shard_responses, k, 0, usize::MAX)
}

/// Scatter FT.SEARCH to all shards via SPSC (no local vector_store needed).
///
/// Used by connection handlers that don't have direct vector_store access.
/// Sends VectorSearch to every shard (including local) via SPSC, collects
/// results, and merges into a global top-K response.
/// Scatter FT.SEARCH to all shards (local + remote), merge top-K results.
///
/// Local shard: direct VectorStore access via shard_databases (no SPSC self-send).
/// Remote shards: SPSC dispatch with VectorSearch message.
/// Single-shard (num_shards == 1): local-only, no SPSC needed.
pub async fn scatter_vector_search_remote(
    index_name: Bytes,
    query_blob: Bytes,
    k: usize,
    as_of_lsn: u64,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Frame {
    let _ = shard_databases; // E2 removes
    // LOCAL: direct vector store access (avoids SPSC self-send).
    // Phase 171 SCAT-01: honor AS_OF on the coordinator's own shard by
    // routing through `search_local_filtered` with the resolved LSN rather
    // than the AS_OF-unaware `search_local` helper. WS5a: db_index scopes
    // index visibility here too.
    let local_result = crate::shard::slice::with_shard(|s| {
        crate::command::vector_search::search_local_filtered(
            &mut s.vector_store,
            &index_name,
            &query_blob,
            k,
            None,
            0,
            usize::MAX,
            None,
            as_of_lsn,
            db_index,
        )
    });

    // REMOTE: SPSC to all other shards
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg =
            ShardMessage::VectorSearch(Box::new(crate::shard::dispatch::VectorSearchPayload {
                index_name: index_name.clone(),
                query_blob: query_blob.clone(),
                k,
                as_of_lsn,
                reply_tx,
                db_index,
            }));
        let _ = spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    let mut shard_responses = Vec::with_capacity(num_shards);
    shard_responses.push(local_result);
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => shard_responses.push(frame),
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR shard reply channel closed during vector search scatter-gather",
                ));
            }
        }
    }

    crate::command::vector_search::merge_search_results(&shard_responses, k, 0, usize::MAX)
}

/// Broadcast an FT.* command (FT.CREATE, FT.DROPINDEX) to ALL shards.
///
/// Each shard creates its own copy of the index so HSET auto-indexing works
/// regardless of which shard the key routes to.
///
/// Local shard: direct VectorStore access via shard_databases.
/// Remote shards: SPSC dispatch with VectorCommand message.
/// Single-shard (num_shards == 1): local-only, no SPSC needed.
/// `db_index` (WS5a): the originating connection's currently-SELECTed
/// logical db — forwarded to every remote shard via `ShardMessage::VectorCommand`
/// so FT.CREATE tags the new index to the right db everywhere, and
/// FT.DROPINDEX / FT.COMPACT / FT.CONFIG resolve/mutate only the caller's db.
pub async fn broadcast_vector_command(
    command: std::sync::Arc<Frame>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Frame {
    let _ = shard_databases; // E2 removes
    // REMOTE FIRST: send to all other shards via SPSC before local mutation.
    // This ensures we detect remote failures before committing locally,
    // avoiding partial index metadata across the cluster.
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
            db_index,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    // Collect remote results — fail if any shard errors or disconnects
    for rx in receivers {
        match rx.recv().await {
            Ok(Frame::Error(e)) => return Frame::Error(e),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR vector command failed: cross-shard reply channel closed",
                ));
            }
            _ => {}
        }
    }

    // LOCAL: execute only after all remote shards succeeded
    // FT.DROPINDEX with DD flag needs Database to delete indexed docs.
    let is_dropindex = match command.as_ref() {
        Frame::Array(arr) if !arr.is_empty() => {
            matches!(&arr[0], Frame::BulkString(b) if b.eq_ignore_ascii_case(b"FT.DROPINDEX"))
        }
        _ => false,
    };

    // Split borrows so rustc sees `&mut s.vector_store`, `&mut s.text_store`
    // and `&s.graph_store` as disjoint fields. The database no longer needs to
    // be one of them: its guard borrows from the L4 set behind the `Arc`, not
    // from `s`, so cloning the handle first frees `s` for the store borrows.
    let local_result = crate::shard::slice::with_shard(|s| {
        let db_set = std::sync::Arc::clone(&s.databases);
        let (vs, ts);
        #[cfg(feature = "graph")]
        let graph_ref;
        {
            vs = &mut s.vector_store;
            ts = &mut s.text_store;
            #[cfg(feature = "graph")]
            {
                graph_ref = &s.graph_store;
            }
        }
        // WS5a / Gap 8: use the caller's real db, not a hardcoded db 0 —
        // FT.DROPINDEX DD deletes indexed docs from the SAME db the index
        // is scoped to.
        let mut db_guard = if is_dropindex {
            db_set.try_write(db_index as usize)
        } else {
            None
        };
        let db_opt = db_guard.as_deref_mut();
        crate::shard::spsc_handler::dispatch_vector_command(
            vs,
            ts,
            #[cfg(feature = "graph")]
            Some(graph_ref),
            &command,
            db_opt,
            db_index,
        )
    });
    local_result
}

/// Scatter `FT.INVALIDATE_RANGE` to all shards and return the summed deleted-document count.
///
/// Unlike `broadcast_vector_command` (which returns the first non-error response),
/// this helper sends the command to every shard, collects each shard's `Frame::Integer`
/// count, and returns `Frame::Integer(sum)`.  If any shard returns an error the error
/// is propagated immediately (same early-exit semantics as `broadcast_vector_command`).
///
/// # Lock safety
/// All per-shard local execution is synchronous (no `.await` inside the local block),
/// so no `MutexGuard` is held across an `.await` point.
#[cfg(feature = "text-index")]
pub async fn scatter_invalidate_range(
    command: std::sync::Arc<Frame>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Frame {
    let _ = shard_databases; // E2 removes
    // PARTIAL-STATE: sends fire to all remotes in parallel, then we collect
    // sequentially. If any remote returns an error after others already
    // applied their deletes, this call returns Frame::Error but the
    // successful remotes have already advanced their text_version_token
    // and removed matching docs — there is no cross-shard rollback.
    // Lunaris must treat error replies as "retry the whole invalidation"
    // rather than "no work was done."
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
            db_index,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    // Collect remote counts — fail on any error (see PARTIAL-STATE above).
    let mut total: i64 = 0;
    for rx in receivers {
        match rx.recv().await {
            Ok(Frame::Integer(n)) => total = total.saturating_add(n),
            Ok(Frame::Error(e)) => return Frame::Error(e),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.INVALIDATE_RANGE: cross-shard reply channel closed",
                ));
            }
            Ok(other) => {
                // Unexpected response type — surface for debugging.
                let _ = other;
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.INVALIDATE_RANGE: unexpected response from remote shard",
                ));
            }
        }
    }

    // Execute locally and add to total.
    let local = crate::shard::slice::with_shard(|s| {
        crate::shard::spsc_handler::dispatch_vector_command(
            &mut s.vector_store,
            &mut s.text_store,
            #[cfg(feature = "graph")]
            Some(&s.graph_store),
            &command,
            None,
            db_index,
        )
    });

    match local {
        Frame::Integer(n) => Frame::Integer(total.saturating_add(n)),
        Frame::Error(e) => Frame::Error(e),
        other => {
            let _ = other;
            Frame::Error(Bytes::from_static(
                b"ERR FT.INVALIDATE_RANGE: unexpected local response",
            ))
        }
    }
}

/// Scatter `FT.INFO` to all shards and merge the per-shard stats (XC-SHARD-1).
///
/// Vector data is key-hash partitioned: each shard's index holds only the
/// vectors whose keys route there, so a single shard's FT.INFO reports ~1/N of
/// the true document count. This helper collects every shard's response and
/// sums the additive fields via
/// [`crate::command::vector_search::merge_ft_info_responses`]; config fields
/// come from the local response (identical everywhere by FT.CREATE broadcast).
///
/// # Lock safety
/// Local execution is synchronous inside `with_shard` (no `.await` while the
/// shard slice is borrowed).
pub async fn scatter_ft_info(
    command: std::sync::Arc<Frame>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Frame {
    let _ = shard_databases;
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
            db_index,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    let mut remote_responses: Vec<Frame> = Vec::with_capacity(receivers.len());
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => remote_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.INFO: cross-shard reply channel closed",
                ));
            }
        }
    }

    let local = crate::shard::slice::with_shard(|s| {
        crate::shard::spsc_handler::dispatch_vector_command(
            &mut s.vector_store,
            &mut s.text_store,
            #[cfg(feature = "graph")]
            Some(&s.graph_store),
            &command,
            None,
            db_index,
        )
    });

    crate::command::vector_search::merge_ft_info_responses(local, &remote_responses)
}

/// Answer `FT.SEARCH <idx> "*"` when only the vector engine can (moon#695), or
/// `None` to leave the query on its normal path.
///
/// The single entry point the connection handlers call. It has to be consulted
/// BEFORE the text engine: `is_text_query("*")` is true, so a bare `*` reaches the
/// text path at every routing site, and for a VECTOR-only index that path answers
/// `ERR no such index` for an index `FT._LIST` lists. Every other query — text,
/// KNN, SPARSE, HYBRID, and `*` on any index carrying a TEXT/TAG/NUMERIC field —
/// returns `None` here and is completely unaffected.
pub async fn ft_match_all_if_vector_only(
    cmd_args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Option<Frame> {
    let index_name = crate::shard::slice::with_shard(|s| {
        crate::command::vector_search::vector_only_match_all_index(
            &s.text_store,
            &s.vector_store,
            cmd_args,
            db_index,
        )
    })?;
    let (offset, count) = crate::command::vector_search::parse_limit_clause(cmd_args);

    // One shard owns every key, so there is nothing to gather.
    if num_shards == 1 {
        return Some(crate::shard::slice::with_shard(|s| {
            crate::command::vector_search::match_all_local(
                &s.vector_store,
                index_name.as_ref(),
                db_index,
                offset,
                count,
            )
        }));
    }

    Some(
        scatter_ft_match_all(
            index_name,
            offset,
            count,
            my_shard,
            num_shards,
            dispatch_tx,
            spsc_notifiers,
            db_index,
        )
        .await,
    )
}

/// Scatter a vector-only `FT.SEARCH <idx> "*"` (moon#695) and merge the answers.
///
/// Keys partition to exactly one shard, so a match-all is the union of every
/// shard's live key map. `merge_text_results` already sums per-shard `reply[0]`
/// into the global total (C4), and because every match-all score is equal and its
/// sort is stable it preserves the order the responses are collected in — so they
/// are gathered in SHARD order rather than completion order, to keep the answer
/// deterministic run to run.
///
/// The command forwarded to each shard is REWRITTEN to `LIMIT 0 (offset+count)`.
/// Shipping the caller's own LIMIT would make every shard skip its own first
/// `offset` documents and then the coordinator skip `offset` again. Capping each
/// shard at `offset+count` is sufficient and not a silent truncation: the merged
/// list is a concatenation in shard order, so no document past a shard's first
/// `offset+count` can reach the requested page. With no LIMIT at all the clause is
/// omitted entirely, so `*` returns every document at any shard count — the same
/// answer the single-shard path gives.
pub async fn scatter_ft_match_all(
    index_name: Bytes,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Frame {
    let mut parts = vec![
        Frame::BulkString(Bytes::from_static(b"FT.SEARCH")),
        Frame::BulkString(index_name),
        Frame::BulkString(Bytes::from_static(b"*")),
    ];
    if count != usize::MAX {
        parts.push(Frame::BulkString(Bytes::from_static(b"LIMIT")));
        parts.push(Frame::BulkString(Bytes::from_static(b"0")));
        parts.push(Frame::BulkString(Bytes::from(
            offset.saturating_add(count).to_string(),
        )));
    }
    let command = std::sync::Arc::new(Frame::Array(parts.into()));

    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
            db_index,
        };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push((target, reply_rx));
    }

    let local = crate::shard::slice::with_shard(|s| {
        crate::shard::spsc_handler::dispatch_vector_command(
            &mut s.vector_store,
            &mut s.text_store,
            #[cfg(feature = "graph")]
            Some(&s.graph_store),
            &command,
            None,
            db_index,
        )
    });

    let mut by_shard: Vec<Option<Frame>> = (0..num_shards).map(|_| None).collect();
    if let Some(slot) = by_shard.get_mut(my_shard) {
        *slot = Some(local);
    }
    for (target, rx) in receivers {
        match rx.recv().await {
            Ok(frame) => {
                if let Some(slot) = by_shard.get_mut(target) {
                    *slot = Some(frame);
                }
            }
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.SEARCH: cross-shard reply channel closed during match-all",
                ));
            }
        }
    }
    let responses: Vec<Frame> = by_shard.into_iter().flatten().collect();

    let top_k = if count == usize::MAX {
        usize::MAX
    } else {
        offset.saturating_add(count)
    };
    crate::command::vector_search::merge_text_results(&responses, top_k, offset, count)
}

/// Two-phase DFS scatter-gather for globally accurate BM25 text search (per D-04).
///
/// **Phase 1** — DocFreq scatter: collect (term, df) + total N from every shard,
/// aggregate into global document frequency statistics.
///
/// **Phase 2** — TextSearch scatter: execute BM25 search on every shard using the
/// injected global IDF weights, then merge per-shard top-K results.
///
/// **Single-shard fast path** (per D-06): when `num_shards == 1`, the local shard's
/// FieldStats are globally accurate, so the DFS pre-pass is skipped entirely.
///
/// # Lock safety
/// Every access to `shard_databases.text_store(shard_id)` returns a `MutexGuard`.
/// All local data extraction is wrapped in a block scope so the guard is dropped
/// **before** any `.await` point — required by RESEARCH Pitfall 2.
pub async fn scatter_text_search(
    index_name: Bytes,
    query: Bytes,
    top_k: usize,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    highlight_opts: Option<crate::command::vector_search::HighlightOpts>,
    summarize_opts: Option<crate::command::vector_search::SummarizeOpts>,
    db_index: u8,
) -> Frame {
    let _ = shard_databases; // E2 removes

    // ── Parse once for Phase-1 df terms + highlight terms (fts-query-eval-dispatch 2b) ──
    // Parse inside a with_shard block so we have the index schema, then move owned
    // values out. with_shard releases the borrow before any .await.
    #[cfg(feature = "text-index")]
    let (field_queries, term_strings) = {
        use crate::text::query::{QuerySchema, collect_df_field_terms, collect_highlight_terms};
        let parse_result: Result<
            (Vec<(Option<usize>, Vec<String>)>, Vec<String>),
            crate::protocol::Frame,
        > = crate::shard::slice::with_shard(|s| {
            match s.text_store.get_index_for_db(&index_name, db_index) {
                None => Err(Frame::Error(Bytes::from_static(b"ERR no such index"))),
                Some(text_index) => {
                    let schema = QuerySchema::from_index(text_index);
                    match crate::text::query::parse_query(&query, &schema) {
                        Err(e) => Err(Frame::Error(e.wire_error())),
                        Ok(node) => {
                            let fq = collect_df_field_terms(&node, text_index);
                            let ts = collect_highlight_terms(&node, text_index);
                            Ok((fq, ts))
                        }
                    }
                }
            }
        });
        match parse_result {
            Err(err_frame) => return err_frame,
            Ok(pair) => pair,
        }
    };
    #[cfg(not(feature = "text-index"))]
    let (field_queries, _term_strings): (Vec<(Option<usize>, Vec<String>)>, Vec<String>) =
        (Vec::new(), Vec::new());

    // ── Single-shard fast path (per D-06) ────────────────────────────────────
    if num_shards == 1 {
        // Local IDF is globally accurate with one shard — skip DFS pre-pass.
        // Use run_text_query_on_index with no global IDF (single-shard path).
        //
        // text_store + databases[0] accessed simultaneously via a single
        // `with_shard` call to avoid a reentrant `with_shard*` panic.
        let result = crate::shard::slice::with_shard(|s| {
            let ts = &s.text_store;
            let text_index = match ts.get_index_for_db(&index_name, db_index) {
                Some(idx) => idx,
                None => return Frame::Error(Bytes::from_static(b"ERR no such index")),
            };
            #[cfg(feature = "text-index")]
            {
                let mut r = crate::command::vector_search::ft_text_search::run_text_query_on_index(
                    text_index, &query, None, None, top_k, offset, count,
                );
                if highlight_opts.is_some() || summarize_opts.is_some() {
                    // databases[db_index] borrowed disjointly from text_store — both live on `s`.
                    if let Some(db) = s.databases.try_write(db_index as usize) {
                        crate::command::vector_search::ft_text_search::apply_post_processing(
                            &mut r,
                            &term_strings,
                            text_index,
                            &db,
                            highlight_opts.as_ref(),
                            summarize_opts.as_ref(),
                        );
                    }
                }
                r
            }
            #[cfg(not(feature = "text-index"))]
            {
                let _ = text_index;
                Frame::Error(Bytes::from_static(b"ERR text-index feature not enabled"))
            }
        });
        return result;
    }

    // ── Phase 1: scatter DocFreq to all shards ────────────────────────────────
    // Collect (term, df, N) from each shard to build global IDF weights.
    // field_queries comes from collect_df_field_terms (above); shape unchanged.
    let mut doc_freq_receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_doc_freq: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: extract df/N directly — no SPSC overhead.
            // Shard slice released before any .await.
            let response = crate::shard::slice::with_shard(|s| {
                match s.text_store.get_index_for_db(&index_name, db_index) {
                    Some(text_index) => {
                        let mut items: Vec<Frame> = Vec::new();
                        for (field_idx_opt, terms) in &field_queries {
                            let fidx = field_idx_opt.unwrap_or(0);
                            let (term_dfs, n) = text_index.doc_freq_for_terms(fidx, terms);
                            for (term, df) in term_dfs {
                                items.push(Frame::BulkString(Bytes::from(term)));
                                items.push(Frame::Integer(i64::from(df)));
                            }
                            items.push(Frame::BulkString(Bytes::from_static(b"N")));
                            items.push(Frame::Integer(i64::from(n)));
                        }
                        Frame::Array(items.into())
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                }
            });
            local_doc_freq = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::DocFreq(Box::new(crate::shard::dispatch::DocFreqPayload {
                index_name: index_name.clone(),
                field_queries: field_queries.clone(),
                reply_tx,
                db_index,
            }));
            let _ = spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            doc_freq_receivers.push(reply_rx);
        }
    }

    // Collect Phase 1 responses and aggregate.
    let mut doc_freq_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_doc_freq {
        doc_freq_responses.push(local);
    }
    for rx in doc_freq_receivers {
        match rx.recv().await {
            Ok(frame) => doc_freq_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR DFS phase 1 channel closed unexpectedly",
                ));
            }
        }
    }

    let (global_df, global_n) = aggregate_doc_freq(&doc_freq_responses);

    // ── Phase 2: scatter TextSearch with global IDF to all shards ─────────────
    let mut search_receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_search: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: execute with global IDF via run_text_query_on_index.
            // text_store + databases[0] folded into a single `with_shard` to
            // avoid reentrant `with_shard*` panic. Slice released before .await.
            let response = crate::shard::slice::with_shard(|s| {
                match s.text_store.get_index_for_db(&index_name, db_index) {
                    Some(text_index) => {
                        #[cfg(feature = "text-index")]
                        {
                            let mut r = crate::command::vector_search::ft_text_search::run_text_query_on_index(
                                text_index,
                                &query,
                                Some(&global_df),
                                Some(global_n),
                                top_k,
                                0,      // each shard returns top_k; coordinator applies final offset
                                top_k,
                            );
                            if highlight_opts.is_some() || summarize_opts.is_some() {
                                if let Some(db) = s.databases.try_write(db_index as usize) {
                                    crate::command::vector_search::ft_text_search::apply_post_processing(
                                        &mut r,
                                        &term_strings,
                                        text_index,
                                        &db,
                                        highlight_opts.as_ref(),
                                        summarize_opts.as_ref(),
                                    );
                                }
                            }
                            r
                        }
                        #[cfg(not(feature = "text-index"))]
                        {
                            let _ = text_index;
                            Frame::Error(Bytes::from_static(b"ERR text-index feature not enabled"))
                        }
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                }
            });
            local_search = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg =
                ShardMessage::TextSearch(Box::new(crate::shard::dispatch::TextSearchPayload {
                    index_name: index_name.clone(),
                    // Send raw query bytes; each remote shard re-parses with the full AST.
                    query: query.clone(),
                    global_df: global_df.clone(),
                    global_n,
                    top_k,
                    offset: 0, // each shard returns top_k; coordinator applies final offset+count
                    count: top_k,
                    // Pass opts to each remote shard — each applies post-processing locally.
                    highlight_opts: highlight_opts.clone(),
                    summarize_opts: summarize_opts.clone(),
                    reply_tx,
                    db_index,
                }));
            let _ = spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            search_receivers.push(reply_rx);
        }
    }

    // Collect Phase 2 responses.
    let mut search_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_search {
        search_responses.push(local);
    }
    for rx in search_receivers {
        match rx.recv().await {
            Ok(frame) => search_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR DFS phase 2 channel closed unexpectedly",
                ));
            }
        }
    }

    // Merge and apply final pagination.
    crate::command::vector_search::merge_text_results(&search_responses, top_k, offset, count)
}

/// Scatter a FieldFilter (TAG — Plan 07 adds NumericRange) across all shards.
///
/// Plan 152-06 (B-02): mirrors `scatter_text_search` for FieldFilter clauses
/// but:
/// - Skips the DFS pre-pass (FieldFilter has no per-term IDF).
/// - Dispatches `ShardMessage::InvertedSearch` instead of `TextSearch`.
/// - Merges response frames via `merge_text_results` — results arrive with
///   `score=0.0`, `merge_text_results` preserves insertion order within its
///   bucket so the per-shard doc_id ascending order becomes a consistent
///   cross-shard ordering after the sort-by-score tie-break (score is
///   uniform, so the secondary key — key-bytes — resolves deterministically).
///
/// # Lock safety
/// Single-shard fast path scopes the `MutexGuard` inside a block so it
/// drops before any `.await` (RESEARCH Pitfall 2).
#[cfg(feature = "text-index")]
#[allow(clippy::too_many_arguments)]
pub async fn scatter_text_search_filter(
    index_name: Bytes,
    filter: crate::command::vector_search::ft_text_search::FieldFilter,
    top_k: usize,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    db_index: u8,
) -> Frame {
    let _ = shard_databases; // E2 removes
    // ── Single-shard fast path ────────────────────────────────────────────────
    if num_shards == 1 {
        let response = crate::shard::slice::with_shard(|s| {
            match s.text_store.get_index_for_db(&index_name, db_index) {
                None => Frame::Error(Bytes::from_static(b"ERR no such index")),
                Some(text_index) => {
                    let clause = crate::command::vector_search::ft_text_search::TextQueryClause {
                        field_name: None,
                        terms: Vec::new(),
                        filter: Some(filter),
                    };
                    let results =
                        crate::command::vector_search::ft_text_search::execute_query_on_index(
                            text_index, &clause, None, None, top_k,
                        );
                    crate::command::vector_search::ft_text_search::build_text_response(
                        &results, offset, count,
                    )
                }
            }
        });
        return response;
    }

    // ── Multi-shard fan-out ──────────────────────────────────────────────────
    let mut receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_response: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            let response = crate::shard::slice::with_shard(|s| {
                match s.text_store.get_index_for_db(&index_name, db_index) {
                    None => Frame::Error(Bytes::from_static(b"ERR no such index")),
                    Some(text_index) => {
                        let clause =
                            crate::command::vector_search::ft_text_search::TextQueryClause {
                                field_name: None,
                                terms: Vec::new(),
                                filter: Some(filter.clone()),
                            };
                        let results =
                            crate::command::vector_search::ft_text_search::execute_query_on_index(
                                text_index, &clause, None, None, top_k,
                            );
                        // Each shard returns top_k; coordinator applies final offset+count
                        // after merging.
                        crate::command::vector_search::ft_text_search::build_text_response(
                            &results, 0, top_k,
                        )
                    }
                }
            });
            local_response = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::InvertedSearch(Box::new(
                crate::shard::dispatch::InvertedSearchPayload {
                    index_name: index_name.clone(),
                    filter: filter.clone(),
                    top_k,
                    offset: 0,
                    count: top_k,
                    reply_tx,
                    db_index,
                },
            ));
            let _ = spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    let mut responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_response {
        responses.push(local);
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR InvertedSearch channel closed unexpectedly",
                ));
            }
        }
    }

    // Uniform score=0.0 across shards; merge_text_results stabilizes order.
    crate::command::vector_search::merge_text_results(&responses, top_k, offset, count)
}

/// Aggregate document frequencies from multiple shard `DocFreq` responses.
///
/// Each response is a `Frame::Array` with interleaved `[term, df, ..., "N", n]` entries.
/// This function sums `df` per term across shards and sums `N` (total docs) across shards.
///
/// Returns `(global_df: HashMap<String, u32>, global_n: u32)`.
pub(crate) fn aggregate_doc_freq(
    responses: &[Frame],
) -> (std::collections::HashMap<String, u32>, u32) {
    let mut global_df: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut global_n: u32 = 0;

    for resp in responses {
        let items = match resp {
            Frame::Array(items) => items,
            _ => continue, // Skip error frames from shards that don't have the index
        };

        let mut i = 0;
        while i + 1 < items.len() {
            match &items[i] {
                Frame::BulkString(key) => {
                    if key.as_ref() == b"N" {
                        // "N" sentinel: next item is the total doc count for this shard
                        if let Frame::Integer(n) = &items[i + 1] {
                            global_n = global_n.saturating_add(*n as u32);
                        }
                        i += 2;
                    } else {
                        // term -> df pair
                        let term = match std::str::from_utf8(key) {
                            Ok(s) => s.to_owned(),
                            Err(_) => {
                                i += 2;
                                continue;
                            }
                        };
                        if let Frame::Integer(df) = &items[i + 1] {
                            *global_df.entry(term).or_insert(0) = global_df
                                .get(&term)
                                .copied()
                                .unwrap_or(0)
                                .saturating_add(*df as u32);
                        }
                        i += 2;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }
    }

    (global_df, global_n)
}

/// Broadcast SWAPDB to all shards and await acknowledgement AND durability
/// from each (issue #133).
///
/// # Flow
///
/// - Local shard FIRST: durable AOF append (v3-5 group-commit + barrier)
///   BEFORE the swap AND before any remote dispatch — mirrors the
///   single-shard `handler_single.rs` SWAPDB contract so the coordinator
///   shard's own record survives a kill-9, and (adversarial-review fix)
///   guarantees that every local abort point (WAL backpressure, AOF
///   enqueue failure, fsync failure) fires while the CLUSTER is still
///   untouched — no remote shard has been told to swap yet, so an aborted
///   SWAPDB leaves all shards consistent.
///
/// NOTE (replication): SWAPDB is deliberately NOT recorded on the live
/// replication plane by this local leg. Replicas cannot currently execute
/// a streamed SWAPDB at all — `replication::apply::apply_local` routes it
/// into `cmd_dispatch`, which hard-errors ("SWAPDB must be issued at the
/// connection handler level") and the record silently no-ops. Fanning the
/// local record out would ship bytes the replica cannot apply, and doing
/// the backlog/offset bookkeeping BEFORE the durability gate can diverge
/// master/replica on an abort. Wiring replica-side SWAPDB application
/// (both this local leg and the remote legs' pre-existing
/// `wal_append_and_fanout` emission, which has the same no-op fate) is
/// tracked as a separate issue.
/// - Remote shards: send `ShardMessage::SwapDb` (its SPSC arm durably logs
///   via `wal_append_and_fanout` — WAL v3 + repl backlog/offset/fan-out +
///   AOF — BEFORE swapping and acking), then this function issues ONE
///   `fsync_barrier(target)` per remote shard AFTER observing its ack,
///   confirming the fsync the SPSC arm could not await inline (SPSC arms
///   run synchronously inside the shard event loop — they cannot `.await`).
///
/// All-shard acks AND (under `appendfsync=always`) all-shard fsync barriers
/// are awaited before returning `+OK`. Between the first and last ack a
/// brief window exists where a cross-shard GET may observe the pre-swap
/// state on one shard and post-swap on another. This matches Redis cluster
/// relaxed semantics and is documented as the "brief-skew" acceptance.
///
/// # Durability (defect 1: no fsync rendezvous)
///
/// `appendfsync=always` requires every shard's SWAPDB record to be fsynced
/// to disk BEFORE the client observes `+OK`. This function:
///   1. Appends the LOCAL shard's record via
///      `AofWriterPool::send_append_group` (the same v3-5 group-commit
///      primitive [`persist_local_leg`] uses for MSET/BITOP/COPY/DEL local
///      legs) and, when `Always`, awaits ONE `fsync_barrier(my_shard)`
///      BEFORE performing the swap — a failure aborts with NO mutation
///      applied anywhere on this shard yet.
///   2. Dispatches `ShardMessage::SwapDb` to every remote shard.
///   3. After EACH remote ack, issues ONE `fsync_barrier(target)`. Because
///      the remote's `Append` was enqueued into that shard's AOF writer
///      channel strictly before its `reply_tx.send(())` — observed here via
///      `rx.recv().await`, a happens-before edge — and the writer processes
///      that channel in FIFO order regardless of which thread produced each
///      message, the barrier's `AppendSync` is guaranteed to be queued
///      AFTER the remote's `Append`: an acked barrier proves it durable.
///      This is the identical ordering proof the existing H1-BARRIER
///      cross-shard write path already relies on (see
///      `handler_monoio/mod.rs`'s "call fsync_barrier once per target shard
///      AFTER responses are collected").
///
/// A remote barrier failure occurs AFTER that shard's swap was already
/// applied — there is no rollback path (SWAPDB has none). The response
/// truthfully reports durability-unconfirmed (mirrors the F2 bounded-wait
/// discipline documented on `try_send_append_durable`) rather than a false
/// `+OK`; the swap itself is NOT undone. `fsync_barrier` internally no-ops
/// under `EverySec`/`No`, so every barrier call below is unconditional and
/// only actually awaits a disk fsync when the policy is `Always`.
///
/// # Local-leg AOF/recovery gap (defect 2)
///
/// Before this fix the local leg only wrote to the generic
/// `wal_append_txs` channel (`ShardDatabases::try_wal_append_required`),
/// which the event loop drains into WAL v3 ONLY — never the per-shard AOF
/// writer (`AofWriterPool`). For `--shards >= 2 --appendonly yes`,
/// `main.rs`'s `replay_per_shard` — NOT WAL v3 — is the recovery authority:
/// it unconditionally wipes every shard's databases and replays ONLY from
/// the per-shard AOF manifest (`appendonlydir/shard-N/`). The coordinator
/// shard's own SWAPDB record therefore never reached the plane recovery
/// actually reads: a kill-9 right after `+OK` permanently lost the LOCAL
/// shard's half of the swap on restart while remote shards' halves
/// survived — cross-shard keyspace divergence, worse than a mere fsync
/// gap. The WAL v3 write below is KEPT (harmless — other record types on
/// this channel need it, and it is a documented non-authority for this
/// deployment shape) but is no longer the only durability plane the local
/// leg touches.
#[allow(clippy::too_many_arguments)]
pub async fn coordinate_swapdb(
    a: usize,
    b: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: ReplStateRef<'_>,
) -> Frame {
    // Serves num_shards >= 1: the tokio single-shard SWAPDB lives in
    // handler_single.rs, but the monoio handler routes ALL shard counts here
    // (at shards=1 the remote loop is simply empty).
    debug_assert!(num_shards >= 1);
    // Local shard first: durable append BEFORE the swap AND before any
    // remote dispatch. SWAPDB has no command-level rollback; anything that
    // can fail must fail while NOTHING in the cluster has mutated —
    // dispatching remotes first (the pre-fix order) meant a local abort
    // left N-1 shards swapped and this one not.
    {
        let mut a_buf = itoa::Buffer::new();
        let mut b_buf = itoa::Buffer::new();
        let wal_frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"SWAPDB")),
            Frame::BulkString(Bytes::copy_from_slice(a_buf.format(a).as_bytes())),
            Frame::BulkString(Bytes::copy_from_slice(b_buf.format(b).as_bytes())),
        ]);
        let serialized = crate::persistence::aof::serialize_command(&wal_frame);

        // WAL v3 — see "Local-leg AOF/recovery gap" doc above: kept for
        // parity with other record types on this channel, but no longer the
        // sole durability plane.
        if !shard_databases.try_wal_append_required(
            my_shard,
            crate::persistence::wal_v3::record::WalRecordType::Command,
            serialized.clone(),
        ) {
            return Frame::Error(bytes::Bytes::from_static(
                b"ERR SWAPDB aborted: WAL enqueue failed (persistence backpressure)",
            ));
        }

        // AOF pool — the actual multi-shard recovery authority (defect 2).
        // Same v3-5 group-commit contract `persist_local_leg` uses for
        // MSET/BITOP/COPY/DEL: enqueue fire-and-forget, then ONE barrier
        // under `Always` instead of a per-write awaited fsync.
        if let Some(pool) = aof_pool {
            let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(
                repl_state,
                my_shard,
                serialized.len(),
            );
            match pool
                .send_append_group(my_shard, lsn, 0, serialized.clone())
                .await
            {
                Ok(needs_barrier) => {
                    if needs_barrier && pool.fsync_barrier(my_shard).await.is_err() {
                        return Frame::Error(bytes::Bytes::from_static(
                            crate::persistence::aof::AOF_FSYNC_ERR,
                        ));
                    }
                }
                Err(_) => {
                    return Frame::Error(bytes::Bytes::from_static(
                        crate::persistence::aof::AOF_FSYNC_ERR,
                    ));
                }
            }
        }

        // Local durability confirmed (or persistence disabled) — apply the
        // swap.
        crate::shard::slice::with_shard(|s| {
            if a != b {
                s.databases.swap(a, b);
            }
        });

        // #386 — replication plane, exactly once per client SWAPDB. Today's
        // replica applies the merged wire as ONE stream, so the record must
        // appear on it exactly once: the coordinator emits it here, AFTER
        // the durability gate (an abort above never reaches this line, so a
        // failed SWAPDB can never ship to replicas) and after the local
        // swap; the remote legs' SPSC arms write AOF/WAL only. Safe on both
        // runtimes: this runs on the shard's own OS thread (monoio shard
        // thread / tokio per-shard LocalSet), whose event loop drains
        // `self_msg`. When #406 lands per-shard demuxed replicas this must
        // flip to per-shard emission.
        crate::replication::state::record_local_write_global(my_shard, serialized);
    }

    // ChannelMesh has no self-send slot (target_index panics when my_id == target_id).
    // Skip self in the SPSC loop; handle the local shard inline below.
    let remote_count = num_shards.saturating_sub(1);
    let mut targets: Vec<usize> = Vec::with_capacity(remote_count);
    let mut receivers: Vec<channel::OneshotReceiver<()>> = Vec::with_capacity(remote_count);

    for target in 0..num_shards {
        if target == my_shard {
            continue; // handled inline below
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::SwapDb { a, b, reply_tx };
        let _ = spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        targets.push(target);
        receivers.push(reply_rx);
    }

    // Await every remote shard's ack, then confirm ITS durability with one
    // fsync_barrier (H1-BARRIER pattern) — the SwapDb SPSC arm cannot
    // `.await` a barrier itself, so the coordinator closes that gap here,
    // after observing the ack (ordering proof in the doc comment above).
    // Every leg is drained even after a failure (mirrors `coordinate_mset`):
    // a timed-out/closed/unconfirmed leg must not collapse into a false OK,
    // but it also must not skip confirming the OTHER shards.
    let mut leg_err: Option<Frame> = None;
    for (target, rx) in targets.into_iter().zip(receivers) {
        match rx.recv().await {
            Ok(()) => {
                if let Some(pool) = aof_pool
                    && pool.fsync_barrier(target).await.is_err()
                    && leg_err.is_none()
                {
                    leg_err = Some(Frame::Error(bytes::Bytes::from_static(
                        b"ERR SWAPDB durability unconfirmed on a remote shard \
                          (fsync barrier failed after the swap was already applied)",
                    )));
                }
            }
            Err(_) => {
                if leg_err.is_none() {
                    leg_err = Some(Frame::Error(bytes::Bytes::from_static(
                        b"ERR cross-shard reply channel closed during SWAPDB",
                    )));
                }
            }
        }
    }
    if let Some(err) = leg_err {
        return err;
    }

    Frame::SimpleString(bytes::Bytes::from_static(b"OK"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Both arms are driven at a millisecond timeout via `recv_reply_within`
    // rather than the real 30s constant. Gated to runtime-tokio because the
    // timer arm is the tokio one under this cfg (the monoio sleep needs a
    // monoio runtime); the logic under test is runtime-independent.
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn recv_reply_reports_closed_when_sender_is_dropped() {
        let (tx, rx) = channel::oneshot::<Frame>();
        drop(tx);
        let got = recv_reply_within(rx, std::time::Duration::from_millis(50)).await;
        assert_eq!(
            got.err(),
            Some(ReplyFailure::Closed),
            "a dropped sender is a closed channel, not a timeout"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn recv_reply_reports_timeout_when_target_never_replies() {
        // Hold the sender alive and never send: a wedged owner shard. This is
        // the case that must NOT be reported as "closed" — the target may still
        // be executing, so the caller cannot claim the command never ran.
        let (_tx, rx) = channel::oneshot::<Frame>();
        let got = recv_reply_within(rx, std::time::Duration::from_millis(20)).await;
        assert_eq!(
            got.err(),
            Some(ReplyFailure::TimedOut),
            "a silent-but-open target is a timeout, not a closed channel"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn recv_reply_returns_the_reply_when_one_arrives() {
        let (tx, rx) = channel::oneshot::<Frame>();
        let _ = tx.send(Frame::SimpleString(Bytes::from_static(b"PONG")));
        let got = recv_reply_within(rx, std::time::Duration::from_millis(50)).await;
        assert!(matches!(got, Ok(Frame::SimpleString(ref s)) if s.as_ref() == b"PONG"));
    }

    #[test]
    fn test_btreemap_ascending_order() {
        // BTreeMap guarantees ascending shard order -- VLL deadlock prevention
        let keys = vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"key3"),
            Bytes::from_static(b"key4"),
        ];
        let mut groups: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
        for key in &keys {
            let shard = key_to_shard(key, 4);
            groups.entry(shard).or_default().push(key.clone());
        }
        let shard_ids: Vec<usize> = groups.keys().copied().collect();
        for i in 1..shard_ids.len() {
            assert!(
                shard_ids[i] > shard_ids[i - 1],
                "BTreeMap should yield ascending shard IDs"
            );
        }
    }

    #[test]
    fn test_hash_tag_co_location() {
        let keys = vec![
            Bytes::from_static(b"{user}.name"),
            Bytes::from_static(b"{user}.email"),
            Bytes::from_static(b"{user}.age"),
        ];
        let mut shards: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for key in &keys {
            shards.insert(key_to_shard(key, 8));
        }
        assert_eq!(
            shards.len(),
            1,
            "all keys with same hash tag should map to one shard"
        );
    }

    // ── R-1: bounded spsc_send backpressure ──
    // A single-capacity ring at `target_idx` for (my_shard=1, target_shard=0),
    // since `ChannelMesh::target_index(1, 0) == 0`.
    #[cfg(feature = "runtime-tokio")]
    fn make_ring(
        capacity: usize,
    ) -> (
        Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
        ringbuf::HeapCons<ShardMessage>,
        Vec<Arc<channel::Notify>>,
    ) {
        use ringbuf::HeapRb;
        use ringbuf::traits::Split;
        let (prod, cons) = HeapRb::<ShardMessage>::new(capacity).split();
        let dispatch_tx = Rc::new(RefCell::new(vec![prod]));
        let notifiers = vec![Arc::new(channel::Notify::new())];
        (dispatch_tx, cons, notifiers)
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn spsc_send_pushes_when_ring_has_space() {
        use ringbuf::traits::Consumer;
        let (dispatch_tx, mut cons, notifiers) = make_ring(4);
        let outcome = spsc_send(
            &dispatch_tx,
            1, // my_shard
            0, // target_shard -> target_idx 0
            ShardMessage::BlockCancel { wait_id: 42 },
            &notifiers,
        )
        .await;
        assert_eq!(outcome, PushOutcome::Pushed);
        // The message actually landed in the ring.
        assert!(
            matches!(
                cons.try_pop(),
                Some(ShardMessage::BlockCancel { wait_id: 42 })
            ),
            "pushed message must be present in the target ring"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn spsc_send_gives_up_when_ring_never_drains() {
        // Fill the capacity-1 ring so every push attempt fails: a wedged target.
        // The bounded retry MUST terminate with Backpressure rather than spin
        // forever — a regression to the old unbounded loop would hang here. A
        // tiny budget (5 retries × 1ms) keeps the test fast and deterministic.
        let (dispatch_tx, _cons, notifiers) = make_ring(1);
        // Pre-fill the ring (hold `_cons` so nothing drains it).
        {
            use ringbuf::traits::Producer;
            let mut prods = dispatch_tx.borrow_mut();
            assert!(
                prods[0]
                    .try_push(ShardMessage::BlockCancel { wait_id: 1 })
                    .is_ok()
            );
        }
        let start = std::time::Instant::now();
        let outcome = spsc_send_bounded(
            &dispatch_tx,
            1,
            0,
            ShardMessage::BlockCancel { wait_id: 2 },
            &notifiers,
            5,
            std::time::Duration::from_millis(1),
        )
        .await;
        assert_eq!(
            outcome,
            PushOutcome::Backpressure,
            "a ring that never drains must yield Backpressure, not hang"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "bounded retry must terminate promptly, took {:?}",
            start.elapsed()
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_mget_all_local() {
        use crate::storage::Database;
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(b"a", Bytes::from_static(b"1"));
        dbs[0].set_string(b"b", Bytes::from_static(b"2"));

        let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
        // coordinate_mget uses with_shard_db for local keys; ShardSlice must be initialized.
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ];

        // With num_shards=1, all keys are local
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_mget(
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"1")));
                assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"2")));
            }
            _ => panic!("expected Array response"),
        }
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_mset_all_local() {
        use crate::storage::Database;
        let dbs = vec![Database::new()];
        let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"x")),
            Frame::BulkString(Bytes::from_static(b"10")),
            Frame::BulkString(Bytes::from_static(b"y")),
            Frame::BulkString(Bytes::from_static(b"20")),
        ];

        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let mut local_barrier_pending = false;
        let result = coordinate_mset(
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            None,
            &None,
            &mut local_barrier_pending,
            &response_pool,
        )
        .await;
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify keys were set via ShardSlice path.
        crate::shard::slice::with_shard_db(0, |db| {
            let entry = db.get(b"x");
            assert!(entry.is_some());
        });
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_del_all_local() {
        use crate::storage::Database;
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(b"a", Bytes::from_static(b"1"));
        dbs[0].set_string(b"b", Bytes::from_static(b"2"));
        dbs[0].set_string(b"c", Bytes::from_static(b"3"));

        let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
        // coordinate_multi_del_or_exists uses with_shard_db for local keys; ShardSlice must be initialized.
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
        ];

        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let mut local_barrier_pending = false;
        let result = coordinate_multi_del_or_exists(
            b"DEL",
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            None,
            &None,
            &mut local_barrier_pending,
            &response_pool,
        )
        .await;
        assert_eq!(result, Frame::Integer(2)); // a and b deleted, nonexistent = 0
    }

    // ── aggregate_doc_freq tests ───────────────────────────────────────────────

    /// Helper: build a DocFreq response frame from a list of (term, df) pairs + N.
    fn make_doc_freq_frame(term_dfs: &[(&str, u32)], n: u32) -> Frame {
        let mut items: Vec<Frame> = Vec::new();
        for (term, df) in term_dfs {
            items.push(Frame::BulkString(Bytes::copy_from_slice(term.as_bytes())));
            items.push(Frame::Integer(i64::from(*df)));
        }
        items.push(Frame::BulkString(Bytes::from_static(b"N")));
        items.push(Frame::Integer(i64::from(n)));
        Frame::Array(items.into())
    }

    #[test]
    fn test_aggregate_doc_freq_two_shards() {
        // Shard A: "machine" df=3, N=10
        // Shard B: "machine" df=5, N=15
        // Global: "machine" df=8, N=25
        let shard_a = make_doc_freq_frame(&[("machine", 3)], 10);
        let shard_b = make_doc_freq_frame(&[("machine", 5)], 15);

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 25, "global N should be 10+15=25");
        assert_eq!(
            global_df.get("machine").copied(),
            Some(8),
            "global df for 'machine' should be 3+5=8"
        );
    }

    #[test]
    fn test_aggregate_doc_freq_missing_term_on_one_shard() {
        // Shard A: "rare" df=1, N=10
        // Shard B: no "rare" entry, N=8
        // Global: "rare" df=1, N=18
        let shard_a = make_doc_freq_frame(&[("rare", 1)], 10);
        let shard_b = make_doc_freq_frame(&[], 8); // empty term list, just N=8

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 18, "global N should be 10+8=18");
        assert_eq!(
            global_df.get("rare").copied(),
            Some(1),
            "global df for 'rare' should be 1 (only present on shard A)"
        );
    }

    #[test]
    fn test_aggregate_doc_freq_multiple_terms() {
        // Two shards each have two terms
        let shard_a = make_doc_freq_frame(&[("rust", 4), ("async", 2)], 20);
        let shard_b = make_doc_freq_frame(&[("rust", 6), ("async", 3)], 30);

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 50, "global N should be 20+30=50");
        assert_eq!(global_df.get("rust").copied(), Some(10), "rust df=4+6=10");
        assert_eq!(global_df.get("async").copied(), Some(5), "async df=2+3=5");
    }

    #[test]
    fn test_aggregate_doc_freq_error_frame_skipped() {
        // If one shard returns an error (e.g. index not found), it should be skipped.
        let shard_a = make_doc_freq_frame(&[("term", 3)], 10);
        let shard_err = Frame::Error(Bytes::from_static(b"ERR unknown index"));

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_err]);

        // Error frame should be skipped; only shard_a contributes
        assert_eq!(global_n, 10);
        assert_eq!(global_df.get("term").copied(), Some(3));
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_scatter_text_search_single_shard_skips_dfs() {
        // Single-shard (num_shards==1): scatter_text_search must return immediately
        // from its own inline fast path (run_text_query_on_index) without sending
        // any DocFreq or TextSearch ShardMessages via SPSC. We verify this by:
        //   1. Passing an empty dispatch_tx (no SPSC channels — would panic if used)
        //   2. Verifying the result is an Array (success format, not a channel error)
        //
        // We use an empty TextStore (no indexes), so the result is "ERR no such index".
        // That's still the correct single-shard path — no channels were touched.
        use crate::storage::Database;

        let dbs = vec![Database::new()];
        let (shard_databases_inner, mut inits) = ShardDatabases::new(vec![dbs]);
        // scatter_text_search's single-shard fast path uses with_shard directly;
        // ShardSlice must be initialized on this thread.
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let shard_databases = Arc::new(shard_databases_inner);

        // Empty dispatch_tx — any SPSC send would panic (no channels configured).
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();

        let result = scatter_text_search(
            Bytes::from_static(b"nonexistent_index"),
            Bytes::from_static(b"machine"), // raw query bytes (fts-query-eval-dispatch 2b)
            10,
            0,
            10,
            0, // my_shard
            1, // num_shards = 1 -> single-shard fast path
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            None, // highlight_opts
            None, // summarize_opts
            0,    // db_index
        )
        .await;

        // Should be "ERR no such index" (single-shard run_text_query_on_index path),
        // NOT a channel error. This proves the DFS pre-pass was skipped entirely.
        match &result {
            Frame::Error(e) => {
                let msg = std::str::from_utf8(e).unwrap_or("");
                assert!(
                    msg.contains("no such index"),
                    "expected 'no such index' error for missing index, got: {}",
                    msg
                );
            }
            other => panic!("expected Error frame, got: {:?}", other),
        }
    }
}
