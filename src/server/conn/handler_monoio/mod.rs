// Note: some imports/variables may be conditionally used across feature flags
//! Monoio connection handler using ownership-based I/O (AsyncReadRent/AsyncWriteRent).
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

mod dispatch;
mod ft;
pub(crate) mod idle_park;
mod pubsub;
mod read;
mod txn;
mod write;

/// c10k C1 — bound a reply write so a peer that stops reading cannot park the
/// handler forever.
///
/// `write_all` on a socket whose receive window is closed never returns. The
/// handler is then stuck holding the whole serialized reply (this handler
/// coalesces an entire batch into ONE `Bytes` before its single write syscall,
/// so a deep pipeline is hundreds of MB) plus its `maxclients` slot, for as
/// long as the client cares to wait. N such clients is an OOM that costs the
/// attacker nothing but an unread socket.
///
/// `monoio::select!` drops the losing future — the same pattern the idle
/// timeout already uses on the read side — which takes the reply buffer with
/// it. That is exactly what we want here: we are tearing the connection down,
/// so there is nobody left to deliver it to.
///
/// Evaluates to `true` when the write completed, `false` when it failed or
/// stalled (both meaning "close this connection"). `$wt` is an
/// `Option<Duration>`; `None` keeps the pre-C1 wait-forever behaviour.
macro_rules! write_all_bounded {
    ($stream:expr, $data:expr, $wt:expr, $cap:expr, $live:expr, $client_id:expr) => {{
        // Bind first: `$data` is typically a `split().freeze()` and must be
        // evaluated exactly once, before we can measure it.
        let data = $data;
        let pending = data.len();
        if $cap != 0 && pending > $cap {
            // c10k C1: `client-output-buffer-limit`. Redis leaves the normal
            // class unlimited by default, which is exactly why an unread
            // socket can OOM it; moon ships a real ceiling instead. Refuse the
            // reply rather than buffer it, and close — Redis's own behaviour
            // once a hard limit is crossed.
            tracing::warn!(
                "Connection {} reply of {} bytes exceeds the output buffer limit of {} — closing",
                $client_id,
                pending,
                $cap,
            );
            false
        } else {
            // Publish what this connection is holding, so a stalled reply is
            // visible in CLIENT LIST (`obl`/`omem`) while it happens.
            $live.begin_write(pending);
            // c10k C1: only a write big enough to actually block arms the
            // watchdog. A timer per batch flush would land once per command
            // at pipeline depth 1 and buy nothing there — a reply that fits
            // in the socket buffer never blocks. See `util::arm_write_timeout`.
            let ok = match super::util::arm_write_timeout(pending, $wt) {
                None => {
                    let (r, _): (std::io::Result<usize>, bytes::Bytes) =
                        $stream.write_all(data).await;
                    r.is_ok()
                }
                Some(dur) => {
                    let mut ok = false;
                    monoio::select! {
                        res = $stream.write_all(data) => {
                            let (r, _): (std::io::Result<usize>, bytes::Bytes) = res;
                            ok = r.is_ok();
                        }
                        _ = monoio::time::sleep(dur) => {
                            tracing::warn!(
                                "Connection {} reply write made no progress for {}ms — closing ({} bytes held, client is not reading)",
                                $client_id,
                                dur.as_millis(),
                                pending,
                            );
                        }
                    }
                    ok
                }
            };
            $live.end_write(pending, ok);
            ok
        }
    }};
}

use crate::runtime::cancel::CancellationToken;
use bytes::{Bytes, BytesMut};
use ringbuf::traits::Producer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::command::metadata;
use crate::command::{DispatchResult, dispatch, dispatch_read};
use crate::persistence::aof;
use crate::protocol::Frame;
use crate::server::conn::shared::is_transaction_control;
use crate::shard::dispatch::key_to_shard;
use crate::shard::mesh::ChannelMesh;
use crate::storage::eviction::{EvictionRun, evict_to_budget};
use crate::workspace::{strip_workspace_prefix_from_response, workspace_rewrite_args};

use super::affinity::MigratedConnectionState;
use super::{
    apply_resp3_conversion, execute_transaction_sharded, extract_bytes, extract_command,
    extract_primary_key, handle_blocking_command_monoio, handle_config, is_multi_key_command,
    propagate_shard_subscription, propagate_subscription, queued_blocking_frame, resp3_shape_for,
    try_inline_dispatch_loop, unpropagate_shard_subscription, unpropagate_subscription,
};
use crate::framevec;
use crate::pubsub::subscriber::Subscriber;
use crate::server::codec::RespCodec;
use crate::server::response_slot::ResponseSlotPool;
use crate::shard::dispatch::ShardMessage;
// L3b: the Phase 2b cross-shard batch path awaits replies via the
// zero-allocation `ResponseSlotPool` (tokio parity, handler_sharded), not a
// per-batch flume oneshot. Cross-thread wakes DO reach monoio tasks (the
// `sync` feature's waker channel + driver unpark — proven at runtime by
// tests/spsc_wake_floor_red.rs::swf0 on both drivers); the slot's
// AtomicWaker rides that mechanism. Transaction (txn.rs) and blocking-write
// (write.rs) paths remain on oneshots — they are off the hot path.

/// RAII client-registry entry: deregisters on drop. Lives as a handler
/// local, EXCEPT for a task-parked connection (c1M P1), where it travels
/// inside [`MonoioHandlerResult::ParkIdle`] to the readiness watcher so the
/// CLIENT LIST/KILL entry and maxclients slot survive the parked lifetime.
#[cfg(feature = "runtime-monoio")]
pub struct RegistryGuard(pub(crate) u64);

#[cfg(feature = "runtime-monoio")]
impl Drop for RegistryGuard {
    fn drop(&mut self) {
        crate::client_registry::deregister(self.0);
    }
}

/// Park plumbing for `handle_connection_sharded_monoio` (c1M P1).
#[cfg(feature = "runtime-monoio")]
pub(crate) struct ParkArgs {
    /// Only call sites that ROUTE `ParkIdle` (spawning the readiness
    /// watcher) may pass true — anywhere else a park return would drop the
    /// stream and silently close a healthy idle connection.
    pub can_park: bool,
    /// Registration carried across a park/wake cycle. A resumed parked
    /// connection reuses this guard instead of re-registering, so the
    /// CLIENT LIST entry (name, connected_at, kill state) stays
    /// continuously intact and TOTAL_CLIENTS/shard counters don't churn —
    /// there is no deregistered window a racing CLIENT LIST/KILL could
    /// observe. `None` everywhere except `spawn_resumed_parked_conn`.
    pub kept_registration: Option<RegistryGuard>,
}

#[cfg(feature = "runtime-monoio")]
impl ParkArgs {
    // Only referenced by the Linux-gated R-6 fail-open re-serve sites in
    // conn_accept.rs; dead on non-Linux builds.
    #[allow(dead_code)]
    pub(crate) const NO_PARK: ParkArgs = ParkArgs {
        can_park: false,
        kept_registration: None,
    };
    pub(crate) const PARK: ParkArgs = ParkArgs {
        can_park: true,
        kept_registration: None,
    };
}

/// Result of `handle_connection_sharded_monoio` execution.
///
/// Same purpose as the Tokio handler's `HandlerResult`: the generic handler cannot
/// perform FD extraction, so it returns the stream when migration is triggered.
#[cfg(feature = "runtime-monoio")]
pub enum MonoioHandlerResult {
    /// Normal connection close.
    Done,
    /// Migration triggered: caller should extract raw FD and send via SPSC.
    MigrateConnection {
        state: MigratedConnectionState,
        target_shard: usize,
    },
    /// c1M P1: the connection idled past `--conn-park-secs` in the
    /// downshifted state; the handler task exits and the caller parks the
    /// stream behind a tiny readiness watcher
    /// (`conn_accept::spawn_parked_idle_watcher`). Only returned when the
    /// caller opted in via `park.can_park` — every other call site would
    /// drop the stream and silently close a healthy connection.
    /// `registry_guard` keeps the CLIENT LIST/KILL entry (and its maxclients
    /// slot) alive for the parked lifetime; on wake the watcher hands it
    /// back to the resumed handler (`ParkArgs::kept_registration`), so the
    /// registry entry is never dropped across a park/wake cycle.
    ParkIdle {
        state: Box<MigratedConnectionState>,
        registry_guard: RegistryGuard,
    },
    /// PSYNC arrived on this connection. Caller must hand the underlying
    /// `monoio::net::TcpStream` to
    /// `crate::replication::master::handle_psync_inline_single_shard` /
    /// `handle_psync_inline_multi_shard` for snapshot transfer + live streaming.
    HijackForPsync {
        client_repl_id: String,
        client_offset: i64,
        peer_addr: String,
    },
}

/// Write-path eviction gate shared by the slice and guard write paths.
///
/// Reads the runtime config, fetches this shard's elastic budget (GAP-1),
/// and runs the spill-aware evictor when disk offload is wired, the plain
/// budget evictor otherwise. Returns the evictor's OOM frame verbatim.
///
/// Task #34 review (defect 1 follow-through): this gate has no
/// `ShardManifest` handle (only the tick-driven memory-pressure cascade in
/// `persistence_tick.rs` does), so under `--disk-offload enable` the
/// spill-sender branch below reliably takes the "no manifest reachable"
/// plain-drop fallback inside
/// `evict_to_budget` for EVERY
/// write past `maxmemory` — this is, empirically, the actual eviction path a
/// live server hits on ordinary writes (HSET, SET, ...) under disk-offload,
/// not the sync `SpillContext` path. It previously called the non-reporting
/// wrapper (hardcoded no-op sink), so those plain-drops never reached
/// `record_reason_del_conn` — silently unreported eviction under the single
/// most common disk-offload deployment shape. Now wired to the same sink the
/// no-spill-sender branch below already uses.
#[cfg(feature = "runtime-monoio")]
fn run_write_eviction_gate(
    ctx: &super::core::ConnectionContext,
    db: &mut crate::storage::db::Database,
    sel_db: usize,
    cmd: &[u8],
) -> Result<(), Frame> {
    let rt = ctx.runtime_config.read();
    let budget = ctx.shard_databases.elastic_budget(ctx.shard_id);
    let global_result = if let Some(ref sender) = ctx.spill_sender {
        let mut fid = ctx.spill_file_id.get();
        let dir = ctx
            .disk_offload_dir
            .as_deref()
            .unwrap_or(std::path::Path::new("."));
        let res = evict_to_budget(
            db,
            &rt,
            EvictionRun::async_spill(sender, dir, &mut fid, sel_db, None)
                .budget(budget)
                .report(&mut |key| {
                    crate::replication::reason_del::record_reason_del_conn(
                        &ctx.repl_state,
                        ctx.shard_id,
                        ctx.num_shards,
                        ctx.aof_pool.as_ref(),
                        sel_db,
                        key,
                    );
                }),
        );
        ctx.spill_file_id.set(fid);
        res
    } else {
        // task #34 (Wave A): plain-drop eviction on the generic per-command
        // write path (the local-shard leg of any write when disk-offload is
        // off/unwired) — emit a dual-plane DEL for every victim so an
        // attached replica and the AOF replay converge with the master's
        // own eviction decision.
        evict_to_budget(
            db,
            &rt,
            EvictionRun::plain().budget(budget).report(&mut |key| {
                crate::replication::reason_del::record_reason_del_conn(
                    &ctx.repl_state,
                    ctx.shard_id,
                    ctx.num_shards,
                    ctx.aof_pool.as_ref(),
                    sel_db,
                    key,
                );
            }),
        )
    };
    // WS6 fix (HIGH, adversarial review 2026-07-08): a command that can only
    // shrink memory (HDEL, SREM, LPOP, ...) must never be REJECTED by either
    // gate below, or a key/db that crosses its noeviction boundary has no
    // self-recovery path. Eviction is still attempted above — an evicting
    // policy may as well reclaim while the write lock is already held; only
    // the reject is bypassed. See `db_quota::is_shrink_only_command`.
    let shrink_only = crate::storage::db_quota::is_shrink_only_command(cmd);
    if !shrink_only {
        global_result?;
    }
    // WS5b: per-db quota, additive and finer-grained than the whole-instance
    // maxmemory gate above. Zero-cost when unconfigured for this db.
    // `_for_command` exempts SELECT/SWAPDB — see `db_quota::command_exempt_from_db_quota`
    // (this chokepoint runs on `metadata::is_write`-flagged commands, which
    // includes SELECT despite it not writing to the current db).
    let db_quota_result =
        crate::storage::db_quota::check_db_maxmemory_for_command(db, sel_db, &rt, cmd);
    if shrink_only { Ok(()) } else { db_quota_result }
}

/// Monoio connection handler using ownership-based I/O (AsyncReadRent/AsyncWriteRent).
/// Dispatches commands through `crate::command::dispatch()` with monoio's ownership I/O model.
#[cfg(feature = "runtime-monoio")]
#[tracing::instrument(skip_all, level = "debug")]
pub(crate) async fn handle_connection_sharded_monoio<
    S: monoio::io::AsyncReadRent + monoio::io::AsyncWriteRent + idle_park::IdleParkRead,
>(
    mut stream: S,
    peer_addr: String,
    ctx: &super::core::ConnectionContext,
    shutdown: CancellationToken,
    client_id: u64,
    can_migrate: bool,
    initial_read_buf: BytesMut,
    migrated_state: Option<&MigratedConnectionState>,
    // Raw socket fd for CLIENT KILL force-close (R-3), or -1 if unavailable
    // (non-unix). Threaded from the concrete spawn site; the generic `S` here
    // has no `AsRawFd` bound.
    kill_fd: i32,
    // c1M P1 park plumbing: opt-in flag + optional registration carried
    // across a park/wake cycle (see [`ParkArgs`]).
    park: ParkArgs,
) -> (MonoioHandlerResult, Option<S>) {
    use monoio::io::AsyncWriteRentExt;

    // Solo-conn spin gate (L1 convoy fix): register this connection on the
    // shard thread so the C2 reply-spin's sibling check (`xshard_may_spin`)
    // sees it. RAII — decrements when the handler returns, including the
    // migration hand-off (the conn re-registers on its new shard's thread).
    let _conn_guard = crate::shard::slice::ShardConnGuard::new();

    // NOTE: do NOT call record_connection_opened() here — the caller
    // (conn_accept.rs) already increments via try_accept_connection().

    // c10k D3 (lazy resumed-buffer sizing): a fresh connection warms up with
    // the full 8 KiB per buffer (no growth stall on its first pipeline). A
    // REHYDRATED handler (park wake / migration) starts at 512 B: wakes
    // arrive in fleet-sized bursts (synchronized keepalives), and 2×8 KiB
    // per wake dominated the burst working set before the first byte was
    // even parsed. BytesMut grows on demand — a busy resumed conn pays one
    // amortized regrow; the idle majority never pays the 16 KiB at all.
    let rehydrated = migrated_state.is_some();
    let init_cap = if rehydrated { 512 } else { 8192 };
    let mut read_buf = if initial_read_buf.is_empty() {
        BytesMut::with_capacity(init_cap)
    } else {
        let mut buf = initial_read_buf;
        buf.reserve(init_cap);
        buf
    };
    let mut write_buf = BytesMut::with_capacity(init_cap);
    // c10k A1: set when a blocking command leaves unparsed input in
    // `read_buf` (see the read-skip guard in the main loop).
    // #438: a resumed MIGRATED (or task-park-resumed) connection starts with
    // the source handler's unparsed remainder already in read_buf — without
    // arming the flag its first iteration awaited a fresh socket read and
    // the carried bytes sat unprocessed until the client happened to send
    // more (pipelined tails crossing a migration stalled indefinitely).
    let mut carried_input = !read_buf.is_empty();
    let mut codec = RespCodec::default();
    let mut conn = super::core::ConnectionState::new(
        client_id,
        peer_addr.clone(),
        &ctx.requirepass,
        ctx.shard_id,
        ctx.num_shards,
        can_migrate,
        ctx.runtime_config.read().acllog_max_len,
        migrated_state,
    );
    conn.refresh_acl_cache(&ctx.acl_table);
    let db_count = ctx.shard_databases.db_count();

    // Register in global client registry for CLIENT LIST/INFO/KILL. A
    // resumed parked connection arrives with its registration still held
    // (`kept_registration`) and reuses it — the entry was never dropped, so
    // name/connected_at/kill state persist and counters don't churn. The
    // `live_handle` miss arm is unreachable while the guard is alive (the
    // guard is the only deregistration path); registering fresh there is a
    // fail-safe, not a code path.
    let (client_live, registry_guard) = match park.kept_registration {
        Some(guard) => {
            let live = crate::client_registry::live_handle(client_id).unwrap_or_else(|| {
                // #438 conn-secondary: reaching here means the entry vanished
                // while its guard was alive — an invariant violation, not a
                // code path. Fail open (re-register so the conn stays served
                // and killable) but LOUDLY; register() itself now balances
                // the counters if the entry raced back into existence.
                tracing::warn!(
                    "client {}: registry entry missing on park resume despite live guard; re-registering",
                    client_id
                );
                crate::client_registry::register(
                    client_id,
                    peer_addr.clone(),
                    ctx.local_addr_string(),
                    conn.current_user.clone(),
                    ctx.shard_id,
                    kill_fd,
                )
            });
            (live, guard)
        }
        None => (
            crate::client_registry::register(
                client_id,
                peer_addr.clone(),
                ctx.local_addr_string(),
                conn.current_user.clone(),
                ctx.shard_id,
                kill_fd,
            ),
            RegistryGuard(client_id),
        ),
    };

    // Functions API registry — LAZY per connection (P-1 footprint): built on
    // first FUNCTION/FCALL/FCALL_RO via `ensure_function_registry`, so the
    // >99% of connections that never touch the Functions API pay zero
    // registry + eviction-ctx cost. Kept as a local because Rc<RefCell<>> is
    // !Send. The eviction ctx (Gap B — FCALL-internal `redis.call` writes run
    // the same OOM gate as EVAL) is built by `ctx.build_lua_eviction_ctx()`.
    // moon#514: PER-SHARD, not per-connection. The old `Rc::new(RefCell::new(
    // None))` here scoped the whole Functions API to one TCP connection —
    // `FUNCTION LOAD` on one connection left `FUNCTION LIST` empty on the
    // next, and every other connection's `FCALL` answered `ERR Function not
    // found`. This handle is the shard thread's single registry, which the
    // SPSC drain loop also reaches to apply fan-outs and run routed FCALLs.
    let func_registry: Rc<RefCell<Option<crate::scripting::FunctionRegistry>>> =
        crate::scripting::shard_function_registry();

    // Pre-allocate read buffer outside the loop to avoid per-read heap allocation.
    // Monoio's ownership I/O takes ownership and returns the buffer, so we reassign.
    // D3: same lazy sizing as read_buf/write_buf — the first read of a
    // rehydrated conn is usually a probe-sized frame (keepalive PING); the
    // shrink logic at the loop tail governs the steady state either way.
    let mut tmp_buf = vec![0u8; init_cap];

    // c10k W11: two-stage idle park (see idle_park.rs). Cancel-capable
    // streams register for the shard chore's ≥1s sweep; `downshifted` tracks
    // whether this connection currently holds the probe-sized working set.
    let idle_reg = S::SUPPORTS_IDLE_PARK.then(|| idle_park::register(client_id));
    let mut downshifted = false;

    // Client idle timeout (`timeout N`) is NOT enforced here — see
    // `client_registry::kill_idle_clients`, run by the 1 s shard chore.
    //
    // c10k hardening D1: this used to be a `select!` arm racing the read
    // against `sleep(timeout)`, sitting FIRST in the read loop's if/else
    // chain below. That made the stage-1 downshift, the stage-2 park and
    // task-exit parking all structurally unreachable whenever `timeout` was
    // set — every connection silently reverted from the parked ~3.3 KB to
    // its full ~46 KB working set, in exactly the deployments that use the
    // only slowloris knob we ship. It also read the config once at setup, so
    // `CONFIG SET timeout` never reached a live connection, and it had no
    // exemption for replication links. The sweep has none of those problems
    // and additionally reaches connections whose handler task has exited.

    // c10k C1: reply-write ceiling. 0 = wait forever (pre-C1 behaviour).
    // Read once, like the idle timeout above — this must not touch the lock on
    // the hot path.
    let (write_timeout, out_cap_normal) = {
        let rt = ctx.runtime_config.read();
        (
            match rt.client_write_timeout_ms {
                0 => None,
                ms => Some(std::time::Duration::from_millis(ms)),
            },
            rt.client_output_buffer_limit_normal,
        )
    };

    // Pre-allocate batch containers outside the loop to avoid per-batch heap allocation.
    // These are cleared and reused each iteration instead of being recreated.
    let mut responses: Vec<Frame> = Vec::with_capacity(64);
    // The trailing `Resp3Shape` is the reply shape, classified at ENQUEUE time
    // where the command's args are still in scope. The batch reply carries only
    // the command NAME, so without this tag a cross-shard reply could not be
    // converted at all — and skipping it would re-create the exact
    // "shape changes by context" defect. It is `Copy` and one byte, so it costs
    // no allocation on the shard hot path (task `resp3-type-fidelity` §3).
    // moon#460: no command name here. The cross-shard reply loop used to need
    // it to pick the RESP3 conversion; since the shape is classified at ENQUEUE
    // into a 1-byte `Copy` tag (`Resp3Shape`), the loop reads only the tag. The
    // field survived as `_cmd_name` and cost a `Bytes` clone per cross-shard
    // command on the shard hot path.
    type RemoteMeta = (
        usize,
        Option<Bytes>,
        Option<crate::tracking::invalidation::TrackedWriteKeys>,
        crate::protocol::resp3::Resp3Shape,
    );
    let mut remote_groups: HashMap<
        usize,
        Vec<(
            usize,
            std::sync::Arc<Frame>,
            Option<Bytes>,
            Option<crate::tracking::invalidation::TrackedWriteKeys>,
            crate::protocol::resp3::Resp3Shape,
        )>,
    > = HashMap::with_capacity(ctx.num_shards);
    let mut reply_futures: Vec<(Vec<RemoteMeta>, usize)> = Vec::with_capacity(ctx.num_shards);
    // v3-5 group commit: response indexes of coordinator LOCAL-leg writes whose
    // AOF append was enqueued but not yet fsync-confirmed (appendfsync=always).
    // Drained by ONE fsync_barrier(ctx.shard_id) at end of batch.
    let mut local_leg_write_idxs: Vec<usize> = Vec::new();

    // Pre-allocated response slots for zero-allocation cross-shard dispatch
    // (L3b, tokio parity — handler_sharded/mod.rs). One slot per target shard;
    // Phase 2b sends at most one slotted batch per target per round and drains
    // every pushed slot before the round ends, so a slot is always EMPTY when
    // reused. The pool lives on this task's stack — see the await-side SAFETY
    // note in Phase 2b for the lifetime contract.
    let response_pool = ResponseSlotPool::new(ctx.num_shards, ctx.shard_id);

    // Pre-allocate frames Vec outside the loop; reused via .clear() each iteration.
    let mut frames: Vec<Frame> = Vec::with_capacity(64);
    // Set when the parser rejects a frame. The connection still dies, but not
    // silently and not before the valid frames that preceded the fault in the
    // same read have been executed and answered.
    let mut proto_fault: Option<crate::protocol::ProtoFault> = None;

    loop {
        // Check if CLIENT KILL targeted this connection (lock-free, QW8)
        if client_live.is_killed() {
            break;
        }

        // Subscriber mode: bidirectional select on client commands + published messages.
        //
        // RESP2 ONLY. Under RESP3 a subscribed connection stays in the normal
        // command loop and takes the delivery branch further down instead —
        // that is what lets it keep issuing commands while subscribed, which
        // is a reason RESP3 exists and which Redis allows. Entering this loop
        // for a RESP3 connection is what put it in the RESP2 jail.
        if conn.subscription_count > 0 && conn.protocol_version < 3 {
            #[allow(clippy::unwrap_used)]
            // conn.pubsub_rx is always Some when conn.subscription_count > 0
            let rx = conn.pubsub_rx.as_ref().unwrap();
            // c10k W1: rent the main loop's tmp_buf instead of allocating a
            // fresh zeroed 8 KiB per select iteration. The buffer is only
            // LOST when the pubsub arm wins (the dropped read op owns it —
            // io_uring cancel semantics); the refill below re-arms it. A
            // read-win restores ownership, so command traffic allocates
            // nothing. Stale bytes past `n` are never read (`&buf[..n]`).
            if tmp_buf.len() < 8192 {
                tmp_buf.resize(8192, 0);
            }
            let sub_tmp_buf = std::mem::take(&mut tmp_buf);
            // #438: a deferred batch tail (subscribe/blocking carry) already
            // sits re-encoded in read_buf — parse it without awaiting the
            // socket. CARRY_READY is an impossible real read length (reads
            // are bounded by the 8 KiB buffer), used as an in-band "no new
            // bytes, just parse" marker so the arm's parse loop is shared.
            // The flag is only CLEARED inside the read arm's body — if the
            // pubsub or shutdown arm wins this select round, the carry stays
            // armed and retries next iteration (a take() here would eat the
            // flag on a lost race and re-introduce the stall).
            const CARRY_READY: usize = usize::MAX;
            let have_carry = !read_buf.is_empty() && carried_input;
            monoio::select! {
                read_result = async {
                    if have_carry {
                        (Ok(CARRY_READY), sub_tmp_buf)
                    } else {
                        stream.read(sub_tmp_buf).await
                    }
                } => {
                    // Read arm won: the carry (if any) is being consumed now.
                    carried_input = false;
                    let (result, buf) = read_result;
                    tmp_buf = buf;
                    let buf = &tmp_buf;
                    match result {
                        Ok(0) => {
                            // Client half-closed — break out of loop.
                            // Stream drop (end of function) triggers monoio's cleanup.
                            break;
                        }
                        Ok(n) => {
                            if n != CARRY_READY {
                                read_buf.extend_from_slice(&buf[..n]);
                            }
                            // Parse frames from buffer
                            loop {
                                match codec.decode_frame(&mut read_buf) {
                                    Ok(Some(frame)) => {
                                        if let Some((cmd, cmd_args)) = extract_command(&frame) {
                                            match cmd {
                                                _ if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'subscribe' command"));
                                                        let mut resp_buf = BytesMut::new();
                                                        codec.encode_frame(&err, &mut resp_buf);
                                                        let data = resp_buf.freeze();
                                                        let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(channel) = extract_bytes(arg) {
                                                            // ACL channel permission check
                                                            let denied = {
                                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                                let acl_guard = ctx.acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&conn.current_user, channel.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                                continue;
                                                            }
                                                            #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some when in subscriber mode
                                                            let sub = Subscriber::with_protocol(
                                                                conn.pubsub_tx.clone().unwrap(),
                                                                conn.subscriber_id,
                                                                conn.protocol_version >= 3,
                                                            );
                                                            ctx.pubsub_registry.write().subscribe(channel.clone(), sub);
                                                            propagate_subscription(&ctx.all_remote_sub_maps, &channel, ctx.shard_id, ctx.num_shards, false);
                                                            conn.subscription_count += 1;
                                                            // Register pub/sub affinity for this client IP
                                                            if conn.subscription_count == 1 {
                                                                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                                    ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                                }
                                                            }
                                                            let resp = crate::pubsub::subscribe_response(&channel, conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"SSUBSCRIBE") => {
                                                    // Sharded subscribe from INSIDE RESP2
                                                    // subscriber mode. On Redis's allow-list, so
                                                    // it must be served here rather than refused.
                                                    //
                                                    // The arity guard is not decoration: without
                                                    // it the loop below simply does not run and
                                                    // the client is answered with NOTHING, which
                                                    // it cannot tell from a slow server. The
                                                    // sharded handler already guards here; this
                                                    // arm did not.
                                                    if cmd_args.is_empty() {
                                                        let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'ssubscribe' command"));
                                                        let mut resp_buf = BytesMut::new();
                                                        codec.encode_frame(&err, &mut resp_buf);
                                                        let data = resp_buf.freeze();
                                                        let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(channel) = extract_bytes(arg) {
                                                            let denied = {
                                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                                let acl_guard = ctx.acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&conn.current_user, channel.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                                continue;
                                                            }
                                                            #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some in subscriber mode
                                                            let sub = Subscriber::with_protocol(
                                                                conn.pubsub_tx.clone().unwrap(),
                                                                conn.subscriber_id,
                                                                conn.protocol_version >= 3,
                                                            );
                                                            ctx.pubsub_registry.write().ssubscribe(channel.clone(), sub);
                                                            propagate_shard_subscription(&ctx.all_remote_sub_maps, &channel, ctx.shard_id, ctx.num_shards);
                                                            conn.subscription_count += 1;
                                                            let resp = crate::pubsub::ssubscribe_response(&channel, conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"SUNSUBSCRIBE") => {
                                                    let targets: Vec<Bytes> = if cmd_args.is_empty() {
                                                        ctx.pubsub_registry.write().sunsubscribe_all(conn.subscriber_id)
                                                    } else {
                                                        cmd_args.iter().filter_map(extract_bytes).collect()
                                                    };
                                                    if targets.is_empty() {
                                                        conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                        let resp = crate::pubsub::sunsubscribe_none_response(conn.subscription_count);
                                                        let mut resp_buf = BytesMut::new();
                                                        codec.encode_frame(&resp, &mut resp_buf);
                                                        let data = resp_buf.freeze();
                                                        let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                    } else {
                                                        for ch in &targets {
                                                            ctx.pubsub_registry.write().sunsubscribe(ch.as_ref(), conn.subscriber_id);
                                                            unpropagate_shard_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards);
                                                            conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                            let resp = crate::pubsub::sunsubscribe_response(ch, conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let removed = ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id);
                                                        for ch in &removed {
                                                            unpropagate_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards, false);
                                                        }
                                                        if removed.is_empty() {
                                                            conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                            let resp = crate::pubsub::unsubscribe_none_response(conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for ch in &removed {
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::unsubscribe_response(ch, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    } else {
                                                        for arg in cmd_args {
                                                            if let Some(channel) = extract_bytes(arg) {
                                                                ctx.pubsub_registry.write().unsubscribe(channel.as_ref(), conn.subscriber_id);
                                                                unpropagate_subscription(&ctx.all_remote_sub_maps, &channel, ctx.shard_id, ctx.num_shards, false);
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::unsubscribe_response(&channel, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let err = Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'psubscribe' command"));
                                                        let mut resp_buf = BytesMut::new();
                                                        codec.encode_frame(&err, &mut resp_buf);
                                                        let data = resp_buf.freeze();
                                                        let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                        if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        continue;
                                                    }
                                                    for arg in cmd_args {
                                                        if let Some(pattern) = extract_bytes(arg) {
                                                            // ACL channel permission check
                                                            let denied = {
                                                                #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
                                                                let acl_guard = ctx.acl_table.read().unwrap();
                                                                acl_guard.check_channel_permission(&conn.current_user, pattern.as_ref())
                                                            };
                                                            if let Some(deny_reason) = denied {
                                                                let err = Frame::Error(Bytes::from(format!("NOPERM {}", deny_reason)));
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&err, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                                continue;
                                                            }
                                                            #[allow(clippy::unwrap_used)] // conn.pubsub_tx is always Some when in subscriber mode
                                                            let sub = Subscriber::with_protocol(
                                                                conn.pubsub_tx.clone().unwrap(),
                                                                conn.subscriber_id,
                                                                conn.protocol_version >= 3,
                                                            );
                                                            ctx.pubsub_registry.write().psubscribe(pattern.clone(), sub);
                                                            propagate_subscription(&ctx.all_remote_sub_maps, &pattern, ctx.shard_id, ctx.num_shards, true);
                                                            conn.subscription_count += 1;
                                                            // Register pub/sub affinity for this client IP
                                                            if conn.subscription_count == 1 {
                                                                if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                                                                    ctx.pubsub_affinity.write().register(addr.ip(), ctx.shard_id);
                                                                }
                                                            }
                                                            let resp = crate::pubsub::psubscribe_response(&pattern, conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") => {
                                                    if cmd_args.is_empty() {
                                                        let removed = ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id);
                                                        for pat in &removed {
                                                            unpropagate_subscription(&ctx.all_remote_sub_maps, pat, ctx.shard_id, ctx.num_shards, true);
                                                        }
                                                        if removed.is_empty() {
                                                            conn.subscription_count = ctx.pubsub_registry.read().total_subscription_count(conn.subscriber_id);
                                                            let resp = crate::pubsub::punsubscribe_none_response(conn.subscription_count);
                                                            let mut resp_buf = BytesMut::new();
                                                            codec.encode_frame(&resp, &mut resp_buf);
                                                            let data = resp_buf.freeze();
                                                            let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                            if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                        } else {
                                                            for pat in &removed {
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::punsubscribe_response(pat, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    } else {
                                                        for arg in cmd_args {
                                                            if let Some(pattern) = extract_bytes(arg) {
                                                                ctx.pubsub_registry.write().punsubscribe(pattern.as_ref(), conn.subscriber_id);
                                                                unpropagate_subscription(&ctx.all_remote_sub_maps, &pattern, ctx.shard_id, ctx.num_shards, true);
                                                                conn.subscription_count = conn.subscription_count.saturating_sub(1);
                                                                let resp = crate::pubsub::punsubscribe_response(&pattern, conn.subscription_count);
                                                                let mut resp_buf = BytesMut::new();
                                                                codec.encode_frame(&resp, &mut resp_buf);
                                                                let data = resp_buf.freeze();
                                                                let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                                if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                            }
                                                        }
                                                    }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"PING") => {
                                                    let resp = Frame::Array(framevec![
                                                        Frame::BulkString(Bytes::from_static(b"pong")),
                                                        Frame::BulkString(Bytes::from_static(b"")),
                                                    ]);
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&resp, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"QUIT") => {
                                                    let resp = Frame::SimpleString(Bytes::from_static(b"OK"));
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&resp, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    let _ = wr; // ignore write error on quit
                                                    return (MonoioHandlerResult::Done, None); // exit connection
                                                }
                                                _ if cmd.eq_ignore_ascii_case(b"RESET") => {
                                                    // The sanctioned way out of subscriber mode.
                                                    // Only the SHARDED handler used to accept it,
                                                    // so which answer a client got depended on the
                                                    // shard count.
                                                    //
                                                    // All THREE namespaces, and the remote maps
                                                    // with them. Clearing only two leaves the
                                                    // connection listed under `shard_channels`
                                                    // while it believes it is back to issuing
                                                    // ordinary commands — an unsolicited
                                                    // `smessage` then lands in its reply stream
                                                    // and desynchronises every reply after it,
                                                    // which is the very defect this task exists
                                                    // to fix.
                                                    let gone_ch = { ctx.pubsub_registry.write().unsubscribe_all(conn.subscriber_id) };
                                                    let gone_pat = { ctx.pubsub_registry.write().punsubscribe_all(conn.subscriber_id) };
                                                    let gone_shard = { ctx.pubsub_registry.write().sunsubscribe_all(conn.subscriber_id) };
                                                    for ch in &gone_ch {
                                                        unpropagate_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards, false);
                                                    }
                                                    for pat in &gone_pat {
                                                        unpropagate_subscription(&ctx.all_remote_sub_maps, pat, ctx.shard_id, ctx.num_shards, true);
                                                    }
                                                    for ch in &gone_shard {
                                                        unpropagate_shard_subscription(&ctx.all_remote_sub_maps, ch, ctx.shard_id, ctx.num_shards);
                                                    }
                                                    conn.subscription_count = 0;
                                                    let resp = Frame::SimpleString(Bytes::from_static(b"RESET"));
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&resp, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                    break;
                                                }
                                                _ => {
                                                    // One allow-list, one text — see
                                                    // `server::conn::subscriber_mode`. This loop is
                                                    // RESP2-only now, so the refusal always applies.
                                                    let err = crate::server::conn::subscriber_mode::subscriber_mode_error(cmd);
                                                    let mut resp_buf = BytesMut::new();
                                                    codec.encode_frame(&err, &mut resp_buf);
                                                    let data = resp_buf.freeze();
                                                    let (wr, _): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
                                                    if wr.is_err() { return (MonoioHandlerResult::Done, None); }
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => break, // need more data
                                    Err(_) => return (MonoioHandlerResult::Done, None),  // parse error
                                }
                            }
                        }
                        Err(_) => break, // connection error
                    }
                }
                msg = rx.recv_async() => {
                    match msg {
                        Ok(data) => {
                            // Data is pre-serialized RESP bytes. Coalesce any burst
                            // already queued into ONE write_all — one syscall per
                            // burst instead of per message (the delivery ceiling
                            // under fan-out publish load). Single-message case
                            // stays zero-copy/zero-alloc via the is_empty fast path.
                            const MAX_COALESCE_BYTES: usize = 64 * 1024;
                            let payload: Bytes = if rx.is_empty() {
                                data
                            } else {
                                let mut agg = BytesMut::with_capacity((data.len() * 4).min(MAX_COALESCE_BYTES));
                                agg.extend_from_slice(&data);
                                while agg.len() < MAX_COALESCE_BYTES {
                                    match rx.try_recv() {
                                        Ok(next) => agg.extend_from_slice(&next),
                                        Err(_) => break,
                                    }
                                }
                                agg.freeze()
                            };
                            if !write_all_bounded!(stream, payload, write_timeout, out_cap_normal, client_live, client_id) { break; }
                        }
                        Err(_) => break, // all senders dropped
                    }
                }
                _ = shutdown.cancelled() => { break; }
            }
            continue;
        }

        // Read data from stream using monoio ownership I/O.
        // Reuse pre-allocated buffer; restore length for the read. While
        // downshifted (c10k W11) the park size is the probe buffer; the
        // resize below re-inflates lazily on the first post-idle iteration.
        let park_len = if downshifted {
            idle_park::IDLE_PROBE_BUF
        } else {
            idle_park::PARK_BUF_FULL
        };
        if tmp_buf.len() != park_len {
            tmp_buf.resize(park_len, 0);
        }
        // c10k A1: a blocking command's peer watch may have pulled bytes the
        // client pipelined behind it out of the kernel and into `read_buf`.
        // Pre-A1 those bytes stayed in the socket, so the read below returned
        // them at once; now they are already here, and parking in read() first
        // would hang the pipelined command until the client happened to send
        // more. Skip exactly one read and let the parser drain them. A carry
        // that is only a partial frame parses to nothing, `frames.is_empty()`
        // sends us straight back here, and the flag is already cleared.
        //
        // c10k D1: there is deliberately no `idle_timeout` arm here. `timeout N`
        // is enforced out-of-band by `client_registry::kill_idle_clients` (the
        // 1 s shard chore), which also reaches task-parked connections — an
        // in-loop `select!` on `sleep(timeout)` used to sit ahead of the park
        // and made stage-1 downshift, stage-2 park and task-exit parking all
        // structurally unreachable whenever `timeout` was set.
        if std::mem::take(&mut carried_input) {
            // Nothing to read: `read_buf` already holds unparsed input.
        } else if conn.protocol_version >= 3 && conn.subscription_count > 0 {
            // RESP3 subscriber: deliver pub/sub messages while parked in
            // read(), instead of diverting the connection into the RESP2
            // subscriber loop. This is the whole mechanism behind "RESP3 lets
            // one connection subscribe AND issue commands" — the connection
            // never leaves this loop, so every command still dispatches
            // normally and the deliveries arrive between replies.
            //
            // The channel carries ALREADY-SERIALIZED bytes, framed by
            // `publish()` according to the subscriber's own protocol (Push for
            // RESP3), so this arm writes them verbatim. That also means a
            // delivery can never be spliced into a half-written reply: both
            // are whole frames handed to `write_all` from the same task, and
            // this loop only reaches here when no reply is mid-flight.
            #[allow(clippy::unwrap_used)] // guarded by subscription_count > 0
            let rx = conn.pubsub_rx.as_ref().unwrap();
            let sub_buf = std::mem::take(&mut tmp_buf);
            let mut delivery: Option<bytes::Bytes> = None;
            monoio::select! {
                _ = shutdown.cancelled() => { break; }
                read_result = stream.read(sub_buf) => {
                    let (result, returned_buf) = read_result;
                    tmp_buf = returned_buf;
                    match result {
                        Ok(0) => break,
                        Ok(n) => { read_buf.extend_from_slice(&tmp_buf[..n]); }
                        Err(_) => break,
                    }
                }
                msg = rx.recv_async() => {
                    // The read future loses its buffer here (io_uring cancel
                    // semantics), exactly as the RESP2 subscriber select
                    // above documents; the pre-park sizing re-arms it.
                    delivery = msg.ok();
                }
            }
            if let Some(data) = delivery {
                if !write_all_bounded!(
                    stream,
                    data,
                    write_timeout,
                    out_cap_normal,
                    client_live,
                    client_id
                ) {
                    break;
                }
                continue;
            }
        } else if let Some(rx) = conn.monitor_rx.as_ref()
            && conn.monitor_attached
        {
            // MONITOR: deliver feed lines while parked in read(). A monitor
            // connection stays in this loop — it is not diverted anywhere —
            // because Redis keeps serving it (PING, INFO, RESET all work while
            // attached); only keyspace commands are refused, and that refusal
            // is a gate below, not a separate mode loop.
            //
            // The channel carries already-formatted `+…\r\n` lines, so this
            // arm writes them verbatim. Same tearing argument as the RESP3
            // subscriber arm above: both a reply and a feed line are whole
            // frames written by this one task, and the loop only parks here
            // when no reply is in flight.
            let mon_buf = std::mem::take(&mut tmp_buf);
            let mut line: Option<bytes::Bytes> = None;
            monoio::select! {
                _ = shutdown.cancelled() => { break; }
                read_result = stream.read(mon_buf) => {
                    let (result, returned_buf) = read_result;
                    tmp_buf = returned_buf;
                    match result {
                        Ok(0) => break,
                        Ok(n) => { read_buf.extend_from_slice(&tmp_buf[..n]); }
                        Err(_) => break,
                    }
                }
                msg = rx.recv_async() => {
                    line = msg.ok();
                }
            }
            if let Some(data) = line {
                if !write_all_bounded!(
                    stream,
                    data,
                    write_timeout,
                    out_cap_normal,
                    client_live,
                    client_id
                ) {
                    break;
                }
                continue;
            } else if conn.monitor_attached && !crate::monitor::is_attached(client_id) {
                // The registry dropped this sink because the connection could
                // not keep up. Contracted policy: the monitor DIES, loudly,
                // rather than silently receiving an incomplete feed that an
                // operator would read as a quiet server.
                break;
            }
        } else if conn.tracking_rx.is_some() {
            // CLIENT TRACKING: deliver invalidation Push frames while parked
            // in read(). Only tracking connections take this select — the
            // hot path below is untouched for everyone else. Losing-future
            // buffer semantics mirror the other parked reads in this loop.
            let track_buf = std::mem::take(&mut tmp_buf);
            let mut push_frame: Option<Frame> = None;
            monoio::select! {
                // F1 (#438): tracking conns park in this select (not the
                // cancel-registered reads below), so the shutdown drain
                // reaches them via the token. Losing the read future drops
                // `track_buf` — acceptable, the connection is exiting.
                _ = shutdown.cancelled() => { break; }
                read_result = stream.read(track_buf) => {
                    let (result, returned_buf) = read_result;
                    tmp_buf = returned_buf;
                    match result {
                        Ok(0) => break,
                        Ok(n) => { read_buf.extend_from_slice(&tmp_buf[..n]); }
                        Err(_) => break,
                    }
                }
                push = async {
                    match conn.tracking_rx {
                        Some(ref rx) => rx.recv_async().await.ok(),
                        None => std::future::pending().await,
                    }
                } => {
                    push_frame = push;
                }
            }
            if let Some(frame) = push_frame {
                let mut push_buf = BytesMut::new();
                crate::protocol::serialize_resp3(&frame, &mut push_buf);
                if !write_all_bounded!(
                    stream,
                    push_buf.freeze(),
                    write_timeout,
                    out_cap_normal,
                    client_live,
                    client_id
                ) {
                    break;
                }
                continue;
            }
        } else if downshifted {
            // c10k W11 stage 2: park with the probe buffer. Real data
            // restores the full working set (lazily, via the pre-park sizing
            // above) and re-arms stage 1.
            //
            // c1M P1: when task parking is enabled and the session is
            // parkable, the stage-2 read is ALSO cancelable and registered
            // with the sweep under the longer `--conn-park-secs` threshold —
            // a cancel here means "exit the task", leaving only a tiny
            // readiness watcher (conn_accept::spawn_parked_idle_watcher).
            // The predicate is stable while parked in read (no commands can
            // execute), and reaching this arm already excludes subscriber /
            // tracking connections (each takes its own arm). Connections with
            // `timeout N` set DO reach here now (D1): the idle deadline is
            // enforced by the shard chore's registry sweep, which reaches a
            // task-parked connection just as CLIENT KILL does.
            let parkable = park.can_park
                && S::SUPPORTS_TASK_PARK
                && idle_park::park_after_ms() > 0
                // c10k D2: unparsed input must NOT block the park. Requiring an
                // EMPTY read_buf here let one byte (`*`) pin a connection into
                // the unregistered plain read below, out of the sweep's reach
                // forever. The remainder rides along in `read_buf_remainder`
                // and is re-parsed on resume; its size is already bounded by
                // `client_query_buffer_limit` upstream, so no second cap
                // belongs here (one would just be an escape hatch to size
                // past — see `park_policy`).
                && crate::server::conn::park_policy::remainder_allows_park(
                    read_buf.len(),
                    write_buf.len(),
                )
                && !conn.in_multi
                && conn.command_queue.is_empty()
                && conn.active_cross_txn.is_none()
                // F6 (#438, sec L2): unauthenticated conns never task-park.
                // On an auth-enabled server, parking a pre-AUTH conn would
                // let an attacker hold a maxclients-worth of silent sockets
                // at ~3.3 KB each, indefinitely and invisibly cheap. Keeping
                // them un-parked leaves each one pinned to a full handler
                // task — costly enough to surface in CPU/RSS monitoring —
                // and `timeout N` (the slowloris knob) still reaps them. On
                // no-auth servers `authenticated` is true from accept, so
                // this changes nothing there.
                && conn.authenticated
                // Replica-handshake conns (sent REPLCONF) never park: PSYNC
                // on a resumed parked conn is unsupported (warn+close).
                && !conn.saw_replconf
                // Stream-side veto LAST (it can do real work): TLS refuses
                // while its wrapper buffers / rustls session hold anything
                // the raw fd's readability can't signal.
                && stream.task_park_safe();
            if let (true, Some(reg)) = (parkable, idle_reg.as_ref()) {
                let handle = reg.slot.handle();
                reg.slot.mark_parked_stage2(ctx.cached_clock.ms());
                let (result, returned_buf) = stream.idle_park_read(tmp_buf, handle).await;
                reg.slot.mark_unparked();
                tmp_buf = returned_buf;
                match result {
                    Ok(0) => break,
                    Ok(n) => {
                        read_buf.extend_from_slice(&tmp_buf[..n]);
                        downshifted = false;
                    }
                    // Real socket/TLS error (EOF without close_notify,
                    // ECONNRESET, …): terminate. Parking instead would spin
                    // park→wake→park forever — the dead fd stays readable.
                    // #438 conn-secondary: `was_swept_cancel` also demands
                    // sweep provenance — a bare errno 125 that no sweep of
                    // ours produced is a real error too.
                    Err(ref e) if !reg.slot.was_swept_cancel(e) => break,
                    // F1 (#438): cancelled by the shutdown drain, not the
                    // stage-2 sweep — exit through the flush+FIN epilogue
                    // instead of task-parking into a watcher that would just
                    // be dropped.
                    Err(_) if shutdown.is_cancelled() => break,
                    Err(_) => {
                        // Cancelled by the stage-2 sweep: exit the task.
                        // read_buf holds at most MAX_PARKED_REMAINDER bytes
                        // (predicate) and `read_buf.split()` below carries
                        // them into the parked state, so a partial frame
                        // resumes exactly where it left off rather than being
                        // dropped or pinning the connection awake (D2).
                        let state = Box::new(MigratedConnectionState {
                            selected_db: conn.selected_db,
                            authenticated: conn.authenticated,
                            client_name: conn.client_name.clone(),
                            protocol_version: conn.protocol_version,
                            current_user: conn.current_user.clone(),
                            flags: 0,
                            read_buf_remainder: read_buf.split(),
                            client_id,
                            peer_addr: peer_addr.clone(),
                            workspace_id: conn.workspace_id,
                        });
                        return (
                            MonoioHandlerResult::ParkIdle {
                                state,
                                registry_guard,
                            },
                            Some(stream),
                        );
                    }
                }
            } else {
                let (result, returned_buf) = stream.read(tmp_buf).await;
                tmp_buf = returned_buf;
                match result {
                    Ok(0) => break,
                    Ok(n) => {
                        read_buf.extend_from_slice(&tmp_buf[..n]);
                        downshifted = false;
                    }
                    Err(_) => break,
                }
            }
        } else if let Some(reg) = idle_reg.as_ref() {
            // c10k W11 stage 1: full-size read, registered for the shard
            // chore's ≥1s idle sweep. Cancel-and-await is loss-free: a
            // completion that raced the cancel still delivers its bytes.
            let handle = reg.slot.handle();
            reg.slot.mark_parked(ctx.cached_clock.ms());
            let (result, returned_buf) = stream.idle_park_read(tmp_buf, handle).await;
            reg.slot.mark_unparked();
            tmp_buf = returned_buf;
            match result {
                Ok(0) => break,
                Ok(n) => {
                    read_buf.extend_from_slice(&tmp_buf[..n]);
                }
                // Real socket/TLS error: terminate now. (Pre-P1 this arm
                // could lump errors in with the cancel because stage 2
                // always performed a read that re-surfaced them; the
                // task-park path parks WITHOUT reading, so a mistaken
                // downshift here would feed the park→wake→park spin.)
                // #438 conn-secondary: provenance-checked — see stage 2.
                Err(ref e) if !reg.slot.was_swept_cancel(e) => break,
                // F1 (#438): cancelled by the shutdown drain, not the idle
                // sweep — exit through the flush+FIN epilogue instead of
                // re-parking a read nothing will ever complete.
                Err(_) if shutdown.is_cancelled() => break,
                Err(_) => {
                    // Cancelled by the idle sweep: shed the working set,
                    // re-park small.
                    idle_park::downshift_idle_buffers(&mut tmp_buf, &mut read_buf, &mut write_buf);
                    stream.on_idle_downshift();
                    downshifted = true;
                    continue;
                }
            }
        } else {
            let (result, returned_buf) = stream.read(tmp_buf).await;
            tmp_buf = returned_buf;
            match result {
                Ok(0) => break,
                Ok(n) => {
                    read_buf.extend_from_slice(&tmp_buf[..n]);
                }
                Err(_) => break,
            }
        }

        // D3: a rehydrated conn starts with a small (512 B) owned read buffer;
        // the moment a read saturates it (real traffic, not a keepalive-sized
        // probe), restore the full 8 KiB so bulk transfers aren't capped at
        // 512 B per syscall. Sited with the C2 check below: after every read
        // arm, once per iteration.
        if tmp_buf.len() < 8192 && read_buf.len() >= tmp_buf.len() {
            tmp_buf = vec![0u8; 8192];
        }

        // c10k C2: query-buffer ceiling. One check per read iteration, sited
        // after every read arm and ahead of both parse paths — an incomplete
        // frame is exactly what makes `read_buf` grow, and an incomplete
        // frame decodes to nothing, so the loop would otherwise come straight
        // back here and read more. `$536870911` plus a dribble of bytes pins
        // half a gigabyte per connection this way, invisible to
        // `used_memory`, and the auth gate runs after parsing — so it costs
        // no credentials. Unauthenticated connections get the much smaller
        // pre-auth ceiling; see `util::query_buf_limit`.
        {
            let (limit, preauth) = {
                let rt = ctx.runtime_config.read();
                (
                    rt.client_query_buffer_limit,
                    rt.client_query_buffer_limit_preauth,
                )
            };
            if super::util::query_buf_exceeded(read_buf.len(), conn.authenticated, limit, preauth) {
                let (_r, _b): (std::io::Result<usize>, bytes::Bytes) = stream
                    .write_all(bytes::Bytes::from_static(
                        super::util::QUERY_BUF_LIMIT_ERROR,
                    ))
                    .await;
                break;
            }
        }

        // Inline dispatch: GET/SET directly from raw bytes, skipping Frame construction.
        // Skip when unauthenticated or workspace-bound (prefix injection in normal path only).
        if conn.authenticated && conn.workspace_id.is_none() {
            // Inline writes safe only when: ACL unrestricted, !in_multi, !tracking,
            // !is_replica, no spill_sender. Replica check reads the lock-free
            // `is_replica_mirror` (kept in sync by `ReplicationState::set_role`)
            // instead of `repl_state.try_read()` — the RwLock CAS was a measured
            // per-op cost on ARM (see S3.5a note in dispatch.rs), and unlike
            // try_read the mirror stays accurate while the lock is held.
            let is_replica = ctx
                .is_replica_mirror
                .as_ref()
                .is_some_and(|m| m.load(std::sync::atomic::Ordering::Acquire));
            // `!tracking_active()`: inline writes bypass the dispatch-path
            // CLIENT TRACKING invalidation hook, so ANY tracking client in
            // the process (not just this conn) forces writes through the
            // normal path. One relaxed atomic load; free when tracking is off.
            //
            // task #34 (Wave A) fix: `try_inline_dispatch`'s SET fast path
            // (`server::conn::blocking`) does NOT feed the replication
            // backlog/fan-out at all — only `record_local_write`/
            // `record_local_write_db` (the generic dispatch path) do.
            // Before this gate, a master with `--disk-offload disable` and
            // an attached replica silently dropped every plain `SET` from
            // the replication stream (verified: a lone `SET foo bar` never
            // reached the replica). `!fanout_hint_active()` — the same
            // cheap, never-cleared-once-true Relaxed load
            // `handler_monoio::ft::replication_fanout_active` uses as its
            // first gate — forces plain SET onto the generic dispatch path
            // for the rest of the process once any replica has ever begun
            // attaching, matching the existing `ctx.spill_sender.is_none()`
            // precedent of "fall back to the full path when it must do more
            // than this fast path knows how to do". These write-only
            // conditions do not gate GET, which has its own `can_inline_reads`
            // gate below — reads must never be inlined for a restricted or
            // tracking connection, but a replica/spill/fanout master may still
            // serve them from the fast path.
            //
            // Hoisted: both gates need it, and it is one Acquire load plus a
            // compare (`cached_acl_unrestricted && acl_cache_fresh()`), so
            // computing it once keeps the read gate free relative to the
            // pre-fix code, which already paid for it on the write gate.
            let acl_unrestricted = conn.acl_skip_allowed();
            // Reads may be inlined only when this connection can provably skip
            // the ACL check AND is not itself a client-side-caching client:
            // the inline path calls neither the ACL gate nor `track_read_keys`.
            // A restricted user, or a `CLIENT TRACKING ON` connection, falls
            // back to generic dispatch where both run. Deliberately NOT gated
            // on the process-global `tracking_active()` — only THIS
            // connection's own reads populate its invalidation set, so one
            // tracking client must not push every other connection off the
            // fast path (writes still use the global gate, since a
            // non-tracking writer must invalidate everyone else).
            //
            // `!conn.in_multi` is shared with the write gate and is not
            // optional: inside an open transaction a command must be QUEUED,
            // and the inline path answers it instead. A client then receives
            // the value where it expects `+QUEUED`, and `EXEC` omits the read
            // entirely — `MULTI; GET k; EXEC` returned `*0` rather than
            // `*1[$1 v]`. `MGET`, not being inline-eligible, queued correctly
            // all along, which is what isolated the path.
            // See `tests/multi_queues_inline_get.rs`.
            // MONITOR: the inline path answers straight from the read buffer
            // and never sees `peer_addr`, so it cannot format a feed line. Rather
            // than thread the address through the hottest function in the
            // codebase, the fast path stands down while any monitor is attached
            // — the feed is then correct BY CONSTRUCTION on this path instead of
            // by a hook someone must remember to keep in sync. The cost is
            // throughput while monitoring, which is the moment an operator has
            // already accepted diagnostic overhead; when unattached this is one
            // Relaxed load that the branch below would take anyway.
            let monitored = crate::monitor::any_attached();
            let can_inline_reads =
                acl_unrestricted && !conn.in_multi && !conn.tracking_state.enabled && !monitored;
            let can_inline_writes = acl_unrestricted
                && !monitored
                && !conn.in_multi
                && !conn.tracking_state.enabled
                && !crate::tracking::tracking_active()
                && !is_replica
                && ctx.spill_sender.is_none()
                && !crate::replication::state::fanout_hint_active();
            let inlined = try_inline_dispatch_loop(
                &mut read_buf,
                &mut write_buf,
                &ctx.shard_databases,
                ctx.shard_id,
                conn.selected_db,
                &ctx.aof_pool,
                &ctx.repl_state,
                ctx.cached_clock.ms(),
                ctx.num_shards,
                can_inline_reads,
                can_inline_writes,
                // R6: cluster mode disables the inline fast path entirely —
                // GET/SET must reach try_handle_cluster_routing for MOVED/ASK.
                crate::cluster::cluster_enabled(),
                // moon#522: the inline path frames its own GET-miss null, so
                // it needs this connection's protocol version. Without it the
                // null spelling depended on which shard owned the key.
                conn.protocol_version >= 3,
                &ctx.runtime_config,
            );
            crate::admin::metrics_setup::record_dispatch_local_inline(inlined as u64);
            if inlined > 0 && read_buf.is_empty() {
                // All commands were inlined -- flush write_buf and continue
                if !write_buf.is_empty() {
                    let data = write_buf.split().freeze();
                    if !write_all_bounded!(
                        stream,
                        data,
                        write_timeout,
                        out_cap_normal,
                        client_live,
                        client_id
                    ) {
                        break;
                    }
                }
                continue;
            }
            // If read_buf still has data, fall through to normal Frame parsing
            // for remaining commands. Inlined responses are already in write_buf.
        }

        // Parse all complete frames from the read buffer (reuse pre-allocated Vec, cap at 1024)
        frames.clear();
        loop {
            match codec.decode_frame(&mut read_buf) {
                Ok(Some(frame)) => {
                    frames.push(frame);
                    if frames.len() >= 1024 {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    // A protocol fault kills the connection, but not before
                    // the frames already parsed from this same read are run
                    // and answered, and not before the client is told WHY.
                    // The old `return Done` here dropped both — so
                    // `PING\r\n<bad frame>` in one write answered nothing.
                    proto_fault = codec.take_last_fault();
                    break;
                }
            }
        }

        if frames.is_empty() {
            // Nothing valid preceded the fault: report it and close.
            if let Some(kind) = proto_fault.take() {
                let data = bytes::Bytes::from(super::util::proto_error_frame(kind));
                let (_wr, _b): (std::io::Result<usize>, bytes::Bytes) =
                    stream.write_all(data).await;
                return (MonoioHandlerResult::Done, None);
            }
            continue;
        }

        // CLIENT PAUSE: delay processing if server is paused
        crate::client_pause::expire_if_needed();
        if let Some(remaining) = crate::client_pause::check_pause(true) {
            monoio::time::sleep(remaining).await;
        }

        // Process frames (do NOT clear write_buf -- may have inline dispatch responses).
        let mut should_quit = false;
        responses.clear();
        remote_groups.clear();
        local_leg_write_idxs.clear();
        // The trailing bool marks a SHARDED publish. One batch map, split at flush:
        // the two namespaces share the fan-out plumbing but never the destination.
        let mut publish_batches: std::collections::HashMap<
            usize,
            Vec<(usize, Bytes, Bytes, bool)>,
        > = std::collections::HashMap::new();

        // Refresh time once per batch — sub-millisecond accuracy not needed per-command.
        crate::shard::slice::with_shard_db(conn.selected_db, |db| {
            db.refresh_now_from_cache(&ctx.cached_clock);
        });

        // Batch-level eviction gate: snapshot `maxmemory != 0` once per batch
        // (and cache the spill-sender presence check). When neither is set —
        // the common non-memory-bound benchmark path — the per-command write
        // branch can skip the `runtime_config.read()` lock acquire + the
        // `evict_to_budget` call entirely. Saves a small RwLock lock
        // pair per write command in a pipelined batch.
        //
        // Safety: `maxmemory` changes via `CONFIG SET maxmemory N` are picked
        // up on the NEXT batch. A batch spans sub-millisecond; operators do
        // not observe this granularity.
        //
        // WS5b fix-first review: this condition MUST also consult the per-db
        // quota atomic. `run_write_eviction_gate` (below) is the only caller
        // of `check_db_maxmemory_for_command` in this file, and it is gated
        // entirely behind `batch_eviction_active` — without this term, a
        // server started with `--maxmemory 0` and no disk-offload spill
        // sender (e.g. `--disk-offload disable`) never runs the db-quota
        // check for any non-inline write (HSET/LPUSH/SADD/ZADD/INCR/APPEND/
        // MSET/SET-with-options/RESTORE/...), even with `--db-maxmemory`
        // configured. Reproduced empirically: 60x1KB HSET into a 4KB-quota'd
        // db all succeeded while plain SET was correctly rejected (SET takes
        // the separate, correctly-gated inline fast path in blocking.rs).
        // Mirrors the Lua bridge's early-exit gate in scripting/bridge.rs,
        // which already included this term.
        let batch_eviction_active = ctx.spill_sender.is_some()
            || ctx.runtime_config.read().maxmemory != 0
            || crate::storage::db_quota::db_maxmemory_any_set();

        let mut auth_delay_ms: u64 = 0;

        // #438 batch-tail hardening: index-based iteration (not
        // `frames.drain(..)`) so early-flush arms (blocking / SUBSCRIBE /
        // PSYNC) can defer themselves plus the unconsumed tail to the next
        // batch iteration, and so subscribe/blocking breaks carry the parsed
        // tail forward instead of silently dropping it (Drain::drop discarded
        // the remainder on break).
        let num_frames = frames.len();
        let mut deferred_tail_from: Option<usize> = None;
        let mut frame_idx = 0usize;
        while frame_idx < num_frames {
            let frame = std::mem::replace(&mut frames[frame_idx], Frame::Null);
            frame_idx += 1;
            // --- AUTH gate ---
            match dispatch::check_auth_gate(
                &frame,
                &mut conn,
                ctx,
                &peer_addr,
                client_id,
                &mut responses,
                &mut auth_delay_ms,
                &mut codec,
            ) {
                dispatch::AuthGateResult::Consumed => continue,
                dispatch::AuthGateResult::Quit => {
                    should_quit = true;
                    break;
                }
                dispatch::AuthGateResult::NotAuth => {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"NOAUTH Authentication required.",
                    )));
                    continue;
                }
                dispatch::AuthGateResult::Authenticated => {}
            }

            let (cmd, cmd_args) = match extract_command(&frame) {
                Some(pair) => pair,
                None => {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR invalid command format",
                    )));
                    continue;
                }
            };

            // Every intercept below answers through `shaped!()`, never through
            // `responses` directly: an intercept short-circuits the dispatch exit
            // where the RESP2->RESP3 shape policy is applied, so the sink applies it
            // on push instead of each intercept having to remember. See
            // `crate::server::conn::intercept` (moon#462).
            //
            // Read once per command rather than per push: `HELLO 3` changes it
            // mid-batch, and a reply must be shaped for the protocol the command
            // ARRIVED under, which is what the encoder's protocol-switch record
            // assumes too.
            let intercept_proto = conn.protocol_version;
            macro_rules! shaped {
                () => {
                    &mut crate::server::conn::intercept::InterceptReplies::new(
                        &mut responses,
                        cmd,
                        cmd_args,
                        intercept_proto,
                    )
                };
            }
            // --- QUIT ---
            if cmd.eq_ignore_ascii_case(b"QUIT") {
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                should_quit = true;
                break;
            }

            // --- ASKING ---
            if cmd.eq_ignore_ascii_case(b"ASKING") {
                conn.asking = true;
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                continue;
            }

            // --- READONLY / READWRITE ---
            //
            // Both are cluster-only: a standalone instance answers the
            // measured refusal rather than a misleading +OK, because a
            // client that gets +OK believes replica reads are enabled.
            if cmd.eq_ignore_ascii_case(b"READONLY") || cmd.eq_ignore_ascii_case(b"READWRITE") {
                if let Some(err) = crate::cluster::readonly_verb_reply(cmd, cmd_args) {
                    responses.push(err);
                    continue;
                }
                conn.readonly = cmd.eq_ignore_ascii_case(b"READONLY");
                responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
                continue;
            }
            // #438 batch-tail crash class: an early-flush command (blocking /
            // SUBSCRIBE / PSUBSCRIBE / PSYNC) while remote-slotted commands
            // are pending would flush their Frame::Null placeholders to the
            // client (wrong replies) and clear `responses`, leaving phase 2's
            // drain to index into an empty vec — shard-thread panic, whole
            // process aborts. Defer this command and the unconsumed tail to
            // the next batch iteration; phase 2 resolves the pending replies
            // and the epilogue flushes them first. The two `is_empty()`
            // loads keep the common local-only batch at zero name compares.
            if (!remote_groups.is_empty() || !publish_batches.is_empty())
                && (crate::server::conn::blocking::is_blocking_command_args(cmd, cmd_args)
                    || cmd.eq_ignore_ascii_case(b"SUBSCRIBE")
                    || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE")
                    || cmd.eq_ignore_ascii_case(b"PSYNC"))
            {
                frames[frame_idx - 1] = frame;
                deferred_tail_from = Some(frame_idx - 1);
                break;
            }

            // #507 pipeline ordering: a command that does NOT route by its own
            // single key executes inline, against shards whose earlier writes
            // in this same batch are still sitting in `remote_groups`
            // undispatched. Reading there returns state the client already
            // wrote; writing there is overwritten when the pending write lands.
            // Defer it and the unconsumed tail exactly as #438 does above —
            // phase 2 resolves the pending replies first, and the tail
            // re-parses at the top of the next batch with `remote_groups`
            // empty, so this cannot loop.
            if !remote_groups.is_empty()
                && crate::server::conn::shared::must_wait_for_pending_remote(cmd, cmd_args)
            {
                frames[frame_idx - 1] = frame;
                deferred_tail_from = Some(frame_idx - 1);
                break;
            }
            // --- Connection-level commands (dispatched to dispatch.rs) ---
            //
            // Length-gated dispatch: each `try_handle_*` starts with a
            // `cmd.eq_ignore_ascii_case(b"NAME")` early-return whose body is
            // too large for rustc to inline. By pre-checking `cmd_len` at the
            // call site we avoid the function call + prologue for the common
            // case where a pipelined SET/GET length does not match any of the
            // connection-level command names. Cut per-command dispatch cost
            // from ~14 non-matching function calls to ~1 on SET/GET workloads.
            let cmd_len = cmd.len();
            // MONITOR feed for the two ACL-EXEMPT commands below.
            //
            // AUTH and HELLO carry Redis's NO_AUTH flag and are therefore
            // intercepted ABOVE the ACL gate — which puts them above the main
            // feed hook further down, so they would never be fed at all. They
            // are also the only two commands carrying credentials, i.e. exactly
            // the ones whose absence from the feed is least acceptable and
            // whose arguments must be redacted. Fed here, once; the intercepts
            // `continue`, so they cannot reach the main hook and double-feed.
            if cmd.eq_ignore_ascii_case(b"AUTH") || cmd.eq_ignore_ascii_case(b"HELLO") {
                crate::monitor::feed_frames(conn.selected_db, &peer_addr, cmd, cmd_args);
            }

            // === ACL-EXEMPT COMMANDS ===
            //
            // AUTH and HELLO carry Redis's `NO_AUTH` flag and are permitted
            // regardless of the current user's permissions - a restricted user
            // must always be able to re-authenticate as somebody else, and
            // gating HELLO would break the RESP3 handshake. `check_auth_gate`
            // above only handles them while UNauthenticated, so these are the
            // post-authentication path and must stay ahead of the gate.
            if cmd_len == 4
                && dispatch::try_handle_auth(
                    cmd,
                    cmd_args,
                    &mut conn,
                    ctx,
                    &peer_addr,
                    &mut auth_delay_ms,
                    shaped!(),
                )
            {
                continue;
            }
            if cmd_len == 5
                && dispatch::try_handle_hello(
                    cmd,
                    cmd_args,
                    &mut conn,
                    ctx,
                    client_id,
                    &peer_addr,
                    &mut auth_delay_ms,
                    shaped!(),
                    &mut codec,
                )
            {
                continue;
            }
            // RESET sits ABOVE the ACL gate deliberately: the registry marks it
            // NO_AUTH|LOADING|STALE, and returning a connection to its default
            // (unauthenticated) state is exactly what a client does when it has
            // lost track of that state. It is also above the MULTI queueing
            // step below — measured on redis-server 8.6.1, RESET inside MULTI
            // executes immediately and discards the transaction.
            if cmd_len == 5
                && crate::server::conn::shared::try_handle_reset(
                    cmd,
                    cmd_args,
                    client_id,
                    &mut conn,
                    &ctx.requirepass,
                    &ctx.tracking_table,
                    &*ctx.pubsub_registry,
                    &mut responses,
                    Some(&mut codec),
                )
            {
                continue;
            }

            // === ACL GATE - every privileged intercept MUST sit below this ===
            //
            // c10k hardening B1. This gate used to sit ~200 lines further
            // down, below the script/ACL/CONFIG/REPLICAOF/BGSAVE/SHUTDOWN
            // intercepts, while the comment attached to it claimed the
            // opposite invariant. Because those intercepts `continue` on a
            // match, they returned before any permission check ever ran: a
            // user holding nothing but `~app:* +get` was correctly refused a
            // plain SET yet could still run `ACL SETUSER evil on nopass ~*
            // +@all`, `CONFIG SET`, arbitrary Lua, `REPLICAOF` and `BGSAVE`
            // - verified end-to-end, see tests/acl_privileged_intercepts.rs.
            // Authenticated-restricted-user escalation to full admin.
            //
            // The auth gate runs earlier still, so unauthenticated clients
            // get NOAUTH rather than NOPERM; only AUTH/HELLO above are exempt
            // (Redis marks both NO_AUTH).
            //
            // If you add a privileged intercept, add it BELOW this line.
            if dispatch::try_enforce_acl(cmd, cmd_args, &mut conn, ctx, &peer_addr, shaped!()) {
                continue;
            }

            // === MULTI QUEUE GATE — every non-transactional intercept MUST
            // sit below this ===
            //
            // Redis executes exactly six commands while a transaction is open
            // (MULTI, EXEC, DISCARD, WATCH, RESET, QUIT); EVERYTHING else
            // queues. Moon decided this hundreds of lines further down, below
            // the INFO / CLIENT / WS / MQ / PUBLISH / SUBSCRIBE intercepts, so
            // each of those executed for real inside a transaction. Measured
            // against redis-server 8.6.1: `SUBSCRIBE ch` put the connection
            // into subscriber mode mid-MULTI, and `INFO server` returned a
            // 3 KB dump where Redis returns `+QUEUED`.
            //
            // Deliberately BELOW the ACL gate: Redis refuses a forbidden
            // command at queue time, and this repo has already shipped one ACL
            // bypass that came from an intercept sitting above its check.
            //
            // This handler is the DEFAULT runtime (`runtime-monoio`). The
            // sharded/tokio handlers carry the identical gate — a fix in only
            // one of them is invisible to whichever job builds the other.
            if conn.in_multi
                && !is_transaction_control(cmd)
                && !crate::server::conn::shared::is_intercept_only(cmd)
            {
                // FT.* isn't wired through the txn execution path; reject it
                // explicitly rather than failing incidentally later.
                if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR FT.* commands are not supported inside MULTI/EXEC",
                    )));
                    continue;
                }
                // Queue-time validation (Redis CLIENT_DIRTY_EXEC): a command
                // that could never run poisons the whole transaction HERE, so
                // EXEC refuses everything rather than applying the valid half.
                if let Some(err) = crate::server::conn::shared::queue_time_rejection(cmd, cmd_args)
                {
                    conn.multi_dirty = true;
                    responses.push(err);
                    continue;
                }
                // Blocking commands must not block at EXEC. Most queue as
                // their non-blocking twin; the four whose twin answers a
                // different SHAPE queue unchanged and run in immediate-only
                // mode at EXEC instead (moon#524). `queued_blocking_frame`
                // owns that split.
                if crate::server::conn::blocking::is_blocking_command(cmd) {
                    conn.command_queue
                        .push(queued_blocking_frame(cmd, cmd_args));
                } else {
                    conn.command_queue.push(frame);
                }
                responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                continue;
            }

            // --- MONITOR: attach, and the rules that apply once attached ---
            //
            // BELOW the ACL gate and BELOW the MULTI queue gate, deliberately.
            // Above the ACL gate it would exempt itself from ACL, which is the
            // exact shape of the v0.8.6 P0 — and MONITOR is the last command
            // that should be reachable without permission, since it reads every
            // other user's arguments. Below the MULTI gate, a queued command
            // `continue`s before reaching the feed, which is how M9 (queued
            // commands are fed at EXEC, not at queue time) falls out for free
            // rather than needing its own special case.
            if cmd.eq_ignore_ascii_case(b"MONITOR") {
                // Already attached -> None: Redis answers NOTHING. Measured —
                // it is not an error, it is silence.
                if let Some(reply) = crate::server::conn::monitor_mode::handle_monitor(
                    cmd_args.len(),
                    client_id,
                    &mut conn.monitor_attached,
                    &mut conn.monitor_rx,
                ) {
                    responses.push(reply);
                }
                continue;
            }
            if conn.monitor_attached
                && let Some(refusal) = crate::server::conn::monitor_mode::refuse_if_keyspace(
                    cmd,
                    cmd_args
                        .first()
                        .and_then(crate::command::helpers::extract_bytes)
                        .map(|b| b.as_ref()),
                )
            {
                responses.push(refusal);
                continue;
            }

            // --- MONITOR feed ---
            //
            // Before dispatch, so a BLOCKING command appears when it is issued
            // rather than when it unblocks — that ordering is the whole reason
            // an operator watches the feed. Costs one Relaxed atomic load when
            // no monitor is attached; everything else (the hidden-set check,
            // the registry/arity check, formatting, redaction, fan-out) lives
            // behind that load in a #[cold] path.
            //
            // EXEC first replays its queue: Redis feeds a queued command when
            // it EXECUTES, so the body lines appear at EXEC time, in order,
            // followed by the EXEC line itself (measured 5µs apart). A dirty
            // transaction is refused wholesale and never runs, so it feeds
            // nothing.
            if cmd.eq_ignore_ascii_case(b"EXEC") && !conn.multi_dirty {
                for queued in &conn.command_queue {
                    if let Some((qcmd, qargs)) = extract_command(queued) {
                        crate::monitor::feed_frames(conn.selected_db, &peer_addr, qcmd, qargs);
                    }
                }
            }
            crate::monitor::feed_frames(conn.selected_db, &peer_addr, cmd, cmd_args);

            if cmd_len == 7 && dispatch::try_handle_cluster(cmd, cmd_args, ctx, shaped!()) {
                continue;
            }
            if cmd_len == 7
                && dispatch::try_handle_evalsha(cmd, cmd_args, &conn, ctx, shaped!()).await
            {
                continue;
            }
            if cmd_len == 4
                && dispatch::try_handle_eval(cmd, cmd_args, &conn, ctx, &shutdown, shaped!()).await
            {
                continue;
            }
            if cmd_len == 6
                && dispatch::try_handle_script(cmd, cmd_args, ctx, &shutdown, shaped!()).await
            {
                continue;
            }
            if dispatch::try_handle_cluster_routing(cmd, cmd_args, &mut conn, ctx, shaped!()) {
                continue;
            }
            if cmd_len == 3
                && dispatch::try_handle_acl(cmd, cmd_args, &mut conn, ctx, &peer_addr, shaped!())
            {
                continue;
            }
            if cmd_len == 6 && dispatch::try_handle_config(cmd, cmd_args, ctx, shaped!()) {
                continue;
            }
            // REPLICAOF (9) or SLAVEOF (7)
            if (cmd_len == 9 || cmd_len == 7)
                && dispatch::try_handle_replicaof(cmd, cmd_args, ctx, shaped!())
            {
                continue;
            }
            if cmd_len == 8 && dispatch::try_handle_replconf(cmd, cmd_args, ctx, shaped!()) {
                // Likely a replica mid-handshake (PSYNC next): permanently
                // exclude from task-parking — the resumed-parked path does
                // not support the PSYNC hijack.
                conn.saw_replconf = true;
                continue;
            }
            // PSYNC: arrives only on a master, hijacks the connection. Encode
            // any pending responses, flush, then return the stream so the
            // caller can drive the resync handshake.
            if cmd_len == 5 {
                if let Some((repl_id, offset)) =
                    dispatch::try_handle_psync(cmd, cmd_args, ctx, shaped!())
                {
                    // Earlier frames in this batch may hold barrier-pending
                    // local-leg writes — confirm them before this early flush.
                    crate::server::conn::shared::resolve_local_leg_barrier(
                        &ctx.aof_pool,
                        ctx.shard_id,
                        &mut local_leg_write_idxs,
                        &mut responses,
                    )
                    .await;
                    crate::server::conn::shared::encode_response_batch(
                        &mut conn,
                        &responses,
                        &mut write_buf,
                    );
                    if !write_buf.is_empty() {
                        let data = write_buf.split().freeze();
                        if !write_all_bounded!(
                            stream,
                            data,
                            write_timeout,
                            out_cap_normal,
                            client_live,
                            client_id
                        ) {
                            return (MonoioHandlerResult::Done, None);
                        }
                    }
                    return (
                        MonoioHandlerResult::HijackForPsync {
                            client_repl_id: repl_id,
                            client_offset: offset,
                            peer_addr: peer_addr.clone(),
                        },
                        Some(stream),
                    );
                }
                // try_handle_psync may have pushed an error response (multi-shard,
                // bad args, etc.); fall through so it gets flushed normally.
                if !responses.is_empty() && cmd.eq_ignore_ascii_case(b"PSYNC") {
                    continue;
                }
            }
            if cmd_len == 4 && dispatch::try_handle_info(cmd, cmd_args, &conn, ctx, shaped!()).await
            {
                continue;
            }
            // WAIT blocks on replica ACKs (R1) — must run at the connection
            // layer; generic dispatch is synchronous and used to answer :0.
            if cmd_len == 4 && dispatch::try_handle_wait(cmd, cmd_args, ctx, shaped!()).await {
                continue;
            }
            if dispatch::try_enforce_readonly(cmd, cmd_args, ctx, shaped!()) {
                continue;
            }
            // MA12: Disk full enforcement
            if dispatch::try_enforce_disk_full(cmd, shaped!()) {
                continue;
            }
            // CLIENT early (ID, SETNAME, GETNAME, TRACKING) -- admin subcmds fall through to ACL gate
            if cmd_len == 6
                && dispatch::try_handle_client_early(cmd, cmd_args, client_id, &mut conn, shaped!())
            {
                continue;
            }
            // --- Pub/sub commands ---
            // C2 fix: inside MULTI, PUBLISH must fall through to the queue
            // (it used to execute immediately, fanning out before the
            // transaction's writes were applied).
            if !conn.in_multi
                && pubsub::try_handle_publish(
                    cmd,
                    cmd_args,
                    &conn,
                    ctx,
                    &mut responses,
                    &mut publish_batches,
                )
            {
                continue;
            }
            match pubsub::try_handle_subscribe_entry(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                &peer_addr,
                &mut responses,
                &mut local_leg_write_idxs,
                &mut codec,
                &mut write_buf,
                &mut stream,
            )
            .await
            {
                pubsub::SubscribeResult::NotSubscribe => {}
                pubsub::SubscribeResult::ArgError => continue,
                pubsub::SubscribeResult::Subscribed => {
                    // RESP3 never diverts into subscriber mode, so there is
                    // nothing to defer: the rest of the batch keeps running in
                    // THIS loop, which is the point. Breaking here would jump
                    // to a subscriber loop that the RESP3 gate above no longer
                    // enters, stranding the connection.
                    if conn.protocol_version >= 3 {
                        continue;
                    }
                    // #438: carry the parsed-but-unconsumed batch tail into
                    // the next iteration (subscriber mode) instead of
                    // dropping it — `[SUBSCRIBE ch, PING]` in one pipelined
                    // write used to swallow the PING.
                    if frame_idx < num_frames {
                        deferred_tail_from = Some(frame_idx);
                    }
                    break;
                }
                pubsub::SubscribeResult::WriteError => return (MonoioHandlerResult::Done, None),
            }
            if pubsub::try_handle_unsubscribe(cmd, &mut responses) {
                continue;
            }
            if pubsub::try_handle_pubsub_introspection(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            // --- Persistence + ACL gate + CLIENT admin + Functions ---
            if dispatch::try_handle_persistence(cmd, ctx, shaped!()) {
                continue;
            }
            // --- SHUTDOWN [NOSAVE|SAVE] ---
            match dispatch::try_handle_shutdown(cmd, cmd_args, ctx, &shutdown, shaped!()).await {
                dispatch::ShutdownOutcome::NotShutdown => {}
                dispatch::ShutdownOutcome::Rejected => {
                    continue;
                }
                dispatch::ShutdownOutcome::Exiting => {
                    should_quit = true;
                    break;
                }
            }
            // (B1) The ACL gate that used to sit here now runs far above, ahead
            // of every privileged intercept. Its old comment claimed exactly
            // the invariant the code above it violated.
            // --- SWAPDB: handler-layer intercept (needs async + multi-db access) ---
            if dispatch::try_handle_swapdb(cmd, cmd_args, &mut conn, ctx, shaped!()).await {
                continue;
            }
            if dispatch::try_handle_client_admin(cmd, cmd_args, client_id, &conn, shaped!()) {
                continue;
            }
            // CLIENT TRACKING mutates server-side invalidation state — post-ACL
            // (H-3), matching handler_sharded's placement of all CLIENT subcmds.
            if cmd_len == 6
                && dispatch::try_handle_client_tracking(
                    cmd,
                    cmd_args,
                    client_id,
                    &mut conn,
                    ctx,
                    shaped!(),
                )
            {
                continue;
            }
            // CDC.READ (8) — stateless WAL reader, no shard state involved.
            // Post-ACL (H-3): it reads arbitrary WAL directories off disk, so
            // it must be deniable (-@dangerous / allow-list users).
            if cmd_len == 8 && dispatch::try_handle_cdc_read(cmd, cmd_args, shaped!()) {
                continue;
            }
            if dispatch::try_handle_functions(
                cmd,
                cmd_args,
                &conn,
                ctx,
                &func_registry,
                &shutdown,
                shaped!(),
            )
            .await
            {
                continue;
            }

            // --- TXN.BEGIN / TXN.COMMIT / TXN.ABORT ---
            if txn::try_handle_txn_begin(cmd, cmd_args, &mut conn, ctx, &mut responses) {
                continue;
            }
            if txn::try_handle_txn_commit(cmd, cmd_args, &mut conn, ctx, &mut responses).await {
                continue;
            }
            if txn::try_handle_txn_abort(cmd, cmd_args, &mut conn, ctx, &mut responses).await {
                continue;
            }

            // --- TEMPORAL.SNAPSHOT_AT / TEMPORAL.INVALIDATE ---
            if txn::try_handle_temporal_snapshot_at(cmd, cmd_args, ctx, &mut responses) {
                continue;
            }
            if txn::try_handle_temporal_invalidate(cmd, cmd_args, &frame, ctx, &mut responses).await
            {
                continue;
            }

            // --- WS.* ---
            if write::try_handle_ws_command(cmd, cmd_args, &mut conn, ctx, &mut responses).await {
                continue;
            }

            // --- MQ.* ---
            if write::try_handle_mq_command(cmd, cmd_args, &frame, &mut conn, ctx, &mut responses)
                .await
            {
                continue;
            }

            // --- MULTI / EXEC / DISCARD ---
            let mut exec_publishes: Vec<(usize, Bytes, Bytes)> = Vec::new();
            if write::try_handle_multi_exec(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                &mut responses,
                &mut exec_publishes,
            )
            .await
            {
                // C2: PUBLISH queued inside MULTI fans out only now — after the
                // transaction body has been applied — and its placeholder in the
                // EXEC reply array is patched with the real receiver count.
                if !exec_publishes.is_empty() {
                    let exec_idx = responses.len() - 1;
                    for (inner, ch, msg) in exec_publishes.drain(..) {
                        // Channel ACL gates the txn PUBLISH path (C2 security):
                        // a denied channel is patched with NOPERM, never sent.
                        let patched = match crate::server::conn::shared::publish_channel_acl_deny(
                            &ctx.acl_table,
                            &conn.current_user,
                            &ch,
                        ) {
                            Some(err) => err,
                            None => Frame::Integer(
                                crate::server::conn::shared::publish_post_txn(
                                    ctx, &shutdown, &ch, &msg,
                                )
                                .await,
                            ),
                        };
                        if let Frame::Array(items) = &mut responses[exec_idx] {
                            if inner < items.len() {
                                items[inner] = patched;
                            }
                        }
                    }
                }
                continue;
            }

            // --- Workspace key prefix injection ---
            // MUST happen before key_to_shard() so the {ws_id} hash tag determines
            // shard routing. This is the ONLY code path where workspace prefixing
            // occurs (WS-07, WS-12). All subsequent dispatch uses cmd_args (shadowed).
            let rewritten = conn
                .workspace_id
                .as_ref()
                .map(|ws_id| workspace_rewrite_args(cmd, cmd_args, ws_id));
            let cmd_args: &[Frame] = rewritten.as_deref().unwrap_or(cmd_args);

            // --- BLOCKING COMMANDS ---
            match dispatch::try_handle_blocking(
                cmd,
                cmd_args,
                &mut conn,
                ctx,
                &mut responses,
                &mut local_leg_write_idxs,
                &mut codec,
                &mut write_buf,
                &mut read_buf,
                &mut stream,
                &shutdown,
                &client_live,
            )
            .await
            {
                dispatch::BlockingResult::NotBlocking => {}
                dispatch::BlockingResult::Queued => continue,
                dispatch::BlockingResult::Handled => {
                    // A1: anything left in read_buf is either this batch's
                    // unparsed tail or bytes the peer watch carried; parse
                    // before the next read either way.
                    carried_input = !read_buf.is_empty();
                    // #438: parsed-but-unconsumed frames after the blocking
                    // command used to be dropped by the drain-on-break; carry
                    // them into the next iteration.
                    if frame_idx < num_frames {
                        deferred_tail_from = Some(frame_idx);
                    }
                    break;
                }
                dispatch::BlockingResult::WriteError => return (MonoioHandlerResult::Done, None),
                // c10k A1: peer vanished mid-block. Nothing to write; the
                // registry entry and maxclients slot are released by returning.
                dispatch::BlockingResult::PeerGone => return (MonoioHandlerResult::Done, None),
            }

            // --- MULTI queue mode: queue commands when in transaction ---
            if conn.in_multi {
                // FT.* vector commands aren't wired through the txn execution
                // path; reject them explicitly inside MULTI (matches
                // handler_single) instead of an incidental later error.
                if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
                    responses.push(Frame::Error(Bytes::from_static(
                        b"ERR FT.* commands are not supported inside MULTI/EXEC",
                    )));
                    continue;
                }
                conn.command_queue.push(frame);
                responses.push(Frame::SimpleString(Bytes::from_static(b"QUEUED")));
                continue;
            }

            // --- Cross-shard aggregation commands: KEYS, SCAN, DBSIZE + multi-key ---
            if dispatch::try_handle_cross_shard_commands(
                cmd,
                cmd_args,
                &conn,
                ctx,
                shaped!(),
                &mut local_leg_write_idxs,
            )
            .await
            {
                continue;
            }

            // --- FT.* vector search commands ---
            if ft::try_handle_ft_command(cmd, cmd_args, &frame, &conn, ctx, &mut responses).await {
                continue;
            }

            // --- GRAPH.* graph commands ---
            #[cfg(feature = "graph")]
            if write::try_handle_graph_command(
                cmd,
                cmd_args,
                &frame,
                &mut conn,
                ctx,
                &mut responses,
            )
            .await
            {
                continue;
            }

            // --- MA2: KILL SNAPSHOT <txn_id> ---
            if cmd.eq_ignore_ascii_case(b"KILL") {
                let response = crate::shard::slice::with_shard(|s| {
                    crate::command::server_admin::kill_snapshot(&mut s.vector_store, cmd_args)
                });
                responses.push(response);
                continue;
            }

            // --- P8: VACUUM — manual reclamation (MVCC passes only; manifest/WAL
            //     not accessible from connection handler, returns 0 for those counts).
            //
            // B1 fix: `VACUUM VECTOR <idx> [WEIGHT N]` and `VACUUM GRAPH <name>`
            // need the dedicated vacuum_vector / vacuum_graph entry points
            // (the parent `vacuum()` still returns the v0.1.14 stub for these
            // arms). Intercept the subcommand here before falling through.
            if cmd.eq_ignore_ascii_case(b"VACUUM") {
                if let Some(sub_frame) = cmd_args.first() {
                    if let Some(sub) = crate::command::helpers::extract_bytes(sub_frame) {
                        if sub.eq_ignore_ascii_case(b"VECTOR") {
                            let response = crate::shard::slice::with_shard(|s| {
                                crate::command::server_admin::vacuum_vector(
                                    &mut s.vector_store,
                                    &cmd_args[1..],
                                    conn.selected_db as u8,
                                )
                            });
                            responses.push(response);
                            continue;
                        }
                        #[cfg(feature = "graph")]
                        if sub.eq_ignore_ascii_case(b"GRAPH") {
                            let graph_merge_max = ctx.config.graph_merge_max_segments;
                            let graph_dead = ctx.config.graph_dead_edge_trigger;
                            let response = crate::shard::slice::with_shard(|s| {
                                crate::command::server_admin::vacuum_graph(
                                    &mut s.graph_store,
                                    &cmd_args[1..],
                                    graph_merge_max,
                                    graph_dead,
                                )
                            });
                            responses.push(response);
                            continue;
                        }
                    }
                }
                // Unconditional slice path for generic VACUUM.
                let response = crate::shard::slice::with_shard(|s| {
                    crate::command::server_admin::vacuum(
                        &mut s.vector_store,
                        None, // manifest — not available in connection handler
                        None, // wal_v3 — not available in connection handler
                        cmd_args,
                        crate::command::server_admin::DEFAULT_VACUUM_PRUNE_MARGIN,
                        None, // disk_offload_dir — dead: wal is None on this path too
                        0,    // shard_id — dead, see above
                    )
                });
                responses.push(response);
                continue;
            }

            // --- P8: DEBUG RECLAMATION ---
            if cmd.eq_ignore_ascii_case(b"DEBUG") {
                if let Some(sub) = cmd_args.first() {
                    if let Some(s) = crate::command::helpers::extract_bytes(sub) {
                        if s.eq_ignore_ascii_case(b"RECLAMATION") {
                            let response = crate::shard::slice::with_shard(|s| {
                                crate::command::server_admin::debug_reclamation(
                                    &s.vector_store,
                                    None,
                                    None,
                                )
                            });
                            responses.push(response);
                            continue;
                        }
                    }
                }
                // Other DEBUG subcommands fall through.
            }

            // moon#592: a write whose argv names a key on a DIFFERENT shard
            // than the one routing picked is refused BEFORE routing. Routing
            // sends it to `first_key`'s owner, which then executes the whole
            // command against that one slice — so the other key is read from,
            // and written to, the wrong shard's table: acked, invisible, gone.
            if let Some(err) = crate::server::conn::shared::cross_shard_multikey_rejection(
                cmd,
                cmd_args,
                ctx.num_shards,
            ) {
                responses.push(err);
                continue;
            }

            // moon#570: a list MOVE whose destination is owned by another
            // shard is refused BEFORE routing. Routing sends it to the
            // SOURCE's owner, which then executes both halves locally and
            // writes the destination into the wrong shard's slice — acked,
            // invisible, gone.
            if let Some(err) = crate::server::conn::shared::cross_shard_move_rejection(
                cmd,
                cmd_args,
                ctx.num_shards,
            ) {
                responses.push(err);
                continue;
            }

            // --- Routing: keyless, local, or remote ---
            let target_shard =
                extract_primary_key(cmd, cmd_args).map(|key| key_to_shard(key, ctx.num_shards));

            let is_local = match target_shard {
                None => true,
                Some(s) if s == ctx.shard_id => true,
                _ => false,
            };

            // Affinity sampling: record shard target for migration decision.
            // Migration is deferred until AFTER the current batch is fully processed.
            if let (Some(tracker), Some(target)) = (&mut conn.affinity_tracker, target_shard) {
                if let Some(migrate_to) = tracker.record(target) {
                    // IP-level hint: future connections from the same IP skip the
                    // 16-sample warm-up and land directly on the data shard.
                    if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
                        ctx.pubsub_affinity
                            .write()
                            .register_key(addr.ip(), migrate_to);
                    }
                    // Migration preconditions (D4 #438): shared gate —
                    // MULTI / cross-txn / subs / tracking / replica state
                    // doesn't transfer. Re-checked at the batch-end
                    // execution point, which is authoritative.
                    if conn.migration_eligible() {
                        conn.migration_target = Some(migrate_to);
                    }
                }
            }

            // Pre-classify write commands for AOF + tracking + replication
            // fan-out (the fanout hint is one Relaxed load, false until the
            // first replica ever begins attaching).
            let is_write = if ctx.aof_pool.is_some()
                || conn.tracking_state.enabled
                || crate::replication::state::fanout_hint_active()
            {
                // `is_persisted_write`: SELECT is W-flagged but conn-state
                // only — persisting the literal client SELECT poisons the
                // stream db context for interleaved connections (task #35).
                metadata::is_persisted_write(cmd)
            } else {
                false
            };

            if is_local {
                crate::admin::metrics_setup::record_dispatch_local();

                // T2.2 MOVE / T2.3 COPY ... DB n — intercept before write-path
                // (needs two dbs). Direct name checks below subsume the outer
                // metadata::is_write() gate — both names are write commands and
                // hot-path SETs/GETs would pay a redundant PHF lookup if we kept
                // the wrapper. Branch predictor learns "false" for both checks
                // under typical workloads.
                if cmd.eq_ignore_ascii_case(b"MOVE") {
                    // TXN guard: MOVE mutates two DBs and bypasses undo/intents.
                    // Reject during an active cross-store TXN so TXN.ABORT can
                    // still roll back cleanly (matches handler_sharded policy).
                    if conn.in_cross_txn() {
                        // #499: poison the txn so COMMIT cannot report OK.
                        conn.mark_cross_txn_rejected(cmd);
                        responses.push(Frame::Error(bytes::Bytes::from_static(
                            crate::command::transaction::ERR_TXN_CROSS_SHARD,
                        )));
                        continue;
                    }
                    use crate::command::keyspace::move_cmd as ksmv;
                    let src_db = conn.selected_db;
                    let db_count = ctx.shard_databases.db_count();
                    let response = match ksmv::parse_move_args(cmd_args, db_count) {
                        Err(e) => e,
                        Ok((_key, dst_db)) if dst_db == src_db => Frame::Integer(0),
                        Ok((key, dst_db)) => crate::shard::slice::with_shard(|s| {
                            ksmv::with_two_slice_dbs(
                                &mut s.databases,
                                src_db,
                                dst_db,
                                |src, dst| ksmv::move_core(src, dst, &key),
                            )
                        }),
                    };
                    // AOF only on actual success (:1). Matches handler_single.
                    // H1 fix: durable path under `appendfsync=always`
                    // awaits the writer's fsync ack before responding to
                    // the client.
                    if matches!(response, Frame::Integer(1)) {
                        // v0.7 local-leg live replication — same contract as
                        // the main write leg: record (backlog+offset, sync)
                        // before any await, AOF leg does not double-advance
                        // the offset (lsn = 0).
                        let repl_active = ft::replication_fanout_active(ctx);
                        if repl_active || ctx.aof_pool.is_some() {
                            let serialized = aof::serialize_command_for_log(&frame);
                            let lsn = if repl_active {
                                ft::record_local_write_db(
                                    ctx,
                                    conn.selected_db,
                                    serialized.clone(),
                                );
                                0
                            } else {
                                aof::AofWriterPool::issue_append_lsn(
                                    &ctx.repl_state,
                                    ctx.shard_id,
                                    serialized.len(),
                                )
                            };
                            let Some(ref pool) = ctx.aof_pool else {
                                responses.push(response);
                                continue;
                            };
                            match pool
                                .send_append_group(ctx.shard_id, lsn, conn.selected_db, serialized)
                                .await
                            {
                                // Always: durability confirmed by ONE
                                // fsync_barrier per batch (resolve_local_leg_barrier
                                // before serialization) instead of an awaited
                                // fsync per pipelined command.
                                Ok(true) => local_leg_write_idxs.push(responses.len()),
                                Ok(false) => {}
                                Err(_) => {
                                    responses.push(Frame::Error(bytes::Bytes::from_static(
                                        aof::AOF_FSYNC_ERR,
                                    )));
                                    continue;
                                }
                            }
                        }
                    }
                    responses.push(response);
                    continue;
                }

                if cmd.eq_ignore_ascii_case(b"COPY") {
                    use crate::command::keyspace::move_cmd as ksmv;
                    let src_db = conn.selected_db;
                    let db_count = ctx.shard_databases.db_count();
                    if let Some(copy_result) = ksmv::parse_copy_db_args(cmd_args, src_db, db_count)
                    {
                        // TXN guard: COPY ... DB n bypasses undo bookkeeping.
                        // Reject only when DB clause is present (cross-DB);
                        // same-DB COPY falls through to the normal write path.
                        if conn.in_cross_txn() {
                            // #499: poison the txn so COMMIT cannot report OK.
                            conn.mark_cross_txn_rejected(cmd);
                            responses.push(Frame::Error(bytes::Bytes::from_static(
                                crate::command::transaction::ERR_TXN_CROSS_SHARD,
                            )));
                            continue;
                        }
                        let response = match copy_result {
                            Err(e) => e,
                            Ok(ca) => crate::shard::slice::with_shard(|s| {
                                ksmv::with_two_slice_dbs(
                                    &mut s.databases,
                                    src_db,
                                    ca.dst_db,
                                    |src, dst| {
                                        ksmv::copy_core(
                                            src,
                                            dst,
                                            &ca.src_key,
                                            &ca.dst_key,
                                            ca.replace,
                                        )
                                    },
                                )
                            }),
                        };
                        // AOF only on actual success (:1). Matches handler_single
                        // — `:0` (key absent / dst exists w/o REPLACE) is a no-op.
                        // H1: durable path awaits fsync under appendfsync=always.
                        if matches!(response, Frame::Integer(1)) {
                            // v0.7 local-leg live replication — same contract
                            // as the main write leg (record backlog+offset
                            // synchronously before await; AOF leg does not
                            // double-advance, lsn = 0).
                            let repl_active = ft::replication_fanout_active(ctx);
                            if repl_active || ctx.aof_pool.is_some() {
                                let serialized = aof::serialize_command_for_log(&frame);
                                let lsn = if repl_active {
                                    ft::record_local_write_db(
                                        ctx,
                                        conn.selected_db,
                                        serialized.clone(),
                                    );
                                    0
                                } else {
                                    aof::AofWriterPool::issue_append_lsn(
                                        &ctx.repl_state,
                                        ctx.shard_id,
                                        serialized.len(),
                                    )
                                };
                                let Some(ref pool) = ctx.aof_pool else {
                                    responses.push(response);
                                    continue;
                                };
                                match pool
                                    .send_append_group(
                                        ctx.shard_id,
                                        lsn,
                                        conn.selected_db,
                                        serialized,
                                    )
                                    .await
                                {
                                    // Same one-barrier-per-batch contract as MOVE.
                                    Ok(true) => local_leg_write_idxs.push(responses.len()),
                                    Ok(false) => {}
                                    Err(_) => {
                                        responses.push(Frame::Error(bytes::Bytes::from_static(
                                            aof::AOF_FSYNC_ERR,
                                        )));
                                        continue;
                                    }
                                }
                            }
                        }
                        responses.push(response);
                        continue;
                    }
                    // No DB clause or same-db: fall through to normal write path
                }

                if metadata::is_write(cmd) {
                    // WRITE PATH: eviction + dispatch via ShardSlice.
                    //
                    // Fast path: when neither maxmemory nor disk-offload is
                    // configured (default deployment + default bench), the
                    // eviction call is a no-op.
                    //
                    // Returns Ok((result, new_selected_db, hset_inserts)) on success
                    // or Err(oom_frame) when OOM eviction fails (caller pushes + continues).
                    let write_result: Result<
                        (
                            DispatchResult,
                            usize,
                            smallvec::SmallVec<[(bytes::Bytes, u64); 4]>,
                        ),
                        Frame,
                    > = crate::shard::slice::with_shard(|s| {
                        let sel_db = conn.selected_db;
                        let db = &mut s.databases[sel_db];

                        if batch_eviction_active {
                            run_write_eviction_gate(ctx, db, sel_db, cmd)?;
                        }

                        // KV undo-log capture (MUST precede dispatch)
                        if let Some(ref mut txn) = conn.active_cross_txn {
                            if cmd.eq_ignore_ascii_case(b"DEL")
                                || cmd.eq_ignore_ascii_case(b"UNLINK")
                            {
                                for arg in cmd_args.iter() {
                                    if let Frame::BulkString(key_bytes) = arg {
                                        if let Some(old_entry) = db.get(key_bytes.as_ref()).cloned()
                                        {
                                            txn.kv_undo.record_delete(key_bytes.clone(), old_entry);
                                            let lsn = txn.snapshot_lsn;
                                            let tid = txn.txn_id;
                                            // Direct field access — the outer with_shard
                                            // closure already owns `s`; re-entering
                                            // with_shard here panics (slice re-entrancy
                                            // guard). `db` borrows s.databases only, so
                                            // s.kv_write_intents is a disjoint field (NLL).
                                            s.kv_write_intents.record_write(
                                                key_bytes.clone(),
                                                lsn,
                                                tid,
                                            );
                                        }
                                    }
                                }
                            } else if let Some(key) =
                                crate::server::conn::shared::extract_primary_key(cmd, cmd_args)
                            {
                                let old_entry = db.get(key.as_ref()).cloned();
                                let lsn = txn.snapshot_lsn;
                                let tid = txn.txn_id;
                                match old_entry {
                                    None => txn.kv_undo.record_insert(key.clone()),
                                    Some(entry) => txn.kv_undo.record_update(key.clone(), entry),
                                }
                                // Direct field access — see DEL/UNLINK arm above.
                                s.kv_write_intents.record_write(key.clone(), lsn, tid);
                            }
                        }

                        // Dispatch
                        let mut new_sel_db = sel_db;
                        let result = dispatch(db, cmd, cmd_args, &mut new_sel_db, db_count);
                        let response_frame = match &result {
                            DispatchResult::Response(f) | DispatchResult::Quit(f) => f.clone(),
                        };
                        let is_error = matches!(response_frame, Frame::Error(_));

                        // HSET auto-index: disjoint field borrows (NLL)
                        // &mut s.vector_store + &mut s.text_store are separate
                        // from s.databases[sel_db] already borrowed above as `db`.
                        // Re-borrow db after dispatch since `db` was moved into dispatch.
                        let hset_inserts = if !is_error && cmd.eq_ignore_ascii_case(b"HSET") {
                            if let Some(key) = cmd_args.first().and_then(|f| extract_bytes(f)) {
                                crate::shard::spsc_handler::auto_index_hset_public(
                                    &mut s.vector_store,
                                    &mut s.text_store,
                                    key.as_ref(),
                                    cmd_args,
                                    sel_db as u8,
                                )
                            } else {
                                smallvec::SmallVec::new()
                            }
                        } else {
                            smallvec::SmallVec::new()
                        };

                        // Auto-delete vectors on DEL/UNLINK (conn-local write path).
                        // Parity with the SPSC Execute arm and the tokio sharded
                        // handler — without this, deleted keys keep matching
                        // FT.SEARCH at shards=1 (soak-diagnostic resurrection bug).
                        if !is_error
                            && (cmd.eq_ignore_ascii_case(b"DEL")
                                || cmd.eq_ignore_ascii_case(b"UNLINK"))
                        {
                            crate::shard::spsc_handler::auto_delete_vectors(
                                &mut s.vector_store,
                                cmd_args,
                                sel_db as u8,
                            );
                            // task #46: tombstone any durable MQ stream(s)
                            // this generic DEL/UNLINK removed, so
                            // `replay_mq_wal` doesn't resurrect them.
                            crate::shard::mq_exec::auto_drop_mq_streams(s, cmd_args, sel_db);
                        }

                        // R4: HDEL of an indexed vector field tombstones it.
                        if !is_error && cmd.eq_ignore_ascii_case(b"HDEL") {
                            crate::shard::spsc_handler::auto_hdel_vectors(
                                &mut s.vector_store,
                                cmd_args,
                                sel_db as u8,
                            );
                        }

                        // R3: FLUSHALL/FLUSHDB clears index contents
                        // (FT.CREATE definitions survive). WS5a: FLUSHDB
                        // scopes to `sel_db`; FLUSHALL clears every db.
                        if !is_error
                            && (cmd.eq_ignore_ascii_case(b"FLUSHDB")
                                || cmd.eq_ignore_ascii_case(b"FLUSHALL"))
                        {
                            crate::shard::spsc_handler::auto_flush_indexes(
                                &mut s.vector_store,
                                &mut s.text_store,
                                cmd.eq_ignore_ascii_case(b"FLUSHDB"),
                                sel_db as u8,
                            );
                            // task #46: tombstone every durable MQ stream
                            // this FLUSHDB/FLUSHALL cleared.
                            crate::shard::mq_exec::auto_drop_mq_streams_on_flush(s, sel_db);
                        }

                        // Blocking wakeup: re-borrow db by index (NLL)
                        if !is_error {
                            // moon#595: shared with the SPSC sites. This gate
                            // used to omit XADD, so a stream reader blocked on
                            // a key THIS shard owns was never woken by a local
                            // write — while the same XADD arriving over SPSC
                            // woke it. Routing-dependent, so it read as a flake.
                            crate::blocking::wakeup::wake_producer(
                                &ctx.blocking_registry,
                                &mut s.databases[new_sel_db],
                                new_sel_db,
                                cmd,
                                cmd_args,
                            );
                        }

                        Ok((result, new_sel_db, hset_inserts))
                    });

                    // Unpack write result — OOM causes immediate continue
                    let (result, new_selected_db, hset_inserts) = match write_result {
                        Ok(t) => t,
                        Err(oom_frame) => {
                            responses.push(oom_frame);
                            continue;
                        }
                    };
                    conn.selected_db = new_selected_db;

                    // 1-in-16 latency sampling (outside closure — needs conn.cached_metrics)
                    conn.cmd_counter = conn.cmd_counter.wrapping_add(1);
                    let sample_latency = (conn.cmd_counter & 0xF) == 0;
                    let dispatch_start = sample_latency.then(std::time::Instant::now);

                    let mut response = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };

                    if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                        if let Some(start) = dispatch_start {
                            let elapsed_us = start.elapsed().as_micros() as u64;
                            crate::admin::metrics_setup::record_command_cached(
                                cmd_str,
                                elapsed_us,
                                &mut conn.cached_metrics,
                            );
                            if let Frame::Array(ref args) = frame {
                                crate::admin::metrics_setup::global_slowlog().maybe_record(
                                    elapsed_us,
                                    args.as_slice(),
                                    peer_addr.as_bytes(),
                                    conn.client_name
                                        .as_ref()
                                        .map_or(b"" as &[u8], |n| n.as_ref()),
                                );
                            }
                        } else {
                            crate::admin::metrics_setup::record_command_no_latency_cached(
                                cmd_str,
                                &mut conn.cached_metrics,
                            );
                        }
                    }

                    // D-2: keyless FLUSHDB/FLUSHALL routed local-only cleared just
                    // this shard — broadcast to every other shard (outside the
                    // with_shard closure: this awaits). Any failed leg turns the
                    // reply into an explicit partial-flush error, never silent +OK.
                    if !matches!(response, Frame::Error(_))
                        && (cmd.eq_ignore_ascii_case(b"FLUSHDB")
                            || cmd.eq_ignore_ascii_case(b"FLUSHALL"))
                    {
                        if ctx.num_shards > 1 {
                            if let Err(e) = crate::shard::coordinator::coordinate_flush_broadcast(
                                &frame,
                                ctx.shard_id,
                                ctx.num_shards,
                                conn.selected_db,
                                &ctx.dispatch_tx,
                                &ctx.spsc_notifiers,
                            )
                            .await
                            {
                                response = e;
                            }
                        }
                        // CLIENT TRACKING: a flush drops every cached key —
                        // push the RESP3 flush invalidation (invalidate + Null)
                        // to all tracking clients. Process-global table: one
                        // hook at the originating connection covers all shards.
                        if !matches!(response, Frame::Error(_)) {
                            crate::tracking::invalidation::invalidate_flush(&ctx.tracking_table);
                        }
                    }

                    // AOF logging for successful local writes.
                    // H1: durable path awaits fsync under appendfsync=always.
                    // On AOF failure we override `response` to an error
                    // frame and skip downstream side-effects (tracking
                    // invalidation, etc.) below — the client must see
                    // the failure, not a silent inconsistency.
                    let mut aof_failed = false;
                    // Always-mode local writes join the per-batch group commit:
                    // the append is enqueued fire-and-forget here and confirmed
                    // by ONE fsync_barrier before response serialization
                    // (resolve_local_leg_barrier), amortizing the fsync across
                    // the whole pipelined batch — previously each command
                    // awaited its own fsync ack (~1 fsync per write, the
                    // measured 8x deficit vs Redis at P16).
                    let mut aof_barrier_pending = false;
                    if !matches!(response, Frame::Error(_)) && is_write {
                        // v0.7 local-leg live replication: record the wire
                        // bytes BEFORE any await — `record_local_write` does
                        // the backlog append + offset advance synchronously
                        // (mutation and replication record are one no-await
                        // stretch, atomic w.r.t. the inline PSYNC task's
                        // snapshot capture on this thread) and defers only
                        // the live replica try_send to the event-loop drain.
                        // The AOF leg below must NOT also advance the offset
                        // (lsn = 0; per-shard order is append order, same
                        // contract as wal_append_and_fanout's cross-shard
                        // legs).
                        let repl_active = ft::replication_fanout_active(ctx);
                        if repl_active || ctx.aof_pool.is_some() {
                            let serialized = aof::serialize_command_for_log(&frame);
                            let lsn = if repl_active {
                                ft::record_local_write_db(
                                    ctx,
                                    conn.selected_db,
                                    serialized.clone(),
                                );
                                0
                            } else {
                                aof::AofWriterPool::issue_append_lsn(
                                    &ctx.repl_state,
                                    ctx.shard_id,
                                    serialized.len(),
                                )
                            };
                            if let Some(ref pool) = ctx.aof_pool {
                                match pool
                                    .send_append_group(
                                        ctx.shard_id,
                                        lsn,
                                        conn.selected_db,
                                        serialized,
                                    )
                                    .await
                                {
                                    Ok(true) => aof_barrier_pending = true,
                                    Ok(false) => {}
                                    Err(_) => {
                                        response = Frame::Error(bytes::Bytes::from_static(
                                            aof::AOF_FSYNC_ERR,
                                        ));
                                        aof_failed = true;
                                    }
                                }
                            }
                        }
                    }
                    // Suppress downstream effects on AOF failure — the
                    // client sees the error frame, no tracking churn.
                    if aof_failed {
                        responses.push(response);
                        continue;
                    }

                    // Phase 166 (Plan 02): record VectorIntents from HSET auto-index
                    // into active cross-store TXN so TXN.ABORT can tombstone them.
                    if !matches!(response, Frame::Error(_)) && !hset_inserts.is_empty() {
                        if let Some(txn) = conn.active_cross_txn.as_mut() {
                            for (index_name, key_hash) in hset_inserts {
                                txn.record_vector(key_hash, index_name);
                            }
                        }
                    }

                    // CLIENT TRACKING: any successful write (from ANY client,
                    // tracking or not) invalidates trackers of every written
                    // key — gated off the hot path by tracking_active().
                    if !matches!(response, Frame::Error(_)) {
                        crate::tracking::invalidation::invalidate_after_write(
                            &ctx.tracking_table,
                            cmd,
                            cmd_args,
                            client_id,
                        );
                    }
                    let mut response =
                        apply_resp3_conversion(cmd, cmd_args, response, conn.protocol_version);
                    if let Some(ws_id) = conn.workspace_id.as_ref() {
                        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                    }
                    // Only successful writes join the barrier set — an error
                    // response must not be overwritten by a barrier failure.
                    if aof_barrier_pending && !matches!(response, Frame::Error(_)) {
                        local_leg_write_idxs.push(responses.len());
                    }
                    responses.push(response);
                } else {
                    // Snapshot visibility filter for active cross-store transactions.
                    // MVCC: hide keys written by uncommitted foreign transactions.
                    if conn.in_cross_txn() {
                        if let Some(ref txn) = conn.active_cross_txn {
                            if let Some(key) =
                                crate::server::conn::shared::extract_primary_key(cmd, cmd_args)
                            {
                                let snapshot_lsn = txn.snapshot_lsn;
                                let my_txn_id = txn.txn_id;
                                // Clone committed treemap to release vector_store lock
                                // before acquiring kv_intents lock (lock ordering).
                                let committed = crate::shard::slice::with_shard(|s| {
                                    s.vector_store.txn_manager().committed_snapshot()
                                });
                                let visible = crate::shard::slice::with_shard(|s| {
                                    s.kv_write_intents.is_key_visible(
                                        key.as_ref(),
                                        snapshot_lsn,
                                        my_txn_id,
                                        &committed,
                                    )
                                });
                                if !visible {
                                    responses.push(Frame::Null);
                                    continue;
                                }
                            }
                        }
                    }

                    // task #59: GET on a key that only lives in the cold tier
                    // otherwise pays a blocking `pread` inline on this shard's
                    // event-loop thread (up to ~1.9s under spill/AOF write
                    // backlog), stalling every sibling connection on the
                    // shard. Peek (cheap, in-memory) whether this key needs a
                    // disk read; if so, `.await` the REAL result (no
                    // timeout -- see `tmp/task59-design.md` for why a bounded
                    // fallback was rejected as a correctness regression) off
                    // this shard thread, which lets siblings run while we
                    // wait, then promote the actual outcome into hot RAM so
                    // the normal synchronous `dispatch_read` below answers
                    // from RAM with no disk I/O of its own. Scoped to GET
                    // only for now; MGET/HGET/etc. and MULTI/EXEC/Lua keep
                    // using the original synchronous cold-read path
                    // (`Database::promote_cold_if_present`), unchanged.
                    if cmd.eq_ignore_ascii_case(b"GET") {
                        if let Some(key) = cmd_args.first().and_then(extract_bytes) {
                            let peek_now_ms = ctx.cached_clock.ms();
                            let cold_loc =
                                crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                                    if db.is_hot(key.as_ref()) {
                                        None
                                    } else if db
                                        .promote_inflight_if_present(key.as_ref(), peek_now_ms)
                                    {
                                        // #459: mid-spill, payload still in
                                        // RAM. Now hot, so `dispatch_read`
                                        // below answers it — and no disk read
                                        // was needed. Without this the key is
                                        // in no plane this peek consults and
                                        // GET answers nil for a key EXISTS
                                        // reports as present.
                                        None
                                    } else {
                                        db.cold_lookup_location(key.as_ref())
                                    }
                                });
                            if let Some((loc, shard_dir)) = cold_loc {
                                let prewarm_now_ms = ctx.cached_clock.ms();
                                let outcome =
                                    crate::storage::tiered::cold_read_pool::read_cold_entry_async(
                                        &shard_dir,
                                        loc,
                                        prewarm_now_ms,
                                    )
                                    .await;
                                crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                                    // TOCTOU fix (task #59 review round 2):
                                    // `promote_cold_outcome` revalidates that
                                    // the cold index still maps `key` to
                                    // `loc` before promoting -- closes the
                                    // window where a concurrent DEL/FLUSHDB
                                    // on this same shard thread ran while we
                                    // were suspended on the `.await` above,
                                    // which would otherwise resurrect a
                                    // deleted key.
                                    db.promote_cold_outcome(
                                        key.as_ref(),
                                        prewarm_now_ms,
                                        loc,
                                        outcome,
                                    );
                                });
                            }
                        }
                    }

                    // READ PATH: shared lock — no contention with other shards' reads
                    let now_ms = ctx.cached_clock.ms();
                    conn.cmd_counter = conn.cmd_counter.wrapping_add(1);
                    let sample_latency = (conn.cmd_counter & 0xF) == 0;
                    let dispatch_start = sample_latency.then(std::time::Instant::now);
                    let mut sel_db = conn.selected_db;
                    let result = crate::shard::slice::with_shard_db(conn.selected_db, |db| {
                        dispatch_read(db, cmd, cmd_args, now_ms, &mut sel_db, db_count)
                    });
                    let new_read_selected_db = sel_db;
                    conn.selected_db = new_read_selected_db;
                    if let Ok(cmd_str) = std::str::from_utf8(cmd) {
                        if let Some(start) = dispatch_start {
                            let elapsed_us = start.elapsed().as_micros() as u64;
                            crate::admin::metrics_setup::record_command_cached(
                                cmd_str,
                                elapsed_us,
                                &mut conn.cached_metrics,
                            );
                            if let Frame::Array(ref args) = frame {
                                crate::admin::metrics_setup::global_slowlog().maybe_record(
                                    elapsed_us,
                                    args.as_slice(),
                                    peer_addr.as_bytes(),
                                    conn.client_name
                                        .as_ref()
                                        .map_or(b"" as &[u8], |n| n.as_ref()),
                                );
                            }
                        } else {
                            crate::admin::metrics_setup::record_command_no_latency_cached(
                                cmd_str,
                                &mut conn.cached_metrics,
                            );
                        }
                    }

                    let response = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => {
                            should_quit = true;
                            f
                        }
                    };

                    // Track every key of a successful local read (MGET a b
                    // must track both, not just the first).
                    if conn.tracking_state.enabled
                        && !conn.tracking_state.bcast
                        && !matches!(response, Frame::Error(_))
                    {
                        crate::tracking::invalidation::track_read_keys(
                            &ctx.tracking_table,
                            cmd,
                            cmd_args,
                            client_id,
                            conn.tracking_state.noloop,
                        );
                    }
                    let mut response =
                        apply_resp3_conversion(cmd, cmd_args, response, conn.protocol_version);
                    if let Some(ws_id) = conn.workspace_id.as_ref() {
                        strip_workspace_prefix_from_response(ws_id, cmd, &mut response);
                    }
                    responses.push(response);
                } // end read/write split

            // (tracking and response push handled inside read/write branches above)
            } else if let Some(target) = target_shard {
                // TXN cross-shard guard: reject cross-shard writes in active TXN (no undo log).
                if conn.in_cross_txn() && metadata::is_write(cmd) {
                    // #499: poison the txn — the rejected write is NOT part of the
                    // transaction, so TXN.COMMIT must refuse rather than commit the
                    // accepted subset behind a `+OK`.
                    conn.mark_cross_txn_rejected(cmd);
                    responses.push(Frame::Error(bytes::Bytes::from_static(
                        crate::command::transaction::ERR_TXN_CROSS_SHARD,
                    )));
                    continue;
                }
                // SHARED-READ FAST PATH: bypass SPSC for cross-shard reads.
                // Guard: skip if pending writes exist for this target (pipeline ordering).
                // The fast path can be disabled via --cross-shard-fast-path=off to route
                // all foreign-shard reads through SPSC (eliminates RwLock contention at
                // the cost of one extra channel round-trip per read command).
                // See docs/production-guide.md §Cross-shard fast path.
                // E2: Cross-shard fast path disabled — ShardSlice is thread-local;
                // foreign-shard data can only be read via SPSC hop. All cross-shard
                // reads now route through the SPSC channel below regardless of
                // is_dispatch_read_supported. The fast path can be re-enabled in a
                // later wave when the cross-shard snapshot protocol is implemented.
                // See shardslice-migration TASK.md § C6.
                // Cross-shard write: deferred SPSC dispatch.
                // When workspace rewriting occurred, rebuild the frame with
                // prefixed args so the target shard stores the correct key.
                let dispatch_frame = if rewritten.is_some() {
                    let mut parts = Vec::with_capacity(1 + cmd_args.len());
                    parts.push(Frame::BulkString(Bytes::copy_from_slice(cmd)));
                    parts.extend_from_slice(cmd_args);
                    Frame::Array(parts.into())
                } else {
                    frame.clone()
                };
                let resp_idx = responses.len();
                responses.push(Frame::Null); // placeholder, filled after batch dispatch
                // Pre-compute AOF bytes before moving frame into Arc
                // `is_persisted_write`: never AOF/replicate a literal client
                // SELECT (task #35 — poisons the stream db context).
                let aof_bytes = if ctx.aof_pool.is_some() && metadata::is_persisted_write(cmd) {
                    Some(aof::serialize_command_for_log(&dispatch_frame))
                } else {
                    None
                };
                // CLIENT TRACKING: capture the write's key set at enqueue time
                // (gated); invalidation fires when the remote reply confirms.
                let track_keys = if crate::tracking::tracking_active() && metadata::is_write(cmd) {
                    Some(crate::tracking::invalidation::written_keys(cmd, cmd_args))
                } else {
                    None
                };
                // Remote READ by a tracking client: register the keys now
                // (Redis tracks reads even for missing keys, so registering
                // before the reply is faithful).
                if conn.tracking_state.enabled && !conn.tracking_state.bcast {
                    crate::tracking::invalidation::track_read_keys(
                        &ctx.tracking_table,
                        cmd,
                        cmd_args,
                        client_id,
                        conn.tracking_state.noloop,
                    );
                }
                // Classify HERE: this is the last point at which the command's
                // args exist. The reply loop below sees only this tag (moon#460).
                let resp3_shape = if conn.protocol_version >= 3 {
                    resp3_shape_for(cmd, cmd_args)
                } else {
                    crate::protocol::resp3::Resp3Shape::None
                };
                remote_groups.entry(target).or_default().push((
                    resp_idx,
                    std::sync::Arc::new(dispatch_frame),
                    aof_bytes,
                    track_keys,
                    resp3_shape,
                ));
                crate::admin::metrics_setup::record_dispatch_cross_spsc();
            }
        }

        // #438: re-encode any deferred batch tail back into the FRONT of
        // read_buf and skip the next socket read — the frames re-parse on the
        // next loop iteration, AFTER phase 2 below resolves every pending
        // remote reply and the epilogue flushes them. RESP command arrays
        // (arrays of bulk strings) round-trip losslessly through
        // serialize_resp3. If a migration executes at this batch's end, the
        // carried bytes ride along in `read_buf_remainder`.
        if let Some(from) = deferred_tail_from {
            let mut carry = BytesMut::with_capacity(64 + read_buf.len());
            for f in &frames[from..num_frames] {
                crate::protocol::serialize_resp3(f, &mut carry);
            }
            carry.extend_from_slice(&read_buf);
            read_buf = carry;
            carried_input = true;
        }

        // Phase 2a: Flush accumulated PUBLISH batches as PubSubPublishBatch messages
        if !publish_batches.is_empty() {
            let mut batch_slots: Vec<(
                std::sync::Arc<crate::shard::dispatch::PubSubResponseSlot>,
                Vec<usize>,
            )> = Vec::new();
            let mut split: Vec<(usize, Vec<(usize, Bytes, Bytes, bool)>)> = Vec::new();
            for (target, entries) in publish_batches.drain() {
                let (sharded, plain): (Vec<_>, Vec<_>) =
                    entries.into_iter().partition(|(_, _, _, s)| *s);
                if !plain.is_empty() {
                    split.push((target, plain));
                }
                if !sharded.is_empty() {
                    split.push((target, sharded));
                }
            }
            for (target, entries) in split {
                let n = entries.len();
                let is_sharded = entries[0].3;
                let slot = std::sync::Arc::new(
                    crate::shard::dispatch::PubSubResponseSlot::with_counts(1, n),
                );
                let resp_indices: Vec<usize> = entries.iter().map(|(idx, ..)| *idx).collect();
                let pairs: Vec<(Bytes, Bytes)> = entries
                    .into_iter()
                    .map(|(_, ch, msg, _)| (ch, msg))
                    .collect();

                let idx = ChannelMesh::target_index(ctx.shard_id, target);
                // E1: bounded backpressure retry instead of one bare try_push
                // — a transiently-full ring no longer loses the batch. Borrow
                // taken+released per attempt, never held across the backoff
                // await (tokio parity — handler_sharded).
                let mut pending = Some(if is_sharded {
                    ShardMessage::SPublishBatch {
                        pairs,
                        slot: slot.clone(),
                    }
                } else {
                    ShardMessage::PubSubPublishBatch {
                        pairs,
                        slot: slot.clone(),
                    }
                });
                let outcome = crate::shard::dispatch::push_with_backpressure(
                    &shutdown,
                    crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
                    crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
                    || match pending.take() {
                        None => true,
                        Some(m) => {
                            let mut producers = ctx.dispatch_tx.borrow_mut();
                            match producers[idx].try_push(m) {
                                Ok(()) => true,
                                Err(back) => {
                                    pending = Some(back);
                                    false
                                }
                            }
                        }
                    },
                )
                .await;
                match outcome {
                    crate::shard::dispatch::PushOutcome::Pushed => {
                        ctx.spsc_notifiers[target].notify_one();
                    }
                    outcome => {
                        // Give-up: deliver-to-zero so the reply can't hang —
                        // but loudly (was a silent drop pre-E1).
                        tracing::warn!(
                            "Shard {}: PUBLISH batch fan-out to shard {target} dropped ({outcome:?})",
                            ctx.shard_id
                        );
                        crate::admin::metrics_setup::record_xshard_fanout_drop("publish");
                        slot.add(0);
                    }
                }
                batch_slots.push((slot, resp_indices));
            }
            // Resolve all batch slots (E4: bounded — a wedged shard degrades
            // to an under-reported count, never a hung client).
            for (slot, resp_indices) in &batch_slots {
                if !crate::shard::dispatch::await_pubsub_slot_bounded(
                    slot,
                    crate::shard::dispatch::XSHARD_REPLY_TIMEOUT,
                )
                .await
                {
                    tracing::warn!(
                        "Shard {}: PUBLISH batch reply timed out awaiting remote shard",
                        ctx.shard_id
                    );
                    crate::admin::metrics_setup::record_xshard_reply_timeout("publish");
                }
                for (i, resp_idx) in resp_indices.iter().enumerate() {
                    let remote_count = slot.counts[i].load(std::sync::atomic::Ordering::Relaxed);
                    if remote_count > 0 {
                        if let Frame::Integer(ref mut total) = responses[*resp_idx] {
                            *total += remote_count;
                        }
                    }
                }
            }
        }

        // E4: set when a cross-shard reply await times out. The per-connection
        // ResponseSlot is REUSED across batches — a late fill after a timeout
        // would be read by the NEXT batch as its own reply — so a timeout is
        // fatal: error the affected entries, flush, then close the connection.
        let mut xshard_reply_fatal = false;

        // Phase 2b: Dispatch all deferred remote commands as batched
        // PipelineBatchSlotted messages (one per target shard), await all in parallel.
        if !remote_groups.is_empty() {
            reply_futures.clear();

            // L3b: dispatch via the pre-allocated ResponseSlotPool (no per-batch
            // flume oneshot alloc). `target` is captured per batch so the H1
            // fsync barrier at the bottom of the loop can route to the owning
            // shard's pool (not ctx.shard_id — mirrors handler_sharded).
            for (target, entries) in remote_groups.drain() {
                let slot_arc = response_pool.slot_arc(target);
                let (meta, commands): (Vec<RemoteMeta>, Vec<std::sync::Arc<Frame>>) = entries
                    .into_iter()
                    .map(|(idx, arc_frame, aof, tk, shape)| ((idx, aof, tk, shape), arc_frame))
                    .unzip();

                let msg = ShardMessage::PipelineBatchSlotted {
                    db_index: conn.selected_db,
                    commands,
                    response_slot: crate::shard::dispatch::ResponseSlotPtr(slot_arc),
                };
                let target_idx = ChannelMesh::target_index(ctx.shard_id, target);
                // F3: bounded backpressure retry. The closure retains the
                // message on a full ring; the helper checks `shutdown` before
                // every backoff so a graceful shutdown can't hang on a wedged
                // peer. Borrow of `ctx.dispatch_tx` is taken+released inside
                // each attempt — never held across the await.
                let mut pending = Some(msg);
                let outcome = crate::shard::dispatch::push_with_backpressure(
                    &shutdown,
                    crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
                    crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
                    || match pending.take() {
                        None => true,
                        Some(m) => {
                            let mut producers = ctx.dispatch_tx.borrow_mut();
                            match producers[target_idx].try_push(m) {
                                Ok(()) => true,
                                Err(back) => {
                                    pending = Some(back);
                                    false
                                }
                            }
                        }
                    },
                )
                .await;
                match outcome {
                    crate::shard::dispatch::PushOutcome::Pushed => {
                        tracing::trace!(
                            "Shard {}: pushed PipelineBatchSlotted to shard {}, notifying",
                            ctx.shard_id,
                            target
                        );
                        ctx.spsc_notifiers[target].notify_one();
                        reply_futures.push((meta, target));
                    }
                    crate::shard::dispatch::PushOutcome::Backpressure
                    | crate::shard::dispatch::PushOutcome::Cancelled => {
                        // Target shard not draining (saturated/wedged) or
                        // shutting down. The batch was NEVER accepted, so this
                        // is a clean reject — `slot_ptr` had no side effect
                        // (the slot stays EMPTY); fail this batch's entries
                        // instead of parking the connection forever.
                        tracing::warn!(
                            "Shard {}: cross-shard push to shard {} gave up ({:?}); rejecting batch",
                            ctx.shard_id,
                            target,
                            outcome
                        );
                        for (resp_idx, _, _, _) in &meta {
                            responses[*resp_idx] = Frame::Error(Bytes::from_static(
                                b"ERR cross-shard dispatch backpressure",
                            ));
                        }
                    }
                }
            }

            // L3b: await each response slot directly (tokio parity —
            // handler_sharded/mod.rs). Cross-thread wakes reach this task via
            // the slot's AtomicWaker + monoio's `sync`-feature waker channel
            // (proven by tests/spsc_wake_floor_red.rs::swf0 on both drivers).
            //
            // DROP-SAFETY (ResponseSlotPtr is Arc-owned): every batch pushed above
            // carries an `Arc<ResponseSlot>` clone, so the slot outlives BOTH this
            // connection's `response_pool` AND the in-flight message. Abandoning
            // this await (drop, panic-unwind, or a future shutdown break) can no
            // longer dangle the target shard's late `slot.fill()` — the refcount
            // keeps the slot alive until the last handle drops. (This replaced the
            // old raw-pointer-into-stack-pool design, whose contract required the
            // await to run to completion to avoid a panic-unwind UAF; see the
            // `ResponseSlotPtr` doc + PR review.) Since E4 the await is BOUNDED
            // (XSHARD_REPLY_TIMEOUT) — expiry errors the batch and closes the
            // connection, because the reused slot could otherwise hand a late
            // fill to the next batch. The tokio handler carries the identical
            // bounded await.
            //
            // C2 pipeline guard (see XSHARD_SPIN_MAX_BATCH_REMOTE): total cross-shard
            // commands in THIS batch. The reply-side spin may engage only for a singleton
            // foreign read; >1 means a pipeline / multi-key fan-out where a synchronous
            // spin would serialize the reads and starve pipelined throughput (s4-P16 −27%).
            let batch_remote_total: usize = reply_futures.iter().map(|(meta, _)| meta.len()).sum();
            for (meta, target) in reply_futures.drain(..) {
                // C2 (xshard-read-fastpath): adaptive idle-gated reply-side spin.
                // When this shard is near-idle (xshard_may_spin) AND this batch holds
                // a single cross-shard read, busy-poll the response slot for a bounded
                // budget to skip the reply-side cross-thread wake (the c1 win). The
                // poll is synchronous — it holds no borrow across `.await`; on miss it
                // falls through to the slot's park path. When the gate is closed
                // (busy/pipelined shard) the path is an immediate park.
                let _wait_guard = crate::shard::slice::XshardWaitGuard::new();
                let shard_responses = {
                    let mut spun = None;
                    if crate::shard::slice::xshard_should_spin(batch_remote_total) {
                        for _ in 0..crate::shard::slice::xshard_spin_budget() {
                            if let Some(r) = response_pool.slot_for(target).try_take() {
                                spun = Some(r);
                                break;
                            }
                            core::hint::spin_loop();
                        }
                    }
                    match spun {
                        Some(r) => Some(r),
                        // E4: bounded await — a wedged owner shard can no
                        // longer hang this client task forever.
                        None => {
                            crate::shard::dispatch::await_response_slot_bounded(
                                response_pool.future_for(target),
                                crate::shard::dispatch::XSHARD_REPLY_TIMEOUT,
                            )
                            .await
                        }
                    }
                };
                let Some(shard_responses) = shard_responses else {
                    tracing::error!(
                        "Shard {}: cross-shard reply from shard {target} timed out; \
                         failing the batch and closing the connection (slot unsafe to reuse)",
                        ctx.shard_id
                    );
                    crate::admin::metrics_setup::record_xshard_reply_timeout("dispatch");
                    for (resp_idx, _, _, _) in &meta {
                        responses[*resp_idx] =
                            Frame::Error(Bytes::from_static(b"ERR cross-shard reply timeout"));
                    }
                    xshard_reply_fatal = true;
                    // Skip the fsync barrier too: with no reply there is
                    // nothing durable to confirm for these entries.
                    continue;
                };
                // H1-BARRIER: collect write resp_idxs before consuming meta
                // so we can overwrite them if the fsync barrier fails.
                let mut write_resp_idxs: Vec<usize> = Vec::new();
                for ((resp_idx, aof_bytes, track_keys, resp3_shape), resp) in
                    meta.into_iter().zip(shard_responses)
                {
                    // C4-FOLD-FIX: AOF append for cross-shard writes is now done
                    // inside the SPSC arm (PipelineBatchSlotted), BEFORE the response
                    // slot is filled. Appending here (after awaiting the response)
                    // defers the append until after drain_spsc_shared returns, which
                    // makes AofFold's pending_aof_count undercount it → escape to
                    // new incr → double-apply on restart. The SPSC arm now owns the
                    // AOF write; aof_bytes below is used only for the barrier check.
                    // Shape was classified at enqueue, where the args still existed.
                    let resp = crate::protocol::resp3::apply_shape(
                        resp3_shape,
                        resp,
                        conn.protocol_version,
                    );
                    if aof_bytes.is_some() && !matches!(resp, Frame::Error(_)) {
                        write_resp_idxs.push(resp_idx);
                    }
                    // CLIENT TRACKING: remote write confirmed — invalidate the
                    // keys captured at enqueue time.
                    if let Some(keys) = track_keys {
                        if !matches!(resp, Frame::Error(_)) {
                            crate::tracking::invalidation::invalidate_keys(
                                &ctx.tracking_table,
                                &keys,
                                client_id,
                            );
                        }
                    }
                    responses[resp_idx] = resp;
                }

                // H1-BARRIER (C4-FOLD-FIX follow-up): under appendfsync=always,
                // call fsync_barrier once per target shard AFTER responses are
                // collected. The SPSC arm enqueued the Append fire-and-forget;
                // the barrier enqueues a zero-length AppendSync into the SAME
                // shard channel. Because the writer processes messages in order,
                // an acked barrier proves all prior Appends to this shard are on
                // durable storage. Under EverySec/No this is a zero-cost noop.
                if !write_resp_idxs.is_empty() {
                    if let Some(ref pool) = ctx.aof_pool {
                        if pool.fsync_barrier(target).await.is_err() {
                            for idx in write_resp_idxs {
                                responses[idx] =
                                    Frame::Error(Bytes::from_static(aof::AOF_FSYNC_ERR));
                            }
                        }
                    }
                }
            }
        }

        // v3-5 GROUP-COMMIT BARRIER: coordinator LOCAL legs were enqueued
        // fire-and-forget into MY shard's AOF writer during dispatch; one
        // barrier here confirms every one of them with a single fsync instead
        // of the retired per-command awaited fsync (2000ms tail stack). Runs
        // BEFORE response serialization — the client never sees +OK for a
        // write whose durability was not confirmed. Early-flush paths (PSYNC,
        // blocking, SUBSCRIBE) resolve the same barrier before THEIR flushes.
        crate::server::conn::shared::resolve_local_leg_barrier(
            &ctx.aof_pool,
            ctx.shard_id,
            &mut local_leg_write_idxs,
            &mut responses,
        )
        .await;

        // AUTH rate limiting: delay response to slow down brute-force attacks
        if auth_delay_ms > 0 {
            monoio::time::sleep(std::time::Duration::from_millis(auth_delay_ms)).await;
        }

        // Serialize all responses into write_buf, then do ONE write_all syscall.
        // `encode_batch`, not a bare loop: a pipelined HELLO changes the protocol
        // partway through and the replies before it must keep the old encoding.
        crate::server::conn::shared::encode_response_batch(&mut conn, &responses, &mut write_buf);

        // Write all responses in one batch using ownership I/O
        if !write_buf.is_empty() {
            let data = write_buf.split().freeze();
            if !write_all_bounded!(
                stream,
                data,
                write_timeout,
                out_cap_normal,
                client_live,
                client_id
            ) {
                break;
            }
        }

        // E4: a timed-out cross-shard reply slot must never be reused — the
        // error replies are flushed above, now close.
        if xshard_reply_fatal {
            break;
        }

        // Update live state after each batch — lock-free (QW8, 2026-06
        // review: this was a global registry write lock per batch), and
        // clock-free (shard-cached ms, not Instant::now()).
        client_live.touch(
            conn.selected_db,
            crate::client_registry::ClientFlags {
                subscriber: conn.subscription_count > 0,
                in_multi: conn.in_multi,
                // A batch just completed, so this connection is by definition
                // not blocked right now; the blocked bit is owned by
                // `set_blocked` around the blocking await.
                blocked: false,
                replica: conn.saw_replconf,
            },
            ctx.cached_clock.ms(),
        );

        // Check if migration was triggered during frame processing.
        // All responses for the current batch have been written, so the
        // client sees no interruption -- TCP socket stays open.
        // D4 (#438): re-evaluate eligibility HERE — the latch fired
        // mid-batch and the batch tail may have entered MULTI,
        // subscribed, or enabled tracking since. Ineligible → keep the
        // latch and retry at the next clean batch end.
        if let Some(target_shard) = conn.migration_target
            && conn.migration_eligible()
        {
            let migrated_state = MigratedConnectionState {
                selected_db: conn.selected_db,
                authenticated: conn.authenticated,
                client_name: conn.client_name.clone(),
                protocol_version: conn.protocol_version,
                current_user: conn.current_user.clone(),
                flags: 0,
                read_buf_remainder: read_buf.split(),
                client_id,
                peer_addr: peer_addr.clone(),
                workspace_id: conn.workspace_id,
            };
            return (
                MonoioHandlerResult::MigrateConnection {
                    state: migrated_state,
                    target_shard,
                },
                Some(stream),
            );
        }

        if should_quit {
            break;
        }

        // The valid prefix has been executed and flushed. Name the fault,
        // then close — in that order.
        if let Some(kind) = proto_fault.take() {
            let data = bytes::Bytes::from(super::util::proto_error_frame(kind));
            let (_wr, _b): (std::io::Result<usize>, bytes::Bytes) = stream.write_all(data).await;
            return (MonoioHandlerResult::Done, None);
        }

        // Check shutdown (polled after each batch -- acceptable for MVP)
        if shutdown.is_cancelled() {
            break;
        }

        // Shrink buffers if they grew too large (c10k W1: floor lowered
        // 64 KiB → 16 KiB — the old floor let 16–64 KiB high-waters ratchet
        // until disconnect; see tmp/C10K-REVIEW.md).
        if read_buf.capacity() > super::util::IO_BUF_SHRINK_TRIGGER {
            let remaining = read_buf.split();
            read_buf = BytesMut::with_capacity(8192);
            if !remaining.is_empty() {
                read_buf.extend_from_slice(&remaining);
            }
        }
        if write_buf.capacity() > super::util::IO_BUF_SHRINK_TRIGGER {
            write_buf = BytesMut::with_capacity(8192);
        }
        if tmp_buf.capacity() > super::util::IO_BUF_SHRINK_TRIGGER {
            tmp_buf = vec![0u8; 8192];
        }
        // c10k W1: drop the batch scratch capacity BEFORE parking in read().
        // Both vecs are dead scratch here (responses fully serialized to
        // write_buf, frames fully dispatched); clearing at the top of the
        // next iteration is too late for a burst-then-idle connection, which
        // parks holding the 1024-frame high-water (~74 KB each — the E5
        // permanent-ratchet finding). Sustained >256-frame pipelines pay one
        // shrink+regrow per batch (~sub-µs vs a 1024-command batch).
        // Deliver anything the batch's commands queued. Here rather than
        // per-command: a pipeline of 1000 SETs then costs one fan-out per
        // target shard instead of 1000, and the connection is about to park in
        // read() anyway. With notifications off this is one thread-local
        // borrow of an empty Vec.
        crate::notify_fanout::flush_from_connection(ctx);

        responses.clear();
        super::util::shrink_batch_vec(&mut responses);
        frames.clear();
        super::util::shrink_batch_vec(&mut frames);
    }

    // --- Graceful TCP shutdown: send FIN to client to avoid CLOSE_WAIT ---
    // Uses monoio's own shutdown() which properly manages the fd through
    // the runtime (unlike raw libc::shutdown which corrupts monoio state).
    let _ = stream.shutdown().await;

    // Phase 166: release any leaked cross-store TXN (client disconnected mid-txn).
    // Idempotent: TXN.ABORT already takes() active_cross_txn so this is a no-op if abort ran.
    // Closes T-161-05 — without this, a disconnect after TXN.BEGIN + SET would leak
    // kv_intents and pin the key invisible for all subsequent readers. Mirrors the
    // sharded runtime block in handler_sharded.rs so both paths delegate to the same
    // shared helper. FIN has already been sent; shard state is still intact.
    if let Some(txn) = conn.active_cross_txn.take() {
        // Box::pin (c10k future diet): this ~5.4 KB rollback state machine
        // otherwise sits inline in EVERY connection future; boxing costs one
        // alloc on the leaked-txn teardown path only.
        Box::pin(crate::transaction::abort::abort_cross_store_txn_routed(
            &ctx.shard_databases,
            ctx.shard_id,
            conn.selected_db,
            ctx.num_shards,
            &ctx.dispatch_tx,
            &ctx.spsc_notifiers,
            *txn,
        ))
        .await;
    }

    // --- Disconnect cleanup: propagate unsubscribe to all shards' remote subscriber maps ---
    if conn.subscriber_id > 0 {
        let removed_channels = {
            ctx.pubsub_registry
                .write()
                .unsubscribe_all(conn.subscriber_id)
        };
        let removed_patterns = {
            ctx.pubsub_registry
                .write()
                .punsubscribe_all(conn.subscriber_id)
        };
        // The sharded namespace partly self-heals — `spublish_shared` drops a
        // subscriber whose channel is closed — but the REMOTE maps never do.
        // Without this, every other shard keeps fanning SPUBLISH batches at a
        // shard with no local receiver, for the life of the process, and the
        // map grows one entry per disconnected sharded subscriber.
        let removed_shard = {
            ctx.pubsub_registry
                .write()
                .sunsubscribe_all(conn.subscriber_id)
        };
        for ch in removed_channels {
            unpropagate_subscription(
                &ctx.all_remote_sub_maps,
                &ch,
                ctx.shard_id,
                ctx.num_shards,
                false,
            );
        }
        for ch in removed_shard {
            unpropagate_shard_subscription(
                &ctx.all_remote_sub_maps,
                &ch,
                ctx.shard_id,
                ctx.num_shards,
            );
        }
        for pat in removed_patterns {
            unpropagate_subscription(
                &ctx.all_remote_sub_maps,
                &pat,
                ctx.shard_id,
                ctx.num_shards,
                true,
            );
        }
        // Clear pub/sub affinity on disconnect (no subscriptions remain).
        // Preserves any key-access hint — storage locality outlives the subscription.
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            ctx.pubsub_affinity.write().remove_pubsub(&addr.ip());
        }
    }

    // --- Disconnect cleanup: detach from the MONITOR feed ---
    //
    // Unconditional and cheap: `detach` is a no-op for a connection that was
    // never a monitor. Leaving a dead sink registered would keep the feed
    // formatting and fanning out to a closed channel, and would keep
    // `any_attached()` true — which also holds the inline fast path down for
    // the rest of the process.
    if conn.monitor_attached {
        crate::monitor::detach(client_id);
    }

    // --- Disconnect cleanup: release CLIENT TRACKING registration ---
    // A client that disconnects without `CLIENT TRACKING OFF` would otherwise
    // leave `ACTIVE_TRACKERS` nonzero and keep `tracking_active()` hot for the
    // rest of the process (single/sharded handlers already do this on close).
    // `untrack_all` only decrements when the client was actually tracked, so
    // gating on `tracking_active()` keeps the common no-tracking close lock-free.
    if crate::tracking::tracking_active() {
        ctx.tracking_table.lock().untrack_all(client_id);
    }

    // NOTE: connection close is recorded by the caller (conn_accept.rs) to
    // preserve symmetry with `try_accept_connection`, which owns the
    // increment.  Decrementing here too produces a double-decrement on the
    // AtomicU64 counter — it wraps to u64::MAX on the second subtraction
    // and all subsequent `try_accept_connection` comparisons against
    // `maxclients` reject new connections.
    (MonoioHandlerResult::Done, None)
}
