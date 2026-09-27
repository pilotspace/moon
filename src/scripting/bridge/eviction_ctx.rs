//! [`LuaEvictionCtx`]: the shard context of a script's eviction/OOM gate and
//! of its dual-plane (AOF + replication) write-effect emission.

use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use crate::config::RuntimeConfig;
use crate::persistence::aof::AofWriterPool;
use crate::protocol::Frame;
use crate::replication::state::ReplicationState;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::eviction::{EvictionRun, evict_to_budget};
use crate::storage::tiered::spill_thread::SpillRequest;

use super::script_state::{SCRIPT_OOM_MODE, ScriptOomMode};
#[cfg(feature = "runtime-monoio")]
use super::txn_capture::capture_txn_del;
use super::txn_capture::capture_txn_effect;

/// Shard context needed to enforce `--maxmemory` eviction before a Lua
/// `redis.call`/`redis.pcall` WRITE actually mutates the database.
///
/// # Why closure-capture instead of a thread-local
///
/// The bridge already uses a thread-local raw pointer (`CURRENT_DB`) for the
/// `Database` itself, because that pointer's target changes on every script
/// invocation. This context is different: it is the same for every script run
/// on a given shard for the shard's entire lifetime (the shard's
/// `ShardDatabases`/`RuntimeConfig`/spill handles never change identity).
/// `redis.call`/`redis.pcall` are Lua closures created exactly once per shard
/// by [`crate::scripting::setup_lua_vm`], at a point where the caller already
/// owns cloneable handles to all of this — so it is captured directly into
/// the `move` closure. This adds zero new `unsafe` code (the existing
/// `CURRENT_DB` unsafe deref is untouched) and zero per-call allocation. The
/// common case (`maxmemory` unset, no spill) is decided by a single Relaxed
/// load of the process-global [`crate::storage::eviction::maxmemory_is_set`]
/// atomic, so a tight `redis.call('SET', ...)` loop never takes the
/// `RuntimeConfig` lock at all in that case.
#[derive(Clone)]
pub struct LuaEvictionCtx(Option<LuaEvictionInner>);

#[derive(Clone)]
struct LuaEvictionInner {
    shard_databases: Arc<ShardDatabases>,
    runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
    shard_id: usize,
    spill_sender: Option<flume::Sender<SpillRequest>>,
    spill_file_id: Rc<Cell<u64>>,
    disk_offload_dir: Option<PathBuf>,
    /// Wave A part 2 (task #34): handles for dual-plane (AOF + replication)
    /// emission of a script's write effects. See
    /// [`LuaEvictionCtx::emit_effect`]. Read on BOTH runtimes since
    /// moon#517 — the per-plane gate moved into
    /// `reason_del::record_bytes_conn` (AOF everywhere, replication on
    /// monoio only).
    num_shards: usize,
    repl_state: Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    aof_pool: Option<Arc<AofWriterPool>>,
    /// Task #38: lock-free snapshot of `ReplicationState::is_replica_mirror`,
    /// cloned out once at ctx-construction time — same pattern as
    /// `ConnectionContext::is_replica_mirror` (`server/conn/core.rs`), so a
    /// tight `redis.call('SET', ...)` loop from Lua checks a single
    /// `Acquire` load instead of taking `repl_state`'s `RwLock` per write.
    /// `ReplicationState::set_role()` is the single owner of the mirror
    /// invariant and updates the same `AtomicBool` thereafter.
    is_replica_mirror: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl LuaEvictionCtx {
    /// No-op gate. Used by unit tests (no real shard context available).
    /// Production call sites (Lua EVAL/EVALSHA and Lua FUNCTION/FCALL) must
    /// build a real ctx via [`LuaEvictionCtx::new`] — see
    /// `src/shard/conn_accept.rs` and `src/scripting/functions.rs`.
    pub fn disabled() -> Self {
        LuaEvictionCtx(None)
    }

    /// Real gate, built from the shard's own handles at VM-setup time.
    ///
    /// `num_shards`/`repl_state`/`aof_pool` are the same handles
    /// `ConnectionContext` carries — threaded through here so a script's
    /// write effects (Wave A part 2) reach both durability planes exactly
    /// like every other write path. They are stable for the shard's entire
    /// lifetime, same as the pre-existing eviction handles this ctx already
    /// caches once per shard.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_databases: Arc<ShardDatabases>,
        runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
        shard_id: usize,
        spill_sender: Option<flume::Sender<SpillRequest>>,
        spill_file_id: Rc<Cell<u64>>,
        disk_offload_dir: Option<PathBuf>,
        num_shards: usize,
        repl_state: Option<Arc<parking_lot::RwLock<ReplicationState>>>,
        aof_pool: Option<Arc<AofWriterPool>>,
    ) -> Self {
        // Task #38: snapshot the lock-free mirror the same way
        // `ConnectionContext::new` does (`server/conn/core.rs`) — cloned out
        // under the read-lock once here, kept in sync thereafter by
        // `ReplicationState::set_role()` writing the same `AtomicBool`.
        let is_replica_mirror = repl_state
            .as_ref()
            .map(|rs| rs.read().is_replica_mirror.clone());
        LuaEvictionCtx(Some(LuaEvictionInner {
            shard_databases,
            runtime_config,
            shard_id,
            spill_sender,
            spill_file_id,
            disk_offload_dir,
            num_shards,
            repl_state,
            aof_pool,
            is_replica_mirror,
        }))
    }

    /// Task #38: true iff this shard currently believes it is a read-only
    /// replica (`ReplicationState::role == Replica`). Checked by
    /// `make_redis_call_fn` before letting a Lua `redis.call`/`redis.pcall`
    /// execute a `WRITE`-flagged inner command — mirrors upstream Redis,
    /// which fails a script at the *first* write attempt inside it rather
    /// than rejecting `EVAL`/`EVALSHA` outright (a read-only script must
    /// still run on a replica). `false` for a disabled ctx (unit tests) and
    /// whenever no `ReplicationState` is wired up (standalone server).
    pub(super) fn is_replica(&self) -> bool {
        self.0
            .as_ref()
            .and_then(|inner| inner.is_replica_mirror.as_ref())
            .is_some_and(|mirror| mirror.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Run the same eviction/OOM gate the connection handlers use
    /// (`run_write_eviction_gate` in `handler_monoio/mod.rs`), against `db`
    /// (the shard's `Database`, already borrowed by the caller via the
    /// `CURRENT_DB` thread-local). Returns the standard OOM `Frame::Error`
    /// on failure; `Ok(())` if within budget, eviction succeeded, or
    /// `maxmemory` is unset.
    ///
    /// Task #34 review (defect 3): a script's write can push the shard over
    /// `maxmemory` and force eviction of a BYSTANDER key — one this gate's
    /// policy sampled, not necessarily anything the script itself touched.
    /// Before this fix that plain-drop went through the non-reporting
    /// eviction variants (a hardcoded no-op sink), so it never reached
    /// `record_reason_del_conn` — an attached replica (or the AOF) never
    /// learned the bystander key was gone. Wired to the exact same
    /// dual-plane emission every other write-path eviction gate uses.
    pub(super) fn gate(
        &self,
        db: &mut crate::storage::Database,
        db_index: usize,
    ) -> Result<(), Frame> {
        self.gate_inner(db, db_index, None)
    }

    /// [`Self::gate`] for the script's own `redis.call(cmd, ...)` (moon#1241):
    /// a command that can only shrink memory (`db_quota::is_shrink_only_command`:
    /// DEL, UNLINK, HDEL, LPOP, EXPIRE, ...) is never REFUSED by the
    /// maxmemory gate or the per-db quota — eviction still runs, only the
    /// reject is bypassed — exactly as `run_write_eviction_gate` (connection
    /// path) and `spsc_eviction_gate` (routed leg) do. Redis allows those in
    /// a script under OOM because they are not `denyoom`.
    ///
    /// Only where redis would, by [`ScriptOomMode`]: in an EVAL (redis's
    /// compat mode) shrink-only commands pass; in a FUNCTION registered with
    /// `allow-oom` EVERY command passes maxmemory (redis's
    /// `SCRIPT_ALLOW_OOM`), the per-db quota still refusing growth; a
    /// FUNCTION without it is refused whole under OOM by redis 7.0, so its
    /// writes keep the refusal here.
    pub(super) fn gate_command(
        &self,
        cmd: &[u8],
        db: &mut crate::storage::Database,
        db_index: usize,
    ) -> Result<(), Frame> {
        self.gate_inner(db, db_index, Some(cmd))
    }

    fn gate_inner(
        &self,
        db: &mut crate::storage::Database,
        db_index: usize,
        cmd: Option<&[u8]>,
    ) -> Result<(), Frame> {
        let Some(inner) = self.0.as_ref() else {
            return Ok(());
        };
        // Lock-free fast path: a script issuing thousands of writes checks
        // process-global atomics (Gap C + WS5b), not the RuntimeConfig lock.
        // G1/L3a: whether a spill sender is wired is not a term — it only
        // routes a victim, and there is none without a limit.
        if !crate::storage::eviction::write_gate_active() {
            return Ok(());
        }
        let rt = inner.runtime_config.read();
        let budget = inner.shard_databases.elastic_budget(inner.shard_id);
        // moon#1294: ONE AOF backpressure bound for this eviction run,
        // shared by every victim's reason-DEL (a per-victim bound let one
        // `redis.call` that evicted k keys block the shard k × 500 ms).
        let mut aof_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        let mut on_plain_drop = |key: &[u8]| {
            // moon#894: same body-order rule as `emit_effect`.
            #[cfg(feature = "runtime-monoio")]
            if capture_txn_del(db_index, key) {
                return;
            }
            // Both runtimes (round-3 review MAJOR-1): tokio used to drop the
            // DEL here, so an AOF restart replayed every key a script's write
            // had evicted.
            crate::replication::reason_del::record_reason_del_conn(
                &inner.repl_state,
                inner.shard_id,
                inner.num_shards,
                inner.aof_pool.as_ref(),
                db_index,
                key,
                &mut aof_budget,
            );
        };
        let global_result = if let Some(sender) = &inner.spill_sender {
            let mut fid = inner.spill_file_id.get();
            let dir = inner
                .disk_offload_dir
                .as_deref()
                .unwrap_or(std::path::Path::new("."));
            // moon#1290 N6: tier durably without an AOF. A script the event
            // loop runs itself (a routed EVAL/FCALL, a script in a routed
            // MULTI body) finds the cell held by the drain, which LENDS its
            // manifest for the run (`shard::manifest_cell::lend`, wave-1
            // review F2) — without the lend its victims were plain-dropped.
            let res = crate::shard::manifest_cell::with_manifest(|manifest| {
                evict_to_budget(
                    db,
                    &rt,
                    EvictionRun::async_spill(sender, dir, &mut fid, db_index, manifest)
                        .budget(budget)
                        .report(&mut on_plain_drop),
                )
            });
            inner.spill_file_id.set(inner.spill_file_id.get().max(fid));
            res
        } else {
            evict_to_budget(
                db,
                &rt,
                EvictionRun::plain()
                    .budget(budget)
                    .report(&mut on_plain_drop),
            )
        };
        // moon#1241: the script's bypass (see `gate_command`) — eviction
        // above has run either way. `allow-oom` is redis's flag against
        // maxmemory only: moon's per-db quota, a tenant cap redis lacks,
        // still refuses an allow-oom function's growing writes (PR #1268
        // review) and, as everywhere, never a shrink-only command.
        let mode = SCRIPT_OOM_MODE.with(Cell::get);
        let shrink_only = cmd.is_some_and(crate::storage::db_quota::is_shrink_only_command);
        let bypass_quota = shrink_only && mode != ScriptOomMode::Deny;
        let bypass_global = bypass_quota || (cmd.is_some() && mode == ScriptOomMode::AllowOom);
        if !bypass_global {
            global_result?;
        }
        // WS5b: per-db quota, additive and finer-grained than the
        // whole-instance maxmemory gate above. Zero-cost when unconfigured.
        // NOT wired to `on_plain_drop` — pre-existing, documented gap (see
        // `db_quota::check_db_maxmemory`'s own doc comment), out of scope
        // for task #34.
        let quota = match cmd {
            Some(cmd) => {
                crate::storage::db_quota::check_db_maxmemory_for_command(db, db_index, &rt, cmd)
            }
            None => crate::storage::db_quota::check_db_maxmemory(db, db_index, &rt),
        };
        if bypass_quota { Ok(()) } else { quota }
    }

    /// Wave A part 2 (task #34): dual-plane (AOF + replication) emission of
    /// one successfully-executed script write effect. Called from
    /// `make_redis_call_fn` immediately after a W-flagged `redis.call`/
    /// `redis.pcall` inner command returns a non-error `Frame` — so effects
    /// emit as they happen (a script that writes two keys then errors on a
    /// third still durably records the first two).
    ///
    /// `db_index` is the CONNECTION's selected db, unchanged for the whole
    /// script now that `redis.call('SELECT', ...)` is rejected before
    /// dispatch (see `make_redis_call_fn`) — there is exactly one db per
    /// script execution.
    ///
    /// No-op for a disabled ctx (unit tests) only.
    ///
    /// moon#517: this used to be `#[cfg(feature = "runtime-monoio")]`-gated
    /// in full, with a `let _ = (db_index, cmd_and_args)` arm that silently
    /// DISCARDED the effect on a `runtime-tokio` build — an `EVAL` that
    /// wrote answered OK and left nothing for AOF replay, so the write was
    /// gone after a restart while every ordinary write around it survived.
    /// The gate now lives inside `record_effect_write`, per plane: AOF on
    /// every runtime, replication on monoio only (master-side PSYNC and the
    /// `shard::self_msg` relay it rides on do not exist under tokio — no
    /// ordinary tokio write replicates either, so this is parity, not a
    /// remaining script-specific gap).
    ///
    /// moon#825: `reply` is what the inner command answered — the record is
    /// derived from frame AND reply, so `redis.call('SPOP', k)` propagates as
    /// `SREM k <member>` and `redis.call('XADD', k, '*', …)` with its ID.
    pub(super) fn emit_effect(&self, db_index: usize, cmd_and_args: &[Frame], reply: &Frame) {
        // moon#894: inside a MULTI/EXEC body the effect joins the body's own
        // record list, in order, instead of racing ahead of it.
        if capture_txn_effect(db_index, cmd_and_args, reply) {
            return;
        }
        let Some(inner) = self.0.as_ref() else {
            return;
        };
        crate::replication::reason_del::record_effect_write(
            &inner.repl_state,
            inner.shard_id,
            inner.num_shards,
            inner.aof_pool.as_ref(),
            db_index,
            cmd_and_args,
            reply,
        );
    }
}
