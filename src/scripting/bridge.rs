//! Thread-local database pointer bridge for redis.call/pcall from Lua scripts.
//!
//! The CURRENT_DB thread-local stores a raw pointer to the current Database,
//! set before script execution and cleared after. This is safe because:
//! 1. Each shard is single-threaded (no concurrent access)
//! 2. The pointer is valid for the entire duration of script execution
//! 3. The pointer is cleared immediately after script execution
//!
//! ## Write-effects replication (task #34 Wave A part 2)
//!
//! `EVAL`/`EVALSHA` deliberately carry no `WRITE` command-metadata flag (see
//! `command::metadata::eval_evalsha_never_write_flagged`), so the generic
//! per-command AOF/replication gate in `handler_monoio` never sees the
//! literal `EVAL <script> ...` invocation. Instead, [`make_redis_call_fn`]
//! itself records each successfully-executed, `WRITE`-flagged inner
//! `redis.call`/`redis.pcall` to both durability planes as it happens (see
//! [`LuaEvictionCtx::emit_effect`] / `replication::reason_del::
//! record_effect_write`) — this is the ONLY emission path for a script's
//! writes. Flipping EVAL/EVALSHA to `WRITE` would double-log every write a
//! script makes (the generic gate would replay the raw EVAL a second time
//! on top of the effect records already emitted here) — do not do that.
//!
//! `FCALL` (unlike EVAL/EVALSHA) IS `WRITE`-flagged, mirroring upstream
//! Redis Functions (FCALL always requires write permission; FCALL_RO is the
//! read-only variant) — but that flag only feeds ACL / `READONLY`-replica
//! gating. `try_handle_functions` always consumes FCALL with `continue`
//! before the generic per-command AOF/replication block runs, so FCALL
//! rides the exact same single-emission bridge path as EVAL/EVALSHA.
//!
//! `redis.call('SELECT', ...)` is rejected with a loud script error rather
//! than executed — see the intercept in [`make_redis_call_fn`] for why
//! silently allowing it would corrupt state.

use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use mlua::prelude::*;

use crate::config::RuntimeConfig;
use crate::persistence::aof::AofWriterPool;
use crate::protocol::Frame;
use crate::replication::state::ReplicationState;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::engine::StorageEngine;
use crate::storage::eviction::{EvictionRun, evict_to_budget};
use crate::storage::tiered::spill_thread::SpillRequest;

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
    /// [`LuaEvictionCtx::emit_effect`]. Only read under `runtime-monoio`
    /// (master-side PSYNC and the shard self-msg relay this rides on are
    /// monoio-only) — kept unconditional in the struct so every call site
    /// stays feature-uniform instead of growing a second constructor.
    #[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
    num_shards: usize,
    #[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
    repl_state: Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    #[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
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
    fn is_replica(&self) -> bool {
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
    fn gate(&self, db: &mut crate::storage::Database, db_index: usize) -> Result<(), Frame> {
        let Some(inner) = self.0.as_ref() else {
            return Ok(());
        };
        // Lock-free fast path: a script issuing thousands of writes checks
        // process-global atomics (Gap C + WS5b), not the RuntimeConfig lock.
        if inner.spill_sender.is_none()
            && !crate::storage::eviction::maxmemory_is_set()
            && !crate::storage::db_quota::db_maxmemory_any_set()
        {
            return Ok(());
        }
        let rt = inner.runtime_config.read();
        let budget = inner.shard_databases.elastic_budget(inner.shard_id);
        let mut on_plain_drop = |key: &[u8]| {
            #[cfg(feature = "runtime-monoio")]
            crate::replication::reason_del::record_reason_del_conn(
                &inner.repl_state,
                inner.shard_id,
                inner.num_shards,
                inner.aof_pool.as_ref(),
                db_index,
                key,
            );
            #[cfg(not(feature = "runtime-monoio"))]
            {
                let _ = key;
            }
        };
        let global_result = if let Some(sender) = &inner.spill_sender {
            let mut fid = inner.spill_file_id.get();
            let dir = inner
                .disk_offload_dir
                .as_deref()
                .unwrap_or(std::path::Path::new("."));
            let res = evict_to_budget(
                db,
                &rt,
                EvictionRun::async_spill(sender, dir, &mut fid, db_index, None)
                    .budget(budget)
                    .report(&mut on_plain_drop),
            );
            inner.spill_file_id.set(fid);
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
        global_result?;
        // WS5b: per-db quota, additive and finer-grained than the
        // whole-instance maxmemory gate above. Zero-cost when unconfigured.
        // NOT wired to `on_plain_drop` — pre-existing, documented gap (see
        // `db_quota::check_db_maxmemory`'s own doc comment), out of scope
        // for task #34.
        crate::storage::db_quota::check_db_maxmemory(db, db_index, &rt)
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
    /// No-op for a disabled ctx (unit tests) and under `runtime-tokio`
    /// (master-side PSYNC and the shard self-msg relay this rides on are
    /// monoio-only — see `replication::reason_del::record_effect_write`).
    fn emit_effect(&self, db_index: usize, cmd_and_args: &[Frame]) {
        let Some(_inner) = self.0.as_ref() else {
            return;
        };
        #[cfg(feature = "runtime-monoio")]
        crate::replication::reason_del::record_effect_write(
            &_inner.repl_state,
            _inner.shard_id,
            _inner.num_shards,
            _inner.aof_pool.as_ref(),
            db_index,
            cmd_and_args,
        );
        #[cfg(not(feature = "runtime-monoio"))]
        {
            let _ = (db_index, cmd_and_args);
        }
    }
}

thread_local! {
    /// Raw pointer to the current shard's Database during script execution.
    static CURRENT_DB: Cell<*mut ()> = const { Cell::new(std::ptr::null_mut()) };
    /// Current database index (for SELECT within scripts).
    static CURRENT_DB_IDX: Cell<usize> = const { Cell::new(0) };
    /// Total number of databases.
    static CURRENT_DB_COUNT: Cell<usize> = const { Cell::new(1) };
    /// Whether this script execution has performed any write commands.
    static SCRIPT_HAD_WRITE: Cell<bool> = const { Cell::new(false) };
    /// Whether this script is running in read-only mode (FCALL_RO).
    static SCRIPT_READ_ONLY: Cell<bool> = const { Cell::new(false) };
}

/// Set the thread-local database pointer before script execution.
pub fn set_script_db(db: &mut crate::storage::Database, db_idx: usize, db_count: usize) {
    CURRENT_DB.with(|c| c.set(db as *mut _ as *mut ()));
    CURRENT_DB_IDX.with(|c| c.set(db_idx));
    CURRENT_DB_COUNT.with(|c| c.set(db_count));
    SCRIPT_HAD_WRITE.with(|c| c.set(false));
}

/// Clear the thread-local database pointer after script execution.
pub fn clear_script_db() {
    CURRENT_DB.with(|c| c.set(std::ptr::null_mut()));
    SCRIPT_READ_ONLY.with(|c| c.set(false));
}

/// Set the read-only flag for the current script execution (FCALL_RO).
pub fn set_script_read_only(read_only: bool) {
    SCRIPT_READ_ONLY.with(|c| c.set(read_only));
}

/// Check whether the current script execution is in read-only mode.
pub fn is_script_read_only() -> bool {
    SCRIPT_READ_ONLY.with(|c| c.get())
}

/// Check whether the current script execution has performed any write commands.
pub fn script_had_write() -> bool {
    SCRIPT_HAD_WRITE.with(|c| c.get())
}

/// Create a Lua function that bridges redis.call/redis.pcall to the Rust dispatch().
///
/// If `propagate_errors` is true (redis.call), Frame::Error results are raised as Lua errors.
/// If false (redis.pcall), errors are returned as {err = "..."} tables.
pub fn make_redis_call_fn(
    lua: &Lua,
    propagate_errors: bool,
    eviction_ctx: LuaEvictionCtx,
) -> mlua::Result<LuaFunction> {
    lua.create_function(move |lua, args: LuaMultiValue| {
        // Convert all Lua arguments to Frames
        let frames: Vec<Frame> = args
            .iter()
            .map(|v| crate::scripting::types::lua_value_to_frame(lua, v))
            .collect::<mlua::Result<_>>()?;

        if frames.is_empty() {
            return Err(mlua::Error::RuntimeError(
                "ERR Please specify at least one argument for redis.call()".to_string(),
            ));
        }

        // Extract command name from first argument
        let cmd_bytes = match &frames[0] {
            Frame::BulkString(b) | Frame::SimpleString(b) => b.clone(),
            _ => {
                return Err(mlua::Error::RuntimeError(
                    "ERR Invalid command name".to_string(),
                ));
            }
        };

        // Access database via thread-local pointer (safe: single-threaded shard)
        let result = CURRENT_DB.with(|cell| {
            let ptr = cell.get() as *mut crate::storage::Database;
            if ptr.is_null() {
                return Err(mlua::Error::RuntimeError(
                    "ERR No database context".to_string(),
                ));
            }
            // SAFETY: Single-threaded shard guarantees exclusive access.
            // Pointer is valid for the entire script execution duration.
            let db = unsafe { &mut *ptr };
            let mut db_idx = CURRENT_DB_IDX.with(|c| c.get());
            let db_count = CURRENT_DB_COUNT.with(|c| c.get());

            // Wave A (task #34): `redis.call('SELECT', ...)` used to silently
            // corrupt state — the generic dispatch SELECT handler mutates
            // only the LOCAL `db_idx` below, but every write in this closure
            // still lands on `db`, the ONE `Database` this script execution
            // is pinned to (the `CURRENT_DB` thread-local is set once, by
            // `set_script_db`, before the script starts). A script that
            // called SELECT therefore kept writing the ORIGINAL db while
            // looking like it had switched. Fail loud instead of corrupting
            // a second db; a real multi-db-scripts feature is a follow-up.
            if cmd_bytes.eq_ignore_ascii_case(b"SELECT") {
                return Ok(Frame::Error(Bytes::from_static(
                    b"ERR SELECT inside scripts is not supported by moon yet",
                )));
            }

            // Reject non-readonly commands in read-only mode (FCALL_RO / EVAL_RO)
            // Use positive allowlist (READONLY flag) instead of negative blocklist (!WRITE)
            // to also block PUBLISH and other side-effecting commands.
            let cmd_is_readonly = crate::command::metadata::is_read(&cmd_bytes);
            let cmd_is_write = crate::command::metadata::is_write(&cmd_bytes);
            if SCRIPT_READ_ONLY.with(|c| c.get()) && !cmd_is_readonly {
                return Err(mlua::Error::RuntimeError(
                    "Write commands are not allowed from read-only scripts".to_string(),
                ));
            }
            // Task #38: reject a write attempted from a CLIENT-issued script
            // on a read-only replica, at the first offending `redis.call`/
            // `redis.pcall` — matching upstream Redis (a script that never
            // writes still runs on a replica; one that does is aborted mid-
            // script with `-READONLY`). This intentionally mirrors the exact
            // carve-outs `try_enforce_readonly` uses for the connection-level
            // gate (`server/conn/handler_monoio/dispatch.rs`) — commands that
            // are blanket-`WRITE`-flagged in `COMMAND_META` but carry
            // read-only subcommands. Master→replica Lua effect replication
            // (Wave A part 2, task #34) NEVER reaches this closure: replayed
            // effects are applied via `replication::apply::apply_local`,
            // which dispatches the inner command directly against storage
            // and never runs a Lua VM at all (see that module's doc comment)
            // — so this check cannot collide with, or block, replica apply.
            if cmd_is_write && eviction_ctx.is_replica() {
                let allowed_on_replica = if cmd_bytes.eq_ignore_ascii_case(b"WS") {
                    crate::command::workspace::is_ws_readonly_subcommand(&frames[1..])
                } else if cmd_bytes.eq_ignore_ascii_case(b"MQ") {
                    crate::command::mq::is_mq_readonly_subcommand(&frames[1..])
                } else {
                    #[cfg(feature = "graph")]
                    {
                        cmd_bytes.eq_ignore_ascii_case(b"GRAPH.QUERY")
                            && !crate::command::graph::is_cypher_write_query(&frames[1..])
                    }
                    #[cfg(not(feature = "graph"))]
                    {
                        false
                    }
                };
                if !allowed_on_replica {
                    return Ok(Frame::Error(Bytes::from_static(
                        b"READONLY You can't write against a read only replica.",
                    )));
                }
            }
            if cmd_is_write {
                // Track writes for SCRIPT KILL safety check
                SCRIPT_HAD_WRITE.with(|c| c.set(true));
                // OOM eviction gate (M3): mirrors the connection handlers'
                // `run_write_eviction_gate` — without this, a write inside a
                // script could grow memory past `maxmemory` without limit
                // (EVAL/EVALSHA carry no WRITE command flag, so the
                // dispatch-level OOM check never sees them at all).
                if let Err(oom) = eviction_ctx.gate(db, db_idx) {
                    return Ok(oom);
                }
            }

            // MONITOR: a script-issued command is fed with the literal `lua`
            // in place of a peer address — measured against redis-server 8.6.1,
            // which emits `[0 lua] "SET" "lk" "v"` after the client's own
            // `[0 127.0.0.1:… ] "eval" …` line. This is the one hook site with
            // no connection behind it, so the handler-level hooks structurally
            // cannot cover it: without this call an operator watching a
            // script-driven workload sees every EVAL and none of its effects.
            //
            // Fed BEFORE execution, matching every other hook site, so ordering
            // is issue-order. Costs one `Relaxed` load per `redis.call` when no
            // monitor is attached.
            crate::monitor::feed_frames(db_idx, "lua", &cmd_bytes, &frames[1..]);

            let frame = db.execute_command(&cmd_bytes, &frames[1..], &mut db_idx, db_count);

            // Wave A part 2 (task #34): dual-plane (AOF + replication)
            // emission of the effect, immediately after each successful
            // write — not batched to script end, so a partially-failing
            // script still durably records its completed writes. Skipped
            // for FCALL_RO / EVAL_RO (`cmd_is_write` implies the read-only
            // gate above already passed, so a write here only happens in a
            // normal read-write script).
            if cmd_is_write && !matches!(frame, Frame::Error(_)) {
                eviction_ctx.emit_effect(db_idx, &frames);
            }

            Ok(frame)
        })?;

        // redis.call: propagate errors as Lua errors
        // redis.pcall: return errors as {err = "..."} tables (handled by frame_to_lua_value)
        if propagate_errors {
            if let Frame::Error(e) = &result {
                return Err(mlua::Error::RuntimeError(
                    String::from_utf8_lossy(e).to_string(),
                ));
            }
        }

        crate::scripting::types::frame_to_lua_value(lua, &result)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimeConfig;
    use crate::shard::shared_databases::ShardDatabases;
    use crate::storage::Database;

    fn make_config(maxmemory: usize, policy: &str) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory,
            maxmemory_policy: policy.to_string(),
            maxmemory_samples: 5,
            db_maxmemory: Vec::new(),
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: None,
            appendonly: "no".to_string(),
            appendfsync: "everysec".to_string(),
            aclfile: None,
            dir: ".".to_string(),
            requirepass: None,
            protected_mode: "yes".to_string(),
            acllog_max_len: 128,
            client_pause_deadline_ms: 0,
            client_pause_write_only: false,
            lazyfree_threshold: 64,
            maxclients: 10000,
            client_query_buffer_limit: 1024 * 1024 * 1024,
            client_query_buffer_limit_preauth: 64 * 1024,
            client_write_timeout_ms: 60_000,
            client_output_buffer_limit_normal: 256 * 1024 * 1024,
            timeout: 0,
            tcp_keepalive: 300,
            num_shards: 1,
        }
    }

    /// Unit-level test of the gate wiring (defect 3, task #34 review): a
    /// black-box EVAL repro (fill via normal SETs, EVAL a write that tips
    /// the shard over `maxmemory`, assert an attached replica also loses the
    /// bystander) is possible but adds a full Lua VM + replica harness for a
    /// single wiring check — this pins the same fact directly against
    /// `LuaEvictionCtx::gate`, which is what actually changed.
    ///
    /// RED (before the fix): `gate` called the non-reporting
    /// `evict_to_budget` with a plain or async-spill `EvictionRun`,
    /// which pass a hardcoded no-op sink all the way down — a bystander key
    /// evicted here to make room for the script's write never reached
    /// `record_reason_del_conn`, so it never reached the AOF pool (nor an
    /// attached replica). GREEN (after): the gate now threads a real sink
    /// through to `record_reason_del_conn`, and the AOF pool observes a
    /// `DEL` for the evicted bystander key.
    #[test]
    #[cfg(feature = "runtime-monoio")]
    fn gate_reports_bystander_eviction_to_aof() {
        // Deliberately give the ctx a real `spill_sender` (`Some(..)`) so
        // `gate`'s lock-free fast path (`spill_sender.is_none() && !
        // maxmemory_is_set() && !db_maxmemory_any_set()`) is false by
        // construction, regardless of `MAXMEMORY_GLOBAL`'s CURRENT value —
        // that atomic is process-global and mutated by other tests running
        // concurrently in this same `cargo test --lib` binary
        // (`eviction::tests::maxmemory_publish_and_is_set_roundtrip`), so
        // depending on its value here would be flaky under parallel test
        // execution. `manifest` is always `None` at this call site (real
        // production behavior, not a test shortcut — see
        // `EvictionSink::AsyncSpill`'s doc comment),
        // and `config.appendonly == "no"` (the `make_config` default), so
        // this deterministically takes the "no manifest reachable" plain-
        // drop fallback inside `evict_to_budget`
        // — the sender/shard_dir below are never actually touched by that
        // branch, just required by the signature.
        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(1, "allkeys-lru")));

        let (tx, rx) =
            crate::runtime::channel::mpsc_bounded::<crate::persistence::aof::AofMessage>(64);
        let pool = crate::persistence::aof::AofWriterPool::top_level(tx);

        let (spill_tx, _spill_rx) =
            flume::bounded::<crate::storage::tiered::spill_thread::SpillRequest>(4);
        let tmp = tempfile::tempdir().unwrap();

        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            Some(spill_tx),
            Rc::new(Cell::new(1)),
            Some(tmp.path().to_path_buf()),
            1,    // num_shards
            None, // repl_state
            Some(pool),
        );

        let mut db = Database::new();
        for i in 0..50 {
            db.set_string(
                Bytes::from(format!("bystander:{i}")),
                Bytes::from(vec![0u8; 200]),
            );
        }
        let before_len = db.len();
        assert!(before_len > 0, "setup invariant: fixture must be non-empty");

        // maxmemory=1 byte forces the allkeys-lru policy to evict bystander
        // keys down toward empty on the very next gate check — none of them
        // are anything a script wrote (this call simulates the check BEFORE
        // the script's own write executes).
        let result = ctx.gate(&mut db, 0);
        assert!(result.is_ok(), "gate must succeed once the db is emptied");
        assert!(
            db.len() < before_len,
            "setup invariant: gate must have evicted at least one bystander key"
        );

        let mut saw_del_for_bystander = false;
        while let Ok(crate::persistence::aof::AofMessage::Append { bytes, .. }) = rx.try_recv() {
            let text = String::from_utf8_lossy(&bytes);
            if text.contains("DEL") && text.contains("bystander:") {
                saw_del_for_bystander = true;
            }
        }
        assert!(
            saw_del_for_bystander,
            "bystander eviction inside the Lua gate must emit a DEL record to the AOF plane"
        );
    }

    /// Task #38: `LuaEvictionCtx::is_replica()` must track
    /// `ReplicationState::is_replica_mirror` exactly, including transitions
    /// made AFTER the ctx was constructed — `make_redis_call_fn` calls this
    /// per `redis.call`, so a `REPLICAOF`/`REPLICAOF NO ONE` mid-lifetime
    /// role flip (S3.5a's whole reason for the mirror existing) must be
    /// visible to a long-lived shard's Lua bridge without rebuilding the
    /// ctx.
    ///
    /// RED (before this task): `LuaEvictionInner` carried no
    /// `is_replica_mirror` field at all — `LuaEvictionCtx` had no way to
    /// answer "is this shard a read-only replica right now," so
    /// `make_redis_call_fn` could not reject a writing script on a replica.
    #[test]
    fn is_replica_tracks_role_transitions_after_construction() {
        use crate::replication::state::{ReplicaHandshakeState, ReplicationRole, ReplicationState};

        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        let runtime_config = Arc::new(parking_lot::RwLock::new(make_config(0, "noeviction")));
        let repl_state = Arc::new(parking_lot::RwLock::new(ReplicationState::new(
            1,
            "a".repeat(40),
            "0".repeat(40),
        )));

        let ctx = LuaEvictionCtx::new(
            shard_databases,
            runtime_config,
            0,
            None,
            Rc::new(Cell::new(1)),
            None,
            1,
            Some(repl_state.clone()),
            None,
        );

        assert!(
            !ctx.is_replica(),
            "fresh ReplicationState defaults to Master"
        );

        repl_state.write().set_role(ReplicationRole::Replica {
            host: "127.0.0.1".to_string(),
            port: 6379,
            state: ReplicaHandshakeState::PingPending,
        });
        assert!(
            ctx.is_replica(),
            "is_replica() must observe a role flip that happened after ctx construction"
        );

        repl_state.write().set_role(ReplicationRole::Master);
        assert!(
            !ctx.is_replica(),
            "REPLICAOF NO ONE must flip is_replica() back to false"
        );
    }

    /// `is_replica()` on a `disabled()` ctx (no shard context — plain unit
    /// tests of Lua scripts that don't go through a real shard) must be
    /// `false`, never panic.
    #[test]
    fn is_replica_false_for_disabled_ctx() {
        let ctx = LuaEvictionCtx::disabled();
        assert!(!ctx.is_replica());
    }
}
