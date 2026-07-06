//! Thread-local database pointer bridge for redis.call/pcall from Lua scripts.
//!
//! The CURRENT_DB thread-local stores a raw pointer to the current Database,
//! set before script execution and cleared after. This is safe because:
//! 1. Each shard is single-threaded (no concurrent access)
//! 2. The pointer is valid for the entire duration of script execution
//! 3. The pointer is cleared immediately after script execution

use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use mlua::prelude::*;

use crate::config::RuntimeConfig;
use crate::protocol::Frame;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::engine::StorageEngine;
use crate::storage::eviction::{
    try_evict_if_needed_async_spill_budget, try_evict_if_needed_budget,
};
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
/// `CURRENT_DB` unsafe deref is untouched) and zero per-call allocation: the
/// `Option` check on `maxmemory == 0` (the common case) short-circuits before
/// any lock or budget lookup.
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
}

impl LuaEvictionCtx {
    /// No-op gate. Used by unit tests (no real shard context available) and
    /// by the FCALL path (`src/scripting/functions.rs`), which has a
    /// pre-existing, documented gap for in-function write eviction — closing
    /// it is out of scope for this fix (see `tmp/OOM-SHIELD-CONTEXT.md`).
    pub fn disabled() -> Self {
        LuaEvictionCtx(None)
    }

    /// Real gate, built from the shard's own handles at VM-setup time.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_databases: Arc<ShardDatabases>,
        runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
        shard_id: usize,
        spill_sender: Option<flume::Sender<SpillRequest>>,
        spill_file_id: Rc<Cell<u64>>,
        disk_offload_dir: Option<PathBuf>,
    ) -> Self {
        LuaEvictionCtx(Some(LuaEvictionInner {
            shard_databases,
            runtime_config,
            shard_id,
            spill_sender,
            spill_file_id,
            disk_offload_dir,
        }))
    }

    /// Run the same eviction/OOM gate the connection handlers use
    /// (`run_write_eviction_gate` in `handler_monoio/mod.rs`), against `db`
    /// (the shard's `Database`, already borrowed by the caller via the
    /// `CURRENT_DB` thread-local). Returns the standard OOM `Frame::Error`
    /// on failure; `Ok(())` if within budget, eviction succeeded, or
    /// `maxmemory` is unset.
    fn gate(&self, db: &mut crate::storage::Database, db_index: usize) -> Result<(), Frame> {
        let Some(inner) = self.0.as_ref() else {
            return Ok(());
        };
        let rt = inner.runtime_config.read();
        if rt.maxmemory == 0 && inner.spill_sender.is_none() {
            return Ok(());
        }
        let budget = inner.shard_databases.elastic_budget(inner.shard_id);
        if let Some(sender) = &inner.spill_sender {
            let mut fid = inner.spill_file_id.get();
            let dir = inner
                .disk_offload_dir
                .as_deref()
                .unwrap_or(std::path::Path::new("."));
            let res = try_evict_if_needed_async_spill_budget(
                db, &rt, sender, dir, &mut fid, db_index, budget,
            );
            inner.spill_file_id.set(fid);
            res
        } else {
            try_evict_if_needed_budget(db, &rt, budget)
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

            let frame = db.execute_command(&cmd_bytes, &frames[1..], &mut db_idx, db_count);
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
