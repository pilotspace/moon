//! Thread-local database pointer bridge for redis.call/pcall from Lua scripts.
//!
//! The CURRENT_DB thread-local stores a raw pointer to the current Database,
//! set before script execution and cleared after. This is safe because:
//! 1. Each shard is single-threaded (no concurrent access)
//! 2. The pointer is valid for the entire duration of script execution
//! 3. The pointer is cleared immediately after script execution

use std::cell::Cell;

use mlua::prelude::*;

use crate::protocol::Frame;
use crate::storage::engine::StorageEngine;

thread_local! {
    /// Raw pointer to the current shard's Database during script execution.
    static CURRENT_DB: Cell<*mut ()> = const { Cell::new(std::ptr::null_mut()) };
    /// Current database index (for SELECT within scripts).
    static CURRENT_DB_IDX: Cell<usize> = const { Cell::new(0) };
    /// Total number of databases.
    static CURRENT_DB_COUNT: Cell<usize> = const { Cell::new(1) };
    /// Whether this script execution has performed any write commands.
    static SCRIPT_HAD_WRITE: Cell<bool> = const { Cell::new(false) };
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
}

/// Check whether the current script execution has performed any write commands.
pub fn script_had_write() -> bool {
    SCRIPT_HAD_WRITE.with(|c| c.get())
}

/// Create a Lua function that bridges redis.call/redis.pcall to the Rust dispatch().
///
/// If `propagate_errors` is true (redis.call), Frame::Error results are raised as Lua errors.
/// If false (redis.pcall), errors are returned as {err = "..."} tables.
pub fn make_redis_call_fn(lua: &Lua, propagate_errors: bool) -> mlua::Result<LuaFunction> {
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

            // Track writes for SCRIPT KILL safety check
            if crate::command::metadata::is_write(&cmd_bytes) {
                SCRIPT_HAD_WRITE.with(|c| c.set(true));
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
