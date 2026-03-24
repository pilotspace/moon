pub mod bridge;
pub mod cache;
pub mod sandbox;
pub mod types;

pub use cache::ScriptCache;

use bytes::Bytes;
use mlua::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;

use crate::protocol::Frame;
use crate::storage::Database;

/// Create and return a fully sandboxed Lua 5.4 VM with redis.* API registered.
/// Must be called on the shard thread (Lua is !Send).
pub fn setup_lua_vm() -> mlua::Result<Rc<Lua>> {
    todo!()
}

/// Handle the EVAL Redis command: parse args, validate keys, cache script, execute.
pub fn handle_eval(
    _lua: &Rc<Lua>,
    _cache: &Rc<RefCell<ScriptCache>>,
    _args: &[Frame],
    _db: &mut Database,
    _shard_id: usize,
    _num_shards: usize,
    _selected_db: usize,
    _db_count: usize,
) -> Frame {
    todo!()
}

/// Handle the EVALSHA Redis command: look up cached script by SHA1, then execute.
pub fn handle_evalsha(
    _lua: &Rc<Lua>,
    _cache: &Rc<RefCell<ScriptCache>>,
    _args: &[Frame],
    _db: &mut Database,
    _shard_id: usize,
    _num_shards: usize,
    _selected_db: usize,
    _db_count: usize,
) -> Frame {
    todo!()
}

/// Handle SCRIPT subcommands (LOAD, EXISTS, FLUSH).
/// Returns (response, Option<(sha1, script)>) -- the Option signals fan-out for SCRIPT LOAD.
pub fn handle_script_subcommand(
    _cache: &Rc<RefCell<ScriptCache>>,
    _args: &[Frame],
) -> (Frame, Option<(String, Bytes)>) {
    todo!()
}

/// Validate that all keys hash to the same shard. Returns Some(error Frame) on violation.
pub fn validate_keys_same_shard(
    _keys: &[Bytes],
    _shard_id: usize,
    _num_shards: usize,
) -> Option<Frame> {
    todo!()
}

/// Parse EVAL/EVALSHA arguments into (script, numkeys, keys, argv).
pub fn parse_eval_args(args: &[Frame]) -> Result<(Bytes, usize, Vec<Bytes>, Vec<Bytes>), Frame> {
    let _ = args;
    todo!()
}

/// Execute a Lua script with the given keys/argv, returning a Frame result.
fn run_script(
    _lua: &Lua,
    _script: &[u8],
    _keys: Vec<Bytes>,
    _argv: Vec<Bytes>,
    _db: &mut Database,
    _selected_db: usize,
    _db_count: usize,
) -> Frame {
    todo!()
}
