pub mod bridge;
pub mod cache;
pub mod functions;
pub mod sandbox;
pub mod types;

pub use cache::ScriptCache;
pub use functions::FunctionRegistry;

use bytes::Bytes;
use mlua::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use crate::protocol::Frame;
use crate::storage::Database;

/// Create and return a fully sandboxed Lua 5.4 VM with redis.* API registered.
/// Must be called on the shard thread (Lua is !Send).
///
/// `eviction_ctx` is captured into the `redis.call`/`redis.pcall` closures for
/// the lifetime of this VM (M3 OOM-bypass fix) — pass
/// [`bridge::LuaEvictionCtx::disabled()`] when no shard context applies
/// (tests).
pub fn setup_lua_vm(eviction_ctx: bridge::LuaEvictionCtx) -> mlua::Result<Rc<Lua>> {
    let lua = Rc::new(Lua::new());
    sandbox::setup_sandbox(&lua)?;
    sandbox::register_redis_api(&lua, eviction_ctx)?;
    Ok(lua)
}

/// A shard's one Lua VM slot: `None` until the first connection lands
/// (`conn_accept`) or the first routed script arrives
/// ([`ShardLuaRuntime::vm`]) — whichever gets there first builds it, and every
/// later caller reuses it.
pub type ShardLuaSlot = Rc<RefCell<Option<Rc<Lua>>>>;

/// Bytes the shard's Lua VM currently holds — `Lua::used_memory()`, i.e. the
/// interpreter heap, the figure Redis publishes as `used_memory_lua`.
///
/// `Some(0)` when the shard has not built a VM yet. `None` means the slot was
/// already mutably borrowed and could not be sampled this tick; callers must
/// leave the last published value in place rather than store a spurious 0.
/// (No current caller can actually collide — every `borrow_mut` of the slot is
/// a short synchronous block with no `.await` inside — but a publish path on
/// the shard thread must not be one refactor away from a `RefCell` panic,
/// which on a shard thread aborts the process.)
///
/// # moon#506
///
/// The only sanctioned way to sample a shard's Lua footprint. The bug this
/// replaces was not two VMs: `ShardStoreMemory::lua` carried
/// `ScriptCache::resident_bytes()` (48 bytes for `return 1` — a 40-char SHA1
/// key plus an 8-byte body) while the VM executing that script held ~25KB, and
/// a `used_memory_lua` built on the cache figure would have been wrong by
/// three orders of magnitude on both runtimes.
#[must_use]
pub fn vm_used_memory(slot: &ShardLuaSlot) -> Option<usize> {
    match slot.try_borrow() {
        Ok(vm) => Some(vm.as_ref().map_or(0, |lua| lua.used_memory())),
        Err(_) => None,
    }
}

/// Handle the EVAL Redis command: parse args, validate keys, cache script, run.
pub fn handle_eval(
    lua: &Rc<Lua>,
    cache: &Rc<RefCell<ScriptCache>>,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
) -> Frame {
    let (script, _numkeys, keys, argv) = match parse_eval_args(args) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    // Validate cross-shard keys before touching the Lua VM
    if num_shards > 1 {
        if let Some(err) = validate_keys_same_shard(&keys, shard_id, num_shards) {
            return err;
        }
    }

    // Cache the script (idempotent -- duplicates are no-ops)
    cache.borrow_mut().load(script.clone());

    run_script(lua, script.as_ref(), keys, argv, db, selected_db, db_count)
}

/// Handle the EVALSHA Redis command: look up cached script by SHA1, then run.
pub fn handle_evalsha(
    lua: &Rc<Lua>,
    cache: &Rc<RefCell<ScriptCache>>,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'evalsha' command",
        ));
    }

    // Extract SHA1 hex from first argument
    let sha1_hex = match &args[0] {
        Frame::BulkString(b) => String::from_utf8_lossy(b).to_lowercase(),
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid SHA1 hex string"));
        }
    };

    // Look up script in cache
    let script = {
        let cache_ref = cache.borrow();
        match cache_ref.get(&sha1_hex) {
            Some(s) => s.clone(),
            None => {
                return Frame::Error(Bytes::from_static(
                    b"NOSCRIPT No matching script. Please use EVAL.",
                ));
            }
        }
    };

    // Parse remaining args (construct synthetic eval args with script in place of sha)
    let mut eval_args = vec![Frame::BulkString(script.clone())];
    eval_args.extend_from_slice(&args[1..]);

    let (_script_bytes, _numkeys, keys, argv) = match parse_eval_args(&eval_args) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    // Validate cross-shard keys
    if num_shards > 1 {
        if let Some(err) = validate_keys_same_shard(&keys, shard_id, num_shards) {
            return err;
        }
    }

    run_script(lua, script.as_ref(), keys, argv, db, selected_db, db_count)
}

/// Handle SCRIPT subcommands (LOAD, EXISTS, FLUSH).
/// Returns (response, Option<(sha1, script)>) -- the Option signals fan-out for SCRIPT LOAD.
pub fn handle_script_subcommand(
    cache: &Rc<RefCell<ScriptCache>>,
    args: &[Frame],
) -> (Frame, Option<(String, Bytes)>) {
    let sub = match args.first() {
        Some(Frame::BulkString(b)) => b.clone(),
        _ => {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'script' command",
                )),
                None,
            );
        }
    };

    if sub.eq_ignore_ascii_case(b"LOAD") {
        if args.len() != 2 {
            return (Frame::Error(Bytes::from_static(b"ERR syntax error")), None);
        }
        let script = match &args[1] {
            Frame::BulkString(b) => b.clone(),
            _ => {
                return (
                    Frame::Error(Bytes::from_static(b"ERR invalid script")),
                    None,
                );
            }
        };
        let sha = cache.borrow_mut().load(script.clone());
        (
            Frame::BulkString(Bytes::from(sha.clone())),
            Some((sha, script)),
        )
    } else if sub.eq_ignore_ascii_case(b"EXISTS") {
        let cache_ref = cache.borrow();
        let results: Vec<Frame> = args[1..]
            .iter()
            .filter_map(|f| match f {
                Frame::BulkString(b) => Some(b.clone()),
                _ => None,
            })
            .map(|sha| {
                let exists = cache_ref.exists(std::str::from_utf8(&sha).unwrap_or(""));
                Frame::Integer(if exists { 1 } else { 0 })
            })
            .collect();
        (Frame::Array(results.into()), None)
    } else if sub.eq_ignore_ascii_case(b"FLUSH") {
        cache.borrow_mut().flush();
        (Frame::SimpleString(Bytes::from_static(b"OK")), None)
    } else {
        (
            Frame::Error(Bytes::from(format!(
                "ERR unknown subcommand '{}' for 'script' command",
                String::from_utf8_lossy(&sub)
            ))),
            None,
        )
    }
}

/// The shard's own Lua VM, plus what is needed to build it on first use.
///
/// Exists because moon#508's fix ROUTES a script to the shard owning its keys,
/// so a script can now arrive over the SPSC mesh at a shard that has no
/// connection of its own and has therefore never built a VM. Before routing,
/// the VM was only ever created from `conn_accept`, on the connection's shard.
///
/// The `Rc<RefCell<Option<..>>>` slot is the SAME one `conn_accept` fills, so a
/// shard still has exactly one VM however it is first reached — whichever path
/// gets there first wins and the other reuses it.
pub struct ShardLuaRuntime {
    slot: Rc<RefCell<Option<Rc<Lua>>>>,
    eviction_ctx: bridge::LuaEvictionCtx,
    /// Carried here rather than added to the SPSC handler's already very wide
    /// signature: both are per-shard constants, which is what this struct is.
    num_shards: usize,
}

impl ShardLuaRuntime {
    pub fn new(
        slot: Rc<RefCell<Option<Rc<Lua>>>>,
        eviction_ctx: bridge::LuaEvictionCtx,
        num_shards: usize,
    ) -> Self {
        Self {
            slot,
            eviction_ctx,
            num_shards,
        }
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// The shard's VM, built on first use.
    ///
    /// Returns `None` instead of panicking when the VM cannot be created: this
    /// runs on the shard thread, where a panic aborts the whole process, and a
    /// malformed-script client must never be able to do that. `conn_accept`
    /// still `expect()`s at startup, where failing loudly is right.
    pub fn vm(&self) -> Option<Rc<Lua>> {
        let mut slot = self.slot.borrow_mut();
        if slot.is_none() {
            match setup_lua_vm(self.eviction_ctx.clone()) {
                Ok(vm) => *slot = Some(vm),
                Err(e) => {
                    tracing::error!("Lua VM initialization failed on shard thread: {e}");
                    return None;
                }
            }
        }
        slot.clone()
    }
}

/// Where a script must run, decided by the shard ownership of its keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptRoute {
    /// Run here: no keys at all, single shard, or every key is local.
    Local,
    /// Every key lives on this OTHER shard — send the script there.
    Remote(usize),
    /// The keys span shards. A script executes against ONE shard's database,
    /// so this genuinely cannot be served and must be refused.
    CrossShard,
}

/// Decide where a script's keys require it to run (moon#508).
///
/// Before this existed, [`validate_keys_same_shard`] was the whole policy, and
/// it asked the wrong question: it required every key to hash to the shard the
/// CONNECTION happened to occupy. One key cannot cross slots, but it very
/// easily lives on another shard, so a single-key script was refused with
/// `CROSSSLOT` about `1 - 1/shards` of the time — 7 of 8 measured at
/// `--shards 4`. `CROSSSLOT` was standing in for "I cannot run this *here*",
/// and nothing ever asked where it *could* run.
///
/// Keyless scripts stay local deliberately: with no key there is nothing to
/// route by, every shard is equally correct, and running in place avoids a
/// pointless hop. That is also why `numkeys=0` always worked and made the
/// defect look intermittent instead of systematic.
pub fn route_script_keys(keys: &[Bytes], shard_id: usize, num_shards: usize) -> ScriptRoute {
    if num_shards <= 1 || keys.is_empty() {
        return ScriptRoute::Local;
    }
    use crate::shard::dispatch::key_to_shard;
    let target = key_to_shard(&keys[0], num_shards);
    if keys[1..]
        .iter()
        .any(|k| key_to_shard(k, num_shards) != target)
    {
        return ScriptRoute::CrossShard;
    }
    if target == shard_id {
        ScriptRoute::Local
    } else {
        ScriptRoute::Remote(target)
    }
}

/// Validate that all keys hash to the current shard. Returns Some(error) on violation.
///
/// Retained as the shard-side backstop AFTER [`route_script_keys`] has already
/// sent the script to the owning shard: at that point every key must be local,
/// so a violation here means the routing decision and the execution site
/// disagree — which would silently read another shard's (empty) view of a key.
pub fn validate_keys_same_shard(
    keys: &[Bytes],
    shard_id: usize,
    num_shards: usize,
) -> Option<Frame> {
    if num_shards <= 1 {
        return None;
    }
    use crate::shard::dispatch::key_to_shard;
    for key in keys {
        if key_to_shard(key, num_shards) != shard_id {
            return Some(Frame::Error(Bytes::from_static(
                b"CROSSSLOT Keys in script don't hash to the same slot and shard",
            )));
        }
    }
    None
}

/// Parse EVAL/EVALSHA arguments into (script, numkeys, keys, argv).
pub fn parse_eval_args(args: &[Frame]) -> Result<(Bytes, usize, Vec<Bytes>, Vec<Bytes>), Frame> {
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'eval' command",
        )));
    }
    let script = match &args[0] {
        Frame::BulkString(b) => b.clone(),
        _ => return Err(Frame::Error(Bytes::from_static(b"ERR invalid script"))),
    };
    let numkeys: usize = match &args[1] {
        Frame::BulkString(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            })?,
        Frame::Integer(n) => {
            if *n < 0 {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
            *n as usize
        }
        _ => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };
    if args.len() < 2 + numkeys {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Number of keys can't be greater than number of args",
        )));
    }
    let keys: Vec<Bytes> = args[2..2 + numkeys]
        .iter()
        .filter_map(|f| match f {
            Frame::BulkString(b) => Some(b.clone()),
            _ => None,
        })
        .collect();
    let argv: Vec<Bytes> = args[2 + numkeys..]
        .iter()
        .filter_map(|f| match f {
            Frame::BulkString(b) => Some(b.clone()),
            _ => None,
        })
        .collect();
    Ok((script, numkeys, keys, argv))
}

/// Execute a Lua script with the given keys/argv, returning a Frame result.
///
/// Sets up the thread-local DB pointer, installs timeout hook, populates
/// KEYS and ARGV globals (1-indexed), executes the script, and cleans up.
fn run_script(
    lua: &Lua,
    script: &[u8],
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    db: &mut Database,
    selected_db: usize,
    db_count: usize,
) -> Frame {
    // Set thread-local DB pointer for redis.call/pcall bridge
    bridge::set_script_db(db, selected_db, db_count);

    // Install timeout hook (5-second wall-clock limit)
    let timeout = Duration::from_secs(5);
    if sandbox::install_timeout_hook(lua, timeout).is_err() {
        bridge::clear_script_db();
        return Frame::Error(Bytes::from_static(
            b"ERR Failed to install script timeout hook",
        ));
    }

    // Execute the script
    let result = (|| -> mlua::Result<Frame> {
        // Set KEYS global (1-indexed Lua table)
        let keys_table = lua.create_table()?;
        for (i, key) in keys.iter().enumerate() {
            keys_table.set(i as i64 + 1, lua.create_string(key.as_ref())?)?;
        }
        lua.globals().set("KEYS", keys_table)?;

        // Set ARGV global (1-indexed Lua table)
        let argv_table = lua.create_table()?;
        for (i, arg) in argv.iter().enumerate() {
            argv_table.set(i as i64 + 1, lua.create_string(arg.as_ref())?)?;
        }
        lua.globals().set("ARGV", argv_table)?;

        // Load and execute
        let val: LuaValue = lua.load(script).eval()?;
        types::lua_value_to_frame(lua, &val)
    })();

    // ALWAYS clean up -- both success and error paths (Pitfall 3)
    sandbox::remove_timeout_hook(lua);
    bridge::clear_script_db();

    match result {
        Ok(frame) => frame,
        Err(mlua::Error::RuntimeError(msg)) if msg.contains("ERR Lua script timeout") => {
            Frame::Error(Bytes::from_static(b"BUSY Lua script timeout exceeded"))
        }
        Err(e) => Frame::Error(Bytes::from(format!("ERR Error running script: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_eval_args_basic() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"return 1")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];
        let (script, numkeys, keys, argv) = parse_eval_args(&args).unwrap();
        assert_eq!(script, Bytes::from_static(b"return 1"));
        assert_eq!(numkeys, 0);
        assert!(keys.is_empty());
        assert!(argv.is_empty());
    }

    #[test]
    fn test_parse_eval_args_with_keys_and_argv() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"return KEYS[1]")),
            Frame::BulkString(Bytes::from_static(b"2")),
            Frame::BulkString(Bytes::from_static(b"key1")),
            Frame::BulkString(Bytes::from_static(b"key2")),
            Frame::BulkString(Bytes::from_static(b"arg1")),
        ];
        let (_, numkeys, keys, argv) = parse_eval_args(&args).unwrap();
        assert_eq!(numkeys, 2);
        assert_eq!(keys.len(), 2);
        assert_eq!(argv.len(), 1);
        assert_eq!(keys[0], Bytes::from_static(b"key1"));
        assert_eq!(keys[1], Bytes::from_static(b"key2"));
        assert_eq!(argv[0], Bytes::from_static(b"arg1"));
    }

    #[test]
    fn test_parse_eval_args_too_few_args() {
        let args = vec![Frame::BulkString(Bytes::from_static(b"return 1"))];
        assert!(parse_eval_args(&args).is_err());
    }

    #[test]
    fn test_parse_eval_args_numkeys_exceeds_args() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"return 1")),
            Frame::BulkString(Bytes::from_static(b"3")),
            Frame::BulkString(Bytes::from_static(b"key1")),
        ];
        assert!(parse_eval_args(&args).is_err());
    }

    #[test]
    fn test_setup_lua_vm() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        // Should have redis table
        let redis: LuaValue = lua.globals().get("redis").unwrap();
        assert!(matches!(redis, LuaValue::Table(_)));

        // Should be sandboxed
        let load: LuaValue = lua.globals().get("load").unwrap();
        assert!(load == LuaValue::Nil);
    }

    // ── moon#506: the shard's Lua footprint must be samplable ──────────────

    #[test]
    fn test_vm_used_memory_no_vm_yet_is_zero() {
        let slot: ShardLuaSlot = Rc::new(RefCell::new(None));
        assert_eq!(vm_used_memory(&slot), Some(0));
    }

    #[test]
    fn test_vm_used_memory_reports_a_real_sandboxed_vm() {
        let slot: ShardLuaSlot = Rc::new(RefCell::new(Some(
            setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap(),
        )));
        let bytes = vm_used_memory(&slot).expect("slot is free to borrow");
        // The number this replaces was 48 -- ScriptCache::resident_bytes() for
        // `return 1` (40-char SHA1 key + 8-byte body). A Lua state carrying
        // setup_sandbox + register_redis_api measures in the tens of KB, so
        // this floor separates "the VM" from "the script text" by a wide
        // margin without pinning an mlua-version-specific constant.
        assert!(
            bytes > 4096,
            "a sandboxed VM with the redis API registered reported {bytes} bytes"
        );
    }

    #[test]
    fn test_vm_used_memory_tracks_lua_allocation() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let slot: ShardLuaSlot = Rc::new(RefCell::new(Some(lua.clone())));
        let before = vm_used_memory(&slot).expect("borrowable");

        // Anchored in _G so the collector cannot reclaim it before the sample.
        lua.load("local t = {} for i = 1, 100000 do t[i] = i end _G.KEEP = t")
            .eval::<()>()
            .unwrap();

        let after = vm_used_memory(&slot).expect("borrowable");
        assert!(
            after > before + 100_000,
            "VM memory went {before} -> {after} across a 100k-entry table"
        );
    }

    #[test]
    fn test_vm_used_memory_declines_to_sample_a_borrowed_slot() {
        // The publish path runs on the shard thread, where a RefCell panic
        // aborts the process. `None` tells the caller to keep the previously
        // published value instead of storing a bogus 0.
        let slot: ShardLuaSlot = Rc::new(RefCell::new(Some(
            setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap(),
        )));
        let _held = slot.borrow_mut();
        assert_eq!(vm_used_memory(&slot), None);
    }

    #[test]
    fn test_run_script_simple() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let mut db = Database::new();

        let result = run_script(&lua, b"return 42", vec![], vec![], &mut db, 0, 1);
        assert!(matches!(result, Frame::Integer(42)));
    }

    #[test]
    fn test_run_script_keys_argv() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let mut db = Database::new();

        let result = run_script(
            &lua,
            b"return KEYS[1]",
            vec![Bytes::from_static(b"mykey")],
            vec![],
            &mut db,
            0,
            1,
        );
        assert!(matches!(result, Frame::BulkString(b) if b == Bytes::from_static(b"mykey")));
    }

    #[test]
    fn test_run_script_with_redis_call() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let mut db = Database::new();

        // SET and GET via redis.call
        let result = run_script(
            &lua,
            b"redis.call('SET', 'testkey', 'testval'); return redis.call('GET', 'testkey')",
            vec![],
            vec![],
            &mut db,
            0,
            1,
        );
        assert!(matches!(result, Frame::BulkString(b) if b == Bytes::from_static(b"testval")));
    }

    #[test]
    fn test_run_script_redis_pcall_catches_error() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let mut db = Database::new();

        // pcall should catch errors as table
        let result = run_script(
            &lua,
            b"local ok, err = pcall(redis.call, 'INVALID_CMD'); return redis.pcall('INVALID_CMD_2')",
            vec![],
            vec![],
            &mut db,
            0,
            1,
        );
        // pcall returns {err = ...} table, which converts to Frame::Error
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_run_script_type_conversions() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let mut db = Database::new();

        // Return string
        let result = run_script(&lua, b"return 'hello'", vec![], vec![], &mut db, 0, 1);
        assert!(matches!(result, Frame::BulkString(b) if b == Bytes::from_static(b"hello")));

        // Return nil
        let result = run_script(&lua, b"return nil", vec![], vec![], &mut db, 0, 1);
        assert!(matches!(result, Frame::Null));

        // Return boolean false -> Null
        let result = run_script(&lua, b"return false", vec![], vec![], &mut db, 0, 1);
        assert!(matches!(result, Frame::Null));

        // Return boolean true -> Integer(1)
        let result = run_script(&lua, b"return true", vec![], vec![], &mut db, 0, 1);
        assert!(matches!(result, Frame::Integer(1)));

        // Return table
        let result = run_script(&lua, b"return {1, 2, 3}", vec![], vec![], &mut db, 0, 1);
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[0], Frame::Integer(1)));
            }
            _ => panic!("Expected Array, got {:?}", result),
        }
    }

    #[test]
    fn test_handle_script_subcommand_load() {
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"LOAD")),
            Frame::BulkString(Bytes::from_static(b"return 1")),
        ];
        let (response, fanout) = handle_script_subcommand(&cache, &args);
        assert!(matches!(response, Frame::BulkString(_)));
        assert!(fanout.is_some());
        let (sha, script) = fanout.unwrap();
        assert_eq!(sha.len(), 40);
        assert_eq!(script, Bytes::from_static(b"return 1"));
    }

    #[test]
    fn test_handle_script_subcommand_exists() {
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        let sha = cache.borrow_mut().load(Bytes::from_static(b"return 1"));
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"EXISTS")),
            Frame::BulkString(Bytes::from(sha)),
            Frame::BulkString(Bytes::from_static(
                b"0000000000000000000000000000000000000000",
            )),
        ];
        let (response, fanout) = handle_script_subcommand(&cache, &args);
        assert!(fanout.is_none());
        match response {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], Frame::Integer(1)));
                assert!(matches!(items[1], Frame::Integer(0)));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_handle_script_subcommand_flush() {
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        cache.borrow_mut().load(Bytes::from_static(b"return 1"));
        assert_eq!(cache.borrow().len(), 1);

        let args = vec![Frame::BulkString(Bytes::from_static(b"FLUSH"))];
        let (response, fanout) = handle_script_subcommand(&cache, &args);
        assert!(matches!(response, Frame::SimpleString(_)));
        assert!(fanout.is_none());
        assert_eq!(cache.borrow().len(), 0);
    }

    #[test]
    fn test_handle_eval_basic() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        let mut db = Database::new();

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"return 42")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];

        let result = handle_eval(&lua, &cache, &args, &mut db, 0, 1, 0, 1);
        assert!(matches!(result, Frame::Integer(42)));
    }

    #[test]
    fn test_handle_evalsha_noscript() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        let mut db = Database::new();

        let args = vec![
            Frame::BulkString(Bytes::from_static(
                b"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            )),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];

        let result = handle_evalsha(&lua, &cache, &args, &mut db, 0, 1, 0, 1);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"NOSCRIPT".as_slice())),
            _ => panic!("Expected NOSCRIPT error"),
        }
    }

    #[test]
    fn test_handle_evalsha_after_eval() {
        let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        let mut db = Database::new();

        // First EVAL caches the script
        let eval_args = vec![
            Frame::BulkString(Bytes::from_static(b"return 99")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];
        let _ = handle_eval(&lua, &cache, &eval_args, &mut db, 0, 1, 0, 1);

        // Get the SHA1
        let sha = sha1_smol::Sha1::from(b"return 99").hexdigest();

        // EVALSHA should work now
        let evalsha_args = vec![
            Frame::BulkString(Bytes::from(sha)),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];
        let result = handle_evalsha(&lua, &cache, &evalsha_args, &mut db, 0, 1, 0, 1);
        assert!(matches!(result, Frame::Integer(99)));
    }
}
