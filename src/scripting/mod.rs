pub mod bridge;
pub mod cache;
pub mod sandbox;
pub mod types;

pub use cache::ScriptCache;

use bytes::Bytes;
use mlua::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use crate::protocol::Frame;
use crate::storage::Database;

/// Create and return a fully sandboxed Lua 5.4 VM with redis.* API registered.
/// Must be called on the shard thread (Lua is !Send).
pub fn setup_lua_vm() -> mlua::Result<Rc<Lua>> {
    let lua = Rc::new(Lua::new());
    sandbox::setup_sandbox(&lua)?;
    sandbox::register_redis_api(&lua)?;
    Ok(lua)
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

/// Validate that all keys hash to the current shard. Returns Some(error) on violation.
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
        let lua = setup_lua_vm().unwrap();
        // Should have redis table
        let redis: LuaValue = lua.globals().get("redis").unwrap();
        assert!(matches!(redis, LuaValue::Table(_)));

        // Should be sandboxed
        let load: LuaValue = lua.globals().get("load").unwrap();
        assert!(load == LuaValue::Nil);
    }

    #[test]
    fn test_run_script_simple() {
        let lua = setup_lua_vm().unwrap();
        let mut db = Database::new();

        let result = run_script(&lua, b"return 42", vec![], vec![], &mut db, 0, 1);
        assert!(matches!(result, Frame::Integer(42)));
    }

    #[test]
    fn test_run_script_keys_argv() {
        let lua = setup_lua_vm().unwrap();
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
        let lua = setup_lua_vm().unwrap();
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
        let lua = setup_lua_vm().unwrap();
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
        let lua = setup_lua_vm().unwrap();
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
        let lua = setup_lua_vm().unwrap();
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
        let lua = setup_lua_vm().unwrap();
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
        let lua = setup_lua_vm().unwrap();
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
