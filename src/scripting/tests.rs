//! `scripting` unit tests, moved verbatim out of `mod.rs` (moon#1226: the
//! module was 1,975 lines, over the 1,500-line rule). No test changed.

use super::*;

/// A RESP simple error may not contain CR or LF. mlua's `Display` carries
/// a multi-line Lua traceback, so every runtime error used to be framed
/// with raw newlines in it — `redis-cli` answered `Bad simple string
/// value` and the client never saw what went wrong. The traceback also
/// named a moon SOURCE PATH, which is an information leak (moon#672).
#[test]
fn a_script_error_is_one_line_and_names_no_moon_source_path() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();
    db.set(
        b"sk",
        crate::storage::Entry::new_string(Bytes::from_static(b"not-a-number")),
    );

    // Four different ways to fail, because the framing bug was in the
    // shared fallback arm and any one of them alone could be a special
    // case: a failing redis.call, an explicit error(), a Lua type error,
    // and an unknown command.
    for body in [
        &b"return redis.call('INCR', KEYS[1])"[..],
        b"error('boom')",
        b"local x = nil return x.y",
        b"return redis.call('DEFINITELYNOTACOMMAND')",
    ] {
        let r = run_script(
            &lua,
            body,
            vec![Bytes::from_static(b"sk")],
            vec![],
            &mut db,
            0,
            1,
            &crate::acl::ScriptAcl::trusted(),
            false,
        );
        let Frame::Error(e) = &r else {
            panic!(
                "expected an error for {}: {r:?}",
                String::from_utf8_lossy(body)
            );
        };
        assert!(
            !e.contains(&b'\n') && !e.contains(&b'\r'),
            "error frame carries a newline, which no RESP simple error may: {:?}",
            String::from_utf8_lossy(e)
        );
        assert!(
            !e.windows(3).any(|w| w == b".rs"),
            "error frame leaks a moon source path: {:?}",
            String::from_utf8_lossy(e)
        );
        // Still says something: an empty or bare `ERR` would satisfy both
        // assertions above and tell the client nothing.
        assert!(
            e.len() > b"ERR ".len(),
            "error frame is empty for {}: {:?}",
            String::from_utf8_lossy(body),
            String::from_utf8_lossy(e)
        );
    }
}

/// A redis error raised by `redis.call` must reach the client with its
/// CODE still first, because that is the only part a client matches on.
/// moon already special-cased NOPERM and BUSY for this reason; every
/// other code (WRONGTYPE, OOM, NOSCRIPT, ...) was buried behind
/// `ERR Error running script: runtime error: `, so a client testing for
/// WRONGTYPE saw a plain ERR and could not tell a type clash from a bug
/// (moon#672).
#[test]
fn a_redis_error_code_survives_the_script_that_raised_it() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();
    // A LIST, so a string command against it is a type clash.
    db.set(b"lk", crate::storage::Entry::new_list());

    let r = run_script(
        &lua,
        b"return redis.call('GET', KEYS[1])",
        vec![Bytes::from_static(b"lk")],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    let Frame::Error(e) = &r else {
        panic!("expected an error: {r:?}")
    };
    assert!(
        e.starts_with(b"WRONGTYPE"),
        "the error code must lead, or no client can match it: {:?}",
        String::from_utf8_lossy(e)
    );

    // The control: a plain Lua error carries NO redis code, and must keep
    // the descriptive wrapper rather than being mistaken for one.
    let r2 = run_script(
        &lua,
        b"error('boom')",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    let Frame::Error(e2) = &r2 else {
        panic!("expected an error: {r2:?}")
    };
    assert!(
        e2.starts_with(b"ERR "),
        "a bare Lua error should still be an ERR: {:?}",
        String::from_utf8_lossy(e2)
    );
    assert!(
        String::from_utf8_lossy(e2).contains("boom"),
        "the message was lost: {:?}",
        String::from_utf8_lossy(e2)
    );
}

/// The chunk is named so a Lua-level error points at `user_script`, the
/// name redis uses, rather than at whatever file mlua defaulted to.
#[test]
fn a_lua_error_names_user_script() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();
    let r = run_script(
        &lua,
        b"error('boom')",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    let Frame::Error(e) = &r else {
        panic!("expected an error: {r:?}")
    };
    let text = String::from_utf8_lossy(e);
    assert!(
        text.contains("user_script"),
        "error should name user_script: {text}"
    );
    assert!(text.contains("boom"), "error lost the message: {text}");
}

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

    let result = run_script(
        &lua,
        b"return 42",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
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
        &crate::acl::ScriptAcl::trusted(),
        false,
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
        &crate::acl::ScriptAcl::trusted(),
        false,
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
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    // pcall returns {err = ...} table, which converts to Frame::Error
    assert!(matches!(result, Frame::Error(_)));
}

#[test]
fn test_run_script_type_conversions() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();

    // Return string
    let result = run_script(
        &lua,
        b"return 'hello'",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(matches!(result, Frame::BulkString(b) if b == Bytes::from_static(b"hello")));

    // Return nil
    let result = run_script(
        &lua,
        b"return nil",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(matches!(result, Frame::Null));

    // Return boolean false -> Null
    let result = run_script(
        &lua,
        b"return false",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(matches!(result, Frame::Null));

    // Return boolean true -> Integer(1)
    let result = run_script(
        &lua,
        b"return true",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(matches!(result, Frame::Integer(1)));

    // Return table
    let result = run_script(
        &lua,
        b"return {1, 2, 3}",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
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
    let Frame::BulkString(sha) = response else {
        panic!("SCRIPT LOAD answers the sha");
    };
    assert_eq!(sha.len(), 40);
    // The replay carries the body and the epoch the local insert used.
    match fanout {
        Some(ScriptFanout::Load { script, epoch }) => {
            assert_eq!(script, Bytes::from_static(b"return 1"));
            assert!(epoch <= order::script_flush_epoch());
        }
        other => panic!("SCRIPT LOAD owes a Load replay, got {other:?}"),
    }
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
    // moon#1229/#1235: an accepted flush owes the other shards a replay,
    // under a fresh epoch.
    assert!(matches!(fanout, Some(ScriptFanout::Flush { epoch }) if epoch > 0));
    assert_eq!(cache.borrow().len(), 0);
}

/// moon#1229: `SCRIPT FLUSH [ASYNC|SYNC]` — the modes flush, anything else
/// is refused with redis 7.0.15's text and leaves the cache (and so the
/// other shards, which are only told about an accepted flush) untouched.
#[test]
fn script_flush_accepts_sync_async_and_refuses_other_modes() {
    let b = |s: &'static [u8]| Frame::BulkString(Bytes::from_static(s));
    for mode in [&b"SYNC"[..], b"async"] {
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        cache.borrow_mut().load(Bytes::from_static(b"return 1"));
        let (response, _) = handle_script_subcommand(&cache, &[b(b"FLUSH"), b(mode)]);
        assert_eq!(response, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(cache.borrow().len(), 0, "{mode:?} flushes");
    }
    for bad in [
        vec![b(b"FLUSH"), b(b"BOGUS")],
        vec![b(b"FLUSH"), b(b"SYNC"), b(b"ASYNC")],
    ] {
        let cache = Rc::new(RefCell::new(ScriptCache::new()));
        cache.borrow_mut().load(Bytes::from_static(b"return 1"));
        let (response, fanout) = handle_script_subcommand(&cache, &bad);
        assert_eq!(
            response,
            Frame::Error(Bytes::from_static(
                b"ERR SCRIPT FLUSH only support SYNC|ASYNC option"
            ))
        );
        assert!(fanout.is_none(), "a refused flush owes nobody anything");
        assert_eq!(cache.borrow().len(), 1, "a refused flush keeps the cache");
    }
}

// -- moon#569: `redis.call` runs under the CALLER's ACL ----------------

fn restricted_acl() -> crate::acl::ScriptAcl {
    let mut t = crate::acl::AclTable::new();
    t.ensure_default_user(None);
    t.apply_setuser("app", &["on", ">pw", "~app:*", "+@all"]);
    crate::acl::ScriptAcl::for_user(&std::sync::Arc::new(parking_lot::RwLock::new(t)), "app")
}

/// The bug: a script that DECLARES no key used to reach any key at all,
/// because the dispatcher's key check only ever saw `numkeys`.
#[test]
fn script_acl_blocks_undeclared_out_of_pattern_key() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();
    let acl = restricted_acl();

    let denied = run_script(
        &lua,
        b"return redis.call('GET', 'secret:x')",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &acl,
        false,
    );
    match denied {
        Frame::Error(e) => assert!(
            e.starts_with(crate::acl::SCRIPT_ACL_DENIED_PREFIX.as_bytes()),
            "want a clean NOPERM, got {:?}",
            String::from_utf8_lossy(&e)
        ),
        other => panic!("undeclared out-of-pattern GET was allowed: {other:?}"),
    }

    // ...and an in-pattern key the script also did not declare is FINE:
    // the pattern gates, not the declaration.
    let allowed = run_script(
        &lua,
        b"redis.call('SET', 'app:k', 'v') return redis.call('GET', 'app:k')",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &acl,
        false,
    );
    assert!(
        matches!(&allowed, Frame::BulkString(b) if b.as_ref() == b"v"),
        "legitimate in-pattern script broke: {allowed:?}"
    );
}

/// The denial must survive every laundering shape Lua offers, and the
/// command must not have executed.
#[test]
fn script_acl_survives_pcall_and_indirection() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();
    let acl = restricted_acl();
    for body in [
        // computed name
        &b"return redis.call('GET', 'sec' .. 'ret:x')"[..],
        // behind a closure
        b"local f = function() return redis.call('SET','secret:x','v') end return f()",
        // movable-key layouts
        b"return redis.call('LMPOP', 1, 'secret:l', 'LEFT')",
        b"return redis.call('SORT', 'app:l', 'STORE', 'secret:d')",
        // runtime-computed weight keys: unnameable, so DENY
        b"return redis.call('SORT', 'app:l', 'BY', 'secret:w_*')",
    ] {
        let r = run_script(&lua, body, vec![], vec![], &mut db, 0, 1, &acl, false);
        assert!(
            matches!(&r, Frame::Error(e) if e.starts_with(b"NOPERM")),
            "not denied: {} -> {r:?}",
            String::from_utf8_lossy(body)
        );
    }
    // redis.pcall hands the script an error table instead of raising --
    // the point is that the COMMAND did not run.
    let r = run_script(
        &lua,
        b"local e = redis.pcall('SET','secret:x','v') return redis.call('EXISTS','app:probe')",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &acl,
        false,
    );
    assert!(matches!(r, Frame::Integer(0)), "unexpected: {r:?}");
}

/// A runner that supplies no identity refuses everything rather than
/// inheriting `~*`. `set_script_db` takes the identity as a REQUIRED
/// argument so this state is only reachable deliberately.
#[test]
fn script_acl_default_is_deny_not_allow() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let mut db = Database::new();
    let r = run_script(
        &lua,
        b"return redis.call('GET', 'anything')",
        vec![],
        vec![],
        &mut db,
        0,
        1,
        &crate::acl::ScriptAcl::deny(),
        false,
    );
    assert!(
        matches!(&r, Frame::Error(e) if e.starts_with(b"NOPERM")),
        "no-identity script was allowed to run: {r:?}"
    );
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

    let result = handle_eval(
        &lua,
        &cache,
        &args,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(matches!(result, Frame::Integer(42)));
}

/// `EVAL_RO` is `EVAL` with one difference that matters: a write inside
/// the script is refused. The read half is the control — without it, a
/// handler that refused *everything* in read-only mode would pass.
#[test]
fn eval_ro_reads_but_refuses_a_write() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();
    db.set(
        b"rk",
        crate::storage::Entry::new_string(Bytes::from_static(b"hello")),
    );

    let read = vec![
        Frame::BulkString(Bytes::from_static(b"return redis.call('GET', KEYS[1])")),
        Frame::BulkString(Bytes::from_static(b"1")),
        Frame::BulkString(Bytes::from_static(b"rk")),
    ];
    let r = handle_eval(
        &lua,
        &cache,
        &read,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        true,
    );
    assert!(
        matches!(&r, Frame::BulkString(v) if v.as_ref() == b"hello"),
        "a read-only script must still be able to READ: {r:?}"
    );

    let write = vec![
        Frame::BulkString(Bytes::from_static(
            b"return redis.call('SET', KEYS[1], 'x')",
        )),
        Frame::BulkString(Bytes::from_static(b"1")),
        Frame::BulkString(Bytes::from_static(b"rk")),
    ];
    let w = handle_eval(
        &lua,
        &cache,
        &write,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        true,
    );
    assert!(
        matches!(&w, Frame::Error(e) if e.windows(11).any(|c| c == b"read-only s")),
        "a write from a read-only script must be refused: {w:?}"
    );
    // ...and refused means NOT APPLIED, not merely reported.
    let after = handle_eval(
        &lua,
        &cache,
        &read,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        true,
    );
    assert!(
        matches!(&after, Frame::BulkString(v) if v.as_ref() == b"hello"),
        "the refused write still landed: {after:?}"
    );
}

/// The read-only flag must not leak into the NEXT script on the same VM.
/// It lives in a thread-local, and a shard thread runs every script for
/// its connections, so a sticky flag would silently turn plain `EVAL`
/// into `EVAL_RO` for the rest of the process.
#[test]
fn the_read_only_flag_does_not_outlive_its_script() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();

    let write = vec![
        Frame::BulkString(Bytes::from_static(
            b"return redis.call('SET', KEYS[1], 'x')",
        )),
        Frame::BulkString(Bytes::from_static(b"1")),
        Frame::BulkString(Bytes::from_static(b"rk")),
    ];
    let _ = handle_eval(
        &lua,
        &cache,
        &write,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        true,
    );
    // Same script, same VM, this time as plain EVAL.
    let w = handle_eval(
        &lua,
        &cache,
        &write,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(
        matches!(&w, Frame::SimpleString(v) if v.as_ref() == b"OK"),
        "the previous script's read-only flag leaked into a plain EVAL: {w:?}"
    );
}

/// The arity error must name what the CLIENT sent. All four names share
/// two handlers, so a handler that hard-codes its own name is wrong for
/// half its callers — which is exactly what `EVAL_RO` hit.
#[test]
fn the_arity_error_names_the_command_the_client_sent() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();
    let one = vec![Frame::BulkString(Bytes::from_static(b"body"))];

    for (read_only, want) in [
        (
            false,
            &b"ERR wrong number of arguments for 'eval' command"[..],
        ),
        (true, b"ERR wrong number of arguments for 'eval_ro' command"),
    ] {
        let r = handle_eval(
            &lua,
            &cache,
            &one,
            &mut db,
            0,
            1,
            0,
            1,
            &crate::acl::ScriptAcl::trusted(),
            read_only,
        );
        assert_eq!(
            r,
            Frame::Error(Bytes::from_static(want)),
            "read_only={read_only}"
        );
    }

    // `EVALSHA <sha>` is short by one argument. redis answers on ARITY,
    // never `NOSCRIPT` — a client told NOSCRIPT re-loads the script and
    // retries the same malformed call forever.
    for (read_only, want) in [
        (
            false,
            &b"ERR wrong number of arguments for 'evalsha' command"[..],
        ),
        (
            true,
            b"ERR wrong number of arguments for 'evalsha_ro' command",
        ),
    ] {
        let r = handle_evalsha(
            &lua,
            &cache,
            &one,
            &mut db,
            0,
            1,
            0,
            1,
            &crate::acl::ScriptAcl::trusted(),
            read_only,
        );
        assert_eq!(
            r,
            Frame::Error(Bytes::from_static(want)),
            "read_only={read_only}"
        );
    }
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

    let result = handle_evalsha(
        &lua,
        &cache,
        &args,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
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
    let _ = handle_eval(
        &lua,
        &cache,
        &eval_args,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );

    // Get the SHA1
    let sha = sha1_smol::Sha1::from(b"return 99").hexdigest();

    // EVALSHA should work now
    let evalsha_args = vec![
        Frame::BulkString(Bytes::from(sha)),
        Frame::BulkString(Bytes::from_static(b"0")),
    ];
    let result = handle_evalsha(
        &lua,
        &cache,
        &evalsha_args,
        &mut db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    );
    assert!(matches!(result, Frame::Integer(99)));
}

// ── moon#1167: compiled-function cache ─────────────────────────────────

fn eval(
    lua: &Rc<Lua>,
    cache: &Rc<RefCell<ScriptCache>>,
    db: &mut Database,
    argv: &[&[u8]],
) -> Frame {
    let args: Vec<Frame> = argv
        .iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect();
    handle_eval(
        lua,
        cache,
        &args,
        db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    )
}

fn evalsha(
    lua: &Rc<Lua>,
    cache: &Rc<RefCell<ScriptCache>>,
    db: &mut Database,
    argv: &[&[u8]],
) -> Frame {
    let args: Vec<Frame> = argv
        .iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect();
    handle_evalsha(
        lua,
        cache,
        &args,
        db,
        0,
        1,
        0,
        1,
        &crate::acl::ScriptAcl::trusted(),
        false,
    )
}

/// The whole point of moon#1167: an EVAL then many EVALSHA of the same body
/// compile the chunk ONCE and reuse a single cached function — the compiled
/// map holds exactly one entry no matter how many times it runs, and the
/// results are correct every time.
#[test]
fn evalsha_reuses_one_compiled_function() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();

    let body = b"return 1 + 1";
    assert!(matches!(
        eval(&lua, &cache, &mut db, &[body, b"0"]),
        Frame::Integer(2)
    ));
    assert_eq!(cache.borrow().compiled_len(), 1);

    let sha = sha1_smol::Sha1::from(body).hexdigest();
    for _ in 0..200 {
        assert!(matches!(
            evalsha(&lua, &cache, &mut db, &[sha.as_bytes(), b"0"]),
            Frame::Integer(2)
        ));
    }
    // Reused, not recompiled into new entries.
    assert_eq!(cache.borrow().compiled_len(), 1);

    // A mixed-case sha resolves to the same single cached function.
    assert!(matches!(
        evalsha(
            &lua,
            &cache,
            &mut db,
            &[sha.to_uppercase().as_bytes(), b"0"]
        ),
        Frame::Integer(2)
    ));
    assert_eq!(cache.borrow().compiled_len(), 1);
}

/// SCRIPT FLUSH must drop compiled functions too — after a flush the sha is
/// gone from BOTH maps and EVALSHA answers NOSCRIPT.
#[test]
fn script_flush_clears_compiled_functions() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();

    let body = b"return 7";
    let _ = eval(&lua, &cache, &mut db, &[body, b"0"]);
    let sha = sha1_smol::Sha1::from(body).hexdigest();
    assert_eq!(cache.borrow().compiled_len(), 1);

    let (_resp, _fanout) =
        handle_script_subcommand(&cache, &[Frame::BulkString(Bytes::from_static(b"FLUSH"))]);
    assert_eq!(cache.borrow().compiled_len(), 0);
    assert_eq!(cache.borrow().len(), 0);

    let after = evalsha(&lua, &cache, &mut db, &[sha.as_bytes(), b"0"]);
    assert!(
        matches!(&after, Frame::Error(e) if e.starts_with(b"NOSCRIPT")),
        "flushed sha must be NOSCRIPT: {after:?}"
    );
}

/// The compiled map is LRU-bounded so a storm of unique EVAL bodies cannot
/// grow the Lua heap without limit. The SOURCE map stays unbounded (Redis
/// parity — SCRIPT EXISTS must keep reporting every loaded sha).
#[test]
fn compiled_cache_is_lru_bounded_but_source_is_not() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();

    let unique = super::cache::compiled_cache_cap_for_test() + 500;
    for i in 0..unique {
        let body = format!("return {i}");
        let _ = eval(&lua, &cache, &mut db, &[body.as_bytes(), b"0"]);
    }
    assert!(
        cache.borrow().compiled_len() <= super::cache::compiled_cache_cap_for_test(),
        "compiled map exceeded its cap: {}",
        cache.borrow().compiled_len()
    );
    // Source map is unbounded.
    assert_eq!(cache.borrow().len(), unique);
}

/// A cached function called repeatedly gets FRESH locals and FRESH
/// KEYS/ARGV every call — no state accumulates across calls, and one call's
/// keys never bleed into the next.
#[test]
fn cached_function_has_fresh_locals_and_keys_argv_per_call() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();

    // A fresh local table every call: #t is 1 each time, never 1,2,3…
    let body = b"local t = {} t[#t + 1] = 1 return #t";
    let sha = sha1_smol::Sha1::from(&body[..]).hexdigest();
    let _ = eval(&lua, &cache, &mut db, &[body, b"0"]);
    for _ in 0..5 {
        assert!(matches!(
            evalsha(&lua, &cache, &mut db, &[sha.as_bytes(), b"0"]),
            Frame::Integer(1)
        ));
    }

    // KEYS/ARGV are per-call: the same cached function returns whatever the
    // current call passed, not the first call's values.
    let kbody = b"return {KEYS[1], ARGV[1]}";
    let ksha = sha1_smol::Sha1::from(&kbody[..]).hexdigest();
    let _ = eval(&lua, &cache, &mut db, &[kbody, b"1", b"k1", b"a1"]);
    let r = evalsha(
        &lua,
        &cache,
        &mut db,
        &[ksha.as_bytes(), b"1", b"k2", b"a2"],
    );
    match r {
        Frame::Array(items) => {
            assert!(matches!(&items[0], Frame::BulkString(b) if b.as_ref() == b"k2"));
            assert!(matches!(&items[1], Frame::BulkString(b) if b.as_ref() == b"a2"));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

/// The cached EVAL path still authorizes every inner `redis.call` against
/// the caller's ACL — caching the chunk changes nothing about the
/// thread-local ACL gate, and a denial is returned on every call.
#[test]
fn cached_eval_path_still_enforces_acl() {
    let lua = setup_lua_vm(bridge::LuaEvictionCtx::disabled()).unwrap();
    let cache = Rc::new(RefCell::new(ScriptCache::new()));
    let mut db = Database::new();
    let acl = restricted_acl();

    let args: Vec<Frame> = [&b"return redis.call('GET', 'secret:x')"[..], b"0"]
        .iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect();
    // Twice: a cache hit on the second call must be denied identically.
    for _ in 0..2 {
        let r = handle_eval(&lua, &cache, &args, &mut db, 0, 1, 0, 1, &acl, false);
        assert!(
            matches!(&r, Frame::Error(e) if e.starts_with(b"NOPERM")),
            "cached script bypassed ACL: {r:?}"
        );
    }
}
