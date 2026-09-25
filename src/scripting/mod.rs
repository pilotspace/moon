pub mod bridge;
pub mod cache;
pub mod functions;
pub mod order;
pub mod pending_flush;
pub mod sandbox;
pub mod types;

pub use cache::ScriptCache;
pub use functions::{
    FunctionRegistry, FunctionRegistryOp, apply_registry_op, shard_function_registry,
};

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

/// The arity error names the command the CLIENT sent, which for the `_RO`
/// twins is not the name of the handler they share. Measured against redis
/// 8.6.1: `EVAL_RO body` answers `...for 'eval_ro' command`, and `EVALSHA sha`
/// answers an arity error rather than `NOSCRIPT` — redis checks arity (-3)
/// before it looks a sha up (moon#636).
fn eval_arity_error(sha_form: bool, read_only: bool) -> Frame {
    Frame::Error(Bytes::from_static(match (sha_form, read_only) {
        (false, false) => b"ERR wrong number of arguments for 'eval' command",
        (false, true) => b"ERR wrong number of arguments for 'eval_ro' command",
        (true, false) => b"ERR wrong number of arguments for 'evalsha' command",
        (true, true) => b"ERR wrong number of arguments for 'evalsha_ro' command",
    }))
}

/// Handle the EVAL Redis command: parse args, validate keys, cache script, run.
#[allow(clippy::too_many_arguments)]
pub fn handle_eval(
    lua: &Rc<Lua>,
    cache: &Rc<RefCell<ScriptCache>>,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
    acl: &crate::acl::ScriptAcl,
    // `true` for `EVAL_RO`: any write attempted by the script body is refused
    // at the first `redis.call`, not merely reported afterwards.
    read_only: bool,
) -> Frame {
    // Arity BEFORE parsing: `parse_eval_args` is shared with `EVALSHA` and the
    // routing helpers, so it cannot know which name to put in the error.
    if args.len() < 2 {
        return eval_arity_error(false, read_only);
    }
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

    // moon#1167: cache the source (idempotent) AND get-or-compile the function,
    // computing the sha exactly once. A compile error surfaces here with the
    // same frame `run_script` produced on HEAD.
    //
    // moon#1235: at `--shards > 1` the body is cached by the ORIGIN's fan-out
    // (`eval_script_fanout` claims it locally and replays it, one tagged
    // insert, before the script is routed or run). An insert here would be a
    // second, shard-local insert under a later epoch, which a `SCRIPT FLUSH`
    // landing between the two would leave on this shard only.
    let func = match ensure_compiled_eval(lua, cache, &script, num_shards <= 1) {
        Ok(func) => func,
        Err(frame) => return frame,
    };

    run_compiled(
        lua,
        &func,
        keys,
        argv,
        db,
        selected_db,
        db_count,
        acl,
        read_only,
    )
}

/// Handle the EVALSHA Redis command: look up cached script by SHA1, then run.
#[allow(clippy::too_many_arguments)]
pub fn handle_evalsha(
    lua: &Rc<Lua>,
    cache: &Rc<RefCell<ScriptCache>>,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
    acl: &crate::acl::ScriptAcl,
    // `true` for `EVALSHA_RO` — see [`handle_eval`].
    read_only: bool,
) -> Frame {
    // `EVALSHA <sha>` with no numkeys is an ARITY error, not `NOSCRIPT`:
    // redis rejects on arity (-3) before it ever looks the sha up, and a
    // client that sees NOSCRIPT will pointlessly re-`SCRIPT LOAD` and retry.
    if args.len() < 2 {
        return eval_arity_error(true, read_only);
    }

    // Extract the SHA1 first argument, lowercased into a stack `[u8; 40]` — no
    // per-call `String` allocation (moon#1167).
    let sha_arg = match &args[0] {
        Frame::BulkString(b) => b,
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid SHA1 hex string"));
        }
    };
    // A sha that is not 40 hex chars can never match a cached body (every key
    // is a 40-char lowercase hex digest), so it is `NOSCRIPT` exactly as the
    // old `to_lowercase()` + map lookup produced.
    let mut key = [0u8; 40];
    let matched = sha_arg.len() == 40 && {
        for (dst, &c) in key.iter_mut().zip(sha_arg.iter()) {
            *dst = c.to_ascii_lowercase();
        }
        true
    };

    // Look up the cached SOURCE (drives NOSCRIPT, unchanged) via a zero-copy
    // `&str` view of the stack buffer — non-ASCII lowercased bytes cannot be a
    // hex sha, so they fall through to NOSCRIPT.
    let script = match matched
        .then(|| std::str::from_utf8(&key).ok())
        .flatten()
        .and_then(|s| cache.borrow().get(s).cloned())
    {
        Some(s) => s,
        None => {
            return Frame::Error(Bytes::from_static(
                b"NOSCRIPT No matching script. Please use EVAL.",
            ));
        }
    };

    // Parse numkeys/keys/argv straight from the tail — no synthetic `eval_args`
    // Vec, no clone of the body into arg position (moon#1167).
    let (_numkeys, keys, argv) = match parse_numkeys_keys_argv(&args[1..]) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    // Validate cross-shard keys
    if num_shards > 1 {
        if let Some(err) = validate_keys_same_shard(&keys, shard_id, num_shards) {
            return err;
        }
    }

    // Get-or-compile the cached function (moon#1167). The body came from a
    // successful EVAL/SCRIPT LOAD so it normally compiles; a compile error is
    // still surfaced with the same frame HEAD produced.
    let func = {
        let cached = cache.borrow_mut().get_compiled(&key);
        match cached {
            Some(func) => func,
            None => match compile_user_script(lua, &script) {
                Ok(func) => {
                    cache.borrow_mut().store_compiled(key, func.clone());
                    func
                }
                Err(e) => return script_error_to_frame(e),
            },
        }
    };

    run_compiled(
        lua,
        &func,
        keys,
        argv,
        db,
        selected_db,
        db_count,
        acl,
        read_only,
    )
}

/// The replay a `SCRIPT` subcommand owes the other shards, with the flush
/// epoch that places it in the order every shard agrees on (moon#1235, see
/// [`order`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptFanout {
    /// `SCRIPT LOAD` of `script`, inserted under flush epoch `epoch`.
    Load { script: Bytes, epoch: u64 },
    /// `SCRIPT FLUSH`, issued as flush epoch `epoch`.
    Flush { epoch: u64 },
}

/// Handle SCRIPT subcommands (LOAD, EXISTS, FLUSH).
///
/// Returns the reply and, for a LOAD or FLUSH this shard accepted, the replay
/// the other shards are owed. A refused subcommand owes nothing.
pub fn handle_script_subcommand(
    cache: &Rc<RefCell<ScriptCache>>,
    args: &[Frame],
) -> (Frame, Option<ScriptFanout>) {
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

    if let Some(help) = crate::command::help_text::help_if_requested("SCRIPT", &sub) {
        return (help, None);
    }

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
        // Tagged with the flush epoch current now; the fan-out carries the
        // same tag so every shard files this as one insert (moon#1235).
        let epoch = order::script_flush_epoch();
        let sha = cache.borrow_mut().load_at(script.clone(), epoch);
        (
            Frame::BulkString(Bytes::from(sha)),
            Some(ScriptFanout::Load { script, epoch }),
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
        // `SCRIPT FLUSH [ASYNC|SYNC]`: anything else is refused BEFORE the
        // cache is touched, with redis's exact text (moon#1229 — a refused
        // flush must not be fanned out to the other shards either; the caller
        // keys the fan-out on a non-error reply).
        let mode_ok = match &args[1..] {
            [] => true,
            [Frame::BulkString(m)] => {
                m.eq_ignore_ascii_case(b"SYNC") || m.eq_ignore_ascii_case(b"ASYNC")
            }
            _ => false,
        };
        if !mode_ok {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR SCRIPT FLUSH only support SYNC|ASYNC option",
                )),
                None,
            );
        }
        let epoch = cache.borrow_mut().flush();
        (
            Frame::SimpleString(Bytes::from_static(b"OK")),
            Some(ScriptFanout::Flush { epoch }),
        )
    } else {
        (
            crate::command::helpers::err_unknown_subcommand("SCRIPT", &sub),
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

    /// The shard's OOM/eviction gate, as needed to build this shard's
    /// [`FunctionRegistry`] on the SPSC drain path (moon#514).
    ///
    /// The drain loop has no `ConnectionContext` to call
    /// `build_lua_eviction_ctx()` on, and a fan-out'd `FUNCTION LOAD` — or an
    /// `FCALL` routed here because this shard owns the key — must be able to
    /// materialise the registry with the same write gate a local connection
    /// would have given it.
    pub fn eviction_ctx(&self) -> &bridge::LuaEvictionCtx {
        &self.eviction_ctx
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
    let (numkeys, keys, argv) = parse_numkeys_keys_argv(&args[1..])?;
    Ok((script, numkeys, keys, argv))
}

/// Parse the tail of an EVAL/EVALSHA — everything AFTER the script/sha — into
/// (numkeys, keys, argv).
///
/// Split out (moon#1167) so `EVALSHA` can parse straight from `args[1..]`
/// instead of building a synthetic `[script, args[1..]]` `Vec` per call. The
/// error texts are command-agnostic (they never name `eval` vs `evalsha`), so
/// both callers stay byte-identical to the pre-split behaviour.
fn parse_numkeys_keys_argv(after: &[Frame]) -> Result<(usize, Vec<Bytes>, Vec<Bytes>), Frame> {
    let Some(numkeys_frame) = after.first() else {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'eval' command",
        )));
    };
    let numkeys: usize = match numkeys_frame {
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
    if after.len() < 1 + numkeys {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Number of keys can't be greater than number of args",
        )));
    }
    let keys: Vec<Bytes> = after[1..1 + numkeys]
        .iter()
        .filter_map(|f| match f {
            Frame::BulkString(b) => Some(b.clone()),
            _ => None,
        })
        .collect();
    let argv: Vec<Bytes> = after[1 + numkeys..]
        .iter()
        .filter_map(|f| match f {
            Frame::BulkString(b) => Some(b.clone()),
            _ => None,
        })
        .collect();
    Ok((numkeys, keys, argv))
}

/// The 40 lowercase-hex characters of a sha, as a stack `[u8; 40]` (moon#1167).
///
/// Lets the EVAL path reuse a single computed digest as the compiled-cache key
/// without re-hashing or re-allocating.
fn sha_str_to_key(sha: &str) -> [u8; 40] {
    let mut key = [0u8; 40];
    let bytes = sha.as_bytes();
    let n = bytes.len().min(40);
    key[..n].copy_from_slice(&bytes[..n]);
    key
}

/// Compile a user script into a callable `Function`, cached per shard
/// (moon#1167).
///
/// Replicates `mlua::Chunk::eval`'s mode choice EXACTLY so a cached function is
/// byte-for-byte equivalent to what `.eval()` produced on HEAD: try the source
/// as an EXPRESSION first (`"return " + src`), and only if that fails to
/// compile fall back to loading it as a STATEMENT. `eval()` makes this decision
/// at compile time (never from runtime results), so making it once and caching
/// the resulting `Function` changes no observable behaviour — including the
/// cases where HEAD is more lenient than redis (e.g. `EVAL "1+1" 0` -> 2).
///
/// The chunk is named `@user_script` on both attempts, so a Lua-level error and
/// its traceback quote `user_script`, never a moon source path (moon#672).
fn compile_user_script(lua: &Lua, src: &[u8]) -> mlua::Result<LuaFunction> {
    let mut expr = Vec::with_capacity(b"return ".len() + src.len());
    expr.extend_from_slice(b"return ");
    expr.extend_from_slice(src);
    if let Ok(func) = lua.load(&expr).set_name("@user_script").into_function() {
        return Ok(func);
    }
    lua.load(src).set_name("@user_script").into_function()
}

/// Ensure the EVAL body is cached (source + compiled) and return the compiled
/// function, computing the sha exactly ONCE (moon#1167).
///
/// On a compile error the source is still cached — HEAD's `handle_eval` cached
/// the body before `run_script` ran, so `SCRIPT EXISTS` of an invalid body is
/// `1` on HEAD; that is preserved here rather than tightened.
fn ensure_compiled_eval(
    lua: &Lua,
    cache: &Rc<RefCell<ScriptCache>>,
    script: &Bytes,
    store_source: bool,
) -> Result<LuaFunction, Frame> {
    let sha = sha1_smol::Sha1::from(&script[..]).hexdigest();
    let key = sha_str_to_key(&sha);
    {
        let mut c = cache.borrow_mut();
        if store_source {
            c.load_precomputed(sha, script.clone());
        }
        if let Some(func) = c.get_compiled(&key) {
            return Ok(func);
        }
    }
    match compile_user_script(lua, script) {
        Ok(func) => {
            cache.borrow_mut().store_compiled(key, func.clone());
            Ok(func)
        }
        Err(e) => Err(script_error_to_frame(e)),
    }
}

/// Execute a Lua script SOURCE with the given keys/argv, returning a Frame
/// result.
///
/// Compiles the body (uncached — used by unit tests and any caller that has no
/// per-shard cache) and runs it. Production EVAL/EVALSHA go through the cached
/// path (`ensure_compiled_eval` / [`run_compiled`]); the compile here uses the
/// SAME [`compile_user_script`] so behaviour is identical.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn run_script(
    lua: &Lua,
    script: &[u8],
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    db: &mut Database,
    selected_db: usize,
    db_count: usize,
    acl: &crate::acl::ScriptAcl,
    read_only: bool,
) -> Frame {
    match compile_user_script(lua, script) {
        Ok(func) => run_compiled(
            lua,
            &func,
            keys,
            argv,
            db,
            selected_db,
            db_count,
            acl,
            read_only,
        ),
        Err(e) => script_error_to_frame(e),
    }
}

/// Execute an ALREADY-COMPILED Lua function with the given keys/argv, returning
/// a Frame result (moon#1167).
///
/// Sets up the thread-local DB pointer, installs the timeout hook, populates
/// KEYS and ARGV globals (1-indexed) FRESH per call, calls the function, and
/// cleans up. Calling a cached `Function` gives a fresh activation record every
/// time — its locals are re-initialised per call and never leak between calls
/// or between scripts.
#[allow(clippy::too_many_arguments)]
fn run_compiled(
    lua: &Lua,
    func: &LuaFunction,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    db: &mut Database,
    selected_db: usize,
    db_count: usize,
    acl: &crate::acl::ScriptAcl,
    read_only: bool,
) -> Frame {
    // Set thread-local DB pointer + caller identity for the redis.call/pcall
    // bridge. `acl` is what every inner command is authorized against
    // (moon#569) — the script body itself is never trusted to declare what it
    // will touch.
    bridge::set_script_db(db, selected_db, db_count, acl);
    // `EVAL_RO`/`EVALSHA_RO`. Armed AFTER `set_script_db`, which clears the
    // flag as part of installing a fresh script context, and disarmed by
    // `clear_script_db` on every exit path below — the flag is a thread-local
    // and one shard thread runs every script for its connections, so a sticky
    // `true` would silently turn later plain `EVAL`s into `EVAL_RO`.
    bridge::set_script_read_only(read_only);

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

        // Call the compiled chunk. A Lua chunk is a function over the shared
        // globals, so calling the cached function is identical to loading and
        // running the source — only the lexer/parser/codegen are skipped
        // (moon#1167). The chunk was named `@user_script` at compile time, so
        // errors and tracebacks still quote `user_script`, not a moon source
        // path (moon#672).
        let val: LuaValue = func.call(())?;
        types::lua_value_to_frame(lua, &val)
    })();

    // ALWAYS clean up -- both success and error paths (Pitfall 3)
    sandbox::remove_timeout_hook(lua);
    bridge::clear_script_db();

    match result {
        Ok(frame) => frame,
        Err(e) => script_error_to_frame(e),
    }
}

/// Turn a script-execution failure into the wire error the client sees.
///
/// Shared with `FunctionRegistry::call_function` so EVAL and FCALL answer an
/// ACL denial identically. A `redis.call` denial is raised as an `mlua`
/// `RuntimeError` carrying [`crate::acl::SCRIPT_ACL_DENIED_PREFIX`]; without
/// this arm it would reach the client re-wrapped as
/// `ERR Error running script: runtime error: NOPERM ...`, which no client
/// matches on. Answering with the bare `-NOPERM ...` keeps script denials
/// indistinguishable from dispatch-level denials.
pub(crate) fn script_error_to_frame(e: mlua::Error) -> Frame {
    let msg = match &e {
        mlua::Error::RuntimeError(msg) => msg.clone(),
        other => other.to_string(),
    };
    if msg.contains("ERR Lua script timeout") {
        return Frame::Error(Bytes::from_static(b"BUSY Lua script timeout exceeded"));
    }
    if let Some(at) = msg.find(crate::acl::SCRIPT_ACL_DENIED_PREFIX) {
        // Trim mlua's trailing traceback so the reply is a single line.
        let tail = &msg[at..];
        let end = tail.find('\n').unwrap_or(tail.len());
        return Frame::Error(Bytes::from(tail[..end].trim_end().to_string()));
    }
    if msg.contains("Write commands are not allowed") {
        return Frame::Error(Bytes::from_static(
            b"ERR Write commands are not allowed from read-only scripts",
        ));
    }
    // A redis error raised by `redis.call` reaches the client with its CODE
    // still first. That code is the only part a client matches on, and moon
    // already special-cased NOPERM and BUSY above for exactly this reason —
    // every other code (WRONGTYPE, OOM, NOSCRIPT, ...) was buried behind the
    // wrapper, so a client testing for WRONGTYPE saw a plain ERR and could
    // not tell a type clash from a bug (moon#672). `msg` rather than `e`:
    // for a RuntimeError, `e.to_string()` prepends mlua's "runtime error: ".
    let head = strip_mlua_decoration(first_line(&msg));
    if starts_with_error_code(&head) {
        return Frame::Error(Bytes::from(head));
    }
    Frame::Error(Bytes::from(format!("ERR Error running script: {head}")))
}

/// Whether `msg` opens with a redis error code — an all-uppercase ASCII word
/// of three or more letters followed by a space, which is redis's own
/// convention (`ERR`, `WRONGTYPE`, `OOM`, `NOSCRIPT`, `CROSSSLOT`, ...).
///
/// Shape rather than an allowlist, so a code moon adds later is carried
/// through without anyone having to remember to extend a list — the failure
/// mode of an allowlist here is silent, and it is the client that pays.
/// Peel mlua's own wrapper words off the front of a message.
///
/// A failure raised inside a `redis.call` callback reaches us as
/// `runtime error: WRONGTYPE ...` — mlua's decoration, not the script's and
/// not redis's. Peeling it is what lets the redis error CODE land first, and
/// it is done in a loop because the wrappers nest when a callback raises
/// through another callback.
fn strip_mlua_decoration(msg: String) -> String {
    const WRAPPERS: [&str; 3] = ["runtime error: ", "callback error: ", "error: "];
    let mut out = msg.trim_start().to_string();
    loop {
        let Some(w) = WRAPPERS.iter().find(|w| out.starts_with(**w)) else {
            return out;
        };
        out = out[w.len()..].trim_start().to_string();
    }
}

fn starts_with_error_code(msg: &str) -> bool {
    let Some((word, _rest)) = msg.split_once(' ') else {
        return false;
    };
    word.len() >= 3 && word.bytes().all(|b| b.is_ascii_uppercase())
}

/// The first line of a script error, with any stray control bytes flattened.
///
/// mlua's `Display` appends a multi-line Lua traceback. A RESP **simple**
/// error frame is terminated by the first CRLF and may not contain CR or LF
/// anywhere else, so passing that through produced a frame no client could
/// parse — `redis-cli` answered `Bad simple string value` and the client never
/// saw the error at all (moon#672). Taking the first line matches what the
/// `NOPERM` arm above has always done; the traceback's remaining frames say
/// nothing a client can act on.
pub(crate) fn first_line(msg: &str) -> String {
    let head = msg.split(['\n', '\r']).next().unwrap_or("").trim_end();
    // Belt and braces: a tab or other control byte is legal in a RESP error
    // but renders as noise, and `\0` would truncate for a C client.
    head.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .trim_end()
        .to_string()
}

#[cfg(test)]
mod tests;
