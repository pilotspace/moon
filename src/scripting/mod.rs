pub mod bridge;
pub mod cache;
pub mod functions;
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

    // Cache the script (idempotent -- duplicates are no-ops)
    cache.borrow_mut().load(script.clone());

    run_script(
        lua,
        script.as_ref(),
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

    run_script(
        lua,
        script.as_ref(),
        keys,
        argv,
        db,
        selected_db,
        db_count,
        acl,
        read_only,
    )
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

        // Load and execute. The chunk is NAMED: without it mlua defaults the
        // chunk name to this Rust file and line, so every Lua-level error and
        // traceback quoted a moon source path back at the client (moon#672).
        // `user_script` is the name redis uses, so error text that mentions it
        // reads the same to a client either way.
        let val: LuaValue = lua.load(script).set_name("@user_script").eval()?;
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
mod tests {
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

    // -- moon#569: `redis.call` runs under the CALLER's ACL ----------------

    fn restricted_acl() -> crate::acl::ScriptAcl {
        let mut t = crate::acl::AclTable::new();
        t.ensure_default_user(None);
        t.apply_setuser("app", &["on", ">pw", "~app:*", "+@all"]);
        crate::acl::ScriptAcl::for_user(&std::sync::Arc::new(std::sync::RwLock::new(t)), "app")
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
}
