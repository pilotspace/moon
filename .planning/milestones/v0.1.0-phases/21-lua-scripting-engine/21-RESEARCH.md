# Phase 21: Lua Scripting Engine - Research

**Researched:** 2026-03-25
**Domain:** Lua VM embedding (mlua), EVAL/EVALSHA Redis commands, script caching, sandboxing, type conversion
**Confidence:** HIGH

<!-- NOTE: This document uses "EVAL" to refer to the Redis EVAL command, not JavaScript eval() -->

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Use `mlua` crate (actively maintained, supports Lua 5.4, safe Rust bindings)
- One Lua VM instance per shard (matches thread-per-core -- no sharing, no locks)
- VM pre-initialized with Redis API functions on shard startup
- Lua 5.4 over 5.1 for: integers, bitwise ops, goto, generational GC -- matches Redis 7.4+ direction
- `EVAL script numkeys key [key ...] arg [arg ...]`
- Script text hashed with SHA1; cached in per-shard script cache
- EVALSHA sha1 numkeys key [key ...] arg [arg ...] -- execute cached script by hash
- NOSCRIPT error if SHA1 not in cache -- client must fall back to EVAL
- SCRIPT LOAD -- cache script without executing, return SHA1
- SCRIPT EXISTS sha1 [sha1 ...] -- check which scripts are cached
- SCRIPT FLUSH [ASYNC|SYNC] -- clear script cache
- Script cache is per-shard (consistent with shared-nothing architecture)
- Scripts replicated to replicas via EVALSHA in replication stream
- redis.call(command, ...) -- execute Redis command, propagate errors
- redis.pcall(command, ...) -- execute Redis command, catch errors as Lua table
- redis.log(level, message) -- write to server log
- redis.error_reply(msg) / redis.status_reply(msg) -- return Redis error/status
- KEYS table and ARGV table populated from command arguments
- Return value conversion: Lua table -> RESP Array, Lua number -> RESP Integer, Lua string -> RESP BulkString, nil -> RESP Null
- Disabled: os.execute, io.*, loadfile, dofile, require, debug.*
- Allowed: string.*, table.*, math.*, tonumber, tostring, type, error, pcall, xpcall, select, unpack
- redis.call/redis.pcall execute within the same shard context -- keys must be declared upfront
- Memory limit per script execution (configurable, default 256MB of Lua heap)
- Scripts execute atomically within a shard -- no other commands interleave on that shard
- Script timeout: configurable (default 5 seconds), monitored by shard event loop
- SCRIPT KILL -- terminate long-running script (only if no writes performed)
- If script has performed writes: can only be killed via SHUTDOWN NOSAVE
- All KEYS declared in EVAL must hash to the same shard (enforced)
- If keys span multiple shards: return CROSSSLOT error
- This matches Redis Cluster behavior -- scripts must use hash tags for multi-key operations

### Claude's Discretion
- Whether to pre-compile scripts to bytecode for faster re-execution
- Script debug mode (step-through debugging)
- redis.sha1hex() helper function
- Whether to implement redis.replicate_commands() (already default in Redis 7+)

### Deferred Ideas (OUT OF SCOPE)
- Redis Functions (FUNCTION LOAD/CALL) -- even more complex than EVAL, defer indefinitely
- Script debug mode -- low priority, defer
- Library loading in scripts -- security risk, defer
</user_constraints>

---

## Summary

Phase 21 embeds Lua 5.4 into each shard via the `mlua` crate (v0.11.6). The shard architecture is single-threaded per-core (`Rc<RefCell<...>>`), which is a perfect fit for mlua's default `!Send` `Lua` struct -- no locking required. Each shard will own one `Lua` VM instance stored as `Rc<Lua>` alongside its databases.

The EVAL/EVALSHA command pair is intercepted at the sharded connection handler level (same pattern as CLUSTER, MULTI/EXEC, REPLICAOF) before the normal dispatch loop. Script text is SHA1-hashed using `sha1_smol` (minimal, zero dependencies) and stored in a per-shard `HashMap<String, Bytes>` script cache (hex SHA1 -> script source). The redis.call/redis.pcall bindings call the existing `dispatch()` function directly with a constructed `Frame::Array`, making the Lua-to-Redis bridge a thin adapter.

Script atomicity is provided free by the shard's single-threaded execution model -- no other commands run while the Lua coroutine executes synchronously. Timeout enforcement uses mlua's `set_hook` with `HookTriggers { every_nth_instruction: Some(100000), .. }` -- the hook checks wall-clock elapsed time and errors if the 5-second limit is exceeded.

**Primary recommendation:** Store `lua_vm: Rc<Lua>` and `script_cache: Rc<RefCell<ScriptCache>>` initialized in `Shard::run()` after wrapping databases, then clone both into each spawned connection task. Intercept EVAL/EVALSHA/SCRIPT in the sharded connection handler before normal dispatch.

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| mlua | 0.11.6 | Lua 5.4 VM embedding | Actively maintained; safe bindings; `!Send` fits single-threaded shards |
| sha1_smol | 1.0.1 | SHA1 digest for script caching | Zero dependencies; simple API: `Sha1::from(script).hexdigest()` |

### Cargo.toml additions
```toml
mlua = { version = "0.11", features = ["lua54", "vendored"] }
sha1_smol = "1.0"
```

**Why `vendored`:** Compiles Lua 5.4 from source bundled with mlua. Avoids system Lua version conflicts and ensures reproducible builds.

**Why `lua54`:** Enables Lua 5.4 with integers, bitwise operators, generational GC, `goto`. Required -- exactly one Lua version feature must be enabled.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| sha1_smol | sha1 (RustCrypto 0.10.6) | RustCrypto needs digest trait machinery; sha1_smol has simpler API |
| mlua vendored | system Lua | System Lua version variability across environments |

---

## Architecture Patterns

### Recommended Project Structure
```
src/
+-- scripting/
|   +-- mod.rs          # ScriptEngine: run_script(), handle_eval(), handle_evalsha()
|   +-- sandbox.rs      # Lua VM setup: disable globals, register redis.* API
|   +-- bridge.rs       # redis.call/redis.pcall -> dispatch() adapter
|   +-- cache.rs        # ScriptCache: HashMap<String, Bytes> + SHA1 helpers
|   +-- types.rs        # lua_to_frame(), frame_to_lua() type conversion
+-- shard/
|   +-- mod.rs          # Add: lua_vm: Rc<Lua>, script_cache: Rc<RefCell<ScriptCache>>
+-- command/
|   +-- scripting.rs    # Argument parsing for EVAL/EVALSHA/SCRIPT commands
+-- server/
    +-- connection.rs   # EVAL/EVALSHA/SCRIPT intercept (before dispatch loop)
```

### Pattern 1: Per-Shard Lua VM Ownership

**What:** Each shard owns its `Lua` VM. Not shared, not locked -- just owned by the shard thread.

**When to use:** Always. This is the only safe pattern for the shared-nothing architecture.

**Key property:** `mlua::Lua` is `!Send` by default (uses `Rc` internally). This is CORRECT for the per-shard model -- the Lua VM never crosses thread boundaries.

```rust
// In Shard::run(), after wrapping databases in Rc<RefCell>:
use mlua::prelude::*;

let lua = Rc::new(Lua::new()); // loads safe stdlib subset only
crate::scripting::sandbox::setup_sandbox(&lua)?;

let script_cache = Rc::new(RefCell::new(crate::scripting::cache::ScriptCache::new()));

// Clone into connection tasks (Rc::clone is cheap):
let lua_task = lua.clone();
let cache_task = script_cache.clone();
tokio::task::spawn_local(handle_connection_sharded(
    stream, databases.clone(), ..., lua_task, cache_task, ...
));
```

### Pattern 2: EVAL Command Intercept in Sharded Handler

**What:** EVAL/EVALSHA/SCRIPT intercepted in `handle_connection_sharded()` before the normal dispatch loop.

**Intercept location:** After the CLUSTER routing block, before MULTI/EXEC handling. Same position as REPLICAOF intercept.

```rust
// In handle_connection_sharded():
if cmd.eq_ignore_ascii_case(b"EVAL") || cmd.eq_ignore_ascii_case(b"EVALSHA") {
    let mut dbs = databases.borrow_mut();
    let db = &mut dbs[selected_db];
    let response = crate::scripting::handle_eval(
        &lua_rc,
        &script_cache_rc,
        cmd,
        cmd_args,
        db,
        shard_id,
        num_shards,
        selected_db,
        dbs.len(),
    );
    drop(dbs);
    responses.push(response);
    continue;
}
if cmd.eq_ignore_ascii_case(b"SCRIPT") {
    // SCRIPT LOAD fans out to all shards; SCRIPT EXISTS/FLUSH are local
    let response = crate::scripting::handle_script(
        &script_cache_rc,
        cmd_args,
        &dispatch_tx, // for SCRIPT LOAD fan-out
        shard_id,
    );
    responses.push(response);
    continue;
}
```

### Pattern 3: redis.call Bridge via dispatch()

**What:** The Lua `redis.call` binding constructs a `Frame::Array` and calls the existing `dispatch()` function.

**How to pass mutable Database into the Lua closure:** Use `std::cell::Cell<*mut Database>` -- safe because everything is single-threaded.

```rust
// In scripting/bridge.rs:
use std::cell::Cell;
use crate::command::{dispatch, DispatchResult};
use crate::protocol::Frame;

thread_local! {
    // Pointer to current database, set before script execution
    static CURRENT_DB: Cell<*mut ()> = Cell::new(std::ptr::null_mut());
    static CURRENT_DB_IDX: Cell<usize> = Cell::new(0);
    static CURRENT_DB_COUNT: Cell<usize> = Cell::new(1);
    static SCRIPT_HAD_WRITE: Cell<bool> = Cell::new(false);
}

pub fn set_script_db(db: &mut crate::storage::Database, db_idx: usize, db_count: usize) {
    CURRENT_DB.with(|c| c.set(db as *mut _ as *mut ()));
    CURRENT_DB_IDX.with(|c| c.set(db_idx));
    CURRENT_DB_COUNT.with(|c| c.set(db_count));
    SCRIPT_HAD_WRITE.with(|c| c.set(false));
}

pub fn clear_script_db() {
    CURRENT_DB.with(|c| c.set(std::ptr::null_mut()));
}

pub fn script_had_write() -> bool {
    SCRIPT_HAD_WRITE.with(|c| c.get())
}

pub fn make_redis_call_fn(lua: &Lua, propagate_errors: bool) -> mlua::Result<mlua::Function> {
    lua.create_function(move |lua, args: mlua::MultiValue| {
        let frames: Vec<Frame> = args.iter()
            .map(|v| crate::scripting::types::lua_value_to_frame(lua, v))
            .collect::<mlua::Result<_>>()?;

        if frames.is_empty() {
            return Err(mlua::Error::runtime(
                "Please specify at least one argument for redis.call()"
            ));
        }

        let cmd_bytes = match &frames[0] {
            Frame::BulkString(b) | Frame::SimpleString(b) => b.clone(),
            _ => return Err(mlua::Error::runtime("Invalid command name")),
        };

        // Access database via thread-local pointer (safe: single-threaded)
        let result = CURRENT_DB.with(|cell| {
            let ptr = cell.get() as *mut crate::storage::Database;
            if ptr.is_null() {
                return Err(mlua::Error::runtime("No database context for redis.call"));
            }
            let db = unsafe { &mut *ptr };
            let mut db_idx = CURRENT_DB_IDX.with(|c| c.get());
            let db_count = CURRENT_DB_COUNT.with(|c| c.get());

            // Track write for SCRIPT KILL safety check
            if crate::persistence::aof::is_write_command(&cmd_bytes) {
                SCRIPT_HAD_WRITE.with(|c| c.set(true));
            }

            let r = dispatch(db, &cmd_bytes, &frames[1..], &mut db_idx, db_count);
            Ok(match r {
                DispatchResult::Response(f) | DispatchResult::Quit(f) => f,
            })
        })?;

        // redis.call: propagate error as Lua error
        // redis.pcall: return error as {err = "..."} table
        if propagate_errors {
            if let Frame::Error(e) = &result {
                return Err(mlua::Error::runtime(
                    String::from_utf8_lossy(e).to_string()
                ));
            }
        }

        crate::scripting::types::frame_to_lua_value(lua, &result)
    })
}
```

### Pattern 4: Sandbox Setup

**What:** After creating the Lua VM with `Lua::new()` (which already excludes io/package/debug), further restrict the environment.

**Note:** `Lua::new()` loads ONLY the safe standard library: string, table, math, os (partial), coroutine, utf8. It already excludes io, package, debug, and require. We need to additionally restrict os.* to clock() only.

```rust
// In scripting/sandbox.rs:
pub fn setup_sandbox(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();

    // Restrict os.* to clock() only
    let restricted_os = lua.create_table()?;
    if let Ok(os_table) = globals.get::<mlua::Table>("os") {
        if let Ok(clock_fn) = os_table.get::<mlua::Function>("clock") {
            restricted_os.set("clock", clock_fn)?;
        }
    }
    globals.set("os", restricted_os)?;

    // Ensure package is nil (historical sandbox escape vector)
    globals.set("package", mlua::Nil)?;

    // Remove load/loadstring (Lua 5.4 has 'load', not 'loadstring')
    globals.set("load", mlua::Nil)?;
    globals.set("dofile", mlua::Nil)?;
    globals.set("loadfile", mlua::Nil)?;
    globals.set("collectgarbage", mlua::Nil)?; // prevent GC manipulation

    Ok(())
}
```

### Pattern 5: Type Conversion (Lua <-> Frame)

The complete Redis-compatible conversion table (HIGH confidence -- from official Redis docs):

**Frame -> Lua (RESP2 -> Lua):**
| Frame Variant | Lua Value |
|---------------|-----------|
| Integer(n) | number (integer) |
| BulkString(b) | string |
| SimpleString(b) | table `{ok = string}` |
| Error(e) | table `{err = string}` |
| Null | boolean `false` |
| Array(items) | table (array, 1-indexed) |

**Lua -> Frame (Lua -> RESP2):**
| Lua Value | Frame Variant |
|-----------|---------------|
| number (integer) | Integer |
| number (float) | Integer (truncated -- Redis behavior) |
| string | BulkString |
| table (array) | Array (stops at first nil) |
| table `{ok=str}` | SimpleString |
| table `{err=str}` | Error |
| boolean false | Null |
| boolean true | Integer(1) |
| nil | Null |

### Pattern 6: Script Timeout via Hook

**What:** Install a Lua hook before script execution; check wall-clock time every N instructions.

```rust
// Source: mlua HookTriggers docs
use std::time::{Duration, Instant};
use mlua::{HookTriggers, Lua};

pub fn install_timeout_hook(lua: &Lua, timeout: Duration) -> mlua::Result<()> {
    let deadline = Instant::now() + timeout;
    lua.set_hook(
        HookTriggers {
            every_nth_instruction: Some(100_000), // check every 100k VM instructions
            ..Default::default()
        },
        move |_lua, _debug| {
            if Instant::now() >= deadline {
                Err(mlua::Error::runtime(
                    "ERR Lua script timeout"
                ))
            } else {
                Ok(())
            }
        },
    )
}

pub fn remove_timeout_hook(lua: &Lua) {
    lua.remove_hook();
}
```

**Important:** Always call `remove_timeout_hook()` after script execution, success or failure. The hook closure captures the `Instant` deadline -- leaving it installed contaminates the next script execution.

### Pattern 7: Script Cache

```rust
// In scripting/cache.rs:
use bytes::Bytes;
use std::collections::HashMap;

pub struct ScriptCache {
    // hex sha1 (40 chars) -> script source
    scripts: HashMap<String, Bytes>,
}

impl ScriptCache {
    pub fn new() -> Self {
        ScriptCache { scripts: HashMap::new() }
    }

    /// Cache a script and return its hex SHA1.
    pub fn load(&mut self, script: Bytes) -> String {
        let sha = sha1_smol::Sha1::from(&script[..]).hexdigest();
        self.scripts.entry(sha.clone()).or_insert(script);
        sha
    }

    pub fn get(&self, sha1_hex: &str) -> Option<&Bytes> {
        self.scripts.get(sha1_hex)
    }

    pub fn exists(&self, sha1_hex: &str) -> bool {
        self.scripts.contains_key(sha1_hex)
    }

    pub fn flush(&mut self) {
        self.scripts.clear();
    }
}
```

### Anti-Patterns to Avoid

- **Sharing a single Lua VM across shards:** Never `Arc<Mutex<Lua>>`. Lua is not re-entrant. One VM per shard thread is the only correct design.
- **Adding `lua: Lua` to the `Shard` struct:** `Lua` is `!Send` so it cannot cross the `std::thread::Builder::spawn()` boundary. Initialize the VM inside `Shard::run()` after the thread is already running.
- **Forgetting `remove_hook()` after script execution:** The hook closure captures the deadline `Instant`. Leaving it installed means the next script inherits the old (expired) deadline and times out immediately.
- **0-indexed KEYS/ARGV tables:** Lua tables are 1-indexed. Use `table.set(i + 1, value)` not `table.set(i, value)`.
- **Leaving `package` variable alive:** Historical sandbox escape: `package.loadlib("some.so", "func")`. Explicitly nil it out even if `Lua::new()` removed `require`.
- **Not validating cross-shard keys before touching the Lua VM:** The cross-shard key check must happen BEFORE any Lua VM interaction (before loading script, before registering KEYS/ARGV). Return CROSSSLOT immediately.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Lua VM | Custom interpreter | mlua + Lua 5.4 | Lua C library handles GC, coroutines, error handling |
| SHA1 hashing | Custom hash | sha1_smol | Correct, tested, zero deps |
| Lua error propagation | Custom error table | mlua::Error::runtime() | mlua handles Lua pcall/error semantics |
| Type marshaling | Custom serialization | lua_to_frame / frame_to_lua | Table-driven, covers all RESP2 <-> Lua cases |
| Sandbox | Custom Lua interpreter | `Lua::new()` + manual nil-out | Safe subset already chosen; only need to restrict os.* |

---

## Common Pitfalls

### Pitfall 1: Lua VM Initialization Before Thread Spawn
**What goes wrong:** Adding `lua: Lua` to the `Shard` struct and initializing in `Shard::new()` -- before the shard thread is spawned.
**Why it happens:** Natural tendency to put all shard fields in the struct.
**How to avoid:** Initialize the Lua VM inside `Shard::run()` after the thread is running. The `Lua` struct is `!Send`, so it cannot be created before `std::thread::Builder::spawn()`.
**Warning signs:** Compiler error "`Lua` cannot be sent between threads safely" when trying to move into thread closure.

### Pitfall 2: Mutable Database Access During redis.call
**What goes wrong:** `redis.call` from Lua needs `&mut Database`, but the shard's database vector is already borrowed via `Rc<RefCell>` when EVAL is executing.
**Why it happens:** EVAL borrows `databases.borrow_mut()` to get `&mut Database`, then calls Lua, which calls back into Rust via redis.call needing the same `&mut Database`.
**How to avoid:** Use `thread_local! { static CURRENT_DB: Cell<*mut Database> }`. Set the pointer before calling `lua.load(script).exec()`, use it inside the redis.call closure, clear it after. This is safe because everything is single-threaded.

### Pitfall 3: Script Timeout Hook Not Cleared
**What goes wrong:** Script A times out; error returned to client; but hook not removed. Script B immediately times out.
**Why it happens:** `set_hook()` called before Script A, but `remove_hook()` not called in error path.
**How to avoid:** Wrap in a RAII guard or use `defer` pattern with explicit cleanup in both success and error paths.

### Pitfall 4: Package Variable Left Alive (Sandbox Escape)
**What goes wrong:** `package.loadlib("/lib/x86_64-linux-gnu/liblua5.1.so.5", "luaopen_io")` bypasses sandbox.
**Why it happens:** `Lua::new()` may not fully clean up `package` variable depending on Lua version.
**How to avoid:** Explicitly `globals.set("package", mlua::Nil)?` in sandbox setup.

### Pitfall 5: KEYS/ARGV Are 1-Indexed in Lua
**What goes wrong:** Populating KEYS as 0-indexed; scripts using `KEYS[1]` return nil.
**Why it happens:** Rust iterators are 0-indexed; Lua tables start at 1.
**How to avoid:** Always use `table.set(i + 1, value)` when building KEYS/ARGV tables.

### Pitfall 6: Float Truncation Behavior
**What goes wrong:** `EVAL "return 3.99" 0` returns `(integer) 3` -- clients surprised.
**Why it happens:** Lua number-to-RESP2 conversion truncates floats to integers (Redis-compatible).
**How to avoid:** This is correct Redis behavior -- document it in code comments.

### Pitfall 7: SCRIPT LOAD Cache Desync Across Shards
**What goes wrong:** `SCRIPT LOAD` populates cache on shard 0; `EVALSHA` with keys hashing to shard 1 returns NOSCRIPT.
**Why it happens:** Script cache is per-shard; SCRIPT LOAD routes to one shard by connection assignment.
**How to avoid:** SCRIPT LOAD must fan-out to ALL shards. Add `ShardMessage::ScriptLoad { sha1: String, script: Bytes }` and send it to all shards via the SPSC mesh during the SCRIPT LOAD handler.

### Pitfall 8: nil in Lua Arrays Stops Serialization
**What goes wrong:** `EVAL "return {1, nil, 3}" 0` returns array of length 1.
**Why it happens:** Lua table-to-RESP Array conversion stops at first nil -- this is Redis-compatible behavior.
**How to avoid:** Document and accept this behavior. It matches Redis.

---

## Code Examples

### EVAL argument parsing
```rust
pub fn parse_eval_args(args: &[Frame]) -> Result<(Bytes, usize, Vec<Bytes>, Vec<Bytes>), Frame> {
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'eval' command"
        )));
    }
    let script = match &args[0] {
        Frame::BulkString(b) => b.clone(),
        _ => return Err(Frame::Error(Bytes::from_static(b"ERR invalid script"))),
    };
    let numkeys: usize = match &args[1] {
        Frame::BulkString(b) => std::str::from_utf8(b)
            .ok().and_then(|s| s.parse().ok())
            .ok_or_else(|| Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range"
            )))?,
        _ => return Err(Frame::Error(Bytes::from_static(b"ERR invalid numkeys"))),
    };
    if args.len() < 2 + numkeys {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Number of keys can't be greater than number of args"
        )));
    }
    let keys: Vec<Bytes> = args[2..2 + numkeys].iter()
        .filter_map(|f| match f { Frame::BulkString(b) => Some(b.clone()), _ => None })
        .collect();
    let argv: Vec<Bytes> = args[2 + numkeys..].iter()
        .filter_map(|f| match f { Frame::BulkString(b) => Some(b.clone()), _ => None })
        .collect();
    Ok((script, numkeys, keys, argv))
}
```

### SHA1 script hashing
```rust
pub fn sha1_hex(script: &[u8]) -> String {
    sha1_smol::Sha1::from(script).hexdigest()
}
```

### Cross-shard key validation
```rust
pub fn validate_keys_same_shard(
    keys: &[Bytes],
    shard_id: usize,
    num_shards: usize,
) -> Option<Frame> {
    use crate::shard::dispatch::key_to_shard;
    for key in keys {
        if key_to_shard(key, num_shards) != shard_id {
            return Some(Frame::Error(Bytes::from_static(
                b"CROSSSLOT Keys in script don't hash to the same slot and shard"
            )));
        }
    }
    None
}
```

### SCRIPT subcommand handler
```rust
pub fn handle_script_subcommand(
    cache: &mut ScriptCache,
    args: &[Frame],
) -> Frame {
    let sub = match args.first() {
        Some(Frame::BulkString(b)) => b.clone(),
        _ => return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'script' command"
        )),
    };

    if sub.eq_ignore_ascii_case(b"LOAD") {
        if args.len() != 2 { return Frame::Error(Bytes::from_static(b"ERR syntax error")); }
        let script = match &args[1] { Frame::BulkString(b) => b.clone(), _ => return Frame::Error(Bytes::from_static(b"ERR invalid script")) };
        let sha = cache.load(script);
        Frame::BulkString(Bytes::from(sha))
    } else if sub.eq_ignore_ascii_case(b"EXISTS") {
        let results: Vec<Frame> = args[1..].iter()
            .filter_map(|f| match f { Frame::BulkString(b) => Some(b.clone()), _ => None })
            .map(|sha| {
                let exists = cache.exists(std::str::from_utf8(&sha).unwrap_or(""));
                Frame::Integer(if exists { 1 } else { 0 })
            })
            .collect();
        Frame::Array(results)
    } else if sub.eq_ignore_ascii_case(b"FLUSH") {
        cache.flush();
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}' for 'script' command",
            String::from_utf8_lossy(&sub)
        )))
    }
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| rlua crate | mlua (successor) | ~2021 | Better maintained, Lua 5.4, async support |
| Lua 5.1 (Redis default pre-7.4) | Lua 5.4 | Redis 7.4+ direction | Integer type, generational GC, bitwise ops |
| redis.replicate_commands() required | Default in Redis 7+ | Redis 7.0 | No need to implement |

**Deprecated/outdated:**
- rlua: abandoned, use mlua instead
- lua51 feature flag: do not use; target is lua54

---

## Open Questions

1. **SCRIPT KILL implementation**
   - What we know: Redis allows SCRIPT KILL only if the script has not performed writes. The hook-based timeout raises a Lua error, returning control to Rust.
   - What's unclear: SCRIPT KILL from another client connection requires a cross-shard signal. The shard is single-threaded and blocked during Lua execution -- SCRIPT KILL cannot arrive until the hook fires.
   - Recommendation: Implement SCRIPT KILL with a per-shard `AtomicBool` flag. The hook checks both the timeout deadline AND the kill flag. Document that SCRIPT KILL takes effect within the next hook interval (~100k instructions), not immediately.

2. **SCRIPT LOAD fan-out to all shards**
   - What we know: Script cache is per-shard. SCRIPT LOAD response must return SHA1 immediately.
   - What's unclear: When a client sends SCRIPT LOAD, it hits one shard. EVALSHA with keys on a different shard returns NOSCRIPT.
   - Recommendation: Add `ShardMessage::ScriptLoad { sha1: String, script: Bytes }` to the dispatch enum. The sharded handler intercepts SCRIPT LOAD, adds to local cache, and fans out the message to all other shards via SPSC. This is the correct approach matching the existing PubSubFanOut pattern.

3. **Bytecode pre-compilation (Claude's Discretion)**
   - What we know: mlua supports `lua.load(script).into_function()` which compiles to Lua bytecode. The compiled `Function` can be stored via `lua.create_registry_value(function)` returning a `RegistryKey`.
   - Recommendation: Add `bytecode_cache: HashMap<String, mlua::RegistryKey>` to ScriptCache. On first execution, compile and store. On EVALSHA, look up the registry key and call the function directly. This is a discrete optimization -- implement after core functionality works.

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust integration tests (tokio) + redis crate client |
| Config file | none -- inline test helpers in tests/integration.rs |
| Quick run command | `cargo test scripting -- --nocapture` |
| Full suite command | `cargo test -- --nocapture` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| EVAL-01 | EVAL executes Lua with KEYS/ARGV | integration | `cargo test test_eval_basic` | ❌ Wave 0 |
| EVAL-02 | EVALSHA executes cached script by SHA1 | integration | `cargo test test_evalsha` | ❌ Wave 0 |
| EVAL-03 | NOSCRIPT returned for unknown SHA1 | integration | `cargo test test_evalsha_noscript` | ❌ Wave 0 |
| SCRIPT-01 | SCRIPT LOAD returns SHA1, caches script | integration | `cargo test test_script_load` | ❌ Wave 0 |
| SCRIPT-02 | SCRIPT EXISTS returns 1/0 | integration | `cargo test test_script_exists` | ❌ Wave 0 |
| SCRIPT-03 | SCRIPT FLUSH clears cache | integration | `cargo test test_script_flush` | ❌ Wave 0 |
| SANDBOX-01 | os.execute, io, require raise errors | integration | `cargo test test_sandbox_restrictions` | ❌ Wave 0 |
| SANDBOX-02 | string/table/math available | integration | `cargo test test_sandbox_allowed` | ❌ Wave 0 |
| REDIS-01 | redis.call executes GET/SET within Lua | integration | `cargo test test_redis_call` | ❌ Wave 0 |
| REDIS-02 | redis.pcall catches errors as table | integration | `cargo test test_redis_pcall` | ❌ Wave 0 |
| ATOMIC-01 | Script executes atomically | manual | N/A | manual only |
| TIMEOUT-01 | Script exceeding 5s returns timeout error | integration | `cargo test test_script_timeout` | ❌ Wave 0 |
| CROSSSLOT-01 | Keys on different shards return CROSSSLOT | integration | `cargo test test_crossslot_error` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test scripting`
- **Per wave merge:** `cargo test -- --nocapture`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/integration.rs` -- add scripting test section and all EVAL-*, SCRIPT-*, SANDBOX-*, REDIS-*, TIMEOUT-*, CROSSSLOT-* tests
- [ ] Add to `Cargo.toml`: `mlua = { version = "0.11", features = ["lua54", "vendored"] }` and `sha1_smol = "1.0"`

---

## Sources

### Primary (HIGH confidence)
- [mlua docs.rs v0.11.6](https://docs.rs/mlua/latest/mlua/struct.Lua.html) -- Lua struct API, HookTriggers, set_hook, set_memory_limit
- [mlua HookTriggers docs](https://docs.rs/mlua/latest/mlua/struct.HookTriggers.html) -- every_nth_instruction hook
- [mlua GitHub](https://github.com/mlua-rs/mlua) -- feature flags, vendored, lua54, version 0.11.6
- [Redis Lua API reference](https://redis.io/docs/latest/develop/programmability/lua-api/) -- type conversion tables, sandbox restrictions, redis.* API
- [Redis EVAL command](https://redis.io/docs/latest/commands/eval/) -- command syntax, KEYS/ARGV rules
- [Redis scripting intro](https://redis.io/docs/latest/develop/programmability/eval-intro/) -- atomicity, timeout, EVALSHA, NOSCRIPT, caching
- [sha1_smol docs.rs](https://docs.rs/sha1_smol/latest/sha1_smol/) -- hexdigest() API, v1.0.1
- `cargo search mlua` -- confirmed 0.11.6 is current release

### Secondary (MEDIUM confidence)
- mlua discussions: !Send per-thread pattern -- confirmed `Rc<Lua>` is correct for single-threaded shards
- mlua sandbox discussion -- confirmed manual nil-out is required for Lua 5.4 (built-in sandbox is Luau-only)
- sha1 crate search -- sha1_smol preferred for zero-dependency simplicity

### Tertiary (LOW confidence)
- Redis CVE-2022-0543 analysis -- informs `package` variable nil-out requirement

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- mlua 0.11.6 verified via `cargo search`; sha1_smol 1.0.1 verified via docs.rs
- Architecture: HIGH -- patterns derived from existing shard code + mlua API docs
- Type conversion: HIGH -- from official Redis documentation
- Script timeout: MEDIUM -- hook approach confirmed by mlua docs; SCRIPT KILL cross-thread limitation is a known architectural constraint
- Pitfalls: HIGH -- sourced from Redis official docs + mlua thread discussions

**Research date:** 2026-03-25
**Valid until:** 2026-06-25 (mlua is stable; 90-day validity)
