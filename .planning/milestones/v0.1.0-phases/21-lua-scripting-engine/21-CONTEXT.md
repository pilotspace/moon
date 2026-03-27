# Phase 21: Lua Scripting Engine - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Embed Lua 5.4 scripting via the `mlua` crate with EVAL/EVALSHA commands, SHA1-based script caching, sandboxed execution environment with Redis API bindings (redis.call, redis.pcall, redis.log, redis.error_reply, redis.status_reply). Scripts execute atomically within a shard — no other commands interleave.

</domain>

<decisions>
## Implementation Decisions

### Lua runtime embedding
- [auto] Use `mlua` crate (actively maintained, supports Lua 5.4, safe Rust bindings)
- One Lua VM instance per shard (matches thread-per-core — no sharing, no locks)
- VM pre-initialized with Redis API functions on shard startup
- Lua 5.4 over 5.1 for: integers, bitwise ops, goto, generational GC — matches Redis 7.4+ direction

### EVAL/EVALSHA commands
- [auto] `EVAL script numkeys key [key ...] arg [arg ...]`
- Script text hashed with SHA1; cached in per-shard script cache
- `EVALSHA sha1 numkeys key [key ...] arg [arg ...]` — execute cached script by hash
- NOSCRIPT error if SHA1 not in cache — client must fall back to EVAL

### Script caching
- [auto] SCRIPT LOAD — cache script without executing, return SHA1
- SCRIPT EXISTS sha1 [sha1 ...] — check which scripts are cached
- SCRIPT FLUSH [ASYNC|SYNC] — clear script cache
- Script cache is per-shard (consistent with shared-nothing architecture)
- Scripts replicated to replicas via EVALSHA in replication stream

### Redis API bindings
- [auto] `redis.call(command, ...)` — execute Redis command, propagate errors
- `redis.pcall(command, ...)` — execute Redis command, catch errors as Lua table
- `redis.log(level, message)` — write to server log
- `redis.error_reply(msg)` / `redis.status_reply(msg)` — return Redis error/status
- KEYS table and ARGV table populated from command arguments
- Return value conversion: Lua table → RESP Array, Lua number → RESP Integer, Lua string → RESP BulkString, nil → RESP Null

### Sandbox restrictions
- [auto] Disabled: `os.execute`, `io.*`, `loadfile`, `dofile`, `require`, `debug.*`
- Allowed: `string.*`, `table.*`, `math.*`, `tonumber`, `tostring`, `type`, `error`, `pcall`, `xpcall`, `select`, `unpack`
- `redis.call`/`redis.pcall` execute within the same shard context — keys must be declared upfront
- Memory limit per script execution (configurable, default 256MB of Lua heap)

### Atomic execution
- [auto] Scripts execute atomically within a shard — no other commands interleave on that shard
- Script timeout: configurable (default 5 seconds), monitored by shard event loop
- SCRIPT KILL — terminate long-running script (only if no writes performed)
- If script has performed writes: can only be killed via SHUTDOWN NOSAVE

### Cross-shard scripts
- [auto] All KEYS declared in EVAL must hash to the same shard (enforced)
- If keys span multiple shards: return CROSSSLOT error
- This matches Redis Cluster behavior — scripts must use hash tags for multi-key operations

### Claude's Discretion
- Whether to pre-compile scripts to bytecode for faster re-execution
- Script debug mode (EVAL ... DEBUG YES — step-through debugging)
- redis.sha1hex() helper function
- Whether to implement redis.replicate_commands() (already default in Redis 7+)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase dependencies
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Scripts execute within shard context, per-shard Lua VM

### Current implementation
- `src/command/mod.rs` — Command dispatch (421 lines) — add EVAL/EVALSHA routing
- `src/command/connection.rs` — Connection commands (346 lines) — add SCRIPT subcommands
- All command handlers — redis.call() invokes them from within Lua

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `dispatch()` function — redis.call() invokes this with constructed Frame arguments
- Frame serialization/deserialization — Lua return values converted to/from Frame
- SHA1 — need `sha1` crate or use `ring` for hashing

### Established Patterns
- Command dispatch via Frame::Array — redis.call() constructs the same Frame
- Atomic execution (transaction EXEC) — script execution follows same pattern (hold shard lock)

### Integration Points
- New `src/scripting/` module for Lua VM management, sandbox setup, Redis API bindings
- Each shard owns one Lua VM instance — initialized during shard startup
- EVAL command handler executes script synchronously within shard's event loop
- Script timeout — shard's cooperative scheduler checks elapsed time between Lua operations

</code_context>

<specifics>
## Specific Ideas

- mlua is the modern successor to rlua — better maintained, more features, Lua 5.4 support
- Per-shard Lua VM avoids all concurrency issues — no locks, no shared state
- Script caching by SHA1 is critical for production — EVAL with large scripts is expensive

</specifics>

<deferred>
## Deferred Ideas

- Redis Functions (FUNCTION LOAD/CALL) — even more complex than EVAL, defer indefinitely
- Script debug mode — low priority, defer
- Library loading in scripts — security risk, defer

</deferred>

---

*Phase: 21-lua-scripting-engine*
*Context gathered: 2026-03-24*
