# Lua Sandbox Audit

**Date:** 2026-04-09 (Phase 98, SEC-04)
**Lua version:** Lua 5.4 (vendored via mlua 0.11)
**Status:** Audit complete — no escape vectors found

## Sandbox Configuration

Moon uses `mlua` (Rust bindings for Lua 5.4) with a restricted standard library:

**File:** `src/scripting/sandbox.rs`

### Allowed Libraries

| Library | Status | Justification |
|---|---|---|
| `base` (partial) | Allowed | `type`, `tostring`, `tonumber`, `pcall`, `error`, `select`, `unpack`, `pairs`, `ipairs`, `next` |
| `string` | Allowed | String manipulation — no I/O |
| `table` | Allowed | Table manipulation — no I/O |
| `math` | Allowed | Math functions — no I/O |
| `cjson` (if available) | Allowed | JSON encode/decode — pure computation |

### Blocked Libraries

| Library | Status | Risk if exposed |
|---|---|---|
| `io` | **Blocked** | Filesystem read/write |
| `os` | **Blocked** | Command execution, env vars, file ops |
| `debug` | **Blocked** | Stack inspection, upvalue modification, gc manipulation |
| `package` | **Blocked** | Module loading from filesystem |
| `loadfile` | **Blocked** | Load and execute arbitrary Lua files |
| `dofile` | **Blocked** | Load and execute arbitrary Lua files |
| `load` (with file source) | **Blocked** | Load bytecode from files |
| `collectgarbage` | **Blocked** | GC manipulation can cause timing attacks |
| `rawget`/`rawset` | **Allowed** | Metatable bypass — acceptable for Redis scripting |

### redis.* API

The sandbox registers these functions:

| Function | Description | Safety |
|---|---|---|
| `redis.call(cmd, ...)` | Execute Redis command | Safe — routes through ACL + command dispatch |
| `redis.pcall(cmd, ...)` | Protected call (returns error instead of raising) | Safe |
| `redis.log(level, msg)` | Write to Moon's tracing log | Safe — message is sanitized |
| `redis.error_reply(msg)` | Return error frame | Safe |
| `redis.status_reply(msg)` | Return status frame | Safe |

### Type Conversions

| Lua → Redis | Redis → Lua |
|---|---|
| `string` → BulkString | BulkString → `string` |
| `number` (integer) → Integer | Integer → `number` |
| `boolean` → Integer (1/0) | Null → `false` |
| `table` (array) → Array | Array → `table` |
| `nil` → Null | Error → raises Lua error |

## Resource Limits

| Resource | Limit | Enforcement |
|---|---|---|
| Script execution time | Configurable timeout (default: 5s) | `mlua` timeout hook |
| Memory allocation | Bounded by server maxmemory | Lua allocator hooks (via mlua) |
| Stack depth | Lua default (200 levels) | Built-in |
| Keys accessed | Must be declared in EVAL KEYS array | Validated before execution |

## CVE Review (lua54 vendored source)

mlua 0.11 vendors Lua 5.4.7 (latest stable as of 2026-04). Known CVEs:

| CVE | Affected | Status |
|---|---|---|
| CVE-2022-33099 | Lua < 5.4.4 | Fixed in vendored 5.4.7 |
| CVE-2022-28805 | Lua < 5.4.4 | Fixed in vendored 5.4.7 |
| CVE-2021-44964 | Lua < 5.4.4 | Fixed in vendored 5.4.7 |
| CVE-2021-43519 | Lua < 5.4.4 | Fixed in vendored 5.4.7 |

No open CVEs affecting Lua 5.4.7.

## Potential Escape Vectors (Reviewed)

| Vector | Status | Detail |
|---|---|---|
| `debug.getinfo` | **Blocked** | Debug library not loaded |
| `package.loadlib` | **Blocked** | Package library not loaded |
| `os.execute` | **Blocked** | OS library not loaded |
| `io.open` | **Blocked** | IO library not loaded |
| `load(bytecode)` | **Restricted** | Only string source allowed, no file source |
| `string.dump` → `load` | **Safe** | Can dump+reload functions but stays within sandbox |
| `coroutine.wrap` abuse | **Safe** | Coroutine resume/yield bounded by timeout |
| Metatable __gc abuse | **Low risk** | GC finalizers run in sandbox context |
| `redis.call` as oracle | **By design** | ACL controls which commands are accessible |

## Recommendations

1. **Monitor mlua releases** for security patches to the vendored Lua source.
2. **Consider disabling `load`** entirely — EVALSHA covers script caching without runtime compilation.
3. **Add SCRIPT NO-WRITES flag** in future — allow read-only scripts to skip ACL write checks.
4. **Fuzz the Lua bridge** — add a cargo-fuzz target that feeds random Lua source to the sandbox.

---

*This audit covers the sandbox configuration as of mlua 0.11 + Lua 5.4.7. Re-audit when upgrading mlua or changing sandbox settings.*
