# Phase 24: Senior Engineer Review - Research

**Researched:** 2026-03-25
**Domain:** Test stabilization, compiler hygiene, performance profiling & optimization (Rust, macOS ARM)
**Confidence:** HIGH

## Summary

This research covers the four pillars of the senior engineer review: (1) root-cause analysis of all 9 failing integration tests, (2) compiler warning inventory, (3) hot-path analysis for GET/SET command flow, and (4) profiling toolchain for macOS ARM.

The most critical finding is that **all hash, list, and HSCAN test failures share a single root cause**: the Phase 15 listpack-first encoding introduced compact variants (HashListpack, ListListpack) that the read-only dispatch path cannot upgrade. The non-sharded `handle_connection` batches reads under a shared read lock and dispatches to `_readonly` handlers, which call `get_hash_if_alive()` / `get_list_if_alive()`. These methods return `Ok(None)` for listpack variants (by design -- they're immutable accessors), but the `_readonly` command handlers treat `Ok(None)` as "key not found" instead of falling through to a mutable upgrade path. The cluster tests fail due to a port arithmetic overflow (random test port + 10000 exceeds u16 range). The AOF test fails because AOF replay creates HashListpack entries that cannot be read back through the same broken read-only path.

**Primary recommendation:** Fix the read-only listpack handling first (either upgrade eagerly on write, or handle listpack in read-only accessors without mutation), then fix cluster port overflow, then clean warnings, then profile and optimize.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Fix ALL 9 failing integration tests before any optimization work
- 5 cluster tests (cluster_info/myid/nodes/addslots/keyslot) -- likely runtime config or test setup issues
- 2 hash tests (test_hscan, test_hash_commands) -- likely Phase 15 listpack encoding bugs
- 1 list test (test_list_commands) -- likely Phase 15 listpack encoding bug
- 1 AOF test (test_aof_restore_on_startup) -- likely persistence format mismatch with new types
- Full root-cause analysis for each: run in isolation, capture exact error, trace through code path
- Do NOT revert listpack -- fix the actual encoding/upgrade bugs
- Clean ALL 25 compiler warnings (dead code, unused imports, suggestions)
- Run clippy and fix ALL clippy warnings
- CPU profiling: cargo-flamegraph or Instruments.app (macOS ARM compatible)
- Heap profiling: mimalloc stats API for allocation counting
- Micro-benchmarks: Extend Criterion with GET/SET end-to-end latency benchmarks
- Target workload: GET/SET with 10 clients, 80/20 read/write, 256-byte values
- Throughput first (ops/sec on GET/SET) as headline metric
- Minimal unsafe: only use unsafe where profiling proves > 5% improvement
- Target: >= 1.5x single-instance Redis 7.x throughput at 10+ concurrent clients
- Both Redis and rust-redis on same machine -- relative comparison valid
- Document that real production numbers need separate client machine

### Claude's Discretion
- Which specific compiler/clippy warnings to prioritize
- Exact profiling tool flags and configuration
- Whether to add #[inline] annotations or let LTO handle it
- Order of optimizations after profiling reveals bottlenecks
- Whether to implement custom Criterion benchmarks or use redis-benchmark for smoke test

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core (already in Cargo.toml)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| mimalloc | 0.1 | Global allocator | 5.3x faster allocation vs glibc, per-thread free lists |
| criterion | 0.8 | Micro-benchmarks | Industry standard for Rust benchmarks, HTML reports |
| tokio | 1 | Async runtime | Already the project runtime |

### Profiling Tools (to install)
| Tool | Version | Purpose | macOS ARM Support |
|------|---------|---------|-------------------|
| cargo-flamegraph | latest | CPU flame graphs via dtrace | YES -- uses dtrace on macOS, no Linux-only deps |
| samply | latest | Firefox Profiler integration | YES -- native macOS ARM support, better than flamegraph for ARM |
| Instruments.app | Xcode 16 | Time Profiler, Allocations | YES -- Apple's native profiler, best ARM support |
| tokio-console | latest | Async task scheduling analysis | YES -- cross-platform |

### Benchmark Tools (available on system)
| Tool | Status | Purpose |
|------|--------|---------|
| redis-benchmark | /opt/homebrew/bin/redis-benchmark | Quick throughput smoke test |
| redis-server | /opt/homebrew/bin/redis-server | Redis 7.x baseline comparison |
| memtier_benchmark | NOT INSTALLED | Industry-standard load generator (recommended install) |

**Installation:**
```bash
# Profiling tools
cargo install flamegraph
cargo install samply
cargo install tokio-console

# Benchmark tool (recommended)
brew install memtier_benchmark
```

## Architecture Patterns

### Root-Cause Analysis: The 9 Failing Tests

#### Category 1: Listpack Read-Only Path Bug (5 tests)

**Affected tests:** test_hash_commands, test_hscan, test_list_commands, test_aof_restore_on_startup (partial), and potentially other hash/list operations that happen to pass through the write path.

**Root cause (verified by code trace and test execution):**

The non-sharded `handle_connection` implements read/write lock batching (Phase 7 optimization). Read commands (HGET, HGETALL, LRANGE, HSCAN, etc.) are dispatched via `dispatch_read()` which takes `&Database` (immutable). This calls `_readonly` handler variants (e.g., `hget_readonly`, `lrange_readonly`) which use immutable accessors like `get_hash_if_alive()` and `get_list_if_alive()`.

Phase 15 introduced listpack-first encoding: HSET creates `HashListpack`, LPUSH creates `ListListpack`. The immutable accessors handle this by returning `Ok(None)` for listpack variants, with the design intent that callers would "fall through to mutable upgrade path." But the `_readonly` handlers have no mutable fallback -- they interpret `Ok(None)` as "key not found" and return `Frame::Null` or `Frame::Array(vec![])`.

**Error traces (captured from actual test runs):**
- `test_hash_commands` line 475: `unwrap()` on nil -- HGET returns nil after HSET created HashListpack
- `test_list_commands` line 576: `assert_eq!([], ["c", "b", "a"])` -- LRANGE returns empty after LPUSH created ListListpack
- `test_hscan` line 1125: `assert_eq!(0, 8)` -- HSCAN returns 0 items from HashListpack
- `test_aof_restore_on_startup` line 1451: `unwrap()` on nil -- after AOF replay, HGET returns nil (AOF replay creates HashListpack via dispatch, subsequent read via readonly path fails)

**Fix strategy (two options, recommend Option A):**

**Option A (Recommended): Make `_readonly` accessors handle listpack without mutation.**
Add methods like `get_hash_if_alive_or_listpack()` that return an enum of either `&HashMap` or `&Listpack`, and have the readonly handlers read from listpack directly (iterate pairs, find field). This preserves the read/write lock optimization. The listpack is small (<128 entries) so linear scan is fast.

**Option B: Route read commands through write path when listpack is detected.**
Either always dispatch hash/list reads as writes (performance regression), or do a two-phase check: try read-only first, if `Ok(None)` for a key that should exist, fall through to write lock upgrade. This is complex and fragile.

**Note for unit tests:** Unit tests pass because they call `hget()` (mutable path) which auto-upgrades listpack to HashMap. The integration tests exercise the server's read/write lock batching which is the only path that hits the readonly variants.

#### Category 2: Cluster Bus Port Overflow (5 tests)

**Affected tests:** cluster_info, cluster_myid, cluster_nodes, cluster_addslots, cluster_keyslot

**Root cause (verified by test execution):**

```
thread '<unnamed>' panicked at src/cluster/mod.rs:67:
cluster bus port overflow: TryFromIntError(())
```

`ClusterNode::new()` computes `bus_port = (addr.port() as u32 + 10000).try_into::<u16>()`. Test servers bind to port 0 (OS-assigned), which typically assigns ports in the 49152-65535 ephemeral range. Adding 10000 to a port like 55536 gives 65536, which overflows u16 (max 65535).

**Fix:** Either:
1. Compute bus_port as `port + 10000` with wrapping/clamping and handle gracefully
2. In test setup, bind to a lower port range, or pass bus_port separately
3. Make bus_port configurable or use `port + 10000` only when `port < 55536`, otherwise use a different formula (like Redis's `port + 10000` with no overflow protection, but Redis server ports are typically low)

**Recommended fix:** Guard the bus port calculation: if port + 10000 > 65535, use a modular wrap or just skip cluster bus binding in test mode. The simplest fix is to cap: `let bus_port = ((port as u32 + 10000) % 65536) as u16` or use `.unwrap_or(port.wrapping_add(10000))`.

#### Category 3: AOF Replay with Listpack (1 test)

**Affected test:** test_aof_restore_on_startup

**Root cause:** This test writes HSET via AOF (which creates HashListpack), shuts down, restarts with AOF replay (which re-dispatches HSET creating HashListpack again), then reads via HGET. The read fails for the same reason as Category 1 -- the readonly path can't read listpack.

**Fix:** Same as Category 1. Once the readonly path handles listpack, this test will pass.

### Compiler Warning Inventory (25 warnings)

**Breakdown by category:**
| Category | Count | Fix Strategy |
|----------|-------|-------------|
| Unused imports | ~15 | `cargo fix --lib` auto-fix |
| Dead code (unused functions/constants) | ~5 | Remove or `#[allow(dead_code)]` if intentional API |
| Unnecessary unsafe blocks | 2 | Remove the `unsafe` wrapper |
| `drop()` with reference (no-op) | 2 | Remove the `drop()` call |
| Unused variable | 1 | Prefix with `_` |

**Clippy status:** 302 warnings + 1 error. The error is `this loop never actually loops` in `src/storage/bptree.rs:1275`. Must fix this error before clippy can complete. Clippy warnings likely include manual_ok_err, unnecessary clones, and similar style issues.

**Recommended order:**
1. Fix the 1 clippy error in bptree.rs (loop that never loops)
2. Run `cargo fix --lib` for auto-fixable warnings
3. Manually remove dead code / unused constants
4. Run `cargo clippy --fix` for auto-fixable clippy warnings
5. Manually fix remaining clippy warnings

### Hot-Path Analysis: GET/SET Command Flow

**GET command flow (sharded path -- the production path):**
```
TCP recv (tokio) -> Framed/RespCodec decode -> parse() [memchr CRLF scan]
  -> batch collect (now_or_never loop, up to 1024)
  -> for each frame:
       extract_command() [zero-alloc, borrow from Frame]
       -> key_to_shard() [xxhash64 + modulo]
       -> if local shard:
            databases.borrow_mut()
            db.refresh_now()          // <-- syscall: SystemTime::now() ONCE per batch
            string::get() or get_readonly()
              -> db.get(key)          // DashTable lookup: hash -> segment -> SIMD probe
                -> entry.is_expired_at()
                -> entry.value.as_bytes()
              -> Frame::BulkString(Bytes::copy_from_slice(v))  // <-- ALLOCATION
       -> if remote shard:
            SPSC send to target shard
            oneshot wait for response
  -> serialize response -> Framed/RespCodec encode -> TCP send (tokio)
```

**Identified allocation hotspots on GET path:**
1. **`Bytes::copy_from_slice(v)` in GET response** -- allocates a new buffer and copies the value. For a 256-byte value this is a 256-byte heap allocation per GET. Could be eliminated with zero-copy (return a `Bytes` that shares the stored value's memory).
2. **`Frame::BulkString(...)` construction** -- the Frame enum itself is stack-allocated but contains a `Bytes` which is heap-allocated.
3. **`BumpVec` for batch responses** -- arena-allocated, O(1) bulk dealloc, efficient.
4. **`vec![first_frame]` batch collection** -- initial Vec allocation.

**SET command flow:**
```
Same parse/batch path as GET
  -> string::set()
    -> extract_bytes(&args[0])  // borrow, no alloc
    -> args[0].clone(), args[1].clone()  // Bytes::clone = Arc refcount bump (cheap)
    -> db.set(key, value)
      -> DashTable::insert(key, Entry::new_string(value))
        -> CompactValue: if len <= 12, inline SSO; else Box::new(RedisValue::String(value))
      -> entry_overhead() accounting
  -> AOF serialization if enabled: serialize(&frame, &mut BytesMut) // allocation
```

**Key optimization opportunities (ordered by likely impact):**
1. **Zero-copy GET response**: Instead of `Bytes::copy_from_slice(v)`, if the stored value is a `Bytes`, clone it (Arc bump, not memcpy). For inline CompactValue (<= 12 bytes), copy is unavoidable but cheap.
2. **Release profile with LTO**: No `[profile.release]` section in Cargo.toml. Adding `lto = "thin"` and `codegen-units = 1` can yield 10-20% throughput improvement.
3. **Batch refresh_now()**: Currently called once per batch -- already optimized.
4. **DashTable probe optimization**: SIMD probing already implemented via control bytes. Prefetch hints for next segment could help.
5. **Response serialization**: `itoa` crate already used for integer formatting. Static responses for OK/PONG/NULL already pre-computed.

### Profiling Strategy for macOS ARM

**CPU Profiling:**

| Tool | How | Pros | Cons |
|------|-----|------|------|
| `cargo flamegraph` | `sudo cargo flamegraph --release -- <args>` | SVG output, familiar | Requires dtrace permissions on macOS, SIP may interfere |
| `samply` | `samply record target/release/rust-redis` | Firefox Profiler UI, stack trace demangling | Newer tool, less documentation |
| Instruments.app | Time Profiler instrument on release binary | Best ARM support, Signpost API integration | GUI-only, harder to automate |

**Recommended:** Use `samply` as primary (best macOS ARM support, scriptable, free), Instruments.app as fallback for deep analysis.

**Heap Profiling:**
- mimalloc stats: `mi_stats_print()` via FFI, or `mimalloc::Stats` if exposed
- jemalloc (optional feature): `cargo build --features jemalloc` then use `jemalloc-ctl` for heap profiling
- Manual counting: instrument hot path with counters for allocations per operation

**Benchmark Methodology:**
```bash
# Smoke test (available now with redis-benchmark)
redis-benchmark -h 127.0.0.1 -p 6380 -c 10 -n 100000 -t get,set -d 256

# Full comparison (after installing memtier)
./bench-v2.sh --smoke-test

# Criterion micro-benchmarks (already in benches/)
cargo bench --bench resp_parsing
```

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CPU flame graphs | Custom dtrace scripts | `cargo flamegraph` or `samply` | Handles symbol demangling, call graph aggregation |
| Micro-benchmark harness | Manual timing loops | Criterion 0.8 | Statistical rigor, warmup, outlier detection |
| Memory profiling | Manual counters everywhere | mimalloc stats API + jemalloc feature | Accurate per-allocation tracking |
| Redis protocol load gen | Custom client | redis-benchmark / memtier_benchmark | Industry standard, reproducible |

## Common Pitfalls

### Pitfall 1: Read-Only Path Cannot Mutate (CONFIRMED BUG)
**What goes wrong:** Listpack-encoded values appear as "not found" through readonly accessors.
**Why it happens:** Phase 15 introduced listpack-first encoding but the Phase 7 read/write lock batching dispatches reads through immutable `&Database` path.
**How to avoid:** Either make readonly accessors handle listpack natively, or route listpack-containing reads through the mutable path.
**Warning signs:** Any test that creates data with a write command then reads it back will fail if the data type uses listpack encoding and the read goes through the readonly path.

### Pitfall 2: Profiling Optimized-Away Code
**What goes wrong:** Without `debug = true` in release profile, function names are missing from flamegraphs.
**Why it happens:** Default release profile strips debug info.
**How to avoid:** Add to Cargo.toml:
```toml
[profile.release]
debug = true  # Enable debug symbols for profiling
lto = "thin"  # Cross-crate optimization
codegen-units = 1  # Maximum optimization
```
**Warning signs:** Flamegraph shows only `[unknown]` frames.

### Pitfall 3: Benchmarking Debug Builds
**What goes wrong:** Debug builds are 10-50x slower than release, making benchmark results meaningless.
**Why it happens:** Forgetting `--release` flag.
**How to avoid:** Always use `cargo build --release` and `target/release/rust-redis` for benchmarks.

### Pitfall 4: Cluster Port Overflow in Tests
**What goes wrong:** Ephemeral ports (49152-65535) + 10000 overflows u16.
**Why it happens:** Redis's convention of bus_port = port + 10000 assumes server ports < 55536.
**How to avoid:** Guard the arithmetic or use a different port allocation strategy in tests.

### Pitfall 5: SIP Blocking dtrace on macOS
**What goes wrong:** `cargo flamegraph` fails with permission errors.
**Why it happens:** System Integrity Protection restricts dtrace.
**How to avoid:** Use `samply` instead (uses macOS performance counters, no SIP issues), or run with `sudo`.

### Pitfall 6: Comparing Apples to Oranges in Benchmarks
**What goes wrong:** Claiming "faster than Redis" when using co-located client and server.
**Why it happens:** Network latency dominates real workloads; co-located testing under-reports it.
**How to avoid:** Label results as "conservative lower bound" and document that production comparison needs separate client machine with 25+ GbE.

## Code Examples

### Fix Pattern for Listpack Read-Only Path (Option A)

```rust
// In src/storage/db.rs -- add enum for read-only hash access
pub enum HashRef<'a> {
    Map(&'a HashMap<Bytes, Bytes>),
    Listpack(&'a super::listpack::Listpack),
}

impl<'a> HashRef<'a> {
    pub fn get_field(&self, field: &[u8]) -> Option<Bytes> {
        match self {
            HashRef::Map(map) => map.get(field).cloned(),
            HashRef::Listpack(lp) => {
                for (f, v) in lp.iter_pairs() {
                    if f.as_bytes() == field {
                        return Some(v.to_bytes());
                    }
                }
                None
            }
        }
    }
}

// New accessor that handles both variants
pub fn get_hash_ref_if_alive(&self, key: &[u8], now_ms: u64) -> Result<Option<HashRef<'_>>, Frame> {
    let base_ts = self.base_timestamp;
    match self.data.get(key) {
        None => Ok(None),
        Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
        Some(entry) => match entry.value.as_redis_value() {
            RedisValueRef::Hash(map) => Ok(Some(HashRef::Map(map))),
            RedisValueRef::HashListpack(lp) => Ok(Some(HashRef::Listpack(lp))),
            _ => Err(Self::wrongtype_error()),
        },
    }
}
```

### Cluster Port Overflow Fix

```rust
// In src/cluster/mod.rs -- guard the port arithmetic
let bus_port = addr.port().checked_add(10000).unwrap_or_else(|| {
    // Wrap around for test ports in ephemeral range
    (addr.port() as u32 + 10000) as u16 // wrapping semantics
});
```

### Release Profile for Profiling

```toml
# In Cargo.toml
[profile.release]
debug = true          # Debug symbols for flamegraphs
lto = "thin"          # Cross-crate inlining
codegen-units = 1     # Max optimization (slower build)
opt-level = 3         # Full optimization
```

### Zero-Copy GET Response Pattern

```rust
// In src/command/string.rs -- avoid copy_from_slice
pub fn get(db: &mut Database, args: &[Frame]) -> Frame {
    // ... key extraction ...
    match db.get(key) {
        Some(entry) => match entry.value.as_bytes_cloneable() {
            // New method: returns Bytes (Arc clone for heap, copy for inline)
            Some(v) => Frame::BulkString(v),
            None => Frame::Error(WRONGTYPE_BYTES),
        },
        None => Frame::Null,
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| All reads through mutable path | Read/write lock batching (Phase 7) | Phase 7 | Read parallelism but broke listpack reads |
| HashMap for all hashes | Listpack-first encoding (Phase 15) | Phase 15 | Memory savings but read-only incompatibility |
| port + 10000 unchecked | Needs overflow guard | Phase 20 | Test stability on ephemeral ports |

## Open Questions

1. **Zero-copy GET feasibility**
   - What we know: CompactValue stores inline (<=12 bytes) or heap-allocated RedisValue::String(Bytes). For heap path, `Bytes::clone()` is an Arc refcount bump (cheap). For inline path, copy is unavoidable but the data is <=12 bytes.
   - What's unclear: Whether the current `as_bytes()` returns a `&[u8]` slice that prevents returning an owned `Bytes` without copy.
   - Recommendation: Add `as_bytes_owned()` to CompactValue that returns `Bytes` (clone for heap, copy_from_slice for inline). Profile before and after.

2. **LTO impact on build time vs performance**
   - What we know: `lto = "thin"` adds ~30-60% to build time but enables cross-crate inlining.
   - What's unclear: Exact throughput impact for this codebase.
   - Recommendation: Benchmark with and without LTO to quantify. Use for final measurement, skip during iteration.

3. **Whether sharded path has the same listpack bug**
   - What we know: The sharded `handle_connection_sharded` uses `Rc<RefCell<Vec<Database>>>` (always mutable access). The hash/list tests use `start_server()` which runs the non-sharded path.
   - What's unclear: Whether any sharded-mode read operations also hit readonly paths.
   - Recommendation: Run hash/list tests with sharded server (`start_sharded_server(1)`) to verify. If they pass, the sharded path is clean.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test + criterion 0.8 |
| Config file | Cargo.toml (bench targets) |
| Quick run command | `cargo test --test integration -- --nocapture` |
| Full suite command | `cargo test --release && cargo test --test integration --release` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FIX-01 | Hash listpack read-only path works | integration | `cargo test --test integration test_hash_commands -- --nocapture` | Exists (currently FAILING) |
| FIX-02 | List listpack read-only path works | integration | `cargo test --test integration test_list_commands -- --nocapture` | Exists (currently FAILING) |
| FIX-03 | HSCAN works with listpack encoding | integration | `cargo test --test integration test_hscan -- --nocapture` | Exists (currently FAILING) |
| FIX-04 | AOF restore works with listpack types | integration | `cargo test --test integration test_aof_restore_on_startup -- --nocapture` | Exists (currently FAILING) |
| FIX-05 | Cluster tests pass (port overflow fixed) | integration | `cargo test --test integration cluster_info cluster_myid cluster_nodes cluster_addslots cluster_keyslot -- --nocapture` | Exists (currently FAILING) |
| CLEAN-01 | Zero compiler warnings | build | `cargo build --release 2>&1 \| grep "warning" \| wc -l` | N/A (build check) |
| CLEAN-02 | Zero clippy errors | lint | `cargo clippy --release 2>&1 \| grep "error" \| wc -l` | N/A (lint check) |
| PERF-01 | Flamegraph generated for GET/SET | manual-only | Profile then inspect SVG | N/A |
| PERF-02 | >= 1.5x Redis throughput at 10 clients | benchmark | `redis-benchmark -h 127.0.0.1 -p 6380 -c 10 -n 100000 -t get,set -d 256` | bench-v2.sh exists |

### Sampling Rate
- **Per task commit:** `cargo test --test integration 2>&1 | tail -5`
- **Per wave merge:** Full test suite `cargo test --release`
- **Phase gate:** All 109 integration tests pass + zero warnings + benchmark shows >= 1.5x

### Wave 0 Gaps
- [ ] `[profile.release]` section in Cargo.toml -- needed for profiling with debug symbols
- [ ] `samply` or `cargo-flamegraph` installed -- needed for CPU profiling
- [ ] Criterion bench for GET/SET end-to-end -- `benches/get_set.rs` does not exist

## Sources

### Primary (HIGH confidence)
- Direct code analysis: `src/storage/db.rs` lines 742-776 (`get_hash_if_alive`, `get_list_if_alive`)
- Direct code analysis: `src/command/hash.rs` lines 666-686 (`hget_readonly`)
- Direct code analysis: `src/command/list.rs` lines 791-833 (`lrange_readonly`)
- Direct code analysis: `src/server/connection.rs` lines 992-1018 (read lock batching path)
- Direct code analysis: `src/cluster/mod.rs` lines 63-67 (bus port overflow)
- Test execution output: All 9 failing tests run with `--nocapture` to capture exact error messages
- Compiler output: `cargo build --release` (25 warnings), `cargo clippy --release` (302 warnings + 1 error)

### Secondary (MEDIUM confidence)
- Architecture blueprint: `.planning/architect-blue-print.md` -- performance targets and benchmark methodology
- Phase 15 decisions in STATE.md: Listpack-first encoding design intent

### Tertiary (LOW confidence)
- Profiling tool effectiveness on macOS ARM -- based on general knowledge, not verified on this specific codebase

## Metadata

**Confidence breakdown:**
- Test root-cause analysis: HIGH -- verified by running tests, reading code paths, and tracing exact failure points
- Warning inventory: HIGH -- captured from actual compiler/clippy output
- Hot-path analysis: HIGH -- traced through actual source code
- Profiling tools: MEDIUM -- tools are well-known but not yet installed/verified on this machine
- Optimization opportunities: MEDIUM -- based on code analysis, needs profiling to confirm impact

**Research date:** 2026-03-25
**Valid until:** 2026-04-25 (stable codebase, findings are codebase-specific)
