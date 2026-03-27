# Phase 7: Resolve Bottlenecks and Optimize Performance - Research

**Researched:** 2026-03-23
**Domain:** Rust async server performance optimization (lock contention, memory layout, critical section minimization)
**Confidence:** HIGH

## Summary

Phase 7 targets three measured bottlenecks from the Phase 6 benchmarks: (1) mutex contention under pipelining causing 0.26x Redis throughput at 50 clients/pipeline 64, (2) 3.8x memory overhead per key (47MB vs 12MB for 100K keys), and (3) 257% CPU utilization vs Redis's 90% caused by lock spin-waiting across threads. All three are addressed by the locked decisions: pipeline batching, critical section minimization, and Entry struct compaction.

The optimizations are well-constrained and low-risk. Pipeline batching changes only the connection handler loop. Entry compaction changes the `Entry` struct and its consumers. Both are local changes with clear before/after benchmarking via the existing `bench.sh` script.

**Primary recommendation:** Implement pipeline batching first (highest throughput impact), then Entry compaction (memory), then re-benchmark. Keep changes surgical -- the architecture stays the same, only the hot path behavior changes.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Pipeline batching: collect all available frames from codec stream, acquire lock once, execute all, release, then write all responses
- Minimize critical section: move response serialization and AOF logging OUTSIDE the lock
- Entry compaction: u32 timestamps for created_at/last_access, u32 version, remove created_at if unused
- NO actor model migration
- NO compact encodings (ziplist/listpack) this phase
- Keep tokio multi-thread runtime

### Claude's Discretion
- Exact read-batch loop implementation in connection handler
- Whether to use try_next() or ready! for non-blocking frame collection
- Response buffer strategy (Vec<Frame> vs pre-allocated)
- Whether expires_at should also be compacted (currently Option<Instant>)
- Benchmark warm-up strategy

### Deferred Ideas (OUT OF SCOPE)
- Full actor model migration (mpsc channel to single data-owning task)
- Compact encodings (ziplist/listpack equivalents for small collections)
- Database sharding (multiple Mutexes by key hash)
- io_uring integration
- Custom allocator per-database
</user_constraints>

## Standard Stack

### Core (already in project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.x | Async runtime (multi-thread) | Already in use, keep as-is |
| futures | 0.3 | StreamExt for frame reading | Already in use, provides `poll_next` / `try_next` |
| bytes | 1.11 | Zero-copy buffer management | Already in use |
| tokio-util | 0.7 | Framed codec for RESP | Already in use |

### Supporting (new addition)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| parking_lot | 0.12.5 | Drop-in Mutex replacement | Faster uncontended lock (no syscall), smaller (1 byte vs 40 bytes for std Mutex), better fairness under contention |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| parking_lot::Mutex | std::sync::Mutex | std is already in use; parking_lot is ~2x faster for short critical sections but adds a dependency |
| futures::StreamExt::try_next | tokio_stream::StreamExt | futures already a dep, no need to add another |

**Installation:**
```bash
cargo add parking_lot@0.12
```

**Recommendation on parking_lot:** Use it. The `parking_lot::Mutex` is a well-established Rust ecosystem standard for performance-sensitive code. It is 1 byte vs std's 40 bytes, uses spin-then-park strategy ideal for short critical sections, and is a drop-in replacement (same API). The batching optimization reduces contention, and parking_lot reduces the cost of each remaining lock acquisition. Combined effect is multiplicative.

## Architecture Patterns

### Current Architecture (What We're Optimizing)

```
Connection Handler (per client):
  loop {
    frame = stream.next().await       // read ONE frame
    lock = db.lock()                  // acquire mutex
    result = dispatch(db, frame)      // execute command
    drop(lock)                        // release mutex
    framed.send(result).await         // serialize + write response
  }
```

**Problem:** With pipeline depth 64 and 50 clients = 3200 lock/unlock cycles per pipeline batch. Each lock acquisition contends with all other clients.

### Target Architecture (After Optimization)

```
Connection Handler (per client):
  loop {
    // Phase 1: Collect batch (non-blocking)
    batch = vec![stream.next().await]    // block for first frame
    while let Ok(Some(frame)) = poll_ready(&stream) {
      batch.push(frame)                  // drain buffered frames
    }

    // Phase 2: Execute batch under single lock
    lock = db.lock()
    results = vec![]
    aof_entries = vec![]
    for frame in batch {
      result = dispatch(db, frame)
      results.push(result)
      if is_write { aof_entries.push(serialized) }
    }
    drop(lock)                           // release BEFORE I/O

    // Phase 3: Write responses + AOF (outside lock)
    for result in results {
      framed.send(result).await
    }
    for entry in aof_entries {
      aof_tx.send(entry).await
    }
  }
```

**Result:** 50 lock acquisitions per pipeline batch instead of 3200. Lock held for execution only, not serialization.

### Pattern 1: Non-Blocking Batch Collection

**What:** After receiving the first frame (blocking), drain all immediately-available frames from the codec buffer without blocking.

**When to use:** Pipelined connections where the TCP read buffer contains multiple frames.

**Implementation approach -- use `futures::StreamExt::next()` with `FutureExt::now_or_never()`:**
```rust
use futures::{StreamExt, FutureExt};

// Block for first frame
let first = framed.next().await;
let mut batch = vec![first];

// Drain remaining buffered frames (non-blocking)
loop {
    match framed.next().now_or_never() {
        Some(Some(Ok(frame))) => batch.push(Some(Ok(frame))),
        _ => break,
    }
}
```

**Why `now_or_never()` over `poll_next`:** `poll_next` requires manual `Pin` + `Context` wiring. `now_or_never()` is a clean futures combinator that polls once and returns `None` if not ready. It's the idiomatic approach for "check if data is available without waiting." Already available via the `futures` crate dependency.

**Alternative: `tokio::select!` with `tokio::time::timeout(Duration::ZERO)`** -- more verbose, less idiomatic.

### Pattern 2: Critical Section Minimization

**What:** Separate command execution (needs lock) from I/O operations (doesn't need lock).

**Key insight:** The current code holds the lock while:
1. Checking expiry (needs lock) -- KEEP
2. AOF serialization (doesn't need lock) -- MOVE OUT (already partially done -- AOF bytes serialized before dispatch)
3. Command dispatch (needs lock) -- KEEP
4. AOF channel send (doesn't need lock, is async) -- already outside lock
5. Response write (doesn't need lock, is async) -- already outside lock

Actually, reviewing the current code more carefully (connection.rs lines 549-582): AOF serialization is already done BEFORE the lock (line 549-555), dispatch is under the lock (lines 558-562), and both AOF send (line 571) and response write (line 581) are already outside the lock. The main win is **batching** -- reducing lock acquisition frequency, not lock hold time.

The eviction check (lines 535-546) acquires the lock separately before the dispatch lock. In the batched version, eviction should happen once per batch, inside the same lock acquisition as dispatch.

### Pattern 3: Entry Struct Compaction

**Current Entry layout (measured):**
```rust
pub struct Entry {
    pub value: RedisValue,           // 56 bytes (enum with largest variant)
    pub expires_at: Option<Instant>, // 16 bytes
    pub created_at: Instant,         // 16 bytes  <-- REMOVE (never read)
    pub version: u64,                // 8 bytes   <-- compact to u32
    pub last_access: Instant,        // 16 bytes  <-- compact to u32
    pub access_counter: u8,          // 1 byte
}
// Total metadata: 57 bytes + alignment padding
```

**Optimized Entry layout:**
```rust
pub struct Entry {
    pub value: RedisValue,           // 56 bytes (unchanged)
    pub expires_at: Option<Instant>, // 16 bytes (keep -- needs ms precision for PTTL)
    pub version: u32,                // 4 bytes  (4B versions per key is sufficient)
    pub last_access: u32,            // 4 bytes  (unix seconds -- sufficient for LRU/LFU)
    pub access_counter: u8,          // 1 byte   (unchanged)
}
// Total metadata: 25 bytes + alignment padding
// Savings: ~32 bytes per entry
// For 100K keys: ~3.2 MB saved
```

**Key findings from code analysis:**

1. **`created_at` is NEVER READ** -- confirmed by grep. It is only set in constructors. Safe to remove entirely. Saves 16 bytes.

2. **`last_access` can be u32 unix timestamp** -- only used for:
   - LRU comparison (`entry.last_access < *oldest` in eviction.rs) -- relative ordering preserved with unix seconds
   - LFU decay (`last_access.elapsed().as_secs() / 60` in entry.rs) -- needs helper: `current_secs - last_access_secs` then `/60`
   - Update on access (`self.last_access = Instant::now()`) -- change to `self.last_access = current_unix_secs()`

3. **`version` can be u32** -- only used for WATCH optimistic concurrency. u32 gives 4 billion versions per key, wrapping is acceptable.

4. **`expires_at` should STAY as `Option<Instant>`** -- TTL/PTTL commands need millisecond precision relative to `Instant::now()`. Converting to unix timestamps loses sub-second precision and introduces system clock dependency. The `Option<Instant>` niche optimization already makes it 16 bytes (same as bare Instant). Not worth the complexity and precision loss.

### Anti-Patterns to Avoid

- **Holding MutexGuard across await points:** The batched version must ensure the guard is dropped before any `.await` call. Use block scoping: `let results = { let guard = lock(); ... }; // guard dropped`.
- **Unbounded batch sizes:** A malicious client could pipeline millions of commands. Cap batch size (e.g., 1024 frames) to bound lock hold time.
- **Double-locking in batch:** Current code acquires lock for eviction check, drops it, then re-acquires for dispatch. In batched version, do both in single acquisition.
- **Cloning Frame responses unnecessarily:** The current dispatch returns owned `Frame` values, so no extra cloning needed. Just collect into `Vec<Frame>`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Fast mutex | Custom spinlock | parking_lot::Mutex | Battle-tested, handles priority inversion, fairness |
| Batch frame collection | Manual poll/waker | `futures::FutureExt::now_or_never()` | Clean API, no unsafe, already in dependency tree |
| Unix timestamp helpers | Manual SystemTime dance | Simple `fn current_secs() -> u32` wrapper | Just wrap `SystemTime::now().duration_since(UNIX_EPOCH).as_secs() as u32` |

## Common Pitfalls

### Pitfall 1: MutexGuard Across Await (Send bound violation)
**What goes wrong:** `parking_lot::MutexGuard` is `!Send`. Holding it across `.await` causes compile error with tokio multi-thread runtime.
**Why it happens:** Batched response writing is async, tempting to keep lock during write.
**How to avoid:** Collect all results into `Vec<Frame>` under lock, drop guard, THEN write responses.
**Warning signs:** Compiler error about `MutexGuard` not implementing `Send`.

### Pitfall 2: Forgetting Connection-Level Commands in Batch Loop
**What goes wrong:** AUTH, SUBSCRIBE, MULTI/EXEC, CONFIG, BGSAVE are handled at connection level, not through dispatch. Batching must still route these correctly.
**Why it happens:** The current connection handler has ~15 special-case command intercepts before reaching dispatch.
**How to avoid:** In the batch execution loop, replicate the command routing logic. Or: separate "needs lock" commands from "connection-level" commands in the batch, handle connection-level ones outside the lock.
**Warning signs:** AUTH stops working, MULTI/EXEC breaks, SUBSCRIBE doesn't enter subscriber mode.

### Pitfall 3: Transaction Semantics with Batching
**What goes wrong:** MULTI/EXEC queues commands across multiple frames. Batching must not execute queued commands individually.
**Why it happens:** Batch loop tries to execute everything under one lock.
**How to avoid:** When `in_multi`, commands go to `command_queue`, not to dispatch. Only EXEC triggers atomic execution. The batch loop must maintain this state machine.
**Warning signs:** Transaction atomicity violations, QUEUED responses missing.

### Pitfall 4: Subscriber Mode Bypassed
**What goes wrong:** In subscriber mode, only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT are allowed. Batch loop might skip this check.
**Why it happens:** Subscriber mode uses a different `tokio::select!` branch in the current code.
**How to avoid:** Check `subscription_count > 0` before entering batch execution. Subscriber mode should NOT use batching (it uses bidirectional select).
**Warning signs:** Commands executing in subscriber mode that shouldn't.

### Pitfall 5: Entry Compaction Breaking Eviction Tests
**What goes wrong:** Eviction tests set `entry.last_access = Instant::now() - Duration::from_secs(100)` to simulate old entries. With u32 unix timestamps, this becomes `current_secs() - 100`.
**Why it happens:** Tests directly manipulate `Instant` fields.
**How to avoid:** Update all test code to use the new u32 timestamp API. Provide `Entry::with_last_access(secs: u32)` for testing.
**Warning signs:** Test compilation failures in eviction module.

### Pitfall 6: LFU Decay Precision Change
**What goes wrong:** `lfu_decay` currently uses `last_access.elapsed().as_secs() / 60`. With u32 timestamps, this becomes `(current_secs() - last_access) / 60`. Functionally identical, but must change the function signature.
**Why it happens:** `Instant::elapsed()` is a method, u32 subtraction is manual.
**How to avoid:** Change `lfu_decay` signature from `(counter, last_access: Instant, decay_time)` to `(counter, last_access: u32, decay_time)` and compute elapsed manually.

## Code Examples

### Batch Collection Pattern
```rust
// Source: derived from futures::FutureExt docs + project patterns
use futures::{StreamExt, FutureExt};

let first_result = framed.next().await;
let mut batch: Vec<Frame> = Vec::new();

match first_result {
    Some(Ok(frame)) => batch.push(frame),
    Some(Err(_)) => break,  // protocol error
    None => break,           // client disconnected
}

// Drain buffered frames without blocking
const MAX_BATCH: usize = 1024;
while batch.len() < MAX_BATCH {
    match framed.next().now_or_never() {
        Some(Some(Ok(frame))) => batch.push(frame),
        _ => break,
    }
}
```

### Batched Execution Under Lock
```rust
// Source: derived from current connection.rs dispatch pattern
let mut responses: Vec<Frame> = Vec::with_capacity(batch.len());
let mut aof_entries: Vec<Bytes> = Vec::new();

{
    let mut dbs = db.lock(); // parking_lot::Mutex -- no .unwrap() needed
    let db_count = dbs.len();

    // Eviction check once per batch (not per command)
    if has_writes {
        let rt = runtime_config.read().unwrap();
        if let Err(oom) = try_evict_if_needed(&mut dbs[selected_db], &rt) {
            // Return OOM for all remaining write commands
            // ... handle appropriately
        }
    }

    for frame in &batch {
        // Route connection-level commands vs dispatch
        let result = dispatch(&mut dbs[selected_db], frame.clone(), &mut selected_db, db_count);
        match result {
            DispatchResult::Response(f) => responses.push(f),
            DispatchResult::Quit(f) => {
                responses.push(f);
                should_quit = true;
                break;
            }
        }
    }
} // MutexGuard dropped here -- BEFORE any .await

// Write all responses outside the lock
for response in responses {
    if framed.send(response).await.is_err() {
        break;
    }
}
```

### Entry Struct Compaction
```rust
// Source: derived from current entry.rs + optimization requirements
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current unix timestamp in seconds as u32.
/// Wraps around in year 2106 -- acceptable for LRU/LFU tracking.
#[inline]
pub fn current_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: RedisValue,
    pub expires_at: Option<Instant>,  // kept for ms-precision TTL
    pub version: u32,                  // was u64
    pub last_access: u32,             // was Instant (16 bytes -> 4 bytes)
    pub access_counter: u8,
    // created_at: REMOVED (never read by any command)
}

impl Entry {
    pub fn new_string(value: Bytes) -> Entry {
        Entry {
            value: RedisValue::String(value),
            expires_at: None,
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    pub fn touch_lru(&mut self) {
        self.last_access = current_secs();
    }
}

/// Updated LFU decay using u32 timestamps.
pub fn lfu_decay(counter: u8, last_access: u32, lfu_decay_time: u64) -> u8 {
    if lfu_decay_time == 0 {
        return counter;
    }
    let now = current_secs();
    let elapsed_secs = now.wrapping_sub(last_access) as u64;
    let elapsed_min = elapsed_secs / 60;
    let decay = (elapsed_min / lfu_decay_time) as u8;
    counter.saturating_sub(decay)
}
```

### parking_lot Drop-in Replacement
```rust
// In connection.rs and anywhere using std::sync::Mutex for db:
// Change: use std::sync::Mutex;
// To:     use parking_lot::Mutex;

// Remove all .unwrap() on lock() calls -- parking_lot::Mutex::lock()
// returns MutexGuard directly (no Result, no poisoning).

// Before:
let mut dbs = db.lock().unwrap();
// After:
let mut dbs = db.lock();
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Per-command lock acquire/release | Batch N commands under single lock | This phase | Reduces lock operations by pipeline_depth factor |
| Instant for all timestamps | u32 unix seconds for tracking, Instant for TTL | This phase | Saves ~32 bytes per Entry |
| std::sync::Mutex | parking_lot::Mutex | This phase | Faster uncontended path, no poisoning |

## Open Questions

1. **Batch size cap**
   - What we know: Unbounded batches hold the lock too long
   - What's unclear: Optimal cap value (512? 1024? 4096?)
   - Recommendation: Start with 1024, tune based on benchmarks

2. **Connection-level commands in batch**
   - What we know: AUTH, BGSAVE, CONFIG, SUBSCRIBE, MULTI/EXEC/DISCARD/WATCH are intercepted before dispatch
   - What's unclear: Best way to handle these in a batch -- pre-filter, or inline in the batch loop?
   - Recommendation: Inline in the batch loop (replicate the current routing logic). Pre-filtering is more complex and risks missing edge cases.

3. **Eviction per-batch vs per-write-command**
   - What we know: Current code runs eviction before each write command
   - What's unclear: Is once-per-batch sufficient?
   - Recommendation: Run eviction once at batch start if batch contains writes. If OOM, reject remaining writes in batch.

4. **parking_lot for pubsub_registry Mutex too?**
   - What we know: PubSubRegistry also uses std::sync::Mutex
   - Recommendation: Yes, switch all std::sync::Mutex to parking_lot::Mutex for consistency and performance.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + criterion 0.8 (benchmarks) |
| Config file | Cargo.toml [dev-dependencies] |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| OPT-01 | Pipeline batching improves throughput at high concurrency | integration + benchmark | `./bench.sh --rust-only` | Partial (bench.sh exists) |
| OPT-02 | Entry compaction reduces memory per key | unit | `cargo test --lib storage::entry` | Needs update |
| OPT-03 | All existing tests still pass after refactoring | regression | `cargo test` | Yes |
| OPT-04 | Eviction still works with u32 last_access | unit | `cargo test --lib storage::eviction` | Needs update |
| OPT-05 | LFU decay works with u32 timestamps | unit | `cargo test --lib storage::entry::tests::test_lfu_decay` | Needs update |
| OPT-06 | Transaction atomicity preserved with batching | integration | `cargo test --test integration` | Yes |
| OPT-07 | Subscriber mode still works correctly | integration | `cargo test --test integration` | Yes |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full `cargo test` green + `./bench.sh --rust-only` shows improvement

### Wave 0 Gaps
- None -- existing test infrastructure covers all requirements. Tests need updating for Entry struct changes but no new test files needed.

## Sources

### Primary (HIGH confidence)
- Project source code analysis: `src/server/connection.rs`, `src/storage/entry.rs`, `src/storage/db.rs`, `src/storage/eviction.rs`
- Project benchmark data: `BENCHMARK.md` (measured 2026-03-23 on Apple M4 Pro)
- Rust std library docs: `std::time::Instant` size = 16 bytes, `Option<Instant>` = 16 bytes (niche optimization)
- `cargo search parking_lot` -- verified version 0.12.5 current

### Secondary (MEDIUM confidence)
- parking_lot performance characteristics (well-documented in crate README and Rust ecosystem lore)
- `futures::FutureExt::now_or_never()` pattern for non-blocking polling

### Tertiary (LOW confidence)
- Exact memory savings estimate (depends on struct alignment/padding -- measure with `std::mem::size_of`)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries are existing deps or well-known ecosystem standards
- Architecture: HIGH - patterns directly derived from code analysis, no speculation
- Pitfalls: HIGH - identified from actual code structure and known Rust async patterns
- Memory optimization: MEDIUM - savings estimate needs runtime measurement to confirm

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, no moving targets)
