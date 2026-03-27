# Phase 7: Resolve Bottlenecks and Optimize Performance - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

Address the 3 measured bottlenecks from Phase 6 benchmarks: Mutex contention under high concurrency (0.26x Redis at 50 clients/pipeline 64), memory overhead per key (3.8x Redis), and CPU waste on lock contention (257% vs 90%). Re-benchmark to validate improvements.

</domain>

<decisions>
## Implementation Decisions

### Bottleneck 1: Lock Contention — Pipeline Batching
- Batch pipelined commands under a single lock acquisition — collect all available frames from the codec stream, acquire lock once, execute all, release, then write all responses
- This is the highest-impact, lowest-risk optimization — addresses the main gap at high concurrency
- Do NOT migrate to full actor model — that's a large architectural change for marginal gain over batching
- Keep tokio multi-thread runtime — single-thread removes I/O parallelism benefits
- Current problem: each pipelined command acquires/releases the lock individually, causing contention proportional to pipeline depth * client count

### Bottleneck 2: Lock Hold Time — Minimize Critical Section
- Move response serialization OUTSIDE the lock — acquire lock, execute command, clone the Frame response, release lock, then serialize and write
- Move AOF logging OUTSIDE the lock — collect AOF entries during execution, send to AOF channel after lock release
- Current problem: lock held through response serialization and AOF channel send, unnecessarily extending critical section
- Combined with batching: acquire lock → execute N commands → collect N responses + N AOF entries → release lock → serialize all → send AOF all

### Bottleneck 3: Memory — Entry Struct Compaction
- Replace `created_at: Instant` with `created_at: u32` (unix timestamp seconds) — saves 8 bytes, sufficient precision
- Replace `last_access: Instant` with `last_access: u32` (unix timestamp seconds) — saves 8 bytes
- Replace `version: u64` with `version: u32` — saves 4 bytes, 4 billion versions per key is more than enough
- Keep `access_counter: u8` as-is (already compact, Morris counter)
- Remove `created_at` entirely if not used by any command — check if anything reads it
- Target: reduce Entry overhead from ~48 bytes metadata to ~13 bytes
- Do NOT implement compact encodings (ziplist/listpack) in this phase — separate large effort

### Re-benchmarking Strategy
- Run same `bench.sh` suite before optimization (baseline already captured in BENCHMARK.md)
- Run again after all optimizations
- Add "Before/After Optimization" comparison section to BENCHMARK.md
- Focus metrics: ops/sec at 50 clients pipeline 64 (worst case), memory RSS with 100K keys, CPU utilization

### Claude's Discretion
- Exact read-batch loop implementation in connection handler
- Whether to use `try_next()` or `ready!` for non-blocking frame collection
- Response buffer strategy (Vec<Frame> vs pre-allocated)
- Whether `expires_at` should also be compacted (currently Option<Instant>)
- Benchmark warm-up strategy

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Optimization Targets
- `src/server/connection.rs:64` — `Arc<Mutex<Vec<Database>>>` shared state
- `src/server/connection.rs` (711 lines) — connection handler with per-command lock acquire/release
- `src/storage/entry.rs` (305 lines) — Entry struct with metadata fields
- `src/storage/db.rs` (635 lines) — Database operations under lock

### Benchmark Data
- `BENCHMARK.md` — Current benchmark results with bottleneck analysis
- `bench.sh` — Benchmark runner script for re-validation

### Architecture Context
- `.planning/research/PITFALLS.md` — Pitfall #5 (wrong threading model)
- `.planning/research/ARCHITECTURE.md` — Lock contention discussion

</canonical_refs>

<code_context>
## Existing Code Insights

### Optimization Targets
- `connection.rs` line 64: `db: Arc<Mutex<Vec<Database>>>` — the contention point
- Connection handler loop: reads one frame → locks → dispatches → unlocks → writes response → repeats
- AOF send happens inside the lock hold in some paths
- Entry struct: `value + expires_at(16) + created_at(16) + last_access(16) + version(8) + access_counter(1)` = ~57 bytes metadata

### What NOT to Change
- The `Arc<Mutex>` pattern itself — just reduce how long and how often it's held
- Command handler signatures `(&mut Database, &[Frame]) -> Frame` — keep pure
- Protocol layer — already optimized in Phase 1
- Persistence layer — AOF everysec overhead is already negligible

### Integration Points
- `connection.rs` handler loop — main optimization site for batching + lock minimization
- `entry.rs` Entry struct — compaction site
- `db.rs` — may need timestamp helper methods for compacted format
- `BENCHMARK.md` — append optimization results

</code_context>

<specifics>
## Specific Ideas

- The read-batch pattern: `while let Some(frame) = stream.try_next().now_or_never() { batch.push(frame) }` — collect all available frames without blocking
- For AOF batching: collect `Vec<Bytes>` of serialized commands during execution, send all after lock release
- Measure before/after with exact same bench.sh parameters for fair comparison
- Consider using `parking_lot::Mutex` instead of `std::sync::Mutex` — faster for short critical sections (no syscall for uncontended case)

</specifics>

<deferred>
## Deferred Ideas

- Full actor model migration (mpsc channel to single data-owning task) — only if batching doesn't close the gap sufficiently
- Compact encodings (ziplist/listpack equivalents for small collections) — separate phase
- Database sharding (multiple Mutexes by key hash) — alternative to actor model
- io_uring integration for I/O — Linux-only optimization
- Custom allocator per-database — arena allocation

</deferred>

---

*Phase: 07-compare-architecture-resolve-bottlenecks-and-optimize-performance*
*Context gathered: 2026-03-23*
