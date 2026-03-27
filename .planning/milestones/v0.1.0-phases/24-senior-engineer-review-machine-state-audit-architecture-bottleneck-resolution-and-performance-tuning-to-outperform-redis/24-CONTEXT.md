# Phase 24: Senior Engineer Review — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Full-stack senior engineer audit of the rust-redis codebase: fix all failing tests, eliminate compiler warnings, profile hot paths, resolve bottlenecks, tune for throughput, and validate that the system outperforms single-instance Redis 7.x by >= 1.5x at 10+ concurrent clients on available dev hardware.

</domain>

<decisions>
## Implementation Decisions

### Test stabilization (fix before optimize)
- Fix ALL 9 failing integration tests before any optimization work
- 5 cluster tests (cluster_info/myid/nodes/addslots/keyslot) — likely runtime config or test setup issues
- 2 hash tests (test_hscan, test_hash_commands) — likely Phase 15 listpack encoding bugs
- 1 list test (test_list_commands) — likely Phase 15 listpack encoding bug
- 1 AOF test (test_aof_restore_on_startup) — likely persistence format mismatch with new types
- Full root-cause analysis for each: run in isolation, capture exact error, trace through code path
- Do NOT revert listpack — fix the actual encoding/upgrade bugs

### Code hygiene
- Clean ALL 25 compiler warnings (dead code, unused imports, suggestions)
- Run clippy and fix ALL clippy warnings — clippy catches performance anti-patterns (unnecessary clones, suboptimal iteration, missing inlines)
- Dead code removal especially important before profiling — dead code confuses flamegraphs

### Profiling strategy
- **CPU profiling**: cargo-flamegraph or Instruments.app (macOS ARM compatible)
- **Heap profiling**: mimalloc stats API for allocation counting (DHAT needs Linux, document Linux-only steps)
- **Micro-benchmarks**: Extend Criterion with GET/SET end-to-end latency benchmarks
- **Async profiling**: tokio-console for task scheduling, wake patterns, poll durations
- **Target workload**: GET/SET with 10 clients, 80/20 read/write, 256-byte values — the bread-and-butter Redis workload
- Start with GET/SET, pivot to whatever flamegraph reveals as hot

### Optimization priorities
- **Throughput first** (ops/sec on GET/SET) — headline metric, latency and memory follow naturally
- **Minimal unsafe**: only use unsafe where profiling proves > 5% improvement on hot path. We already have unsafe in DashTable/CompactValue — don't add more without proof
- Prefer safe optimizations: allocation reduction, batching, inlining, buffer reuse

### Outperformance definition
- **Target**: >= 1.5x single-instance Redis 7.x throughput at 10+ concurrent clients
- Achievable because sharded architecture uses multiple cores vs Redis's single thread
- **Client setup**: co-located redis-benchmark/memtier, clearly labeled as "conservative lower bound"
- Both Redis and rust-redis on same machine — relative comparison is valid
- Document that real production numbers need separate client machine with 25+ GbE

### Claude's Discretion
- Which specific compiler/clippy warnings to prioritize
- Exact profiling tool flags and configuration
- Whether to add #[inline] annotations or let LTO handle it
- Order of optimizations after profiling reveals bottlenecks
- Whether to implement custom Criterion benchmarks or use redis-benchmark for smoke test

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Quantitative performance targets — Target numbers and methodology
- `.planning/architect-blue-print.md` §How to benchmark fairly against Redis — Fair comparison methodology

### Phase 23 benchmark infrastructure
- `bench-v2.sh` — Benchmark automation script (smoke test mode available)
- `BENCHMARK-v2.md` — Methodology and results template
- `BENCHMARK.md` — Phase 6/7 baseline results for before/after comparison

### Current machine state (snapshot at context gathering)
- Release build: compiles with 25 warnings
- 981 lib tests pass, 100/109 integration tests pass (9 failing)
- 44,576 lines of Rust
- Failing tests: cluster_info, cluster_myid, cluster_nodes, cluster_addslots, cluster_keyslot, test_hscan, test_hash_commands, test_list_commands, test_aof_restore_on_startup

### Key source files for hot-path analysis
- `src/server/connection.rs` — Connection handler (both sharded and non-sharded paths)
- `src/shard/mod.rs` — Shard event loop (SPSC polling, dispatch, wakeup hooks)
- `src/protocol/parse.rs` — RESP parser (memchr + atoi from Phase 8)
- `src/storage/db.rs` — Database accessors (DashTable, CompactEntry)
- `src/storage/dashtable/mod.rs` — DashTable get/insert/remove
- `src/storage/dashtable/segment.rs` — Segment SIMD probing + prefetch
- `src/command/string.rs` — GET/SET command handlers

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `bench-v2.sh` with `--smoke-test` mode — fast dev validation (1K keys, 10s)
- Criterion benchmarks in `benches/` — resp_parsing, entry_memory, bptree_memory
- `cargo-flamegraph` compatible (release profile with debug symbols)

### Established Patterns
- All command handlers operate on `&mut Database` — single point for hot-path optimization
- Pipeline batching via `now_or_never()` — already optimized in Phase 7
- SPSC channel polling in shard event loop — cross-shard overhead measurable

### Integration Points
- Profiling targets the sharded path (`handle_connection_sharded`)
- Benchmark comparison uses `redis-benchmark` or `memtier_benchmark` against both servers
- Results documented in PERFORMANCE-AUDIT.md (new deliverable)

</code_context>

<specifics>
## Specific Ideas

- The 9 failing integration tests are the #1 priority — broken code paths make all profiling meaningless
- Phase 15 listpack-first encoding likely introduced regressions in hash/list commands — test those command families end-to-end
- Cluster tests may fail because the test server doesn't start with `--cluster-enabled` flag
- AOF test may fail because new data types (Stream, compact encodings) aren't handled in AOF replay
- After fixes: flamegraph the GET/SET path to find the top 5 CPU consumers

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 24-senior-engineer-review*
*Context gathered: 2026-03-25*
