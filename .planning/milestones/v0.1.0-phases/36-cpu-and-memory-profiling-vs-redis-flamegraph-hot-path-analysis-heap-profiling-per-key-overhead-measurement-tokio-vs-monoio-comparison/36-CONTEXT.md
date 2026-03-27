# Phase 36: CPU and Memory Profiling vs Redis — Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Create a comprehensive profiling suite that measures and compares CPU usage (flamegraphs, hot-path breakdown), memory efficiency (per-key overhead, RSS growth, fragmentation), and runtime comparison (Tokio vs Monoio) against Redis 8.6.1. Produces a PROFILING-REPORT.md with actionable data.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion.

**Profiling tools available on macOS:**
- `cargo bench` — Criterion micro-benchmarks (already have 4: resp_parsing, get_hotpath, entry_memory, bptree_memory)
- `cargo flamegraph` — requires `cargo install flamegraph` + dtrace on macOS
- `instruments` — Xcode profiling (CPU, allocations, leaks)
- `redis-cli INFO memory` — used_memory, used_memory_rss, mem_fragmentation_ratio
- `redis-cli INFO stats` — instantaneous_ops_per_sec, total_commands_processed
- `redis-cli DEBUG SLEEP` — not available in rust-redis
- `ps -o rss` — process RSS measurement
- `jemalloc mallctl` — heap stats (if jemalloc feature enabled)
- `mimalloc stats` — allocation stats (default allocator)

**What to measure:**

1. **CPU Hot Path (flamegraph)**
   - Build with `debug = true` in release profile (temporarily)
   - Run under load, capture flamegraph
   - Compare: rust-redis Tokio vs Monoio vs Redis
   - Identify: what % is parsing, dispatch, hash table, I/O, runtime overhead

2. **Per-Key Memory Overhead**
   - Insert N keys (10K, 50K, 100K, 500K, 1M) with 256B values
   - Measure RSS before/after
   - Calculate per-key delta
   - Compare: rust-redis vs Redis
   - Test: strings, hashes (5 fields), lists (10 elements), sorted sets (10 members)

3. **Memory Fragmentation**
   - Insert 1M keys, delete 500K random keys, measure RSS vs used_memory
   - Compare fragmentation ratio between mimalloc and Redis/jemalloc

4. **Criterion Micro-Benchmarks (existing)**
   - Run existing benches: resp_parsing, get_hotpath, entry_memory, bptree_memory
   - Add new bench: compact_key (inline vs heap path comparison)

5. **Tokio vs Monoio CPU Comparison**
   - Same workload on both runtimes
   - Measure CPU % via `ps -o %cpu`
   - Measure ops/watt efficiency (ops/sec per CPU%)

**Output:** `PROFILING-REPORT.md` with tables, charts description, and optimization recommendations.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `benches/resp_parsing.rs` — RESP parser benchmarks
- `benches/get_hotpath.rs` — GET command hot-path breakdown (11 stages)
- `benches/entry_memory.rs` — CompactEntry memory benchmark
- `benches/bptree_memory.rs` — B+ tree memory benchmark
- `bench-production.sh` — production benchmark script (10 scenarios)
- `src/storage/db.rs:257` — `estimate_memory()` per-entry tracking

### Key Files
- `Cargo.toml` — profile.release settings
- `benches/` — Criterion benchmarks
- `bench-production.sh` — benchmark runner

</code_context>

<specifics>
## Specific Ideas

Create `profile.sh` script that automates the full profiling run and generates PROFILING-REPORT.md.

</specifics>

<deferred>
## Deferred Ideas

- Linux-specific perf profiling (perf stat, perf record)
- eBPF-based syscall tracing
- Continuous profiling integration (pprof endpoint)

</deferred>
