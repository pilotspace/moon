# Phase 23: End-to-End Performance Validation and Benchmark Suite - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Comprehensive performance validation of the fully reworked architecture against Redis 7.x and Redis Cluster. Use memtier_benchmark with Zipfian distribution, coordinated-omission-aware open-loop testing, and full throughput-latency curves. Validate memory efficiency, snapshot overhead, and per-key overhead on 64-core hardware. Publish results in BENCHMARK-v2.md.

</domain>

<decisions>
## Implementation Decisions

### Benchmark tool
- [auto] Primary: `memtier_benchmark` — industry standard, multi-threaded, HdrHistogram percentiles
- Secondary: custom Rust benchmark for micro-benchmarks (DashTable lookup, RESP parsing, etc.)
- Do NOT use redis-benchmark alone — it lacks coordinated-omission awareness and Zipfian distribution

### Workload configuration
- [auto] Pre-populate: 10M keys, 256-byte values (industry standard)
- Key distribution: Zipfian (alpha 0.99) — matches real-world hot-spot patterns
- GET:SET ratio: 1:10 (read-heavy, typical cache workload)
- Test duration: 180 seconds with 30-second warmup, 5 iterations reporting median with min/max
- Pipeline depths: 1, 16, 64
- Concurrency: 1 client, 10 clients, 50 clients, 100 clients

### Open-loop testing (coordinated omission)
- [auto] Use memtier's `--rate-limiting` for open-loop testing at fixed rates
- Test at: 10%, 25%, 50%, 75%, 90% of peak throughput
- Plot full throughput-latency curve (throughput on X, p99 latency on Y)
- Closed-loop results reported separately (standard benchmarks) with coordinated omission caveat

### Comparison targets
- [auto] Compare against:
  1. Single-instance Redis 7.x (same machine, single core)
  2. Redis Cluster with N shards (same machine, same core count)
  3. Our Phase 7 baseline (pre-rework) for before/after delta
- Client runs on SEPARATE machine (same AZ, 25+ GbE) — never co-located

### Performance targets (from blueprint)
- [auto] Throughput (no pipeline): >= 5M ops/sec on 64 cores
- Throughput (pipeline=16): >= 12M ops/sec
- P99 latency: < 200μs for GET/SET under 50% load
- Per-key memory overhead: <= 24 bytes
- Memory during snapshot: < 5% spike

### Memory benchmarks
- [auto] INFO MEMORY comparison: our server vs Redis with identical datasets
- Per-key overhead calculation: (total memory - base memory) / key count
- Snapshot memory spike: measure peak RSS during BGSAVE vs steady-state RSS
- Fragmentation ratio: allocated vs requested memory

### Reporting
- [auto] BENCHMARK-v2.md with:
  - Methodology section (hardware, OS, kernel version, tool versions, exact commands)
  - Throughput tables (ops/sec at each concurrency × pipeline depth)
  - Latency tables (p50, p90, p95, p99, p99.9, p99.99)
  - Throughput-latency curves (plotted or tabulated)
  - Memory comparison tables
  - Snapshot overhead measurements
  - Before/after delta vs Phase 7 baseline
  - Honest analysis of where we beat/match/lose to Redis Cluster

### Claude's Discretion
- Exact memtier_benchmark flags and parameters
- Whether to include flamegraph profiling results
- Graph generation (gnuplot, matplotlib, or tables only)
- Additional workload patterns (write-heavy, mixed, large values)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §How to benchmark fairly against Redis — memtier_benchmark methodology, Zipfian distribution, coordinated omission, fair comparison against Redis Cluster
- `.planning/architect-blue-print.md` §Quantitative performance targets on 64-core hardware — Target table

### Phase 6/7 baseline
- `BENCHMARK.md` — Phase 6/7 benchmark results (46KB) — before/after baseline
- `bench.sh` — Existing benchmark automation script (24KB) — extend for v2 benchmarks

### Prior phase results
- `.planning/phases/06-setup-and-benchmark-real-use-cases/06-03-PLAN.md` — Original benchmark methodology
- `.planning/phases/07-compare-architecture-resolve-bottlenecks-and-optimize-performance/07-03-PLAN.md` — Post-optimization benchmark comparison

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `bench.sh` — Server lifecycle management (start/wait/benchmark/kill) — extend for memtier_benchmark
- `BENCHMARK.md` — Template for results reporting
- `Criterion` benchmarks in `benches/` — micro-benchmark infrastructure

### Established Patterns
- bench.sh manages full server lifecycle — same pattern for v2
- BENCHMARK.md format with tables and analysis — extend for v2

### Integration Points
- memtier_benchmark must be installed on test machine
- Redis 7.x and Redis Cluster must be available for comparison
- Hardware requirements: ideally 64-core machine for target validation
- Network: separate client machine with 25+ GbE connection

</code_context>

<specifics>
## Specific Ideas

- Blueprint warns: "Avoid Dragonfly's 25x faster framing" — always compare against properly-deployed Redis Cluster
- Blueprint: Twitter's analysis shows production workloads have Zipfian α of 0.6–2.2
- Blueprint: 3-byte default values are meaningless — use 256-byte values
- Honest reporting: if we lose to Redis Cluster in some scenarios, document why and what would fix it

</specifics>

<deferred>
## Deferred Ideas

- Continuous benchmark CI (automated regression testing) — future DevOps phase
- Production load replay testing — requires real traffic capture

</deferred>

---

*Phase: 23-end-to-end-performance-validation-and-benchmark-suite*
*Context gathered: 2026-03-24*
