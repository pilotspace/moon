# Phase 6: Setup and Benchmark Real Use-Cases - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

Performance validation of the completed Rust Redis server. Run industry-standard benchmarks (redis-benchmark), measure throughput/latency/memory across all data types, compare against Redis 7.x on the same hardware, and document results with bottleneck analysis. Also set up proper project packaging (README, build instructions, Docker).

</domain>

<decisions>
## Implementation Decisions

### Benchmark Tooling
- Primary: `redis-benchmark` (ships with Redis installation) — industry-standard, directly comparable
- Secondary: `criterion` micro-benchmarks for internal operations (already have RESP parse benchmarks from Phase 1)
- Add benchmark runner script (`bench.sh`) that automates the full suite and captures results
- Results stored in `BENCHMARK.md` at project root — version-controlled, reproducible

### Workload Scenarios
- Standard redis-benchmark commands: SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP, SADD, HSET, SPOP, ZADD, ZRANGEBYSCORE, MSET (10 keys)
- Pipeline depths: 1 (no pipeline), 16, 64
- Client concurrency: 1, 10, 50 concurrent connections
- Data sizes: default (3 bytes value), 256 bytes, 1KB, 4KB
- Persistence modes: no persistence, AOF everysec, AOF always
- Key count: 100K keys for memory benchmarks

### Performance Metrics
- Ops/sec (throughput) per command at each concurrency/pipeline level
- p50 / p99 / p999 latency
- Memory usage (RSS) under load and at rest with 100K keys
- CPU utilization during benchmark runs
- Persistence overhead (ops/sec delta with AOF on vs off)

### Comparison Baseline
- Run identical redis-benchmark suite against Redis 7.x on same machine
- Report side-by-side in BENCHMARK.md tables
- No hard performance targets — document actual performance honestly
- Identify top 3 bottlenecks with brief analysis for future optimization

### Project Setup
- README.md with: project description, features, build instructions, usage, benchmark results summary
- Dockerfile for easy build/run
- `.github/` CI config (cargo test + cargo clippy + cargo fmt --check)
- Cargo.toml metadata (description, license, repository)

### Claude's Discretion
- Exact benchmark script implementation details
- Whether to add flamegraph profiling
- README structure and prose
- CI provider choice (GitHub Actions recommended)
- Docker base image choice

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Codebase
- `Cargo.toml` — project metadata to update
- `src/main.rs` — server entry point (for README usage docs)
- `src/config.rs` — CLI flags (for README documentation)
- `benches/resp_parsing.rs` — existing criterion benchmarks to extend

### Redis Benchmark Reference
- redis-benchmark is part of the Redis installation — no local docs needed

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `benches/resp_parsing.rs` — existing criterion benchmark harness
- `src/config.rs` — all CLI flags documented (for README)
- All command modules — feature list for README
- `tests/integration.rs` — test patterns for CI config

### Established Patterns
- criterion for micro-benchmarks
- cargo test for CI validation
- jemalloc as global allocator (important for benchmark consistency)

### Integration Points
- Project root — README.md, Dockerfile, BENCHMARK.md, bench.sh
- `.github/workflows/` — CI configuration
- `Cargo.toml` — metadata fields

</code_context>

<specifics>
## Specific Ideas

- Benchmark script should output both raw numbers and a formatted markdown table for easy copy into BENCHMARK.md
- README should include a "quick start" section: `cargo build --release && ./target/release/rust-redis`
- Dockerfile should use multi-stage build (builder + slim runtime)
- Include a "Known Limitations" section in README (no cluster, no Lua, no streams)

</specifics>

<deferred>
## Deferred Ideas

- Flamegraph profiling and optimization — future work
- Continuous benchmarking in CI — v2
- Load testing with realistic mixed workloads — v2

</deferred>

---

*Phase: 06-setup-and-benchmark-real-use-cases*
*Context gathered: 2026-03-23*
