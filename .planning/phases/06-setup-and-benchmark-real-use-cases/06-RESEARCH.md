# Phase 6: Setup and Benchmark Real Use-Cases - Research

**Researched:** 2026-03-23
**Domain:** Performance benchmarking, project packaging, CI/CD
**Confidence:** HIGH

## Summary

This phase is a non-functional "polish" phase: no new server features, just benchmarking the completed Rust Redis server against Redis 7.x, documenting performance characteristics, and setting up project infrastructure (README, Dockerfile, CI). The server already implements 80+ commands across strings, hashes, lists, sets, sorted sets, pub/sub, transactions, persistence, and eviction -- all using jemalloc, tokio multi-thread runtime, and a Bytes-based RESP protocol layer.

The primary tool is `redis-benchmark` (ships with Redis) for external throughput/latency testing, plus the existing criterion micro-benchmarks. Redis is not currently installed on the dev machine and must be installed (e.g., `brew install redis`) to get both `redis-benchmark` and `redis-server` for baseline comparison. The benchmark script (`bench.sh`) should automate running the full suite against both servers and output markdown tables.

**Primary recommendation:** Install Redis via Homebrew for `redis-benchmark` and `redis-server` baseline. Write a `bench.sh` script that runs identical workloads against both servers and generates BENCHMARK.md tables. Use a simple multi-stage Dockerfile (no cargo-chef needed for a project this size). Use GitHub Actions with `dtolnay/rust-toolchain@stable` for CI.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Primary benchmark tool: `redis-benchmark` (ships with Redis installation)
- Secondary: `criterion` micro-benchmarks (existing in `benches/resp_parsing.rs`)
- Benchmark runner script: `bench.sh` automating full suite
- Results stored in `BENCHMARK.md` at project root
- Workload scenarios: SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP, SADD, HSET, SPOP, ZADD, ZRANGEBYSCORE, MSET (10 keys)
- Pipeline depths: 1, 16, 64
- Client concurrency: 1, 10, 50
- Data sizes: default (3 bytes), 256 bytes, 1KB, 4KB
- Persistence modes: no persistence, AOF everysec, AOF always
- Key count: 100K keys for memory benchmarks
- Metrics: ops/sec, p50/p99/p999 latency, RSS memory, CPU utilization, persistence overhead
- Comparison: Run identical suite against Redis 7.x on same machine, side-by-side tables
- No hard performance targets -- document actual performance honestly
- Identify top 3 bottlenecks with brief analysis
- README.md: project description, features, build instructions, usage, benchmark summary
- Dockerfile: multi-stage build
- `.github/` CI config: cargo test + cargo clippy + cargo fmt --check
- Cargo.toml metadata: description, license, repository

### Claude's Discretion
- Exact benchmark script implementation details
- Whether to add flamegraph profiling
- README structure and prose
- CI provider choice (GitHub Actions recommended)
- Docker base image choice

### Deferred Ideas (OUT OF SCOPE)
- Flamegraph profiling and optimization -- future work
- Continuous benchmarking in CI -- v2
- Load testing with realistic mixed workloads -- v2
</user_constraints>

## Standard Stack

### Core
| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| redis-benchmark | 7.x (via brew) | External throughput/latency benchmarking | Industry standard, directly comparable results |
| redis-server | 7.x (via brew) | Baseline comparison server | The reference implementation |
| criterion | 0.8 (already in Cargo.toml) | Micro-benchmarks for internal operations | Already used for RESP parsing benchmarks |

### Supporting
| Tool | Version | Purpose | When to Use |
|------|---------|---------|-------------|
| bash | system | bench.sh script runner | Orchestrates benchmark suite |
| GitHub Actions | N/A | CI pipeline | Automated test/lint on push |
| Docker | latest | Containerized builds | Deployment and reproducibility |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| redis-benchmark | memtier_benchmark | More features but less universally available; redis-benchmark is decision-locked |
| GitHub Actions | GitLab CI | GitHub Actions is the standard for GitHub-hosted Rust projects |
| debian-slim runtime | alpine | alpine needs musl target; debian-slim is simpler with glibc and jemalloc compatibility |
| scratch runtime | debian-slim | scratch is smaller but lacks shell for debugging; debian-slim is more practical |

**Installation (dev machine):**
```bash
brew install redis  # Provides redis-benchmark and redis-server
```

## Architecture Patterns

### Project File Structure (New Files)
```
project-root/
  README.md              # Project documentation
  BENCHMARK.md           # Performance results
  Dockerfile             # Multi-stage build
  .dockerignore          # Exclude target/, .git/, etc.
  .github/
    workflows/
      ci.yml             # Test + lint + format CI
  bench.sh               # Benchmark runner script
  benches/
    resp_parsing.rs      # Existing criterion benchmarks
```

### Pattern 1: Multi-Stage Dockerfile for Rust + jemalloc
**What:** Two-stage build: compile in `rust:1.85` (or latest stable), run in `debian:bookworm-slim`
**When to use:** Always for Rust projects with native dependencies like jemalloc
**Why debian-slim over alpine:** tikv-jemallocator requires glibc; musl builds are possible but add complexity

```dockerfile
# Stage 1: Build
FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -s /bin/bash rustredis
COPY --from=builder /app/target/release/rust-redis /usr/local/bin/rust-redis
USER rustredis
EXPOSE 6379
CMD ["rust-redis", "--bind", "0.0.0.0"]
```

**Key details:**
- `--bind 0.0.0.0` is required for Docker networking (default 127.0.0.1 won't work)
- Non-root user for security
- Bookworm-slim for glibc compatibility with jemalloc
- No cargo-chef needed -- this project compiles fast enough without it

### Pattern 2: GitHub Actions CI for Rust
**What:** Three-job workflow: test, clippy, rustfmt
**When to use:** Every push and PR to main

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --all-features

  clippy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy -- -D warnings

  fmt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt
      - run: cargo fmt --check
```

**Key details:**
- `dtolnay/rust-toolchain@stable` -- the standard modern action (replaces deprecated `actions-rs/toolchain`)
- `Swatinem/rust-cache@v2` -- caches cargo registry and target dir for faster builds
- Separate jobs so failures are clear and they run in parallel

### Pattern 3: Benchmark Script Structure
**What:** bash script that runs redis-benchmark against both servers and captures results
**When to use:** For the bench.sh automation

```bash
#!/usr/bin/env bash
set -euo pipefail

# Configuration
PORT_RUST=6399
PORT_REDIS=6400
REQUESTS=100000
KEYSPACE=100000

# Commands to benchmark
TESTS="set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zrangebyscore,mset"

# Function to run benchmark suite
run_suite() {
    local port=$1 label=$2
    for clients in 1 10 50; do
        for pipeline in 1 16 64; do
            for datasize in 3 256 1024 4096; do
                redis-benchmark -p "$port" -c "$clients" -P "$pipeline" \
                    -d "$datasize" -n "$REQUESTS" -r "$KEYSPACE" \
                    -t "$TESTS" -q --csv
            done
        done
    done
}
```

**Key redis-benchmark flags used:**
- `-p <port>` -- connect to specific port (run servers on different ports)
- `-c <clients>` -- concurrent connections
- `-P <pipeline>` -- pipeline depth
- `-d <size>` -- value data size in bytes
- `-n <requests>` -- total requests
- `-r <keyspace>` -- random key range
- `-t <tests>` -- comma-separated test list
- `-q` -- quiet mode (just ops/sec)
- `--csv` -- CSV output for parsing into markdown tables
- `-e` -- show server errors (useful for debugging unsupported commands)

### Anti-Patterns to Avoid
- **Running benchmarks on a loaded machine:** Close other applications, disable spotlight indexing during runs
- **Forgetting to build with --release:** Debug mode benchmarks are meaningless
- **Using default port for both servers:** Use different ports to run baseline comparison
- **Not warming up the server:** First benchmark run may include allocation overhead; consider a warmup pass
- **Benchmarking commands we don't support:** redis-benchmark's MSET does 10 keys by default and uses specific patterns; verify our server handles the exact command format before recording results

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| External benchmarking | Custom TCP load generator | redis-benchmark | Industry standard, comparable results, handles pipelining/concurrency |
| CSV to markdown | Custom parser | Simple awk/sed in bench.sh | Straightforward text transformation |
| CI pipeline | Custom scripts | GitHub Actions | Standard, free for public repos |
| Docker caching | Complex layer optimization | Simple two-stage build | Project compiles in <30s, cargo-chef overhead not justified |

## Common Pitfalls

### Pitfall 1: redis-benchmark Uses Commands We Don't Fully Support
**What goes wrong:** redis-benchmark may use command variants or options our server doesn't implement (e.g., specific MSET format, LMPOP, etc.)
**Why it happens:** redis-benchmark targets full Redis 7.x feature set
**How to avoid:** Use `-t` flag to specify only supported commands explicitly. Run with `-e` flag first to see any error responses. Test each command individually before running the full suite.
**Warning signs:** Abnormally low ops/sec or 0 results for a command

### Pitfall 2: Port Conflicts During Benchmarking
**What goes wrong:** Running our server on port 6379 conflicts with a Redis server also on 6379
**Why it happens:** Default port is the same
**How to avoid:** Run our server on a non-default port (e.g., 6399) and Redis on another (e.g., 6400). Or stop one server before starting the other.
**Warning signs:** "Address already in use" errors

### Pitfall 3: Inconsistent Benchmark Conditions
**What goes wrong:** Results are not comparable because servers ran under different conditions
**Why it happens:** System load, thermal throttling, background processes
**How to avoid:** Run both benchmark suites back-to-back on the same machine with minimal other load. Document hardware/OS. Consider multiple runs and averaging.
**Warning signs:** High variance between runs (>10% for the same workload)

### Pitfall 4: Memory Measurement Timing
**What goes wrong:** RSS measurement doesn't capture peak memory or reports stale data
**Why it happens:** OS memory reporting is lazy; RSS doesn't update instantly
**How to avoid:** Wait a moment after loading data before measuring RSS. Use `ps -o rss= -p $PID` or `/proc/$PID/status` on Linux. On macOS, use `ps -o rss= -p $PID`.
**Warning signs:** Memory numbers that seem too low for the data loaded

### Pitfall 5: Dockerfile jemalloc Build Failure
**What goes wrong:** tikv-jemallocator fails to compile in Docker
**Why it happens:** Missing build-essential or make in the builder image
**How to avoid:** The `rust:1.85-bookworm` image includes build-essential. If using a slimmer base, install `build-essential` and `make` first.
**Warning signs:** Compilation errors mentioning `jemalloc` or `make`

### Pitfall 6: AOF Benchmarking Creates Huge Files
**What goes wrong:** Running 100K+ requests with AOF=always creates massive AOF files
**Why it happens:** Every write is fsynced and logged
**How to avoid:** Use a temporary directory for AOF/RDB during benchmarks. Clean up between runs. Monitor disk space.
**Warning signs:** Slow disk, out of space errors

## Code Examples

### redis-benchmark: Standard Throughput Test
```bash
# Source: redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/
# Basic SET/GET test with 50 clients, 100K requests
redis-benchmark -p 6399 -c 50 -n 100000 -t set,get -q

# With pipelining (16 commands per pipeline)
redis-benchmark -p 6399 -c 50 -n 100000 -t set,get -P 16 -q

# With larger values (1KB)
redis-benchmark -p 6399 -c 50 -n 100000 -t set,get -d 1024 -q

# CSV output for parsing
redis-benchmark -p 6399 -c 50 -n 100000 -t set,get --csv

# With random keys (100K keyspace)
redis-benchmark -p 6399 -c 50 -n 100000 -r 100000 -t set,get -q
```

### Memory Measurement (macOS)
```bash
# Get RSS in KB for a process
ps -o rss= -p $(pgrep rust-redis)

# Load 100K keys then measure
redis-benchmark -p 6399 -n 100000 -r 100000 -t set -q
RSS_AFTER=$(ps -o rss= -p $(pgrep rust-redis))
echo "RSS: ${RSS_AFTER} KB"
```

### BENCHMARK.md Table Format
```markdown
## SET Performance

| Clients | Pipeline | Data Size | rust-redis (ops/sec) | Redis 7.x (ops/sec) | Ratio |
|---------|----------|-----------|---------------------|---------------------|-------|
| 1       | 1        | 3B        | XXX                 | XXX                 | X.XXx |
| 50      | 1        | 3B        | XXX                 | XXX                 | X.XXx |
| 50      | 16       | 3B        | XXX                 | XXX                 | X.XXx |
| 50      | 64       | 3B        | XXX                 | XXX                 | X.XXx |
```

### Cargo.toml Metadata Fields
```toml
[package]
name = "rust-redis"
version = "0.1.0"
edition = "2024"
description = "A high-performance Redis-compatible server written in Rust"
license = "MIT"
repository = "https://github.com/username/rust-redis"
keywords = ["redis", "database", "cache", "server"]
categories = ["database", "network-programming"]
```

### .dockerignore
```
target/
.git/
.github/
*.md
.planning/
benches/
tests/
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| actions-rs/toolchain | dtolnay/rust-toolchain@stable | 2023 | actions-rs is unmaintained; dtolnay is the standard |
| Single-stage Docker | Multi-stage Docker | Standard practice | 99% size reduction (1.4GB -> ~50MB) |
| Manual CI scripts | GitHub Actions with caching | 2022+ | Swatinem/rust-cache@v2 cuts CI times by 50%+ |

## Existing Project Assets

### Commands Implemented (for README feature list)
**Connection:** PING, ECHO, QUIT, SELECT, COMMAND, INFO, AUTH
**Strings (16):** GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX
**Keys (12):** DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST, TYPE, UNLINK, SCAN, KEYS, RENAME, RENAMENX
**Hashes (12):** HSET, HGET, HDEL, HMSET, HMGET, HGETALL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSETNX, HSCAN
**Lists (12):** LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LPOS
**Sets (14):** SADD, SREM, SMEMBERS, SCARD, SISMEMBER, SMISMEMBER, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SRANDMEMBER, SPOP, SSCAN
**Sorted Sets (18):** ZADD, ZREM, ZSCORE, ZCARD, ZINCRBY, ZRANK, ZREVRANK, ZPOPMIN, ZPOPMAX, ZSCAN, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZCOUNT, ZLEXCOUNT, ZUNIONSTORE, ZINTERSTORE
**Pub/Sub:** SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUBLISH
**Transactions:** MULTI, EXEC, DISCARD, WATCH, UNWATCH
**Persistence:** BGSAVE, BGREWRITEAOF
**Config:** CONFIG GET, CONFIG SET

### CLI Flags (for README usage section)
From `src/config.rs`:
- `--bind` (default: 127.0.0.1)
- `--port` / `-p` (default: 6379)
- `--databases` (default: 16)
- `--requirepass` (optional)
- `--appendonly` (default: no)
- `--appendfsync` (default: everysec)
- `--save` (optional, e.g., "3600 1 300 100")
- `--dir` (default: .)
- `--dbfilename` (default: dump.rdb)
- `--appendfilename` (default: appendonly.aof)
- `--maxmemory` (default: 0 = unlimited)
- `--maxmemory-policy` (default: noeviction)
- `--maxmemory-samples` (default: 5)

### Existing Infrastructure
- **Allocator:** tikv-jemallocator (global allocator in main.rs)
- **Runtime:** tokio multi-thread
- **Logging:** tracing + tracing-subscriber with env filter
- **Existing benchmarks:** `benches/resp_parsing.rs` (criterion 0.8, 7 benchmarks for RESP parse/serialize)
- **Existing tests:** `tests/integration.rs` + unit tests throughout
- **Edition:** 2024, Rust 1.94.0

## Open Questions

1. **Redis installation availability**
   - What we know: Redis is not installed on the dev machine (no brew redis)
   - What's unclear: Whether the user wants to install Redis 7.x for baseline comparison, or just benchmark our server alone
   - Recommendation: bench.sh should handle both cases -- skip Redis baseline if redis-server is not available, but document how to install for comparison

2. **MSET benchmark format**
   - What we know: redis-benchmark's MSET test uses "MSET key_000000000001 value ... key_000000000010 value" (10 keys)
   - What's unclear: Whether our MSET handles the exact random key format redis-benchmark generates
   - Recommendation: Test manually first with `redis-benchmark -t mset -n 1 -q` against our server to verify compatibility

3. **CPU utilization measurement**
   - What we know: User wants CPU utilization during benchmarks
   - What's unclear: Best cross-platform approach (macOS dev, Linux CI)
   - Recommendation: Use `ps -o %cpu -p $PID` for a snapshot, or run `time` on the benchmark command for wall clock vs CPU time

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + criterion 0.8 |
| Config file | Cargo.toml (bench section exists) |
| Quick run command | `cargo test` |
| Full suite command | `cargo test --all-features && cargo bench` |

### Phase Validation Approach
This phase produces artifacts (docs, scripts, configs) rather than server code. Validation is primarily manual:

| Deliverable | Validation Method | Automated? |
|-------------|-------------------|------------|
| bench.sh | Script runs without errors | Semi (can dry-run) |
| BENCHMARK.md | Results populated | Manual |
| README.md | Renders correctly | Manual |
| Dockerfile | `docker build .` succeeds | `docker build -t rust-redis .` |
| CI config | Push triggers workflow | Manual (verify on GitHub) |
| Cargo.toml metadata | `cargo metadata` shows fields | `cargo metadata --format-version=1 \| jq .packages[0].description` |

### Sampling Rate
- **Per task:** Verify file created and well-formed
- **Phase gate:** All deliverables exist, bench.sh runs, Docker builds, cargo test passes

### Wave 0 Gaps
None -- this phase creates new files rather than modifying server code. No test infrastructure gaps.

## Sources

### Primary (HIGH confidence)
- [Redis benchmark documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/) -- redis-benchmark flags and usage
- Project codebase (src/command/mod.rs, src/config.rs, Cargo.toml) -- existing features and configuration

### Secondary (MEDIUM confidence)
- [depot.dev Rust Dockerfile best practices](https://depot.dev/blog/rust-dockerfile-best-practices) -- multi-stage build patterns
- [dtolnay/rust-toolchain](https://github.com/dtolnay/rust-toolchain) -- GitHub Actions Rust setup
- [Swatinem/rust-cache](https://github.com/Swatinem/rust-cache) -- CI caching

### Tertiary (LOW confidence)
- None -- all findings verified with official sources

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- redis-benchmark is the canonical tool, locked by user decision
- Architecture: HIGH -- Dockerfile and CI patterns are well-established
- Pitfalls: HIGH -- based on direct project knowledge (jemalloc, port config, command support)

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, no fast-moving dependencies)
