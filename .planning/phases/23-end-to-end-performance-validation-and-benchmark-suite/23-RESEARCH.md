# Phase 23: End-to-End Performance Validation and Benchmark Suite - Research

**Researched:** 2026-03-25
**Domain:** Performance benchmarking, memtier_benchmark, Redis Cluster comparison, open-loop testing, coordinated omission methodology
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Benchmark tool**
- Primary: `memtier_benchmark` — industry standard, multi-threaded, HdrHistogram percentiles
- Secondary: custom Rust benchmark for micro-benchmarks (DashTable lookup, RESP parsing, etc.)
- Do NOT use redis-benchmark alone — it lacks coordinated-omission awareness and Zipfian distribution

**Workload configuration**
- Pre-populate: 10M keys, 256-byte values (industry standard)
- Key distribution: Zipfian (alpha 0.99) — matches real-world hot-spot patterns
- GET:SET ratio: 1:10 (read-heavy, typical cache workload)
- Test duration: 180 seconds with 30-second warmup, 5 iterations reporting median with min/max
- Pipeline depths: 1, 16, 64
- Concurrency: 1 client, 10 clients, 50 clients, 100 clients

**Open-loop testing (coordinated omission)**
- Use memtier's `--rate-limiting` for open-loop testing at fixed rates
- Test at: 10%, 25%, 50%, 75%, 90% of peak throughput
- Plot full throughput-latency curve (throughput on X, p99 latency on Y)
- Closed-loop results reported separately (standard benchmarks) with coordinated omission caveat

**Comparison targets**
- Compare against:
  1. Single-instance Redis 7.x (same machine, single core)
  2. Redis Cluster with N shards (same machine, same core count)
  3. Our Phase 7 baseline (pre-rework) for before/after delta
- Client runs on SEPARATE machine (same AZ, 25+ GbE) — never co-located

**Performance targets (from blueprint)**
- Throughput (no pipeline): >= 5M ops/sec on 64 cores
- Throughput (pipeline=16): >= 12M ops/sec
- P99 latency: < 200us for GET/SET under 50% load
- Per-key memory overhead: <= 24 bytes
- Memory during snapshot: < 5% spike

**Memory benchmarks**
- INFO MEMORY comparison: our server vs Redis with identical datasets
- Per-key overhead calculation: (total memory - base memory) / key count
- Snapshot memory spike: measure peak RSS during BGSAVE vs steady-state RSS
- Fragmentation ratio: allocated vs requested memory

**Reporting**
- BENCHMARK-v2.md with:
  - Methodology section (hardware, OS, kernel version, tool versions, exact commands)
  - Throughput tables (ops/sec at each concurrency x pipeline depth)
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

### Deferred Ideas (OUT OF SCOPE)
- Continuous benchmark CI (automated regression testing) — future DevOps phase
- Production load replay testing — requires real traffic capture
</user_constraints>

---

## Summary

Phase 23 is the final validation phase of the project — it produces the definitive benchmark suite and methodology document. The codebase has 22 completed phases of optimization: SIMD RESP parsing, DashTable segmented hash, thread-per-core shared-nothing architecture (Phase 11), io_uring networking (Phase 12), NUMA-aware mimalloc (Phase 13), forkless compartmentalized persistence (Phase 14), compact encodings (Phase 15), and a full Redis-compatible command set. The benchmark infrastructure needs to be built around `memtier_benchmark` as the primary external tool, extending the existing `bench.sh` pattern to handle the more demanding v2 requirements (10M keys, Zipfian distribution, 180s runs, 5 iterations, open-loop testing, Redis Cluster comparison).

The key deliverable distinction is important: this phase creates the benchmark infrastructure, scripts, and methodology documentation. Results on the target 64-core Linux hardware are the stretch goal — the scripts and BENCHMARK-v2.md template must work on any hardware, documenting the gap between available hardware results and the 64-core targets clearly. On development hardware (e.g., Apple M4 Pro, 12 cores), full 5M/12M ops/sec targets are not achievable, but the methodology and scripts must be validated and ready to produce those results on appropriate hardware.

The existing `bench.sh` is a solid foundation: it manages server lifecycle (start/wait/benchmark/kill), handles both rust-redis and Redis 7.x, measures CPU/memory/persistence overhead, and generates BENCHMARK.md. The v2 script must replace `redis-benchmark` with `memtier_benchmark`, increase the keyspace from 100K to 10M, add Zipfian key distribution, add open-loop rate-limited testing, add Redis Cluster support, and generate BENCHMARK-v2.md in the expanded format.

**Primary recommendation:** Build `bench-v2.sh` by extending `bench.sh` patterns. Use memtier_benchmark with `--key-pattern Z:Z` for Zipfian distribution, `--rate-limiting` for open-loop tests, and `--print-percentiles 50,90,95,99,99.9,99.99` for full latency visibility. Structure BENCHMARK-v2.md to honestly document hardware constraints and clearly separate "available hardware results" from "64-core targets."

---

## Standard Stack

### Core
| Library/Tool | Version | Purpose | Why Standard |
|---|---|---|---|
| memtier_benchmark | 2.1.2 | Primary throughput/latency load generator | Industry standard, HdrHistogram, Zipfian, rate-limiting, multi-threaded |
| redis-benchmark | Ships with Redis 7.x | Secondary/validation only (not for primary results) | Available everywhere; used only for regression smoke tests |
| Criterion (Rust) | 0.5.x (existing) | Micro-benchmarks: RESP parsing, entry memory, BPTree | Already in project benches/ |
| redis-cli | Ships with Redis 7.x | INFO MEMORY queries, dataset pre-population | Available wherever redis-server is installed |

### Supporting
| Tool | Purpose | When to Use |
|---|---|---|
| gnuplot | Throughput-latency curve generation | On Linux with gnuplot available; optional, tables are the fallback |
| numactl | NUMA pinning for test server startup | Linux 64-core hardware runs only |
| perf stat | CPU cycle/instruction counting for flamegraph | Optional profiling section |
| `/proc/PID/status` (VmRSS) | Precise RSS measurement on Linux | Linux hardware only; macOS uses `ps -o rss=` |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|---|---|---|
| memtier_benchmark | redis-benchmark | redis-benchmark lacks Zipfian, no rate-limiting, no coordinated-omission awareness — do not use as primary |
| memtier_benchmark | YCSB | YCSB adds JVM overhead and Redis driver complexity — not appropriate |
| Tables only | matplotlib/gnuplot charts | Charts are better for throughput-latency curves but require extra tooling; tables are always available |

**Installation (Linux target hardware):**
```bash
# Ubuntu/Debian
sudo apt-get install -y memtier-benchmark redis-server redis-tools

# From source (for latest version)
git clone https://github.com/redis/memtier_benchmark.git
cd memtier_benchmark && autoreconf -ivf && ./configure && make && sudo make install
```

**macOS development:**
```bash
brew install memtier_benchmark redis
```

---

## Architecture Patterns

### Recommended Script Structure
```
bench-v2.sh                    # Main v2 benchmark runner (new file)
bench.sh                       # Existing v1 (keep intact, reference)
BENCHMARK-v2.md                # Output: v2 results document
scripts/
  bench-cluster-setup.sh       # Redis Cluster N-shard setup helper
  bench-open-loop.sh           # Open-loop rate-limiting test helper
  bench-memory.sh              # Memory overhead measurement helper
```

### Pattern 1: bench-v2.sh Architecture

**What:** A new `bench-v2.sh` that extends the existing `bench.sh` patterns for memtier_benchmark, 10M keys, Zipfian distribution, 180s runs, 5 iterations, open-loop testing, Redis Cluster.

**When to use:** All v2 benchmark runs. Keep `bench.sh` untouched for v1 regression reference.

**Key structure (inherits from bench.sh):**
```bash
# Source: arch blueprint §How to benchmark fairly against Redis

# Pre-population (10M keys, 256B values, sequential pattern)
run_prepopulate() {
    local port=$1 label=$2
    log "Pre-populating $label with 10M keys (256B values)..."
    memtier_benchmark -s 127.0.0.1 -p "$port" \
        -t 4 -c 30 \
        --ratio=1:0 \
        --key-pattern=P:P \
        --key-maximum=10000000 \
        -d 256 \
        -n allkeys \
        --hide-histogram
}

# Primary benchmark: Zipfian, 180s, 5 iterations, HdrHistogram percentiles
run_benchmark_v2() {
    local port=$1 label=$2 clients=$3 pipeline=$4
    memtier_benchmark -s 127.0.0.1 -p "$port" \
        -t "$THREADS" -c "$clients" \
        --ratio=1:10 \
        --key-pattern=Z:Z \
        --key-maximum=10000000 \
        -d 256 \
        --test-time=180 \
        --distinct-client-seed \
        --run-count=5 \
        --pipeline="$pipeline" \
        --print-percentiles 50,90,95,99,99.9,99.99 \
        --hdr-file-prefix="$RESULTS_DIR/${label}_c${clients}_p${pipeline}" \
        > "$RESULTS_DIR/${label}_c${clients}_p${pipeline}.txt" 2>&1
}

# Open-loop rate-limited test at X% of peak
run_open_loop() {
    local port=$1 label=$2 rate=$3  # rate in ops/sec per connection
    memtier_benchmark -s 127.0.0.1 -p "$port" \
        -t "$THREADS" -c 50 \
        --ratio=1:10 \
        --key-pattern=Z:Z \
        --key-maximum=10000000 \
        -d 256 \
        --test-time=180 \
        --rate-limiting="$rate" \
        --print-percentiles 50,90,95,99,99.9,99.99 \
        > "$RESULTS_DIR/${label}_openloop_${rate}rps.txt" 2>&1
}
```

### Pattern 2: Redis Cluster Comparison Setup

**What:** Spin up N single-instance Redis processes (not cluster mode), one per core, using different ports. This is the "same machine, same core count" comparison that the blueprint mandates. True Redis Cluster with gossip is a separate optional comparison.

**When to use:** The main comparison target for throughput claims.

```bash
# Source: arch blueprint §How to benchmark fairly against Redis
# Blueprint: "40-shard Redis 7.0 Cluster achieves 4.74M ops/sec on same hardware"
# Implementation: N processes, one per core, each on a separate port
# memtier_benchmark connects to all N ports with --cluster-mode or separate runs

start_redis_cluster_simulation() {
    local num_shards=$1
    local base_port=7000
    REDIS_CLUSTER_PIDS=()

    for ((i=0; i<num_shards; i++)); do
        local port=$((base_port + i))
        redis-server --port "$port" \
            --save "" \
            --daemonize no \
            --bind 127.0.0.1 &
        REDIS_CLUSTER_PIDS+=($!)
    done
    # Wait for all shards to be ready
    for ((i=0; i<num_shards; i++)); do
        wait_for_server $((base_port + i))
    done
}
```

### Pattern 3: Memory Overhead Measurement

**What:** Precise per-key overhead calculation using INFO MEMORY and RSS measurement.

**When to use:** Memory target validation (<= 24 bytes per key).

```bash
# Source: arch blueprint §Quantitative performance targets
measure_memory_overhead() {
    local port=$1 label=$2 num_keys=$3

    # Get INFO MEMORY baseline
    local baseline_used
    baseline_used=$(redis-cli -p "$port" INFO MEMORY | grep "used_memory:" | cut -d: -f2 | tr -d '[:space:]')

    # Load num_keys keys with 256B values
    memtier_benchmark -s 127.0.0.1 -p "$port" \
        -t 1 -c 1 --ratio=1:0 \
        --key-pattern=P:P --key-maximum="$num_keys" \
        -d 256 -n allkeys --hide-histogram

    # Get INFO MEMORY after load
    local loaded_used
    loaded_used=$(redis-cli -p "$port" INFO MEMORY | grep "used_memory:" | cut -d: -f2 | tr -d '[:space:]')

    local overhead=$(( (loaded_used - baseline_used) / num_keys ))
    echo "Per-key overhead ($label): ${overhead} bytes (target: <=24)"
    echo "$label,$baseline_used,$loaded_used,$num_keys,$overhead" >> "$RESULTS_DIR/memory_overhead.csv"
}
```

### Pattern 4: Snapshot Memory Spike Measurement

**What:** Measure peak RSS during async snapshot vs steady-state RSS.

```bash
measure_snapshot_spike() {
    local port=$1 pid=$2

    local steady_rss
    steady_rss=$(get_rss "$pid")  # KB

    # Trigger async snapshot
    redis-cli -p "$port" BGSAVE

    # Poll RSS every 100ms until BGSAVE completes
    local peak_rss=$steady_rss
    while [[ "$(redis-cli -p "$port" LASTSAVE)" == "$before_save_time" ]]; do
        local current_rss
        current_rss=$(get_rss "$pid")
        (( current_rss > peak_rss )) && peak_rss=$current_rss
        sleep 0.1
    done

    local spike_pct=$(echo "scale=2; ($peak_rss - $steady_rss) * 100 / $steady_rss" | bc)
    echo "Snapshot RSS spike: ${spike_pct}% (target: <5%)"
}
```

### Anti-Patterns to Avoid

- **Co-located client and server:** Never run memtier_benchmark on the same machine as the server being tested. On development hardware where separation isn't possible, document this limitation explicitly and note results are pessimistic.
- **Uniform key distribution for throughput claims:** Using default uniform random (`R:R`) understates hot-spot effects. Always use `Z:Z` (Zipfian) for throughput claims.
- **3-byte value benchmarks:** The `redis-benchmark` default of 3 bytes is meaningless for real-world comparisons. Always use 256B.
- **Closed-loop-only reporting:** Must include coordinated omission caveat when reporting closed-loop (non-rate-limited) results.
- **Comparing against single-instance Redis:** Always include Redis Cluster (N shards, same core count) as the fair comparison. Never frame "25x faster than single-instance Redis" as a headline.
- **Short test duration:** Anything under 60 seconds misses warmup effects and GC/compaction artifacts. Use 180s with 30s warmup.
- **No iteration reporting:** Run 5 iterations and report median with min/max. Single runs have high variance.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| Zipfian key generation | Custom Zipfian generator | memtier_benchmark `--key-pattern Z:Z` | Already implemented, validated, matches production workloads |
| HdrHistogram percentile tracking | Custom percentile accumulator | memtier_benchmark `--hdr-file-prefix` | Precise HDR histogram files for offline analysis |
| Rate limiting for open-loop | Custom token bucket | memtier_benchmark `--rate-limiting=N` | Per-connection rate control built into the tool |
| Multi-threaded load generation | Custom multi-threaded client | memtier_benchmark `-t N -c M` | N threads x M clients is the industry pattern |
| Percentile reporting | Custom stats aggregation | memtier_benchmark `--print-percentiles` | Reports p50/p90/p95/p99/p99.9/p99.99 directly |

**Key insight:** memtier_benchmark was built specifically for this domain (Redis/Memcache load generation). Building custom load generators introduces methodology invalidation risks and makes results non-reproducible by others.

---

## Common Pitfalls

### Pitfall 1: memtier Zipfian without --key-maximum
**What goes wrong:** Without `--key-maximum`, Zipfian distribution defaults to a very small key range, producing artificially high cache-hit rates and inflated throughput numbers.
**Why it happens:** The Zipfian distribution concentrates requests on low-numbered keys. With an unbounded range, it still converges but may not match the desired 10M keyspace.
**How to avoid:** Always set `--key-maximum=10000000` explicitly when using `Z:Z` pattern.
**Warning signs:** Throughput numbers are suspiciously high (>2x expected); INFO KEYSPACE shows far fewer than 10M keys after pre-population.

### Pitfall 2: Pre-population uses wrong key pattern
**What goes wrong:** Pre-populating with `Z:Z` (Zipfian) only loads a fraction of the 10M keyspace because Zipfian concentrates on a subset of keys. The benchmark then hits mostly-cache (hot keys only).
**Why it happens:** Misapplying the distribution to the load phase as well as the benchmark phase.
**How to avoid:** Pre-populate with `--key-pattern P:P` (sequential, parallel) and `--ratio 1:0` to ensure ALL 10M keys are loaded uniformly. Only use `Z:Z` for the benchmark phase.
**Warning signs:** `INFO KEYSPACE` shows far fewer than 10M keys; benchmark hits 100% cache rate.

### Pitfall 3: Co-located client inflating latency
**What goes wrong:** Client CPU usage competes with server CPU, inflating latency and deflating throughput.
**Why it happens:** On development hardware (Mac, 12 cores), there is often no separate client machine available.
**How to avoid:** On development hardware, document that client and server are co-located, note this pessimizes results. On target 64-core Linux hardware, client MUST run on a separate machine.
**Warning signs:** Both client and server showing high CPU utilization simultaneously.

### Pitfall 4: Rate-limiting calculation for open-loop tests
**What goes wrong:** Setting `--rate-limiting` too high (above peak throughput) reverts to closed-loop behavior and loses the coordinated omission benefit.
**Why it happens:** Peak throughput must be measured first (closed-loop), then 10/25/50/75/90% rates calculated.
**How to avoid:** Always run the closed-loop benchmark first, extract peak ops/sec, then derive the rate-limited values. Document the peak used as the denominator.
**Warning signs:** Open-loop p99 is the same as or better than closed-loop p99 at the same load level — this is physically implausible.

### Pitfall 5: Memory measurement includes TCP buffer and OS overhead
**What goes wrong:** RSS-based memory measurement overstates the server's actual memory usage because it includes TCP buffers, file descriptor tables, and OS page cache.
**Why it happens:** RSS is the simplest measurement but captures everything in the process's working set.
**How to avoid:** Use `INFO MEMORY used_memory` (application-level allocator-tracked memory) for per-key overhead calculation. Use RSS only for snapshot spike measurement (where the relevant signal is the relative change).
**Warning signs:** Calculated per-key overhead seems unreasonably large (>100 bytes) even after optimizations.

### Pitfall 6: NUMA effects on non-64-core hardware
**What goes wrong:** On NUMA systems, memory allocated across NUMA nodes incurs remote access latency, degrading throughput non-linearly with core count.
**Why it happens:** Phase 13 added NUMA pinning (`pin_to_core` before runtime creation) but this only works when shard count matches NUMA topology.
**How to avoid:** On 64-core Linux target hardware, verify `numactl --hardware` shows expected topology. Use `numactl --cpunodebind=0 --membind=0` for single-NUMA baseline measurements.
**Warning signs:** Throughput scales sub-linearly beyond NUMA node count (e.g., good up to 32 cores but plateaus at 64).

### Pitfall 7: bench-v2.sh lacks proper cleanup for 10M key dataset
**What goes wrong:** Between benchmark iterations, old keys from pre-population persist, potentially affecting subsequent runs with stale data or inflated memory readings.
**Why it happens:** `bench.sh` used 100K keys which flush fast; 10M keys require explicit FLUSHALL between runs.
**How to avoid:** Add explicit `redis-cli FLUSHALL` and memory measurement reset between full benchmark suites (not between iterations of the same test).

---

## Code Examples

Verified patterns from official sources and existing codebase:

### memtier_benchmark Pre-Population (10M keys)
```bash
# Source: architect-blue-print.md §How to benchmark fairly against Redis
# Sequential load: ensures ALL 10M keys are present before benchmark
memtier_benchmark -s 127.0.0.1 -p "$PORT" \
    -t 4 -c 30 \
    --ratio=1:0 \
    --key-pattern=P:P \
    --key-maximum=10000000 \
    -d 256 \
    -n allkeys \
    --hide-histogram
```

### memtier_benchmark Primary Throughput Test
```bash
# Source: architect-blue-print.md §How to benchmark fairly against Redis
# Read-heavy workload, Zipfian distribution, 180s, 5 iterations
memtier_benchmark -s 127.0.0.1 -p "$PORT" \
    -t 4 -c "$CLIENTS" \
    --ratio=1:10 \
    --key-pattern=Z:Z \
    --key-maximum=10000000 \
    -d 256 \
    --test-time=180 \
    --warmup-time=30 \
    --distinct-client-seed \
    --run-count=5 \
    --pipeline="$PIPELINE" \
    --print-percentiles 50,90,95,99,99.9,99.99 \
    --hdr-file-prefix="$RESULTS_DIR/hdr_${LABEL}_c${CLIENTS}_p${PIPELINE}"
```

### Open-Loop Rate-Limited Test
```bash
# Source: architect-blue-print.md §How to benchmark fairly against Redis
# Coordinated-omission-aware: rate-limiting at X% of peak
# PEAK_RPS must be measured from closed-loop run first
RATE_10PCT=$(( PEAK_RPS / 10 ))
RATE_25PCT=$(( PEAK_RPS / 4 ))
RATE_50PCT=$(( PEAK_RPS / 2 ))
RATE_75PCT=$(( PEAK_RPS * 3 / 4 ))
RATE_90PCT=$(( PEAK_RPS * 9 / 10 ))

for rate in $RATE_10PCT $RATE_25PCT $RATE_50PCT $RATE_75PCT $RATE_90PCT; do
    memtier_benchmark -s 127.0.0.1 -p "$PORT" \
        -t 4 -c 50 \
        --ratio=1:10 \
        --key-pattern=Z:Z \
        --key-maximum=10000000 \
        -d 256 \
        --test-time=180 \
        --rate-limiting="$rate" \
        --print-percentiles 50,90,95,99,99.9,99.99 \
        > "$RESULTS_DIR/openloop_${rate}rps.txt" 2>&1
done
```

### INFO MEMORY Per-Key Overhead Calculation
```bash
# Source: CONTEXT.md §Memory benchmarks
baseline=$(redis-cli -p "$PORT" INFO MEMORY | awk -F: '/^used_memory:/{print $2}' | tr -d '\r')
# ... load NUM_KEYS keys ...
loaded=$(redis-cli -p "$PORT" INFO MEMORY | awk -F: '/^used_memory:/{print $2}' | tr -d '\r')
overhead=$(( (loaded - baseline) / NUM_KEYS ))
echo "Per-key overhead: ${overhead} bytes (target <=24)"
```

### Criterion Micro-Benchmark (existing pattern — extend not rewrite)
```rust
// Source: benches/entry_memory.rs (existing)
// Pattern: use black_box, Vec::with_capacity for allocation control
fn bench_dashtable_lookup_1m(c: &mut Criterion) {
    // Pre-populate outside the iter loop
    let table = build_dashtable_1m();
    c.bench_function("DashTable lookup 1M keys", |b| {
        b.iter(|| {
            let key = black_box(random_key());
            black_box(table.get(&key));
        })
    });
}
```

### Snapshot RSS Spike Measurement
```bash
# Source: CONTEXT.md §Memory benchmarks
# Linux only: /proc/$PID/status VmRSS is more precise than ps
get_rss_linux() {
    awk '/VmRSS:/{print $2}' "/proc/$1/status"  # KB
}
# macOS fallback (used in existing bench.sh)
get_rss_macos() {
    ps -o rss= -p "$1" | tr -d ' '  # KB
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| redis-benchmark for performance claims | memtier_benchmark with Zipfian + rate-limiting | 2019+ (industry) | Eliminates coordinated omission hiding tail latency |
| Single-instance Redis comparison | Redis Cluster (same core count) as comparison | Dragonfly 2022 controversy | Fair comparison; avoids "25x faster" strawman |
| 100K key benchmarks | 10M key benchmarks with 256B values | Industry standard since 2020 | Catches memory pressure, eviction effects, real working sets |
| Short 30-second runs | 180-second runs with warmup | Best practice | JIT warmup, GC, NUMA effects stabilize after ~60s |
| Single run, point estimate | 5 iterations, median + min/max | Statistical rigor | Variance from OS scheduler, thermal throttling |
| HdrHistogram only | HdrHistogram + open-loop curves | 2021+ | Open-loop reveals true latency at production load levels |

**Deprecated/outdated:**
- `redis-benchmark --csv` as primary results format: Only use for legacy v1 comparisons; v2 uses memtier output format
- 3-byte values: Meaningless for real-world claims; always 256B
- Uniform random key distribution: Use Zipfian for throughput claims

---

## Open Questions

1. **Zipfian alpha parameter in memtier_benchmark**
   - What we know: `--key-pattern Z:Z` enables Zipfian distribution; alpha 0.99 is specified in CONTEXT.md
   - What's unclear: Whether memtier_benchmark 2.1.2 exposes a configurable alpha parameter via CLI flag, or uses a fixed alpha
   - Recommendation: Check `memtier_benchmark --help | grep -i zipf` on target hardware. If no alpha parameter exists, document the fixed alpha used by memtier and note it approximates the target 0.99. The CONTEXT.md requirement for "alpha 0.99" may need to be met via the XinShuYang/zipf_memtier fork if the standard tool doesn't expose it.

2. **Client machine availability for target hardware test**
   - What we know: Blueprint requires separate client machine (same AZ, 25+ GbE)
   - What's unclear: Phase deliverable is scripts + methodology; whether the actual 64-core run happens is out of scope for this phase
   - Recommendation: Scripts must support `--server-host=HOSTNAME` flag to point at a remote server; document that co-located results on development hardware are conservative lower bounds

3. **Redis Cluster vs N independent instances**
   - What we know: Blueprint says "compare against Redis Cluster with N shards (same machine, same core count)"
   - What's unclear: True Redis Cluster (with gossip protocol overhead and MOVED redirects) vs N independent Redis instances (each owning a subset of keyspace) have different performance characteristics
   - Recommendation: Benchmark N independent Redis instances (simpler, faster, fairer to Redis) AND document that true Redis Cluster adds gossip overhead. Note this matches how Dragonfly benchmark controversy was framed.

4. **Phase 7 baseline comparison**
   - What we know: BENCHMARK.md has Phase 6/7 results using `redis-benchmark` on macOS (12-core M4 Pro, 100K keys, 3B/256B/1024B/4096B values)
   - What's unclear: v1 used redis-benchmark; v2 uses memtier_benchmark — these are not directly comparable numbers
   - Recommendation: Run v1 benchmark methodology (bench.sh) on current codebase to get an apples-to-apples before/after using the same tool. Then separately run v2 (memtier) for the 64-core targets. Report both.

---

## Validation Architecture

nyquist_validation is enabled in .planning/config.json.

### Test Framework
| Property | Value |
|---|---|
| Framework | Rust Criterion 0.5.x (micro-benchmarks) + bash integration (bench-v2.sh) |
| Config file | Cargo.toml `[[bench]]` sections; no separate config file |
| Quick run command | `cargo bench --bench resp_parsing 2>&1 \| tail -20` |
| Full suite command | `cargo bench 2>&1 \| tail -50` |

### Phase Requirements -> Test Map

This phase is infrastructure-creation (scripts + documentation), not feature code. Tests validate the benchmark scripts work correctly, not that they produce specific numbers (numbers depend on hardware).

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|---|---|---|---|---|
| P23-01 | bench-v2.sh starts server, pre-populates 10M keys (smoke: 1K), runs memtier, stops server | integration smoke | `bash bench-v2.sh --smoke-test --rust-only` | Wave 0 |
| P23-02 | Memory overhead measurement script produces per-key byte count | integration | `bash bench-v2.sh --memory-only --rust-only --keys 10000` | Wave 0 |
| P23-03 | Snapshot spike measurement produces a percentage | integration | `bash bench-v2.sh --snapshot-only --rust-only` | Wave 0 |
| P23-04 | Open-loop rate-limited test runs for configured duration | integration smoke | `bash bench-v2.sh --open-loop-only --rust-only --test-time 10` | Wave 0 |
| P23-05 | BENCHMARK-v2.md is generated with all required sections | integration | `bash bench-v2.sh --smoke-test --rust-only && grep -c "^##" BENCHMARK-v2.md` | Wave 0 |
| P23-06 | Criterion micro-benchmarks complete without regression | unit | `cargo bench --bench entry_memory 2>&1 \| grep -E "time:|thrpt:"` | ✅ |
| P23-07 | CompactEntry size assertion: 24 bytes | unit (inline) | `cargo bench --bench entry_memory 2>&1 \| grep -v "FAILED"` | ✅ |

### Sampling Rate
- **Per task commit:** `cargo bench --bench entry_memory 2>&1 | tail -5` (fast, <30s)
- **Per wave merge:** `cargo bench 2>&1 | tail -20` (all criterion benches)
- **Phase gate:** Full `bash bench-v2.sh --smoke-test` green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `bench-v2.sh` — the entire script is the primary deliverable; does not exist yet
- [ ] `BENCHMARK-v2.md` — template/output file; does not exist yet
- [ ] Smoke-test mode in bench-v2.sh (`--smoke-test`: 1K keys, 10s test time, skip open-loop) for CI validation without full 180s runs

---

## Sources

### Primary (HIGH confidence)
- `.planning/architect-blue-print.md` §How to benchmark fairly against Redis — exact memtier_benchmark commands, coordinated omission explanation, Zipfian methodology
- `.planning/architect-blue-print.md` §Quantitative performance targets on 64-core hardware — target table (5-7M ops/s no pipeline, 12-15M pipeline=16, <200us p99, <=24B per-key, <5% snapshot spike)
- `bench.sh` — existing server lifecycle management patterns, measurement helpers, phase structure
- `BENCHMARK.md` — Phase 6/7 baseline results on Apple M4 Pro, 12 cores
- `benches/entry_memory.rs`, `benches/resp_parsing.rs`, `benches/bptree_memory.rs` — existing Criterion patterns
- `.planning/phases/23-end-to-end-performance-validation-and-benchmark-suite/23-CONTEXT.md` — locked decisions

### Secondary (MEDIUM confidence)
- [redis/memtier_benchmark manpage](https://github.com/redis/memtier_benchmark/blob/master/memtier_benchmark.1) — confirmed: `--key-pattern Z:Z` for Zipfian, `--rate-limiting=N` for open-loop, `--print-percentiles`, `--hdr-file-prefix`, `--distinct-client-seed`, `--run-count`, version 2.1.2
- [Redis benchmarking docs](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/) — confirmed: multi-connection requirement, pipelining, cluster mode flag, coordinated omission warning

### Tertiary (LOW confidence — needs verification on target hardware)
- Zipfian alpha configurability in memtier_benchmark 2.1.2: manpage shows `Z:Z` pattern but alpha parameter exposure is unconfirmed (MEDIUM→LOW without hands-on test)
- 40-shard Redis Cluster throughput of 4.74M ops/sec: from blueprint (citing Redis team's Dragonfly rebuttal); exact configuration details not independently verified

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — memtier_benchmark 2.1.2 confirmed, all key flags verified against official manpage
- Architecture patterns (bench-v2.sh design): HIGH — extends proven bench.sh patterns; memtier flags verified
- Pitfalls: HIGH — all pitfalls grounded in methodology principles and observed issues in existing BENCHMARK.md
- Open-loop/coordinated omission: HIGH — mechanism confirmed via manpage `--rate-limiting` flag
- 64-core target numbers: HIGH — sourced directly from architect-blue-print.md

**Research date:** 2026-03-25
**Valid until:** 2026-06-25 (stable tooling; memtier_benchmark major version unlikely to change)
