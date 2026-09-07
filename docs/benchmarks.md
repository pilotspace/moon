---
title: "Benchmarks"
description: "Performance results, methodology, and reproduction steps."
---

# Benchmarks

!!! warning "Read the provenance label on every table"
    `CLAUDE.md` requires every published benchmark number to come from a **Linux**
    host. Only the tables on this page explicitly marked *Linux* satisfy that.
    Every table marked **macOS dev reference** was measured on an Apple M4 Pro
    (12 cores, 24 GB) and is kept as a development record only — it must not be
    quoted as a production result, and its ratios are **not** assumed to carry
    over to Linux. Two of them demonstrably did not: the macOS per-key memory
    tables claimed 27–35% less at ≥1 KB values, and the Linux re-measurement
    (BENCHMARK.md §3) puts it at **15–17%**; and every non-GET/SET command family
    runs 0.40–0.67× Redis at p≥8 (§2.12). All runs co-locate client and server using
    `redis-benchmark` — a closed-loop tool — with a fresh server instance per
    memory data point. The canonical report is
    [**BENCHMARK.md**](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md).

## Executive summary — Linux only

Every row below was measured on Linux. Where a condition is not recorded in the
source report, this table says so rather than filling it in.

| Metric | Moon vs Redis | Conditions |
|--------|:---:|------------|
| Peak GET (v0.8.7) | **2.40×** | GCE c3-standard-8 x86_64, Redis 7.0.15, `--shards 1`, c=50, p=64 — BENCHMARK.md §2.12 |
| Peak GET (ARM64, v0.8.7) | **2.29×** | GCE t2a-standard-8 Neoverse-N1, Redis 7.0.15, `--shards 1`, c=50, p=64 — §2.12 |
| Peak SET (v0.8.7) | **1.78× x86 / 2.02× ARM** | same runs as above — §2.12 |
| Every other command family | **0.40–0.67×** | INCR/LPUSH/SPOP/HSET at p≥8, both arches — §2.12 |
| Peak GET (v0.1.6, absolute) | **5.11M ops/sec (1.72×)** | GCloud c3-standard-8 x86_64, p=64. Redis `io-threads` setting and payload size **not recorded** — §2.1 |
| Peak SET (v0.1.6, absolute) | **3.50M ops/sec (1.92×)** | same run — §2.1 |
| Memory, ≥1 KB values | **15–17% less than Redis** | GCE c3-standard-8 **x86_64**, moon `--shards 1` vs **Redis 7.4.2/jemalloc**, `redis-benchmark -r N`, per-key RSS, 2026-09-04 — BENCHMARK.md §3.2. 9.5% at the smallest key count tested |
| Memory, 256 B values | tie (0.92–1.02×) | same run — §3.2 |
| Memory, 32 B values | **11–51% worse than Redis** | same run — §3.2 |
| Empty-server RSS | **1.7× worse than Redis** | same run — 12.6–12.9 MB vs 7.5–7.7 MB, §3.1. Cause unknown, tracked in [#821](https://github.com/pilotspace/moon/issues/821) |
| Memory, 64 B values (aarch64) | **1.16× worse than Redis** | GCE t2a-standard-8, moon `--shards 8` vs Redis 7.0.15 `--io-threads 8 --io-threads-do-reads yes`, `-r 200000` — §2.14 |
| Idle RSS, 8 shards (aarch64) | **1.26× worse than Redis** | same run — §2.14 |
| CPU per operation | **tie** | 10.55 µs vs 11.33 µs, inside Redis's own 11.9% spread — §2.14 |
| Shard scaling (s8/s1) | **1.42× / 2.14× / 3.79×** | p=1 / p=8 / p=64, explicitly-keyed families — §2.14 |
| AOF `everysec` SET p=16 | **1.32× Redis** | GCE c3-standard-8, Redis 7.0.15, `--shards 2` — §7.3 |
| AOF `always` SET p=16 | **0.91× Redis** | same run — §7.3 |
| Vector search (384d) | **12.7K QPS** | GCloud c3-standard-8 x86_64, HNSW + TurboQuant 8-bit, COSINE, 50K vectors, K=10 — §10.1 |
| Data correctness | **132/132 tests** | All types, 1/4/12 shards |

!!! warning "Scope of the pipelined GET/SET win"
    GET and SET are the two commands Moon serves from an inline byte path that
    bypasses frame construction and the dispatch table. `SET k v` runs 2.08×
    Redis while `SET k v EX 100` — same work, one disqualifying option — runs
    0.87×. The boundary is the fast path, not the engine. See BENCHMARK.md §2.12.

## Memory efficiency

### Per-key memory (Linux, x86_64, 1 shard) — measured 2026-09-04

**Host:** GCE `c3-standard-8`, Linux 6.17.0-1022-gcp, x86_64, 8 vCPU, 31 GB.
**Moon:** commit `d5f3501b`, `--shards 1`. **Oracle:** Redis 7.4.2 built against
**jemalloc 5.3.0** — both Redis binaries already on the host were libc-malloc
builds, which inflate Redis RSS and would have biased every ratio in Moon's
favour, so Redis was rebuilt from source for this run. **Method:**
`scripts/bench-resources.sh`, fresh server instance per data point,
`redis-benchmark -r N` for unique keys, per-key = (loaded RSS − baseline RSS) /
`DBSIZE`. Full twelve-point table: [BENCHMARK.md §3.2](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md).

| Value size | Keys | Redis/key | Moon/key | Moon / Redis | Result |
|:---:|:---:|---:|---:|:---:|---|
| 32 B | 63K | 123 B | 186 B | **1.51×** | **Moon 51% MORE** |
| 32 B | 316K | 127 B | 147 B | **1.16×** | **Moon 16% MORE** |
| 32 B | 632K | 129 B | 143 B | **1.11×** | **Moon 11% MORE** |
| 256 B | 63K | 408 B | 418 B | 1.02× | tie |
| 256 B | 316K | 410 B | 383 B | 0.93× | Moon 6.6% less |
| 256 B | 632K | 409 B | 377 B | 0.92× | Moon 7.8% less |
| 1 KB | 63K | 1,388 B | 1,256 B | 0.90× | Moon 9.5% less |
| 1 KB | 316K | 1,382 B | 1,155 B | 0.84× | **Moon 16.4% less** |
| 1 KB | 632K | 1,380 B | 1,172 B | 0.85× | **Moon 15.1% less** |
| 4 KB | 63K | 5,266 B | 4,404 B | 0.84× | **Moon 16.4% less** |
| 4 KB | 316K | 5,261 B | 4,360 B | 0.83× | **Moon 17.1% less** |
| 4 KB | 632K | 5,259 B | 4,352 B | 0.83× | **Moon 17.2% less** |

**Read it as:** at values **≥ 1 KB** Moon uses **15–17% less** memory per key,
falling to 9.5% at the smallest key count tested. At **256 B** the two are within
±8%. At **32 B Moon loses**, by 11–51%. This is `--shards 1`, x86_64, string
values, one point per cell — no repetitions, so no run-to-run spread is reported,
and the band between 32 B and 256 B is not sampled at all.

!!! danger "This replaces the retired '27–35% less memory' claim — and Moon is not what changed"
    The old published 1M × 1 KB row was Redis 1,571 B / Moon 1,153 B. Re-measured
    here: Redis **1,380 B** / Moon **1,172 B**. Moon's own figure moved **1.6%**;
    the *oracle* moved **12%**. The old claim was inflated by a Redis baseline
    measured on macOS and/or without jemalloc, not by anything Moon did. Against
    a jemalloc Redis on Linux the figure is 15–17%.

!!! tip
    Moon's large-value advantage comes from `HeapString(Vec<u8>)` (48 bytes
    overhead) vs Redis's `robj` + SDS chain (~64–80 bytes overhead). That model
    does **not** explain the 32 B loss — a 32-byte value is above Moon's 12-byte
    inline cutoff and the overhead arithmetic predicts a win there too. No cause
    is asserted. The TTL-overhead claim that used to sit here is **unverified**:
    the harness section that would measure it omits `redis-benchmark -r`, so it
    loads one key.

### Baseline RSS (empty server) — Linux, corrected

| Server | RSS (measured 2026-09-04) | Previously published |
|--------|---------------------------|----------------------|
| Redis 7.4.2 (jemalloc) | **7.5–7.7 MB** | 7.0 MB |
| Moon (1 shard) | **12.6–12.9 MB** — ~1.7× Redis | 7.0 MB |
| Moon (12 shards) | *not measured on Linux* | 15.7 MB (**unverified / stale**, macOS) |

Both measured ranges cover all 12 data points of the run. The "identical 7.0 MB"
row was an Apple M4 Pro development reference. The cause of Moon's 1.7× baseline
is not yet known and is tracked in
[#821](https://github.com/pilotspace/moon/issues/821). Do not quote 7.0 MB.

### Superseded: per-key memory (macOS dev reference, 1-shard)

!!! warning "Kept as a development record only — do not quote"
    Measured on an **Apple M4 Pro (12 cores, 24 GB)**. The Linux tables above
    supersede it. Its Redis column in particular is the source of the retired
    27–35% claim.

| Value size | Keys | Redis/key | Moon/key | Winner | Ratio |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 32 B | ~63K | 118 B | 147 B | Redis | 0.80x |
| 256 B | ~63K | 412 B | 407 B | Tied | 1.01x |
| 1,024 B | ~63K | 1,879 B | **1,207 B** | **Moon** | **1.56x** |
| 4,096 B | ~63K | 5,131 B | **4,352 B** | **Moon** | **1.18x** |

At 1M keys:

| Value size | Redis RSS | Moon RSS | Redis/key | Moon/key | Winner |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 32 B | 78.2 MB | 95.8 MB | 118 B | 147 B | Redis |
| 256 B | 231.5 MB | 234.4 MB | 372 B | 376 B | Tied |
| 1,024 B | 954.2 MB | **703.0 MB** | 1,571 B | **1,153 B** | **Moon** |

## Throughput

!!! note "macOS dev reference"
    Both tables in this subsection were measured on an Apple M4 Pro, not on
    Linux. The Linux throughput matrix is the executive summary above
    (BENCHMARK.md §2.12 / §2.14).

### Single-shard SET (macOS dev reference, pipeline=16, 50 clients)

| Value size | Redis SET/s | Moon SET/s | Ratio |
|:---:|:---:|:---:|:---:|
| 32 B | 1,298,701 | **1,754,386** | **1.35x** |
| 256 B | 1,219,512 | **1,639,344** | **1.34x** |
| 1,024 B | 1,010,101 | **1,030,928** | 1.02x |
| 4,096 B | 540,541 | **571,429** | 1.06x |

### Multi-shard peak throughput (macOS dev reference)

| Config | Moon | Redis | Ratio |
|--------|:---:|:---:|:---:|
| 8-shard GET p=16 c=50 | 2.60M | 1.41M | **1.84x** |
| 8-shard SET p=16 c=50 | 2.52M | 1.27M | **1.99x** |
| 4-shard GET p=64 c=50 | **3.79M** | 2.41M | **1.57x** |

## CPU efficiency

**On Linux, CPU per operation is a tie.** BENCHMARK.md §2.14 measures 10.55 µs/op
for moon `--shards 8` against 11.33 µs/op for Redis `--io-threads 8`
(GCE t2a-standard-8, `utime+stime` from `/proc/<pid>/stat`, 5 reps). Moon's 6.9%
edge sits inside Redis's own 11.9% run-to-run spread, so it is not a win. The
durable difference is stability: moon's CPU cost varies 2.0% run to run against
Redis's 11.9%.

The "45× / 23× better CPU" figure this page used to publish has been **removed**.
It was derived from an Apple M4 Pro table whose CPU column was sampled with
`ps -o %cpu=` — a process-lifetime average, not steady-state load — and whose CPU
and RPS columns were not taken in the same run. The underlying macOS table is kept
in BENCHMARK.md §5.1 as a development record, with no ratio derived from it.

## Persistence (AOF) performance

!!! note "macOS dev reference — the Linux figures are the campaign table below"
    This first table was measured on an Apple M4 Pro. The Linux write-path
    measurement (GCE c3-standard-8, Redis 7.0.15, `--shards 2`, 3 alternated
    reps) is the max-durability table that follows it: **1.32×** for `everysec`
    SET p=16 and **0.91×** for `always` SET p=16.

| Pipeline | Moon SET/s | vs Redis (no AOF) | vs Redis (AOF everysec) |
|:---:|:---:|:---:|:---:|
| p=1 | 146K | 0.95x | 0.95x |
| p=8 | 1,117K | 1.68x | **1.68x** |
| p=16 | 1,887K | 1.90x | **2.21x** |
| p=64 | **2,778K** | 1.80x | **2.75x** |

!!! note
    Moon's per-shard WAL avoids the global serialization point that Redis's single AOF file introduces. The advantage grows with pipeline depth because per-shard WAL scales linearly with shards.

### Max-durability (`appendfsync always`) and `everysec` write path

A 2026-07 write-path campaign (GCE c3-standard-8, Redis 7.0.15, `--shards 2`, 3 alternated reps) closed Moon's remaining AOF-on deficits:

| Policy / workload | Before | After | vs Redis |
|:---|---:|---:|:---:|
| `always` SET p16 | 5.7K (0.12x) | **40.1K** | **0.91x** |
| `always` SET p1 | ~3.2K | 3.1K | parity (fsync-device-bound) |
| `everysec` SET p16 | 605K | **789K** | **1.32x** |
| `everysec` SET p1 | 117K (0.80x) | **135K** | **0.99x parity** |
| Pub/sub fan-out delivery | 438 msg/s (drops) | **5.09M msg/s** | **1.04x, 0 drops** |

The wins come from per-batch group commit (one fsync per pipeline batch, not per command), a single coalesced `write_all` per batch, a park-free AOF-writer poll that removes a ~150K/sec futex-wake storm on the shard thread under `everysec`, and coalesced pub/sub delivery writes. Durability is unchanged (`always` = RPO 0, `everysec` = RPO ≤ 1s), verified by the SIGKILL crash-recovery matrix. Full detail: `BENCHMARK.md` §7.3.

## Latency

No Moon-vs-Redis latency comparison has been measured on Linux. The 8-shard p50
figure this page used to publish was an Apple M4 Pro development reference, taken
with `redis-benchmark` — a closed-loop tool, which under-reports latency once the
server saturates (see [Coordinated Omission](references.md)). It is retained with
its provenance in BENCHMARK.md §9.1 and is **not** republished here as a result.

Architecturally, multi-core parallelism reduces per-shard queue depth, so the
median request should wait less. That expectation has not been confirmed on a
Linux host.

## Production workload patterns (macOS dev reference)

!!! warning
    Measured on an Apple M4 Pro, not on Linux, and not reproduced there. On Linux,
    BENCHMARK.md §2.12 measures several of these command families (INCR, LPUSH,
    HSET) at **0.40–0.67× Redis** at p≥8, so these ratios should not be read as
    production expectations.

| Scenario | Description | Moon vs Redis |
|----------|-------------|:---:|
| Session store | 80% GET / 15% SET, 512B values | **1.24x** |
| Rate limiting | INCR with 100-200 clients | **1.15x** |
| Leaderboard | ZADD + ZRANGEBYSCORE | **1.06-1.25x** |
| App caching | 1KB-4KB values, MSET batch | **1.10-1.27x** |
| Job queue | LPUSH/RPOP producer-consumer | **1.06x** |
| User profiles | HSET, HGET | **1.10x** |

## How to reproduce

```bash
# Build with native CPU optimizations
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Memory and CPU benchmark
./scripts/bench-resources.sh --shards 1

# Production workload scenarios
./scripts/bench-production.sh --shards 1

# Multi-shard scaling
./scripts/bench-production.sh --shards 4
./scripts/bench-production.sh --shards 8

# Data consistency tests
./scripts/test-consistency.sh --shards 1
./scripts/test-consistency.sh --shards 4
```

!!! warning
    Co-located benchmarks (client and server on the same machine) are conservative. Separate-machine benchmarks with dedicated NICs show higher throughput. Always use `redis-benchmark -r <num_keys>` to generate unique keys. Use `redis-benchmark` 8.x which correctly handles `\r` in progress output.
