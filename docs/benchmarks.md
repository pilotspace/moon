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
    tables claimed 27–35% less at ≥1 KB values, and the Linux re-measurement of
    2026-09-08 ([BENCHMARK.md §3](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md))
    puts it at **16%** at 1 KB; and every non-GET/SET command family runs
    **0.45–0.87× Redis at p≥8**
    ([BENCHMARK.md §2](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md)).
    All runs co-locate client and server using `redis-benchmark` — a closed-loop
    tool — with a fresh server instance per memory data point. The canonical
    report is [**BENCHMARK.md**](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md),
    re-measured 2026-09-08; every run it supersedes is preserved verbatim in the
    [**benchmark archive**](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md),
    which is where the `§2.x` / `§3.x` / `§7.x` / `§10.x` section numbers now live.

## Executive summary — Linux only

Every row below was measured on Linux. Rows dated **2026-09-08** come from the
current report; rows with an earlier date are the most recent measurement of that
subsystem and were **not** re-measured on that date. Where a condition is not
recorded in the source report, this table says so rather than filling it in.

| Metric | Moon vs Redis | Conditions |
|--------|:---:|------------|
| GET, p=64 | **1.62× x86 / 1.60× ARM** | 2026-09-08, moon `ae6cd003` (v0.8.9), GCE c3-standard-8 x86_64 + t2a-standard-8 aarch64, Redis 7.0.15, `--shards 1`, c=50, keys over `-r 100000`, n=5 interleaved — BENCHMARK.md §2 |
| SET, p=64 | **1.32× x86 / 1.55× ARM** | same run — §2 |
| GET / SET, p=8 | **1.20× / 1.20× x86 · 1.18× / 1.19× ARM** | same run — §2 |
| Every other command family, p=8 | **0.62–0.81× x86 / 0.64–0.77× ARM** | INCR, LPUSH, SADD, SPOP, HSET, ZADD — same run — §2 |
| Every other command family, p=64 | **0.45–0.75× x86 / 0.53–0.87× ARM** | same run — §2 |
| All families, p=1 | 0.89–1.08× x86 / 0.77–1.05× ARM | roughly a tie; ARM container families are the weak end — same run — §2 |
| Peak GET, absolute | **2.97M ops/s** x86 vs Redis 1.83M · **1.53M** ARM vs 0.95M | same run — §2 |
| Peak SET, absolute | **2.01M ops/s** x86 vs Redis 1.52M · **1.32M** ARM vs 0.85M | same run — §2 |
| vs v0.8.7, non-inlined families | **+5 to +23% x86 / +5 to +18% ARM** (raw ops/s) | v0.8.7 `d63ffcd8` rebuilt on the same hosts, same day, same harness; Redis control drifted a median −0.4% (x86) / +0.3% (ARM) — §2 |
| Memory per key, 8 B – 1 KB | **0.78–0.92× x86 / 0.84–0.92× ARM** (Moon uses 8–22% **less** on x86, 8–16% on ARM) | 2026-09-08, `--shards 1`, Redis 7.0.15/jemalloc, `-r 200000`, fresh server per point, n=3 (n=2 at 16/32/48/128 B), arithmetic-floor guard — §3 |
| Idle RSS, `--shards 1` | **tie** — 0.99× x86, 0.96× ARM | 13.01 vs 13.19 MB (x86), 11.43 vs 11.93 MB (ARM) — same run — §3 |
| Peak GET (v0.1.6, absolute) | 5.11M ops/sec (1.72×) | GCloud c3-standard-8 x86_64, p=64. Redis `io-threads` and payload size **not recorded**, and the run drove a *single hot key* rather than `-r N` — archive §2.1, superseded |
| Memory, 64 B values (aarch64, `--shards 8`) | 1.16× worse than Redis | 2026-09-01, GCE t2a-standard-8, Redis 7.0.15 `--io-threads 8 --io-threads-do-reads yes`, `-r 200000` — archive §2.14; a different configuration from the `--shards 1` rows above |
| Idle RSS, 8 shards (aarch64) | 1.26× worse than Redis | same run — archive §2.14 |
| CPU per operation | **tie** | 10.55 µs vs 11.33 µs, inside Redis's own 11.9% spread — 2026-09-01, archive §2.14 |
| Shard scaling (s8/s1) | **1.42× / 2.14× / 3.79×** | p=1 / p=8 / p=64, explicitly-keyed families — 2026-09-01, archive §2.14 |
| moon s8 vs Redis `--io-threads 8` | **1.26–1.32×** (p=8), **2.74–2.91×** (p=64) | 2026-09-01, both arches — archive §2.14 |
| AOF `everysec` SET p=16 | **1.32× Redis** | 2026-07-08, GCE c3-standard-8, Redis 7.0.15, `--shards 2` — archive §7.3 |
| AOF `always` SET p=16 | **0.91× Redis** | same run — archive §7.3 |
| Vector search (384d) | 12.7K QPS | **2026-04-15**, GCloud c3-standard-8 x86_64, HNSW + TurboQuant 8-bit, COSINE, 50K vectors, K=10 — archive §10.1, and **superseded** there by §10.5's concurrent-client reframing. The current vs-competitor figures are the 2026-07-08 Qdrant runs (archive §10.9). |
| Data correctness | **132/132 tests** | All types, 1/4/12 shards |

!!! warning "Scope of the pipelined GET/SET win"
    GET and plain `SET key value` are the two commands Moon serves from an inline
    byte path that bypasses frame construction and the dispatch table. Those two
    win at depth — **1.62× / 1.32×** on x86 and **1.60× / 1.55×** on ARM at p=64.
    **Every other family measured (INCR, LPUSH, SADD, SPOP, HSET, ZADD) loses at
    p≥8**, by 13–55%, on both architectures. The boundary is the fast path, not
    the engine: measured on v0.8.7, `SET k v` ran 2.08× Redis while
    `SET k v EX 100` — same work, one disqualifying option — ran 0.87×
    (archive §2.12). **Any summary quoting the GET number alone describes a
    two-command fast path, not the server.** Full matrix:
    [BENCHMARK.md §2](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md).

## Memory efficiency

### Per-key memory (Linux, `--shards 1`, both arches) — measured 2026-09-08

**Hosts:** GCE `c3-standard-8` (Xeon 8481C, x86_64) and `t2a-standard-8`
(Neoverse-N1, aarch64), 8 vCPU, Ubuntu, kernel 6.17.
**Moon:** `ae6cd003` (v0.8.9), `--shards 1 --appendonly no --disk-offload disable`.
**Oracle:** Redis 7.0.15 (jemalloc), `--save "" --appendonly no`.
**Method:** `scripts/bench-ab-memory.sh`, fresh server instance per data point,
`-r 200000` distinct keys (`DBSIZE ≈ 173,000`), per-key =
(loaded RSS − idle RSS) / `DBSIZE`, RSS from `/proc/<pid>/status`, n=3. Every row
is checked against its arithmetic floor (`key(16) + value + 24`) and a leg
reporting below its own floor aborts, because that is physically impossible.
Full table and method: [BENCHMARK.md §3](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md).

| Value size | Moon x86 | Redis x86 | Moon / Redis | Moon ARM | Redis ARM | Moon / Redis |
|:---:|---:|---:|:---:|---:|---:|:---:|
| 8 B | 97.9 B | 125.1 B | **0.78×** | 97.5 B | 113.0 B | **0.86×** |
| 16 B † | 113.3 B | 136.1 B | **0.83×** | 113.3 B | 128.7 B | **0.88×** |
| 32 B † | 130.1 B | 158.3 B | **0.82×** | 129.7 B | 145.6 B | **0.89×** |
| 48 B † | 146.1 B | 174.9 B | **0.84×** | 145.7 B | 161.8 B | **0.90×** |
| 64 B | 163.9 B | 178.8 B | **0.92×** | 162.8 B | 177.0 B | **0.92×** |
| 128 B † | 230.0 B | 259.8 B | **0.89×** | 228.9 B | 257.3 B | **0.89×** |
| 256 B | 362.2 B | 421.7 B | **0.86×** | 361.1 B | 419.0 B | **0.86×** |
| 1,024 B | 1,164.6 B | 1,393.3 B | **0.84×** | 1,161.9 B | 1,386.1 B | **0.84×** |
| **idle RSS** | 13.01 MB | 13.19 MB | 0.99× (tie) | 11.43 MB | 11.93 MB | 0.96× |

`†` = n=2 repetitions; every other row is n=3.

**Read it as:** Moon uses **less memory per key at every size measured, on both
architectures** — 8–22% on x86 and 8–16% on ARM, weakest at 64 B on both;
strongest at 8 B on x86 and at 1 KB on ARM, where
`CompactValue` inlines the value into the entry (values ≤12 B). The result does
not depend on the idle-RSS subtraction: Moon's **absolute** loaded RSS is lower at
every size too (1 KB values, x86: 210 MB vs 249 MB). Scope: `--shards 1`, string
values, one key shape; 4 KB was not part of this run.

!!! warning "Two claims from the 2026-09-04 run did not reproduce — both stay on the record"
    The 2026-09-04 Linux run reported Moon **11–51% worse at 32 B** on x86
    `--shards 1` (archive §3.2), and an empty-server RSS of **12.6–12.9 MB against
    Redis 7.5–7.7 MB — Moon 1.7× worse** (archive §3.1). 16/32/48/128 B were
    measured on 2026-09-08 specifically to test the first point: Moon is **0.82×
    at 32 B on x86 and 0.89× on ARM — an 18% and 11% win**. On idle RSS the two servers tie. Neither older figure is
    being called wrong here — the newer measurement did not reproduce it, and the
    conditions differ: the 2026-09-04 oracle was **Redis 7.4.2**, this one is
    **Redis 7.0.15**. On idle RSS the disagreement is on the *Redis* side
    (7.5–7.7 MB then vs 13.19 MB now, same host class), which is what
    [#821](https://github.com/pilotspace/moon/issues/821) tracks. Both runs are on
    the record; the older one is preserved in the
    [archive](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md).

!!! note "Scope — every row above is `--shards 1`"
    At `--shards 8` on aarch64, archive §2.14 (2026-09-01) separately measures a
    **16% loss at 64 B and a 26% idle-RSS loss**, because the default config
    spawns four threads per shard. Nothing above contradicts that; it is a
    different configuration, and it has not been re-measured on this tree.

!!! tip
    Moon's large-value advantage comes from `HeapString(Vec<u8>)` (48 bytes
    overhead) vs Redis's `robj` + SDS chain (~64–80 bytes overhead); its
    small-value advantage comes from `CompactValue` inlining values ≤12 B into the
    entry. The TTL-overhead claim that used to sit here is **unverified**: the
    harness section that would measure it omits `redis-benchmark -r`, so it loads
    one key.

### Baseline RSS (empty server) — Linux

| Server | 2026-09-08, x86_64 | 2026-09-08, aarch64 | Previously published |
|--------|--------------------|---------------------|----------------------|
| Redis (jemalloc) | **13.19 MB** (7.0.15) | **11.93 MB** (7.0.15) | 7.5–7.7 MB (Redis **7.4.2**, 2026-09-04) |
| Moon (1 shard) | **13.01 MB** | **11.43 MB** | 12.6–12.9 MB (2026-09-04) |
| Moon (12 shards) | *not measured on Linux* | *not measured on Linux* | 15.7 MB (**unverified / stale**, macOS) |

Moon's own idle figure is stable across the two Linux runs (12.6–12.9 MB then,
13.01 MB now); Redis's is not, and that is the open question in
[#821](https://github.com/pilotspace/moon/issues/821). The ARM pair independently
reproduces the archive's §2.14 sidebar (11.43 vs 11.93 MB here; 11.68 vs 11.96 MB
there, a different date). Do not quote the retired "identical 7.0 MB" row — it was
an Apple M4 Pro development reference.

### Superseded: per-key memory (Linux x86_64, 2026-09-04)

!!! warning "Kept as a record — superseded by the 2026-09-08 run above"
    Measured against **Redis 7.4.2 / jemalloc 5.3.0** on GCE `c3-standard-8`, moon
    `d5f3501b`, `--shards 1`, `scripts/bench-resources.sh`, `redis-benchmark -r N`,
    one point per cell (no repetitions, so no run-to-run spread is reported). Its
    32 B and idle-RSS rows did not reproduce on 2026-09-08; its 4 KB row was not
    re-measured. Full twelve-point table:
    [benchmark archive §3.2](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md).

| Value size | Keys | Redis 7.4.2/key | Moon/key | Moon / Redis |
|:---:|:---:|---:|---:|:---:|
| 32 B | 63K–632K | 123–129 B | 143–186 B | 1.11–1.51× |
| 256 B | 63K–632K | 408–410 B | 377–418 B | 0.92–1.02× |
| 1 KB | 63K–632K | 1,380–1,388 B | 1,155–1,256 B | 0.84–0.90× |
| 4 KB | 63K–632K | 5,259–5,266 B | 4,352–4,404 B | 0.83–0.84× |
| Empty-server RSS | — | 7.5–7.7 MB | 12.6–12.9 MB | 1.7× |

!!! danger "The retired '27–35% less memory' claim — and Moon is not what changed"
    The old published 1M × 1 KB row was Redis 1,571 B / Moon 1,153 B. Re-measured
    on Linux on 2026-09-04: Redis **1,380 B** / Moon **1,172 B**. Moon's own figure
    moved **1.6%**; the *oracle* moved **12%**. The old claim was inflated by a
    Redis baseline measured on macOS and/or without jemalloc, not by anything Moon
    did — both Redis builds already on that host were libc-malloc, which inflates
    Redis RSS, so Redis was rebuilt against jemalloc for the run.

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
    Linux. The Linux throughput matrix is the executive summary above: the
    current `--shards 1` matrix is [BENCHMARK.md §2](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md), and the multi-shard /
    io-threads rows are the archive's §2.14 (2026-09-01).

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

**On Linux, CPU per operation is a tie.** The [archive](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md)'s §2.14 (measured
2026-09-01, and **not** re-measured on 2026-09-08) puts moon `--shards 8` at
10.55 µs/op against 11.33 µs/op for Redis `--io-threads 8`
(GCE t2a-standard-8, `utime+stime` from `/proc/<pid>/stat`, 5 reps). Moon's 6.9%
edge sits inside Redis's own 11.9% run-to-run spread, so it is not a win. The
durable difference is stability: moon's CPU cost varies 2.0% run to run against
Redis's 11.9%.

The "45× / 23× better CPU" figure this page used to publish has been **removed**.
It was derived from an Apple M4 Pro table whose CPU column was sampled with
`ps -o %cpu=` — a process-lifetime average, not steady-state load — and whose CPU
and RPS columns were not taken in the same run. The underlying macOS table is kept
in the [benchmark archive](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md) §5.1 as a development record, with no ratio
derived from it.

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

The wins come from per-batch group commit (one fsync per pipeline batch, not per command), a single coalesced `write_all` per batch, a park-free AOF-writer poll that removes a ~150K/sec futex-wake storm on the shard thread under `everysec`, and coalesced pub/sub delivery writes. Durability is unchanged (`always` = RPO 0, `everysec` = RPO ≤ 1s), verified by the SIGKILL crash-recovery matrix. Measured 2026-07-08 and **not** re-measured on 2026-09-08 — it is carried forward with its date in [BENCHMARK.md §4](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md), and the full detail is in the [benchmark archive](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md) §7.3.

## Latency

No Moon-vs-Redis latency comparison has been measured on Linux. The 8-shard p50
figure this page used to publish was an Apple M4 Pro development reference, taken
with `redis-benchmark` — a closed-loop tool, which under-reports latency once the
server saturates (see [Coordinated Omission](references.md)). It is retained with
its provenance in the [benchmark archive](https://github.com/pilotspace/moon/blob/main/docs/internal/benchmark-history.md) §9.1 and is **not** republished
here as a result.

Architecturally, multi-core parallelism reduces per-shard queue depth, so the
median request should wait less. That expectation has not been confirmed on a
Linux host.

## Production workload patterns (macOS dev reference)

!!! warning
    Measured on an Apple M4 Pro, not on Linux, and not reproduced there. On Linux,
    the 2026-09-08 matrix ([BENCHMARK.md §2](https://github.com/pilotspace/moon/blob/main/BENCHMARK.md)) measures several of these command
    families (INCR, LPUSH, SADD, HSET, ZADD) at **0.45–0.87× Redis** at p≥8, so
    these ratios should not be read as production expectations.

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
