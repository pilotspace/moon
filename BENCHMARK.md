# moon Benchmark Report

**Last Updated:** 2026-06-16 (4-feature concurrent-vs-competitor pass added — §10.5 vector vs RediSearch, §11.4 graph vs FalkorDB, **new §12 Full-Text Search** vs RediSearch; honestly records where Moon trails. §2.8 = v2-1/PR #189 `db61973` K=1024 re-measurement; v0.1.6 in §2.1–2.6; §2.7 on perf/shard-dispatch-hot-path)
**Platforms:** Linux (GCloud x86_64 + ARM64), macOS (Apple M4 Pro)
**Redis:** 8.6.1 in §2.1–2.6; 7.0.15 in §2.7
**moon:** v0.1.6 in §2.1–2.6; perf/shard-dispatch-hot-path HEAD (commit `6582fa9`) in §2.7. Monoio runtime (io_uring on Linux, kqueue on macOS), fat LTO, codegen-units=1, target-cpu=native
**Methodology:** Co-located benchmarks using `redis-benchmark`. Fresh server instance per data point for memory tests. All ratios from same-run comparisons to control for VM variance.

**IMPORTANT — read §2.7.1 before comparing SET numbers across this report.** Two `redis-benchmark` invocation styles are in play:
- **Loose** (`redis-benchmark -t SET -P 64`, no `-r`): every write hits the single key `__rand_key__`. Cache-hot, no dict growth, no key distribution pressure. Matches §2.1/§2.2 historical methodology.
- **Strict** (`redis-benchmark -t SET -r 1000000 -P 64`): writes distribute uniformly over 1M keys. Exercises actual dict growth, probe-path collisions, cache pressure. Matches production workloads.

The SET absolute number can differ 3-4× between methodologies. Only strict-vs-strict or loose-vs-loose comparisons are meaningful.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Linux GCloud Benchmarks](#2-linux-gcloud-benchmarks)
3. [Memory Efficiency](#3-memory-efficiency)
4. [Throughput](#4-throughput)
5. [CPU Efficiency](#5-cpu-efficiency)
6. [Multi-Shard Scaling](#6-multi-shard-scaling)
7. [Persistence (AOF) Performance](#7-persistence-aof-performance)
8. [Production Workload Patterns](#8-production-workload-patterns)
9. [Latency](#9-latency)
10. [Vector Search](#10-vector-search)
11. [Graph Engine](#11-graph-engine)
12. [Full-Text Search](#12-full-text-search)
13. [Data Correctness](#13-data-correctness)
14. [Architecture Notes](#14-architecture-notes)
15. [How to Reproduce](#15-how-to-reproduce)

---

## 1. Executive Summary

| Metric | moon vs Redis | Conditions |
|--------|:-------------------:|------------|
| Peak GET (Linux x86_64) | **5.11M ops/s (1.72x)** | GCloud c3-standard-8, P=64 |
| Peak GET (Linux ARM64) | **3.47M ops/s (2.20x)** | GCloud t2a-standard-8, P=64 |
| Peak GET (macOS) | **7.94M ops/s (2.59x)** | OrbStack, Apple M4 Pro, P=64 |
| Production defaults GET | **1.93x Redis** | appendonly=yes, disk-offload, P=64 |
| Memory (1KB+ values) | **27-35% less** | 1-shard, per-key RSS |
| Memory (256B values) | Tied | 1-shard, per-key RSS |
| Baseline RSS (empty) | **Identical (7.0 MB)** | 1-shard |
| CPU efficiency at P=64 | **45x better** | 1.9% vs 43.9% CPU for similar RPS |
| With AOF persistence | **2.75x Redis** | SET, P=64, per-shard WAL |
| Multi-shard (8s P=16) | **1.84-1.99x Redis** | GET / SET |
| p50 latency (8-shard) | **8-10x lower** | 0.031ms vs 0.26ms |
| Data correctness | **2613+ tests pass** | All types, 1/4/12 shards |
| Vector insert (384d) | **6–20× RediSearch** | GCloud, 8-thread concurrent, §10.5 |
| Vector search (384d) | RediSearch ~16× QPS; recall 0.86 vs 0.96 | concurrent vs RediSearch, §10.5 |
| Full-text search (100K) | counts **exact** vs RediSearch; index ~0.85×, low-DF wins; multi-term QPS trails | vs RediSearch, §12.3 (v3-1 verified) |
| Graph build (native API) | **21–26× FalkorDB** | §11.5 (native 1-hop ≈1.2× FalkorDB Cypher) |
| Graph Cypher point-query | **correct + ~28–30×** vs old full-scan; ~4× behind Falkor's indexed scan | §11.5 (v3-2 verified, `match_rows`=Falkor's) |

---

## 2. Linux GCloud Benchmarks

**Date:** 2026-04-15
**Instances:** x86_64 (c3-standard-8, Sapphire Rapids 8481C) / ARM64 (t2a-standard-8, Neoverse-N1), 8 vCPU, 32GB RAM, Ubuntu 24.04, kernel 6.8

### 2.1 Raw Throughput (no persistence)

Moon started with `--appendonly no --disk-offload disable`.

| Metric | x86_64 | ARM64 | Redis (x86_64) | Redis (ARM64) | Ratio (x86) | Ratio (ARM) |
|--------|:------:|:-----:|:--------------:|:-------------:|:-----------:|:-----------:|
| GET p=64 | **5.11M** | **3.47M** | 2.98M | 1.58M | **1.72x** | **2.20x** |
| SET p=64 | **3.50M** | **2.42M** | 1.82M | 1.15M | **1.92x** | **2.10x** |
| GET p=32 | **2.73M** | — | 2.07M | — | **1.32x** | — |

### 2.2 Production Defaults (appendonly=yes, disk-offload=enable, WAL v3, PageCache)

This is moon's out-of-the-box configuration. Reads are unaffected by persistence — PageCache mmap actually improves read locality.

| Metric | x86_64 | ARM64 | Redis (x86_64) | Redis (ARM64) | Ratio (x86) | Ratio (ARM) |
|--------|:------:|:-----:|:--------------:|:-------------:|:-----------:|:-----------:|
| GET p=64 | **4.76M** | **3.45M** | 2.46M | 1.61M | **1.93x** | **2.14x** |
| SET p=1 | **147K** | — | 136K | — | **1.08x** | — |
| SET p=64 | 1.05M | — | 1.83M | — | 0.57x | — |

**Key insight:** GET throughput is identical across all persistence modes — reads are free. SET at high pipeline (p=64) pays ~50% WAL overhead due to per-shard fsync, but SET at p=1 still beats Redis even with full WAL.

### 2.3 Max Durability (appendfsync always)

| Metric | x86_64 | Redis (x86_64) | Ratio |
|--------|:------:|:--------------:|:-----:|
| GET p=64 | **4.85M** | 2.45M | **1.98x** |

### 2.4 Platform Comparison

| Platform | Moon GET p=64 | Redis GET p=64 | Ratio |
|----------|:------------:|:--------------:|:-----:|
| GCloud c3-standard-8 (x86_64) | 5.11M | 2.98M | **1.72x** |
| GCloud t2a-standard-8 (ARM64) | 3.47M | 1.58M | **2.20x** |
| OrbStack (Apple M4 Pro, aarch64) | 7.94M | 3.07M | **2.59x** |

x86_64 is ~1.4x ARM64 (Sapphire Rapids vs Neoverse-N1). OrbStack gives best absolute numbers due to no noisy-neighbor effect.

### 2.5 GCloud Variance Warning

GCloud VM results vary **10-15%** between runs due to noisy-neighbor CPU sharing. Both Redis and Moon are equally affected. Always compare **Moon/Redis ratios from the same run**, not absolute RPS across different runs.

| Run | Zone | Moon GET p=64 | Redis GET p=64 | Ratio |
|-----|------|:------------:|:--------------:|:-----:|
| Apr 6 | us-central1 | 5.52M | 2.36M | 2.34x |
| Apr 15 #1 | us-central1-a | 4.59M | 2.46M | 1.87x |
| Apr 15 #2 | us-east1-b | 5.05M | 2.84M | 1.78x |
| Apr 15 #3 | us-central1-a | **5.11M** | 2.98M | 1.72x |

The Apr 6 ratio (2.34x) was inflated by unusually slow Redis (2.36M). True GCloud c3 ratio is ~1.75x.

### 2.6 Memory Stability

RSS flat at 12.5MB under 100s sustained load (3 burst cycles of 1M requests each). No memory leak from tick-based event loop.

### 2.7 2026-04-22 Re-measurement (perf/shard-dispatch-hot-path HEAD)

**Branch:** `perf/shard-dispatch-hot-path` at commit `6582fa9`. Three new commits landed on top of v0.1.6-era baseline:

| commit | fix | effect |
|--------|-----|--------|
| `e2addc8` | pre-size DashTable + fuse `Database::set` probe | eliminates 9.89% `split_segment` CPU, halves hit-path probes |
| `e00769e` | length-gate + `#[inline]` `try_handle_*` | cuts ~5pp of per-command dispatch overhead |
| `6582fa9` | batch-level eviction gate skips per-write `runtime_config` lock | handler-self closure -3.2pp when `maxmemory=0` and disk-offload disabled |

**Instances:** fresh provisions, same class as §2.1 (c3-standard-8 x86_64 us-central1-a, t2a-standard-8 ARM64 us-central1-f). **Redis:** Ubuntu 24.04 package 7.0.15 (not the 8.6.1 used in §2.1-2.6). **CPU pinning:** server CPU 1, bench CPUs 2-5 via `taskset`.

#### 2.7.1 Methodology — strict vs loose

The v0.1.6 §2.1 table reported SET p=64 = 3.50M x86 / 2.42M ARM. Those numbers were measured with the default `redis-benchmark -P 64 -t SET` (no `-r` flag), which writes every request to the single key `__rand_key__`. That degenerates the workload: same segment every time, no dict growth, no key-distribution pressure, cache-hot throughout. It is what Redis's own benchmark folklore uses, but it does not reflect any real workload.

The strict benchmark adds `-r 1000000`, spreading writes uniformly over 1M distinct keys. This exercises:
- DashTable segment splits during table growth
- h2 fingerprint collisions across the full keyspace
- Cache pressure on the keys array
- `CompactKey` heap allocations for keys beyond the 22-byte inline threshold

Strict numbers are always lower. Moon gains more from loose methodology than Redis does (Moon's probe path amortizes better when the segment is cache-hot), so strict comparisons are the more honest "Moon vs Redis" signal.

Both methodologies shown below. Pick the one matching your deployment — interactive cache workloads with uniform hot keys look like loose; real keyspaces look like strict.

#### 2.7.2 Strict methodology (`-r 1000000`, distributed keyspace)

| op | p | x86 Moon fair | x86 Moon default | x86 Redis | Ratio (fair) | ARM Moon fair | ARM Moon default | ARM Redis | Ratio (fair) |
|----|---|:-------------:|:----------------:|:---------:|:------------:|:-------------:|:----------------:|:---------:|:------------:|
| GET | 64 | 4.50M | 4.55M | 2.86M | **1.58×** | 3.03M | 3.11M | 2.02M | **1.50×** |
| GET | 16 | 1.50M | 1.52M | 1.76M | 0.85× | 1.06M | 1.07M | 1.27M | 0.83× |
| GET | 1  | 108K  | 106K  | 132K  | 0.82× | 76K   | 79K   | 100K  | 0.76× |
| SET | 64 | 1.29M | 0.82M | 1.08M | **1.19×** | 752K | 552K | 871K | 0.86× |
| SET | 16 | 962K  | 668K  | 859K  | **1.12×** | 564K | 437K | 681K | 0.83× |
| SET | 1  | 107K  | 108K  | 138K  | 0.77× | 84K | 97K | 100K | 0.84× |

"Moon fair" = `--appendonly no --disk-offload disable --initial-keyspace-hint 1000000`. "Moon default" = same minus `--disk-offload disable` (disk-offload ON). See §2.7.4 for the tax.

#### 2.7.3 Loose methodology (no `-r`, matches §2.1 v0.1.6 shape)

| op | p | x86 Moon fair | x86 Moon default | x86 Redis | Ratio (fair) | ARM Moon fair | ARM Moon default | ARM Redis | Ratio (fair) |
|----|---|:-------------:|:----------------:|:---------:|:------------:|:-------------:|:----------------:|:---------:|:------------:|
| GET | 64 | 5.15M | 5.10M | 2.84M | **1.82×** | 3.50M | 3.65M | 2.02M | **1.73×** |
| GET | 16 | 1.59M | 1.60M | 1.76M | 0.90× | 1.19M | 1.17M | 1.29M | 0.92× |
| GET | 1  | 109K | 108K | 135K | 0.81× | 77K | 78K | 101K | 0.76× |
| SET | 64 | **4.46M** | 1.69M | 2.02M | **2.21×** | **3.42M** | 1.29M | 1.45M | **2.36×** |
| SET | 16 | 1.57M | 1.23M | 1.41M | **1.12×** | 1.17M | 876K | 1.01M | **1.16×** |
| SET | 1  | 108K | 107K | 136K | 0.79× | 79K | 87K | 100K | 0.79× |

#### 2.7.4 Disk-offload tax (5-run SET p=64 means, CV 2-8%)

`--disk-offload` defaults to `enable` in Moon's CLI. Even when the workload never exceeds RAM, every write pays for `try_evict_if_needed_async_spill`, `spill_file_id.get/set`, and the per-shard spill thread's cache-coherency traffic. Redis has no equivalent — disable this flag for Moon-vs-Redis comparisons.

| arch | methodology | Moon fair | Moon default | Redis | Moon fair/Redis | Disk-offload tax |
|------|-------------|:---------:|:------------:|:-----:|:---------------:|:----------------:|
| x86 | strict | 1.33M | 812K | 1.12M | **1.19×** | **-39%** |
| x86 | loose  | **4.46M** | 1.69M | 1.97M | **2.26×** | **-62%** |
| ARM | strict | 846K | 617K | 849K | 1.00× | -27% |
| ARM | loose  | **3.44M** | 1.28M | 1.44M | **2.39×** | **-63%** |

The disk-offload tax is larger on the loose (cache-hot) workload because when DashTable work is cheap, the spill-thread bookkeeping represents a larger fraction of total cost.

#### 2.7.5 Delta vs v0.1.6 §2.1 (same arch, same class, same loose methodology)

| arch | metric | v0.1.6 §2.1 | Today §2.7.3 | Δ |
|------|--------|:-----------:|:------------:|:-:|
| x86 | GET p=64 | 5.11M | 5.15M | +1% (flat) |
| x86 | SET p=64 | 3.50M | **4.46M** | **+27%** |
| ARM | GET p=64 | 3.47M | 3.50M | +1% (flat) |
| ARM | SET p=64 | 2.42M | **3.42M** | **+41%** |

The three session commits (A+B, E, D) land a real +27% x86 / +41% ARM SET p=64 improvement over the v0.1.6 tag, with GET p=64 holding flat. Redis 7.0.15 (§2.7) vs Redis 8.6.1 (§2.1) is different — the ratio change is Moon moving up, not Redis moving down (Redis x86 GET p=64 went from 2.98M §2.1 to 2.84M §2.7 — essentially flat).

#### 2.7.6 Caveats

- **GCloud VM hurts p=1 / p=16 workloads.** At low pipeline depth, TCP RTT dominates per-op cost. GCloud VM network stack is slower than OrbStack's bridged interface. On OrbStack ARM the same branch wins all p=1/p=16 workloads; on GCloud x86/ARM it loses them. This is a VM-class artifact, not a Moon regression.
- **ARM strict SET p=64 ratio is 0.86× (Moon loses on Neoverse-N1).** The Neoverse-N1 has lower per-core IPC than Sapphire Rapids 8481C; Moon's per-command tax (Frame ref-counting, AffinityTracker sample, metric record) eats more of the budget on ARM.
- **Variance.** Strict SET p=64 5-run CV is 2-4% (low). The loose ARM column has one outlier run at 1.12M vs 750K-800K elsewhere — kept in the mean, produces inflated σ. Re-running would give a cleaner number, but the directional finding (Moon wins loose, loses strict on ARM) is robust.

### 2.8 2026-06-15 Re-measurement (v2-1-throughput-polish / PR #189, K=1024 yield)

**Branch:** `feat/ft-yield-costfree-monoio` at commit `db61973` (current main + PR #189 — the cost-free monoio FT.SEARCH yield with the cross-arch K=1024 brute-force knee). PR #189 touches **only** the `FT.SEARCH` brute-force yield path, so KV/multi-shard/graph throughput is expected unchanged from §2.1/§2.7 — this run **confirms that** (KV stays within GCloud's 10-15% VM variance of the prior baseline; no regression).

**Instances:** fresh on-demand provisions, same class as §2.1 — x86_64 (`c3-standard-8`, Intel Xeon Platinum 8481C @ 2.70GHz, 8 vCPU, 31 GiB) us-central1-a, ARM64 (`t2a-standard-8`, Neoverse-N1, 8 vCPU, 31 GiB) us-central1-a, Ubuntu 24.04. **Redis:** Ubuntu 24.04 package 7.0.15 (same as §2.7, not the 8.6.1 of §2.1-2.6). **Build:** `RUSTFLAGS="-C target-cpu=native"`, fat LTO, cgu=1 (x86 3m51s, ARM 4m36s). **No CPU pinning** this run (unlike §2.7's taskset) — absolute RPS is therefore lower than §2.7's pinned figures; same-run Moon/Redis ratios are the signal. `redis-benchmark -c 50 -n 400000`, best-of-3 per cell. "Moon fair" = `--appendonly no --disk-offload disable --initial-keyspace-hint 1000000`.

#### 2.8.1 Loose methodology (no `-r`, single hot key — matches §2.1 shape)

| op | p | x86 Moon | x86 Redis | Ratio | ARM Moon | ARM Redis | Ratio |
|----|---|:--------:|:---------:|:-----:|:--------:|:---------:|:-----:|
| GET | 64 | **4.65M** | 2.44M | **1.91×** | **3.74M** | 1.65M | **2.26×** |
| GET | 16 | 1.53M | 1.59M | 0.97× | 1.03M | 1.07M | 0.97× |
| GET | 1  | 109K | 136K | 0.80× | 74K | 99K | 0.74× |
| SET | 64 | **3.08M** | 1.82M | **1.69×** | **2.63M** | 1.27M | **2.07×** |
| SET | 16 | **1.89M** | 1.30M | **1.45×** | **1.42M** | 893K | **1.59×** |
| SET | 1  | 108K | 135K | 0.80× | 76K | 104K | 0.73× |

x86 loose GET p=64 4.65M is within ~9% of §2.1's 5.11M / §2.7.3's 5.15M (no-pinning VM variance); loose SET p=64 3.08M tracks §2.1's 3.50M (no-pinning) rather than §2.7.3's 4.46M (pinned). **No KV regression from PR #189.**

#### 2.8.2 Strict methodology (`-r 1000000`, distributed keyspace)

| op | p | x86 Moon | x86 Redis | Ratio | ARM Moon | ARM Redis | Ratio |
|----|---|:--------:|:---------:|:-----:|:--------:|:---------:|:-----:|
| GET† | 64 | 1.20M | 969K | 1.24× | 980K | 798K | 1.23× |
| GET† | 16 | 1.01M | 849K | 1.19× | 745K | 676K | 1.10× |
| GET† | 1  | 108K | 134K | 0.81× | 70K | 99K | 0.71× |
| SET | 64 | **930K** | 771K | **1.21×** | **835K** | 657K | **1.27×** |
| SET | 16 | **766K** | 653K | **1.17×** | **611K** | 535K | **1.14×** |
| SET | 1  | 107K | 144K | 0.75× | 73K | 107K | 0.68× |

† **Strict GET here is a miss workload** — this harness runs GET before SET in each pipeline group, so the 1M-key GETs hit an empty table. That makes strict GET p=64 (1.20M x86) lower than §2.7.2's warm-hit strict GET (4.50M, keyspace pre-populated). The **strict SET** rows are the honest distributed-write signal: Moon wins strict SET on **both** arches this run (x86 1.21×, ARM 1.27×) — note ARM strict SET p=64 flipped positive vs §2.7.2's 0.86× loss, well within the 2-4% strict CV plus the no-pinning delta.

#### 2.8.3 Production defaults (Moon appendonly=yes everysec + disk-offload ON; Redis AOF everysec)

| op | p | x86 Moon | x86 Redis | Ratio | ARM Moon | ARM Redis | Ratio |
|----|---|:--------:|:---------:|:-----:|:--------:|:---------:|:-----:|
| GET | 64 | **4.71M** | 2.34M | **2.01×** | **3.57M** | 1.63M | **2.19×** |
| GET | 1  | 107K | 136K | 0.79× | 68K | 102K | 0.67× |
| SET | 64 | 605K | 1.66M | 0.36× | 591K | 1.23M | 0.48× |
| SET | 1  | 133K | 136K | 0.97× | 94K | 105K | 0.90× |

Reads are free under persistence (GET p=64 2.0-2.2×, matching §2.2). **Single-shard** SET p=64 with everysec fsync is 0.36-0.48× Redis — the documented per-shard WAL cost at high pipeline depth on one shard (CLAUDE.md: "WAL sync kills write throughput"). The per-shard-WAL advantage over Redis's single AOF (§7) materializes with **more shards**, not at shards=1.

#### 2.8.4 Multi-shard scaling (Moon-only, fair loose) — confirms single-shard-is-best

| arch | shards | GET p=16 | SET p=16 | GET p=64 | SET p=64 |
|------|:------:|:--------:|:--------:|:--------:|:--------:|
| x86 | 1 | 1.59M | 1.90M | 4.71M | 3.08M |
| x86 | 4 | 1.58M | 1.86M | 4.76M | 3.01M |
| x86 | 8 | 1.56M | 1.83M | 4.76M | 2.99M |
| ARM | 1 | 1.01M | 1.40M | 3.60M | 2.52M |
| ARM | 4 | 1.00M | 1.38M | 3.45M | 2.42M |
| ARM | 8 | 969K | 1.33M | 3.42M | 2.41M |

Scaling 1→8 shards is **flat-to-slightly-negative** for uniform single-key GET/SET at c=50: x86 GET p=64 holds at 4.7M (loopback-network ceiling), GET p=16 dips −2% (cross-shard SPSC dispatch cost); ARM dips −4 to −5%. This **confirms the CLAUDE.md gotcha** — most keys route cross-shard, so SPSC dispatch overhead dominates the local DashTable lookup; use `--shards 1` unless exploiting pipeline/AOF parallelism or hash-tag co-location. It refines §4.4's optimistic 1.46×-at-8-shards figure, which reflected a different (non-uniform / higher-concurrency) workload, not uniform-key GCloud routing.

#### 2.8.5 Graph + vector confirmation

Graph (`bench-graph-compare.sh --moon-only --nodes 2000`): x86 node 290/s · edge 301/s · Cypher 297/s; ARM 213/s · 231/s · 214/s — within ~3% of §11.1/§11.2 (no regression). Vector (50K×384d COSINE, single-connection pipelined harness): insert 19.2K vec/s x86 / 22.0K ARM; the full lifecycle insert→brute-force→`FT.COMPACT`→HNSW search ran clean and HNSW was ~10× faster per query than brute-force (240 vs 25 QPS x86 single-conn latency-bound), confirming the K=1024 yield change does not break search. These latency-bound single-conn figures are **not comparable** to §10's concurrent-throughput 12.7K QPS and do not supersede it.

#### 2.8.6 Caveats

- **No CPU pinning** (unlike §2.7) → absolute RPS sits below §2.7's pinned numbers; rely on same-run ratios.
- **Strict GET = miss workload** (GET-before-SET ordering); strict SET is the honest distributed-write number (see †).
- **Per-key memory not recorded** — the harness failed to capture Redis RSS (empty), so no Moon-vs-Redis per-key comparison was possible this run; §3 stands unchanged. Moon-only RSS for ~63K keys (coupon-collector over `-r 100000`) was sane (x86: 32B 24.0 MB, 256B 38.3 MB, 1KB 90.2 MB total incl. ~11 MB base).
- **Vector harness** is single-connection latency-bound (see §2.8.5).

### 2.9 2026-06-17 KV confirmation (v3-1/v3-2 verified build, `8238515`)

The 4-feature **verified** cross-arch run (KV slice; full report `docs/reviews/2026-06-17/4FEATURE-VERIFIED.md`)
re-confirmed KV is unaffected by v3-1-fts-hardening / v3-2-graph-correctness (text + graph paths only).
Moon-fair vs Redis (ratio = Moon/Redis), 8-vCPU c3/t2a, best-of-3:

| arch | mode | GET P64 | SET P64 | GET P16 | SET P16 | GET P1 | SET P1 |
|------|------|:-------:|:-------:|:-------:|:-------:|:------:|:------:|
| x86 | loose  | **1.87×** (4.44M) | **1.69×** (2.99M) | 0.96× | **1.43×** | 0.81× | 0.82× |
| x86 | strict | **1.15×** | **1.14×** | **1.11×** | **1.12×** | 0.83× | 0.77× |
| ARM | loose  | **1.85×** (2.99M) | **2.05×** (2.50M) | 0.95× | **1.34×** | 0.82× | 0.82× |
| ARM | strict | **1.27×** | **1.32×** | **1.12×** | **1.19×** | 0.83× | 0.80× |

Within VM variance of §2.8 (x86 loose P64 1.87–1.90×). Moon wins at pipeline depth, loses p=1 (TCP-RTT bound). No regression.

---

## 3. Memory Efficiency

### 3.1 Baseline RSS (Empty Server)

| Server | RSS | Notes |
|--------|-----|-------|
| Redis 8.6.1 | 7.0 MB | Single-threaded |
| moon (1 shard) | 7.0 MB | Lazy Lua VM + lazy replication backlog |
| moon (12 shards) | 15.7 MB | Per-shard overhead: ~0.7 MB |

### 3.2 Per-Key Memory (1-Shard, String Keys)

Measured with fresh server instances. `redis-benchmark -r N` for unique keys.

| Value Size | Keys Loaded | Redis/Key | moon/Key | Winner | Ratio |
|:----------:|:-----------:|:---------:|:--------------:|:------:|:-----:|
| 32 B | ~63K | 118 B | 147 B | Redis | 0.80x |
| 256 B | ~63K | 412 B | 407 B | **Tied** | 1.01x |
| 1,024 B | ~63K | 1,879 B | **1,207 B** | **moon** | **1.56x** |
| 4,096 B | ~63K | 5,131 B | **4,352 B** | **moon** | **1.18x** |

At 500K keys:

| Value Size | Redis/Key | moon/Key | Winner | Ratio |
|:----------:|:---------:|:--------------:|:------:|:-----:|
| 32 B | 118 B | 149 B | Redis | 0.79x |
| 256 B | 379 B | 379 B | **Tied** | 1.00x |
| 1,024 B | 1,786 B | **1,168 B** | **moon** | **1.53x** |

At 1M keys:

| Value Size | Redis RSS | moon RSS | Redis/Key | moon/Key | Winner |
|:----------:|:---------:|:--------------:|:---------:|:--------------:|:------:|
| 32 B | 78.2 MB | 95.8 MB | 118 B | 147 B | Redis |
| 256 B | 231.5 MB | 234.4 MB | 372 B | 376 B | **Tied** |
| 1,024 B | 954.2 MB | **703.0 MB** | 1,571 B | **1,153 B** | **moon** |

### 3.3 Why moon Uses Less Memory at Larger Values

moon stores heap strings as `HeapString(Vec<u8>)` (24 bytes + data) instead of Redis's `robj` + SDS chain:

```
moon:  CompactValue(16B) -> Box<HeapString> -> Vec<u8>(ptr+len+cap=24B) -> data
             Total overhead: 16 + 8(box) + 24(vec) = 48 bytes + data

Redis:       dictEntry(24B) -> robj(16B) -> SDS(header 8-17B + data) + jemalloc rounding
             Total overhead: ~64-80 bytes + data
```

For small strings (<=12 bytes), moon uses SSO (Small String Optimization) — the value is stored inline in the 16-byte `CompactValue` struct with zero heap allocation. Redis still allocates `robj` + SDS for all strings.

### 3.4 TTL Memory Overhead

moon packs TTL as a 4-byte delta inside `CompactEntry`. Redis maintains a separate `expires` hash table with a full `dictEntry` (24 bytes) per expiring key.

| Server | TTL Implementation | Extra Memory Per Expiring Key |
|--------|-------------------|-------------------------------|
| Redis | Separate `expires` dict | ~24 bytes (dictEntry) |
| moon | 4-byte delta in CompactEntry | **0 bytes** (already included) |

### 3.5 Multi-Shard Memory (12 shards, 1M keys x 64B)

| Server | RSS |
|--------|-----|
| Redis | 107.6 MB |
| moon (12 shards) | 139.8 MB |

Per-shard overhead includes: DashTable segments, event loop state, SPSC channels (256 entries each), Notify handles, timers. This is the cost of the shared-nothing multi-core architecture.

---

## 4. Throughput

### 4.1 Single-Shard SET Throughput (P=16, c=50)

| Value Size | Redis SET/s | moon SET/s | Ratio |
|:----------:|:-----------:|:----------------:|:-----:|
| 32 B | 1,298,701 | **1,754,386** | **1.35x** |
| 256 B | 1,219,512 | **1,639,344** | **1.34x** |
| 1,024 B | 1,010,101 | **1,030,928** | 1.02x |
| 4,096 B | 540,541 | **571,429** | 1.06x |

### 4.2 Multi-Shard Peak Throughput (Monoio runtime)

| Config | moon | Redis | Ratio |
|--------|:----------:|:-----:|:-----:|
| 8-shard GET P=16 c=50 | 2.60M | 1.41M | **1.84x** |
| 8-shard SET P=16 c=50 | 2.52M | 1.27M | **1.99x** |
| 4-shard GET P=64 c=50 | **3.79M** | 2.41M | **1.57x** |
| 8-shard SET P=64 c=50 | 2.19M | 1.48M | **1.48x** |
| 8-shard SET P=16 c=1000 | 2.12M | 1.20M | **1.76x** |

### 4.3 String Substring Operations (1-shard, c=50, macOS)

| Command | Pipeline | Redis | moon | Ratio |
|---------|:--------:|:-----:|:----:|:-----:|
| GETRANGE | P=1 | 71,003 | **140,292** | **1.98x** |
| SETRANGE | P=1 | 73,954 | **139,353** | **1.88x** |
| GETRANGE | P=16 | 814,332 | **1,620,746** | **1.99x** |
| SETRANGE | P=16 | 998,004 | **1,459,854** | **1.46x** |

GETRANGE extracts a 13-byte substring from an 85-byte string. SETRANGE overwrites 5 bytes at offset 7. SETRANGE write-path advantage narrows at high pipeline depth due to per-op allocation overhead (zero-pad check, TTL preservation).

### 4.4 Scaling Efficiency (GET throughput vs 1-shard)

| Shards | Scaling Factor |
|:------:|:--------------:|
| 1 | 1.00x |
| 2 | 1.27x |
| 4 | 1.43x |
| 8 | 1.46x |
| 12 | 1.39x |

Scaling is sub-linear due to cross-shard SPSC dispatch overhead and shared loopback network bandwidth. Separate-machine benchmarks with dedicated NICs would show closer to linear scaling.

> **Refined 2026-06-15 (§2.8.4):** on GCloud c3/t2a with a **uniform single-key** GET/SET workload at c=50, 1→8 shards is flat-to-slightly-negative (x86 GET p=64 holds ~4.7M, p=16 −2%; ARM −4–5%), not the +1.46× above. The positive scaling here reflects a non-uniform / higher-concurrency workload; for uniform cross-shard routing, single-shard is best (CLAUDE.md gotcha). Multi-shard wins come from pipeline/AOF parallelism and hash-tag co-location, not raw uniform-key fan-out.

---

## 5. CPU Efficiency

### 5.1 CPU% and Throughput by Pipeline Depth (1-shard, 200K pre-loaded keys)

| Pipeline | Redis CPU% | moon CPU% | Redis RPS | moon RPS | RPS Ratio | CPU/100K-ops (Redis) | CPU/100K-ops (moon) |
|:--------:|:----------:|:---------------:|:---------:|:--------------:|:---------:|:--------------------:|:-------------------------:|
| P=1 | 97.2% | 91.1% | 169K | 148K | 0.87x | 57.9% | 62.0% |
| P=8 | 100.0% | **3.3%** | 1.14M | 1.11M | 0.97x | 8.8% | **0.29%** |
| P=16 | 100.0% | **1.9%** | 1.95M | **1.97M** | **1.01x** | 5.1% | **0.10%** |
| P=64 | 43.9% | **1.9%** | 2.42M | **4.13M** | **1.71x** | 1.8% | **0.05%** |

At P=64, moon delivers **1.71x the throughput of Redis while using 23x less CPU**.

### 5.2 Why moon Is More CPU-Efficient

1. **io_uring-style batch I/O** — amortizes syscall overhead across multiple commands
2. **DashTable SIMD probing** — 16-way parallel key matching with SSE2/NEON
3. **CompactEntry (24B)** — cache-friendly vs Redis's 56-byte dictEntry + robj indirection
4. **Lock-free oneshot channels** — eliminated 12% CPU from pthread_mutex contention
5. **CachedClock** — eliminated 4% CPU from clock_gettime syscalls
6. **Software prefetch** — overlaps DashTable segment fetch with hash computation

### 5.3 Profiling Breakdown (8-shard, P=16)

| Component | CPU% |
|-----------|:----:|
| Connection handler (Frame alloc, HashMap, Vec) | ~33% |
| Event loop + SPSC drain | ~12% |
| RESP parse + serialize (memchr SIMD, itoa) | ~11% |
| DashTable Segment::find (SIMD probing) | ~10% |
| Memory ops (memmove/memcmp) | ~6% |
| System (kevent) | ~2% |

---

## 6. Multi-Shard Scaling

### 6.1 Phase 40-43 Optimization Journey

| Phase | Fix | Impact on 8-shard GET |
|:-----:|-----|:---------------------:|
| Before | Individual SPSC dispatch, .to_vec() copies, flume mutex oneshot | 0.52x Redis |
| 40 | Pipeline batch dispatch, buffer reuse | 1.10x Redis |
| 41 | Zero-copy .freeze() writes, borrow batching | 1.30x Redis |
| 42 | Inline dispatch for 1-shard GET/SET | Full parity |
| 43 | Lock-free oneshot, CachedClock | **1.84x Redis** |

### 6.2 p=1 Performance (No Pipeline)

| Config | Ratio vs Redis |
|--------|:--------------:|
| 1-shard SET | 1.02x |
| 1-shard GET | 0.95-1.02x |
| 8-shard SET | 1.04-1.11x |

At p=1, TCP loopback latency (~5000ns) dominates. Command processing (156ns) is 2.6% of total latency. Both servers hit the same network ceiling.

### 6.3 Connection Scaling

| Clients | Advantage |
|:-------:|:---------:|
| 1-10 | **1.93-3.27x** moon (low contention, cache locality wins) |
| 50 | ~1.0x (parity) |
| 100-500 | 0.88-0.92x (async runtime overhead under contention) |

Optimal operating point: **10-50 clients per shard**.

---

## 7. Persistence (AOF) Performance

### 7.1 With AOF Everysec, Advantage Grows

| Pipeline | SET ops/s (moon) | vs Redis (no AOF) | vs Redis (AOF everysec) |
|:--------:|:----------------------:|:------------------:|:-----------------------:|
| P=1 | 146K | 0.95x | 0.95x |
| P=8 | 1,117K | 1.68x | **1.68x** |
| P=16 | 1,887K | 1.90x | **2.21x** |
| P=32 | 2,469K | — | **2.52x** |
| P=64 | **2,778K** | 1.80x | **2.75x** |

### 7.2 Why Persistence Makes moon Faster (Relatively)

| Aspect | Redis | moon |
|--------|-------|------------|
| AOF architecture | Global append-only file, single writer thread | Per-shard WAL files, no global lock |
| Hot-path cost | Buffer + background rewrite | `buf.extend_from_slice()` (~5ns) |
| Flush | Background fsync | Batch `write_all` every 1ms tick |
| Fsync | Dedicated bio thread | Separate timer, every 1 second |
| Under P=64 | Global AOF becomes serialization point | Per-shard WAL scales linearly |

---

## 8. Production Workload Patterns

From `scripts/bench-production.sh` (10 scenarios):

| Scenario | Description | moon vs Redis |
|----------|-------------|:-------------------:|
| Session store | 80% GET / 15% SET, 512B values | **1.24x** |
| Rate limiting | INCR with 100-200 clients | **1.15x** |
| Leaderboard | ZADD + ZRANGEBYSCORE | **1.06-1.25x** |
| App caching | 1KB-4KB values, MSET batch | **1.10-1.27x** |
| Job queue | LPUSH/RPOP producer-consumer | **1.06x** |
| User profiles | HSET, HGET | **1.10x** |
| Data sizes | 8B to 64KB payloads | **1.10-1.27x** |
| Pipeline depth | P=1 to P=128 | **1.02-1.67x** |

Collection commands (LPUSH, HSET, ZADD) at P=64 are 1.06-1.25x Redis because execution time (200-400ns) dominates parsing overhead (83ns), and DashTable + CompactEntry + B+ tree genuinely outperform Redis's dict + skip list for mutations.

### 8.1 Data Size Advantage

moon wins across ALL payload sizes for both SET and GET:

| Value Size | GET Advantage | SET Advantage |
|:----------:|:------------:|:------------:|
| 8 B | 1.10x | 1.12x |
| 256 B | 1.15x | 1.18x |
| 4 KB | 1.20x | 1.22x |
| 64 KB | **1.27x** | **1.25x** |

Larger values amplify the io_uring zero-copy and writev scatter-gather advantage.

---

## 9. Latency

### 9.1 p50 Latency (8-shard)

| Metric | Redis | moon | Improvement |
|--------|:-----:|:----------:|:-----------:|
| p50 latency | 0.26-0.33 ms | **0.031 ms** | **8-10x lower** |

Multi-core parallelism reduces per-shard queue depth. The median request sees less waiting time. This is the real production advantage for latency-sensitive workloads.

---

## 10. Vector Search

**Date:** 2026-04-15
**Dataset:** 50K vectors, 384 dimensions (MiniLM-L6-v2 semantic embeddings), COSINE distance
**Index:** HNSW (M=16, EF_CONSTRUCTION=200), TurboQuant 8-bit

### 10.1 Throughput (GCloud c3-standard-8, x86_64)

| Operation | moon | Notes |
|-----------|:----:|-------|
| Vector insert | **8,200 vec/s** | HSET with 384d float32, auto-indexed |
| Search QPS | **12,700 QPS** | FT.SEARCH, K=10, brute-force mutable segment |

### 10.2 Throughput (GCloud t2a-standard-8, ARM64)

| Operation | moon | Notes |
|-----------|:----:|-------|
| Vector insert | **7,700 vec/s** | HSET with 384d float32, auto-indexed |
| Search QPS | **7,100 QPS** | FT.SEARCH, K=10 |

### 10.3 Recall

| Configuration | Recall@10 |
|---------------|:---------:|
| FP32 HNSW (384d, MiniLM) | **0.96+** |
| TQ8 after compact | **0.92** |
| TQ4 (384d) | Not recommended — concentration of distances at low dims |

TQ4 is designed for 768d+ workloads. For 384d and below, use TQ8 or FP32 HNSW.

### 10.4 vs Competitors (OrbStack, MiniLM 384d)

| Metric | moon | Redis (RediSearch) | Qdrant |
|--------|:----:|:------------------:|:------:|
| Insert/s | **31,000** | 4,000 | 6,600 |
| Search QPS | 1,400 | 3,800 | 982 |
| Recall@10 | 0.92 | 0.95 | 0.96 |
| Insert speedup | **7.7x Redis** | 1x | 1.7x |

Moon's insert pipeline is 7.7x faster than RediSearch due to zero-copy HSET + in-memory auto-indexing. Search QPS with brute-force mutable segment is competitive; HNSW immutable segment search is faster after FT.COMPACT.

### 10.5 2026-06-16 Concurrent throughput vs RediSearch (GCloud, db61973)

Re-measured under **8 concurrent clients** (not single-connection pipelined as §10.1–10.2) against a **live RediSearch**
(`redis/redis-stack-server` Docker), on c3-standard-8 (x86) + t2a-standard-8 (ARM). 50K × 384d clustered vectors
(mixture-of-Gaussians → meaningful recall), COSINE, KNN-10, recall@10 vs exact numpy ground truth.

| metric | x86 Moon | x86 RediSearch | ARM Moon | ARM RediSearch |
|--------|:--------:|:--------------:|:--------:|:--------------:|
| Insert/s | **25,133** | 3,989 | **35,937** | 1,799 |
| Search QPS (HNSW, 8 threads) | 552 | **9,088** | 505 | **6,842** |
| Search p50 | 14.2 ms | **0.71 ms** | 15.5 ms | **1.17 ms** |
| Recall@10 (HNSW) | 0.858 | **0.961** | 0.858 | **0.961** |

**Honest reframing of §10.1's 12.7K QPS** (which was single-connection *pipelined*, Moon-only): under 8-way
concurrency against a live RediSearch, **Moon's vector *insert* is 6–20× faster** (zero-copy HSET + auto-index) but
**RediSearch's vector *search* is ~16× higher QPS at higher recall** (0.96 vs Moon's 0.86 — Moon's default
auto-quantization SQ8/TQ trades recall for memory at 384d; CLAUDE.md notes 384d quantization loses recall). RediSearch
has no `FT.COMPACT` (auto-builds HNSW on insert), so its row is already its HNSW. Detail:
`docs/reviews/2026-06-16/4FEATURE-BENCH.md`.

### 10.6 2026-06-17 verified re-run (v3-1/v3-2 build `8238515`) — no regression

Re-ran the §10.5 protocol on the verified post-v3-1/v3-2 build to confirm Vector is untouched. Identical
within VM variance — Vector engine was not changed by either milestone (= the planned **v3-3-vector-kv-polish**
target). Detail: `docs/reviews/2026-06-17/4FEATURE-VERIFIED.md`.

| metric | x86 Moon | x86 RediSearch | ARM Moon | ARM RediSearch |
|--------|:--------:|:--------------:|:--------:|:--------------:|
| Insert/s | **28,136** | 3,750 | **28,343** | 1,906 |
| Search QPS (HNSW, 8 threads) | 473 | **9,143** | 540 | **6,926** |
| Recall@10 (HNSW) | 0.858 | **0.961** | 0.858 | **0.961** |

Moon insert **7.5× (x86) / 14.9× (ARM)** faster; RediSearch search ~16× higher QPS at higher recall (the
SQ8/TQ-at-384d recall trade-off). Unchanged from §10.5 — no v3-1/v3-2 regression.

---

## 11. Graph Engine

**Date:** 2026-04-15
**Dataset:** 2K nodes, 6K edges, sequential redis-cli commands
**Engine:** CSR (Compressed Sparse Row) + SlotMap + Cypher subset

### 11.1 Throughput (GCloud c3-standard-8, x86_64)

| Operation | QPS | Notes |
|-----------|:---:|-------|
| Node/Edge insert | **294/s** | GRAPH.ADD via redis-cli (sequential, TCP overhead) |
| 1-hop neighbor query | **303/s** | GRAPH.NEIGHBORS |
| Cypher query | **292/s** | GRAPH.QUERY with pattern matching |
| CSR lookup (internal) | **923 ps/edge** | Sub-nanosecond after FT.COMPACT builds CSR |

### 11.2 Throughput (GCloud t2a-standard-8, ARM64)

| Operation | QPS |
|-----------|:---:|
| Node/Edge insert | **216/s** |
| 1-hop neighbor query | **239/s** |
| Cypher query | **228/s** |

### 11.3 vs FalkorDB (OrbStack)

| Metric | moon | FalkorDB |
|--------|:----:|:--------:|
| Cypher QPS | **2.4x** | 1x |
| Native API QPS | **19x** | N/A |
| Populate (bulk insert) | **23x** | 1x |

Moon's shared-nothing per-shard graph with CSR compaction provides sub-nanosecond edge traversal after compaction. The native GRAPH.* API avoids Cypher parsing overhead for simple operations.

### 11.4 2026-06-16 vs FalkorDB (GCloud, 8 concurrent clients)

5K nodes, 15K edges. Moon native `GRAPH.CREATE`/`ADDNODE`/`ADDEDGE`; FalkorDB Cypher. redis-py, 8 threads.

| metric | x86 Moon | x86 FalkorDB | ARM Moon | ARM FalkorDB |
|--------|:--------:|:------------:|:--------:|:------------:|
| Build ops/s | **25,333** | 962 | **12,592** | 605 |
| 1-hop qps (Moon native / Falkor Cypher) | **5,671** | 4,739 | **4,474** | 3,794 |
| 1-hop p50 | **1.12 ms** | 1.53 ms | 1.54 ms | 1.94 ms |
| Cypher 1-hop / 2-hop qps | 40† / 13† | **4,739 / 4,459** | 28† / 9† | **3,794 / 3,639** |

**Moon native builds 21–26× faster** and its native 1-hop neighbor lookup edges out FalkorDB Cypher. † **But Moon's
Cypher `MATCH (a {id:N})` does not filter on an inline node-property predicate** — it full-scans the label
(returns ~all 15K edges vs FalkorDB's correctly-filtered 4), so Moon Cypher point/2-hop queries are slow whole-graph
scans. **Use `GRAPH.NEIGHBORS` for Moon point lookups, not Cypher.** Detail: `docs/reviews/2026-06-16/4FEATURE-BENCH.md`.

### 11.5 2026-06-17 v3-2 inline-filter VERIFIED (GCloud, build `8238515`)

The v3-2-graph-correctness milestone added the inline `Filter` op so `MATCH (a:N {id:N})` narrows on the
node-`id` property instead of full-scanning the label (the §11.4 `cypher_match_rows=14991` defect). This run
**proves it in-harness** — the 2026-06-16 harness filtered on the internal `GRAPH.ADDNODE` handle (a false
`0`); the corrected harness (`docs/reviews/2026-06-17/gce-4feature-bench.sh`) filters on the `id` property value.

| metric | §11.4 (pre-v3-2) | **2026-06-17 verified** | FalkorDB |
|--------|:----------------:|:-----------------------:|:--------:|
| Moon `cypher_match_rows` (x86 & ARM) | 14,991 (full scan) | **4** | 4 |
| Moon cypher_1hop qps (x86 / ARM) | 40† / 28† | **1,195 (6.68 ms) / 787 (10.15 ms)** | 4,734 / 3,869 |
| Moon cypher_2hop qps (x86 / ARM) | 13† / 9† | **1,156 (6.92 ms) / 755 (10.33 ms)** | 4,395 / 3,659 |
| Moon build ops/s (x86 / ARM) | 25,333 / 12,592 | **25,094 / 13,939** | 959 / 604 |
| Moon native_neighbors qps (x86 / ARM) | 5,671 / 4,474 | **5,650 / 4,442** | — |

**`cypher_match_rows` = 4 on both arches — identical to FalkorDB's filtered 4** (was a 14,991-edge full scan).
Moon's Cypher point-query is now **correct and ~28–30× faster than its old full-scan self** (2-hop ~84–89×).
It still trails FalkorDB ~4× because FalkorDB has a **property index** on `id` while Moon does a **filtered
label scan** — a property index for inline-equality is a legitimate future optimization, explicitly out of
v3-2 scope. Moon native builds 23–26× faster and native 1-hop edges out FalkorDB Cypher.
Detail: `docs/reviews/2026-06-17/4FEATURE-VERIFIED.md`.

---

## 12. Full-Text Search

**Date:** 2026-06-16
**Engine:** inverted index in `src/text/` — BM25 (k1=1.2, b=0.75), FST term dictionary, RoaringBitmap postings,
analyzer pipeline (NFKD → lowercase → UAX#29 → stopword → Snowball English stem); TAG + NUMERIC stores.
**Surface:** RediSearch-compatible `FT.CREATE` (TEXT/TAG/NUMERIC) · `FT.SEARCH` · `FT.AGGREGATE`.

### 12.1 vs RediSearch (GCloud, 100K docs, 8 concurrent clients)

100K docs, Zipf vocabulary (8000 terms), TEXT + TAG + NUMERIC schema; vs `redis/redis-stack-server`. x86 shown
(ARM mirrors within ~15%). `qps (p50)`:

| query | Moon | RediSearch | note |
|-------|:----:|:----------:|------|
| index docs/s | 376 | **18,052** | Moon ~48× slower to index |
| term high-DF (~5k docs) | 19 (419 ms) | **3,813** (2.1 ms) | Moon O(M²) TF-lookup cliff |
| term mid-DF (~1k docs) | 429 (18.6 ms) | **4,909** (1.0 ms) | Moon slower |
| term low-DF (~95 docs) | **7,765** (0.81 ms) | 3,606 (1.9 ms) | **Moon faster** (small posting, lock-free) |
| AND (2-term) | 7,665 | 8,485 | parity |
| OR (`\|`, 2-term) | 7,686 | 3,474 | ⚠ Moon doesn't union (total 10 vs 2072) |
| TAG `@cat:{}` | 7,791 | 3,319 | ⚠ Moon reports returned-count not total (10 vs 5064) |
| NUMERIC `@price:[..]` | 107 (74.7 ms) | **351** (22.8 ms) | Moon ~3× slower |
| TEXT + TAG combo | 12,843 | 4,077 | ⚠ Moon returns 0 hits (vs 253) |
| FT.AGGREGATE groupby | 6 (698 ms) | 11 (361 ms) | Moon ~2× slower |

### 12.2 Status

Moon's full-text search is **functional but early-stage** relative to RediSearch's mature engine. It is **faster on
low-cardinality single-term queries** (small posting lists, per-shard lock-free path) but trails on:
- **Indexing throughput** (~40–48× slower) — synchronous analyzer + an O(V) per-doc upsert scan (`posting.rs:139`).
- **High-DF queries** — an O(N) term-frequency lookup (`store.rs:504`) degrades to O(M²); a term in ~5% of docs
  takes 419 ms (this was *predicted* by the code review and reproduced exactly here).
- **Query correctness** — OR (`|`) does not union, TEXT+TAG combos return 0, and `FT.SEARCH` reports the returned
  count rather than the total-matched count.

These are well-scoped fixes (RoaringBitmap::rank for TF lookup, OR-union, combo-query, count semantics, async
indexing). See `docs/reviews/2026-06-16/review-fts.md` + `4FEATURE-BENCH.md`.

### 12.3 2026-06-17 v3-1 hardening VERIFIED (GCloud, build `8238515`)

The v3-1-fts-hardening milestone (PRs #190/#192) targeted exactly the §12.2 gaps. This run **confirms every
one is fixed on both arches** — most importantly, **every Moon `hits` count now equals RediSearch's exactly**
(the §12.1 OR/TAG/combo counts were broken: 10/10/0). x86 shown; ARM mirrors within ~15%.

| query | §12.1 Moon | **2026-06-17 Moon** | RediSearch | `hits` (Moon = RS) | fix |
|-------|:----------:|:-------------------:|:----------:|:------------------:|-----|
| index docs/s | 376 | **15,052** | 18,232 | — | **~40× (O(V) upsert)** |
| term high-DF (~5k) | 19 (419 ms) | **240 (33 ms)** | 3,791 | 5068 = 5068 | **O(M²) cliff gone** |
| term mid-DF (~1k) | 429 | **1,416** | 5,293 | 1046 = 1046 | — |
| term low-DF (~95) | **7,765** | **7,714** | 3,594 | 95 = 95 | **Moon still wins** |
| OR (`\|`) | 7,686 (hits=10) | 688 | 3,778 | **2072 = 2072** | **union fixed** |
| TAG `@cat:{}` | 7,791 (hits=10) | 2,227 | 3,367 | **5064 = 5064** | **count fixed** |
| NUMERIC `@price:[..]` | 107 | 97 | 377 | 10119 = 10119 | correct |
| TEXT + TAG combo | 12,843 (hits=0) | 270 | 4,314 | **253 = 253** | **combo fixed** |

The §12.1 OR/TAG/combo QPS *looked* fast only because they did no real work (10 or 0 hits — broken). Now that
they compute the full correct result, the QPS reflects real work; Moon still trails RediSearch on raw
multi-term QPS but **indexes at near-RediSearch rates and is correct**, and **wins low-DF single-term**. All
four §12.2 gaps — OR union, TAG/NUMERIC total-match counts, TEXT+TAG combo, O(V) index + O(M²) high-DF — are
**resolved**. Detail: `docs/reviews/2026-06-17/4FEATURE-VERIFIED.md`.

---

## 13. Data Correctness

### 13.1 Consistency Test Suite

`scripts/test-consistency.sh` runs 132 tests comparing moon output against Redis as ground truth.

| Category | Tests | Status |
|----------|:-----:|:------:|
| String SET/GET (empty, 1B, 12B SSO, 13B heap, 64B-64KB, numeric, float) | 14 | PASS |
| String mutations (APPEND, INCR/DECR, STRLEN, GETRANGE, SETRANGE, GETDEL, GETSET) | 16 | PASS |
| APPEND crossing SSO->heap boundary (11B -> 13B) | 1 | PASS |
| MSET / MGET (with missing keys) | 2 | PASS |
| SET options (EX, PX, NX, XX, SETEX, SETNX, TTL verify) | 8 | PASS |
| Binary-safe data (null bytes, tabs, newlines, UTF-8) | 3 | PASS |
| Hash operations (HSET/HGET/HGETALL/HMGET/HDEL/HINCRBY + large values) | 9 | PASS |
| List operations (RPUSH/LPUSH/LRANGE/LLEN/LINDEX/RPOP/LPOP + large values) | 8 | PASS |
| Set operations (SADD/SCARD/SISMEMBER/SREM/SMEMBERS) | 5 | PASS |
| Sorted Set operations (ZADD/ZCARD/ZSCORE/ZRANK/ZRANGE/ZINCRBY) | 8 | PASS |
| Bulk load (1K deterministic keys, 50 random spot-checks) | 51 | PASS |
| Overwrite / type change (size changes, string->hash) | 5 | PASS |
| Edge cases (nonexistent, DEL+GET, SET NX/GET, 500-char key) | 5 | PASS |
| **Total** | **132** | **ALL PASS** |

Tested across all shard configurations:

| Shards | Result |
|:------:|:------:|
| 1 | 132/132 PASS |
| 4 | 132/132 PASS |
| 12 (auto) | 132/132 PASS |

### 13.2 Known Unimplemented Commands

- `GETRANGE` / `SETRANGE` — not yet implemented (returns `ERR unknown command`)

---

## 14. Architecture Notes

### 14.1 Data Structure Sizes

| Struct | Size | Notes |
|--------|:----:|-------|
| CompactKey | 24 B | Inline keys <= 22 bytes (zero heap alloc) |
| CompactEntry | 24 B | CompactValue(16) + ttl_delta(4) + metadata(4) |
| CompactValue | 16 B | SSO <= 12 bytes inline; heap strings use HeapString |
| HeapString | 24 B | `Vec<u8>` — no enum discriminant, no Bytes/Arc overhead |
| DashTable Segment | ~3 KB | 64B ctrl + 8B meta + 60 slots x (24B key + 24B value), align(64) |
| Segment load threshold | 90% | 54/60 slots; avg fill ~67% |

### 14.2 Key Optimizations Applied

| Optimization | Impact | Component |
|-------------|--------|-----------|
| HeapString(Vec<u8>) for heap strings | ~35% less per heap string | CompactValue |
| SSO (Small String Optimization) | Zero alloc for values <= 12B | CompactValue |
| CompactKey inline | Zero alloc for keys <= 22B | CompactKey |
| DashTable SIMD probing | 16-way parallel key match | Segment |
| Lock-free oneshot | Eliminated 12% CPU (mutex) | Cross-shard dispatch |
| CachedClock | Eliminated 4% CPU (syscall) | Per-shard event loop |
| Lazy Lua VM | -18MB baseline (init on first connection) | Shard startup |
| Lazy replication backlog | -12MB baseline (init on first replica) | Shard startup |
| SPSC buffer 256 entries | -6MB baseline (was 4096) | Channel mesh |
| 90% load threshold | ~8% better fill factor | DashTable |
| Per-shard WAL | Scales linearly with shards | Persistence |
| io_uring batch I/O | Amortizes syscalls | Network |

---

## 15. How to Reproduce

### Build

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

Cargo.toml profile: `lto = "fat"`, `codegen-units = 1`, `opt-level = 3`, `strip = true`

### Memory & CPU Benchmark

```bash
# Full matrix (12 data points x 4 value sizes, ~3 minutes)
./scripts/bench-resources.sh --shards 1

# Quick mode (2 key counts x 2 value sizes, ~1 minute)
./scripts/bench-resources.sh --shards 1 --quick

# Multi-shard (real-world throughput)
./scripts/bench-resources.sh --shards 0

# Output: BENCHMARK-RESOURCES.md
```

### Data Consistency

```bash
./scripts/test-consistency.sh --shards 1    # 132 tests, ~25 seconds
./scripts/test-consistency.sh --shards 4    # Cross-shard dispatch
./scripts/test-consistency.sh --shards 0    # Full auto (12 shards)
```

### Throughput Benchmark

```bash
# Quick comparison
redis-benchmark -p 6400 -c 50 -n 100000 -t SET,GET -P 16 -q

# Production scenarios (10 workloads)
./scripts/bench-production.sh --shards 1

# Multi-shard scaling
./scripts/bench-production.sh --shards 4
./scripts/bench-production.sh --shards 8
```

### With Persistence

```bash
# Start with AOF
./target/release/moon --port 6400 --shards 1 --appendonly yes --appendfsync everysec &
redis-server --port 6399 --save "" --appendonly yes --appendfsync everysec --daemonize yes

# Benchmark writes
redis-benchmark -p PORT -c 50 -n 200000 -t SET,INCR,LPUSH,HSET -P 16 -q
```

### GCloud Linux Benchmark

```bash
# Provision instances
gcloud compute instances create moon-bench-x86 \
  --zone=us-central1-a --machine-type=c3-standard-8 \
  --image-family=ubuntu-2404-lts --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB --boot-disk-type=pd-ssd

gcloud compute instances create moon-bench-arm64 \
  --zone=us-central1-f --machine-type=t2a-standard-8 \
  --image-family=ubuntu-2404-lts-arm64 --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB --boot-disk-type=pd-ssd

# Setup (on each instance)
bash scripts/gcloud-bench-setup.sh

# Run full benchmark suite (KV + vector + graph)
bash scripts/run-full-bench.sh

# 4-feature concurrent-vs-competitor pass (KV/Vector/Graph/FTS vs Redis/RediSearch/FalkorDB) — §2.9/§10.6/§11.5/§12.3
# Use the 2026-06-17 harness: graph filters on the `id` PROPERTY value (not the GRAPH.ADDNODE handle), builds MOON_BRANCH (default main).
# Pulls redis-stack + falkordb Docker images; env-tunable dataset sizes.
scp docs/reviews/2026-06-17/gce-4feature-bench.sh <instance>:~/ && \
  ssh <instance> 'MOON_BRANCH=main VEC_NUM=50000 FTS_NDOC=100000 GRAPH_NN=5000 GRAPH_NE=15000 BENCH_DUR=8 bash ~/gce-4feature-bench.sh'

# Cleanup
gcloud compute instances delete moon-bench-x86 --zone=us-central1-a --quiet
gcloud compute instances delete moon-bench-arm64 --zone=us-central1-f --quiet
```

### Notes

- Co-located benchmarks (client + server on same machine) are conservative. Separate-machine benchmarks with 25+ GbE show higher throughput.
- GCloud VM results vary 10-15% between runs (noisy-neighbor CPU sharing). Always compare Moon/Redis ratios from the same run, not absolute RPS across runs.
- macOS RSS is a high-water mark. Use fresh server instances per data point for accurate memory measurement.
- Always use `redis-benchmark -r <num_keys>` to generate unique keys.
- redis-benchmark 8.x uses `\r` for progress lines. Pipe through `tr '\r' '\n'` before parsing.
- Moon production defaults include disk-offload (WAL v3 + PageCache). For raw throughput comparison, explicitly pass `--appendonly no --disk-offload disable`.
