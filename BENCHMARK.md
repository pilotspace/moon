# moon Benchmark Report

**Measured:** 2026-09-08 · **moon** `ae6cd003` (v0.8.9) · **Redis** 7.0.15 (jemalloc)
**Hosts:** GCE `c3-standard-8` (Xeon 8481C, x86_64) and `t2a-standard-8` (Neoverse-N1, aarch64), 8 vCPU, Ubuntu, kernel 6.17
**Config:** `--shards 1 --appendonly no --disk-offload disable`, Redis `--save "" --appendonly no`, `-c 50`, keys spread over `-r 100000`

The throughput (§2) and memory (§3) numbers were all measured on the date above.
§4 lists what was **not** re-measured — AOF durability, vector, graph, full-text
and multi-shard scaling are carried forward with their own dates. The full
historical record — eight months of prior runs, including the vector, graph and full-text
competitor benchmarks not repeated here — is archived in
[`docs/internal/benchmark-history.md`](docs/internal/benchmark-history.md).

---

## How to read this

Four rules, each of which exists because breaking it once produced a published
number that later had to be retracted:

1. **Ratios come from the same run.** GCE neighbours move absolute throughput
   10-15%. Redis is restarted and re-measured in *every* repetition here as a
   live drift control, and the legs alternate order each rep.
2. **A ratio is only a result if it clears the noise floor.** The floor is the
   worst within-leg coefficient of variation of the two series being compared.
   Rows marked `*` are inside their floor: a tie, not a win.
3. **Key distribution changes the answer by ~2x.** Every command below is an
   explicit keyed command over `-r 100000`. `redis-benchmark -t lpush|sadd|hset|zadd`
   drives **one literal key** and randomises the *element*, which is how a
   shard-scaling matrix once asked eight families to demonstrate an
   impossibility and reported the tautological answer.
4. **Benchmark numbers come from Linux.** macOS builds and runs, but io_uring,
   O_DIRECT and the spin governor are all `cfg(target_os = "linux")`. Older
   macOS figures are labelled as dev references in the archive and are not
   quoted here.

n=5 interleaved reps per throughput point, n=3 fresh servers per memory point.

---

## 1. Summary

| | x86_64 | aarch64 |
|---|:---:|:---:|
| **GET / SET, p=64** | **1.62x / 1.32x** | **1.60x / 1.55x** |
| **GET / SET, p=8** | **1.20x / 1.20x** | **1.18x / 1.19x** |
| Every other family, p=8 | 0.62-0.81x | 0.64-0.77x |
| Every other family, p=64 | 0.45-0.75x | 0.53-0.87x |
| p=1, all families | 0.89-1.08x | 0.77-1.05x |
| **Memory per key, 8 B - 1 KB** | **0.78-0.92x** (moon uses less) | **0.84-0.92x** |
| Idle RSS, `--shards 1` | 0.99x (tie) | 0.96x |
| **vs v0.8.7:** non-inlined families, p>=8 | **+5 to +25%** | **-1 to +21%** (11/12 rows up) |

**The one-sentence version: moon is faster than Redis on GET and SET at pipeline
depth, slower than Redis on everything else at pipeline depth, and uses 8-22%
less memory per key at every value size measured.**

That split is the headline, not a footnote. Two commands — `GET` and plain
`SET key value` — are served from a byte-level inline path that never builds a
`Frame` or touches the dispatch table. Those two win. The other six families
measured all lose at p>=8, on both architectures — by 13% in the mildest case
and 55% in the worst. Any summary quoting the GET number alone describes a
two-command fast path, not the server.

---

## 2. Throughput

Ratio = moon / Redis. Bold = moon wins, plain = moon loses, `*` = inside the
noise floor.

### x86_64 — GCE c3-standard-8

| command | p=1 | p=8 | p=64 |
|---------|:---:|:---:|:----:|
| GET   | **1.03x** | **1.20x** | **1.62x** |
| SET   | **1.02x** | **1.20x** | **1.32x** |
| INCR  | **1.08x** | 0.71x | 0.55x |
| LPUSH | **1.07x** | 0.70x | 0.61x |
| SADD  | **1.07x** | 0.69x | 0.70x |
| SPOP  | **1.05x** | 0.81x | 0.75x |
| HSET  | **1.08x** | 0.62x | 0.50x |
| ZADD  | 0.89x | 0.62x | 0.45x |

### aarch64 — GCE t2a-standard-8

| command | p=1 | p=8 | p=64 |
|---------|:---:|:---:|:----:|
| GET   | **1.05x** | **1.18x** | **1.60x** |
| SET   | 1.03x* | **1.19x** | **1.55x** |
| INCR  | 1.00x* | 0.66x | 0.62x |
| LPUSH | 0.88x | 0.71x | 0.71x |
| SADD  | 0.93x | 0.69x | 0.74x |
| SPOP  | 0.88x | 0.77x | 0.87x |
| HSET  | 0.91x | 0.64x | 0.62x |
| ZADD  | 0.77x | 0.68x | 0.53x |

Noise floors: 0.5-13.6% (x86), 0.8-8.5% (ARM). Every unmarked ratio above sits
several floor-widths from 1.0.

### Peak absolute, p=64

| | moon | Redis |
|---|---:|---:|
| GET, x86_64 | **2.97 M ops/s** | 1.83 M |
| SET, x86_64 | **2.01 M ops/s** | 1.52 M |
| GET, aarch64 | **1.53 M ops/s** | 0.95 M |
| SET, aarch64 | **1.32 M ops/s** | 0.85 M |

### What the shape means

- **p=1 is roughly a tie**, both architectures, with a real ARM weakness on the
  container families (0.77-0.93x). At depth 1 the cost is dominated by the
  wake/reply round trip, which both engines pay.
- **p>=8 splits in two**, identically on both architectures. GET/SET pull ahead
  as the inline path amortises; every other family falls behind as per-command
  dispatch, `Frame` construction and intercept-chain work stop being hidden by
  network cost.
- **ZADD is the worst family everywhere** (0.45x x86 / 0.53x ARM at p=64) and
  the only family that also loses at p=1 on both arches. Sorted sets carry the
  heaviest per-op cost in the current tree.
- **SPOP is the least-bad container family** (0.75x / 0.87x at p=64) — small
  reply, no member-position bookkeeping.


### Change since v0.8.7 — the write-path regression is reversed

The archive's §2.13 documented a 9-21% write-path loss that landed between
v0.6.0 and v0.8.7, on every non-inlined family, and closed without attribution.
v0.8.7 (`d63ffcd8`) was rebuilt on these same two hosts, same toolchain, same
`RUSTFLAGS`, and run through the same harness — each version normalised by its
own freshly-measured Redis. Change in the moon/Redis ratio, main vs v0.8.7:

| command | x86 p=8 | x86 p=64 | ARM p=8 | ARM p=64 |
|---------|:---:|:---:|:---:|:---:|
| INCR  | **+16.2%** | **+23.5%** | **+14.0%** | **+17.7%** |
| HSET  | **+14.0%** | **+18.3%** | **+11.1%** | **+16.1%** |
| SPOP  | **+25.0%** | **+15.0%** | -1.4%* | **+14.2%** |
| SADD  | +5.3%* | **+12.0%** | **+4.7%** | **+12.4%** |
| LPUSH | **+9.7%** | **+11.3%** | **+4.9%** | **+8.5%** |
| ZADD  | +6.8%* | **+6.5%** | **+20.8%** | **+7.7%** |
| GET   | +0.6%* | +1.9%* | +2.9%* | +0.1%* |
| SET   | -1.0%* | -1.0%* | +1.3%* | -3.0% |

**The six non-inlined families all improved, on both architectures**, by roughly
the margin §2.13 recorded as lost — INCR gained back 23.5% against a recorded
17.5% loss, HSET 18.3% against 14.7%, LPUSH 11.3% against 13.1%. The families
that never regressed (GET, SET) did not move.

**The one loss §2.13 recorded that has *not* come back is SET at p=64** (-14.9%
there; -1.0% x86 / -3.0% ARM here, i.e. flat). Whatever cost SET at depth is
still in the tree.

Two checks that this is moon moving and not the host: the raw moon ops/s delta
tracks the ratio change on every row (INCR p=8 x86: ratio +16.2%, raw +16.7%),
and Redis — unchanged code, measured in both runs — drifted by a median of
**-0.4%** (x86) and **+0.3%** (ARM) between them. The two runs are sequential
rather than interleaved with each other, which is the weaker design; it is
adequate here only because the control held still, and that was verified rather
than assumed.

---

## 3. Memory

Fresh server per data point, `-r 200000` distinct keys (`DBSIZE ~ 173,000`),
per-key = (loaded RSS - idle RSS) / DBSIZE, RSS read from `/proc/<pid>/status`.

Every row is checked against its arithmetic floor — `key(16) + value + 24`, the
bytes that must exist with zero allocator slack. A leg reporting below its own
floor aborts, because that is physically impossible and means the harness is
wrong. An earlier published per-key win did exactly that, undetected, because it
was measuring `redis-benchmark`'s default **3-byte** value.

| value size | moon x86 | Redis x86 | ratio | moon ARM | Redis ARM | ratio |
|---|---:|---:|:---:|---:|---:|:---:|
| 8 B    | 97.9 B | 125.1 B | **0.78x** | 97.5 B | 113.0 B | **0.86x** |
| 16 B   | 113.7 B | 136.4 B | **0.83x** | 113.9 B | 128.7 B | **0.89x** |
| 32 B   | 130.6 B | 158.3 B | **0.83x** | 129.8 B | 145.6 B | **0.89x** |
| 48 B   | 146.5 B | 175.0 B | **0.84x** | 145.7 B | 161.9 B | **0.90x** |
| 64 B   | 163.9 B | 178.8 B | **0.92x** | 162.8 B | 177.0 B | **0.92x** |
| 128 B  | 229.2 B | 259.8 B | **0.88x** | 228.9 B | 257.3 B | **0.89x** |
| 256 B  | 362.2 B | 421.7 B | **0.86x** | 361.1 B | 419.0 B | **0.86x** |
| 1024 B | 1164.6 B | 1393.3 B | **0.84x** | 1161.9 B | 1386.1 B | **0.84x** |
| **idle RSS** | 13.01 MB | 13.19 MB | 0.99x | 11.43 MB | 11.93 MB | 0.96x |

**moon uses less memory per key at every size measured, on both architectures** —
8-22%, weakest at 64 B and strongest at 8 B, where `CompactValue` inlines the
value into the entry (values <=12 B).

The result does not depend on the idle-RSS subtraction: moon's **absolute**
loaded RSS is lower at every size too (1 KB values, x86: 210 MB vs 249 MB).

### This contradicts two archived claims

Both are recorded rather than quietly dropped:

- The archive's §3.2 reports moon **11-51% worse at 32 B** on x86 `--shards 1`.
  16/32/48/128 B were measured here specifically to test that point, on both
  architectures: moon is **0.83x** at 32 B on x86 and **0.89x** on ARM — a
  17% and 11% win. Not reproduced on either. That run compared against Redis
  **7.4.2**; this one against 7.0.15.
- The archive's §3.1 reports idle RSS moon 12.6-12.9 MB vs Redis 7.5-7.7 MB
  (moon 1.7x worse). Here Redis 7.0.15 idles at **13.19 MB** on the same host
  class, making the pair a tie. The disagreement is on the *Redis* side, which
  is what [#821](https://github.com/pilotspace/moon/issues/821) tracks.

The ARM idle figures do reproduce the archive's §2.14 sidebar (11.43 vs 11.93 MB
here, 11.68 vs 11.96 MB there) — independent agreement on a different date.

**Scope:** `--shards 1`, string values, one key shape. The archived §2.14 measured
`--shards 8` on ARM and found moon **26% worse on idle RSS**, because default
config spawns four threads per shard. Nothing here contradicts that; it is a
different configuration.

---

## 4. Not re-measured on this date

Carried from the archive with their original dates and venues. These are the
most recent measurement of each subsystem, **not** measurements of `ae6cd003`.

| area | last measured | result | where |
|---|---|---|---|
| Durability write path (AOF) | 2026-07-08, GCE x86 | `everysec` SET P16 **1.32x**, P1 0.99x; `always` P16 0.91x; pub/sub 5.09 M msg/s | archive §7.3 |
| Vector vs Qdrant | 2026-07-08, GCE both arches | ingest **10x**, search **2.7-3.4x**, time-to-green **1.6-2.3x** | archive §10.9 |
| Vector ANN-benchmarks | 2026-07-08, GCE ARM | iso-recall **1.7-3.2x** vs RediSearch, **2.9-5x** vs Qdrant | archive §10.10 |
| Graph Cypher vs FalkorDB | 2026-07-07, GCE dedicated | 1-hop **2.34x** (x86) / **2.67x** (ARM) | archive §11.9 |
| Full-text vs RediSearch | 2026-06-17, GCE | counts exact; index 15,052 docs/s; low-DF wins, multi-term trails | archive §12.3 |
| Multi-shard scaling | 2026-09-01, GCE ARM | s8/s1 = 1.42x (p=1), 2.14x (p=8), 3.79x (p=64) | archive §2.14 |
| moon s8 vs Redis io-threads 8 | 2026-09-01, both arches | **1.26-1.32x** (p=8), **2.74-2.91x** (p=64); CPU tie; memory loss at 64 B | archive §2.14 |

---

## 5. Known gaps

- **Only 8 command families are covered.** GET, SET, INCR, LPUSH, SADD, SPOP,
  HSET, ZADD. Ranges, scans and every multi-key command are unmeasured against
  Redis.
- **`--shards 1` only.** The shard-scaling and io-threads comparisons above are
  from 2026-09-01 and have not been repeated on this tree.
- **No persistence leg.** All rows are `--appendonly no --disk-offload disable`.
  The default config a user actually gets (`--appendonly yes`, disk-offload on)
  is not measured here; the archive records a ~53% default-only SET deficit that
  `--disk-offload disable` had been hiding for months.
- **No latency distribution.** Throughput only. `redis-benchmark` is closed-loop,
  so its p50 is a median under load, not a tail-latency result.
- **RESP2 only.** `redis-benchmark` cannot negotiate RESP3.

---

## 6. Reproduce

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release   # fat LTO, cgu=1

# Throughput: 8 families x 3 depths, n=5 interleaved, Redis re-measured every
# rep. Provenance (binary sha256, versions, CPU) goes into the CSV header.
./scripts/bench-ab-matrix.sh --moon-bin ./target/release/moon --reps 5 > matrix.csv
python3 scripts/bench-ab-report.py matrix.csv     # ratios + noise floors

# Memory: fresh server per point, arithmetic-floor guard.
./scripts/bench-ab-memory.sh --moon-bin ./target/release/moon --reps 3 > mem.csv
./scripts/bench-ab-memory.sh --sizes 16,32,48,128 --reps 2 > mem-fine.csv
```

Both scripts refuse to report rather than report something false: the matrix
aborts a leg whose `DBSIZE` is under 50,000 (the keyspace never materialised),
and the memory harness aborts a row landing below its arithmetic floor.

Requires `redis-server` and `redis-benchmark` on PATH. Run on Linux; a macOS run
exercises neither io_uring nor the Linux-only paths.

The raw CSVs behind every table above — including the v0.8.7 legs, with binary
sha256 and CPU model in each header — are kept in
[`docs/internal/bench-data/2026-09-08/`](docs/internal/bench-data/2026-09-08/),
so any row here can be recomputed without re-running anything.

### A note on `redis-benchmark` output parsing

`redis-benchmark -t lrange_100` prints **two** `requests per second` lines — the
LPUSH used to seed the list, then the LRANGE — and a parser taking the first
match reports the seeding rate under the LRANGE label. `scripts/bench-compare.sh`
does this, so its four LRANGE rows have never been LRANGE numbers. Separately, a
"first numeric field" parser reports `1` as the throughput of
`zadd z:__rand_int__ 1 m:__rand_int__`, having found the literal score. The
harnesses here anchor on the position of the words `requests per second`.
