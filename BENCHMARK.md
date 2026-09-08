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

n=5 interleaved reps per throughput point. Memory points are n=3 fresh servers,
except the four sizes added later to test a specific archived claim (16/32/48/
128 B), which are n=2 and marked `†` in §3.

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
| **vs v0.8.7:** non-inlined families, p>=8 | **+5 to +23%** | **+5 to +18%** (raw ops/s, 12/12 rows up) |

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
at least 2.4 floor-widths from 1.0, except ARM GET p=1, which sits at 1.19 —
close enough that it should be read as "at or just above parity", not a win.

These floors are 3-4x wider than the archive's §2.12 run (0.2-3.6%) because the
request counts here are smaller (100k/400k/1.5M at p=1/8/64 against its
200k/600k/4M). This run trades precision for covering two more command families
and a second binary; where a tighter number matters, §2.12's method is the one
to repeat.

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
own freshly-measured Redis.

Change in the moon/Redis **ratio**, and — because a ratio moves when either
side does — the change in moon's **raw ops/s**, which cannot be moved by Redis:

| command | x86 p=8 ratio / raw | x86 p=64 ratio / raw | ARM p=8 ratio / raw | ARM p=64 ratio / raw |
|---------|:---:|:---:|:---:|:---:|
| INCR  | +16.2% / **+16.7%** | +23.5% / **+23.1%** | +14.0% / **+12.2%** | +17.7% / **+17.5%** |
| HSET  | +14.0% / **+14.8%** | +18.3% / **+18.9%** | +11.1% / **+8.9%** | +16.1% / **+13.9%** |
| SPOP  | +25.0% / **+13.2%** | +15.0% / **+17.0%** | -1.4% / **+7.1%** | +14.2% / **+14.9%** |
| SADD  | +5.3% / **+4.6%** | +12.0% / **+10.3%** | +4.7% / **+4.9%** | +12.4% / **+13.3%** |
| LPUSH | +9.7% / **+9.5%** | +11.3% / **+11.3%** | +4.9% / **+7.2%** | +8.5% / **+9.1%** |
| ZADD  | +6.8% / **+5.4%** | +6.5% / **+5.5%** | +20.8% / **+5.3%** | +7.7% / **+5.8%** |
| GET   | +0.6% / +2.6% | +1.9% / +0.0% | +2.9% / +3.0% | +0.1% / +0.8% |
| SET   | -1.0% / -1.4% | -1.0% / -3.5% | +1.3% / +0.7% | -3.0% / -0.3% |

**Read the raw column.** On it, all twelve non-inlined rows improved on both
architectures — **+4.6% to +23.1%** on x86 and **+4.9% to +17.5%** on ARM, no
exceptions — while GET and SET moved by at most 3.5%. That is roughly the margin
the archive's §2.13 recorded as lost (INCR -17.5%, SPOP -21.4%, HSET -14.7%,
LPUSH -13.1%), so the write-path regression is reversed.

**The one loss §2.13 recorded that has *not* come back is SET at p=64** (-14.9%
there; -3.5% raw x86 / -0.3% raw ARM here, i.e. flat). Whatever cost SET at depth
is still in the tree.

**Where the two columns disagree, trust the raw one, and do not quote the
ratio.** Redis was re-measured in both runs and drifted by a median of -0.4%
(x86) / +0.3% (ARM) — but the *per-cell* drift ranges -9.4%…+1.9% and
-12.8%…+8.6%, and the two widest-drifting cells are exactly the ones that
produce the biggest ratio numbers: x86 SPOP p=8 reads +25.0% by ratio and
+13.2% raw because Redis fell 9.4%, and ARM ZADD p=8 reads +20.8% by ratio and
+5.3% raw because Redis fell 12.8%. ARM SPOP p=8 flips sign for the same reason
(-1.4% by ratio, +7.1% raw, Redis up 8.6%). An earlier draft of this section
claimed the raw delta tracked the ratio on every row; it does not, and the two
places it fails were the two headline maxima.

These two runs are sequential rather than interleaved with each other, which is
the weaker design — that is precisely why the raw column is published here
rather than the ratio alone.

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
| 16 B † | 113.3 B | 136.1 B | **0.83x** | 113.3 B | 128.7 B | **0.88x** |
| 32 B † | 130.1 B | 158.3 B | **0.82x** | 129.7 B | 145.6 B | **0.89x** |
| 48 B † | 146.1 B | 174.9 B | **0.84x** | 145.7 B | 161.8 B | **0.90x** |
| 64 B   | 163.9 B | 178.8 B | **0.92x** | 162.8 B | 177.0 B | **0.92x** |
| 128 B †| 230.0 B | 259.8 B | **0.89x** | 228.9 B | 257.3 B | **0.89x** |
| 256 B  | 362.2 B | 421.7 B | **0.86x** | 361.1 B | 419.0 B | **0.86x** |
| 1024 B | 1164.6 B | 1393.3 B | **0.84x** | 1161.9 B | 1386.1 B | **0.84x** |
| **idle RSS** | 13.01 MB | 13.19 MB | 0.99x | 11.43 MB | 11.93 MB | 0.96x |

`†` = n=2 repetitions; every other row is n=3.

**moon uses less memory per key at every size measured, on both architectures** —
8-22% on x86 and 8-16% on ARM. The weakest point on both is 64 B. The strongest
differs: on x86 it is 8 B (0.78x), where `CompactValue` inlines the value into
the entry (values <=12 B); on ARM it is 1 KB (0.84x).

The result does not depend on the idle-RSS subtraction: moon's **absolute**
loaded RSS is lower at every size too (1 KB values, x86: 210 MB vs 249 MB).

### This contradicts two archived claims

Both are recorded rather than quietly dropped:

- The archive's §3.2 reports moon **11-51% worse at 32 B** on x86 `--shards 1`.
  16/32/48/128 B were measured here specifically to test that point, on both
  architectures: moon is **0.82x** at 32 B on x86 and **0.89x** on ARM — an
  18% and 11% win. Not reproduced on either. That run compared against Redis
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
| Vector vs Qdrant | 2026-07-08, GCE both arches | ingest **5.8-10.4x**, search **2.8x** (ARM) / **3.6x** (x86), time-to-green **1.6x** (x86) / **2.3x** (ARM) | archive §10.9 |
| Vector ANN-benchmarks | 2026-07-08, GCE ARM | glove-200 at iso-recall: **1.7-3.2x** vs RediSearch, **2.9-3.1x** vs Qdrant (gist-960 differs — see archive) | archive §10.10 |
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
python3 scripts/bench-ab-report.py matrix.csv 5   # ratios + noise floors
#                                            ^ expected reps: pins the
#   completeness check, so a run truncated by an aborted leg reports
#   INCOMPLETE instead of quietly redefining "all reps" as whatever survived.

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
