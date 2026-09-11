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

**Two later runs amend this table without replacing it** (§2 keeps each as
measured). As of 2026-09-11: ZADD is no longer the worst family — it gained
+39.8% at p=64 and **HSET is now last**; and the memory line above is
string-only. On *containers*, moon is 0.54-0.56x on sets and 0.89-0.91x on
lists, but **1.21-1.29x on hashes and 1.29-1.38x on sorted sets** — i.e. worse
than Redis on the two shapes where both engines use the same encoding. "moon
uses less memory per key" is a claim about string values, and does not
generalise to containers.

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

### Update 2026-09-10 — two of these rows have since moved

The table above is the `ae6cd003` record and is left as measured. A re-run on
2026-09-10 (moon `c095f86d`, same two host classes, same Redis 7.0.15, same
harness, n=5) found two families materially changed. That run is on kernel
`7.0.0-1011-gcp`, not the 6.17 above, and moon's absolute GET p=64 came in at
1.31 M/s against the 1.53 M here — so `ae6cd003` was **rebuilt and re-run on the
same machines** rather than compared across dates. It reproduces the ARM ratios
above within 0.02-0.04 on six of eight families.

ARM is the leg to read: Redis drifted <1% between the two ARM runs, so raw and
ratio agree. On x86 Redis fell 3.7-9.5% across the session, so only x86 ratios
are quoted.

| command | ARM p=64 ratio | moon raw ARM p=64 | x86 p=64 ratio |
|---|:---:|:---:|:---:|
| **ZADD** | 0.49x -> **0.68x** | **+38.0%** | 0.47x -> **0.72x** |
| **SADD** | 0.72x -> 0.63x | **-13.9%** | 0.76x -> 0.69x |
| SPOP | 0.86x -> 0.78x | -8.3% | 0.75x -> 0.67x |
| INCR / LPUSH / HSET | within 0.03x | -3.0 to -4.8% | within 0.03x |

**Two claims above no longer hold.** ZADD is no longer the worst family, and it
is no longer the only family losing at p=1 on both arches — **HSET is now the
worst** (0.587x ARM / 0.599x x86 at p=64). ZADD's gain bisects cleanly to the
window opening with #878 (*ZADD reaches its listpack encoding*): flat across the
two earlier windows, +44.7% in the last.

The SADD/SPOP loss does **not** bisect to one commit — it is spread across three
windows. The obvious suspect, #877 (*SADD reaches its listpack encoding*), was
isolated against its own parent and costs SADD 2.7% (at its noise floor) and
SPOP 3.8%; it is a minor contributor, not the cause. Tracked as
[#923](https://github.com/pilotspace/moon/issues/923). Raw CSVs for all six runs
are in [`docs/internal/bench-data/2026-09-10/`](docs/internal/bench-data/2026-09-10/).

> **Superseded 2026-09-11 — "spread across three windows" was wrong.** It came
> from a three-point split whose middle window sat at its own noise floor. A
> four-point split at n=10 puts the whole loss in **one** commit, #861; the two
> flanking windows are ties on all 48 of their cells. See the next subsection.
> The #877 figure above stands — it remains a tie at its own floor.


### Update 2026-09-11 — #938 costs nothing measurable, and SADD/SPOP is attributed

Ten PRs landed after the 2026-09-10 run, two of them on the measured hot path:
**#938** put a `stamp_mutation` on all 23 `&mut` accessor acquisitions —
disassembly put that at **+8 instructions per container write**, never
throughput-tested — and **#929** added arms to the keyless table that *every*
command traverses. This run is the regression check.

`5bf716a9` against `ab91a23e`, same two hosts, same Redis 7.0.15, same harness,
**n=10** per point (two passes of five, run `A,B,B,A` so a monotone session
drift cannot land on one binary). The baseline binaries are byte-identical to
the ones measured on 2026-09-10 — their sha256s match that run's CSV headers —
so the two reports are continuous. Raw CSVs, with per-file provenance, in
[`docs/internal/bench-data/2026-09-11/`](docs/internal/bench-data/2026-09-11/).

**#938's +8 instructions do not resolve.** The four families it touches, at the
two depths where per-op cost is the whole budget:

| family | ARM raw Δ | ARM floor | x86 raw Δ | x86 floor |
|---|---:|:---:|---:|:---:|
| SADD p=8 | +0.6% | 1.4% | -1.1% | 0.8% |
| SADD p=64 | +1.9% | 1.7% | -0.3% | 0.7% |
| HSET p=8 | -0.2% | 2.2% | -0.4% | 1.5% |
| HSET p=64 | +0.6% | 1.8% | -1.3% | 1.3% |
| LPUSH p=8 | +0.1% | 1.9% | +0.0% | 1.2% |
| LPUSH p=64 | -1.5% | 1.6% | +0.7% | 1.1% |
| ZADD p=8 | -0.7% | 2.2% | +0.2% | 1.3% |
| ZADD p=64 | -0.2% | 1.5% | +0.5% | 3.0% |

Fifteen of sixteen cells are ties. The one exception — ARM SADD p=64, +1.9%
against a 1.7% floor — is a *gain*, which added instructions cannot cause, and
the two architectures disagree in sign on six of eight rows. **This is the
absence of a resolvable effect, not a measured zero:** 8 instructions against a
~3.2 µs/op budget (ARM SADD p=64) is ~0.1% of the work, an order of magnitude
under the floor this harness supports. It would need a different instrument —
`perf stat` on instruction counts, not ops/s — to see at all. #929 likewise
moved nothing: SET, GET and INCR are ties at every depth on both hosts.

**HSET is still the worst family**, and the p=64 shape is unchanged:

| command | ARM p=1 | ARM p=8 | ARM p=64 | x86 p=1 | x86 p=8 | x86 p=64 |
|---------|:---:|:---:|:---:|:---:|:---:|:---:|
| GET   | **1.05x** | **1.17x** | **1.53x** | **1.06x** | **1.30x** | **1.61x** |
| SET   | **1.03x** | **1.15x** | **1.46x** | **1.04x** | **1.21x** | **1.31x** |
| INCR  | 0.93x | 0.68x | 0.63x | **1.12x** | 0.72x | 0.55x |
| LPUSH | 0.87x | 0.75x | 0.71x | **1.09x** | 0.73x | 0.64x |
| SADD  | 0.87x | 0.67x | 0.66x | **1.08x** | 0.68x | 0.67x |
| SPOP  | 0.79x | 0.71x | 0.80x | 0.99x* | 0.67x | 0.67x |
| HSET  | 0.86x | 0.66x | **0.62x** | **1.06x** | 0.65x | **0.55x** |
| ZADD  | 0.82x | 0.80x | 0.70x | **1.03x** | 0.83x | 0.70x |

Bold in the HSET row marks the worst cell per architecture, not a win. INCR is
now statistically tied with it at the bottom (ARM 0.632x against HSET's 0.620x,
floors 1.4% and 1.6%; x86 0.553x against 0.546x, floors 1.5% and 3.1%) — read
them as a shared last place, not a ranking.

**ZADD's +38% held, and it now survives a read.** Against `ae6cd003` on the
same host, ZADD p=64 is **+39.8%** raw (0.502x → 0.704x, floor 1.9%, Redis
control -0.4%), and it gained at the shallower depths too (+15.9% at p=1,
+16.6% at p=8). But the 2026-09-10 figure was **write-only**, and that mattered
more than it looked: before **#932**, one `ZSCORE` taken on the *mutable*
dispatch path — inside MULTI/EXEC, inside Lua, or through `try_inline_dispatch`
— converted a listpack zset to a skiplist permanently.

| binary | read path | encoding after | RSS/key, before → after |
|---|---|---|---|
| `ab91a23e` | plain connection | listpack | 218.0 → 218.5 B (+0.2%) |
| `ab91a23e` | inside MULTI/EXEC | **skiplist** | **217.8 → 4,433.1 B (+1935%)** |
| `ab91a23e` | inside Lua (EVAL) | **skiplist** | **217.9 → 4,433.0 B (+1934%)** |
| `5bf716a9` | all three | listpack | 217.8 → 219.1–220.6 B (+0.6–1.2%) |
| Redis 7.0.15 | all three | listpack | 141.5 → 141.9–142.2 B (+0.3–0.5%) |

(ARM; x86 reaches 4,441 B by the same two paths and is otherwise within 0.2%.) Both instruments were read — `OBJECT ENCODING`
and `/proc` RSS — and they agree. Note the plain-connection row: the defect was
never reachable from the read-only dispatch path, so a probe issuing a bare
`ZSCORE` runs **clean against the buggy binary** and proves nothing. That
negative control is why the sweep covers all three paths.

**moon#923 is settled: SADD/SPOP bisects to #861.** The largest unexplained
window, `ae6cd003..1a7bd83f` (12 commits), was split at three points on ARM,
two passes each, n=10 per point:

| window | commits | SADD p=64 | SPOP p=64 | ZADD p=64 |
|---|---|---:|---:|---:|
| W1 | the first 8 | +0.2% *tie* | +0.7% *tie* | -0.6% *tie* |
| **W2** | **#861 alone** | **-6.4%** | **-4.5%** | **-4.7%** |
| W3 | the last 3 | +2.0% *tie* | -0.1% *tie* | +0.7% *tie* |
| endpoint | all 12 | -4.4% | -4.0% | -4.6% |

W1 and W3 are ties on every one of their 24 cells — 48 cells, no survivors. The
entire window is one commit: **#861, `perf(storage): box RedisValue's fat
variants`** (`75ad520c`). Within W2, SET, GET, INCR, LPUSH and HSET are ties, so
the cost falls on SADD, SPOP and ZADD specifically.

**The mechanism is not established, and the obvious explanation is wrong.** The
natural reading — #861 boxed `Set` and `SortedSet`, so SADD/SPOP/ZADD pay a
dependent load per access — was tested and does not hold. Under the matrix
harness's *own* load, every container stays compact:

| key | encoding | elements |
|---|---|---:|
| `set:…` | **listpack** | 8–19 |
| `z:…` | **listpack** | 14 |
| `list:…` | **listpack** | 15 |
| `hash:…` | **listpack** | 1 |

`SetListpack` and `SortedSetListpack` are the 24-byte variants; #861 never
boxed them, and none of the benchmarked keys ever reaches `RedisValue::Set` or
`SortedSetBPTree`. So the regression is **not** boxed-payload indirection. What
#861 did change for every container key is the enum itself — 128 B → 40 B,
moving `CompactValue`'s block from jemalloc's 128-byte size class to the
48-byte one. Why that costs SADD/SPOP/ZADD ~5% while leaving LPUSH and HSET
(also listpack, comparable payloads) untouched is **unexplained**, and this run
does not explain it. `perf` on the two W2 binaries is the next step.

This also settles the standing #861 refutation, in both directions. That comment
traced `HSET`, found it takes `get_or_create_hash_listpack`'s early-true branch
and constructs no `RedisValue`, and concluded no box is allocated — **all of
which the bisect confirms**: HSET is a tie in W2. The error was generalising
from HSET to SADD/SPOP, which the comment itself flagged as unmeasured
("whether traversing an already-boxed variant costs on *read* paths is a
separate question"). #861 is now attributed by bisect, not by mechanism. Neither
the original hypothesis nor its refutation had the reason right.

The trade was deliberate and is documented in #861 itself — 80 B/key saved on
every container key. What was missing is the throughput side of it, which is
this table. It is a real, measured cost that nobody priced at the time, not a
defect, and not on its own a reason to revert.

**Two rows where the control failed.** Redis's own series went bimodal on ZADD
p=8 (clustering at ~275k and ~320k within one session) and SPOP p=8. Dividing by
a control that noisy manufactures a result: the naive ratio reads **+13.0%** for
ZADD p=8 on ARM, while moon's own raw series moved -0.7% against a 2.2% floor.
No number is published for those rows. `scripts/bench-ab-delta.py` now refuses
any row whose control CV exceeds 5% and says so, rather than quoting the ratio.

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

### Containers — added 2026-09-11

Everything above measures **string values**. The compact-encoding campaign
(moon#897, moon#830 → #920/#921/#922/#932) exists to make *containers* cheap,
and until this run that was measured nowhere. `scripts/bench-ab-memory-containers.sh`
loads 200,000 keys of exactly four 8-byte elements and reports **two** figures
per type, because either alone misleads:

- **untouched** — the container as first written. A best case.
- **touched** — after **one** secondary write per key, using the very mutator
  each PR taught to mutate in place (HINCRBY, LSET, SREM, ZINCRBY). Any workload
  that writes a key twice sees this number, not the one above.

| type | phase | moon x86 | moon ARM | Redis x86 | Redis ARM | ratio x86 | ratio ARM |
|---|---|---:|---:|---:|---:|:---:|:---:|
| hash | untouched | 203.2 B | 202.7 B | 168.6 B | 157.5 B | 1.21x | 1.29x |
| hash | touched   | 203.9 B | 203.7 B | 168.4 B | 157.8 B | 1.21x | 1.29x |
| list | untouched | 217.1 B | 216.6 B | 244.6 B | 237.2 B | **0.89x** | **0.91x** |
| list | touched   | 217.3 B | 216.9 B | 244.4 B | 237.8 B | **0.89x** | **0.91x** |
| set  | untouched | 217.6 B | 217.4 B | 400.3 B | 385.8 B | **0.54x** | **0.56x** |
| set  | touched   | 218.3 B | 218.4 B | 400.2 B | 386.0 B | **0.55x** | **0.57x** |
| zset | untouched | 217.4 B | 217.3 B | 168.9 B | 157.8 B | 1.29x | 1.38x |
| zset | touched   | 217.9 B | 217.7 B | 168.7 B | 158.1 B | 1.29x | 1.38x |

n=3, fresh server per point, engine order alternating, each row checked against
its arithmetic floor. A re-run of the whole matrix reproduced every cell to
within 0.5 B.

**Two of these four rows are not like-for-like**, which `encodings-arm.txt`
records rather than leaves to assumption. Against Redis **7.0.15**:

| shape | moon | Redis 7.0.15 |
|---|---|---|
| hash, 4 small fields | listpack | listpack |
| zset, 4 small members | listpack | listpack |
| list, 4 small elements | listpack | **quicklist** |
| set, 4 non-integer members | listpack | **hashtable** |

`set-max-listpack-entries` does not exist in 7.0.15 — listpack-encoded sets
arrived in Redis 7.2 — so a four-member set of non-integer strings is forced to
a hashtable, and moon's 0.54x there is listpack-against-hashtable, not a like
encoding beaten. The list row compares against a quicklist. **Only hash and zset
are a fair encoding comparison, and moon loses both**, by 21–29% (hash) and
29–38% (zset). moon's per-container-key cost is near-constant at ~203–218 B
across all four types, while Redis's varies with its encoding; that flatness is
what wins the set row and loses the hash and zset rows.

**What the campaign bought.** The `touched` column is flat at HEAD — one
secondary write costs +0.1% to +0.5%. Before #920/#921/#922 the same write
flattened the container:

| type | one secondary write, `ab91a23e` | at `5bf716a9` |
|---|---:|---:|
| hash (HINCRBY) | 202 B → 898 B (**+344%**) | +0.3% |
| list (LSET)    | 217 B → 404 B (**+86%**)  | +0.1% |
| set (SREM)     | 218 B → 663 B (**+205%**) | +0.3% |
| zset (ZINCRBY) | 217 B → 4,478 B (**+1959%**) | +0.2% |

(x86. ARM agrees on hash, list and zset within 0.2 points and reaches 645 B on
the set row, +197%.)

That is the answer to why both figures are published. On `ab91a23e` the
`untouched` number was a best case no real workload saw — a single ZINCRBY cost
20x the key's memory — and a report quoting only it would have been accurate and
completely misleading. Redis is flat in both columns on every type.

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

**Scope:** `--shards 1`, one key shape; string values above, containers in the subsection below. The archived §2.14 measured
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

# Same binary vs a different binary (did THIS commit regress moon?). Redis is
# the control: the tool picks raw or ratio per row from the control's own
# movement, and publishes NEITHER when the control's CV exceeds 5%.
python3 scripts/bench-ab-delta.py --head new.csv --base old.csv

# Memory: fresh server per point, arithmetic-floor guard.
./scripts/bench-ab-memory.sh --moon-bin ./target/release/moon --reps 3 > mem.csv
./scripts/bench-ab-memory.sh --sizes 16,32,48,128 --reps 2 > mem-fine.csv

# Containers: per-key RSS for hash/list/set/zset, reported BOTH untouched and
# after one secondary write -- the untouched figure alone is a best case.
./scripts/bench-ab-memory-containers.sh --moon-bin ./target/release/moon --keys 200000 --reps 3

# Does a READ destroy a small zset's encoding? Sweeps the plain, MULTI/EXEC and
# Lua dispatch paths, because the defect this checks for was unreachable from
# the read-only path -- a bare ZSCORE runs clean on a BUGGY binary.
./scripts/bench-zset-read-encoding.sh --moon-bin ./target/release/moon --keys 200000
```

These scripts refuse to report rather than report something false: the matrix
aborts a leg whose `DBSIZE` is under 50,000 (the keyspace never materialised),
the memory harnesses abort a row landing below their arithmetic floor, all of
them refuse to start on an occupied port (a stranger's numbers would otherwise
be attributed to both engines), and `bench-ab-delta.py` suppresses any row whose
Redis control is too unstable to divide by.

Requires `redis-server` and `redis-benchmark` on PATH. Run on Linux; a macOS run
exercises neither io_uring nor the Linux-only paths.

The raw CSVs behind every table above — including the v0.8.7 legs, with binary
sha256 and CPU model in each header — are kept in
[`docs/internal/bench-data/2026-09-08/`](docs/internal/bench-data/2026-09-08/),
with the two re-measurements in
[`2026-09-10/`](docs/internal/bench-data/2026-09-10/) and
[`2026-09-11/`](docs/internal/bench-data/2026-09-11/), so any row here can be
recomputed without re-running anything.

### A note on `redis-benchmark` output parsing

`redis-benchmark -t lrange_100` prints **two** `requests per second` lines — the
LPUSH used to seed the list, then the LRANGE — and a parser taking the first
match reports the seeding rate under the LRANGE label. `scripts/bench-compare.sh`
does this, so its four LRANGE rows have never been LRANGE numbers. Separately, a
"first numeric field" parser reports `1` as the throughput of
`zadd z:__rand_int__ 1 m:__rand_int__`, having found the literal score. The
harnesses here anchor on the position of the words `requests per second`.
