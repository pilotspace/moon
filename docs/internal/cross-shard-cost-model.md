# The cross-shard cost model — what was measured, and what was disproved

Written after a multi-week investigation into why moon lost to Redis on
non-pipelined workloads. Its purpose is **negative knowledge**: seven
optimisations were measured and found worthless, and five of my own published
claims turned out to be wrong. Both lists are here so nobody spends another
afternoon re-deriving them.

Every number below comes from a Linux host — GCE `t2a-standard-8` (aarch64,
8 vCPU), dedicated load generator, one server at a time, populated keyspace.
No macOS number appears in this document, and none should be added: io_uring,
O_DIRECT, connection migration and the spin governor's `/proc` sampling are all
`cfg(target_os = "linux")`, so a native run cannot exercise them.

---

## 1. The finding: cost is per-park, not per-message

Sweeping pipeline depth and fitting per-command CPU against the two candidate
terms:

| pipeline depth | CPU%/kops | msgs/cmd | parks/cmd |
|---|---|---|---|
| p=1  | 2.55 | — | — |
| p=4  | 1.00 | — | — |
| p=8  | 0.70 | — | — |
| p=16 | 0.55 | — | — |

```text
cost = 0.413 − 0.046·msgs/cmd + 2.488·parks/cmd        (CPU%/kops)
```

**A park costs 2.49 CPU%/kops ≈ 24.9 core-µs. A cross-shard message costs
approximately nothing.** At p=1 the park term is 85% of total cost.

The fit is corroborated model-free: from p=1 to p=16, messages per command fall
2.3×, parks per command fall 14×, and measured cost falls 4.6× — tracking parks,
not messages.

**This is the single most useful result in the investigation.** It reframes
every cross-shard optimisation: the question is never "can I send fewer
messages", it is "can I avoid the park". An optimisation that halves SPSC
traffic while preserving one park per command is worth ~nothing.

### Independent corroboration (PR #768)

#768 fans a spanning multi-key read into the existing slotted batch instead of
deferring the batch tail. Measured on the same rig, `--shards 4`:

| shape | control | fix | delta |
|---|---:|---:|---|
| bulk | 24,093 ops/s | 386,267 ops/s | +1503% |
| round-trip | 22,529 ops/s | 34,048 ops/s | +51.1% |

Deferrals fell 7,938 → 0 and 6,000 → 0. Every deferral is a batch boundary and
therefore a park. The commit message credits removing SPSC sends; the model says
the win is the parks, and the model is what the pipeline sweep measured
directly. Same fix, same magnitude, different mechanism — and the mechanism is
what predicts where the pattern pays next.

---

## 2. Where the time actually goes

`perf record -F 999 -g`, 8 shards, p=1:

| bucket | share |
|---|---|
| kernel (network stack) | **59.9%** |
| moon user time | 37.7% |

Splitting that user time:

| bucket | share of user time |
|---|---|
| monoio runtime | 32.6% |
| bare atomic RMW | 15.0% |
| **actual Redis work** | **16.8%** |

**83% of moon's user time is not Redis work.** Against Redis on the same host,
65% of moon's excess cost is USER time (9.5 vs 3.1 core-µs/op) — so "it's all
kernel, nothing to do" is false, even though the kernel is the single largest
bucket.

*Instrumentation note:* bucket on the DSO column (`$3`) plus the `[k]` marker,
not the thread-name column (`$2`). Getting this wrong put 51% in "other" and
made an 8-shard profile total 111%. Kernel symbols need
`sysctl kernel.kptr_restrict=0` and `perf_event_paranoid=-1`, or they resolve as
bare addresses.

---

## 3. Seven measured dead ends — do not re-propose these

| # | Idea | Result |
|---|---|---|
| 1 | Cross-connection batching | Predicted **0.99×**. Batching more work per message optimises the term measured at ~zero. |
| 2 | Remove the per-reply `Box::pin(sleep(30s))` timer | ON 2.540 vs OFF 2.545 CPU%/kops — **indistinguishable**. The timer is a monoio wheel entry, not an io_uring op; it is nearly free. |
| 3 | Extend the inline path to foreign reads | Inline forfeit is only **0.187** of commands; a perfect fix caps at **+7%**. |
| 4 | Reply-side spin | 302K → 272K → 100K → **9,303 rps** as `MAX_CONNS` rises: a **32× convoy collapse**. Bench-only, never production. |
| 5 | Shrink the connection future to help re-poll | The future is **6,208 bytes**; shrinking it changed nothing measurable. |
| 6 | Idle spin | Costs 2% CPU at **zero** clients; not a throughput lever. |
| 7 | Connection steering to co-locate keys | Information-theoretically useless for uniform keys: a connection's next key is independent of its last. |
| 8 | Shared guard for reads on the SPSC execute arms | In-place rate 62.7% -> 62.9% at p=1. The exclusive guard was **not** what made foreign `try_read` decline. |

Dead ends 1, 4 and 7 are the ones most likely to be re-invented, because each
sounds obviously correct before it is measured.

Dead end 8 deserves its reasoning written out, because the argument for it was
good. All four SPSC execute arms take `s.databases.write(db_idx)` for every
command, reads included, so an SPSC-routed read holds the owner's database
EXCLUSIVELY for its whole execution — exactly the condition under which a
foreign reader's `db_plane::try_read` returns `None` and diverts to the SPSC
path it was trying to avoid. That predicts a self-sustaining loop, and it
predicts that the fast path's in-place rate is suppressed by its own fallbacks.

Implemented (shared guard + `dispatch_read` for hot, read-supported commands,
exclusive guard otherwise) and measured: **62.7% -> 62.9%** in place at
`--shards 8` p=1. The premise is refuted; see §8 for what the shortfall
actually was. The change was reverted rather than shipped, because a hot-path
change with no measured effect is cost without evidence.

---

## 4. Five retractions — claims I published and later disproved

Recorded because each was believed, acted on, and wrong.

1. **"moon is 0.64× Redis best-vs-best."** A measurement artifact. The control
   leg drifted 43% across the run. Interleave A/B legs and establish a noise
   floor before quoting any ratio.
2. **"moon wins 1.75× at p=16."** Both servers were **client-capped** at
   1.58M rps — the benchmark, not either server, was the bottleneck.
3. **The `(N−1)/N` per-command hop model.** Refuted directly by measurement.
   Connection→shard assignment is by kernel 4-tuple hash and key→shard by
   xxh64, so they are uncorrelated — but the per-command consequence I derived
   from that did not hold.
4. **"The whole deficit is one handoff."** The inline forfeit is 10% of it.
5. **"Foreign readers are blocked by the owner's write guard."** Refuted by the
   S0b experiment (below). This was my own diagnosis and it was wrong.

---

## 5. L4 shared read plane — status

**The prototype result stands:** sharing databases so a foreign shard serves
reads locally measured **+19.4% throughput, −22.4% CPU/op**, on less total CPU.
Against Redis on the same host, with L4 on:

| workload | moon | Redis | ratio |
|---|---:|---:|---|
| uniform random | 363,192 | 317,284 | **1.14×** |
| tag co-located | 518,808 | 345,672 | **1.50×** |
| hot set | 408,582 | 365,882 | **1.12×** |

CPU per operation remains **1.31× Redis** on the uniform workload — moon wins
throughput while still costing more per op.

**S0/S0b returned NO-GO,** and this bounds what to expect from the production
version. S0b made the owner take a *shared* guard for its own reads instead of
an exclusive one, on the prototype's whole-slice lock. Throughput moved 1.018× —
nothing. So owner-write-guard reader-blocking is **not** the remaining
bottleneck, and finer lock granularity will not beat the prototype.

What survives that NO-GO, precisely:

- The production per-`(shard, db)` design is justified on **soundness**, not on
  beating the prototype. The prototype shares the whole `!Send` `ShardSlice`,
  which needs `unsafe` and exposes non-`Sync` vector/text/graph stores. It
  cannot ship in that form at all.
- The realistic target for the safe version is **prototype parity** (~367K rps),
  not better. If acceptance lands materially below that, the design is dead.
- L4-ON sits at **724% of 800%** — unsaturated, with no current hypothesis for
  what limits it. Re-profiling L4-ON is the outstanding question.

---

## 6. Measurement traps that produced wrong numbers here

Each of these silently produced a plausible, wrong result at least once:

- **Piping a gate discards its exit code.** `cargo test … | grep …` reports
  *grep's* status. Redirect to a file and capture `$?`.
- **Impossible CPU samples.** 406K rps at 1024% on an 8-vCPU host. Time the real
  window with `date +%s.%N` at both ends and discard any sample above
  `nproc × 100`; that guard then caught four more.
- **`pkill -f` matches your own session.** The ssh command line contains the
  literal path being matched. Put cleanup and launch in an on-box script.
- **`& disown` inside a compound `gcloud ssh` command silently does not launch.**
  Use a dedicated on-box wrapper script.
- **Python buffers stdout** — a healthy benchmark run looks like a hang. Use
  `python3 -u`.
- **Scraping `/metrics` costs throughput.** Enabling `--admin-port` and scraping
  depressed throughput 11% (327K vs 363K). Within-run comparisons stay valid;
  absolute and cross-run numbers do not.
- **`redis-benchmark`'s built-in tests mostly use ONE key, and `-r` cannot
  change it.** Verified against a live server by `FLUSHDB` + run + `DBSIZE`:
  `lpush`, `rpush`, `lpop`, `rpop`, `sadd`, `spop`, `hset`, `zadd` all touch a
  single literal key (`mylist` / `myset` / `myhash` / `myzset`) with or without
  `-r` — `-r` randomises the *element*, not the key. Only `set`, `get`, `incr`
  and `mset` take a randomised key, and only when `-r` is passed (without it
  the key is the literal string `key:__rand_int__`).

  For a shared-nothing server this is fatal to any scaling claim: a single key
  is owned by a single shard, so `--shards N` executes the whole workload on one
  thread however many threads exist, and `sN/s1 ~= 1.0` is the architecturally
  correct answer rather than a finding. A 12-family matrix built on
  `redis-benchmark -t` has **8 families whose answer is fixed at 1.0 before the
  server is started**. Drive uniform workloads with an explicit command instead
  — `redis-benchmark -r 100000 LPUSH list:__rand_int__ v` — and assert `DBSIZE`
  afterwards.

- **A p=1 leg that is not CPU-bound measures the network, and its ratios
  collapse toward 1.0.** Symptom: throughput barely varies across command
  families whose server cost varies several-fold. On one 12-family ARM matrix
  the p=64 spread across families was 13.7x while the p=1 spread was 1.29x, and
  every p=1 leg (both servers, both shard counts) sat in a 77-110K band — 3-4x
  below what §7 measures for the same configuration at `c=200`. Check the
  per-family spread at p=1 against the spread at p=64 before believing any p=1
  ratio.

- **`moon_dispatch_path_total` is not a remote-fraction metric.** The inline path
  records `path="local_inline"`, and the counter was absent on the prototype
  binary — it read a false 100%.
- **redis-benchmark 8.x emits `\r`** for progress lines: `tr '\r' '\n'` before
  grepping, and match the RPS by position — `awk '{print $2}'` yields
  `summary:`.

---

## 7. Shard-count and preset baselines

`GET`, p=1, c200, populated keyspace, one server at a time:

| config | ops/s |
|---|---:|
| `--shards 1` | 128,758 |
| `--shards 2` | 147,929 |
| `--shards 4` | 195,065 |
| `--shards 8` | 332,779 |
| Redis (`io-threads 8`) | 499,251 |

Scaling is sub-linear, as documented — cross-shard dispatch dominates the local
DashTable lookup. The gap at `--shards 8` is what L4 attacks.

Separately, `--profile standalone` as shipped from v0.7 through v0.8.7 measured
**65,156 ops/s** against stock `--shards 8`'s 332,779 — ~5× slower, the worst
row in a 30-row tuning sweep. That preset paired `--io-busy-poll-us 40` with
`--io-driver epoll`; the epoll readiness spin serialises against a single
shard's accept+read path once more than a handful of connections are live
(#772).

---

---

## 8. The cross-shard read fast path removes every foreign-read park

Measured at `--shards 8`, uniform keyspace, `GET` p=1 c50, same binary, one flag
apart. Counter ratios from `INFO stats` — no wall-clock number is quoted:

| `--cross-shard-fast-path` | served in place | parks/cmd |
|---|---:|---:|
| `off` | 0.0% | 0.87336 |
| `auto` | 100.0% | 0.00023 |

Against the §1 fit that is `0.413 + 2.488 x parks/cmd` = **2.586 -> 0.413
CPU%/kops**, a 6.3x predicted cut in per-op cost for cross-shard reads at p=1.
§2.5's target of 0.486 parks/cmd is not merely met, it is bypassed. The default
flipped to `auto` on this measurement.

**Why it shipped `off` for so long — and why that reading was wrong.** The
evidence was moon#768's `-8.61%` CPU/op and a doubled `s8 p16` variance, plus
the standing puzzle that #768 measured only **50.5%** of reads served in place
where the model predicted 87.5%. All three come from one cause: the fast path
declines a key that is **not resident**, because `dispatch_read` cannot consult
the cold tier (the moon#610 class), and a declined read falls back to the hop.
So the "in-place rate" is the benchmark's **key hit rate**, not a measure of the
mechanism. Populating 100k keys with N `SET`s leaves `1-exp(-N/100k)` of them
resident, and the in-place rate tracks that curve to within 0.3 points:

| resident keys (`DBSIZE`) | predicted coverage | served in place | parks/cmd |
|---:|---:|---:|---:|
| 63,114 | 63.2% | 62.9% | 0.325 |
| 86,396 | 86.5% | 86.2% | 0.121 |
| 98,169 | 98.2% | 98.2% | 0.016 |
| 99,967 | 100.0% | 100.0% | 0.0003 |
| 100,000 | 100.0% | 100.0% | 0.000 |

A run whose hit rate wanders therefore produces exactly the run-to-run variance
that held the default down. **Always populate to saturation before A/B-ing this
flag,** and report `DBSIZE` with the result.

**What this does NOT touch.** Writes. The gate is `!metadata::is_write(cmd)`, so
`INCR`/`LPUSH`/`SADD`/`HSET`/`SET` stay at 0.875 parks/cmd at p=1 — measured
unchanged with the flag on. The write side needs its own mechanism.

### 8.1 The model itself is confirmed, exactly

`INFO stats` counters at `--shards 8`, uniform keys, n=200,000 per cell, against
the closed form derived from `handler_monoio`'s phase-2b batch structure
(`msgs/cmd = (N-1)(1-(1-1/N)^P)/P`, `parks/cmd = (1-(1/N)^P)/P`):

| P | msgs/cmd predicted | measured | parks/cmd predicted | measured |
|---|---:|---:|---:|---:|
| 1 | 0.8750 | 0.8721-0.8758 | 0.8750 | 0.8717-0.8755 |
| 8 | 0.5744 | 0.5722-0.5755 | 0.1250 | 0.1250-0.1260 |
| 64 | 0.1094 | 0.1093-0.1094 | 0.015625 | 0.015625 |

Six command families, four significant figures, no free parameters. **Mean park
depth is 12-18 at `c=50`**, not ~1 — parks overlap heavily, so the 24.9 core-us
constant is a per-park CPU charge and not a serialized wait.

### 8.2 The multi-key coordinator is invisible to every one of these counters

Spanning multi-key writes (`MSET`, `MSETNX`, `BITOP`, `DEL`) do not take the
slotted batch: `multikey_placement` returns `Coordinator`, and
`coordinate_mset` awaits per-shard `MultiExecute` replies on freshly allocated
oneshots **inline, mid-batch**. Measured `MSET` of 4 uniform keys at
`--shards 8`: `total_dispatch_cross_spsc` 0.0016/cmd and `parks/cmd` 0.0016 —
the park model scores it as already optimal — while `spsc_notify_wakes` runs
**1.82, 1.61, 1.51 per command at p=1, p=8, p=64**. It does not amortise with
pipeline depth at all, because every `MSET` is a batch boundary.

That is consistent with the family's measured shape: `MSET` gains only **2.65x**
from p=1 to p=64 where the other eleven families gain 6.79x-29.86x (median 7.6x). Fanning a spanning
multi-key write into the slotted batch is the write-side counterpart of moon#768
(`MGET`/`EXISTS`, `MultiKeyPlacement::Fanout`), which measured +1503% on the
read side by removing exactly this batch boundary.

**Do not score a cross-shard path with these counters without first checking
that the path increments them.**

## See also

- [`env-knobs.md`](env-knobs.md) — spin governor, THP soak, io_uring dead ends.
- `src/shard/db_plane.rs` — the L4 plane's implementation and its lock rules.
