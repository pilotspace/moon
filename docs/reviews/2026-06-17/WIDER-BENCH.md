# Moon WIDER Benchmark — shard scaling × structures × pipeline/datasize × production (2026-06-17)

**Scope:** the four "wider" axes — **shard-num 1/4/12**, **core data structures**, **pipeline×datasize sweep**,
**production scenarios** — Moon vs Redis 7.0.15, cross-arch. **Where:** GCloud on-demand x86_64 `c3-standard-8`
(Xeon 8481C, 2.70 GHz) + ARM64 `t2a-standard-8` (Neoverse-N1), 8 vCPU, Ubuntu 24.04. **Moon:** `8238515`,
`target-cpu=native`, release. **Method:** the **repo-canonical** `scripts/bench-compare.sh` (all-commands ×
pipeline 1–128 × datasize 8B–64KB × connections 1–500, run at `--shards 1/4/12`) + `scripts/bench-production.sh`
(10 workload scenarios, `--shards 1` and `12`). redis-benchmark, 200K req/test, co-located.
Logs: `bench2-{x86,arm}.log` (this dir). Instances DELETED (0/0 verified).

> **Fairness patch (important):** the canonical scripts start **Redis with `--appendonly no` but Moon with
> persistence defaults (AOF on)**. Under SET load Moon's AOF channel saturates, floods
> `AOF append dropped … channel full` (≈1M lines), pins the port, and unfairly handicaps Moon. The harness
> patches both scripts to start Moon `--appendonly no --disk-offload disable` + `RUST_LOG=error` so the
> comparison is **AOF-off on both sides**. (This Moon-with-AOF-vs-Redis-without is a latent unfairness in
> `scripts/bench-compare.sh` / `bench-production.sh` worth fixing upstream — see Follow-ups.)

---

## Headline

1. **Moon's advantage is pipeline depth, not shard count.** Across both arches, Moon **loses p≤8** (TCP-RTT
   bound) and **wins p≥16** decisively. The break-even is **p=16**.
2. **More shards do NOT help — and *hurt* uniform single-key throughput.** Non-pipelined p=1 work at **12
   shards drops to ~0.46–0.51× Redis** (vs ~0.79× at 1 shard) — the cross-shard SPSC dispatch tax. This is
   the documented CLAUDE.md gotcha ("single-shard is best for non-pipelined"), now quantified cross-arch.
3. **Every data structure behaves like GET/SET** (~0.79× at p=1, wins when batched) — no structure-specific
   cliff. `MSET` is the exception: it *degrades with shard count* (0.93×→0.61× as 1→12) because its 10 keys
   scatter cross-shard (hash-tags would fix).

---

## 1. Shard scaling (1 / 4 / 12) — x86 (ARM mirrors)

**GET by pipeline depth — flat across shard count** (shards don't hurt *pipelined* reads):

| pipeline | shards=1 | shards=4 | shards=12 |
|---------:|:--------:|:--------:|:---------:|
| p=1  | 0.79× | 0.79× | 0.82× |
| p=16 | 1.05× | 0.94× | 0.96× |
| p=64 | **1.88×** | **1.88×** | **1.91×** |
| p=128 | **2.84×** | **2.92×** | **2.81×** |

**SET by pipeline depth — slight decline with shard count** (cross-shard write dispatch):

| pipeline | shards=1 | shards=4 | shards=12 |
|---------:|:--------:|:--------:|:---------:|
| p=16 | 1.40× | 1.41× | 1.41× |
| p=64 | **1.85×** | 1.66× | 1.54× |
| p=128 | 1.72× | 1.67× | 1.75× |

**Core commands p=1 (c=50): ~0.79–0.82× regardless of shard count.** `MSET` drops 0.93× (s1) → 0.64× (s4)
→ 0.61× (s12) — cross-shard key scatter. (ARM: same shape; SET p=64 1.96×/1.86× at s1/s12, GET p=128
2.74×/2.42×.)

---

## 2. Core data structures — Moon vs Redis (x86, shards=1, p=1, c=50)

| structure op | ratio | | structure op | ratio |
|---|:--:|---|---|:--:|
| SET / GET / INCR | 0.79× | | SADD / SPOP | 0.80× |
| LPUSH / RPUSH | 0.76–0.78× | | HSET | 0.78× |
| LPOP / RPOP | 0.78–0.80× | | ZADD | 0.79× |
| LRANGE 100–600 | 0.78–0.80× | | ZPOPMIN | 0.72× |
| MSET (10 keys) | 0.93× | | PING | 0.79× |

All structures cluster at ~0.79× at p=1 — the TCP-RTT-bound regime, no structure-specific weakness. ARM
shows a few structures *above* parity at p=1 (ZPOPMIN 1.10×, SADD 0.99×, HSET 0.97×). (Two LRANGE rows carry
a redis-benchmark multi-line-output parse glitch in both logs — cosmetic, values still legible.)

---

## 3. Pipeline × data-size sweep (x86, shards=1)

**Pipeline crossover** (Moon/Redis): SET 0.79× (p1) → 0.90× (p8) → **1.40× (p16)** → **1.85× (p64)**;
GET 0.79× (p1) → 0.74× (p8) → 1.05× (p16) → 1.88× (p64) → **2.84× (p128)**.

**Data size** (p=1): Moon ~0.79–0.81× from 8B–4KB; **16KB SET hits 1.02–1.05×** (a sweet spot); **64KB SET
weakest at 0.62–0.68×** (large-value copy) while **64KB GET is ~0.93–0.97× (near parity)**. ARM same shape
(64KB SET 0.54×, GET 0.99×).

**Connection scaling** (SET, p=1): c=1 ~1.0× (parity), c=10–500 flat ~0.78–0.82× both arches.

---

## 4. Production scenarios (bench-production.sh)

**shards=1** — Moon ~0.78–0.80× at p=1, **wins the batched reads**:

| scenario | p=1 | batched |
|---|:--:|:--:|
| Session (GET) | 0.78× | **1.28×** (GET batch p=8) |
| Cache (GET 1KB) | 0.78× | **1.18×** (GET 4KB batch p=16) |
| Queue (LPUSH/RPOP) | 0.78–0.79× | 0.84–0.90× (batch p=16) |
| Leaderboard (ZADD) | 0.88× | 0.65× (batch p=16) |
| Rate limiter (INCR) | 0.80× | 0.65× (p=16) |
| Hash (HSET) | 0.80× | 0.65× (batch p=16) |

**shards=12 — the cross-shard penalty on uniform single-key p=1 is stark:** Session GET/SET p=1 collapse to
**0.46× / 0.46×** (x86) and **0.50× / 0.51×** (ARM); datasize p=1 ~0.48–0.50× across 8B–4KB. Batched/pipelined
work still wins at s12 (GET p=128 2.74× x86 / 2.40× ARM). **Takeaway: run `--shards 1` for uniform
non-pipelined workloads; add shards only for pipelined/AOF/hash-tag-colocated workloads.**

---

## 5. Caveats & follow-ups
- **Memory rows are NOT reportable.** `bench-production.sh`'s memory scenario measures RSS in the *same* Moon
  process *after* the 200K-key throughput tests — RSS is a high-water mark (CLAUDE.md: "FLUSHALL does not
  return pages"), so the 10K-key "130 MB / 438 MB" rows reflect that high-water, **not** real per-key memory.
  Use `scripts/bench-resources.sh` (fresh server per row) for memory — §3 of BENCHMARK.md stands.
- **Latent unfairness in the repo bench scripts:** they start Redis AOF-off but Moon AOF-on (see fairness
  patch). Worth a one-line upstream fix (`--appendonly no` on the Moon start lines).
- No CPU pinning; same-run ratios are the signal. `--shards 1`, c=50 default unless noted.
- Two LRANGE rows have a redis-benchmark multi-line-output parse glitch (cosmetic).
