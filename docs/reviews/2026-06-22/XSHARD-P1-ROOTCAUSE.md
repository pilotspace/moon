# Why `s=12, p=1` collapses vs Redis — root cause + controlled A/B (2026-06-22)

**Question:** the wider rebench (`WIDER-BENCH.md`) showed Moon at **shards=12, pipeline=1**
dropping to **~0.46× Redis** on production scenarios — the worst cell in the whole matrix.
Why, specifically, and what makes it worse than `shards=1`?

**Answer (one line):** the collapse is an **interaction of three factors** — cross-shard
dispatch tax × no pipeline amortization × random keys that *permanently defeat* the
connection-affinity self-heal. None of the three alone produces it; all three together do.

---

## The decisive evidence: a discrepancy already in our own data

Two reports disagreed on the *identical* workload `GET p=1 s12`:

| source | result | key generator |
|---|:--:|---|
| `bench-compare.sh` (WIDER §1) | **0.82×** | `redis-benchmark … -q` — **no `-r`** → one fixed `__rand_key__` |
| `bench-production.sh` (WIDER §4) | **0.46×** | `… -r <N>` (lines 141/236/285/477/521) → keys spread over the keyspace |

Same shard count, same pipeline depth — only the **key distribution** differs. That single
variable moves the number from 0.82× to 0.46×, which points squarely at *cross-shard key
scatter* (and the self-heal it defeats), not shard count per se.

---

## Controlled A/B (single VM, co-located) — isolates the interaction

`docs/reviews/2026-06-22/xshard-p1-ab.sh` → `xshard-p1-ab.log`. OrbStack `moon-dev`
(aarch64, Ubuntu 24.04), Moon built **Linux ELF** `target-linux/release` `target-cpu=native`
commit `0f592b4`, Redis 7.x. Fixed: `p=1, c=50, n=200000`. Varied: `shards ∈ {1,12}` ×
`keymode ∈ {single, random(-r 100000)}`. Moon/Redis throughput ratio:

| | single key (affinity **can** self-heal) | random keys (affinity **cannot**) |
|---|:--:|:--:|
| **shards=1** (cross-shard impossible) | GET 0.80× · SET 1.00× | GET 0.95× · SET 1.15× |
| **shards=12** | GET 0.74× · SET 0.73× | **GET 0.47× · SET 0.41×** |

**The collapse is confined to the `s12 × random` cell.** Reading the factorial:

- **Random keys alone don't hurt** — `s1 random` is GET 0.95× / SET 1.15× (at/above parity):
  at one shard no key can route off-thread, so scatter is free.
- **12 shards alone barely hurt** — `s12 single` is GET 0.74× / SET 0.73×: the one hot key
  has a dominant owner shard, so the connection **migrates local** and recovers most of the gap.
- **Both together collapse** — `s12 random` is GET 0.47× / SET 0.41×, reproducing the
  production report's 0.46× within VM variance.

This is a textbook interaction effect: the penalty is not (shards) + (scatter); it only
exists in their product.

---

## The mechanism, grounded in code

### 1. Cross-shard dispatch = 2 cross-thread wakes + 2 core reschedules per op
When a key isn't owned by the connection's shard, the inline fast path bails
(`src/server/conn/blocking.rs:1251`) and the op takes the full hop
(`src/server/conn/handler_monoio/mod.rs:1639–1856`):
1. push `PipelineBatch` to the target's SPSC ring + `spsc_notifiers[target].notify_one()` —
   **wake #1** (eventfd/kqueue → wakes the target core);
2. target drains, executes, sends the reply oneshot — **wake #2** (wakes the originating core);
3. plus SPSC-ring + oneshot cache-line bouncing between two cores.

Redis does the whole op in one thread (parse → hashtable → reply): **zero cross-core
coordination.** `0.41–0.47× ≈ 1/2.2`, consistent with paying ~2 cross-core round-trips per op.

### 2. `p=1` removes the only thing that amortizes the hop
The multi-shard design pays for the hop by **batching**: one push+notify+wake carries *N*
commands. At `p=1`, N=1 — the fixed hop cost is paid in full *per op*, and the closed request
loop makes throughput latency-bound. At `p≥16` the same hop carries 16+ commands → cost/op ÷16
→ Moon flips to winning (GET p=128 ≈ 2.8× in WIDER §1). Break-even is p=16.

### 3. Random keys permanently defeat the affinity self-heal
`AffinityTracker` (`src/server/conn/affinity.rs`) migrates a connection onto the shard it
mostly talks to — but only when **one shard owns ≥10/16 (62.5%) of sampled accesses**
(`affinity.rs:16-20,97`):
- **single key** → one shard gets 16/16 → migrate after 16 cmds → **local** → inline fast path;
- **random keys over 12 shards** → ~1.3 samples/shard, `P(any shard ≥10) ≈ 10⁻⁸` → `record()`
  returns `None` **forever** → the connection is stuck cross-shard for life (91.7% of ops hop).

The unit test `record_returns_none_when_threshold_not_met` ("spread evenly… none ≥ 10/16")
encodes exactly this defeat.

### 4. The idle reply-spin can't rescue it under load
The C2 spin that skips wake #2 (`src/shard/slice.rs:255-275`, `xshard_should_spin`) engages
only when the shard has **≤2 in-flight waiters (near-idle)** *and* the batch is a singleton.
A `c=50` bench keeps every shard busy → gate closed → every reply parks → full cross-thread
wake. (Even when it fires it removes only wake #2, never wake #1.)

---

## Takeaways / levers (all already in the codebase)
- **`--shards 1`** for uniform non-pipelined workloads — `s1 random` is GET 0.95× / SET 1.15×
  vs `s12 random` 0.47× / 0.41×.
- **Hash-tags** `user:{1234}:*` to co-locate a client's keys → one dominant shard → affinity
  migration *does* fire even with `-r`, recovering toward the `s12 single` row.
- **Pipeline ≥16** if the workload allows — that's where extra shards become a net win.
- The penalty is **not** a structure-specific cliff (WIDER §2: every structure ≈0.79× at p=1)
  and **not** the AOF-fairness bug (`322d58b`, AOF off on both sides here). It is purely the
  cross-shard dispatch model meeting a scatter+no-batch workload.

## Provenance
- Bench was first run against the stale `target/release/moon`, which is a **macOS Mach-O**
  (`cf fa ed fe`); OrbStack host-proxied it so it bound the *host* loopback and the VM bench
  got nothing (the documented Mach-O trap). Rebuilt to `target-linux/release` (ELF `7f 45 4c 46`,
  verified bound in-VM via `ss`) before the numbers above. Servers torn down; 0 leaked procs.
