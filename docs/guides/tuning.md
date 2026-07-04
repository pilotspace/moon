# Tuning Guide

Moon's defaults are chosen for the most common deployment: **one application, a moderate
number of connections, durability on**. Out of the box you get single-shard operation
(best per-operation latency), AOF persistence with `everysec` fsync (measured ≈ zero
throughput cost at the default shard count), and conservative I/O settings that are safe
on shared or virtualized hardware.

This page tells you when to move away from those defaults, per workload. Every
recommendation here is backed by measurements on dedicated-vCPU GCE instances
(ARM `c4a` Axion and x86 `c3` Sapphire Rapids, 2026-07); your numbers will vary but the
*direction* of each knob is portable.

## Quick recipes

| Workload | Recipe |
|---|---|
| Pure cache (no durability) | `--appendonly no --maxmemory <bytes> --maxmemory-policy allkeys-lru` |
| Sessions / rate limiting (few conns, latency-sensitive) | defaults; add `--io-busy-poll-us 40` on dedicated cores |
| High-concurrency API backend (8+ conns) | `--shards 4` (+ busy-poll on dedicated cores) |
| Pipelined / batch ingest | `--shards 4` or more; pipeline depth ≥ 16 |
| Durable primary store | defaults (`--appendonly yes --appendfsync everysec`); `always` only if you accept the write-latency cost |
| Bulk load | `--initial-keyspace-hint <expected keys>` |
| Container / CI / WSL | `--io-driver epoll --memory-arenas-cap 2` |
| Vector search | see [Vector search guide](../vector-search-guide.md); match quantization to dimension |

## Shard count: the most important knob

Moon shards its keyspace across independent per-core threads. A key owned by another
shard costs a cross-thread hop (~10 µs round trip), so **more shards is not automatically
faster** — it depends on how much concurrency and pipelining your traffic has:

| Traffic shape | Best setting | Measured (vs Redis, GCE) |
|---|---|---|
| 1–4 connections, no pipelining | `--shards 1` (default) | 1.2× (ARM) – 1.66× (x86) with busy-poll |
| 8+ connections, no pipelining | `--shards 4` | 1.3–1.5× at 8 conns; 1.67–1.86× at 64 conns |
| Pipelined (depth ≥ 16) | `--shards 4`+ | break-even at depth 16; up to 2.8× at depth 128 |
| 1 connection on many shards | avoid | a single unpipelined conn pays the hop on ~every op (0.85–0.99×) |

Rules of thumb:

- Start with the default `--shards 1`. It wins whenever concurrency is low and is the
  fair configuration for memory comparisons.
- Move to `--shards 4` when you serve **8 or more concurrent connections** or any
  pipelined traffic. Don't exceed the number of physical cores; very high shard counts
  hurt shallow workloads (dispatch overhead dominates).
- `--shards 0` auto-detects the CPU count — use it only on hosts dedicated to Moon
  with genuinely concurrent traffic.
- **Co-locate multi-key operations with hash tags**: `user:{1234}:name` and
  `user:{1234}:session` land on the same shard, so MGET/MSET/transactions on them never
  pay a hop.

## Busy-polling: single-op latency on dedicated cores

`--io-busy-poll-us 40` makes each shard thread poll for new I/O for up to 40 µs before
sleeping, deleting the wakeup latency that otherwise dominates shallow request/response
traffic. This is the flag that takes single-connection GET/SET from below Redis parity
to **1.2× (ARM) / 1.66× (x86)**, and it compounds with multi-shard concurrency (8-conn
throughput +25% on top of the shard win).

The trade-offs are explicit:

- Costs up to the budget in CPU per idle park (~4%/core at 40 µs) — you are trading
  idle CPU for latency.
- **Only enable it on pinned, dedicated cores** (bare metal, dedicated-vCPU cloud
  instances). On shared/oversubscribed hosts (laptops, burstable VMs, busy Kubernetes
  nodes) it *regresses* performance — the spin fights neighbors for the core.
- Values 20–100 µs behave similarly; 40 is a good default. `0` (default) disables it.

## Persistence: what durability costs

Measured on GCE with the default single shard:

- **`everysec` (default): free.** Throughput is indistinguishable from `--appendonly no`
  in steady state. There is no reason to turn AOF off for speed alone.
- **`always`: −9% at depth 1** on unpipelined SET (still 1.27× Redis with busy-poll), but
  under *pipelined* writes each batch waits for its fsync barrier and tail latencies grow
  to the `--aof-fsync-timeout-ms` bound (2 s default) plus queue time. Use `always` only
  for genuinely fsync-per-write requirements, and avoid deep pipelines on that path.
- **`--appendonly no`** for pure caches: saves the disk I/O entirely and removes recovery
  time. Pair with `--maxmemory` + `--maxmemory-policy allkeys-lru` (or `allkeys-lfu`).
- **Multi-shard + AOF note:** at `--shards ≥ 2` Moon currently writes both the AOF and
  the per-shard WAL (~2.7× the disk *volume* of the data ingested; throughput is
  unaffected — the tax is disk bandwidth/wear). If you run multi-shard as a cache, turn
  `--appendonly no`; if you need durability, budget the disk accordingly.

## Memory

- `--maxmemory` is a whole-instance budget; shards share it elastically (a hot shard can
  borrow headroom from cold ones automatically — no per-shard tuning needed, even under
  heavily skewed key distributions).
- For **bulk loads**, `--initial-keyspace-hint 1000000` (or your expected key count)
  pre-sizes the tables and avoids rehash pauses mid-load.
- In **small containers**, cap allocator arenas: `--memory-arenas-cap 2` (default 8).
  Also size `--vec-warm-mmap-budget` down if you use vector search under a cgroup limit.
- Comparing per-key memory against Redis? Use `--shards 1` and a fresh server; RSS is a
  high-water mark, so measure by loading a known keyspace, not by deltas.

## Platform notes

- **Linux** is the production target. The default `--io-driver auto` picks io_uring;
  on some platforms plain epoll measures 2–4% *faster* for key-value traffic (we saw
  this on GCE ARM Axion) — if you're chasing the last few percent, A/B `--io-driver
  epoll` on your own hardware. In containers/WSL/older kernels where io_uring is
  unavailable or blocked by seccomp, set `--io-driver epoll` (or `MOON_NO_URING=1`).
- **macOS** runs the full feature set via kqueue but is a development platform — don't
  benchmark on it.
- **Pinning**: for latency-critical deployments, pin Moon's shard threads and your
  client/proxy to disjoint cores (`taskset`/cpuset). Every latency number above assumes
  no core sharing between client and server.

## Client-side checklist

- More than ~1,000 connections needs `ulimit -n 65536` (5,000 clients with pipelining
  will drop connections without it).
- Connection pools: with `--shards 1`, a handful of pooled connections is enough to
  saturate the server; with `--shards 4`, size the pool at 8+ so all shards stay busy.
- Pipelining is Moon's strongest regime — batch what you can. The advantage over Redis
  *grows* with depth (per-shard AOF removes Redis's single-file serialization).
- Leave `--tcp-keepalive 300` (default) on; set `--timeout` only if you have
  leak-prone clients.

## Observability

`--admin-port 9100` enables `/metrics` (Prometheus), `/healthz`, `/readyz`, and the web
console at `/ui/`. It is off by default; enabling it adds a small per-batch accounting
cost on cross-shard traffic — negligible for most deployments, but leave it off on
single-purpose benchmark rigs.

## Vector search (FT.*)

- `EF_RUNTIME` (per index) trades recall for QPS at query time.
- Set `COMPACT_THRESHOLD` at or above your expected dataset size if you want a single
  final compaction; explicit `FT.COMPACT` on a small mutable segment is a no-op below
  the threshold.
- Match quantization to dimension: **SQ8** (or full-precision HNSW) for ≤ 384-d
  embeddings; **TQ4** shines at 768-d and above. Validate recall with real embeddings,
  not random vectors.
- Details: [Vector search guide](../vector-search-guide.md).
