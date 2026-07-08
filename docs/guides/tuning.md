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
| Sessions / rate limiting (few conns, latency-sensitive) | defaults; add `--io-busy-poll-us 40` on dedicated cores, or `--profile standalone` (see [Profiles](#profiles) — **pinned cores only**) |
| High-concurrency API backend (8+ conns) | `--shards 4` (+ busy-poll on dedicated cores) |
| Pipelined / batch ingest | `--shards 4` or more; pipeline depth ≥ 16 |
| Durable primary store | defaults (`--appendonly yes --appendfsync everysec`, ~1.32× Redis at depth); `always` for RPO 0 — disk-fsync-bound, pipelines fine |
| Pub/sub fan-out (many subscribers) | defaults; delivery coalesces automatically — no tuning |
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

## Profiles

`--profile <name>` bundles a set of proven flags for a given deployment shape into one
switch, instead of you having to remember and re-type the individual recipe every time.

**Precedence rule:** a profile only fills flags you left at their default. Any flag you
pass explicitly — on the CLI or in `moon.conf` — always wins over the profile's value.
Startup logs exactly which flags the profile set, so `--profile` is never a silent
behavior change:

```text
INFO --profile standalone: set --shards=1, --io-busy-poll-us=40, --io-driver=epoll
     (implied by io-busy-poll-us) (unset flags only; pass a flag explicitly on the
     CLI to override the preset)
```

### `standalone`

For a single dedicated Moon instance answering low-pipeline request/response traffic —
the "beat Redis at p=1" shape from the [Busy-polling](#busy-polling-single-op-latency-on-dedicated-cores)
and [shard count](#shard-count-the-most-important-knob) sections above, as one flag:

```
moon --profile standalone
```

Expands to (only for flags left unset):

| Flag | Value | Why |
|---|---|---|
| `--shards` | `1` | best per-op latency for low-concurrency, non-pipelined traffic |
| `--io-busy-poll-us` | `40` | deletes scheduler sleep/wake latency from the request path |
| `--io-driver` | `epoll` | implied by busy-poll (legacy driver only; io_uring CQEs aren't observable this way) |

> **⚠ Pinned/dedicated cores required.** `--io-busy-poll-us` busy-loops the shard thread
> for up to the budget before parking. On a host with genuinely idle, pinned cores this
> deletes wakeup latency and is the single biggest lever behind Moon's p=1 win over Redis
> (measured 1.19–1.21× ARM, 1.65–1.66× x86 on GCE, 2026-07). On **shared or oversubscribed
> cores** — OrbStack's default VM, laptops, burstable/noisy-neighbor cloud instances,
> busy Kubernetes nodes — the same spin **regresses** throughput: it fights every other
> tenant for the core instead of yielding it. **Do not** reach for `--profile standalone`
> on such hosts; run without it (or with `--io-busy-poll-us 0`, the plain default) and
> rely on shard count alone.
>
> There is currently no automatic pinned-core detection — this is an operator judgment
> call, not something Moon can safely default on for you.

An unrecognized profile name is a startup error (exit code 2), not a silent no-op:

```
$ moon --profile bogus
moon: unknown --profile 'bogus' (supported: standalone)
```

## Persistence: what durability costs

Moon's AOF write path is engineered so that durability is cheap at pipeline depth and
device-bound (not server-bound) when you demand fsync-per-write. The mechanics below are
**automatic — there are no knobs to turn** — but knowing them tells you which policy fits
your workload. Measured on GCE `c3-standard-8` (pd-ssd), `--shards 2`, vs Redis 7.0.15;
full matrix in [`BENCHMARK.md` §7.3](../../BENCHMARK.md).

- **`everysec` (default): a win, not just free.** At pipeline depth SET runs **~1.32×
  Redis** (the writer coalesces each batch into one `write_all` and polls its queue
  park-free, so it never bottlenecks on write syscalls or futex wakes); non-pipelined SET
  is at **parity**. There is no reason to turn AOF off for speed. RPO ≤ 1 s.
- **`always` (RPO = 0): now safe to pipeline.** Every write reaches disk before its `+OK`.
  - *Non-pipelined* throughput is **bounded by your disk's fsync rate**, not by Moon: one
    `fdatasync` per write. On network-attached cloud disks (pd-ssd) that is a few thousand
    writes/sec; on local NVMe it is tens of thousands. Redis hits the same wall, so this is
    **parity with any correct engine** — the disk sets the ceiling.
  - *Pipelined* writes used to collapse (each command awaited its own fsync). Moon now
    **group-commits**: a whole pipeline batch is made durable by one fsync barrier, so P16
    SET recovered from 0.12× to **~0.91× Redis**. Deep pipelines on `always` are fine now —
    the per-batch barrier amortizes the fsync. Under sustained overload a batch can still
    wait up to `--aof-fsync-timeout-ms` (2 s default) for its barrier; raise it only if you
    prefer a longer stall over an error under disk saturation.
- **`--appendonly no`** for pure caches: saves the disk I/O entirely and removes recovery
  time. Pair with `--maxmemory` + `--maxmemory-policy allkeys-lru` (or `allkeys-lfu`).
- **Multi-shard + AOF note:** at `--shards ≥ 2` Moon currently writes both the AOF and
  the per-shard WAL (~2.7× the disk *volume* of the data ingested; throughput is
  unaffected — the tax is disk bandwidth/wear). If you run multi-shard as a cache, turn
  `--appendonly no`; if you need durability, budget the disk accordingly.

**Which policy?** Use `everysec` unless a compliance/financial requirement demands zero
data loss on a host crash; then use `always` and size expectations to your disk's fsync
rate (or provision faster storage). Both preserve their guarantee under SIGKILL —
validated by the crash-recovery matrix (100% of acked writes recovered).

## Pub/sub fan-out

Moon's subscriber delivery path **coalesces automatically** — when a publish burst queues
faster than a subscriber's socket drains, the whole burst is delivered in one `write_all`
instead of one syscall per message. In practice this means a fast publisher fanning out to
many subscribers is no longer syscall-bound: measured fan-out delivery reached **5.09M
msg/s (≈1.04× Redis) with zero drops**, versus near-total message loss on a per-message
write path (see [`BENCHMARK.md` §7.3](../../BENCHMARK.md)). There are no knobs to turn.

The one thing to know: each subscriber has a bounded in-flight queue (256 messages). A
subscriber that stays slower than the publish rate will still have messages **dropped** —
this is the intentional slow-subscriber policy (a slow consumer must not stall the
publisher or grow memory unbounded). If you see drops, the fix is on the consumer side
(read faster, or fan out through more subscribers), not a server setting. Pub/sub messages
are fire-and-forget and never persisted, regardless of `--appendonly`.

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

## Tiered memory offload (KV + vector, `--disk-offload`)

`--disk-offload` (default `enable`) lets cold data leave RAM instead of staying
resident forever:

- **KV**: cold values spill to `KvLeafPage` DataFiles under `--disk-offload-dir`
  (default: same as `--dir`). `--disk-offload-threshold` (default `0.85`) is
  the RAM-pressure trigger — once a shard's published KV memory crosses
  `threshold × per-shard budget`, the eviction tick runs an ordered cascade
  *before* falling back to plain LRU/LFU eviction: PageCache clock-sweep
  eviction → force-demote the oldest HOT vector segments to WARM → proactive
  spill via the background `SpillThread` → a `noeviction` warning if none of
  that relieved pressure. This makes offload proactive instead of edge-
  triggered — you don't have to hit `maxmemory` exactly to start shedding.
- **Cold reads** (`GET` on a spilled key) go through `PageCache` when a page
  is already cached (repeated cold reads that land on the same 4KB
  `KvLeafPage` — several keys packed into one page, or a key churned
  cold→hot→cold — are served without a second `pread`). A promoted key is
  moved back into the hot DashTable on its first cold hit, same as before.
- **Vector segments**: immutable (HOT) HNSW+TurboQuant segments transition to
  a mmap-backed WARM tier — see the next section.
- `--pagecache-size` (default: 25% of `--maxmemory`) sizes the buffer pool
  backing both the KV cold-read cache and vector/graph page I/O; it starts
  empty and grows lazily, so setting it high does not pre-commit RAM.

## Vector/FTS/graph idle-unload

Immutable vector segments (`ImmutableSegment`: full in-memory HNSW graph +
TurboQuant codes + f16 exact-rerank sidecar) don't have to stay resident
forever once a workload goes cold. Two independent, differently-behaved
tiers can receive a HOT segment — **COLD wins if both would fire at once**:

- `--engine-offload-idle-secs <secs>` (default `3600`, `0` disables this
  criterion) — seconds since the segment last served a search. Idle segments
  go straight to a **COLD** stub: the HNSW graph, TQ/SQ8 codes, and f16
  sidecar are dropped from memory entirely, keeping only the segment
  directory path, doc count, and enough metadata to reload. This is the one
  to lower if you want genuinely cold segments to actually free RAM.
- `--segment-warm-after <secs>` (default `3600`) — age since the segment was
  compacted, regardless of query traffic. Segments that age out *without*
  also being idle go to the **WARM** tier (`WarmSearchSegment`) instead —
  see the caveat below.

Set `--engine-offload-idle-secs` below `--segment-warm-after` if you want
idleness to be the effective, memory-freeing trigger for most segments (the
common case); a segment that's old-but-still-busy will hit `--segment-
warm-after` first and land on WARM, which does not free memory (below).

**COLD tier (idle-triggered) — real memory savings.** On the next query that
touches a COLD segment, it is synchronously reloaded via the same on-disk
`.mpf`-file path used at server boot (`WarmSearchSegment::from_files`), so
recall is exactly preserved — the exact-rerank sidecar is never silently
dropped, only paged back in. The reload is single-flight per segment (a
`parking_lot::Mutex` guards the promote-and-reload sequence), so concurrent
queries hitting the same COLD segment block behind one reload rather than
each re-reading the segment from disk.
Measured on a real server (40,000 × 768-dim vectors, SQ8, single shard):

| Phase | RSS |
|---|---|
| Before unload (HOT) | 400,544 KB |
| After unload (COLD) | 295,760 KB (**−26.2%**) |
| After reload (touched, back to WARM) | 395,376 KB |

> **⚠ First-touch reload latency is a SHARD-WIDE stall, not just a
> per-query one.** All three call sites that reload a COLD segment
> (`SegmentHolder::search_filtered`, `SegmentHolder::search_mvcc`, and the
> FT.SEARCH yielding/worker-pool path's snapshot capture in
> `command/vector_search/ft_search/dispatch.rs`) run their promote-and-reload
> step INSIDE `crate::shard::slice::with_shard(...)`, i.e. on the shard's own
> single OS thread, before any `.await` boundary — confirmed by tracing every
> call site, not assumed. Under monoio's thread-per-core model this means
> the reload blocks every connection sharing that shard, not only the one
> that triggered it, for the reload's duration (`--shards 1` makes this
> "every connection on the server"). PR #179's off-loop worker pool
> (`crate::vector::search_pool`) only carries the actual HNSW beam search off
> the event loop after capture — the capture phase, including a COLD reload,
> is deliberately synchronous by design (`SearchSnapshot` capture must run
> under one `&mut VectorIndex` borrow). Moving the reload itself off-thread
> would require `SegmentHolder` to be reachable from the async continuation
> without holding open a `VectorIndex` borrow (e.g. wrapping it in an `Arc`,
> a ~100-call-site change) — out of scope for this pass; tracked as a
> follow-up. In practice this only matters the FIRST query after a segment
> goes idle: measured on the 40K×768d fixture above (same-instance
> measurement, `--shards 1`), the first-touch `FT.SEARCH` round-trip that
> triggered the reload took **79.59 ms**, during which a concurrent `PING`
> hammering a second connection to the same shard peaked at **76.70 ms**
> (vs a sub-millisecond baseline) — confirming the stall is real, shard-wide,
> and essentially the full duration of the reload itself, but bounded to a
> single segment-reload's worth of wall time, once per idle segment touched.

**WARM tier (pure-age-triggered) — does not reduce RSS by itself.**
`WarmSearchSegment::from_files` opens each `.mpf` file's mmap only for the
duration of loading and immediately copies every payload into owned
`Vec<u8>` / a parsed `HnswGraph` of essentially the same size as the HOT
segment it replaces — the two structures that dominate memory at scale (TQ
codes + the HNSW graph) are fully duplicated in heap memory, not lazily
paged in from disk. The idle/age *triggers* are both correctly wired
(`FT.INFO` counters, recall, and `num_docs` are verified correct across
both transitions), and the **subsequent** `--vec-warm-mmap-budget` LRU
eviction *does* free real memory (it drops the `Arc` outright, same as COLD
— but without a promote-on-touch reload path, see the known bug below). But
"demote to WARM" alone is not a memory-saving operation — it exists so an
old-but-still-queried segment doesn't pay the COLD reload latency on every
touch. A true zero-copy WARM tier (HNSW traversal + TQ-ADC distance kernels
operating directly on borrowed mmap'd bytes instead of owned buffers) is an
open, larger follow-up; until then, prefer tuning
`--engine-offload-idle-secs` (COLD) over `--segment-warm-after` (WARM) when
the goal is lower RSS.

> **⚠ Known pre-existing bug, not introduced by the COLD tier:**
> `MmapBudget::enforce_budget`'s WARM-tier LRU eviction drops the `Arc<WarmSearchSegment>`
> outright with no reload-on-touch mechanism, despite its own doc comment
> claiming one exists — once a WARM segment is evicted by the mmap budget it
> stops being searched until restart. This is a correctness gap in the
> pre-existing WARM path, not the new COLD path (COLD always reloads on
> touch). Tracked for a follow-up fix.

`FT.INFO <index>` reports tier residency (summed across shards):

- `graph_segments` / `segments_with_exact_rerank` — HOT segments, and how
  many still carry the sidecar (should equal `graph_segments`; less means
  something dropped a sidecar somewhere upstream — see the vector search
  guide's HQ-1 notes).
- `warm_segments` / `warm_segments_with_exact_rerank` — same pair for the
  WARM tier. A gap here after a fresh idle-unload is a regression: file an
  issue, it means the exact-rerank sidecar failed to transfer.
- `unloaded_segments` / `unloaded_segments_with_exact_rerank` — same pair
  for the COLD tier. `unloaded_segments_with_exact_rerank` reflects whether
  the *stub* remembers it had a sidecar before unload (used to detect drift
  after reload), not whether the sidecar is currently resident (it isn't —
  that's the point of COLD).

Both tiers correctly participate in `FLUSHALL`/`FLUSHDB`/`FT.DROPINDEX`
(their on-disk directories are tombstoned/removed like any other segment),
survive server restart cleanly (COLD/WARM segments are just on-disk data —
they're rediscovered fresh as HOT by the existing boot-recovery scan, no
special-cased restoration needed), and are skipped by GraphUnion background
merge scheduling (`needs_merge`/`begin_background_merge` only ever consider
`immutable`, never `warm`/`unloaded`, so a COLD segment can't be corrupted
by a concurrent merge attempt).

**Known limitation shared by both tiers (pre-existing, not introduced by
this work):** per-key tombstoning (`DEL`/`HDEL` on an indexed vector field)
does not currently walk WARM or COLD segments — a delete against a key that
lives only in a WARM/COLD segment does not take effect until that segment
is later merged or dropped. This is an existing gap in the tombstone path,
not something the COLD tier introduces or worsens.

FTS (`TextStore`) and the graph engine do not yet have an equivalent
idle-unload path — FTS has no aggregate memory-accounting API yet, and while
the graph engine's on-disk segment (`MmapCsrSegment`) is already mmap-backed,
it has no idle/LRU-eviction-driven unload comparable to vector's
`MmapBudget`. Both are natural follow-ups but need their own design pass.

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

- `EF_RUNTIME` (per index) trades recall for QPS at query time — and is now
  **runtime-tunable**: `FT.CONFIG SET <idx> EF_RUNTIME <n>` (10–4096, `0` = auto)
  applies to the next `FT.SEARCH` immediately, persists across restarts, and needs no
  index rebuild. Use it to walk the recall/QPS curve on a live index (e.g. drop ef
  during traffic spikes, raise it for offline evaluation).
- Set `COMPACT_THRESHOLD` at or above your expected dataset size if you want a single
  final compaction; explicit `FT.COMPACT` on a small mutable segment is a no-op below
  the threshold.
- Match quantization to dimension **and metric**: **SQ8** (or full-precision HNSW) for
  ≤ 384-d embeddings; **TQ4** shines at 768-d and above and is strongest on the
  unit-sphere metrics (COSINE / IP). TQ on raw L2 uses a norm-corrected distance
  estimator (an earlier norm-scaled ranking collapsed on unnormalized data, which is
  why L2 indexes default to SQ8); SQ8 remains the recommended choice for raw-L2
  workloads. Validate recall with real embeddings, not random vectors.
- **Query cost scales with segment count**: each FT.SEARCH runs the full ef beam on
  *every* graph segment on *every* shard (cost ≈ shards × segments × ef). An index
  that accumulated 50+ segments during a bulk load answers the same query 4–5× slower
  than the same index merged to 1 segment per shard. See the settle recipe below.
- Details: [Vector search guide](../vector-search-guide.md).

## Vector bulk load and compaction

Newly-inserted vectors are **searchable immediately** against the brute-force mutable
tier — exact results, but an O(N) scan per query. The HNSW graph that makes search
O(log N) is built by *compaction*, and Moon now builds it **concurrently with ingest**
across cores, so a bulk load reaches HNSW-tier serving shortly after the last insert
rather than on a later `FT.COMPACT`. On an 8-vCPU dedicated GCE instance, 50K × 384-d
vectors reach HNSW-quality serving in ≈ 9–10 s end to end.

You usually don't need to touch anything — the defaults do the right thing. Reach for a
knob only in these cases:

- **`COMPACT_THRESHOLD`** (per index, at `FT.CREATE`) sets when a mutable segment freezes
  into an immutable HNSW segment. It's the main time-to-serve vs recall lever:
  - *Streaming / continuous ingest* — leave it at the default. Segments compact in the
    background as thresholds are crossed; queries stay fast throughout.
  - *One-shot bulk load where recall matters most* — set it **at or above your dataset
    size** and call `FT.COMPACT` once at the end. You get a single optimal segment (best
    recall, no multi-segment beam split) at the cost of a later first-fast-query.
  - Lower thresholds build more, smaller segments sooner (faster time-to-serve, ~0.001
    lower recall@10 from multi-segment search); higher thresholds do the opposite.
- **`MOON_VEC_COMPACT_WORKERS`** (env) sizes the background compaction thread pool.
  Default is half the machine's cores, clamped to `[1, 8]`. Raise it on write-heavy
  fleets that compact many indexes or shards at once; set `1` for strict shard-thread
  isolation on latency-critical nodes. Segments of ~10K+ vectors additionally build with
  a multi-core parallel HNSW builder; smaller segments use the single-threaded builder.
  Both are automatic and correct on core-pinned deployments — no tuning required.
- **Trade-off to expect:** overlapping the HNSW build with ingest shares cores, so peak
  *ingest* throughput drops while a build runs, and multi-segment serving costs about
  0.001 recall@10 versus a single fully-compacted segment. That buys a dramatically
  faster time-to-first-fast-query. If you care about raw ingest rate and will query
  later, prefer the high-`COMPACT_THRESHOLD` + single final `FT.COMPACT` recipe above.
- **Multi-shard:** each shard compacts its own segments independently and the trigger
  fires per shard, so bulk loads parallelize across shards automatically. Co-locate
  related vectors with hash tags only if you also do multi-key KV ops on them; vector
  search itself scatter-gathers across shards regardless.
- **`--max-unflushed-immutable-segments 0` during million-scale bulk loads.** The
  write-stall guard (default 20) counts **total** immutable segments, not just
  unflushed ones. A 1M+ load accumulates segments faster than background merges retire
  them, so the guard trips permanently and every write returns `MOONERR busy` —
  measured 24× ingest slowdown (4,500 → 190 vec/s) on otherwise-idle hardware. Disable
  it for the load, restore the default for steady-state serving (it exists to bound
  memory under pathological churn). Loaders must still retry on `MOONERR busy` — it is
  backpressure, not an error.
- **Settle before latency-sensitive serving.** After a bulk load, merge each shard's
  segments to one: `VACUUM VECTOR <idx>` force-merges all immutable segments
  (recall-gated GraphUnion). Two caveats: (1) over the wire it acts on the
  **connection's local shard only** — with SO_REUSEPORT per-shard listeners, issue it
  over ~8× more fresh connections than shards and repeat until `FT.INFO
  graph_segments` stops shrinking; (2) merging is currently slow at scale (~1 h for
  1.18M × 200-d on 8 ARM vCPUs, largely single-threaded). The payoff on that dataset:
  same index, same ef, **4–5× higher QPS** (e.g. 888 vs 336 qps at recall 0.83). The
  index serves correct results throughout — settle when you can, not before you must.
- **Merged indexes need higher ef for the same recall ceiling.** Multi-segment search
  unions independent per-segment beams, so an unmerged index over-scans and reaches
  higher recall at a given ef (0.9865 vs 0.933 at ef=256 on glove-1.18M). After
  settling, raise `FT.CONFIG SET <idx> EF_RUNTIME` (e.g. 512) to reclaim the >0.95
  band — still several× faster than the unmerged equivalent.
