# Architectural blueprint for a Redis replacement in Rust

A Rust-based Redis replacement can realistically achieve **10–12× throughput**, **5× lower p99 latency**, and **2.5–3.5× better memory efficiency** versus single-instance Redis by combining a thread-per-core shared-nothing architecture, io_uring-driven networking, cache-optimized hash tables, and forkless persistence. This report provides the complete technical blueprint for building it — grounded in deep analysis of Redis internals, competitive alternatives (Dragonfly, KeyDB, Garnet), and the Rust systems programming ecosystem as of early 2026.

The core insight is straightforward: Redis's single-threaded command execution and fork-based persistence are fundamental architectural constraints, not tuning problems. Dragonfly proved that a shared-nothing redesign on the same hardware achieves **3.8–6.4M ops/sec** versus Redis's ~150–200K. Rust's ownership model, zero-cost abstractions, and maturing io_uring ecosystem make it the ideal language to push even further.

---

## Why Redis bottlenecks where it does

Redis's architecture centers on a deliberately minimal event loop (`ae.c`, ~1,300 lines) abstracting over epoll/kqueue/select. Each iteration of `aeProcessEvents` calls `beforesleep` (which handles pending writes), blocks on `epoll_wait`, processes fired file events, then processes time events via `serverCron` at ~10 Hz. Every command — from a 1 µs `GET` to a 500 ms `SORT` — executes on a **single thread**. This was antirez's intentional choice: Redis is memory-bound, not CPU-bound, and single-threading eliminates locks, race conditions, and context-switch overhead.

The bottleneck calculus has changed. Modern hardware offers 64–128 cores, 25–100 GbE networking, and NVMe storage with sub-4 µs latency. Redis leaves **98% of available compute** on the table. Redis 6.0+ added I/O threading (distributing socket reads/writes across threads while keeping command execution single-threaded), yielding **37–112% improvement** with 8 I/O threads — but this is a half-measure that preserves the fundamental constraint.

Three additional bottlenecks compound the problem. **Fork-based persistence** (`BGSAVE`) copies the process's page table, which blocks the main thread for **60+ ms on a 25 GB dataset** and can spike to seconds above 100 GB, with copy-on-write potentially doubling memory under write-heavy loads. **Memory overhead** averages **~56 bytes per key-value pair** (24-byte `dictEntry` + 8-byte bucket pointer + 16-byte `robj` header + 3–5 byte SDS header), and the dual-table incremental rehashing scheme doubles this during resizing. **Memory fragmentation** from jemalloc's size-class rounding and variable-size deletions routinely pushes `mem_fragmentation_ratio` above 1.5, requiring active defragmentation that consumes 1–25% of CPU.

The command processing pipeline flows: socket `read()` → `querybuf` (SDS) → RESP parsing via state machine → command lookup in dict → `cmd->proc(client)` execution → `addReply*()` to client output buffer → queued for `handleClientsWithPendingWrites` → socket `write()`. Pipelining works naturally since `processInputBuffer` loops over all parsed commands in the query buffer, but this is sequential within the single thread.

Redis's core data structures have their own overhead profiles. The **dict** uses chaining with singly-linked lists, incrementally rehashing between two hash tables (moving one bucket per operation and spending up to 1 ms per `serverCron` tick). Skip lists for sorted sets store **~37 bytes per entry** (element SDS, double score, backward pointer, variable-level forward pointers with spans). **Listpack** (replacing ziplist in Redis 7+) stores entry-then-backlen format to avoid cascade updates, but remains O(N) for random access and limited to small collections (~128 elements).

---

## What the competition proved is possible

**Dragonfly** is the most architecturally significant competitor and the primary design reference. Built in C++ by ex-Google engineers, it uses a **shared-nothing, thread-per-core architecture** where each core thread owns an `EngineShard` containing its own `DbSlice` (PrimeTable via DashTable + ExpireTable). Inter-thread interaction occurs exclusively via message passing — no mutexes or spinlocks touch the data path.

Dragonfly's **DashTable** (based on the "Dash: Scalable Hashing on Persistent Memory" paper) replaces Redis's pointer-heavy dict with a segmented, open-addressed hash table. A directory points to segments, each containing **56 regular buckets + 4 stash buckets**. Entries are stored inline (no pointer chaining), achieving **6–16 bytes overhead per item** versus Redis's 16–32 bytes. When a segment fills, only that segment splits — not the entire table. This yields ~40% memory reduction in certain configurations.

For multi-key operations (MGET, MSET, transactions), Dragonfly implements **VLL (Very Lightweight Locking)** from the VLDB 2012 paper. A connection fiber coordinates sub-operations across shards via message passing, acquiring lightweight intent locks in ascending shard-ID order to prevent deadlocks. Most operations execute "out-of-order" (no conflicts), making VLL overhead negligible.

Dragonfly's benchmarks on a c6gn.16xlarge (64 vCPUs): **3.8M QPS** without pipelining, **10–15M QPS** with pipeline=30, and **~30% less memory** than Redis at idle with **near-zero memory spike during snapshots** (forkless, async snapshotting using DashTable's compartmentalized structure). However, Redis's rebuttal is important: a **40-shard Redis 7.0 Cluster** on the same machine achieved **4.74M ops/sec** (18–40% more than Dragonfly), arguing that the "25× claim" is a strawman comparing single-process Redis to a multi-threaded system.

**KeyDB** took a different approach — forking Redis and adding multi-threading with a **shared-state spinlock** on the hash table. This achieves ~5× single-threaded Redis throughput but hits a scaling ceiling because spinlock contention grows with core count (KeyDB recommends only 4 threads). Its killer feature is **active replication** (multi-master with last-write-wins), eliminating sentinel complexity.

**Garnet** (Microsoft Research, .NET/C#) introduced a "shared-memory network design" where TLS processing and storage operations execute on the **I/O completion thread itself**, avoiding data movement to shard threads. It uses the **Tsavorite** (FASTER fork) log-structured store with tiered storage (memory → SSD → cloud). Garnet shows strong scaling and claims p99.9 latency under 300 µs, but uses a proprietary benchmark tool (`Resp.benchmark`) with 8-byte keys/values and batch sizes of 4096, making comparisons difficult.

---

## The target architecture: thread-per-core shared-nothing

The recommended architecture maps one shard per CPU core, each running an independent async event loop with its own io_uring instance, hash table partition, expiry index, and memory pool. No shared mutable state exists between shards.

```
┌─────────────────────────────────────────────────────────────┐
│                     Listener (Core 0)                       │
│  io_uring multishot accept → hash(client_fd) → assign core  │
└──────────┬──────────────────────────────────────────────────┘
           │ Per-connection assignment
┌──────────▼────────┐  ┌───────────────────┐  ┌──────────────┐
│   Shard 0 (Core 0)│  │  Shard 1 (Core 1) │  │ Shard N (N)  │
│ ┌───────────────┐ │  │ ┌───────────────┐ │  │              │
│ │ io_uring ring │ │  │ │ io_uring ring │ │  │    . . .     │
│ │ (recv/send)   │ │  │ │ (recv/send)   │ │  │              │
│ ├───────────────┤ │  │ ├───────────────┤ │  │              │
│ │ RESP Parser   │ │  │ │ RESP Parser   │ │  │              │
│ │ (memchr SIMD) │ │  │ │ (memchr SIMD) │ │  │              │
│ ├───────────────┤ │  │ ├───────────────┤ │  │              │
│ │ DashTable     │ │  │ │ DashTable     │ │  │              │
│ │ (keyspace     │ │  │ │ (keyspace     │ │  │              │
│ │  partition)   │ │  │ │  partition)   │ │  │              │
│ ├───────────────┤ │  │ ├───────────────┤ │  │              │
│ │ TTL Index     │ │  │ │ TTL Index     │ │  │              │
│ ├───────────────┤ │  │ ├───────────────┤ │  │              │
│ │ Bump Arena    │ │  │ │ Bump Arena    │ │  │              │
│ │ (per-request) │ │  │ │ (per-request) │ │  │              │
│ └───────────────┘ │  │ └───────────────┘ │  │              │
│ SPSC channels ◄──►│  │◄──► SPSC channels │  │              │
└───────────────────┘  └───────────────────┘  └──────────────┘
```

**Keyspace partitioning** uses `xxhash64(key) % num_shards` for non-cluster mode, or `CRC16(key) % 16384` → `slot % num_shards` for Redis Cluster compatibility. Hash tags (`{tag}`) ensure related keys co-locate.

**Connection handling** follows a fiber-per-connection model. Each connection is pinned to the shard thread that accepted it. For single-key commands (the vast majority), the connection fiber either handles the command locally (if the key maps to its own shard) or dispatches a message to the owning shard's SPSC channel and awaits the response. For multi-key operations, the coordinator pattern from Dragonfly/VLL applies: identify all involved shards, dispatch sub-operations in parallel, collect results.

**Runtime choice**: **Glommio** is the strongest fit — it provides Seastar-heritage thread-per-core design with three io_uring rings per thread (main, latency-sensitive, NVMe polling), cooperative scheduling with latency classes, `SharedChannel`/`ConnectedChannel` for cross-shard communication, and DMA file I/O for persistence. It's production-proven at Datadog. **Monoio** is a viable alternative with broader platform support (macOS/kqueue fallback) and GAT-based I/O traits that model io_uring's buffer-ownership semantics in the type system. ByteDance deploys it in production via the Monolake proxy framework.

Benchmarks confirm the approach: Monoio achieves **~2× Tokio throughput at 4 cores** and **~3× at 16 cores**. TechEmpower results show thread-per-core Tokio `current_thread` setups achieve **1.5–2× multi-threaded Tokio** for HTTP workloads, even without io_uring. Glommio's creator (ex-ScyllaDB) notes that a context switch (~5 µs) now exceeds a storage I/O operation (<4 µs with io_uring), fundamentally favoring the thread-per-core model.

---

## Hash table design: merging DashTable with Swiss Table

The primary index should combine Dragonfly's macro-level segmented architecture (for incremental rehashing and memory efficiency) with Rust's `hashbrown` Swiss Table micro-level SIMD optimization (for fast probing).

**Segment structure**: A directory (array of pointers) maps to segments. Each segment contains ~56 regular buckets + 4 stash buckets. Routing: `hash(key)` → segment ID → 2 home buckets within the segment. Overflow goes to stash buckets. When a segment fills, it splits into two — only that segment rehashes, **not the entire table**. This eliminates Redis's dual-table memory spike.

**Within each segment, apply Swiss Table techniques**: Each bucket group has 16 1-byte control bytes storing an H2 hash fingerprint (7 bits) + full/empty/deleted flag. **SIMD comparison** (`_mm_cmpeq_epi8` on x86, scalar 8-byte comparison on ARM) scans 16 control bytes simultaneously against the lookup key's H2. Most lookups resolve in a single 16-byte SIMD scan — **~6 ns per probed group**. Load factor supports up to **87.5%** without degradation.

**Value representation** uses a compact 16-byte struct with small-string optimization:

- **Small values (≤12 bytes)**: 4-byte length field + 12 bytes inline data. No heap allocation.
- **Large values**: 4-byte length + 4-byte inline prefix (for fast comparison) + 8-byte heap pointer.
- **Type tag**: Low 3 bits of the pointer (or a dedicated nibble) encode the value type (string, list, set, hash, zset, stream, integer).
- **TTL embedding**: 4-byte delta from a per-shard base timestamp (seconds precision, ~136-year range). Eliminates the separate 32-byte expiry entry that Redis uses.

This achieves **~16–24 bytes per key-value pair** versus Redis's ~56, a **57–71% reduction**. For sorted sets, a B+ tree (as Dragonfly uses) reduces per-entry overhead from Redis's ~37 bytes to **2–3 bytes** by packing multiple elements per node with sequential memory layout.

---

## Networking layer: io_uring from socket to response

Each shard thread operates one io_uring instance configured for maximum throughput:

- **Multishot accept** (Linux 5.19+): A single SQE continuously produces CQEs for new connections, eliminating per-accept submission overhead.
- **Multishot recv with provided buffer rings** (Linux 6.0+): The kernel selects buffers on-demand from a pre-registered ring when data arrives. This eliminates pre-allocation waste and enables efficient handling of many idle connections.
- **Registered file descriptors** (`IORING_REGISTER_FILES`): Pre-register all client socket FDs to avoid per-I/O `fd` lookup and refcount operations. Reduces CPU overhead by **~30%** in multi-threaded database workloads.
- **Registered buffers** (`IORING_REGISTER_BUFFERS`): Pre-map read/write buffers into kernel space. Eliminates per-I/O `get_user_pages()`/`put_page()` overhead. Benchmarks show **up to 3.5× fewer CPU cycles per transmitted byte** for large messages.
- **SQE batching**: Accumulate all submissions during one event loop iteration, flush with a single `io_uring_submit()`. With Spectre mitigations making each syscall cost ~1–5 µs, batching 32 operations reduces per-op cost to 30–150 ns.

The io_uring advantage over epoll is most pronounced for **request-response workloads** (exactly the KV store pattern): ~10% higher throughput at 1,000 connections in Alibaba Cloud benchmarks, with the gap widening under higher connection counts and Spectre-mitigated kernels. Database workload benchmarks show io_uring achieving **5M+ IOPS** with lower p99 latency under saturation and **30% less CPU utilization**.

**Response path optimization**: For a `GET` response, serialize the RESP header (`$<len>\r\n`) into a small stack-allocated buffer, reference the value data directly from the hash table via an `iovec`, and append the trailing `\r\n` as a static slice. Use `writev` (or `io_uring_prep_writev`) to scatter-gather all three segments in a single operation — **zero-copy from storage to socket**. In a shared-nothing single-threaded model, the value remains pinned during the write because only one thread accesses the shard.

Pre-computed static responses eliminate formatting entirely for common cases: `+OK\r\n`, `+PONG\r\n`, `$-1\r\n`, and integer responses `:0\r\n` through `:999\r\n` stored as `&'static [u8]` slices. The `itoa` crate provides **~3× faster** integer formatting than `std::fmt` for dynamic integer responses.

---

## RESP parsing at SIMD speed

The RESP protocol's `\r\n`-terminated line format is ideal for SIMD acceleration. The `memchr` crate (v2.8) provides **6.5× faster** byte scanning than `std` using SSE2/AVX2 (x86) and NEON (ARM) with runtime feature detection.

The parsing pipeline for pipelined commands:

1. **Receive**: io_uring multishot recv fills a `BytesMut` buffer (from the `bytes` crate).
2. **Scan**: `memchr(b'\r', buf)` locates CRLF terminators at SIMD speed (~32 bytes/cycle on AVX2). Verify `buf[pos+1] == b'\n'`.
3. **Parse**: Extract the type byte (first byte: `*`, `$`, `+`, `-`, `:`), parse length via fast integer parsing from the `atoi` crate, then `buf.split_to(len).freeze()` for **zero-copy payload extraction**. The `Bytes` handle shares the underlying allocation without copying.
4. **Batch**: Parse ALL pipelined commands before executing any. This enables dispatching commands to different shards simultaneously within a single pipeline.
5. **Arena temporaries**: Use a per-connection `bumpalo::Bump` arena for all intermediate parsing structures. Reset after each command batch — O(1) bulk deallocation, excellent cache locality (bump allocation is ~2 ns).

For high-throughput SIMD optimization beyond `memchr`, apply the `simd-json` technique: load 32 bytes into an AVX2 register, compare against `\r` splat, extract a bitmask of match positions, and use `trailing_zeros()` to find the first match. In practice, `memchr` already implements this — only optimize further if profiling identifies RESP parsing as a bottleneck.

---

## Memory management: per-shard allocators and arena patterns

**Global allocator**: `mimalloc` as `#[global_allocator]` delivers **5.3× faster average allocation** than glibc under multi-threaded workloads, with per-thread free lists that naturally complement the thread-per-core architecture. It supports NUMA-aware allocation via `MIMALLOC_USE_NUMA_NODES`. Alternative: `tikv-jemallocator` (v0.6) if heap profiling via `mallctl` or detailed allocation statistics are needed during development.

**Per-request arena**: Each connection maintains a `bumpalo::Bump` allocator for RESP parsing temporaries, command argument storage, and response assembly. Bump allocation costs ~2 ns (pointer increment). After each command batch completes, `bump.reset()` reclaims all memory in O(1). This eliminates thousands of individual `free()` calls per pipeline batch.

**NUMA-aware placement**: Pin each shard thread to a specific core via `sched_setaffinity()`. Bind the shard's memory region to the local NUMA node via `mbind()` with `MPOL_BIND`. Local memory access runs at ~70 ns versus ~112–119 ns for remote access — a **62% penalty** that compounds across millions of lookups. The `numanji` crate provides a NUMA-local allocator with jemalloc fallback.

**Cache-line discipline**: Align all per-shard mutable structures to 64 bytes (`#[repr(C, align(64))]`) to prevent false sharing. False sharing between cores can cause **300× slowdown** at 32 threads. Use `crossbeam_utils::CachePadded<T>` for atomic counters. Separate read-heavy fields (hash table metadata) from write-heavy fields (statistics, expiry timestamps) onto different cache lines. Prefetch the next hash bucket during lookup via `core::arch::x86_64::_mm_prefetch` to hide memory latency.

---

## Persistence without fork: async compartmentalized snapshots

Redis's `fork()` + COW persistence is the single largest source of latency spikes and memory overhead. On a 25 GB dataset, `fork()` blocks the main thread for ~60 ms copying page tables. Under write-heavy loads, COW can **double memory usage**. TLB shootdowns further degrade performance.

The alternative, proven by Dragonfly: **forkless, async snapshotting** leveraging the DashTable's compartmentalized structure. Each segment can be independently serialized to a point-in-time snapshot while the shard thread continues serving requests. The key insight is that the segmented hash table provides natural checkpoint boundaries:

1. Mark a snapshot epoch.
2. Iterate segments. For each segment, serialize its current state before any new modifications touch it.
3. If a key in a not-yet-serialized segment is modified, capture the old value before overwriting (write-ahead log within the segment).
4. Serialization runs as a low-priority cooperative task within the shard's event loop, yielding between segments.

This produces **near-zero memory overhead** during snapshots (versus Redis's 2–3× spike) and eliminates the fork latency entirely. Each shard can snapshot independently, writing parallel per-shard files (similar to Dragonfly's DFS format) or a single interleaved RDB-compatible file.

For AOF-style durability, append serialized commands to a per-shard write-ahead log via io_uring's DMA file write path (Glommio's `DmaStreamWriter`). Use `fsync` batching — accumulate writes across commands and fsync once per millisecond, matching Redis's default `appendfsync everysec` durability guarantee with lower overhead through io_uring's batched submission.

---

## Cluster design and multi-key coordination

Maintain Redis Cluster compatibility using the **16,384 hash slot** scheme with `CRC16(key) % 16384` and gossip protocol on a separate cluster bus port. Within a single node, slots map to local shards: `slot_id % num_local_shards`.

For **MOVED/ASK redirections** during slot migration, each shard maintains its own slot ownership bitmap. The migration coordinator (a dedicated fiber on the migrating shard) iterates the slot's keys, sending them to the target node via MIGRATE, while ASK-redirecting new requests for migrating slots.

The **replication protocol** (PSYNC2-compatible) streams the per-shard write-ahead log to replicas. Each shard maintains its own replication backlog (circular buffer, configurable, default 1 MB per shard). Partial resynchronization works at the shard level — a replica reconnecting only needs to catch up on the shards where it fell behind, not the entire dataset. This is a significant improvement over Redis's single-backlog design.

For **multi-key transactions** spanning shards, the VLL coordinator acquires intent locks on involved shards in ascending shard-ID order (preventing deadlocks), executes sub-operations, and commits atomically. Single-shard transactions (the common case) execute with zero coordination overhead.

---

## How to benchmark fairly against Redis

**Use memtier_benchmark** as the industry-standard tool. It provides multi-threaded load generation, configurable GET:SET ratios, Zipfian/Gaussian key distributions, and HdrHistogram-based percentile reporting. A fair baseline comparison:

```bash
# Pre-populate: 10M keys, 256B values
memtier_benchmark -s $SERVER -t 8 -c 30 --ratio=1:0 \
  --key-pattern=P:P --key-maximum=10000000 -n allkeys -d 256

# Benchmark: read-heavy, Zipfian distribution, 180s
memtier_benchmark -s $SERVER -t 8 -c 30 --ratio=1:10 \
  --key-pattern=G:G --key-maximum=10000000 -d 256 \
  --test-time=180 --distinct-client-seed --run-count=5 \
  --print-percentiles 50,90,95,99,99.9,99.99
```

Critical methodology requirements: run client on a **separate machine** (same AZ, 25+ GbE), use **Zipfian or Gaussian** key distribution (uniform underestimates hot-spot effects — Twitter's analysis shows production workloads have Zipfian α of 0.6–2.2), use **256-byte values** (industry standard; 3-byte defaults are meaningless), test for **≥180 seconds** with warm-up, and run **≥5 iterations** reporting median with min/max range.

**Avoid Dragonfly's "25× faster" framing**: always compare against a properly-deployed **Redis Cluster** using the same core count. Redis's team showed 40-shard Redis 7.0 Cluster achieves 4.74M ops/sec on the same hardware where Dragonfly achieves 3.8M. The fair comparison is same-hardware, same-core-count, operational deployment versus operational deployment.

**Beware coordinated omission**: closed-loop benchmarks (send request, wait, repeat) systematically hide tail latency. When a request takes 500 ms, ~500 requests that should have been sent are omitted. Use memtier's `--rate-limiting` for open-loop testing at various fixed rates (10%, 25%, 50%, 75%, 90% of peak) and plot the full throughput-latency curve.

---

## Quantitative performance targets on 64-core hardware

| Metric | Redis 7 (single) | Redis 7 (40-shard cluster) | Target (Rust, 64 cores) |
|--------|:-:|:-:|:-:|
| Throughput (no pipeline) | ~200K ops/s | ~5M ops/s | **5–7M ops/s** |
| Throughput (pipeline=16) | ~500K ops/s | N/A | **12–15M ops/s** |
| P99 latency | ~1 ms | ~1 ms | **<200 µs** |
| Per-key memory overhead | ~56 bytes | ~56 bytes + cluster 16B | **16–24 bytes** |
| Memory during snapshot | **2–3× spike** | 2–3× spike | **<5% spike** |
| Cores utilized | 1 (+I/O threads) | 40 processes | **64 (single process)** |

The operational advantage beyond raw numbers: a single-process Rust KV store on 64 cores eliminates the **40 separate Redis processes**, 40 separate memory pools, inter-shard proxy overhead, and cluster management complexity that Redis Cluster requires to achieve comparable throughput.

---

## Concrete implementation roadmap

The build should proceed in phases that deliver measurable performance milestones:

**Phase 1 — Single-shard prototype** (4–6 weeks): Single-threaded Glommio/Monoio event loop with io_uring, `memchr`-accelerated RESP2 parser, `hashbrown` HashMap for storage, GET/SET/DEL commands, `bytes::BytesMut` buffer management. Target: match single-instance Redis throughput (~200K ops/sec) to validate the I/O path.

**Phase 2 — Thread-per-core sharding** (4–6 weeks): Expand to N shard threads, implement keyspace partitioning with SPSC channels for cross-shard dispatch, add connection-to-shard assignment, implement multi-key coordination (MGET/MSET via VLL pattern). Target: linear scaling to **2M+ ops/sec** on 16 cores.

**Phase 3 — DashTable and memory optimization** (6–8 weeks): Replace `hashbrown` with a custom segmented hash table (Dash-inspired segments + Swiss Table SIMD probing), implement small-string optimization, tagged pointers, embedded TTL. Target: **2.5× memory reduction** versus Redis per-key overhead.

**Phase 4 — Full data structure support** (8–10 weeks): Implement lists (quicklist-style doubly-linked listpacks), sets (intset + hash table), sorted sets (B+ tree + hash), hashes (listpack + hash table), streams (radix tree + listpack). Target: pass the Redis command compatibility test suite for the top 100 commands.

**Phase 5 — Persistence and replication** (6–8 weeks): Forkless async snapshots, per-shard WAL with batched `fsync`, RDB-format export, PSYNC2-compatible replication protocol. Target: zero-downtime persistence with <5% memory overhead during snapshots.

**Phase 6 — Cluster mode** (6–8 weeks): 16,384 hash slots, gossip protocol, MOVED/ASK redirections, slot migration, cluster-aware client routing. Target: full Redis Cluster API compatibility.

The total estimated timeline of 34–46 weeks for a senior team of 3–4 Rust engineers reflects the scope of building a production-grade system. The phased approach delivers a benchmarkable single-shard prototype within the first month, enabling continuous performance validation against Redis throughout development.

---

## Conclusion: a tractable engineering challenge with clear design choices

The path to outperforming Redis is no longer speculative — Dragonfly, Garnet, and ScyllaDB have independently validated that shared-nothing, thread-per-core architectures deliver order-of-magnitude improvements over single-threaded designs on modern multi-core hardware. Rust adds three advantages that C++ alternatives lack: compile-time prevention of data races across shard boundaries (ownership model makes shared mutable state a type error), zero-cost abstractions for the async I/O layer (no Boost.Fibers overhead), and a memory safety guarantee that eliminates an entire class of production incidents.

The critical architectural decisions, ranked by impact: **(1)** Thread-per-core shared-nothing with per-shard DashTables — this single choice determines the throughput ceiling. **(2)** io_uring with registered buffers, fixed files, and multishot operations — this determines the I/O overhead floor. **(3)** Forkless persistence via compartmentalized snapshots — this eliminates Redis's worst operational hazard. **(4)** 16-byte compact value encoding with SSO and tagged pointers — this determines memory efficiency. **(5)** `memchr`-accelerated SIMD RESP parsing with `bytes::Bytes` zero-copy — this minimizes per-command overhead.

The one genuinely hard problem is multi-key transaction coordination across shards without sacrificing single-key performance. VLL provides the proven algorithm, but the implementation must ensure that the common path (single-shard operations, which represent >95% of real workloads) incurs zero overhead from the transaction machinery. Getting this right — fast path truly fast, slow path correct — is the core engineering challenge that separates a benchmark demo from a production system.