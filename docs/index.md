---
title: "Moon"
description: "A high-performance, Redis-compatible in-memory data store written in Rust."
hide:
  - navigation
---

<div class="moon-hero" markdown>
<div class="moon-hero__inner" markdown>

# Moon

<p class="moon-hero__tagline">The Redis-compatible in-memory data store, reimagined in Rust — 250+ commands, vector + full-text search, and cross-store ACID, with a thread-per-core core that reaches <strong>2.29–2.40× Redis on pipelined GET</strong>.</p>

[Get started](quickstart.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/pilotspace/moon){ .md-button }

<div class="moon-stats" markdown>
<div markdown>**2.40×** Redis, GET p=64</div>
<div markdown>**1.78×** Redis, SET p=64</div>
<div markdown>**15–17%** less memory, ≥1 KB values</div>
<div markdown>**132/132** consistency tests</div>
</div>

<p class="moon-hero__tagline" style="font-size:0.8rem;opacity:0.85;margin-top:0.6rem" markdown>Measured on Linux, x86_64 — GCE c3-standard-8, <code>--shards 1</code>, c=50. Throughput vs Redis 7.0.15 at p=64; <strong>GET and SET only</strong> — every other command family runs 0.40–0.67× Redis at p≥8. Memory vs Redis 7.4.2/jemalloc, per-key RSS; Moon is <strong>worse</strong> below 256 B values and 1.7× worse when empty. <a href="benchmarks/">Conditions and full matrix</a>.</p>

</div>
</div>

Moon is a Redis-compatible in-memory data store built from scratch in Rust. It implements 250+ commands with a thread-per-core shared-nothing architecture. Beyond Redis compatibility, Moon provides cross-store ACID transactions, HNSW vector + BM25 full-text search, a Cypher property graph, workspace partitioning, durable message queues, and bi-temporal MVCC.

!!! warning "Before quoting any performance number from this site"
    Every published figure must come from a **Linux** host per `CLAUDE.md`, and
    every ratio depends on conditions that change it by more than the ratio
    itself. Two in particular: the pipelined win is **GET/SET only** — on Linux,
    INCR, LPUSH, SPOP and HSET all run **0.40–0.67× Redis at p≥8** — and the
    memory win is **large-value only**: 15–17% less per key at ≥1 KB values, but
    **11–51% *more* at 32 B**, and an empty moon server uses **1.7× the RSS of an
    empty Redis** ([#821](https://github.com/pilotspace/moon/issues/821)). All of
    it is x86_64; on aarch64 at `--shards 8` moon is 16% worse at 64 B. The
    [benchmarks page](benchmarks.md) labels the host on every table; anything
    marked *macOS dev reference* is a development record, not a result.

!!! note
    **Production-grade architecture, pre-1.0 maturity.** Single-node Moon (v0.2.0) is recommended for production caching, AI workloads, and Redis-compatible OLTP. Multi-node clustering and multi-shard master PSYNC are **alpha** — see the [production contract](configuration.md) for the honest GA matrix. Wire protocol and on-disk format are LTS as of v0.2; CLI flags may still evolve until v1.0.

## Highlights

<div class="grid cards" markdown>

-   :material-console-line:{ .lg .middle } __250+ commands__

    ---

    Strings, hashes, lists, sets, sorted sets, streams, geo, HyperLogLog, pub/sub, transactions, Lua scripting, vector search, and graph.

    [:octicons-arrow-right-24: Command reference](commands.md)

-   :material-chip:{ .lg .middle } __Thread-per-core__

    ---

    Shared-nothing design with per-shard event loops, DashTable SIMD probing, and lock-free channels.

    [:octicons-arrow-right-24: Architecture](architecture.md)

-   :material-lightning-bolt:{ .lg .middle } __Dual runtime__

    ---

    Monoio (io_uring on Linux, kqueue on macOS) for peak performance. Tokio for portability.

    [:octicons-arrow-right-24: Runtimes](architecture.md#dual-runtime)

-   :material-database:{ .lg .middle } __Per-shard persistence__

    ---

    Forkless RDB snapshots and per-shard WAL with no global lock. AOF advantage grows with pipeline depth.

    [:octicons-arrow-right-24: Persistence](guides/persistence.md)

-   :material-lock:{ .lg .middle } __Cross-store transactions__

    ---

    `TXN.BEGIN/COMMIT/ABORT` for atomic writes across KV, vector, and graph stores with undo-log rollback.

    [:octicons-arrow-right-24: Transactions](guides/transactions.md)

-   :material-magnify:{ .lg .middle } __Full-text + vector search__

    ---

    BM25 inverted index, HNSW + TurboQuant vectors, three-way hybrid fusion, and `FT.AGGREGATE`.

    [:octicons-arrow-right-24: Search guide](guides/full-text-search.md)

-   :material-server-network:{ .lg .middle } __Workspaces & queues__

    ---

    Multi-tenant namespace isolation (WS) and durable at-least-once queues with dead-letter and triggers (MQ).

    [:octicons-arrow-right-24: Workspaces](guides/workspaces.md)

-   :material-power-plug:{ .lg .middle } __Drop-in compatible__

    ---

    Works with any Redis client — connect with `redis-cli`, Jedis, ioredis, or redis-py out of the box.

    [:octicons-arrow-right-24: Quick start](quickstart.md)

</div>

## Feature overview

Everything Moon ships today, grouped by area. **252 commands** (per the `COMMAND_META` registry) across 25 groups — the full list lives in the [command reference](commands.md).

### Core data store (Redis-compatible)

| Feature | What it does | Docs |
|---------|--------------|------|
| **Data structures** | Strings, Hashes, Lists, Sets, Sorted Sets, and Streams — 100+ operations with RESP2/RESP3 semantics. | [Commands](commands.md) |
| **Geospatial** | `GEOADD`, `GEOSEARCH`, `GEORADIUS`, `GEODIST`, `GEOHASH`, and more (8 ops). | [Commands](commands.md) |
| **HyperLogLog** | Probabilistic cardinality — `PFADD`, `PFCOUNT`, `PFMERGE` (3 ops). | [Commands](commands.md) |
| **Keyspace & TTL** | Expiry, `SCAN`, `TYPE`, key management, and keyspace notifications (15 ops). | [Commands](commands.md) |
| **Pub/Sub** | Channel and pattern-based messaging (5 ops). | [Commands](commands.md) |
| **Transactions** | `MULTI`/`EXEC`/`DISCARD`/`WATCH` optimistic locking (5 ops). | [Transactions](guides/transactions.md) |
| **Lua scripting** | Sandboxed `EVAL`/`EVALSHA` with lazy sandbox init (5 ops). | [Commands](commands.md) |
| **Connection & ACL** | `AUTH`, `HELLO`, RESP2/3 negotiation, and 8 ACL commands. | [Security](security.md) |
| **Drop-in protocol** | Works with `redis-cli`, Jedis, ioredis, redis-py, and any RESP client. | [Redis compatibility](redis-compat.md) |

### Search & AI

| Feature | What it does | Docs |
|---------|--------------|------|
| **Vector search** | Native HNSW + TurboQuant (4-bit) index — COSINE/L2/IP, `EF_RUNTIME` tuning (11 `FT.*` ops). Light mode measured at 452 B/vector vs Redis Stack's 3,840 B on a macOS dev reference rig; not reproduced on Linux. | [Vector search](vector-search-guide.md) |
| **Full-text search** | BM25 inverted index over `TEXT`/`TAG`/`NUMERIC` fields with typo tolerance. | [Full-text search](guides/full-text-search.md) |
| **Hybrid fusion** | Three-way BM25 + dense + sparse retrieval fused via Reciprocal Rank Fusion (RRF). | [Full-text search](guides/full-text-search.md) |
| **Aggregations** | `FT.AGGREGATE` pipelines — `GROUPBY`, `REDUCE`, `SORTBY`, `FILTER`, `LIMIT`. | [Full-text search](guides/full-text-search.md) |
| **Property graph** | Cypher subset with vector-guided traversal — `GRAPH.*` (14 ops). | [Commands](commands.md) |
| **Semantic cache** | `FT.CACHESEARCH` single-RTT cache-or-search for LLM responses. | [SDK](guides/sdk.md) |
| **Memory engine** | Converged KV + vector + graph + ACID as a substrate for AI agent memory. | [Memory engine](guides/memory-engine.md) |
| **Python SDK** | `moondb` typed client with LangChain and LlamaIndex vector-store adapters. | [SDK](guides/sdk.md) |

### Durability & recovery

| Feature | What it does | Docs |
|---------|--------------|------|
| **Per-shard WAL/AOF** | Lock-free per-shard append log — advantage grows with pipeline depth. | [Persistence](guides/persistence.md) |
| **RDB snapshots** | Forkless point-in-time snapshots with no global stall. | [Persistence](guides/persistence.md) |
| **Point-in-time recovery** | Replay the WAL to any timestamp (PITR). | [PITR](guides/pitr.md) |
| **Change data capture** | Per-shard WAL streamed as Debezium-compatible JSON envelopes. | [CDC](guides/cdc.md) |
| **Cross-store ACID** | `TXN.BEGIN/COMMIT/ABORT` atomic writes across KV, vector, and graph with undo-log rollback. | [Transactions](guides/transactions.md) |

### Multi-tenancy & messaging

| Feature | What it does | Docs |
|---------|--------------|------|
| **Workspaces** | Multi-tenant namespace isolation with per-workspace auth — `WS` (5 ops). | [Workspaces](guides/workspaces.md) |
| **Message queues** | Durable at-least-once queues with dead-letter and debounced triggers — `MQ` (7 ops). | [Message queues](guides/message-queues.md) |
| **Temporal queries** | Bi-temporal MVCC with `AS_OF` / `VALID_AT` time-travel across KV and graph. | [Temporal](guides/temporal.md) |

### Performance & architecture

| Feature | What it does | Docs |
|---------|--------------|------|
| **Thread-per-core** | Shared-nothing design with per-shard event loops and SO_REUSEPORT. | [Architecture](architecture.md) |
| **Dual runtime** | Monoio (io_uring on Linux, kqueue on macOS) for peak throughput; Tokio for portability. | [Architecture](architecture.md#dual-runtime) |
| **Compact SSO types** | Inline keys (≤23 B) and values (≤12 B) — no heap allocation below the cutoff. Per-key memory on Linux x86_64: **15–17% less than Redis at ≥1 KB values, 11–51% more at 32 B**; see [benchmarks](benchmarks.md). | [Architecture](architecture.md) |
| **Lock-free hot path** | DashTable SIMD probing and `flume` channels — no global locks on writes. | [Architecture](architecture.md) |

### Operations & deployment

| Feature | What it does | Docs |
|---------|--------------|------|
| **Replication** | `PSYNC`-based primary/replica streaming (5 ops). Multi-shard master PSYNC is **alpha**. | [Production contract](PRODUCTION-CONTRACT.md) |
| **Clustering** | Gossip + slot routing — `CLUSTER` (9 ops). **Alpha**; single-node is the recommended production target. | [Clustering](guides/clustering.md) |
| **TLS** | Encrypted client connections with cert rotation runbook. | [TLS](guides/tls.md) |
| **Docker** | Container images for local and production deployment. | [Docker](guides/docker.md) |
| **Monitoring** | `INFO` sections, slowlog, and metrics for observability. | [Monitoring](guides/monitoring.md) |
| **Security** | ACL rules, Lua sandbox isolation, and a published threat model. | [Security](security.md) |

## Key metrics

Linux only, each row with the conditions that produced it. Full report and the
macOS development tables: [benchmarks](benchmarks.md).

| Metric | Result | Conditions |
|--------|--------|------------|
| Peak GET (v0.8.7) | **2.40× Redis** | GCE c3-standard-8 x86_64, Redis 7.0.15, `--shards 1`, c=50, p=64 |
| Peak GET, ARM64 (v0.8.7) | **2.29× Redis** | GCE t2a-standard-8 Neoverse-N1, same config |
| Peak SET (v0.8.7) | **1.78× x86 / 2.02× ARM** | same runs |
| Every other command family | **0.40–0.67× Redis** | INCR/LPUSH/SPOP/HSET at p≥8, both arches |
| Peak GET, absolute (v0.1.6) | **5.11M ops/sec (1.72×)** | GCloud c3-standard-8 x86_64, p=64; Redis `io-threads` and payload size not recorded |
| Memory, ≥1 KB values | **15–17% less than Redis** | GCE c3-standard-8 x86_64, `--shards 1`, Redis 7.4.2/jemalloc, per-key RSS (9.5% at 63K keys) |
| Memory, 256 B values | tie (±8%) | same run |
| Memory, 32 B values | **11–51% worse than Redis** | same run |
| Empty-server RSS | **1.7× worse than Redis** | same run — 12.6–12.9 MB vs 7.5–7.7 MB ([#821](https://github.com/pilotspace/moon/issues/821)) |
| Memory, 64 B values (aarch64) | **1.16× worse than Redis** | GCE t2a-standard-8, `--shards 8` vs Redis `--io-threads 8` |
| CPU per operation | **tie** (10.55 vs 11.33 µs) | same run; moon 6× more stable run to run |
| Shard scaling (8 shards / 1 shard) | **1.42× / 2.14× / 3.79×** | p=1 / p=8 / p=64, explicitly-keyed families |
| Vector search (384d) | **12.7K QPS** | GCloud c3-standard-8 x86_64, HNSW + TurboQuant 8-bit, COSINE, 50K vectors, K=10 |
| Data correctness | **132/132 tests** | all types, 1/4/12 shards |

## Quick start

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
cargo build --release
./target/release/moon --port 6379 --shards 4
```

```bash
redis-cli -p 6379
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
```

[:octicons-arrow-right-24: Full quick start guide](quickstart.md){ .md-button }
