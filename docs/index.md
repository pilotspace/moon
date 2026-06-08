---
title: "Moon"
description: "A high-performance, Redis-compatible in-memory data store written in Rust."
hide:
  - navigation
---

<div class="moon-hero" markdown>
<div class="moon-hero__inner" markdown>

# Moon

<p class="moon-hero__tagline">The Redis-compatible in-memory data store, reimagined in Rust — 250+ commands, vector + full-text search, and cross-store ACID, at up to <strong>2.2× Redis throughput</strong>.</p>

[Get started](quickstart.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/pilotspace/moon){ .md-button }

<div class="moon-stats" markdown>
<div markdown>**5.11M** GET ops/sec</div>
<div markdown>**3.50M** SET ops/sec</div>
<div markdown>**27–35%** less memory</div>
<div markdown>**132/132** consistency tests</div>
</div>

</div>
</div>

Moon is a Redis-compatible in-memory data store built from scratch in Rust. It implements 250+ commands with a thread-per-core shared-nothing architecture, achieving up to **1.7–2.2× Redis throughput** while using **27–35% less memory** for real-world value sizes. Beyond Redis compatibility, Moon provides cross-store ACID transactions, HNSW vector + BM25 full-text search, a Cypher property graph, workspace partitioning, durable message queues, and bi-temporal MVCC.

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
| **Vector search** | Native HNSW + TurboQuant (4-bit) index — COSINE/L2/IP, `EF_RUNTIME` tuning, up to 8.5× less memory/vector (11 `FT.*` ops). | [Vector search](vector-search-guide.md) |
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
| **Compact SSO types** | Inline keys (≤23 B) and values (≤12 B) — 27–35% less memory on real-world sizes. | [Architecture](architecture.md) |
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

Headline numbers vs Redis 8.6.1 on GCloud c3-standard-8 (x86_64), peak throughput. Full report: see [benchmarks](benchmarks.md).

| Metric | Result | Conditions |
|--------|--------|------------|
| Peak GET throughput | **5.11M ops/sec** | GCloud x86_64, p=64 (1.72× Redis) |
| Peak SET throughput | **3.50M ops/sec** | GCloud x86_64, p=64 (1.92× Redis) |
| Peak GET (ARM64) | **3.47M ops/sec** | GCloud Neoverse-N1, p=64 (2.20× Redis) |
| Memory (1KB+ values) | **27-35% less** | per-key RSS measurement |
| Vector search (384d) | **12.7K QPS** | HNSW + TurboQuant, COSINE |
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
