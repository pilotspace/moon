---
title: "Moon"
description: "A high-performance, Redis-compatible in-memory data store written in Rust."
hide:
  - navigation
---

<div class="moon-hero" markdown>
<div class="moon-hero__inner" markdown>

# Moon

<p class="moon-hero__tagline">The Redis-compatible in-memory data store, reimagined in Rust — 230+ commands, vector + full-text search, and cross-store ACID, at up to <strong>2.2× Redis throughput</strong>.</p>

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

Moon is a Redis-compatible in-memory data store built from scratch in Rust. It implements 230+ commands with a thread-per-core shared-nothing architecture, achieving up to **1.7–2.2× Redis throughput** while using **27–35% less memory** for real-world value sizes. Beyond Redis compatibility, Moon provides cross-store ACID transactions, HNSW vector + BM25 full-text search, a Cypher property graph, workspace partitioning, durable message queues, and bi-temporal MVCC.

!!! note
    **Production-grade architecture, pre-1.0 maturity.** Single-node Moon (v0.2.0) is recommended for production caching, AI workloads, and Redis-compatible OLTP. Multi-node clustering and multi-shard master PSYNC are **alpha** — see the [production contract](configuration.md) for the honest GA matrix. Wire protocol and on-disk format are LTS as of v0.2; CLI flags may still evolve until v1.0.

## Highlights

<div class="grid cards" markdown>

-   :material-console-line:{ .lg .middle } __230+ commands__

    ---

    Strings, hashes, lists, sets, sorted sets, streams, pub/sub, transactions, Lua scripting, vector search, and graph.

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
