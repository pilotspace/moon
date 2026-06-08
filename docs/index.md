---
title: "Moon"
description: "A high-performance, Redis-compatible in-memory data store written in Rust."
---

# Moon

Moon is a Redis-compatible in-memory data store built from scratch in Rust. It implements 230+ commands with a thread-per-core shared-nothing architecture, achieving up to **1.7–2.2x Redis throughput** while using **27-35% less memory** for real-world value sizes. Beyond Redis compatibility, Moon provides cross-store ACID transactions, HNSW vector + BM25 full-text search, a Cypher property graph, workspace partitioning, durable message queues, and bi-temporal MVCC.

!!! note
    **Production-grade architecture, pre-1.0 maturity.** Single-node Moon (v0.2.0) is recommended for production caching, AI workloads, and Redis-compatible OLTP. Multi-node clustering and multi-shard master PSYNC are **alpha** — see the [production contract](configuration.md) for the honest GA matrix. Wire protocol and on-disk format are LTS as of v0.2; CLI flags may still evolve until v1.0.

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

## Highlights

<div class="grid cards" markdown>

-   [__230+ commands__](commands.md)
    Strings, hashes, lists, sets, sorted sets, streams, pub/sub, transactions, Lua scripting, vector search, graph, and more.
-   [__Thread-per-core architecture__](architecture.md)
    Shared-nothing design with per-shard event loops, DashTable SIMD probing, and lock-free channels.
-   [__Dual runtime__](architecture.md#dual-runtime)
    Monoio (io_uring on Linux, kqueue on macOS) for peak performance. Tokio for portability.
-   [__Per-shard persistence__](guides/persistence.md)
    Forkless RDB snapshots and per-shard WAL with no global lock. AOF advantage grows with pipeline depth.
-   [__Cross-store transactions__](guides/transactions.md)
    TXN.BEGIN/COMMIT/ABORT for atomic writes across KV, vector, and graph stores with undo-log rollback.
-   [__Full-text + vector search__](commands.md#full-text-search-2)
    BM25 inverted index, HNSW + TurboQuant vectors, three-way hybrid fusion, FT.AGGREGATE with GROUPBY/REDUCE.
-   [__Workspaces and message queues__](guides/workspaces.md)
    Multi-tenant namespace isolation (WS), durable at-least-once queues with dead-letter and triggers (MQ).
-   [__Drop-in compatible__](quickstart.md)
    Works with any Redis client. Connect with redis-cli, Jedis, ioredis, or redis-py out of the box.

</div>

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

-   [__Full quick start guide__](quickstart.md)
    Prerequisites, build options, Docker, and connecting with clients.
