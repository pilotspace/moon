# Moon (rust-redis)

## What This Is

A high-performance, Redis-compatible in-memory key-value store written in Rust. Implements 200+ Redis commands across all major data types, with dual-runtime support (Tokio + Monoio), thread-per-core shared-nothing architecture, SIMD-accelerated parsing, forkless persistence, TLS 1.3, ACL security, replication, cluster mode, Lua scripting, and streams. Consistently outperforms Redis 8.x by 1.5-3x on throughput benchmarks while using less memory for 1KB+ values.

## Core Value

Simple, easy to use, high performance, and effectively memory-efficient — a Redis-compatible server that does fewer things but does them exceptionally well.

## Current State

**Version:** v0.1.0 (shipped 2026-03-27)
**Codebase:** ~54K lines Rust across 96 files
**Architecture:** Thread-per-core shared-nothing with per-shard DashTable (Swiss Table SIMD probing)
**Runtimes:** Tokio (all platforms) + Monoio (Linux io_uring / macOS kqueue)
**Performance:** 1.84-1.99x Redis throughput at 8 shards, 3.17x at pipeline depth 64

## Requirements

### Validated

- RESP2/RESP3 protocol parser and serializer — v0.1.0
- TCP server with concurrent connections and graceful shutdown — v0.1.0
- All 5 core data types: Strings, Lists, Hashes, Sets, Sorted Sets — v0.1.0
- Key expiration (TTL) with lazy + active expiration — v0.1.0
- RDB persistence (forkless compartmentalized snapshots) — v0.1.0
- AOF persistence (per-shard WAL with batched fsync) — v0.1.0
- Command pipelining with adaptive batching — v0.1.0
- Pub/Sub messaging with cross-shard fan-out — v0.1.0
- LRU/LFU eviction policies — v0.1.0
- CLI compatibility (redis-cli, redis-benchmark) — v0.1.0
- Blocking commands (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX) — v0.1.0
- Streams data type (XADD/XREAD/XRANGE/XGROUP consumer groups) — v0.1.0
- RESP3 protocol with HELLO negotiation — v0.1.0
- Replication with PSYNC2 protocol — v0.1.0
- Cluster mode with hash slots and gossip protocol — v0.1.0
- Lua scripting (EVAL/EVALSHA with sandbox) — v0.1.0
- ACL system with per-user permissions — v0.1.0
- TLS 1.3 via rustls + aws-lc-rs — v0.1.0
- Thread-per-core shared-nothing architecture — v0.1.0
- SIMD-accelerated RESP parsing — v0.1.0

### Active

- [ ] Vector search (HNSW index, per-shard, filtered search)
- [ ] Redis Functions (server-side scripting v2)
- [ ] Client-side caching with invalidation tracking

### Out of Scope

- Redis Modules API — build features natively instead
- Sentinel — cluster mode covers HA needs

## Context

Shipped v0.1.0 with 54K LOC Rust in 4 days (43 phases, 132 plans).
Tech stack: Rust, Tokio, Monoio, jemalloc, rustls, mlua, bumpalo.
Benchmarks show 1.84-1.99x Redis throughput, 27-35% less memory at 1KB+ values.
132/132 data consistency tests pass across 1/4/12 shard configurations.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Thread-per-core shared-nothing | Eliminates lock contention, linear scaling | Good — 1.84-1.99x Redis at 8 shards |
| Dual runtime (Tokio + Monoio) | Portability (macOS) + performance (Linux io_uring) | Good — works on both platforms |
| DashTable with Swiss Table SIMD | O(1) lookup with cache-friendly probing | Good — faster than std HashMap |
| Forkless persistence | No COW memory spike, async per-segment snapshots | Good — near-zero overhead |
| Per-shard WAL | Avoids global lock, parallel fsync | Good — 2.75x Redis with AOF |
| CompactKey SSO (23-byte inline) | Eliminates heap allocation for short keys | Good — 62% less per-key overhead |
| HeapString for values | Eliminates Arc overhead for non-shared strings | Good — beats Redis memory at 1KB+ |
| Lock-free oneshot channels | Replaces tokio::oneshot, 12% CPU reduction | Good — measurable in flamegraph |
| CachedClock for timestamps | Avoids syscall per operation, 4% throughput gain | Good — validated in profiling |
| rustls + aws-lc-rs for TLS | FIPS-capable, no OpenSSL dependency | Good — clean build, dual-port support |

## Constraints

- **Language**: Rust (stable toolchain)
- **Protocol**: RESP2/RESP3 compatible — works with redis-cli and all Redis client libraries
- **Architecture**: Thread-per-core shared-nothing with per-shard event loops
- **Dependencies**: Minimal — tokio, monoio, jemalloc, rustls, mlua, bumpalo
- **Memory**: Efficient layout — CompactKey SSO, HeapString, CompactValue, arena allocation

---
*Last updated: 2026-03-27 after v0.1.0 milestone*
