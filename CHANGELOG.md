# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-27

Initial release. A Redis-compatible in-memory data store written in Rust, achieving 1.84-1.99x Redis throughput at 8 shards and 27-35% less memory for 1KB+ values.

### Added

#### Core Data Types (Phases 1-5)
- RESP2 protocol parser and serializer with inline command support
- TCP server with concurrent connections, graceful shutdown, and `redis-cli` compatibility
- String commands: GET, SET, MGET, MSET, INCR/DECR, APPEND, GETEX, GETDEL (17 commands)
- Hash commands: HSET, HGET, HGETALL, HINCRBY, HSCAN (14 commands)
- List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LPOS (12 commands)
- Set commands: SADD, SREM, SINTER, SUNION, SDIFF, SPOP (15 commands)
- Sorted Set commands: ZADD, ZRANGE, ZRANGEBYSCORE, ZINCRBY, ZPOPMIN (18 commands)
- Key management: DEL, EXISTS, EXPIRE, TTL, SCAN, KEYS, RENAME (13 commands)
- Lazy + active key expiration with probabilistic sampling
- RDB persistence with point-in-time snapshots
- AOF persistence with configurable fsync (always/everysec/no)
- Pub/Sub messaging: SUBSCRIBE, PUBLISH, PSUBSCRIBE (4 commands)
- Transactions: MULTI/EXEC/DISCARD with WATCH optimistic locking
- LRU/LFU/random eviction policies with configurable maxmemory

#### Performance Architecture (Phases 6-15)
- SIMD-accelerated RESP parsing via memchr CRLF scanning and atoi
- CompactValue 16-byte SSO struct with embedded TTL delta
- DashTable segmented hash table with Swiss Table SIMD probing
- Thread-per-core shared-nothing architecture with per-shard event loops
- io_uring networking layer with multishot accept/recv and registered buffers
- Per-shard memory management with jemalloc and bumpalo arenas
- Forkless compartmentalized persistence (no COW memory spike)
- B+ tree sorted sets replacing BTreeMap for cache-friendly access
- Per-connection arena allocation with bumpalo

#### Protocol & Data Types (Phases 16-18)
- RESP3 protocol: Map, Set, Double, Boolean, VerbatimString, Push frames
- HELLO command for protocol negotiation
- Client-side caching invalidation via Push frames
- Blocking commands: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX
- Streams data type: XADD, XREAD, XRANGE, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM

#### Clustering & Replication (Phases 19-20, 26)
- PSYNC2-compatible replication with per-shard WAL streaming
- Partial resync support with replication backlog
- Cluster mode with 16,384 hash slots and gossip protocol
- MOVED/ASK redirections and live slot migration
- Majority consensus failover election with automatic promotion

#### Scripting & Security (Phases 21-22, 43)
- Lua 5.4 scripting via mlua: EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH
- Sandboxed Lua VM with Redis API bindings (redis.call, redis.pcall)
- ACL system: per-user command/key/channel permissions
- ACL SETUSER, GETUSER, DELUSER, LIST, WHOAMI, LOG, SAVE, LOAD
- TLS 1.3 via rustls + aws-lc-rs with dual-port support
- mTLS client authentication
- Protected mode (reject non-loopback when no password set)

#### Optimization (Phases 24-42)
- WAL v2 format: checksums, header, block framing, corruption isolation
- CompactKey SSO: 23-byte inline keys, eliminating heap allocation
- Response buffer pooling and adaptive pipeline batching
- Dual runtime: Tokio (all platforms) + Monoio (Linux io_uring / macOS kqueue)
- Full Monoio migration: channels, TCP, codec, spawn, persistence, replication
- Direct GET serialization bypassing Frame allocation
- Zero-copy argument slicing from parse buffer
- Lock-free oneshot channels (12% CPU reduction vs tokio::oneshot)
- CachedClock timestamp caching (4% throughput gain)
- HeapString for values (eliminates Arc overhead)
- Inline dispatch for single-shard commands

### Performance

| Benchmark | Result |
|-----------|--------|
| Peak GET throughput | 3.79M ops/sec (4 shards, p=64) |
| Peak SET with AOF | 2.78M ops/sec (AOF everysec, p=64) |
| vs Redis (pipeline=64) | 3.17x SET, 2.50x GET |
| vs Redis (8 shards, p=16) | 1.84-1.99x |
| vs Redis with AOF | 2.75x (per-shard WAL vs global) |
| Memory (1KB+ values) | 27-35% less than Redis |
| Memory (empty server) | Identical 7.0 MB baseline |
| p50 latency (8 shards) | 0.031ms (Redis: 0.26ms) |
| Data consistency | 132/132 tests pass |

### Technical Details

- **Language:** Rust (stable, edition 2024)
- **Lines of code:** ~54,000 across 96 files
- **Dependencies:** tokio, monoio, jemalloc, rustls, mlua, bumpalo, bytes, clap
- **Supported platforms:** Linux (io_uring via Monoio), macOS (kqueue via Monoio or Tokio)
- **Build time:** ~50s release build

[0.1.0]: https://github.com/pilotspace/moon/releases/tag/v0.1.0
