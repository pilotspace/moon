# Roadmap: Rust Redis

## Overview

Build a Redis-compatible in-memory key-value store in Rust that achieves **10-12x throughput**, **5x lower p99 latency**, and **2.5-3.5x better memory efficiency** versus single-instance Redis. Phases 1-7 delivered a correct, feature-complete single-threaded Redis clone (109 commands, 487 tests). Phases 8-23 rework the architecture toward the blueprint's thread-per-core shared-nothing design with io_uring, DashTable, forkless persistence, and full Redis feature parity (Streams, Cluster, Replication, Lua, ACL).

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Protocol Foundation** - RESP2 parser/serializer with zero-copy Bytes-based frames and inline command support (completed 2026-03-23)
- [x] **Phase 2: Working Server** - TCP server with string commands, key management, lazy expiration, and redis-cli compatibility (completed 2026-03-23)
- [x] **Phase 3: Collection Data Types** - Hash, List, Set, Sorted Set commands, active expiration, SCAN iteration, and AUTH (completed 2026-03-23)
- [x] **Phase 4: Persistence** - RDB snapshots, AOF append-only file, startup restore, and manual trigger commands (completed 2026-03-23)
- [x] **Phase 5: Pub/Sub, Transactions, and Eviction** - Channel messaging, MULTI/EXEC with WATCH, and LRU/LFU memory eviction (completed 2026-03-23)
- [x] **Phase 6: Setup and Benchmark** - README, Dockerfile, CI, bench.sh, BENCHMARK.md with Redis 7.x comparison (completed 2026-03-23)
- [x] **Phase 7: Bottleneck Resolution** - Entry compaction, parking_lot migration, pipeline batching (completed 2026-03-23)
- [x] **Phase 8: RESP Parser SIMD Acceleration** - memchr-accelerated CRLF scanning, bumpalo arena for parsing temporaries, atoi fast integer parsing (completed 2026-03-24)
- [x] **Phase 9: Compact Value Encoding** - 16-byte SSO struct, tagged pointers, embedded TTL delta, eliminate per-key heap allocations for small values (completed 2026-03-25)
- [x] **Phase 10: DashTable Hash Table** - Segmented hash table with Swiss Table SIMD probing, per-segment incremental rehashing, inline entry storage (completed 2026-03-24)
- [x] **Phase 11: Thread-per-Core Shared-Nothing** - Glommio/Monoio runtime, per-core shard with independent event loop, SPSC cross-shard channels, VLL coordination (completed 2026-03-24)
- [x] **Phase 12: io_uring Networking** - Multishot accept/recv, registered buffers and FDs, SQE batching, zero-copy response scatter-gather (completed 2026-03-24)
- [x] **Phase 13: Per-Shard Memory Management** - mimalloc global allocator, bumpalo per-request arenas, NUMA-aware placement, cache-line discipline (completed 2026-03-24)
- [x] **Phase 14: Forkless Persistence** - Async compartmentalized snapshots via DashTable segments, per-shard WAL with batched fsync, near-zero memory overhead (completed 2026-03-24)
- [x] **Phase 15: B+ Tree Sorted Sets** - Cache-friendly B+ tree replacing BTreeMap, listpack for small collections, intset for integer sets (completed 2026-03-24)
- [x] **Phase 16: RESP3 Protocol** - RESP3 frame types (Map, Set, Double, Boolean, Verbatim, Push), HELLO negotiation, client-side caching hints (completed 2026-03-24)
- [x] **Phase 17: Blocking Commands** - BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX with fiber-based blocking and timeout (completed 2026-03-24)
- [x] **Phase 18: Streams Data Type** - Radix tree + listpack storage, XADD/XREAD/XRANGE/XGROUP consumer groups (completed 2026-03-24)
- [x] **Phase 19: Replication** - PSYNC2-compatible protocol, per-shard WAL streaming, partial resync, replica promotion (completed 2026-03-24)
- [x] **Phase 20: Cluster Mode** - 16,384 hash slots, gossip protocol, MOVED/ASK redirections, live slot migration (completed 2026-03-24)
- [x] **Phase 21: Lua Scripting** - Embedded Lua 5.4 via mlua, EVAL/EVALSHA, script caching, sandbox with Redis API bindings (completed 2026-03-24)
- [x] **Phase 22: ACL System** - Per-user permissions, command/key/channel restrictions, ACL SETUSER/GETUSER/LIST/DELUSER (completed 2026-03-24)
- [x] **Phase 23: Final Performance Validation** - memtier_benchmark suite, throughput-latency curves, memory profiling, comparison vs Redis Cluster on 64 cores (completed 2026-03-24)

## Phase Details

### Phase 1: Protocol Foundation
**Goal**: A complete, isolated RESP2 protocol library that parses and serializes all frame types with zero-copy semantics
**Depends on**: Nothing (first phase)
**Requirements**: PROTO-01, PROTO-02
**Success Criteria** (what must be TRUE):
  1. Parser correctly decodes all RESP2 frame types (Simple Strings, Errors, Integers, Bulk Strings, Arrays, Null) from raw bytes
  2. Serializer encodes Redis responses back to valid RESP2 wire format
  3. Inline commands (plain text without RESP framing) are parsed correctly
  4. Parser uses Bytes-based owned frames with no lifetime parameters leaking beyond the protocol module
**Plans**: 2 plans

Plans:
- [ ] 01-01-PLAN.md — Core RESP2 parser, serializer, and Frame types (PROTO-01)
- [ ] 01-02-PLAN.md — Inline command parser and benchmarks (PROTO-02)

### Phase 2: Working Server
**Goal**: A running TCP server that accepts redis-cli connections and executes string commands, key management, and lazy expiration -- the first "working Redis"
**Depends on**: Phase 1
**Requirements**: PROTO-03, NET-01, NET-02, NET-03, CONN-01, CONN-02, CONN-03, CONN-04, CONN-05, CONN-07, STR-01, STR-02, STR-03, STR-04, STR-05, STR-06, STR-07, STR-08, STR-09, KEY-01, KEY-02, KEY-03, KEY-04, KEY-05, KEY-06, KEY-08, EXP-01
**Success Criteria** (what must be TRUE):
  1. redis-cli connects to the server on a configurable port (default 6379), sends PING, and receives PONG
  2. User can SET a key with TTL options (EX/PX/NX/XX), GET it back, and observe it disappears after expiration
  3. User can perform atomic integer operations (INCR/DECR) and multi-key operations (MGET/MSET) from redis-cli
  4. User can manage keys (DEL, EXISTS, EXPIRE, TTL, TYPE, KEYS, RENAME) and observe correct behavior
  5. Server handles multiple concurrent redis-cli sessions, pipelined commands, and graceful shutdown (Ctrl+C)
**Plans**: 4 plans

Plans:
- [ ] 02-01-PLAN.md — Storage engine, RESP codec, TCP server, and connection commands
- [ ] 02-02-PLAN.md — String commands (GET/SET/MGET/MSET/INCR/DECR/APPEND/STRLEN + legacy + get-and-modify)
- [ ] 02-03-PLAN.md — Key management commands (DEL/EXISTS/EXPIRE/TTL/PERSIST/TYPE/KEYS/RENAME)
- [ ] 02-04-PLAN.md — Integration tests and redis-cli verification

### Phase 3: Collection Data Types
**Goal**: All five Redis data types are operational with full command coverage, plus active expiration, cursor-based iteration, and password authentication
**Depends on**: Phase 2
**Requirements**: CONN-06, KEY-07, KEY-09, EXP-02, HASH-01, HASH-02, HASH-03, HASH-04, HASH-05, HASH-06, HASH-07, HASH-08, LIST-01, LIST-02, LIST-03, LIST-04, LIST-05, LIST-06, SET-01, SET-02, SET-03, SET-04, SET-05, SET-06, ZSET-01, ZSET-02, ZSET-03, ZSET-04, ZSET-05, ZSET-06, ZSET-07, ZSET-08, ZSET-09
**Success Criteria** (what must be TRUE):
  1. User can create and manipulate hashes (HSET/HGET/HGETALL/HINCRBY), lists (LPUSH/RPUSH/LPOP/RPOP/LRANGE), sets (SADD/SMEMBERS/SINTER/SUNION), and sorted sets (ZADD/ZRANGE/ZRANK/ZINCRBY) from redis-cli
  2. User can safely iterate large keyspaces and collections with SCAN/HSCAN/SSCAN/ZSCAN without blocking the server
  3. Server autonomously expires keys in the background (not just on access) so memory does not grow unbounded from expired-but-unaccessed keys
  4. User can protect the server with requirepass and must AUTH before executing commands
  5. User can UNLINK keys for non-blocking deletion
**Plans**: 7 plans

Plans:
- [ ] 03-01-PLAN.md — Foundation: extend RedisValue with 4 collection variants, Database type helpers, UNLINK, SCAN, TYPE update
- [ ] 03-02-PLAN.md — Hash commands (HSET/HGET/HDEL/HMSET/HMGET/HGETALL/HEXISTS/HLEN/HKEYS/HVALS/HINCRBY/HINCRBYFLOAT/HSETNX/HSCAN)
- [ ] 03-03-PLAN.md — List commands (LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX/LSET/LINSERT/LREM/LTRIM/LPOS)
- [ ] 03-04-PLAN.md — Set commands (SADD/SREM/SMEMBERS/SCARD/SISMEMBER/SMISMEMBER/SINTER/SUNION/SDIFF/stores/SRANDMEMBER/SPOP/SSCAN)
- [ ] 03-05-PLAN.md — Sorted Set commands (ZADD/ZREM/ZSCORE/ZCARD/ZRANGE/ZRANGEBYSCORE/ZRANK/ZINCRBY/ZCOUNT/ZUNIONSTORE/ZINTERSTORE/ZPOPMIN/ZPOPMAX/ZSCAN)
- [ ] 03-06-PLAN.md — Infrastructure: active expiration background task, AUTH command, connection authentication gate
- [ ] 03-07-PLAN.md — Integration tests and redis-cli verification checkpoint

### Phase 4: Persistence
**Goal**: Data survives server restarts through RDB snapshots and AOF append-only logging
**Depends on**: Phase 3
**Requirements**: PERS-01, PERS-02, PERS-03, PERS-04, PERS-05
**Success Criteria** (what must be TRUE):
  1. User can trigger BGSAVE and find an RDB snapshot file on disk that captures the current dataset
  2. With AOF enabled, every write command is logged and the server can replay the AOF on startup to restore state
  3. After a server crash and restart, data is restored from the most recent RDB or AOF (depending on configuration)
  4. User can trigger BGREWRITEAOF to compact the AOF file without data loss
  5. User can configure AOF fsync policy (always/everysec/no) to trade durability for performance
**Plans**: 3 plans

Plans:
- [ ] 04-01-PLAN.md — Persistence config, RDB binary format (serialize/deserialize), BGSAVE command, startup RDB loading
- [ ] 04-02-PLAN.md — AOF writer task, write-command logging, AOF replay, BGREWRITEAOF, startup restore priority, auto-save timer
- [ ] 04-03-PLAN.md — Integration tests and redis-cli persistence verification checkpoint

### Phase 5: Pub/Sub, Transactions, and Eviction
**Goal**: Channel-based messaging, atomic multi-command transactions with optimistic locking, and memory-bounded operation with eviction policies
**Depends on**: Phase 4
**Requirements**: PUB-01, PUB-02, PUB-03, PUB-04, TXN-01, TXN-02, EVIC-01, EVIC-02, EVIC-03
**Success Criteria** (what must be TRUE):
  1. One redis-cli session can SUBSCRIBE to a channel and receive messages PUBLISHED by another session in real time
  2. User can PSUBSCRIBE with glob patterns and receive messages from matching channels
  3. User can wrap commands in MULTI/EXEC and observe atomic execution (all-or-nothing)
  4. User can WATCH a key before MULTI and observe the transaction abort if another client modifies the key
  5. When maxmemory is reached, server evicts keys according to configured policy (LRU or LFU) instead of rejecting writes
**Plans**: 4 plans

Plans:
- [ ] 05-01-PLAN.md — Entry metadata, eviction engine (LRU/LFU/random/volatile/TTL), RuntimeConfig, CONFIG GET/SET
- [ ] 05-02-PLAN.md — Pub/Sub subsystem (PubSubRegistry, subscriber mode, PUBLISH)
- [ ] 05-03-PLAN.md — Transactions (MULTI/EXEC/DISCARD/WATCH with optimistic locking)
- [ ] 05-04-PLAN.md — Integration tests for Pub/Sub, transactions, eviction, and CONFIG

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 7 (v1.0 complete) → 8 → 23 (v2.0 architecture rework)

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Protocol Foundation | 2/2 | Complete | 2026-03-23 |
| 2. Working Server | 4/4 | Complete | 2026-03-23 |
| 3. Collection Data Types | 7/7 | Complete | 2026-03-23 |
| 4. Persistence | 3/3 | Complete | 2026-03-23 |
| 5. Pub/Sub, Transactions, and Eviction | 4/4 | Complete | 2026-03-23 |
| 6. Setup and Benchmark | 3/3 | Complete | 2026-03-23 |
| 7. Bottleneck Resolution | 3/3 | Complete | 2026-03-23 |
| 8. RESP Parser SIMD Acceleration | 2/3 | Gap Closure | 2026-03-24 |
| 9. Compact Value Encoding | 2/2 | Complete   | 2026-03-25 |
| 10. DashTable Hash Table | 2/2 | Complete    | 2026-03-24 |
| 11. Thread-per-Core Shared-Nothing | 7/7 | Complete    | 2026-03-24 |
| 12. io_uring Networking | 4/4 | Complete    | 2026-03-24 |
| 13. Per-Shard Memory Management | 3/3 | Complete    | 2026-03-24 |
| 14. Forkless Persistence | 3/3 | Complete    | 2026-03-24 |
| 15. B+ Tree Sorted Sets | 5/5 | Complete    | 2026-03-24 |
| 16. RESP3 Protocol | 3/3 | Complete    | 2026-03-24 |
| 17. Blocking Commands | 3/3 | Complete    | 2026-03-24 |
| 18. Streams Data Type | 2/2 | Complete    | 2026-03-24 |
| 19. Replication (PSYNC2) | 4/4 | Complete    | 2026-03-24 |
| 20. Cluster Mode | 4/4 | Complete    | 2026-03-24 |
| 21. Lua Scripting | 2/2 | Complete    | 2026-03-24 |
| 22. ACL System | 3/3 | Complete    | 2026-03-24 |
| 23. Final Performance Validation | 3/3 | Complete    | 2026-03-24 |

### Phase 6: Setup and benchmark real use-cases

**Goal:** Performance validation against Redis 7.x with industry-standard benchmarks, plus project packaging (README, Dockerfile, CI)
**Requirements**: SETUP-01, SETUP-02, SETUP-03
**Depends on:** Phase 5
**Success Criteria** (what must be TRUE):
  1. README.md documents all features, CLI flags, build instructions, Docker usage, and known limitations
  2. Dockerfile produces a working multi-stage build with non-root user
  3. CI config runs cargo test + clippy + fmt on push/PR
  4. bench.sh automates redis-benchmark across all workload scenarios (13 commands, 3 concurrency levels, 3 pipeline depths, 4 data sizes)
  5. BENCHMARK.md contains throughput, latency, memory, and persistence overhead results with bottleneck analysis
**Plans:** 4 plans (3 complete + 1 gap closure)

Plans:
- [ ] 06-01-PLAN.md — Project packaging: README, Dockerfile, .dockerignore, CI config, Cargo.toml metadata
- [ ] 06-02-PLAN.md — Benchmark runner script (bench.sh)
- [ ] 06-03-PLAN.md — Run benchmarks and produce BENCHMARK.md with results and analysis

### Phase 7: Compare architecture, resolve bottlenecks, and optimize performance

**Goal:** Resolve the 3 measured bottlenecks from Phase 6 benchmarks (lock contention, memory overhead, CPU waste) through pipeline batching, Entry compaction, and parking_lot migration, with before/after benchmark validation
**Requirements**: OPT-01, OPT-02, OPT-03, OPT-04
**Depends on:** Phase 6
**Success Criteria** (what must be TRUE):
  1. Pipelined commands execute under a single lock acquisition per batch instead of per-command
  2. Entry struct metadata reduced from ~57 bytes to ~25 bytes per key
  3. parking_lot::Mutex replaces std::sync::Mutex for faster uncontended locking
  4. BENCHMARK.md contains before/after optimization comparison with measurable throughput and memory improvements
**Plans:** 4 plans (3 complete + 1 gap closure)

Plans:
- [ ] 07-01-PLAN.md — Entry struct compaction (remove created_at, u32 version/last_access)
- [ ] 07-02-PLAN.md — parking_lot migration and pipeline batching in connection handler
- [ ] 07-03-PLAN.md — Re-benchmark and before/after comparison in BENCHMARK.md

### Phase 8: RESP Parser SIMD Acceleration

**Goal:** Replace hand-written byte-by-byte RESP2 parser with memchr-accelerated SIMD scanning (SSE2/AVX2/NEON) and per-connection bumpalo arena allocation for O(1) bulk deallocation of parsing temporaries
**Depends on:** Phase 7
**Success Criteria** (what must be TRUE):
  1. `memchr` crate (v2.8+) replaces all manual byte scanning loops in parse.rs for CRLF detection
  2. `atoi` crate provides fast integer parsing for RESP length prefixes
  3. Per-connection `bumpalo::Bump` arena used for all per-request parsing temporaries, reset after each pipeline batch
  4. Criterion benchmarks show >= 4x improvement in RESP parse throughput for pipelined workloads (32+ commands)
  5. All existing tests pass unchanged
**Plans:** 3 plans

Plans:
- [ ] 08-01-PLAN.md — SIMD parser: memchr CRLF scanning, atoi integer parsing, Cursor elimination
- [ ] 08-02-PLAN.md — Bumpalo arena integration and pipeline benchmark extension
- [ ] 08-03-PLAN.md — Gap closure: concrete arena allocation and naive reference benchmark

### Phase 9: Compact Value Encoding and Entry Optimization

**Goal:** Reduce per-key memory overhead from ~56 bytes to ~16-24 bytes through a 16-byte compact value struct with small-string optimization (SSO for values <= 12 bytes inline), tagged pointers encoding value type in low bits, and embedded TTL as 4-byte delta from per-shard base timestamp
**Depends on:** Phase 8
**Success Criteria** (what must be TRUE):
  1. Values <= 12 bytes stored inline (no heap allocation) via small-string optimization
  2. Value type (string/list/set/hash/zset/stream/integer) encoded in low 3 bits of pointer or dedicated nibble
  3. TTL stored as 4-byte delta from base timestamp instead of separate 8-byte absolute milliseconds field
  4. Per-key memory overhead measured at <= 24 bytes via Criterion memory benchmarks
  5. All existing tests pass; memory usage reduced >= 40% on 1M key benchmark
**Plans:** 2/2 plans complete

Plans:
- [ ] 09-01-PLAN.md — CompactValue SSO struct, CompactEntry 24-byte replacement, TTL delta, full codebase migration
- [ ] 09-02-PLAN.md — Memory benchmark (1M keys) validating >= 40% reduction

### Phase 10: DashTable Segmented Hash Table with Swiss Table SIMD Probing

**Goal:** Replace std::HashMap with a custom segmented hash table combining Dragonfly's DashTable macro-architecture (directory -> segments -> buckets with per-segment rehashing) and hashbrown's Swiss Table SIMD micro-optimization (16-byte control groups with _mm_cmpeq_epi8 probing)
**Depends on:** Phase 9
**Success Criteria** (what must be TRUE):
  1. Hash table uses directory -> segment -> bucket hierarchy with 56 regular + 4 stash buckets per segment
  2. Segment splits independently (only the full segment rehashes, not entire table) with zero memory spike
  3. Swiss Table SIMD control bytes enable 16-way parallel key fingerprint comparison per probe group
  4. Load factor supports 87.5% without degradation; lookup resolves in <= 2 SIMD scans on average
  5. DashTable passes all existing storage tests as drop-in replacement for HashMap
  6. Memory overhead per entry <= 16 bytes (vs HashMap's ~56 bytes)
**Plans:** 3 plans (2 complete, 1 gap closure)

Plans:
- [ ] 10-01-PLAN.md — DashTable data structure: SIMD group, segment, directory, public API, iterator (DASH-01, DASH-02, DASH-03, DASH-04, DASH-06)
- [ ] 10-02-PLAN.md — Wire DashTable into Database as drop-in HashMap replacement (DASH-05)

### Phase 11: Thread-per-Core Shared-Nothing Architecture

**Goal:** Migrate from Tokio multi-threaded runtime with Arc<Mutex<Database>> to a thread-per-core shared-nothing architecture where each CPU core runs an independent shard with its own async event loop, DashTable partition, expiry index, and memory pool -- zero shared mutable state between shards
**Depends on:** Phase 10
**Requirements:** SHARD-01 through SHARD-11
**Success Criteria** (what must be TRUE):
  1. Each CPU core runs one shard with independent Tokio current_thread event loop
  2. Keyspace partitioned via xxhash64(key) % num_shards with consistent hash assignment
  3. Cross-shard communication via SPSC lock-free channels (no mutexes on data path)
  4. Multi-key commands (MGET/MSET) coordinate across shards via VLL pattern with ascending shard-ID lock ordering
  5. Single-key commands (>95% of workload) execute with zero cross-shard coordination overhead
  6. PubSub works across shards via SPSC fan-out
  7. All existing tests pass with shared-nothing backend
**Plans:** 7/7 plans complete

Plans:
- [x] 11-01-PLAN.md — Shard struct, ShardMessage enum, key_to_shard dispatch, hash tag extraction
- [ ] 11-02-PLAN.md — SPSC channel mesh (ringbuf) for inter-shard communication
- [ ] 11-03-PLAN.md — Bootstrap N shard threads with Tokio current_thread runtimes, sharded listener
- [ ] 11-04-PLAN.md — Connection handler adapted for per-shard access, cross-shard SPSC dispatch, VLL coordinator
- [ ] 11-05-PLAN.md — Per-shard PubSubRegistry with cross-shard fan-out
- [ ] 11-06-PLAN.md — Persistence adaptation (RDB per-shard snapshots, AOF global writer, startup distribute)
- [ ] 11-07-PLAN.md — Integration tests for cross-shard commands, PubSub, persistence + human verification

### Phase 12: io_uring Networking Layer

**Goal:** Replace epoll/kqueue I/O with io_uring for each shard thread, using multishot accept (single SQE for continuous connection acceptance), multishot recv with provided buffer rings, registered file descriptors and buffers, SQE batching, and zero-copy response scatter-gather via writev
**Depends on:** Phase 11
**Success Criteria** (what must be TRUE):
  1. Each shard thread operates one io_uring instance with multishot accept (Linux 5.19+)
  2. Multishot recv with provided buffer rings (Linux 6.0+) handles connection reads
  3. All client socket FDs pre-registered via IORING_REGISTER_FILES
  4. Read/write buffers pre-mapped via IORING_REGISTER_BUFFERS
  5. SQE batching: accumulate all submissions per event loop iteration, single io_uring_submit()
  6. GET responses use writev scatter-gather (header + value ref + CRLF) for zero-copy from DashTable to socket
  7. >= 10% throughput improvement over epoll baseline at 1000+ connections
  8. macOS/kqueue fallback path maintained for development (io_uring Linux-only)
**Plans:** 4/4 plans complete

Plans:
- [ ] 12-01-PLAN.md — IO module foundation: static responses, IoDriver trait, TokioDriver, cfg-gated Linux deps
- [ ] 12-02-PLAN.md — Linux io_uring driver: UringDriver, FdTable, BufRingManager, multishot accept/recv, writev
- [ ] 12-03-PLAN.md — Integration: wire UringDriver into shard event loop, cfg-gated Linux path, full test suite
- [ ] 12-04-PLAN.md — Gap closure: wire multishot accept (SO_REUSEPORT) + writev scatter-gather + fix mem::forget leak

### Phase 13: Per-Shard Memory Management and NUMA Optimization

**Goal:** Implement per-shard memory isolation with mimalloc as global allocator, bumpalo per-request arenas for each connection, NUMA-aware thread/memory placement, and cache-line discipline to eliminate false sharing
**Depends on:** Phase 12
**Success Criteria** (what must be TRUE):
  1. `mimalloc` replaces jemalloc as `#[global_allocator]` with per-thread free lists
  2. Each connection maintains a `bumpalo::Bump` arena; all per-request temporaries allocated from it; O(1) reset per batch
  3. Shard threads pinned to specific cores via `sched_setaffinity()`; memory bound to local NUMA node via `mbind(MPOL_BIND)`
  4. All per-shard mutable structures aligned to 64 bytes (`#[repr(C, align(64))]`) preventing false sharing
  5. Read-heavy fields (hash table metadata) separated from write-heavy fields (stats, expiry timestamps) on different cache lines
  6. Hash bucket prefetching via `_mm_prefetch` during lookup to hide memory latency
  7. Allocation benchmark shows >= 3x faster allocation throughput vs jemalloc under multi-threaded workloads
**Plans:** 3/3 plans complete

Plans:
- [ ] 13-01-PLAN.md — mimalloc global allocator swap, jemalloc feature flag, NUMA thread pinning
- [ ] 13-02-PLAN.md — DashTable Segment cache-line alignment and prefetch hints
- [ ] 13-03-PLAN.md — Per-connection bumpalo arena in sharded handler

### Phase 14: Forkless Compartmentalized Persistence

**Goal:** Replace clone-on-snapshot persistence with forkless async snapshotting leveraging DashTable's segmented structure -- each segment serialized independently while the shard continues serving requests, with write-ahead capture for modified-but-not-yet-serialized segments
**Depends on:** Phase 13
**Success Criteria** (what must be TRUE):
  1. Snapshot epoch marks a point-in-time; segments iterated and serialized without blocking the shard event loop
  2. Keys modified in not-yet-serialized segments have old value captured before overwrite (segment-level WAL)
  3. Serialization runs as low-priority cooperative task within shard event loop, yielding between segments
  4. Memory overhead during snapshot < 5% (vs current clone-based 2-3x spike)
  5. Per-shard WAL with io_uring DMA writes and batched fsync (1ms interval, matching Redis appendfsync everysec)
  6. Parallel per-shard snapshot files (DFS-style) with optional single interleaved RDB-compatible export
  7. Each shard snapshots independently; replica can partially resync from per-shard WAL
  8. Snapshot latency on 25GB dataset < 1ms main-thread stall (vs Redis's 60ms+ fork)
**Plans:** 3/3 plans complete

Plans:
- [ ] 14-01-PLAN.md — DashTable segment iteration API + SnapshotState engine with segment-level COW
- [ ] 14-02-PLAN.md — Per-shard WAL writer with batched fsync and WAL replay
- [ ] 14-03-PLAN.md — Integration: wire snapshot + WAL into shard event loop, BGSAVE, auto-save, startup restore

### Phase 15: B-Plus Tree Sorted Sets and Optimized Collection Encodings

**Goal:** Replace BTreeMap-based sorted sets with a cache-friendly B+ tree (2-3 bytes/entry vs ~37 bytes), implement listpack encoding for small hashes/lists/sets/zsets, and intset encoding for integer-only sets
**Depends on:** Phase 14
**Success Criteria** (what must be TRUE):
  1. Custom B+ tree stores sorted set entries with <= 3 bytes overhead per entry (vs BTreeMap's ~37 bytes)
  2. B+ tree nodes pack multiple elements sequentially for cache-friendly iteration and SIMD-friendly comparison
  3. Listpack encoding used for small collections (configurable threshold, default <= 128 elements)
  4. Intset encoding used for sets containing only integers (compact sorted integer array)
  5. Automatic encoding upgrade from compact -> full when collection exceeds threshold
  6. All ZADD/ZRANGE/ZRANGEBYSCORE/ZRANK operations maintain O(log N) or better complexity
  7. Memory reduction >= 10x for sorted sets with 1M entries vs current BTreeMap implementation
**Plans:** 6/6 plans complete

Plans:
- [x] 15-01-PLAN.md — B+ tree data structure (arena-allocated, cache-friendly)
- [x] 15-02-PLAN.md — Listpack compact encoding + Intset sorted integer array
- [x] 15-03-PLAN.md — RedisValue enum extension, Database accessor dispatch, persistence updates
- [x] 15-04-PLAN.md — Sorted set command migration (BTreeMap -> BPTree)
- [x] 15-05-PLAN.md — OBJECT ENCODING command, intset-aware SADD, integration verification
- [x] 15-06-PLAN.md — Gap closure: listpack-first hash/list creation + BPTree memory benchmark

### Phase 16: RESP3 Protocol Support

**Goal:** Implement RESP3 protocol with new frame types (Map, Set, Double, Boolean, Verbatim String, Big Number, Push), HELLO command for protocol negotiation, and client-side caching invalidation hints
**Depends on:** Phase 11 (thread-per-core needed for push notifications)
**Success Criteria** (what must be TRUE):
  1. HELLO command negotiates RESP2 or RESP3 per-connection with optional AUTH
  2. All RESP3 frame types parsed and serialized (Map, Set, Double, Boolean, Null, Verbatim String, Big Number, Push)
  3. Responses use native RESP3 types (e.g., HGETALL returns Map instead of flat Array)
  4. CLIENT TRACKING enables server-push invalidation messages for client-side caching
  5. RESP2 backward compatibility maintained (default protocol until HELLO negotiates RESP3)
  6. All existing RESP2 tests pass unchanged; new RESP3-specific tests added
**Plans:** 3/3 plans complete

Plans:
- [ ] 16-01-PLAN.md — RESP3 frame types: parser + serializer (Map, Set, Double, Boolean, VerbatimString, BigNumber, Push)
- [ ] 16-02-PLAN.md — HELLO command, protocol-aware codec, RESP3 response conversion
- [ ] 16-03-PLAN.md — CLIENT TRACKING with invalidation Push frame delivery

### Phase 17: Blocking Commands

**Goal:** Implement blocking list/sorted-set commands (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX) with fiber-based blocking, configurable timeout, and fair wakeup ordering when data arrives
**Depends on:** Phase 11 (fiber-per-connection model needed)
**Success Criteria** (what must be TRUE):
  1. BLPOP/BRPOP block the connection fiber until data available or timeout expires
  2. BLMOVE atomically pops from source and pushes to destination with blocking semantics
  3. BZPOPMIN/BZPOPMAX block until sorted set has elements or timeout
  4. Fair FIFO ordering: first client to block on a key gets served first when data arrives
  5. Timeout precision within 10ms; zero timeout means block indefinitely
  6. Blocked clients do not consume CPU (fiber yields to scheduler)
  7. Cross-shard blocking coordination for multi-key BLPOP (key may be on different shard)
**Plans:** 3/3 plans complete

Plans:
- [x] 17-01-PLAN.md — BlockingRegistry, LMOVE command, wakeup hooks
- [x] 17-02-PLAN.md — Blocking command handlers, shard timer, integration tests
- [ ] 17-03-PLAN.md — Cross-shard multi-key BLPOP coordinator (gap closure)

### Phase 18: Streams Data Type

**Goal:** Implement Redis Streams with radix tree + listpack storage, supporting XADD/XREAD/XRANGE/XLEN/XTRIM and consumer groups with XGROUP/XREADGROUP/XACK/XPENDING/XCLAIM
**Depends on:** Phase 15 (listpack encoding)
**Success Criteria** (what must be TRUE):
  1. Stream entries stored in radix tree keyed by entry ID (timestamp-sequence) with listpack leaf nodes
  2. XADD appends entries with auto-generated or explicit IDs; XLEN returns count; XTRIM caps stream length
  3. XRANGE/XREVRANGE support ID range queries with COUNT limit
  4. XREAD supports blocking reads from multiple streams with timeout (requires Phase 17 blocking infra)
  5. Consumer groups: XGROUP CREATE/DESTROY, XREADGROUP for consumer reads, XACK for acknowledgment
  6. XPENDING shows unacknowledged entries; XCLAIM transfers ownership of pending entries
  7. Stream persistence works with both RDB and AOF
**Plans:** 2/2 plans complete

Plans:
- [ ] 18-01-PLAN.md — Stream types, core commands (XADD/XLEN/XRANGE/XREVRANGE/XTRIM/XDEL/XREAD), RDB persistence
- [ ] 18-02-PLAN.md — Consumer groups (XGROUP/XREADGROUP/XACK/XPENDING/XCLAIM/XAUTOCLAIM/XINFO), XREAD BLOCK

### Phase 19: Replication with PSYNC2 Protocol

**Goal:** Implement PSYNC2-compatible master-replica replication with per-shard WAL streaming, partial resynchronization for brief disconnections, and full resynchronization via RDB transfer for new replicas
**Depends on:** Phase 14 (per-shard WAL required)
**Success Criteria** (what must be TRUE):
  1. REPLICAOF/SLAVEOF command initiates replication handshake with master
  2. Full resync: master sends RDB snapshot followed by backlog stream
  3. Partial resync: replica reconnects and catches up from per-shard replication backlog (circular buffer, default 1MB/shard)
  4. Per-shard replication: only catch up on shards that fell behind (vs Redis's single-backlog)
  5. Replica serves read-only queries during replication
  6. Replication offset tracked per-shard; PSYNC2 replication ID survives restarts
  7. INFO replication reports correct master/replica state and lag
**Plans:** 4/4 plans complete

Plans:
- [ ] 19-01-PLAN.md — ReplicationState, ReplicationBacklog, PSYNC2 decision logic, ID persistence
- [ ] 19-02-PLAN.md — Shard WAL fan-out integration, RegisterReplica/UnregisterReplica messages
- [ ] 19-03-PLAN.md — REPLICAOF/REPLCONF/SLAVEOF commands, READONLY enforcement, INFO replication, replica outbound task
- [ ] 19-04-PLAN.md — Full/partial resync orchestration, ReplicationState startup injection, WAIT command, integration tests

### Phase 20: Cluster Mode with Hash Slots and Gossip Protocol

**Goal:** Implement Redis Cluster compatibility with 16,384 hash slots, gossip-based cluster bus, MOVED/ASK redirections, live slot migration, and cluster-aware client routing
**Depends on:** Phase 19 (replication needed for cluster HA)
**Success Criteria** (what must be TRUE):
  1. 16,384 hash slots with CRC16(key) % 16384 mapping; hash tags ({tag}) ensure co-location
  2. Cluster bus on port+10000 with gossip protocol for node discovery and failure detection
  3. CLUSTER MEET/ADDSLOTS/DELSLOTS/INFO/NODES/SLOTS commands implemented
  4. MOVED redirection when client accesses key on wrong node
  5. ASK redirection during live slot migration; MIGRATING/IMPORTING states handled
  6. Slot migration: iterate slot keys, MIGRATE to target node, atomic slot ownership transfer
  7. Automatic failover: replica promotes to master when master detected as failed by majority
  8. Within a node, slots map to local shards: slot_id % num_local_shards
**Plans:** 4/4 plans complete

Plans:
- [x] 20-01-PLAN.md — ClusterState, ClusterNode, SlotRoute types, CRC16 hash slot engine (slot_for_key test vectors)
- [x] 20-02-PLAN.md — Gossip binary protocol serialization, cluster bus TCP listener on port+10000
- [x] 20-03-PLAN.md — CLUSTER command handlers, slot routing pre-dispatch (MOVED/ASK/ASKING), main.rs wiring
- [ ] 20-04-PLAN.md — nodes.conf persistence, GetKeysInSlot shard handler, failover vote machinery, integration tests

### Phase 21: Lua Scripting Engine

**Goal:** Embed Lua 5.4 scripting via mlua crate with EVAL/EVALSHA commands, SHA1-based script caching, sandboxed execution environment with Redis API bindings (redis.call, redis.pcall, redis.log)
**Depends on:** Phase 11 (scripts must execute atomically within a shard)
**Success Criteria** (what must be TRUE):
  1. EVAL executes Lua scripts with KEYS and ARGV arguments
  2. EVALSHA executes cached scripts by SHA1 hash; SCRIPT LOAD/EXISTS/FLUSH manage cache
  3. Sandbox: no file I/O, no os.execute, no require; only Redis API and safe stdlib functions
  4. redis.call() and redis.pcall() execute Redis commands within the script's shard context
  5. Scripts execute atomically (no other commands interleave on the same shard)
  6. Script timeout configurable (default 5s); SCRIPT KILL terminates long-running scripts
  7. Cross-shard scripts coordinate via VLL (keys must be declared upfront)
**Plans:** 2/2 plans complete

Plans:
- [ ] 21-01-PLAN.md -- Lua scripting module: ScriptCache, sandbox setup, type conversion, redis.call bridge, ScriptEngine orchestration
- [ ] 21-02-PLAN.md -- Shard wiring: ScriptLoad SPSC message, VM init in Shard::run, EVAL/EVALSHA/SCRIPT intercept in connection handler, integration tests

### Phase 22: ACL System and Per-User Permissions

**Goal:** Implement Redis 6+ ACL system with per-user command/key/channel permissions, replacing single-password AUTH with fine-grained access control
**Depends on:** Phase 16 (RESP3 needed for AUTH with username)
**Success Criteria** (what must be TRUE):
  1. ACL SETUSER creates/modifies users with password, command permissions (+@all, -@dangerous), key patterns (~*), channel patterns (&*)
  2. ACL GETUSER/LIST/DELUSER/WHOAMI for user management and introspection
  3. AUTH username password authenticates as specific user (RESP3 HELLO also supports auth)
  4. Default user maintains backward compatibility with single-password AUTH
  5. Command filtering: denied commands return NOPERM error
  6. Key pattern matching: users restricted to specific key patterns
  7. ACL LOG tracks denied command attempts for security monitoring
  8. ACL SAVE/LOAD persists ACL configuration to file
**Plans:** 3/3 plans complete

Plans:
- [ ] 22-01-PLAN.md — ACL data layer: AclTable, AclUser, rule parser, AclLog, file I/O
- [ ] 22-02-PLAN.md — Wire ACL into connection handlers: auth_acl, current_user, NOPERM gate, ACL commands
- [ ] 22-03-PLAN.md — Integration tests: all 8 ACL requirement IDs

### Phase 23: End-to-End Performance Validation and Benchmark Suite

**Goal:** Comprehensive performance validation of the fully reworked architecture against Redis 7.x and Redis Cluster using memtier_benchmark with Zipfian distribution, coordinated-omission-aware open-loop testing, and full throughput-latency curves on 64-core hardware
**Depends on:** Phase 22
**Success Criteria** (what must be TRUE):
  1. memtier_benchmark with Zipfian distribution (alpha 0.99), 256-byte values, 10M keys, 180s test duration, 5 iterations
  2. Throughput (no pipeline): >= 5M ops/sec on 64 cores (vs Redis Cluster ~5M)
  3. Throughput (pipeline=16): >= 12M ops/sec on 64 cores
  4. P99 latency: < 200us for GET/SET under 50% load
  5. Per-key memory overhead: <= 24 bytes (verified via INFO memory comparison with Redis)
  6. Memory during snapshot: < 5% spike (vs Redis's 2-3x)
  7. Open-loop (rate-limited) testing at 10/25/50/75/90% of peak, plotting full throughput-latency curve
  8. Comparison against properly-deployed Redis Cluster using same core count (fair comparison)
  9. Results published in BENCHMARK-v2.md with methodology, raw data, and analysis
**Plans:** 3/3 plans complete

Plans:
- [ ] 23-01-PLAN.md -- bench-v2.sh core: server lifecycle, pre-population, primary throughput test
- [ ] 23-02-PLAN.md -- Memory overhead, snapshot spike, and open-loop rate-limited suites
- [ ] 23-03-PLAN.md -- generate_report(), smoke-test run, BENCHMARK-v2.md with dev-hardware results

### Phase 24: Senior Engineer Review: Machine State Audit, Architecture Bottleneck Resolution, and Performance Tuning to Outperform Redis

**Goal:** Comprehensive senior-level audit of the entire rust-redis codebase — verify all 23 phases actually compile and integrate correctly as a unified system, resolve any architectural bottlenecks discovered through full-stack profiling, apply production-grade tuning (hot-path optimization, allocation reduction, syscall minimization), and validate that the system demonstrably outperforms single-instance Redis 7.x on available hardware
**Depends on:** Phase 23
**Success Criteria** (what must be TRUE):
  1. Full `cargo build --release` compiles cleanly (zero errors, minimal warnings)
  2. Full `cargo test` passes (all lib + integration tests green)
  3. Machine state audit: document all modules, dependencies, feature flags, binary size, dead code
  4. Hot-path profiling: identify and eliminate top 5 CPU bottlenecks via flamegraph analysis
  5. Allocation profiling: identify and eliminate unnecessary heap allocations on GET/SET fast path
  6. Syscall audit: minimize context switches per operation (batching, buffering, io_uring utilization)
  7. Lock contention audit: verify zero-contention on single-shard fast path, minimize cross-shard overhead
  8. End-to-end smoke benchmark on dev hardware: rust-redis outperforms single-instance Redis 7.x on GET/SET throughput at >= 10 concurrent clients
  9. All identified bottlenecks resolved or documented with clear rationale for deferral
  10. PERFORMANCE-AUDIT.md with findings, changes, before/after metrics
**Plans:** 4/4 plans complete

Plans:
- [x] 24-01-PLAN.md -- Fix listpack readonly path bug (6 test failures)
- [x] 24-02-PLAN.md -- Fix cluster port overflow + clean warnings + release profile
- [x] 24-03-PLAN.md -- Zero-copy GET optimization + full test validation
- [x] 24-04-PLAN.md -- Benchmark comparison and PERFORMANCE-AUDIT.md
### Phase 25: WAL v2 Format Hardening — checksums, header, block framing, corruption isolation

**Goal:** Harden the per-shard WAL with a versioned 32-byte binary header, block-level CRC32 checksums, and corruption isolation that stops replay on first bad block — replacing raw RESP concatenation with a structured format that detects bit-flips and torn writes
**Requirements**: WAL-V2-WRITE, WAL-V2-READ, WAL-V2-COMPAT
**Depends on:** Phase 24
**Plans:** 2/2 plans complete

Plans:
- [x] 25-01-PLAN.md — WAL v2 writer: header + block framing + CRC32 on flush
- [x] 25-02-PLAN.md — WAL v2 replay: v1/v2 auto-detection + CRC verification + corruption isolation

### Phase 26: Cluster Failover Completion — majority consensus, election task, bus handlers, FAILOVER command

**Goal:** Complete cluster failover: PFAIL->FAIL majority consensus, async election task with jittered delay and replica ranking, bus handler processing for FailoverAuthRequest/Ack, and CLUSTER FAILOVER [FORCE|TAKEOVER] command
**Requirements**: FAILOVER-01 through FAILOVER-07
**Depends on:** Phase 25
**Plans:** 3/3 plans complete

Plans:
- [x] 26-01-PLAN.md — Majority consensus for PFAIL->FAIL, election task with jittered delay, pfail_reports tracking
- [x] 26-02-PLAN.md — Bus handlers for FailoverAuthRequest/Ack, gossip ticker election trigger
- [x] 26-03-PLAN.md — CLUSTER FAILOVER [FORCE|TAKEOVER] command implementation

### Phase 27: CompactKey SSO for Key Storage — inline short keys, eliminate Bytes heap overhead

**Goal:** Replace Bytes key representation in DashTable with a 24-byte CompactKey struct that inlines short keys (<=23 bytes) to eliminate heap allocations and improve cache locality for >90% of real Redis keys
**Requirements**: COMPACTKEY-01, COMPACTKEY-02, COMPACTKEY-03
**Depends on:** Phase 26
**Success Criteria** (what must be TRUE):
  1. CompactKey struct is exactly 24 bytes with inline path for keys <= 23 bytes and heap path for longer keys
  2. DashTable uses CompactKey instead of Bytes as key type with identical lookup API
  3. All 1095+ existing tests pass with CompactKey integration
  4. No heap allocation for keys <= 23 bytes (verified by unit tests)
**Plans:** 2/2 plans complete

Plans:
- [x] 27-01-PLAN.md — CompactKey struct with SSO + DashTable key type swap
- [x] 27-02-PLAN.md — Wire CompactKey through Database, persistence, and all consumers
### Phase 28: Performance Tuning — response buffer pooling, io_uring registered buffers, adaptive pipeline batching

**Goal:** Reduce allocator pressure and kernel overhead on the io_uring send path via pre-registered buffer pools, batch command dispatch to minimize RefCell borrow overhead, and add connection buffer shrink guards
**Requirements**: PERF-28-01, PERF-28-02, PERF-28-03
**Depends on:** Phase 27
**Plans:** 2/2 plans complete

Plans:
- [x] 28-01-PLAN.md — Registered send buffer pool for io_uring WRITE_FIXED
- [x] 28-02-PLAN.md — Adaptive pipeline batching and connection buffer management

### Phase 29: Runtime Abstraction Layer — extract async runtime traits for Tokio/Glommio/Monoio swappability with DMA persistence path

**Goal:** Extract runtime-agnostic trait boundaries (RuntimeTimer, RuntimeSpawn, RuntimeFactory, FileIo) behind Cargo feature flags so a future runtime swap becomes mechanical file replacement rather than architectural rewrite
**Requirements**: RT-01, RT-02, RT-03, RT-04
**Depends on:** Phase 28
**Plans:** 2/2 plans complete

Plans:
- [x] 29-01-PLAN.md — Define runtime traits and Tokio implementations with feature flags
- [x] 29-02-PLAN.md — Wire traits into main.rs, wal.rs, and shard event loop
### Phase 30: Monoio Runtime Migration — implement Monoio backend for runtime traits, thread-per-core LocalExecutor, io_uring-native I/O, kqueue macOS fallback

**Goal:** Implement Monoio backend for all 6 runtime abstraction traits with compile-time feature selection and mutual exclusion
**Requirements**: MONOIO-IMPL
**Depends on:** Phase 29
**Plans:** 1/1 plans complete

Plans:
- [x] 30-01-PLAN.md — Implement Monoio runtime traits + feature flags

### Phase 31: Full Monoio Migration — replace Tokio channels, select!, TCP, codec, spawn across 30+ files for complete runtime swap

**Goal:** Replace all direct Tokio dependencies (channels, select!, TCP, codec, spawn, timers, signals, CancellationToken) with runtime-agnostic primitives and cfg-gated alternatives so the binary compiles and runs with `--features runtime-monoio` only
**Requirements**: MONOIO-FULL-01, MONOIO-FULL-02, MONOIO-FULL-03, MONOIO-FULL-04
**Depends on:** Phase 30
**Plans:** 4/4 plans complete

Plans:
- [x] 31-01-PLAN.md — Runtime-agnostic channel primitives (flume), CancellationToken, codec decoupling
- [x] 31-02-PLAN.md — Core server migration (main, shard, listener, connection, commands)
- [x] 31-03-PLAN.md — Subsystem migration (persistence, replication, cluster, blocking, pubsub, tracking, ACL)
- [x] 31-04-PLAN.md — Integration verification (dual-runtime compile, full test suite)

### Phase 32: Complete Monoio Runtime — replace tokio::select!, tokio::spawn, tokio::net TCP with monoio equivalents for functional Monoio binary

**Goal:** Functional Monoio binary that starts, accepts connections, and handles SET/GET/PING via redis-cli
**Requirements**: MONOIO-01, MONOIO-02, MONOIO-03, MONOIO-04, MONOIO-05, MONOIO-06
**Depends on:** Phase 31
**Plans:** 3/3 plans complete

Plans:
- [x] 32-01-PLAN.md — Monoio listener, signal handling, shard event loop select!
- [x] 32-02-PLAN.md — Monoio connection handler with ownership-based GAT I/O
- [x] 32-03-PLAN.md — Build verification and smoke test (SET/GET/PING)

### Phase 33: Monoio Multi-Shard and Persistence — cross-shard dispatch, connection distribution, AOF writer, WAL under monoio runtime

**Goal:** Functionally correct multi-shard monoio binary with cross-shard dispatch (MGET/MSET/DEL/KEYS/SCAN/DBSIZE), AOF logging, and WAL persistence
**Requirements**: MONOIO-SHARD-01, MONOIO-SHARD-02, MONOIO-SHARD-03, MONOIO-SHARD-04, MONOIO-SHARD-05
**Depends on:** Phase 32
**Plans:** 2/2 plans complete

Plans:
- [x] 33-01-PLAN.md — Upgrade monoio connection handler with cross-shard dispatch and AOF logging
- [x] 33-02-PLAN.md — Integration verification and human sign-off
### Phase 34: Monoio Pub/Sub, Blocking Commands, and Transactions — subscriber mode, BLPOP/BRPOP wakeup, MULTI/EXEC under monoio

**Goal:** Pub/Sub subscriber mode, blocking commands (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX), and MULTI/EXEC transactions all functional under monoio runtime via cfg-gated monoio::select!, monoio::time::sleep, and monoio::spawn
**Requirements**: MONOIO-PUBSUB-01, MONOIO-TX-01, MONOIO-TEST-01, MONOIO-BLOCK-01, MONOIO-BLOCK-02
**Depends on:** Phase 33
**Plans:** 2/2 plans complete

Plans:
- [x] 34-01-PLAN.md — Pub/Sub subscriber mode, MULTI/EXEC transactions, and test gating in monoio handler
- [x] 34-02-PLAN.md — Blocking commands (handle_blocking_command_monoio) with timeout and wakeup

### Phase 35: Monoio Replication and Cluster — PSYNC2 TCP streams, gossip bus, failover election under monoio runtime

**Goal:** Enable replication (PSYNC2 master/replica) and cluster mode (gossip bus, failover election) under the monoio runtime via cfg-gating, achieving full feature parity for the monoio binary
**Requirements**: MONOIO-REPL-01, MONOIO-REPL-02, MONOIO-CLUSTER-01, MONOIO-CLUSTER-02, MONOIO-CLUSTER-03
**Depends on:** Phase 34
**Plans:** 2/2 plans complete

Plans:
- [x] 35-01-PLAN.md — Monoio replication: master PSYNC handler + replica connection/handshake/streaming
- [x] 35-02-PLAN.md — Monoio cluster: bus listener, gossip ticker, failover election

### Phase 36: CPU and Memory Profiling vs Redis — flamegraph hot-path analysis, heap profiling, per-key overhead measurement, Tokio vs Monoio comparison

**Goal:** Create comprehensive profiling suite measuring and comparing CPU usage, memory efficiency (per-key overhead, RSS growth, fragmentation), and runtime comparison (Tokio vs Monoio) against Redis 8.6.1. Produces automated profile.sh and PROFILING-REPORT.md.
**Requirements**: PROF-01, PROF-02, PROF-03, PROF-04, PROF-05
**Depends on:** Phase 35
**Plans:** 2/2 plans complete

Plans:
- [x] 36-01-PLAN.md — CompactKey Criterion benchmark + profile.sh automated profiling script
- [x] 36-02-PLAN.md — Execute profiling suite and generate PROFILING-REPORT.md

### Phase 37: Quick-Win Optimizations — DashTable prefetch, branchless CompactKey eq, jemalloc default, no-Arc local responses (OPT-1 to OPT-4)

**Goal:** Apply 4 low-risk, high-ROI micro-optimizations: software prefetch in DashTable lookups (-8ns), branchless CompactKey equality (-5ns), jemalloc as default allocator (-35B/key RSS), and remove unnecessary Arc clone in cross-shard dispatch
**Requirements**: OPT-1, OPT-2, OPT-3, OPT-4
**Depends on:** Phase 36
**Plans:** 2/2 plans complete

Plans:
- [x] 37-01-PLAN.md — DashTable software prefetch (OPT-1) + branchless CompactKey equality (OPT-2)
- [x] 37-02-PLAN.md — jemalloc default allocator (OPT-3) + remove unnecessary Arc frame clone (OPT-4)
### Phase 38: Core Optimizations — direct GET serialization, zero-copy arg slicing, slab segment allocator (OPT-5 to OPT-7)

**Goal:** Eliminate 3 core hot-path bottlenecks: direct GET serialization via PreSerialized Frame variant (-24ns/GET), two-pass zero-copy RESP parsing via freeze+slice (-13ns/arg), and slab segment allocator for DashTable (-25B/key RSS)
**Requirements**: OPT-5, OPT-6, OPT-7
**Depends on:** Phase 37
**Plans:** 3/3 plans complete

Plans:
- [x] 38-01-PLAN.md — Zero-copy argument slicing: two-pass parse with freeze+zerocopy extraction (OPT-6)
- [x] 38-02-PLAN.md — Direct GET serialization: PreSerialized Frame variant skipping as_bytes_owned+Frame intermediary (OPT-5)
- [x] 38-03-PLAN.md — Slab segment allocator: contiguous Vec slabs replacing per-segment Box allocations (OPT-7)

### Phase 39: Advanced Optimizations — arena-allocated frames, writev zero-copy GET response (OPT-8 to OPT-9)

**Goal:** Eliminate Frame::Array heap allocation via SmallVec inline storage (-18ns/command) and extend writev zero-copy path for PreSerialized GET responses (-14ns/GET)
**Requirements**: OPT-8, OPT-9
**Depends on:** Phase 38
**Plans:** 2/2 plans complete

Plans:
- [x] 39-01-PLAN.md — SmallVec for Frame::Array/Set/Push inline storage (OPT-8)
- [x] 39-02-PLAN.md — Extend writev zero-copy path for PreSerialized responses (OPT-9)

### Phase 40: fix all P0, P1, P2

**Goal:** Fix 3 critical multi-shard performance issues: port pipeline batch dispatch to Monoio handler (P0), fix per-read allocation and channel capacity (P1), add connection stability improvements (P2)
**Requirements**: FIX-P0, FIX-P1, FIX-P2
**Depends on:** Phase 39
**Plans:** 2/2 plans complete

Plans:
- [x] 40-01-PLAN.md — Port pipeline batch dispatch from Tokio to Monoio handler (FIX-P0)
- [x] 40-02-PLAN.md — Buffer reuse, channel capacity increase, container pooling (FIX-P1, FIX-P2)
### Phase 41: Eliminate double memory copies and batch optimizations (P0 hot-path)

**Goal:** Eliminate all unnecessary memory copies and allocation overhead in the Monoio connection handler hot path — zero-copy write via Bytes::freeze(), optimized read path, batched borrow_mut, single refresh_now, pre-allocated frames, and MAX_BATCH cap
**Requirements**: MEMCPY-01, MEMCPY-02, MEMCPY-03, MEMCPY-04, MEMCPY-05, MEMCPY-06
**Depends on:** Phase 40
**Plans:** 2/2 plans complete

Plans:
- [x] 41-01-PLAN.md — Zero-copy write path (.freeze()), read path optimization, frames Vec pre-allocation
- [x] 41-02-PLAN.md — Hoist borrow_mut outside frame loop, single refresh_now, MAX_BATCH=1024 cap

### Phase 42: Inline dispatch for single-shard and missing Monoio features (P1)

**Goal:** Implement inline dispatch shortcut for single-shard GET/SET (bypassing Frame construction for ~30-50% p=1 improvement) and migrate all 12 missing Tokio features to Monoio handler for production parity (AUTH, ACL, RESP3, Cluster, Lua, Replication, BGSAVE, Client Tracking, PUBLISH)
**Requirements**: INLINE-DISPATCH-01, MONOIO-PARITY-01, MONOIO-PARITY-02, MONOIO-PARITY-03, MONOIO-PARITY-04, MONOIO-PARITY-05, MONOIO-PARITY-06, MONOIO-PARITY-07, MONOIO-PARITY-08, MONOIO-PARITY-09, MONOIO-PARITY-10, MONOIO-PARITY-11, MONOIO-PARITY-12
**Depends on:** Phase 41
**Plans:** 2/2 plans complete

Plans:
- [x] 42-01-PLAN.md — Inline dispatch fast path for single-shard GET/SET in Monoio handler
- [x] 42-02-PLAN.md — Migrate all 12 missing Tokio features to Monoio handler for production parity

### Phase 43: Security Hardening: TLS, ACL Bug Fixes, Protected Mode & Eviction Fixes

**Goal:** Production-ready security baseline — TLS 1.3 via rustls for encrypted client connections, fix all ACL enforcement bugs across sharded handlers, implement protected mode, and fix eviction to work in the production (sharded) code path.

**Requirements:**
- TLS 1.3 via rustls + aws-lc-rs: --tls-port/--tls-cert-file/--tls-key-file config, shard-side handshake, configurable cipher suites, optional mTLS
- ACL bug fixes: Wire requirepass into both sharded handlers (P0 auth bypass), enforce channel permissions in SUBSCRIBE/PSUBSCRIBE, fix aclfile loading in non-sharded mode, configurable acllog-max-len
- Protected mode: Reject non-loopback connections when no password/ACL set, --protected-mode config, startup warning
- Eviction fixes: Wire try_evict_if_needed into sharded handlers, fix LRU u16 clock wraparound, aggregate memory accounting, background eviction timer

**Depends on:** Phase 42
**Plans:** 3/3 plans complete

Plans:
- [ ] 43-01-PLAN.md — Fix P0 auth bypass, ACL channel enforcement, eviction wiring, LRU clock fix
- [x] 43-02-PLAN.md — Protected mode, aclfile loading fix, configurable acllog-max-len
- [ ] 43-03-PLAN.md — TLS 1.3 via rustls + aws-lc-rs with shard-side handshake
