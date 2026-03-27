# Project Research Summary

**Project:** Rust Redis
**Domain:** Redis-compatible in-memory key-value store
**Researched:** 2026-03-23
**Confidence:** HIGH

## Executive Summary

This project is a Redis-compatible in-memory key-value store written in Rust, with hand-written core data structures and protocol handling. The domain is well-understood: Redis is one of the most studied pieces of infrastructure software, and its architecture (single-threaded data path, async I/O, RESP protocol) maps cleanly to Rust's ownership model and the tokio async runtime. The recommended approach uses tokio for networking, `bytes` for zero-copy buffer management, jemalloc for allocation, and hand-written implementations for RESP parsing, all five Redis data types, expiration, and persistence. The stack is mature and battle-tested with high confidence across all technology choices.

The build order is dictated by clear architectural dependencies: protocol layer and storage layer are independent and should be built first, then composed through the command dispatch layer, then wrapped in networking. All five Redis data types (strings, lists, hashes, sets, sorted sets) share the same storage infrastructure, so the keyspace design must accommodate type-tagged entries from the start. Expiration is cross-cutting and must be wired into the storage engine early, not bolted on. Persistence (RDB/AOF) depends on stable data types and must come after all core types are implemented.

The primary risks are: (1) getting the tokio threading model wrong -- the single-threaded data owner with message passing pattern must be the architectural foundation, (2) zero-copy parsing lifetime infection -- use `Bytes`-based owned frames, not borrowed references, (3) naive expiration that only checks on access -- both lazy and active expiration are required from the start, and (4) fork-based RDB snapshots which are incompatible with tokio -- use COW data structures or incremental serialization instead. All four risks are well-documented with clear prevention strategies.

## Key Findings

### Recommended Stack

The stack centers on tokio (1.49) for async I/O, `bytes` (1.10) for zero-copy buffer management, and jemalloc for memory allocation. All core data structures and the RESP protocol parser are hand-written per project constraints. Supporting libraries are minimal: `hashbrown` for raw hash table API, `ahash` for fast hashing, `smallvec` and `compact_str` for allocation reduction, `thiserror` for structured errors, `tracing` for logging, `clap` for CLI, and `criterion` for benchmarking. Total production dependencies stay under 15 direct crates.

**Core technologies:**
- **tokio + tokio-util + bytes:** Async runtime, TCP, codec framing, zero-copy buffers -- the foundation of every async Rust network server
- **tikv-jemallocator:** Global allocator matching Redis's own choice -- 10-30% throughput improvement for this workload pattern
- **hashbrown + ahash:** Fast hash table with raw API access for custom implementations -- std HashMap uses hashbrown internally but the raw API enables Redis-specific optimizations
- **thiserror + tracing:** Error handling and structured logging -- standard Rust ecosystem choices with high confidence

**What NOT to use:** dashmap (concurrent locks unnecessary for single-threaded data), serde (RESP is simpler than JSON), async-std (stagnated), crossbeam/parking_lot (locks are architectural smell here), fork() (undefined behavior with tokio).

### Expected Features

**Must have (table stakes -- P1, ~30 commands):**
- RESP2 protocol parser/serializer -- everything depends on this
- TCP server with concurrent connections -- the network layer
- String commands (GET/SET/MGET/MSET/INCR family) -- covers 60-80% of Redis traffic
- Key management (DEL/EXISTS/EXPIRE/TTL/TYPE/KEYS) -- essential key lifecycle
- Lazy expiration on key access -- minimum viable TTL support
- Connection commands (PING/ECHO/SELECT/COMMAND) -- redis-cli compatibility

**Should have (P2, ~60 additional commands):**
- Hash, List, Set, Sorted Set commands -- the remaining four data types
- Active expiration (background sweeping) -- prevents memory bloat from lazy-only
- SCAN family -- safe production alternative to KEYS
- AUTH -- basic security

**Defer to v2+ (P3, ~30 additional commands):**
- RDB and AOF persistence -- requires stable data types first
- Pub/Sub, Transactions (MULTI/EXEC/WATCH), Blocking commands (BLPOP/BRPOP)
- LRU/LFU eviction, RESP3, memory-efficient encodings

**Anti-features (never build):** Cluster mode, Lua scripting, Streams, Modules API, Sentinel, full ACL system. These would each multiply project scope with diminishing returns.

### Architecture Approach

The architecture mirrors Redis: single-threaded data path with multi-threaded async I/O. Connection tasks run on tokio's thread pool handling TCP read/write and RESP parsing. Parsed commands flow via mpsc channel to a single data-owning task that executes them without locks, returning responses through oneshot channels. The recommended starting approach is `Arc<Mutex<Database>>` (using `std::sync::Mutex`, NOT `tokio::sync::Mutex`) for simplicity, migrating to channel-based single-owner if benchmarks show contention.

**Major components:**
1. **Protocol Layer** (protocol/) -- Zero-copy RESP2 parser/serializer using `Bytes`. Self-contained, no dependencies on other components. Heavily tested.
2. **Storage Layer** (storage/ + types/) -- KeySpace HashMap with type-tagged Entry values (RedisValue enum + TTL + access metadata). Hand-written data structures for each Redis type. Expiration and eviction are storage-level concerns.
3. **Command Layer** (command/) -- Pure functions: `Command + Database -> Response`. Each command family in its own module. Natural fit for AOF logging and testing.
4. **Server Layer** (server/) -- TCP listener, per-connection tasks, graceful shutdown. Bridges protocol parsing to command dispatch.
5. **Persistence Layer** (persistence/) -- RDB snapshots and AOF write logging. Operates on snapshots/logs, not live data. Background tasks via `spawn_blocking`.
6. **Pub/Sub Subsystem** (pubsub/) -- Channel registry with bounded subscriber delivery. Somewhat independent of other layers.

### Critical Pitfalls

1. **Blocking the event loop with O(N) commands** -- Instrument execution times from day one, build SCAN alongside KEYS, set 1ms time budgets per command. This is the most common failure mode in Redis clones.
2. **Lifetime infection from zero-copy RESP parsing** -- Use `Bytes`-based owned frames. If your `Frame` type has a lifetime parameter that propagates beyond the parser, you have gone too far. This is a Phase 1 decision that cannot be changed later.
3. **Lazy-only expiration causes unbounded memory growth** -- Implement both lazy AND active expiration from the start using Redis's probabilistic sampling (20 random keys, repeat if >25% expired, 1ms budget per cycle).
4. **Fork-based RDB snapshots are incompatible with tokio** -- Never use `fork()` in async Rust. Use COW data structures (`Arc`-wrapped values), incremental serialization, or implement AOF first and RDB as compaction.
5. **Wrong tokio threading model** -- If your `Database` requires `Arc<Mutex<>>` with `tokio::sync::Mutex`, the architecture is wrong. Use `std::sync::Mutex` for short critical sections or the channel-based actor pattern.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Protocol Foundation
**Rationale:** The RESP parser has zero dependencies on any other component and is the foundation everything else builds on. Getting this wrong (lifetime infection, edge case incompatibility) poisons the entire codebase. Build and exhaustively test in isolation.
**Delivers:** Complete RESP2 parser/serializer with zero-copy `Bytes`-based frames. Handles all frame types including null, empty, nested arrays, and inline commands.
**Addresses:** RESP2 protocol (P1 table stakes)
**Avoids:** Pitfall 2 (lifetime infection), Pitfall 9 (protocol edge cases), Pitfall 14 (not using Bytes)

### Phase 2: Storage Engine + String Commands
**Rationale:** The storage layer is the other zero-dependency foundation. Build the KeySpace, Entry type, RedisValue enum, and lazy expiration check. Then wire up through the command layer with string commands (GET/SET/INCR family) which are the simplest and most-used commands. This phase produces the first "working server" with redis-cli.
**Delivers:** TCP server accepting connections, command dispatch, string operations, basic key management (DEL/EXISTS/EXPIRE/TTL), lazy expiration, redis-cli compatibility stubs (PING/ECHO/SELECT/COMMAND/INFO).
**Addresses:** All P1 features (~30 commands)
**Avoids:** Pitfall 5 (wrong threading model), Pitfall 12 (command atomicity), Pitfall 13 (redis-cli compatibility)

### Phase 3: Remaining Data Types
**Rationale:** All four remaining data types (hash, list, set, sorted set) share the same storage infrastructure from Phase 2. Sorted sets are the most complex due to the dual skip-list + hashmap structure and should be tackled last in this phase. Active expiration belongs here because lazy-only causes memory bloat once datasets grow.
**Delivers:** Hash, List, Set, Sorted Set commands. Active expiration engine. SCAN family.
**Addresses:** All P2 features (~60 commands)
**Avoids:** Pitfall 3 (naive expiration), Pitfall 10 (sorted set performance cliff), Pitfall 1 (blocking event loop -- SCAN as safe KEYS alternative)

### Phase 4: Persistence
**Rationale:** Persistence depends on all data types being stable and serializable. AOF is architecturally simpler (append command log) and should come first. RDB requires consistent snapshots which need COW or incremental serialization. AOF rewrite is a separate complexity tier and should be deferred within this phase.
**Delivers:** AOF persistence (append + configurable fsync), RDB snapshots (background serialization), data survives restarts.
**Addresses:** RDB persistence, AOF persistence (P3 differentiators)
**Avoids:** Pitfall 4 (fork-based snapshots), Pitfall 8 (AOF rewrite complexity)

### Phase 5: Pub/Sub + Transactions
**Rationale:** Both features require client connection management infrastructure (subscriber tracking for Pub/Sub, command queuing for MULTI/EXEC). Pub/Sub is somewhat independent and can be built alongside transactions. Blocking commands (BLPOP/BRPOP) share the "wake blocked clients" infrastructure with Pub/Sub.
**Delivers:** SUBSCRIBE/PUBLISH, MULTI/EXEC/DISCARD/WATCH, BLPOP/BRPOP/BZPOPMIN/BZPOPMAX.
**Addresses:** Pub/Sub, Transactions, Blocking commands (P3 features)
**Avoids:** Pitfall 7 (Pub/Sub slow subscriber backpressure)

### Phase 6: Eviction + Memory Optimization
**Rationale:** LRU/LFU eviction requires per-key access metadata (already in Entry struct from Phase 2) and maxmemory configuration. Memory-efficient encodings (ziplist/listpack equivalents) are pure optimization that benefits from stable data type implementations. RESP3 is additive over RESP2.
**Delivers:** LRU eviction (approximated, sampled), LFU eviction, memory-efficient small-collection encodings, RESP3 protocol support, CONFIG GET/SET.
**Addresses:** Remaining P3 features
**Avoids:** Pitfall 6 (memory overhead), Pitfall 11 (incremental rehashing)

### Phase Ordering Rationale

- **Protocol and storage are independent** -- they can even be built in parallel. This is the key architectural insight from the research. They compose through the command layer.
- **String commands first** because they cover 60-80% of Redis traffic and validate the entire pipeline (parse -> dispatch -> execute -> respond) end-to-end with the simplest possible data type.
- **All data types before persistence** because RDB/AOF must serialize every type. Implementing persistence too early means rework when new types are added.
- **Pub/Sub and transactions after persistence** because persistence is higher value (data survives restarts) and these features add client connection management complexity.
- **Eviction and optimization last** because they are pure enhancements to already-working features. The system is fully functional without them.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 4 (Persistence):** RDB binary format, COW snapshot strategies in Rust without fork(), AOF rewrite concurrency model. This is where Redis's C architecture diverges most from idiomatic Rust.
- **Phase 5 (Pub/Sub + Transactions):** Blocking command client wake architecture, WATCH optimistic locking implementation. Fewer community examples to follow.
- **Phase 6 (Eviction + Optimization):** Memory-efficient encodings (Rust equivalents to ziplist/listpack), Morris counter for LFU. Novel territory for Rust.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Protocol):** RESP2 is extremely well-documented. Multiple Rust implementations exist as reference (redis-protocol.rs, mini-redis). Tokio framing tutorial covers the codec pattern.
- **Phase 2 (Storage + Strings):** Standard patterns. Tokio mini-redis is a direct reference implementation. HashMap + enum-tagged values is straightforward.
- **Phase 3 (Data Types):** Each data type maps to well-known Rust equivalents (VecDeque for lists, HashMap for hashes, HashSet for sets). Sorted set skip list is the only non-trivial piece but is well-documented.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All libraries verified on crates.io with current versions. tokio, bytes, hashbrown are industry-standard with hundreds of millions of downloads. No risky or niche dependencies. |
| Features | HIGH | Based on official Redis documentation and command reference. Feature prioritization validated against competitor analysis (Dragonfly, KeyDB, Valkey, mini-redis). Anti-features clearly justified. |
| Architecture | HIGH | Architecture mirrors Redis's own proven design. Validated against multiple sources including Redis internals documentation, Tokio tutorials, and mini-redis reference implementation. Build order derived from clear dependency analysis. |
| Pitfalls | HIGH | Pitfalls sourced from Redis official docs, production incident reports, Rust async ecosystem documentation, and community experience. Each pitfall includes concrete prevention strategies and detection methods. |

**Overall confidence:** HIGH

### Gaps to Address

- **RDB binary format details:** The exact RDB file format encoding (type bytes, length encoding, LZF compression) needs detailed specification during the persistence phase. The format is documented but complex.
- **COW snapshot strategy:** The specific mechanism for consistent RDB snapshots without fork() needs prototyping. Options include `Arc`-wrapped values, `im` crate persistent data structures, or fuzzy incremental serialization. Benchmarking needed to choose.
- **Skip list implementation specifics:** Redis's skip list stores span information for O(log n) rank operations. Whether to implement a full skip list or use `BTreeMap<(Score, Member), ()>` as a simpler alternative needs benchmarking during the sorted set implementation.
- **Windows compatibility:** jemalloc is not available on MSVC. If Windows support matters, mimalloc or the system allocator must be used as fallback. Not critical for initial development.
- **RESP3 negotiation:** The HELLO command and RESP3 type system (13+ types) need detailed specification when implementing in Phase 6. Not blocking for earlier phases.

## Sources

### Primary (HIGH confidence)
- [Redis Commands Reference](https://redis.io/docs/latest/commands/) -- complete command specification
- [Redis RESP Protocol Spec](https://redis.io/docs/latest/develop/reference/protocol-spec/) -- wire protocol format
- [Redis Persistence Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) -- RDB/AOF architecture
- [Redis Key Eviction Docs](https://redis.io/docs/latest/develop/reference/eviction/) -- LRU/LFU algorithms
- [tokio crate (crates.io)](https://crates.io/crates/tokio) -- v1.49.0 verified
- [bytes crate (lib.rs)](https://lib.rs/crates/bytes) -- v1.10.1 verified
- [Tokio Mini-Redis](https://github.com/tokio-rs/mini-redis) -- reference Rust implementation
- [Tokio Framing Tutorial](https://tokio.rs/tokio/tutorial/framing) -- codec pattern

### Secondary (MEDIUM confidence)
- [Redis Deep Dive: Architecture and Event Loop](https://thuva4.com/blog/part-1-the-core-of-redis/) -- internal architecture
- [Redis Single-Threaded I/O Model](https://oneuptime.com/blog/post/2026-01-25-redis-single-threaded-io-model/view) -- threading model
- [Implementing Copyless Redis Protocol in Rust](https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators/) -- zero-copy parsing approach
- [Redis Sorted Sets Internals](https://jothipn.github.io/2023/04/07/redis-sorted-set.html) -- skip list + hash map design
- [jemalloc vs mimalloc comparison](https://medium.com/@syntaxSavage/the-power-of-jemalloc-and-mimalloc-in-rust-and-when-to-use-them-820deb8996fe) -- allocator tradeoffs
- [Improving Key Expiration in Redis (Twitter Engineering)](https://blog.x.com/engineering/en_us/topics/infrastructure/2019/improving-key-expiration-in-redis) -- production expiration challenges

### Tertiary (LOW confidence)
- [Rust HashMap Overhead (ntietz)](https://ntietz.com/blog/rust-hashmap-overhead/) -- memory overhead analysis, single author
- [High Performance .NET: Building a Redis Clone (Ayende)](https://ayende.com/blog/197537-A/high-performance-net-building-a-redis-clone-the-wrong-optimization-path) -- cross-language insights, different ecosystem

---
*Research completed: 2026-03-23*
*Ready for roadmap: yes*
