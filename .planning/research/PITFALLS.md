# Domain Pitfalls

**Domain:** Redis-compatible in-memory key-value store in Rust
**Researched:** 2026-03-23

## Critical Pitfalls

Mistakes that cause rewrites or major issues.

### Pitfall 1: Blocking the Event Loop with Expensive Commands

**What goes wrong:** A single-threaded data path means any slow operation (KEYS *, SORT on large sets, large DEL, serializing a huge hash) blocks ALL clients. This is the single most common failure mode in Redis clones. Developers underestimate how many operations become O(N) at scale and assume everything is "fast because it is in-memory."

**Why it happens:** During development with small datasets, every command completes in microseconds. The problem only surfaces under production-like data volumes (millions of keys, lists with 100K+ elements). Clone implementors often do not have Redis's battle-tested awareness of which commands are dangerous.

**Consequences:** Latency spikes from sub-millisecond to multi-second. All clients stall. Pub/Sub subscribers miss heartbeats. Expiration timers drift. The system appears "hung" under moderate load.

**Prevention:**
- Instrument every command handler with execution time tracking from day one
- Implement a slow-log (like Redis SLOWLOG) early, not as an afterthought
- For O(N) commands (KEYS, SMEMBERS on large sets), implement incremental/cursor-based alternatives (SCAN family) before shipping
- Set hard time budgets per command execution -- if a command takes >1ms, it needs a cursor/incremental variant
- Never iterate all keys in the main data path

**Detection:** Monitor P99 latency. If it spikes 100x over P50 intermittently, a blocking command is likely the cause.

**Phase relevance:** Core data structures phase. Build SCAN-family commands alongside the basic iteration commands, not later.

---

### Pitfall 2: Lifetime Hell in Zero-Copy RESP Parsing

**What goes wrong:** Attempting zero-copy parsing of the RESP protocol leads to complex lifetime annotations that infect the entire codebase. The parsed `Frame` type borrows from the input buffer, but the input buffer must be retained (and cannot be reused) until the command is fully processed. This creates a cascade: the command handler borrows from the frame, which borrows from the buffer, which borrows from the connection -- and suddenly nothing can be moved, stored, or sent across task boundaries.

**Why it happens:** The project description calls for "zero-copy where possible." In network protocol parsing, zero-copy means the parsed representation references the original byte buffer rather than owning copies. Rust's borrow checker enforces that the buffer cannot be freed or rewritten while references exist. For a request-response protocol where you read, parse, execute, and respond sequentially on one thread, this can work -- but the moment you need to store a parsed value (e.g., for Pub/Sub message routing), the lifetime constraints become unmanageable.

**Consequences:** Either (a) the entire codebase becomes lifetime-annotated soup that is painful to maintain and extend, (b) developers reach for `unsafe` to work around it (defeating Rust's safety guarantees), or (c) a late-stage rewrite to owned types negates the zero-copy work.

**Prevention:**
- Use `Bytes` (from the `bytes` crate) for the wire buffer -- it is reference-counted and cheaply cloneable, giving you "zero-copy-like" semantics without lifetime infection
- Parse RESP into an owned `Frame` enum (`Frame::Simple(String)`, `Frame::Bulk(Bytes)`, `Frame::Array(Vec<Frame>)`) -- the `Bytes` inside is still zero-copy from the network buffer because `Bytes` shares the underlying allocation
- Reserve true zero-copy (`&[u8]` references) only for the hot path of inline command dispatch where the reference does not escape the function scope
- Use `Cow<'a, [u8]>` only if profiling proves owned allocation is a bottleneck -- do not start with it

**Detection:** If your `Frame` type has a lifetime parameter that propagates to `Command`, `Connection`, and `Server`, you have gone too far.

**Phase relevance:** RESP protocol phase (Phase 1). This is a foundational decision that cannot easily be changed later.

---

### Pitfall 3: Naive Key Expiration Destroys Performance at Scale

**What goes wrong:** Implementing only lazy expiration (check TTL on access) causes expired keys to accumulate unboundedly when they are never accessed. Memory usage grows without limit. Implementing only active expiration (periodic full scan) blocks the event loop. Implementing a naive active expiration (scan ALL keys with TTLs) is O(N) and blocks the event loop at scale.

**Why it happens:** Lazy expiration is trivially easy to implement and works perfectly in tests. The problem is invisible until the dataset has millions of keys with TTLs set, many of which expire without being accessed (common in caching workloads). Clone implementors defer active expiration as "optimization" and ship with only lazy.

**Consequences:** With lazy-only: memory grows unboundedly, eviction policies are never triggered (keys are technically "expired" but still consuming memory), `DBSIZE` reports incorrect counts, `INFO memory` is misleading. With naive active: periodic latency spikes proportional to dataset size.

**Prevention:**
- Implement both lazy AND active expiration from the start
- For active expiration, use Redis's probabilistic sampling approach: randomly sample N keys with TTLs, delete expired ones, repeat if >25% were expired (with a time budget per cycle, e.g., 1ms)
- Maintain a count of keys with TTLs set to make sampling efficient
- Consider a sorted structure (min-heap or radix tree sorted by expiration time) for the expiration index -- this lets you efficiently find the soonest-to-expire keys without random sampling
- Set a per-cycle time budget (1ms max) for active expiration to avoid event loop stalls

**Detection:** Monitor `expired_keys` counter vs `keys_with_ttl` counter. If the ratio of expired/total grows over time, active expiration is not keeping up. Monitor memory usage -- if it grows despite TTLs being set, lazy-only expiration is likely.

**Phase relevance:** Key expiration phase. Must be implemented alongside TTL support, not deferred to a later optimization phase.

---

### Pitfall 4: Fork-Based RDB Snapshots Do Not Translate to Rust

**What goes wrong:** Redis uses `fork()` for RDB persistence -- the child process gets a copy-on-write view of the entire dataset and serializes it to disk while the parent continues serving. Rust clone implementors either (a) try to use `fork()` in Rust (extremely unsafe with tokio runtime, threads, and allocator state), (b) hold a read lock on the entire dataset during serialization (blocking writes for seconds to minutes), or (c) skip persistence entirely and defer it "to later" when the architecture makes it hard to retrofit.

**Why it happens:** Redis's C codebase uses `fork()` naturally because C has no runtime, no async executor, and no borrow checker. In Rust with tokio, `fork()` is undefined behavior if any threads exist (which tokio always has). The alternative -- consistent snapshots without fork -- requires deliberate architectural support.

**Consequences:** Using `fork()` with tokio leads to silent corruption, deadlocks in the allocator (jemalloc/system allocator may hold locks in the forked process), or crashes. Holding a lock during serialization makes the server unresponsive for large datasets. Skipping persistence means data loss on any restart.

**Prevention:**
- Do NOT use `fork()` in a tokio application. This is not negotiable.
- Design the data model to support incremental, consistent serialization from the start:
  - **Option A (recommended):** Use a copy-on-write data structure (e.g., `im` crate's persistent data structures, or manual COW with `Arc`) so a snapshot can read a frozen version while writes create new nodes
  - **Option B:** Implement a write-ahead log (AOF) first, and build RDB as an offline compaction of the AOF
  - **Option C:** Implement RDB serialization incrementally -- serialize one key at a time during idle event loop cycles, accepting that the snapshot is "fuzzy" (not a point-in-time snapshot)
- AOF is architecturally simpler to implement first: append each mutation command to a file. RDB can come later as an optimization for faster restarts.

**Detection:** If your RDB implementation plan includes the word "fork", stop and reconsider. If your serialization holds any lock for longer than a single key, it will not scale.

**Phase relevance:** Persistence phase. The data structure design in the core phase must accommodate snapshot-friendly patterns (e.g., `Arc`-wrapped values, COW collections).

---

### Pitfall 5: Getting the Tokio Threading Model Wrong

**What goes wrong:** Misunderstanding how to achieve Redis's "single-threaded data, multi-threaded I/O" model with tokio. Common mistakes: (a) using `tokio::spawn` for command handlers, which distributes them across worker threads and introduces data races or forces `Arc<Mutex<>>` on everything, (b) running everything on a single-threaded runtime and getting no I/O parallelism, (c) mixing blocking operations (disk I/O, serialization) into the async event loop.

**Why it happens:** Tokio's default multi-threaded runtime distributes spawned tasks across worker threads. Redis's model requires that all data mutations happen on one thread (no locks needed) while I/O read/write can happen on separate threads. This maps to a specific tokio pattern that is not the obvious default.

**Consequences:** With naive `Arc<Mutex<HashMap>>`: lock contention destroys throughput under concurrent load. Every command acquisition and release of the mutex serializes operations but with the overhead of atomic operations and potential contention. With single-threaded runtime only: I/O becomes a bottleneck because read/parse and write/serialize cannot overlap with command execution.

**Prevention:**
- Use a dedicated tokio task (or `LocalSet`) for the data/command execution loop
- Use an mpsc channel to funnel parsed commands from I/O tasks to the single data task
- I/O tasks (one per connection) run on the multi-threaded runtime: they read from TCP, parse RESP frames, send commands via channel, and write responses back
- The data task receives commands, executes them against the in-memory store (no locks needed -- single owner), and sends responses back via oneshot channels
- This is the "actor model" pattern: one actor owns the data, I/O tasks are message senders
- Use `tokio::task::spawn_blocking` for disk I/O (AOF writes, RDB serialization), never block the async runtime

**Detection:** If your `Db` struct requires `Arc<Mutex<>>` or `Arc<RwLock<>>`, your threading model is wrong for a Redis clone. If you see lock contention in profiling, the architecture needs restructuring, not a faster lock.

**Phase relevance:** TCP server phase (Phase 1). This is the foundational concurrency architecture. Getting it wrong means rewriting the entire command dispatch pipeline.

---

## Moderate Pitfalls

### Pitfall 6: Memory Overhead from Rust's Default Data Structures

**What goes wrong:** Using `std::collections::HashMap<String, Value>` for the main keyspace. Rust's `HashMap` has ~50 bytes of overhead per entry (control bytes, padding, capacity slack). `String` keys add 24 bytes (pointer + length + capacity) plus the allocation itself. For millions of small keys, this overhead dominates actual data storage. A dataset that is 1GB of "useful data" might consume 3-4GB of actual memory.

**Why it happens:** Rust's standard library data structures prioritize general correctness and safety over memory density. Redis uses custom data structures (ziplist/listpack for small collections, dict with incremental rehashing) specifically to minimize per-entry overhead. Clone implementors use `HashMap` and `Vec` because they are convenient, and the overhead is invisible until the dataset is large.

**Prevention:**
- Accept `std::collections::HashMap` for the initial implementation -- correctness first
- Plan for a custom hash table as an optimization milestone (incremental rehashing is important to avoid latency spikes during resize)
- Use `Bytes` instead of `String` for keys -- avoids the capacity field overhead and enables zero-copy from the network
- For small collections (Redis's ziplist equivalent), use a flat `Vec<(Key, Value)>` with linear scan -- this is faster than a HashMap for <64 entries due to cache locality, and uses far less memory
- Use `compact_str` or `smol_str` for small string values to avoid heap allocation for strings under ~22 bytes
- Consider `hashbrown::HashMap` (which `std` already uses internally) with a custom hasher (`ahash` is the default and fast, but `FxHasher` is faster for short keys)

**Prevention (allocator):**
- Switch to `jemalloc` or `mimalloc` as the global allocator early. The default system allocator on Linux (glibc malloc) fragments badly under Redis-like workloads (many small, short-lived allocations). jemalloc reduces fragmentation by 30-50% in these workloads.

**Detection:** Monitor RSS vs logical dataset size. If RSS is >3x the sum of key+value sizes, overhead is excessive. Use `jemalloc`'s profiling to identify allocation hotspots.

**Phase relevance:** Start with standard types (core data structures phase), optimize in a dedicated performance phase. But choose `Bytes` for keys/values and jemalloc from the beginning -- these are low-effort, high-impact choices.

---

### Pitfall 7: Pub/Sub Backpressure and Slow Subscribers

**What goes wrong:** A slow Pub/Sub subscriber backs up messages in an unbounded buffer, consuming unlimited memory. Eventually the server OOMs or the subscriber's connection becomes unusable due to massive message backlogs.

**Why it happens:** The naive implementation of Pub/Sub sends messages to a channel per subscriber. If the subscriber is slow (network congestion, slow client), messages accumulate. Redis itself handles this with output buffer limits (`client-output-buffer-limit pubsub`) and disconnects slow subscribers. Clone implementors often implement the "happy path" (publish to all subscribers) without implementing the "unhappy path" (what happens when a subscriber cannot keep up).

**Consequences:** One slow subscriber can cause the entire server to run out of memory. In production, this is a common cause of Redis OOM crashes.

**Prevention:**
- Use bounded channels (e.g., `tokio::sync::broadcast` with a fixed capacity) for Pub/Sub delivery
- When the channel is full, either drop oldest messages (lossy) or disconnect the subscriber (Redis's approach)
- Implement per-client output buffer tracking and limits
- Log warnings when subscribers fall behind, disconnect after a configurable threshold
- Test Pub/Sub with a subscriber that reads slowly or stops reading entirely

**Detection:** Monitor per-subscriber buffer sizes. If any subscriber's pending message count exceeds a threshold, it is falling behind.

**Phase relevance:** Pub/Sub phase. Implement backpressure from the start -- it is far harder to retrofit than to build in.

---

### Pitfall 8: AOF Rewrite Complexity Explosion

**What goes wrong:** The AOF file grows without bound as every write command is appended. Implementing AOF rewrite (compaction) is surprisingly complex: you need to generate a synthetic command stream representing the current dataset state while the server continues accepting writes, then atomically swap the old AOF for the new one, appending any commands that arrived during the rewrite.

**Why it happens:** Basic AOF append is trivially simple (serialize command, write to file, fsync). This gives a false sense of completion. But without rewrite, the AOF file grows to hundreds of GB and recovery takes hours. The rewrite process is where the real complexity lives -- it is essentially a concurrent snapshot problem (same challenge as RDB).

**Consequences:** Without rewrite: AOF file grows unboundedly, restart recovery time grows linearly with uptime. Naive rewrite: data loss or corruption if commands during rewrite are not captured. Blocking rewrite: server unresponsive for the duration.

**Prevention:**
- Implement basic AOF (append + fsync) first with configurable fsync policy (always, everysec, no)
- Defer AOF rewrite to a later phase -- it is genuinely complex
- When implementing rewrite:
  - Buffer new commands that arrive during rewrite to a separate "rewrite diff" buffer
  - Serialize current dataset state to a new AOF file
  - Append the buffered diff commands to the new file
  - Atomically rename the new file over the old one
- Consider hybrid AOF+RDB format (Redis 7+): the rewrite produces an RDB-format preamble followed by AOF-format tail, combining fast recovery with durability

**Detection:** Monitor AOF file size relative to dataset size. If the AOF is >10x the logical dataset size, rewrite is overdue.

**Phase relevance:** Persistence phase. Basic AOF is straightforward. AOF rewrite should be a separate milestone.

---

### Pitfall 9: Incorrect RESP Protocol Edge Cases Break Client Compatibility

**What goes wrong:** The RESP parser handles the common cases (simple strings, bulk strings, arrays) but mishandles edge cases, breaking compatibility with real Redis clients. Common failures: null bulk strings (`$-1\r\n`), null arrays (`*-1\r\n`), empty bulk strings (`$0\r\n\r\n`), nested arrays, inline commands (non-RESP plain text commands), and binary-safe bulk strings containing `\r\n`.

**Why it happens:** RESP is simple enough that a basic parser works for `SET`/`GET` in testing. But real clients (redis-cli, redis-py, jedis, ioredis) exercise the full protocol surface. Inline commands (typed directly in redis-cli) use a different format entirely. RESP3 adds maps, sets, doubles, booleans, and big numbers.

**Consequences:** redis-cli connects but breaks on certain commands. Client libraries fail mysteriously. Users report "works with our test client but not with redis-py." Debugging protocol-level incompatibilities is extremely time-consuming.

**Prevention:**
- Use the official RESP specification as the source of truth, not blog posts or simplified descriptions
- Implement a comprehensive RESP parser test suite covering: null bulk string, null array, empty bulk string, empty array, nested arrays, inline commands, binary data in bulk strings, integers (including negative), error responses
- Test with real clients early (redis-cli, redis-py, ioredis) -- do not rely solely on custom test clients
- Start with RESP2 only. Add RESP3 support later (it is substantially more complex with 13+ types)
- Handle the inline command format (`PING\r\n` without RESP framing) -- redis-cli uses this for the initial connection

**Detection:** If redis-cli works but a client library does not, or certain commands fail while others succeed, look at protocol edge cases first.

**Phase relevance:** RESP protocol phase (Phase 1). Get this right before building commands on top.

---

### Pitfall 10: Sorted Set Implementation Performance Cliff

**What goes wrong:** Sorted Sets require O(log N) insertion, deletion, and range queries by score, plus O(1) score lookup by member. This requires TWO data structures kept in sync: a skip list (or balanced tree) for score ordering, and a hash map for member-to-score lookup. Implementing only one (e.g., a BTreeMap) makes certain operations O(N) instead of O(log N) or O(1).

**Why it happens:** Redis uses a skip list + hash table combination for sorted sets. Rust's `BTreeMap` provides score ordering but not O(1) member lookup. Using only a `HashMap` provides member lookup but not range queries. Developers often start with one and discover the performance cliff when implementing `ZRANGEBYSCORE`, `ZRANK`, or `ZSCORE` on the "wrong" data structure.

**Consequences:** `ZSCORE` becomes O(log N) instead of O(1) if using only a skip list. `ZRANGEBYSCORE` becomes O(N) if using only a hash map. For leaderboard workloads with millions of members, this is the difference between <1ms and >100ms.

**Prevention:**
- Design Sorted Sets as a dual data structure from the start: `HashMap<Member, Score>` + `BTreeMap<(Score, Member), ()>` (or a custom skip list)
- Wrap both in a `SortedSet` struct that maintains consistency between the two
- The `BTreeMap<(Score, Member), ()>` approach works because Rust's BTreeMap provides efficient range queries and the `(Score, Member)` tuple gives deterministic ordering for equal scores
- A custom skip list is optional -- `BTreeMap` is a perfectly good substitute with similar O(log N) characteristics and better cache behavior for small-to-medium sets
- Test with both small (< 128 elements, where Redis uses a ziplist) and large sets

**Detection:** Benchmark `ZSCORE`, `ZRANK`, and `ZRANGEBYSCORE` independently at 1K, 100K, and 1M members. If any operation shows non-logarithmic scaling, the dual-structure is not working correctly.

**Phase relevance:** Core data structures phase. The dual-structure design must be the starting point, not an optimization.

---

## Minor Pitfalls

### Pitfall 11: Ignoring Incremental Rehashing

**What goes wrong:** Rust's `HashMap` resizes by allocating a new table and moving all entries at once. For a keyspace with millions of entries, this causes a multi-millisecond pause on the event loop during resize. Redis implements incremental rehashing: it maintains two hash tables during resize and migrates entries gradually (a few per command).

**Prevention:** Accept the standard HashMap resize behavior initially. Plan for a custom hash table with incremental rehashing as an optimization milestone. Consider `dashmap` or a custom approach. Monitor resize events as the dataset grows.

**Phase relevance:** Performance optimization phase (not initial implementation).

---

### Pitfall 12: Forgetting Command Atomicity Guarantees

**What goes wrong:** Commands like `MSET`, `RPOPLPUSH`, `GETSET` must be atomic (all-or-nothing). If the implementation processes sub-operations individually, concurrent clients can observe partial state. In a single-threaded model this is "free" (commands execute sequentially), but if the architecture accidentally introduces concurrency (e.g., yielding to the async runtime between sub-operations within a command), atomicity breaks.

**Prevention:** Never yield (`.await`) within a command handler that modifies multiple keys. Each command handler must be a synchronous function that runs to completion without suspension points. The single-threaded data task pattern (Pitfall 5) naturally provides this guarantee if followed correctly.

**Phase relevance:** TCP server / command execution phase. Architectural decision that must be correct from the start.

---

### Pitfall 13: Underestimating redis-cli Compatibility Surface

**What goes wrong:** redis-cli uses many implicit features beyond basic RESP: `AUTH` on connect, `CLIENT SETNAME`, `INFO` for display, `SELECT` for database switching, `COMMAND DOCS` for tab completion. If these are not implemented (even as stubs), redis-cli behaves poorly -- no tab completion, error messages on connect, missing metadata.

**Prevention:** Implement these "meta" commands as stubs early: `PING`, `INFO` (minimal), `CLIENT` (SETNAME, GETNAME, ID), `COMMAND` (COUNT, DOCS), `CONFIG` (GET), `SELECT`, `DBSIZE`, `FLUSHDB`. They do not need full functionality -- just enough to not break redis-cli's startup sequence.

**Phase relevance:** RESP/TCP server phase. Add stubs alongside the first working connection handler.

---

### Pitfall 14: Not Using Bytes Crate for Network Buffers

**What goes wrong:** Using `Vec<u8>` for network read/write buffers means every time you extract parsed data, you either copy it or fight with lifetimes. The `bytes` crate (`Bytes`, `BytesMut`) provides reference-counted, cheaply cloneable byte buffers that integrate directly with tokio's I/O traits.

**Prevention:** Use `BytesMut` for read buffers, `Bytes` for parsed values and shared data (e.g., Pub/Sub messages sent to multiple subscribers). This is the idiomatic tokio approach and mini-redis uses it for good reason.

**Phase relevance:** RESP protocol phase (Phase 1). Foundational choice.

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| RESP Protocol | Lifetime infection from zero-copy parsing (#2), edge case incompatibility (#9) | Use `Bytes`-based owned frames, test with real clients |
| TCP Server / Concurrency | Wrong threading model (#5), command atomicity (#12) | Actor model with mpsc channels, synchronous command handlers |
| Core Data Structures | HashMap memory overhead (#6), Sorted Set dual-structure (#10) | Start with std types + `Bytes` keys, plan optimization phase |
| Key Expiration | Lazy-only expiration (#3), naive active scan | Implement both from day one with probabilistic sampling |
| Persistence (AOF) | AOF rewrite complexity (#8) | Ship basic AOF first, defer rewrite to separate milestone |
| Persistence (RDB) | Fork-based snapshot (#4) | COW data structures or incremental serialization, never fork() |
| Pub/Sub | Slow subscriber OOM (#7) | Bounded channels with disconnect policy |
| Performance | Event loop blocking (#1), allocator fragmentation (#6), rehashing pauses (#11) | jemalloc from start, slow-log, incremental rehashing later |
| Client Compatibility | redis-cli startup (#13) | Implement meta-command stubs early |

## Sources

- [Redis Anti-Patterns](https://redis.io/tutorials/redis-anti-patterns-every-developer-should-avoid/)
- [Redis Persistence Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Key Eviction Docs](https://redis.io/docs/latest/develop/reference/eviction/)
- [Redis RESP Protocol Spec](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis Deep Dive: In-Memory Architecture and Event Loop](https://thuva4.com/blog/part-1-the-core-of-redis/)
- [Async Rust in Practice: Performance, Pitfalls, Profiling (ScyllaDB)](https://www.scylladb.com/2022/01/12/async-rust-in-practice-performance-pitfalls-profiling/)
- [Top 5 Tokio Runtime Mistakes](https://www.techbuddies.io/2026/03/21/top-5-tokio-runtime-mistakes-that-quietly-kill-your-async-rust/)
- [Tokio mini-redis](https://github.com/tokio-rs/mini-redis)
- [Zero-Copy in Rust: Challenges and Solutions](https://coinsbench.com/zero-copy-in-rust-challenges-and-solutions-c0d38a6468e9)
- [Rust: The joy of safe zero-copy parsers](https://itnext.io/rust-the-joy-of-safe-zero-copy-parsers-8c8581db8ab2)
- [Avoiding memory fragmentation in Rust with jemalloc](https://kerkour.com/rust-jemalloc)
- [Redis Persistence Dive Deep](https://engineeringatscale.substack.com/p/redis-persistence-aof-rdb-crash-recovery)
- [Redis Threading Model: Why "Single-Threaded" Is Misunderstood](https://dev.to/ricky512227/understanding-redis-threading-what-i-learned-the-hard-way-paf)
- [Improving Key Expiration in Redis (Twitter Engineering)](https://blog.x.com/engineering/en_us/topics/infrastructure/2019/improving-key-expiration-in-redis)
- [Dragonfly vs Valkey Architecture](https://www.dragonflydb.io/blog/dragonfly-vs-valkey-benchmark-on-google-cloud)
- [High Performance .NET: Building a Redis Clone (Ayende)](https://ayende.com/blog/197537-A/high-performance-net-building-a-redis-clone-the-wrong-optimization-path)
- [Rust HashMap Overhead (ntietz)](https://ntietz.com/blog/rust-hashmap-overhead/)
