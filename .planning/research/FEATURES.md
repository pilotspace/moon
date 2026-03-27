# Feature Research

**Domain:** Redis-compatible in-memory key-value database
**Researched:** 2026-03-23
**Confidence:** HIGH

## Feature Landscape

### Table Stakes (Users Expect These)

These are non-negotiable for any Redis-compatible server. Missing any of these means clients will break or users will immediately dismiss the project.

#### Protocol and Connectivity

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| RESP2 protocol parser/serializer | Every Redis client library speaks RESP2. Without it, nothing works. | MEDIUM | Must handle Simple Strings, Errors, Integers, Bulk Strings, Arrays. RESP2 is simpler than RESP3 and has wider client support. Start here. |
| TCP server with concurrent connections | Redis is a network server. Single-connection-only is useless. | MEDIUM | Tokio async runtime handles this well. Must support hundreds of concurrent connections. |
| PING/PONG | Every client uses PING for health checks and connection validation. | LOW | Trivial to implement but must exist from day one. |
| COMMAND/COMMAND DOCS | Many client libraries call COMMAND on connect to discover capabilities. | LOW | Can return minimal info, but must not error. redis-cli calls this on startup. |
| SELECT (database switching) | redis-cli and many clients assume 16 databases (0-15). | LOW | Simple namespace isolation. Most production use stays on db 0, but clients expect the command to work. |
| AUTH | Basic password authentication. Clients expect it even if optional. | LOW | Single password (requirepass style). ACL-based auth is out of scope. |
| INFO | Monitoring tools, client libraries, and redis-cli all call INFO. | MEDIUM | Return key sections: server, clients, memory, stats, keyspace. Does not need to be complete but must not crash. |
| ECHO, QUIT | Protocol basics that clients may use. | LOW | Trivial. |

#### String Commands (Most Used Data Type)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| GET / SET | The most fundamental Redis operations. ~60-80% of Redis traffic in typical deployments is GET/SET. | LOW | SET must support EX, PX, NX, XX options. |
| MGET / MSET | Batch operations. Critical for performance in real applications. | LOW | Simple extension of GET/SET over multiple keys. |
| INCR / DECR / INCRBY / DECRBY | Atomic counters are a primary Redis use case. | LOW | Must handle string-to-integer parsing and overflow. |
| INCRBYFLOAT | Float counters used in analytics, rate limiting. | LOW | Careful with float precision. |
| APPEND | String concatenation. Used in logging patterns. | LOW | |
| STRLEN | String length. | LOW | |
| SETNX / SETEX / PSETEX | Legacy commands still widely used despite SET options. | LOW | SETNX is the classic distributed lock primitive. |
| GETSET / GETDEL / GETEX | Atomic get-and-modify operations. | LOW | |

#### Key Management (Generic Commands)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| DEL | Delete keys. Universally used. | LOW | |
| EXISTS | Check key existence. Fundamental. | LOW | |
| EXPIRE / PEXPIRE / TTL / PTTL | Key expiration is a defining Redis feature. Every caching use case depends on it. | HIGH | Requires both lazy expiration (check on access) AND active expiration (background sweeping). This is architecturally significant. |
| PERSIST | Remove TTL from a key. | LOW | Depends on expiration infrastructure. |
| TYPE | Return the type of a key. Used by redis-cli and debugging tools. | LOW | |
| KEYS | Pattern matching over keyspace. | MEDIUM | O(N) and dangerous in production, but every Redis user expects it. Consider SCAN as the safe alternative. |
| SCAN / HSCAN / SSCAN / ZSCAN | Cursor-based iteration. The safe alternative to KEYS. | MEDIUM | Important for production use. Requires cursor state management. |
| RENAME / RENAMENX | Rename keys. | LOW | |
| UNLINK | Async delete (non-blocking DEL). | MEDIUM | Requires background cleanup thread/task. |
| OBJECT (ENCODING, REFCOUNT, IDLETIME) | Debugging and introspection. | LOW | |
| DUMP / RESTORE | Serialization. Lower priority but used by migration tools. | HIGH | Complex binary format. Defer if possible. |

#### Hash Commands

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| HSET / HGET / HDEL | Basic hash operations. Hashes are the second most used data type after strings. | LOW | HSET now supports multiple field-value pairs (since Redis 4.0). |
| HMSET / HMGET | Batch hash operations. Heavily used. | LOW | HMSET is technically deprecated (HSET does it now) but clients still send it. |
| HGETALL | Get all fields and values. Very commonly used. | LOW | |
| HEXISTS / HLEN | Check field existence and hash size. | LOW | |
| HKEYS / HVALS | Get all field names or values. | LOW | |
| HINCRBY / HINCRBYFLOAT | Atomic field increment. Used for counters within hashes. | LOW | |
| HSETNX | Set field only if not exists. | LOW | |

#### List Commands

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| LPUSH / RPUSH / LPOP / RPOP | Basic list operations. Lists are used as queues and stacks. | LOW | |
| LLEN / LRANGE / LINDEX | List inspection. LRANGE is heavily used. | LOW | |
| LSET / LINSERT | List modification. | LOW | |
| LREM | Remove elements by value. | MEDIUM | O(N) operation. |
| LTRIM | Trim list to range. Used with LPUSH for capped lists. | LOW | Common pattern: LPUSH + LTRIM for bounded queues. |
| BLPOP / BRPOP | Blocking list pops. Essential for queue patterns. | HIGH | Requires blocking client connection management. Architecturally complex: must wake blocked clients when data arrives. |
| LPOS | Find element position. | LOW | Added in Redis 6.0.6. |
| LMPOP / BLMPOP | Pop from multiple lists. | MEDIUM | |

#### Set Commands

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| SADD / SREM / SMEMBERS / SCARD | Basic set operations. | LOW | |
| SISMEMBER / SMISMEMBER | Membership check. Very common. | LOW | |
| SINTER / SUNION / SDIFF | Set algebra. A key differentiator of Redis sets. | MEDIUM | Must handle multiple input sets. |
| SINTERSTORE / SUNIONSTORE / SDIFFSTORE | Store set algebra results. | MEDIUM | Depends on set algebra. |
| SRANDMEMBER / SPOP | Random element retrieval. | LOW | |

#### Sorted Set Commands

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| ZADD / ZREM / ZSCORE / ZCARD | Basic sorted set operations. | MEDIUM | Sorted sets require a skip list + hash table dual structure. The data structure itself is the complexity. |
| ZRANGE / ZREVRANGE | Range queries by rank. The primary sorted set use case. | MEDIUM | Must support BYSCORE, BYLEX, REV, LIMIT options (Redis 6.2+ unified ZRANGE). |
| ZRANGEBYSCORE / ZREVRANGEBYSCORE | Range queries by score. Heavily used for time-series-like patterns. | MEDIUM | Legacy commands, but widely used. |
| ZRANK / ZREVRANK | Get rank of member. | LOW | |
| ZINCRBY | Increment score. Common in leaderboard patterns. | LOW | |
| ZCOUNT | Count members in score range. | LOW | |
| ZRANGEBYLEX / ZLEXCOUNT | Lexicographic range queries. | MEDIUM | Used for autocomplete patterns. |
| ZUNIONSTORE / ZINTERSTORE | Aggregate sorted sets. | MEDIUM | |
| ZPOPMIN / ZPOPMAX | Pop lowest/highest scored members. | LOW | |
| BZPOPMIN / BZPOPMAX | Blocking pop. | HIGH | Same blocking complexity as BLPOP/BRPOP. |

#### Pipelining

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Command pipelining | Clients send multiple commands without waiting for responses. Critical for performance. | LOW | Falls out naturally from RESP protocol handling if implemented correctly. Not a separate feature -- just don't require request-response lockstep. |

#### Key Expiration Engine

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Lazy expiration | Check TTL on key access and delete if expired. | MEDIUM | Must be checked on every key read/write operation. |
| Active expiration | Background task that probes random keys and deletes expired ones. | HIGH | Redis samples 20 random keys from the expiring set, deletes expired ones, repeats if >25% were expired. This is a core architectural component. |

### Differentiators (Competitive Advantage)

These features go beyond minimal compatibility and provide real value. They align with the project's core value of "fewer things, done exceptionally well."

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| RDB persistence (snapshots) | Allows data to survive restarts. Table stakes for production use but a differentiator vs toy clones. | HIGH | Requires fork-like mechanism or copy-on-write snapshot. In Rust, this means either forking the process (unix only) or implementing incremental snapshot with concurrent access. RDB file format is well-documented. |
| AOF persistence (append-only file) | Write durability. Combined with RDB gives production-grade persistence. | HIGH | Three fsync policies: always, everysec, no. AOF rewrite (compaction) is the hard part. Start with basic AOF logging, add rewrite later. |
| Pub/Sub messaging | Enables real-time messaging patterns. Many Redis users depend on basic pub/sub. | MEDIUM | Fire-and-forget model (at-most-once delivery). Must manage subscriber lists and pattern subscriptions. Not as hard as it sounds if you skip pattern matching initially. |
| Transactions (MULTI/EXEC/DISCARD/WATCH) | Atomic multi-command execution. Used for consistency-critical operations. | MEDIUM | MULTI/EXEC queues commands and executes atomically. WATCH adds optimistic locking (CAS). WATCH is the harder part. |
| LRU eviction | Memory-bounded operation. Without eviction, server crashes when memory fills. | MEDIUM | Redis uses approximated LRU (sample N random keys, evict the least recently used). Much simpler than true LRU. Use 24-bit access timestamp per key. |
| LFU eviction | Better cache hit rates than LRU for many workloads. | HIGH | Requires Morris counter (probabilistic frequency counter) + decay mechanism. More complex than LRU. Implement LRU first, add LFU as enhancement. |
| Memory-efficient data structure encodings | Rust's type system and memory control can produce tighter encodings than C Redis. | HIGH | Redis uses ziplist/listpack for small hashes/lists/sets, intset for small integer sets. This is where Rust can genuinely shine. Arena allocation, compact representations, zero-copy. |
| RESP3 protocol support | Modern protocol with better type support (maps, sets, booleans). | MEDIUM | Negotiate via HELLO command. Many newer clients prefer RESP3. Additive over RESP2, not a rewrite. |
| CONFIG GET/SET | Runtime configuration. Expected by ops tools. | MEDIUM | Start with a subset of config parameters (maxmemory, save intervals, etc). |
| WAIT | Synchronous replication acknowledgement. | HIGH | Only relevant if replication is implemented. Defer. |

### Anti-Features (Commonly Requested, Often Problematic)

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Cluster mode / sharding | Horizontal scaling is sexy on paper. | Massive complexity explosion: gossip protocol, hash slots, resharding, MOVED/ASK redirects, cross-slot command restrictions. Redis cluster took years to stabilize. This would triple the project scope. | Single-node focus. Vertical scaling. If users need sharding, they should use Redis/Valkey. |
| Lua scripting (EVAL/EVALSHA) | Server-side scripting enables atomic complex operations. | Embedding a Lua VM adds a large dependency and attack surface. Edge cases around script timeout, memory limits, and determinism are hard. | Transactions (MULTI/EXEC) cover most atomicity needs. Defer scripting indefinitely. |
| Redis Modules API | Extensibility framework. | Designing a stable plugin API is extremely hard. Module API changes break compatibility. The API surface is enormous. | Build core features natively. No plugin system for v1+. |
| Streams (XADD/XREAD/XGROUP) | Event streaming, consumer groups. | Streams are the most complex Redis data type. Consumer groups alone have ACK/CLAIM/PENDING semantics. This is essentially building a message broker. | Pub/Sub for simple messaging. If users need streams, they need Kafka or Redis. |
| Full ACL system | Enterprise security. | Complex: user management, command-level permissions, key patterns, pub/sub channel permissions. Disproportionate complexity vs value for a learning/hobby project. | Single-password AUTH is sufficient for v1. |
| Sentinel / High Availability | Automatic failover. | Requires leader election, monitoring, notification. This is a distributed systems problem layered on top of the database. | Out of scope. Single-node focus. |
| Client-side caching (tracking) | Performance optimization. | Requires server-side tracking of which keys each client has cached, invalidation messages, redirect mode. Complex protocol extension. | Not needed for v1. Clients handle their own caching. |
| Redis Functions | Newer scripting replacement for EVAL. | Even more complex than Lua scripting. Library management, function persistence. | Same as Lua scripting: defer indefinitely. |
| OBJECT HELP / DEBUG commands | Debugging internals. | Exposes implementation details, creates maintenance burden, potential security issues. | Implement INFO thoroughly instead. |
| WAIT / replication | Synchronous replication. | Requires implementing replication first, which is a large feature. | Single-node focus for v1. |

## Feature Dependencies

```
RESP2 Protocol Parser
    └──requires──> TCP Server
                       └──enables──> All Commands

String Commands (GET/SET)
    └──requires──> RESP2 Protocol Parser
    └──requires──> Key-Value Storage Engine

Key Expiration (EXPIRE/TTL)
    └──requires──> Key-Value Storage Engine
    └──requires──> Background Task Runner (active expiration)
    └──enhances──> All Data Type Commands

Hash Commands
    └──requires──> Key-Value Storage Engine
    └──requires──> RESP2 Protocol Parser

List Commands (basic)
    └──requires──> Key-Value Storage Engine

Blocking List Commands (BLPOP/BRPOP)
    └──requires──> List Commands (basic)
    └──requires──> Client Connection Manager (blocking/wake)

Sorted Set Commands
    └──requires──> Skip List + Hash Table Implementation
    └──requires──> Key-Value Storage Engine

Pub/Sub
    └──requires──> Client Connection Manager
    └──conflicts──> Single-threaded simplicity (subscribers occupy connections)

RDB Persistence
    └──requires──> All Data Type Serialization
    └──requires──> Snapshot Mechanism (fork or COW)

AOF Persistence
    └──requires──> Command Logging Infrastructure
    └──enhances──> RDB Persistence (hybrid mode)
    └──requires──> AOF Rewrite (for compaction, can defer)

Transactions (MULTI/EXEC)
    └──requires──> Command Queue per Connection
    └──enhances──> All Data Type Commands

WATCH (optimistic locking)
    └──requires──> Transactions (MULTI/EXEC)
    └──requires──> Key Modification Tracking

LRU Eviction
    └──requires──> Per-Key Access Timestamp
    └──requires──> maxmemory Configuration
    └──enhances──> Key-Value Storage Engine

LFU Eviction
    └──requires──> LRU Eviction Infrastructure
    └──requires──> Morris Counter Implementation

SCAN Cursor Iteration
    └──requires──> Key-Value Storage Engine
    └──enhances──> KEYS (safe replacement)

RESP3 Protocol
    └──requires──> RESP2 Protocol Parser
    └──requires──> HELLO Command
    └──enhances──> All Commands (richer type responses)

CONFIG GET/SET
    └──requires──> Runtime Configuration Store
    └──enhances──> LRU/LFU Eviction (maxmemory tuning)
    └──enhances──> Persistence (save interval tuning)
```

### Dependency Notes

- **Blocking commands (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX) require client connection management:** The server must track which clients are blocked on which keys and wake them when data arrives. This is architecturally significant and should be implemented as a cohesive system, not piecemeal.
- **Persistence requires all data types to be serializable:** RDB/AOF cannot be implemented until all supported data types have serialization/deserialization code. Plan persistence after data types are stable.
- **Expiration is cross-cutting:** It affects every command that reads or writes keys. The lazy expiration check must be wired into the storage engine early, not bolted on later.
- **Sorted sets require a skip list:** This is the most complex data structure in the project. It should be implemented and tested in isolation before integrating into the command layer.

## MVP Definition

### Launch With (v1 -- "It works with redis-cli")

Minimum viable product: a server that redis-cli can connect to and perform basic operations.

- [ ] RESP2 protocol parser/serializer -- foundation for everything
- [ ] TCP server with concurrent connections via Tokio -- the network layer
- [ ] Connection commands: PING, ECHO, QUIT, SELECT, COMMAND -- redis-cli compatibility
- [ ] String commands: GET, SET (with EX/PX/NX/XX), MGET, MSET, INCR, DECR, INCRBY, DECRBY -- covers 60%+ of Redis usage
- [ ] Key commands: DEL, EXISTS, EXPIRE, TTL, PEXPIRE, PTTL, PERSIST, TYPE, KEYS, RENAME -- essential key management
- [ ] Lazy expiration on key access -- minimum viable expiration
- [ ] INFO command (minimal) -- basic server introspection

### Add After Validation (v1.x -- "It handles real workloads")

Features to add once core string operations are solid and protocol is battle-tested.

- [ ] Hash commands (HSET, HGET, HDEL, HGETALL, HMGET, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY) -- when users need structured data
- [ ] List commands (LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM, LREM) -- when users need queues
- [ ] Set commands (SADD, SREM, SMEMBERS, SCARD, SISMEMBER, SINTER, SUNION, SDIFF) -- when users need unique collections
- [ ] Sorted set commands (ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZINCRBY, ZCARD, ZCOUNT) -- when users need leaderboards/rankings
- [ ] Active expiration (background sweeping) -- when lazy-only expiration causes memory bloat
- [ ] SCAN/HSCAN/SSCAN/ZSCAN -- safe iteration, replaces dangerous KEYS in production
- [ ] Pipelining verification -- should work naturally but needs testing
- [ ] UNLINK (async delete) -- for large key cleanup
- [ ] AUTH (single password) -- basic security

### Future Consideration (v2+ -- "It's production-ready")

Features to defer until data types and core engine are rock solid.

- [ ] RDB persistence -- requires snapshot mechanism, complex but essential for production
- [ ] AOF persistence -- requires command logging, fsync management
- [ ] Pub/Sub (SUBSCRIBE, PUBLISH, PSUBSCRIBE) -- messaging layer
- [ ] Transactions (MULTI, EXEC, DISCARD, WATCH) -- atomic operations
- [ ] Blocking commands (BLPOP, BRPOP, BZPOPMIN, BZPOPMAX) -- queue patterns
- [ ] LRU eviction -- memory-bounded operation
- [ ] LFU eviction -- better cache hit rates
- [ ] RESP3 protocol + HELLO command -- modern client support
- [ ] CONFIG GET/SET -- runtime configuration
- [ ] Memory-efficient encodings (ziplist/listpack equivalents) -- optimization
- [ ] OBJECT command -- introspection
- [ ] DUMP/RESTORE -- serialization for migration

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| RESP2 protocol | HIGH | MEDIUM | P1 |
| TCP server (concurrent) | HIGH | MEDIUM | P1 |
| String commands (GET/SET/INCR family) | HIGH | LOW | P1 |
| Key management (DEL/EXISTS/EXPIRE/TTL) | HIGH | LOW | P1 |
| Lazy expiration | HIGH | MEDIUM | P1 |
| Connection commands (PING/ECHO/SELECT) | HIGH | LOW | P1 |
| INFO command | MEDIUM | MEDIUM | P1 |
| Hash commands | HIGH | LOW | P2 |
| List commands (non-blocking) | HIGH | LOW | P2 |
| Set commands | MEDIUM | LOW | P2 |
| Sorted set commands | MEDIUM | HIGH | P2 |
| Active expiration | HIGH | MEDIUM | P2 |
| SCAN family | MEDIUM | MEDIUM | P2 |
| AUTH | MEDIUM | LOW | P2 |
| RDB persistence | HIGH | HIGH | P3 |
| AOF persistence | HIGH | HIGH | P3 |
| Pub/Sub | MEDIUM | MEDIUM | P3 |
| Transactions (MULTI/EXEC) | MEDIUM | MEDIUM | P3 |
| Blocking commands | MEDIUM | HIGH | P3 |
| LRU eviction | HIGH | MEDIUM | P3 |
| LFU eviction | MEDIUM | HIGH | P3 |
| RESP3 | LOW | MEDIUM | P3 |
| Memory-efficient encodings | MEDIUM | HIGH | P3 |

**Priority key:**
- P1: Must have for launch -- redis-cli works, basic string operations function
- P2: Should have -- all five data types work, server is usable for real workloads
- P3: Nice to have -- production-grade features (persistence, eviction, messaging)

## Competitor Feature Analysis

| Feature | Redis 7.x | Dragonfly | KeyDB | Valkey | mini-redis (Tokio) | Our Approach |
|---------|-----------|-----------|-------|--------|-------------------|--------------|
| String commands | Full (20+) | Full | Full | Full | GET/SET only | Full essential subset (~15 commands) |
| Hash commands | Full (18+) | Full | Full | Full | None | Full essential subset (~12 commands) |
| List commands | Full (15+) | Full | Full | Full | None | Core + blocking later |
| Set commands | Full (15+) | Full | Full | Full | None | Core + set algebra |
| Sorted set commands | Full (25+) | Full | Full | Full | None | Core subset (~15 commands) |
| Persistence (RDB) | Yes | Yes | Yes | Yes | No | Yes (v2) |
| Persistence (AOF) | Yes | Yes | Yes | Yes | No | Yes (v2) |
| Pub/Sub | Yes | Yes | Yes | Yes | Yes (basic) | Yes (v2, basic) |
| Transactions | Yes | Yes | Yes | Yes | No | Yes (v2, MULTI/EXEC/WATCH) |
| Streams | Yes | Yes | Yes | Yes | No | No (anti-feature) |
| Cluster | Yes | Yes (shared-nothing) | Yes | Yes | No | No (anti-feature) |
| Lua scripting | Yes | Yes | Yes | Yes | No | No (anti-feature) |
| Modules | Yes | Partial | Yes | Yes | No | No (anti-feature) |
| Threading model | Single-threaded + I/O threads | Multi-threaded (shared-nothing) | Multi-threaded | Single + I/O threads | Tokio async | Single-threaded data + Tokio I/O |
| Eviction | LRU/LFU + variants | LRU/LFU | LRU/LFU | LRU/LFU | No | LRU first, then LFU |
| Memory efficiency | Ziplist/listpack/intset | Novel encodings | Redis encodings | Redis encodings | HashMap only | Rust-native compact encodings |

## Redis Command Count by Priority

For reference, here is approximately how many commands fall into each priority tier:

- **P1 (MVP):** ~30 commands (strings, key management, connection)
- **P2 (All data types):** ~60 additional commands (hashes, lists, sets, sorted sets, scan)
- **P3 (Production features):** ~30 additional commands (pub/sub, transactions, config, persistence triggers)
- **Total target:** ~120 commands out of Redis's 400+ commands (covers ~95% of real-world usage)

## Sources

- [Redis Commands Reference](https://redis.io/docs/latest/commands/)
- [Redis Data Types](https://redis.io/docs/latest/develop/data-types/)
- [Redis Persistence Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Key Eviction](https://redis.io/docs/latest/develop/reference/eviction/)
- [Redis RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Dragonfly GitHub - Redis-compatible alternative](https://github.com/dragonflydb/dragonfly)
- [Tokio mini-redis - Minimal Rust Redis implementation](https://github.com/tokio-rs/mini-redis)
- [Redis Pub/Sub Documentation](https://redis.io/docs/latest/develop/pubsub/)
- [Redis Persistence Deep Dive - Trade-offs](https://engineeringatscale.substack.com/p/redis-persistence-aof-rdb-crash-recovery)
- [LFU vs LRU - Redis Blog](https://redis.io/blog/lfu-vs-lru-how-to-choose-the-right-cache-eviction-policy/)
- [Redis LRU Algorithm - Antirez](https://antirez.com/news/109)
- [Dragonfly Redis Alternative Comparison](https://www.dragonflydb.io/redis-alternative)
- [Redis Alternatives Comparison 2025](https://sliplane.io/blog/5-awesome-redis-alternatives-you-need-to-know-in-2025)

---
*Feature research for: Redis-compatible in-memory key-value database*
*Researched: 2026-03-23*
