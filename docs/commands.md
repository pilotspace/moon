---
title: "Command reference"
description: "All 250+ supported commands grouped by category."
---

# Command reference

Moon implements 250+ commands. Redis-compatible commands follow [Redis command semantics](https://redis.io/docs/latest/commands/). Moon-native commands (TXN, WS, MQ, TEMPORAL, FT.AGGREGATE) extend the protocol for cross-store transactions, workspaces, message queues, temporal queries, and full-text search.

## Connection (7)

`PING`, `ECHO`, `QUIT`, `SELECT`, `COMMAND`, `INFO`, `AUTH`

## Strings (18)

`GET`, `SET`, `MGET`, `MSET`, `MSETNX`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`

!!! tip
    SET supports `EX`, `PX`, `NX`, `XX`, and `KEEPTTL` options, matching Redis 7+ behavior.

## Keys (15)

`DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `TYPE`, `UNLINK`, `SCAN`, `KEYS`, `RENAME`, `RENAMENX`

## Hashes (25)

`HSET`, `HGET`, `HDEL`, `HMSET`, `HMGET`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HSETNX`, `HSCAN`, `HRANDFIELD`

**Per-field TTL (Valkey 9.0 / 9.1 parity):**

`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT` — set per-field TTL with optional `NX | XX | GT | LT` gate.
`HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL` — read per-field expiry / remaining time.
`HPERSIST` — remove per-field TTL.
`HGETDEL` — atomic get-and-delete.
`HGETEX key [EX | PX | EXAT | PXAT | PERSIST] FIELDS …` — atomic get with TTL update.

Per-field return code on the TTL commands: `-2` = no such field, `-1` = no TTL, `>=0` = absolute/remaining time or `1`/`2` outcome. `HSET` / `HMSET` clear per-field TTLs on overwrite; `HDEL` cleans up the TTL sidecar. The active-expiry tick reaps expired fields and auto-downgrades a TTL'd hash back to a plain hash once the last TTL is removed.

## Lists (16)

`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `BLPOP`, `BRPOP`, `BLMOVE`

!!! note
    Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`) support timeouts and cross-shard wakeup.

## Sets (15)

`SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SINTER`, `SUNION`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SRANDMEMBER`, `SPOP`, `SSCAN`

## Sorted sets (21)

`ZADD`, `ZREM`, `ZSCORE`, `ZCARD`, `ZINCRBY`, `ZRANK`, `ZREVRANK`, `ZPOPMIN`, `ZPOPMAX`, `ZSCAN`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZCOUNT`, `ZLEXCOUNT`, `ZUNIONSTORE`, `ZINTERSTORE`, `BZPOPMIN`, `BZPOPMAX`

## Geospatial (8)

`GEOADD`, `GEOPOS`, `GEODIST`, `GEOHASH`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS`, `GEORADIUSBYMEMBER`

!!! tip
    `GEOSEARCH` / `GEOSEARCHSTORE` (Redis 6.2+) supersede the older `GEORADIUS` / `GEORADIUSBYMEMBER` and support both radius (`BYRADIUS`) and bounding-box (`BYBOX`) queries. Geo sets are stored as sorted sets, so `Z*` commands also work on them.

## HyperLogLog (3)

`PFADD`, `PFCOUNT`, `PFMERGE`

!!! note
    Probabilistic cardinality estimation with a standard error of ~0.81%. `PFCOUNT` accepts multiple keys (union), and `PFMERGE` combines source HLLs into a destination.

## Streams (14)

`XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XTRIM`, `XDEL`, `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`, `XINFO`

## Pub/sub (5)

`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`

## Transactions (5)

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

## Cross-store transactions (3)

`TXN BEGIN`, `TXN COMMIT`, `TXN ABORT`

!!! note
    Cross-store transactions provide ACID semantics across KV, vector, and graph stores. `TXN BEGIN` starts a transaction, subsequent writes are buffered, and `TXN COMMIT` applies them atomically. `TXN ABORT` rolls back via undo-log replay. Cannot be mixed with `MULTI`/`EXEC`.

```
TXN BEGIN          -- Start a cross-store transaction
SET key value      -- Buffered in write intents
HSET doc:1 ...     -- Vector auto-index deferred until commit
GRAPH.ADDNODE ...  -- Graph write intent recorded
TXN COMMIT         -- Apply all changes atomically + WAL record
```

## Scripting (5)

`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`

!!! note
    Lua 5.4 VM via mlua. Sandboxed with `redis.call()` and `redis.pcall()` API bindings. The VM is lazy-initialized on first use to save ~18 MB baseline memory.

## Persistence (2)

`BGSAVE`, `BGREWRITEAOF`

## Replication (5)

`REPLICAOF`, `SLAVEOF`, `REPLCONF`, `PSYNC`, `WAIT`

## Cluster (9)

`CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER MEET`, `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER SETSLOT`, `CLUSTER FAILOVER`, `CLUSTER MYID`

## ACL (8)

`ACL SETUSER`, `ACL GETUSER`, `ACL DELUSER`, `ACL LIST`, `ACL WHOAMI`, `ACL LOG`, `ACL SAVE`, `ACL LOAD`

## Vector search (11)

`FT.CREATE`, `FT.DROPINDEX`, `FT.INFO`, `FT.SEARCH`, `FT.COMPACT`, `FT.RECOMMEND`, `FT.NAVIGATE`, `FT.EXPAND`, `FT.CACHESEARCH`, `FT.CONFIG SET`, `FT.CONFIG GET`

!!! tip
    Vectors are auto-indexed on `HSET`. Create an index with `FT.CREATE`, then `HSET` documents — Moon automatically inserts them into the HNSW graph. See the [vector search guide](vector-search-guide.md) for tuning parameters.

## Full-text search (2)

`FT.AGGREGATE`, `FT.SEARCH` (with BM25 text fields)

!!! note
    `FT.CREATE` supports `TEXT`, `TAG`, and `NUMERIC` field types alongside `VECTOR`. BM25 full-text search uses an inverted index with typo tolerance (Levenshtein fuzzy `%%term%%`), prefix search (`term*`), and three-way RRF hybrid fusion (BM25 + dense vector + sparse vector). `FT.AGGREGATE` provides `GROUPBY`/`REDUCE` with scatter-gather and HLL `COUNT_DISTINCT`.

## Workspaces (5)

`WS CREATE`, `WS DROP`, `WS AUTH`, `WS INFO`, `WS LIST`

!!! note
    Workspaces provide multi-tenant namespace isolation. After `WS AUTH`, all commands on the connection are transparently prefixed to the workspace's keyspace. Workspace names are max 64 bytes. IDs are UUID v7 (time-ordered).

```
WS CREATE myapp               -- Returns workspace UUID
WS AUTH <ws_id>                -- Bind this connection to the workspace
SET user:1 alice               -- Actually stored as {ws_hex}:user:1
GET user:1                     -- Transparently un-prefixed in response
WS LIST                        -- Enumerate all workspaces
WS INFO <ws_id>                -- Workspace statistics
WS DROP <ws_id>                -- Delete workspace and all its data
```

## Message queues (7)

`MQ CREATE`, `MQ PUSH`, `MQ POP`, `MQ ACK`, `MQ DLQLEN`, `MQ TRIGGER`, `MQ PUBLISH`

!!! note
    Durable message queues built on Redis Streams with at-least-once delivery. Failed messages are moved to a dead-letter queue after `MAXDELIVERY` attempts (default 3). `MQ TRIGGER` registers debounced callbacks. `MQ PUBLISH` is for transactional enqueue within a `TXN` block.

```
MQ CREATE orders MAXDELIVERY 5 DEBOUNCE 1000    -- Create durable queue
MQ PUSH orders action process item_id 42        -- Enqueue a message
MQ POP orders COUNT 3                           -- Claim up to 3 messages
MQ ACK orders 1713394800000-0                   -- Acknowledge by stream ID
MQ DLQLEN orders                                -- Dead-letter queue depth
MQ TRIGGER orders "PUBLISH events new" DEBOUNCE 2000  -- Register trigger
```

## Temporal (2)

`TEMPORAL.SNAPSHOT_AT`, `TEMPORAL.INVALIDATE`

!!! note
    Bi-temporal MVCC for point-in-time queries. `TEMPORAL.SNAPSHOT_AT` records a wall-clock → LSN binding. `TEMPORAL.INVALIDATE` sets `valid_to` on a graph entity. Use `FT.SEARCH ... AS_OF <timestamp>` for temporal vector queries and `GRAPH.QUERY ... VALID_AT <timestamp>` for temporal graph queries.

```
TEMPORAL.SNAPSHOT_AT                             -- Record current time → LSN
TEMPORAL.INVALIDATE 42 NODE mygraph              -- Expire node 42
FT.SEARCH idx "*=>[KNN 5 @v $q]" AS_OF 1713394800000  -- Search at point-in-time
GRAPH.QUERY social "MATCH (a) RETURN a" VALID_AT 1713394800000
```

## Graph (14)

`GRAPH.CREATE`, `GRAPH.DROP`, `GRAPH.ADDNODE`, `GRAPH.ADDEDGE`, `GRAPH.GETNODE`, `GRAPH.GETEDGE`, `GRAPH.DELNODE`, `GRAPH.DELEDGE`, `GRAPH.NEIGHBORS`, `GRAPH.DEGREE`, `GRAPH.SETPROP`, `GRAPH.QUERY`, `GRAPH.EXPLAIN`, `GRAPH.PROFILE`

!!! note
    Cypher subset: MATCH, WHERE, RETURN, CREATE, DELETE, SET, MERGE, WITH, ORDER BY, LIMIT, SKIP. Supports hybrid graph+vector queries via `EXPAND GRAPH` clause in `FT.SEARCH`.

Temporal-decay traversal scoring biases path finding toward recently created edges — the recency primitive for agent memory graphs:

```
GRAPH.QUERY g "MATCH p = shortestPath((a:P {name: 'A'})-[*..5]->(c:P {name: 'C'})) RETURN p" --decay 0.1
GRAPH.QUERY g "..." --decay 0.1 --time-weight 2.0   -- scale the age term
FT.NAVIGATE idx "*=>[KNN 5 @vec $q]" PARAMS 2 q <vec> HOPS 2 DECAY 0.1
```

`--decay <λ>` (1/seconds) adds `λ × edge_age_seconds` to each edge's traversal cost, so `shortestPath()` prefers fresh routes over stale ones at equal weight; the flag is read-only-query only (rejected on CREATE/SET/DELETE/MERGE). `FT.NAVIGATE ... DECAY <λ>` applies the same penalty to the discovery edge of each graph-expanded hit — a re-rank of the already-explored expansion, not a steer of the expansion itself. Decay off (no flag) keeps exact distance-only behavior. See the [temporal guide](guides/temporal.md) for details.

## Server (12)

`CONFIG GET`, `CONFIG SET`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `HELLO`, `CLIENT`, `OBJECT`, `DEBUG`, `SLOWLOG`, `WAIT`, `COMMAND DOCS`

## Known limitations

- `GETRANGE` / `SETRANGE` are not yet implemented.
