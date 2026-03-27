# Moon

A high-performance, Redis-compatible in-memory data store written in Rust from scratch.

Moon implements 200+ Redis commands with a thread-per-core shared-nothing architecture, dual-runtime support (Tokio + Monoio), SIMD-accelerated parsing, forkless persistence, and memory-optimized data structures. It consistently outperforms Redis 8.x by **1.5-3x** on throughput while using **27-35% less memory** for real-world value sizes.

## Performance Highlights

Benchmarked against Redis 8.6.1 on Apple M4 Pro (co-located, `redis-benchmark`):

| Metric | Moon vs Redis | Conditions |
|--------|:------------:|------------|
| Peak GET throughput | **3.79M ops/sec** | 4 shards, pipeline=64 |
| Peak SET with AOF | **2.78M ops/sec** | AOF everysec, pipeline=64 |
| Throughput (pipeline=64) | **3.17x faster** | 1 shard, SET |
| Throughput (8 shards) | **1.84-1.99x faster** | GET/SET, pipeline=16 |
| With AOF persistence | **2.75x faster** | Per-shard WAL vs global fsync |
| Memory (1KB+ values) | **27-35% less** | Per-key RSS measurement |
| p50 latency (8 shards) | **8-10x lower** | 0.031ms vs 0.26ms |
| Data correctness | **132/132 tests** | All types, 1/4/12 shards |

See [BENCHMARK.md](BENCHMARK.md) for full results.

## Features

### Data Types
- **Strings** - GET, SET, MGET, MSET, INCR/DECR, APPEND, GETEX, GETDEL, and more
- **Lists** - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINSERT, LPOS, blocking BLPOP/BRPOP/BLMOVE
- **Hashes** - HSET, HGET, HGETALL, HINCRBY, HSCAN, and all hash operations
- **Sets** - SADD, SREM, SINTER, SUNION, SDIFF, SRANDMEMBER, SPOP, SSCAN
- **Sorted Sets** - ZADD, ZRANGE, ZRANGEBYSCORE, ZRANK, ZINCRBY, ZPOPMIN/MAX, blocking BZPOPMIN/MAX
- **Streams** - XADD, XREAD, XRANGE, XLEN, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM

### Architecture
- **Thread-per-core** shared-nothing design with per-shard event loops
- **Dual runtime** - Tokio (all platforms) + Monoio (Linux io_uring / macOS kqueue)
- **DashTable** - Segmented hash table with Swiss Table SIMD probing
- **SIMD parsing** - memchr-accelerated CRLF scanning, atoi fast integer parsing
- **Lock-free channels** - Custom oneshot channels replacing tokio::oneshot (12% CPU reduction)

### Persistence
- **RDB snapshots** - Forkless compartmentalized snapshots (no COW memory spike)
- **AOF** - Per-shard WAL with batched fsync, configurable everysec/always/no
- **WAL v2** - Checksums, block framing, corruption isolation

### Networking & Protocol
- **RESP2/RESP3** - Full protocol support with HELLO negotiation
- **TLS 1.3** - Via rustls + aws-lc-rs, dual-port (plaintext + TLS), mTLS support
- **Pipelining** - Adaptive batch dispatch with response freezing
- **Client-side caching** - Invalidation hints via RESP3 Push frames

### Clustering & Replication
- **Replication** - PSYNC2-compatible, per-shard WAL streaming, partial resync
- **Cluster mode** - 16,384 hash slots, gossip protocol, MOVED/ASK redirections, live slot migration
- **Failover** - Majority consensus election, automatic promotion

### Scripting & Security
- **Lua scripting** - Embedded Lua 5.4 via mlua, EVAL/EVALSHA, sandboxed with Redis API bindings
- **ACL system** - Per-user permissions, command/key/channel restrictions
- **Protected mode** - Rejects non-loopback connections when no password is set

### Memory Optimization
- **CompactKey** - 23-byte inline SSO, eliminates heap allocation for short keys
- **HeapString** - No Arc overhead for non-shared values
- **CompactValue** - 16-byte SSO struct with embedded TTL delta
- **B+ tree sorted sets** - Cache-friendly replacement for BTreeMap
- **Arena allocation** - Per-request bumpalo arenas, per-connection reuse

## Quick Start

### Install from source

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
cargo build --release
```

### Run

```bash
# Default: binds to 127.0.0.1:6379, auto-detects CPU count for shards
./target/release/rust-redis

# With specific options
./target/release/rust-redis --port 6380 --shards 4 --requirepass mysecret
```

### Connect

Any Redis client works out of the box:

```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
127.0.0.1:6379> HSET user:1 name "Alice" age 30
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"
```

### Docker

```bash
# Build
docker build -t moon .

# Run
docker run -p 6379:6379 moon

# Run with persistence
docker run -p 6379:6379 -v moon-data:/data moon \
  rust-redis --bind 0.0.0.0 --dir /data --appendonly yes

# Run with password and TLS
docker run -p 6379:6379 -p 6380:6380 moon \
  rust-redis --bind 0.0.0.0 --requirepass secret \
  --tls-port 6380 --tls-cert-file /certs/server.crt --tls-key-file /certs/server.key
```

## Configuration

All options are available as command-line flags:

### Server

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1` | Bind address |
| `--port` / `-p` | `6379` | Port to listen on |
| `--shards` | `0` (auto) | Number of shards (0 = CPU count) |
| `--databases` | `16` | Number of databases |
| `--requirepass` | *(none)* | Require password authentication |
| `--protected-mode` | `yes` | Reject non-loopback when no password set |

### Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `--appendonly` | `no` | Enable AOF persistence (`yes`/`no`) |
| `--appendfsync` | `everysec` | AOF fsync policy (`always`/`everysec`/`no`) |
| `--appendfilename` | `appendonly.aof` | AOF filename |
| `--save` | *(none)* | RDB auto-save rules (e.g., `"3600 1 300 100"`) |
| `--dir` | `.` | Directory for persistence files |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |

### Memory & Eviction

| Flag | Default | Description |
|------|---------|-------------|
| `--maxmemory` | `0` | Max memory in bytes (0 = unlimited) |
| `--maxmemory-policy` | `noeviction` | Eviction policy |
| `--maxmemory-samples` | `5` | Keys to sample for eviction |

**Eviction policies:** `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`

### TLS

| Flag | Default | Description |
|------|---------|-------------|
| `--tls-port` | `0` (disabled) | TLS listener port |
| `--tls-cert-file` | *(none)* | PEM certificate file |
| `--tls-key-file` | *(none)* | PEM private key file |
| `--tls-ca-cert-file` | *(none)* | CA cert for mTLS client auth |
| `--tls-ciphersuites` | *(default)* | TLS 1.3 cipher suites |

### Cluster

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-enabled` | `false` | Enable cluster mode |
| `--cluster-node-timeout` | `15000` | Node timeout in ms |

### ACL

| Flag | Default | Description |
|------|---------|-------------|
| `--aclfile` | *(none)* | Path to ACL file (Redis-compatible format) |
| `--acllog-max-len` | `128` | Max ACL log entries |

### Example: Production Configuration

```bash
./target/release/rust-redis \
  --bind 0.0.0.0 \
  --port 6379 \
  --tls-port 6380 \
  --tls-cert-file /etc/moon/server.crt \
  --tls-key-file /etc/moon/server.key \
  --shards 8 \
  --requirepass "$REDIS_PASSWORD" \
  --appendonly yes \
  --appendfsync everysec \
  --dir /var/lib/moon \
  --maxmemory 8589934592 \
  --maxmemory-policy allkeys-lfu \
  --aclfile /etc/moon/users.acl
```

## Architecture

```
                    Client Connections
                           |
                    TCP / TLS Listener
                           |
                  ┌────────┴────────┐
                  │  Shard Router    │  (hash(key) % N)
                  └────────┬────────┘
           ┌───────┬───────┼───────┬───────┐
        Shard 0  Shard 1  ...   Shard N-1
           │       │               │
      ┌────┴────┐  │          ┌────┴────┐
      │DashTable│  │          │DashTable│  Swiss Table SIMD
      │  (data) │  │          │  (data) │
      └────┬────┘  │          └────┬────┘
           │       │               │
        Per-Shard WAL          Per-Shard WAL   (batched fsync)
```

Each shard runs on its own thread with:
- Independent event loop (Tokio `current_thread` or Monoio `LocalExecutor`)
- Own DashTable with segmented hash table and SIMD probing
- Own WAL writer for persistence (no global lock)
- Own PubSub registry with cross-shard fan-out via SPSC channels
- Own Lua VM instance for script execution

**Key design choices:**
- **No shared mutable state** between shards — all cross-shard communication via message passing
- **Forkless snapshots** — iterate DashTable segments asynchronously, no COW memory spike
- **CompactKey SSO** — keys up to 23 bytes stored inline (no heap allocation)
- **Lock-free oneshot** — custom channels replace tokio::oneshot for 12% CPU reduction
- **CachedClock** — thread-local timestamp cache avoids syscall per operation

## Benchmarking

```bash
# Quick throughput comparison vs Redis
./scripts/bench-production.sh

# Memory efficiency benchmark
./scripts/bench-resources.sh

# Cargo micro-benchmarks
cargo bench

# Run data consistency tests (132 tests across 1/4/12 shard configs)
./scripts/test-consistency.sh
```

## Testing

```bash
# Unit tests
cargo test --lib

# With logging
RUST_LOG=rust_redis=debug cargo test --lib

# Benchmarks
RUSTFLAGS="-C target-cpu=native" cargo bench
```

## Command Reference

<details>
<summary><strong>200+ supported commands</strong> (click to expand)</summary>

### Connection (7)
PING, ECHO, QUIT, SELECT, COMMAND, INFO, AUTH

### Strings (17)
GET, SET, MGET, MSET, MSETNX, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX

### Keys (13)
DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, UNLINK, SCAN, KEYS, RENAME, RENAMENX

### Hashes (14)
HSET, HGET, HDEL, HMSET, HMGET, HGETALL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSETNX, HSCAN

### Lists (15)
LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LPOS, LMOVE, BLPOP, BRPOP, BLMOVE

### Sets (15)
SADD, SREM, SMEMBERS, SCARD, SISMEMBER, SMISMEMBER, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SRANDMEMBER, SPOP, SSCAN

### Sorted Sets (20)
ZADD, ZREM, ZSCORE, ZCARD, ZINCRBY, ZRANK, ZREVRANK, ZPOPMIN, ZPOPMAX, ZSCAN, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZCOUNT, ZLEXCOUNT, ZUNIONSTORE, ZINTERSTORE, BZPOPMIN, BZPOPMAX

### Streams (13)
XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO

### Pub/Sub (5)
SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH

### Transactions (5)
MULTI, EXEC, DISCARD, WATCH, UNWATCH

### Scripting (4)
EVAL, EVALSHA, SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH

### Persistence (2)
BGSAVE, BGREWRITEAOF

### Replication (4)
REPLICAOF, SLAVEOF, REPLCONF, PSYNC, WAIT

### Cluster (7)
CLUSTER INFO, CLUSTER NODES, CLUSTER SLOTS, CLUSTER MEET, CLUSTER ADDSLOTS, CLUSTER DELSLOTS, CLUSTER SETSLOT, CLUSTER FAILOVER, CLUSTER MYID

### ACL (7)
ACL SETUSER, ACL GETUSER, ACL DELUSER, ACL LIST, ACL WHOAMI, ACL LOG, ACL SAVE, ACL LOAD

### Server (6)
CONFIG GET, CONFIG SET, DBSIZE, FLUSHDB, FLUSHALL, HELLO, CLIENT, OBJECT, DEBUG, SLOWLOG, WAIT, COMMAND DOCS

</details>

## Project Structure

```
src/
  main.rs              # Entry point, CLI args, server bootstrap
  protocol/            # RESP2/RESP3 parser, serializer, codec
  server/              # TCP listener, connection handler, shard router
  storage/             # DashTable, CompactKey, CompactValue, expiration, eviction
  persistence/         # RDB snapshots, AOF writer, WAL v2
  shard/               # Per-shard event loop, message dispatch
  cluster/             # Hash slots, gossip protocol, failover
  replication/         # PSYNC2, backlog, replica streaming
  scripting/           # Lua VM, script cache, Redis API bridge
  security/            # ACL, TLS, protected mode
  runtime/             # Runtime abstraction (Tokio/Monoio traits)
  io/                  # io_uring driver, buffer management
```

## License

MIT
