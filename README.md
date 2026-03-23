# rust-redis

A high-performance Redis-compatible server written in Rust.

Built with Tokio for async I/O, jemalloc for memory efficiency, and a hand-rolled RESP2 parser. Supports 109 commands across 11 command families.

## Features

### Connection (7)
PING, ECHO, QUIT, SELECT, COMMAND, INFO, AUTH

### Strings (17)
GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX

### Keys (13)
DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST, TYPE, UNLINK, SCAN, KEYS, RENAME, RENAMENX

### Hashes (14)
HSET, HGET, HDEL, HMSET, HMGET, HGETALL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSETNX, HSCAN

### Lists (12)
LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LPOS

### Sets (15)
SADD, SREM, SMEMBERS, SCARD, SISMEMBER, SMISMEMBER, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SRANDMEMBER, SPOP, SSCAN

### Sorted Sets (18)
ZADD, ZREM, ZSCORE, ZCARD, ZINCRBY, ZRANK, ZREVRANK, ZPOPMIN, ZPOPMAX, ZSCAN, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZCOUNT, ZLEXCOUNT, ZUNIONSTORE, ZINTERSTORE

### Pub/Sub (4)
SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUBLISH

### Transactions (5)
MULTI, EXEC, DISCARD, WATCH, UNWATCH

### Persistence (2)
BGSAVE, BGREWRITEAOF

### Config (2)
CONFIG GET, CONFIG SET

## Quick Start

### Build from source

```bash
cargo build --release
./target/release/rust-redis
```

Then connect with any Redis client:

```bash
redis-cli
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
```

## Configuration

All options are available as command-line flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1` | Bind address |
| `--port` / `-p` | `6379` | Port to listen on |
| `--databases` | `16` | Number of databases |
| `--requirepass` | *(none)* | Require clients to authenticate with this password |
| `--appendonly` | `no` | Enable append-only file persistence (`yes`/`no`) |
| `--appendfsync` | `everysec` | AOF fsync policy (`always`/`everysec`/`no`) |
| `--appendfilename` | `appendonly.aof` | AOF filename |
| `--save` | *(none)* | RDB auto-save rules (e.g., `"3600 1 300 100"`) |
| `--dir` | `.` | Directory for persistence files |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |
| `--maxmemory` | `0` | Maximum memory in bytes (`0` = unlimited) |
| `--maxmemory-policy` | `noeviction` | Eviction policy when maxmemory is reached |
| `--maxmemory-samples` | `5` | Number of random keys to sample for eviction |

### Example

```bash
./target/release/rust-redis \
  --bind 0.0.0.0 \
  --port 6380 \
  --requirepass mysecret \
  --appendonly yes \
  --maxmemory 268435456 \
  --maxmemory-policy allkeys-lru
```

### Eviction Policies

| Policy | Description |
|--------|-------------|
| `noeviction` | Return errors when memory limit is reached |
| `allkeys-lru` | Evict least recently used keys |
| `allkeys-lfu` | Evict least frequently used keys |
| `allkeys-random` | Evict random keys |
| `volatile-lru` | Evict least recently used keys with an expiry set |
| `volatile-lfu` | Evict least frequently used keys with an expiry set |
| `volatile-random` | Evict random keys with an expiry set |
| `volatile-ttl` | Evict keys with the shortest TTL |

## Docker

### Build the image

```bash
docker build -t rust-redis .
```

### Run the container

```bash
docker run -p 6379:6379 rust-redis
```

### Run with persistence

```bash
docker run -p 6379:6379 -v redis-data:/data rust-redis \
  rust-redis --bind 0.0.0.0 --dir /data --appendonly yes
```

### Run with password

```bash
docker run -p 6379:6379 rust-redis \
  rust-redis --bind 0.0.0.0 --requirepass mysecret
```

## Architecture

- **Protocol:** RESP2 parser with both inline and multibulk support
- **Runtime:** Tokio async runtime with multi-threaded executor
- **Allocator:** jemalloc for reduced fragmentation
- **Storage:** Per-database `HashMap` with lazy expiry and active expiry cycles
- **Persistence:** Custom binary RDB snapshots + AOF with RESP wire format
- **Pub/Sub:** Lock-free publish via `tokio::sync::mpsc` channels
- **Transactions:** Optimistic concurrency control with WATCH versioning

## Known Limitations

- No cluster mode or sharding
- No Lua scripting
- No Redis Streams
- No RESP3 protocol support
- No replication (master/replica)
- No ACL (only single-password AUTH)
- No blocking commands (BLPOP, BRPOP, etc.)

## Testing

```bash
# Run all tests
cargo test

# Run with logging
RUST_LOG=rust_redis=debug cargo test

# Run benchmarks
cargo bench
```

## License

MIT
