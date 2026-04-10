# Migrating from Redis to Moon

Moon is a drop-in replacement for Redis. This guide covers how to migrate
existing Redis deployments to Moon with minimal disruption.

## Compatibility

Moon implements 200+ Redis commands across all core data types. See
[redis-compat.md](/docs/redis-compat.md) for the full compatibility matrix
and known differences.

**Supported:** Strings, Lists, Hashes, Sets, Sorted Sets, Streams, Pub/Sub,
Transactions, Lua scripting (EVAL/EVALSHA), ACLs, TLS, Replication (PSYNC2).

**Not supported:** Redis Modules API, Sentinel, Redis Functions, client-side
caching with invalidation tracking.

## Quick Migration (Single Instance)

### 1. Install Moon

```bash
# From release binary
curl -L https://github.com/user/moon/releases/latest/download/moon-linux-tokio -o moon
chmod +x moon

# Or build from source
cargo install --git https://github.com/user/moon --features runtime-tokio,jemalloc
```

### 2. Export Redis Data

```bash
# Create an RDB snapshot
redis-cli BGSAVE
# Wait for completion
redis-cli LASTSAVE

# Copy the dump file
cp /var/lib/redis/dump.rdb /var/lib/moon/
```

### 3. Start Moon with Existing Data

```bash
moon --port 6379 --dir /var/lib/moon --appendonly yes
```

Moon loads `dump.rdb` on startup and begins accepting commands immediately.

### 4. Verify

```bash
redis-cli -p 6379 PING         # PONG
redis-cli -p 6379 DBSIZE       # Should match Redis key count
redis-cli -p 6379 INFO server  # Verify moon_version
```

## Client Library Compatibility

All standard Redis client libraries work with Moon without code changes:

| Client | Language | Status |
|--------|----------|--------|
| redis-py | Python | Fully compatible |
| go-redis | Go | Fully compatible |
| redis-rs | Rust | Fully compatible |
| ioredis | Node.js | Fully compatible |
| jedis | Java | Fully compatible |
| lettuce | Java | Fully compatible |
| hiredis | C | Fully compatible |
| StackExchange.Redis | .NET | Fully compatible |

No code changes needed. Point your client at Moon's host:port.

## Configuration Mapping

| Redis Config | Moon Equivalent | Notes |
|-------------|----------------|-------|
| `port 6379` | `--port 6379` | Same default |
| `bind 0.0.0.0` | `--bind 0.0.0.0` | Same semantics |
| `requirepass secret` | `--requirepass secret` | Same AUTH flow |
| `appendonly yes` | `--appendonly yes` | Per-shard WAL (faster) |
| `appendfsync everysec` | `--appendfsync everysec` | Same 3 modes |
| `maxmemory 4gb` | `--maxmemory 4gb` | Same eviction |
| `maxmemory-policy allkeys-lru` | `--maxmemory-policy allkeys-lru` | Same policies |
| `save 900 1` | `--save "900 1"` | Same RDB rules |
| `tls-port 6443` | `--tls-port 6443` | rustls backend |
| `tls-cert-file` | `--tls-cert-file` | PEM format |
| `tls-key-file` | `--tls-key-file` | PEM format |

## Performance Differences

Moon typically delivers 1.5-2.5x higher throughput than Redis due to:

- **Thread-per-core architecture** with per-shard data partitioning
- **io_uring** on Linux (monoio runtime) for reduced syscall overhead
- **Per-shard WAL** eliminates the global AOF serialization bottleneck
- **HeapString SSO** reduces per-key memory overhead

Use `--shards N` to set the number of worker threads (default: CPU count).
For single-threaded parity with Redis, use `--shards 1`.

## Persistence Migration

### AOF

Moon's per-shard WAL is not file-compatible with Redis AOF. To migrate:

1. Start Moon with `--appendonly yes`
2. Load data from RDB snapshot
3. Moon begins writing its own WAL immediately

### RDB

Moon reads Redis RDB format on startup. No conversion needed.

## Rolling Migration (Zero Downtime)

For production deployments requiring zero downtime:

1. **Set up Moon as a replica** of your Redis primary:
   ```bash
   moon --port 6380 --replicaof redis-host 6379
   ```

2. **Wait for sync** — Moon performs a full PSYNC2 sync:
   ```bash
   redis-cli -p 6380 INFO replication
   # Wait for master_link_status:up and master_repl_offset to match
   ```

3. **Promote Moon** to primary:
   ```bash
   redis-cli -p 6380 REPLICAOF NO ONE
   ```

4. **Redirect clients** to Moon's port.

## Troubleshooting

### Command not supported

Check `docs/redis-compat.md` for the known incompatibility list. Most gaps
are in rarely-used commands (DEBUG, MODULE, ACL CAT subcategories).

### Memory usage differs

Moon uses different internal data structures (DashTable, HeapString).
Memory usage is typically lower for values >= 1KB, slightly higher for
many tiny keys. Use `INFO memory` to compare.

### TLS cipher mismatch

Moon uses rustls (not OpenSSL). The cipher suite names follow IANA naming.
Use `--tls-ciphersuites` to explicitly set the cipher list if your clients
require specific suites.
