---
title: "Configuration"
description: "All command-line flags and configuration options for Moon."
---

# Configuration

All options are available as command-line flags. Run `moon --help` for the full list.

## Server

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1` | Bind address |
| `--port` / `-p` | `6379` | Port to listen on |
| `--shards` | `1` | Number of shards (`0` = auto-detect CPU count) |
| `--databases` | `16` | Number of databases |
| `--requirepass` | *(none)* | Require password authentication |
| `--protected-mode` | `yes` | Reject non-loopback when no password set |

## Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `--appendonly` | `yes` | Enable AOF persistence (`yes`/`no`) — Moon is durable by default |
| `--appendfsync` | `everysec` | AOF fsync policy (`always`/`everysec`/`no`). `everysec` SET is ~1.32× Redis at pipeline depth and at parity non-pipelined; `always` (RPO 0) is fsync-device-bound — parity non-pipelined, ~0.91× Redis at depth. See `BENCHMARK.md` §7.3 |
| `--aof-fsync-timeout-ms` | `2000` | Bound on a write's wait for durability — the fsync ack under `always`, writer-queue backpressure under `everysec` (0 = unbounded) |
| `--wal-kv-log` | `auto` | KV logging into the per-shard WAL. `auto`: skipped while the AOF is the recovery authority and no CDC subscriber is attached (halves write volume at `--shards >= 2`); `on`: always log (needed for PITR / full CDC history with AOF on); `off`: never |
| `--appendfilename` | `appendonly.aof` | AOF filename |
| `--save` | *(none)* | RDB auto-save rules (e.g., `"3600 1 300 100"`) |
| `--dir` | `.` | Directory for persistence files |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |

## Memory and eviction

| Flag | Default | Description |
|------|---------|-------------|
| `--maxmemory` | `0` | Max memory in bytes (0 = unlimited) |
| `--maxmemory-policy` | `noeviction` | Eviction policy |
| `--maxmemory-samples` | `5` | Keys to sample for eviction |

**Eviction policies:** `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`

## TLS

| Flag | Default | Description |
|------|---------|-------------|
| `--tls-port` | `0` (disabled) | TLS listener port |
| `--tls-cert-file` | *(none)* | PEM certificate file |
| `--tls-key-file` | *(none)* | PEM private key file |
| `--tls-ca-cert-file` | *(none)* | CA cert for mTLS client auth |
| `--tls-ciphersuites` | *(default)* | TLS 1.3 cipher suites |

## Cluster

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-enabled` | `false` | Enable cluster mode |
| `--cluster-node-timeout` | `15000` | Node timeout in ms |

## Replication

Replication (v0.7 GA) is initiated at runtime with the `REPLICAOF <host> <port>`
command — there is **no startup flag**. The relevant startup flags shape the
topology and durability of the pair:

| Flag | On | Effect for replication |
|------|-----|------------------------|
| `--shards N` | master | Multi-core writer; the master merges all shards into one exactly-once replication feed |
| `--shards 1` | replica | **Required** — replicas are single-shard; scale reads by adding replicas |
| `--appendonly yes` | both | Persist the AOF so a restarted node recovers before re-syncing |
| `--appendfsync always` | master | RPO 0 on the master; pair with `WAIT N` for cross-node durability |
| `--appendfsync always` | replica | **Required for zero-RPO** — a replica ACKs on apply, not on fsync, so it must persist durably or a `WAIT`-acked write can still be lost if the replica crashes |

Replicas are read-only (`slave_read_only:1`; writes return `-READONLY`). `WAIT
numreplicas timeout` reports real replica ACKs. Full setup, `WAIT` durability,
promotion (`REPLICAOF NO ONE`), and the replica TTL caveat: see the
**[clustering & replication guide](guides/clustering.md#replication)** and the
[tuning guide](guides/tuning.md#replication-durability).

## ACL

| Flag | Default | Description |
|------|---------|-------------|
| `--aclfile` | *(none)* | Path to ACL file (Redis-compatible format) |
| `--acllog-max-len` | `128` | Max ACL log entries |

## Example: production configuration

```bash
./target/release/moon \
  --bind 0.0.0.0 \
  --port 6379 \
  --tls-port 6380 \
  --tls-cert-file /etc/moon/server.crt \
  --tls-key-file /etc/moon/server.key \
  --admin-port 9100 \
  --console-auth-required \
  --console-auth-secret "$ADMIN_SECRET" \
  --shards 8 \
  --requirepass "$REDIS_PASSWORD" \
  --appendonly yes \
  --appendfsync everysec \
  --dir /var/lib/moon \
  --maxmemory 8589934592 \
  --maxmemory-policy allkeys-lfu \
  --aclfile /etc/moon/users.acl
```

## Web console

| Flag | Default | Description |
|------|---------|-------------|
| `--admin-port` | `0` (disabled) | Admin/metrics HTTP port. Serves `/metrics`, `/healthz`, `/readyz`, and web console at `/ui/` |
| `--console-auth-required` | `false` | Require Bearer/HMAC auth on the admin/console HTTP port |
| `--console-auth-secret` | *(ephemeral)* | HMAC-SHA256 secret for token verification. Empty = auto-generated at startup |
| `--console-cors-origin` | `localhost:5173` | CORS origin allowlist (repeatable). `*` only allowed without auth |
| `--console-rate-limit` | `1000` | Per-IP rate limit in requests/sec on the admin port |
| `--console-rate-burst` | `2000` | Token-bucket burst capacity for the rate limiter |

## Performance tuning

| Flag | Default | Description |
|------|---------|-------------|
| `--maxclients` | `10000` | Maximum simultaneous client connections (0 = unlimited) |
| `--timeout` | `0` (disabled) | Close idle connections after N seconds |
| `--tcp-keepalive` | `300` | TCP keepalive interval in seconds (0 = disabled) |
| `--slowlog-log-slower-than` | `10000` | Slowlog threshold in microseconds |
| `--slowlog-max-len` | `128` | Maximum slowlog entries |
| `--profile` | *(none)* | Apply a named tuning preset (currently `standalone`). Only fills flags left at their default — an explicit flag always wins. Logs exactly what it set. **`standalone` requires pinned/dedicated cores** (regresses on shared hosts). See the [tuning guide](guides/tuning.md#profiles) |
| `--io-driver` | `auto` | I/O driver: `auto` (io_uring on Linux, kqueue on macOS) or `epoll` |
| `--io-busy-poll-us` | `0` (off) | Busy-poll the I/O driver for N µs before parking. Large single-op latency win on **dedicated, pinned cores**; a regression on shared/oversubscribed hosts. See the [tuning guide](guides/tuning.md) |
| `--initial-keyspace-hint` | `0` | Pre-size the keyspace (e.g. `1000000`) to avoid rehash pauses during bulk loads |
| `--memory-arenas-cap` | `8` | Cap jemalloc arenas — lower (e.g. `2`) for small containers |
| `--uring-sqpoll` | *(disabled)* | io_uring SQPOLL idle timeout in ms. Requires CAP_SYS_NICE. Linux only |

## Disk offload (tiered storage)

| Flag | Default | Description |
|------|---------|-------------|
| `--disk-offload` | `enable` | Enable disk offload (RAM → mmap → NVMe) |
| `--disk-offload-dir` | *(same as `--dir`)* | Directory for disk offload files |
| `--disk-offload-threshold` | `0.85` | RAM pressure threshold to trigger offload (0.0-1.0) |
| `--segment-warm-after` | `3600` | Seconds before sealed segments transition to warm tier |

## WAL (Write-Ahead Log)

| Flag | Default | Description |
|------|---------|-------------|
| `--wal-fpi` | `enable` | Enable Full Page Images for torn page defense |
| `--wal-compression` | `lz4` | FPI compression codec |
| `--wal-segment-size` | `16mb` | WAL segment file size |
| `--max-wal-size` | `256mb` | Max WAL size before triggering checkpoint |
| `--checkpoint-timeout` | `300` | Checkpoint timeout in seconds |
| `--checkpoint-completion` | `0.9` | Fraction of checkpoint interval for dirty page flush (0.0-1.0) |
| `--pagecache-size` | *(25% maxmemory)* | PageCache memory budget (e.g., `256mb`, `1gb`) |

## Vector search tuning

| Flag | Default | Description |
|------|---------|-------------|
| `--vec-codes-mlock` | `enable` | mlock vector code pages into RAM |
| `--vec-diskann-beam-width` | `8` | DiskANN beam width for disk-resident search (reserved) |
| `--vec-diskann-cache-levels` | `3` | HNSW upper levels cached for DiskANN hybrid (reserved) |
| `--segment-cold-after` | `86400` | Seconds before warm segments transition to cold tier (reserved) |
| `--segment-cold-min-qps` | `0.1` | QPS threshold for cold candidates (reserved) |

## Tips

!!! note
    The default `--shards 1` gives the best single-operation latency and is the right choice for most deployments. Add shards when you have **many concurrent connections (8+)** or **pipelined/batched traffic** — see the [tuning guide](guides/tuning.md) for measured guidance.

!!! tip
    Hash tags like `{tag}` in key names (e.g., `user:{1234}:name`) route all tagged keys to the same shard, eliminating cross-shard dispatch for MGET/MSET operations.

!!! warning
    Testing with more than 1,000 concurrent clients may require `ulimit -n 65536`. At 5,000 clients with pipelining, connection drops can occur without it.

For workload-specific recipes (cache, high-concurrency API, durable store, vector search, containers), see the **[tuning guide](guides/tuning.md)**.
