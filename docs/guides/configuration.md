# Configuration Reference

Moon is configured entirely through command-line flags. There is no configuration file; use your process manager or shell script to persist flags.

## Usage

```bash
./target/release/moon [OPTIONS]
```

## Network

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1` | Bind address |
| `--port`, `-p` | `6379` | Port to listen on |
| `--admin-port` | `0` (disabled) | Admin/metrics HTTP port. Serves `/metrics`, `/healthz`, `/readyz` |
| `--protected-mode` | `yes` | Reject non-loopback connections when no password is set |

## Server

| Flag | Default | Description |
|------|---------|-------------|
| `--shards` | `0` (auto) | Number of shards. `0` auto-detects from CPU count |
| `--databases` | `16` | Number of logical databases |
| `--requirepass` | *(none)* | Require clients to authenticate with this password |
| `--check-config` | `false` | Validate configuration and exit without starting |

## Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `--appendonly` | `no` | Enable append-only file persistence (`yes`/`no`) |
| `--appendfsync` | `everysec` | AOF fsync policy: `always`, `everysec`, or `no` |
| `--appendfilename` | `appendonly.aof` | AOF filename |
| `--save` | *(none)* | RDB auto-save rules (e.g., `"3600 1 300 100"`) |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |
| `--dir` | `.` | Directory for persistence files |

## Memory & Eviction

| Flag | Default | Description |
|------|---------|-------------|
| `--maxmemory` | `0` (unlimited) | Maximum memory in bytes |
| `--maxmemory-policy` | `noeviction` | Eviction policy: `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl` |
| `--maxmemory-samples` | `5` | Number of random keys to sample for eviction |

## TLS

| Flag | Default | Description |
|------|---------|-------------|
| `--tls-port` | `0` (disabled) | TLS port. Requires `--tls-cert-file` and `--tls-key-file` |
| `--tls-cert-file` | *(none)* | Path to TLS certificate file (PEM format) |
| `--tls-key-file` | *(none)* | Path to TLS private key file (PEM format) |
| `--tls-ca-cert-file` | *(none)* | Path to CA certificate for client authentication (mTLS) |
| `--tls-ciphersuites` | *(none)* | TLS 1.3 cipher suites (comma-separated) |

## ACL

| Flag | Default | Description |
|------|---------|-------------|
| `--aclfile` | *(none)* | Path to ACL file (Redis-compatible format) |
| `--acllog-max-len` | `128` | Maximum entries in the ACL log |

## Cluster

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-enabled` | `false` | Enable cluster mode |
| `--cluster-node-timeout` | `15000` | Cluster node timeout in milliseconds (PFAIL detection) |

## Slowlog

| Flag | Default | Description |
|------|---------|-------------|
| `--slowlog-log-slower-than` | `10000` | Slowlog threshold in microseconds |
| `--slowlog-max-len` | `128` | Maximum entries in the slowlog |

## io_uring (Linux only)

| Flag | Default | Description |
|------|---------|-------------|
| `--uring-sqpoll` | *(none)* | Enable SQPOLL mode with idle timeout in ms. Requires `CAP_SYS_NICE` or root |

## Disk Offload (Tiered Storage)

| Flag | Default | Description |
|------|---------|-------------|
| `--disk-offload` | `enable` | Enable disk offload: `enable` or `disable` |
| `--disk-offload-dir` | *(same as `--dir`)* | Directory for disk offload files |
| `--disk-offload-threshold` | `0.85` | RAM pressure threshold (0.0-1.0) to trigger offload |
| `--segment-warm-after` | `3600` | Seconds before sealed segments transition to warm tier |

## WAL v3

| Flag | Default | Description |
|------|---------|-------------|
| `--wal-fpi` | `enable` | Full Page Images for torn page defense: `enable` or `disable` |
| `--wal-compression` | `lz4` | FPI compression codec |
| `--wal-segment-size` | `16mb` | WAL segment file size |
| `--max-wal-size` | `256mb` | Maximum WAL size before triggering checkpoint |

## Checkpoint

| Flag | Default | Description |
|------|---------|-------------|
| `--checkpoint-timeout` | `300` | Checkpoint timeout in seconds |
| `--checkpoint-completion` | `0.9` | Fraction of checkpoint interval to spread dirty page flushes (0.0-1.0) |
| `--pagecache-size` | *(25% of maxmemory)* | PageCache memory budget (e.g., `256mb`, `1gb`) |

## Vector Search

| Flag | Default | Description |
|------|---------|-------------|
| `--vec-codes-mlock` | `enable` | mlock vector code pages into RAM: `enable` or `disable` |
| `--segment-cold-after` | `86400` | Seconds after last access before WARM segment becomes COLD candidate |
| `--segment-cold-min-qps` | `0.1` | Minimum QPS threshold; segments below this are COLD candidates |
| `--vec-diskann-beam-width` | `8` | DiskANN beam width for disk-resident vector search |
| `--vec-diskann-cache-levels` | `3` | HNSW upper levels cached in memory for DiskANN hybrid search |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG=moon=debug` | Enable tracing output (uses `tracing-subscriber` with `env-filter`) |
| `MOON_NO_URING=1` | Disable io_uring at runtime (for CI/containers/WSL) |
| `RUSTFLAGS="-C target-cpu=native"` | Enable CPU-specific optimizations for benchmarking |

## Size Syntax

Flags that accept sizes support the following suffixes (case-insensitive):

- `kb` -- kilobytes (1024 bytes)
- `mb` -- megabytes (1024^2 bytes)
- `gb` -- gigabytes (1024^3 bytes)
- Plain integers are treated as raw byte counts.

Examples: `256mb`, `1gb`, `64kb`, `16777216`.
