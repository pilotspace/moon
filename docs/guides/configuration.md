# Configuration Reference

Moon is configured through command-line flags and an optional Redis-style configuration file (`moon.conf`). Precedence is **CLI flags → conf file → built-in defaults**. Pass the conf file as the first argument (`moon /etc/moon/moon.conf`) or via `--config`; validate it without starting using `--check-config`. The flags below can all be set in either place.

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
| `--shards` | `1` | Number of shards. `0` auto-detects from CPU count |
| `--databases` | `16` | Number of logical databases |
| `--requirepass` | *(none)* | Require clients to authenticate with this password |
| `--check-config` | `false` | Validate configuration and exit without starting |

### Defaults are tuned for the single-shard case

Most deployments should run the defaults as-is: `--shards 1` gives the best
per-op latency for low-concurrency and non-pipelined traffic (a cross-shard hop
costs ~10µs), keeps every key co-located (no `{hash-tag}` planning), and — with
`--appendonly yes` + `--wal-kv-log auto` — writes each KV record to disk exactly
**once** (the AOF). Reach for more shards only with 8+ concurrent connections or
deep pipelines (measured 1.3–2.5× Redis at 8–64 conns on 4 shards), and see
[Tuning](tuning.md) for `--io-busy-poll-us` (a p=1 latency win on dedicated
cores that now auto-gates on shared ones, so it's safe on any host — or use
`--profile standalone` to fold it in).

## Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `--appendonly` | `yes` | Enable append-only file persistence (`yes`/`no`) — Moon is durable by default |
| `--appendfsync` | `everysec` | AOF fsync policy: `always`, `everysec`, or `no` |
| `--aof-fsync-timeout-ms` | `2000` | Max time a write may block awaiting durability (fsync ack under `always`, writer-queue backpressure under `everysec`) before it fails loudly instead of parking the connection. `0` = unbounded |
| `--wal-kv-log` | `auto` | KV command logging into the per-shard WAL. `auto`: skipped while the AOF is the recovery authority (`--appendonly yes`) and no CDC subscriber is attached — halves write volume at `--shards >= 2`; re-engages automatically when a CDC subscriber attaches. `on`: always log (pre-0.6 behavior; needed for [PITR](pitr.md) or full [CDC](cdc.md) history alongside AOF). `off`: never log KV records |
| `--appendfilename` | `appendonly.aof` | AOF filename |
| `--save` | *(none)* | RDB auto-save rules (e.g., `"3600 1 300 100"`) |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |
| `--dir` | *(user data dir)* | Directory for persistence files. Default auto-resolves per platform: Linux `$XDG_DATA_HOME/moon` (or `~/.local/share/moon`), macOS `~/Library/Application Support/moon`, Windows `%LOCALAPPDATA%\moon` — created on first run. Pre-v0.2.0 data in the startup directory keeps being used (with a warning). Pass any path (e.g. `--dir .`) to override |

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
| `--vector-ef-runtime` | `0` (auto) | Default `EF_RUNTIME` for indexes created without one (10-4096; `0` = per-query auto heuristic). Per-index `FT.CONFIG SET <idx> EF_RUNTIME` overrides at runtime |
| `--vector-rerank-mult` | `4` | Default `RERANK_MULT` for new indexes: exact-rerank depth, top `mult×k` beam candidates re-scored with true f16 distances (1-64). Per-index `FT.CONFIG` overrides |
| `--vector-exact-beam` | off | Default `EXACT_BEAM` for new indexes: HNSW beam navigates exact f16 sidecar distances (recall ≈ graph-limited; QPS cost grows with dimension). Per-index `FT.CONFIG` overrides |

## Graph

| Flag | Default | Description |
|------|---------|-------------|
| `--graph-merge-max-segments` | `8` | Immutable CSR segments per graph before autovacuum merges them |
| `--graph-dead-edge-trigger` | `0.20` | Dead-edge fraction that triggers a segment merge early |
| `--graph-timeout-ms` | `30000` | Default Cypher traversal timeout (0 = unlimited; per-query `TIMEOUT` overrides) |
| `--graph-result-cache-entries` | `256` | Cypher result-cache capacity per graph (repeated read-only queries served from cache) |
| `--graph-result-cache-bytes` | `4194304` | Cypher result-cache byte budget per graph (LRU eviction on either limit) |

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
