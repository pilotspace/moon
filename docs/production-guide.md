# Production Deployment Guide

This guide covers deploying Moon in production, from a single Docker container to tuned multi-shard configurations with TLS, ACL, persistence, and monitoring.

## Quick Start

### Docker run (minimal)

```bash
docker run -d \
  --name moon \
  -p 6379:6379 \
  -v moon-data:/data \
  moondb/moon:latest
```

This starts Moon on port 6379 with auto-detected shard count, persistence directory at `/data`, and protected mode disabled (safe inside Docker networks).

### Docker run (production)

```bash
docker run -d \
  --name moon \
  --restart unless-stopped \
  -p 6379:6379 \
  -p 9100:9100 \
  -v moon-data:/data \
  -v /etc/moon/certs:/certs:ro \
  --ulimit nofile=65536:65536 \
  --memory 8g \
  moondb/moon:latest \
  moon --bind 0.0.0.0 \
    --port 6379 \
    --admin-port 9100 \
    --shards 0 \
    --requirepass "$MOON_PASSWORD" \
    --appendonly yes \
    --appendfsync everysec \
    --dir /data \
    --maxmemory 6442450944 \
    --maxmemory-policy allkeys-lfu \
    --tls-port 6443 \
    --tls-cert-file /certs/server.crt \
    --tls-key-file /certs/server.key
```

### Docker Compose

```bash
# Clone the repo (or just copy docker-compose.yml)
git clone https://github.com/pilotspace/moon.git
cd moon

# Start with defaults (AOF enabled, 4 CPU limit, 2 GB memory)
docker compose up -d

# Override settings via environment
MOON_SHARDS=8 MOON_MAXMEMORY=8589934592 docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down
```

See `docker-compose.yml` for all configurable environment variables.

### Build from source

```bash
docker build -t moon .

# Or for a specific runtime:
docker build --build-arg FEATURES=runtime-tokio,jemalloc -t moon:tokio .

# Multi-platform:
docker buildx build --platform linux/amd64,linux/arm64 -t moondb/moon:latest .
```

## Configuration Reference

All options are command-line flags. Run `moon --help` for the full list.

### Server

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1` | Bind address. Use `0.0.0.0` in containers. |
| `--port` / `-p` | `6379` | Redis protocol port |
| `--admin-port` | `0` (disabled) | Admin/console HTTP port. Serves `/metrics`, `/healthz`, `/readyz`, and web UI at `/ui/` |
| `--shards` | `0` (auto) | Number of shards. 0 = CPU core count. |
| `--databases` | `16` | Number of logical databases |
| `--maxclients` | `10000` | Maximum simultaneous client connections |
| `--timeout` | `0` (disabled) | Close idle connections after N seconds |
| `--tcp-keepalive` | `300` | TCP keepalive interval in seconds |
| `--protected-mode` | `yes` | Reject non-loopback connections when no password set |

### Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `--appendonly` | `no` | Enable AOF persistence (`yes`/`no`) |
| `--appendfsync` | `everysec` | AOF fsync policy: `always`, `everysec`, or `no` |
| `--appendfilename` | `appendonly.aof` | AOF filename |
| `--save` | *(none)* | RDB auto-save rules (e.g., `"3600 1 300 100"`) |
| `--dir` | `.` | Directory for persistence files |
| `--dbfilename` | `dump.rdb` | RDB snapshot filename |

### Memory and Eviction

| Flag | Default | Description |
|------|---------|-------------|
| `--maxmemory` | `0` (unlimited) | Maximum memory in bytes |
| `--maxmemory-policy` | `noeviction` | Eviction policy when maxmemory is reached |
| `--maxmemory-samples` | `5` | Keys to sample per eviction cycle |

Eviction policies: `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`.

### TLS

| Flag | Default | Description |
|------|---------|-------------|
| `--tls-port` | `0` (disabled) | TLS listener port |
| `--tls-cert-file` | *(none)* | PEM certificate file |
| `--tls-key-file` | *(none)* | PEM private key file |
| `--tls-ca-cert-file` | *(none)* | CA cert for mTLS client verification |
| `--tls-ciphersuites` | *(default)* | TLS 1.3 cipher suites (comma-separated) |

### ACL

| Flag | Default | Description |
|------|---------|-------------|
| `--requirepass` | *(none)* | Require password for all clients |
| `--aclfile` | *(none)* | Path to ACL file (Redis-compatible format) |
| `--acllog-max-len` | `128` | Maximum ACL log entries |

### Console/Admin Hardening

| Flag | Default | Description |
|------|---------|-------------|
| `--console-auth-required` | `false` | Require Bearer auth on admin API endpoints |
| `--console-auth-secret` | *(auto-generated)* | HMAC-SHA256 secret for token verification |
| `--console-cors-origin` | `localhost:5173` | CORS origin allowlist (repeatable) |
| `--console-rate-limit` | `1000` | Per-IP request rate limit (req/s) |
| `--console-rate-burst` | `2000` | Token-bucket burst capacity |

### Performance

| Flag | Default | Description |
|------|---------|-------------|
| `--uring-sqpoll` | *(disabled)* | io_uring SQPOLL idle timeout (ms). Requires CAP_SYS_NICE. |
| `--disk-offload` | `enable` | Tiered storage: RAM to NVMe |
| `--slowlog-log-slower-than` | `10000` | Slowlog threshold in microseconds |
| `--slowlog-max-len` | `128` | Maximum slowlog entries |

### Cluster

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-enabled` | `false` | Enable cluster mode |
| `--cluster-node-timeout` | `15000` | Node timeout in ms |

## TLS Setup

Moon uses rustls with aws-lc-rs for TLS 1.3. No OpenSSL dependency.

### Generate certificates (self-signed, for testing)

```bash
# Generate CA
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout ca.key -out ca.crt -days 365 -nodes -subj "/CN=Moon CA"

# Generate server cert
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout server.key -out server.csr -nodes -subj "/CN=moon"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 365

# Clean up
rm server.csr ca.srl
```

### Start with TLS

```bash
docker run -d \
  -p 6443:6443 \
  -v moon-data:/data \
  -v $(pwd)/certs:/certs:ro \
  moondb/moon:latest \
  moon --bind 0.0.0.0 \
    --tls-port 6443 \
    --tls-cert-file /certs/server.crt \
    --tls-key-file /certs/server.key \
    --dir /data
```

### Connect with TLS

```bash
redis-cli --tls --cert certs/client.crt --key certs/client.key \
  --cacert certs/ca.crt -p 6443
```

### mTLS (mutual TLS)

Add `--tls-ca-cert-file /certs/ca.crt` to require client certificates:

```bash
moon --bind 0.0.0.0 \
  --tls-port 6443 \
  --tls-cert-file /certs/server.crt \
  --tls-key-file /certs/server.key \
  --tls-ca-cert-file /certs/ca.crt
```

### Disable plaintext port

Set `--port 0` to accept only TLS connections:

```bash
moon --bind 0.0.0.0 --port 0 --tls-port 6443 \
  --tls-cert-file /certs/server.crt \
  --tls-key-file /certs/server.key
```

## ACL Configuration

Moon supports Redis-compatible ACL files for fine-grained access control.

### Simple password authentication

```bash
moon --requirepass "your-strong-password"
```

### ACL file

Create `users.acl`:

```
# Default user (backward-compatible with --requirepass)
user default on >password ~* &* +@all

# Read-only user
user reader on >reader-pass ~* &* +@read -@write -@admin

# Application user with key restrictions
user app on >app-secret ~app:* ~cache:* &* +@all -@admin -@dangerous

# Admin user
user admin on >admin-secret ~* &* +@all
```

Start with the ACL file:

```bash
moon --aclfile /etc/moon/users.acl
```

In Docker:

```bash
docker run -d \
  -p 6379:6379 \
  -v moon-data:/data \
  -v $(pwd)/users.acl:/etc/moon/users.acl:ro \
  moondb/moon:latest \
  moon --bind 0.0.0.0 --dir /data --aclfile /etc/moon/users.acl
```

### ACL categories

Moon supports Redis ACL categories: `@all`, `@read`, `@write`, `@admin`, `@dangerous`, `@fast`, `@slow`, `@string`, `@hash`, `@list`, `@set`, `@sortedset`, `@stream`, `@pubsub`, `@scripting`, `@connection`, `@server`, `@generic`, `@keyspace`, `@hyperloglog`, `@bitmap`, `@geo`.

### Runtime ACL management

```
AUTH username password
ACL LIST
ACL SETUSER myuser on >pass ~key:* +get +set
ACL DELUSER myuser
ACL SAVE
```

## Persistence Tuning

Moon provides per-shard WAL (Write-Ahead Log) for AOF persistence and forkless RDB snapshots.

### AOF (recommended for production)

```bash
moon --appendonly yes --appendfsync everysec --dir /data
```

| `appendfsync` | Durability | Performance Impact |
|---|---|---|
| `always` | RPO = 0 (zero data loss) | Highest latency; suitable for financial data |
| `everysec` | RPO <= 1 second | Recommended default; negligible throughput impact |
| `no` | OS flush window (minutes) | Cache-mode only; do not use for primary storage |

Moon's per-shard WAL avoids the global serialization bottleneck that Redis's single AOF file creates. The AOF advantage over Redis **grows** with pipeline depth (2.75x at p=64).

### RDB snapshots

```bash
# Snapshot every 3600 seconds if at least 1 key changed,
# or every 300 seconds if at least 100 keys changed
moon --save "3600 1 300 100" --dir /data --dbfilename dump.rdb
```

Moon uses forkless RDB snapshots -- no `fork()`, no COW memory spike.

### Combined AOF + RDB

```bash
moon --appendonly yes --appendfsync everysec \
  --save "3600 1" --dir /data
```

Recovery order: RDB snapshot, then WAL segments, then AOF tail.

### AOF rewrite

Trigger manual compaction:

```
BGREWRITEAOF
```

### Persistence volume in Docker

Always mount a named volume or host directory for `/data`:

```bash
docker run -v moon-data:/data moondb/moon:latest
```

For host-path mounts (better for backup tooling):

```bash
docker run -v /var/lib/moon:/data moondb/moon:latest
```

## Memory Management

### Setting maxmemory

Reserve 20-25% of available RAM for OS, jemalloc overhead, and fragmentation:

```bash
# On a 32 GB host, allocate 24 GB to Moon
moon --maxmemory 25769803776 --maxmemory-policy allkeys-lfu
```

Human-readable suffixes are not supported; use bytes. Common values:

| Memory | Bytes |
|--------|-------|
| 1 GB   | `1073741824` |
| 2 GB   | `2147483648` |
| 4 GB   | `4294967296` |
| 8 GB   | `8589934592` |
| 16 GB  | `17179869184` |
| 32 GB  | `34359738368` |

### Eviction policies

| Policy | Best for |
|--------|----------|
| `noeviction` | Primary storage (returns error on OOM) |
| `allkeys-lfu` | Cache workloads (evicts least frequently used) |
| `allkeys-lru` | Cache workloads (evicts least recently used) |
| `volatile-lfu` | Mixed: only evicts keys with TTL set |
| `volatile-ttl` | Evicts keys closest to expiration |
| `allkeys-random` | Simple random eviction |

### Disk offload (tiered storage)

Moon can spill evicted keys to NVMe instead of deleting them:

```bash
moon --maxmemory 8589934592 --maxmemory-policy allkeys-lfu \
  --disk-offload enable --dir /data
```

Reads from the cold tier use async read-through with full crash recovery.

### jemalloc tuning

Moon ships with jemalloc by default. The allocator is pre-tuned for the thread-per-core architecture. Key environment variables for advanced tuning:

```bash
# Per-CPU arenas (reduces cross-thread contention)
MALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto"
```

In Docker Compose:

```yaml
environment:
  - MALLOC_CONF=percpu_arena:percpu,background_thread:true
```

### Monitoring memory

```
INFO memory
MEMORY USAGE <key>
MEMORY DOCTOR
```

The admin port (`--admin-port 9100`) exposes `/metrics` with Prometheus-format memory gauges.

## Shard Count Selection

Moon uses a thread-per-core, shared-nothing architecture. Each shard owns its event loop, data partition, WAL writer, and Pub/Sub registry.

| Scenario | Recommended `--shards` | Rationale |
|----------|----------------------|-----------|
| Development/testing | `1` | Simplest debugging, deterministic behavior |
| Small workload (<100K keys) | `1` | Single shard avoids cross-shard dispatch overhead |
| General production | `0` (auto) | Matches CPU core count inside the container |
| Memory benchmarking | `1` | Fair per-key comparison against Redis |
| High-throughput pipeline | CPU cores | Per-shard WAL eliminates global serialization bottleneck |

Set CPU limits in Docker to control auto-detected shard count:

```yaml
deploy:
  resources:
    limits:
      cpus: "8"   # --shards 0 will create 8 shards
```

### Hash tags for key co-location

Use `{tag}` in key names to route related keys to the same shard:

```
SET user:{1234}:name "Alice"
SET user:{1234}:email "alice@example.com"
MGET user:{1234}:name user:{1234}:email   # Same shard, no cross-shard dispatch
```

## Health Checks and Monitoring

### Admin port

Enable the admin port for HTTP-based monitoring:

```bash
moon --admin-port 9100
```

| Endpoint | Purpose |
|----------|---------|
| `/healthz` | Liveness probe (returns 200 when server is running) |
| `/readyz` | Readiness probe (returns 200 when accepting connections) |
| `/metrics` | Prometheus metrics (QPS, latency, memory, clients, keyspace) |
| `/ui/` | Web console (Dashboard, Browser, Console, Vectors, Graph, Memory) |

### Kubernetes probes

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9100
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: 9100
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Docker healthcheck

The default Docker healthcheck uses `moon --check-config`. For a more thorough probe when the admin port is available, override with curl:

```yaml
healthcheck:
  test: ["CMD", "curl", "-sf", "http://localhost:9100/healthz"]
  interval: 10s
  timeout: 3s
  retries: 3
```

Note: The default distroless image does not include curl. Use a debian-slim based image or a sidecar for HTTP health checks.

### Redis PING

From any Redis client:

```bash
redis-cli -p 6379 PING
# PONG
```

### INFO command

```
INFO server        # Version, uptime, mode
INFO memory        # RSS, peak, fragmentation ratio
INFO clients       # Connected clients, blocked clients
INFO stats         # Total commands, ops/sec, keyspace hits/misses
INFO replication   # Master/replica status
INFO keyspace      # Per-database key counts
INFO all           # Everything
```

### SLOWLOG

```
SLOWLOG GET 10           # Last 10 slow commands
SLOWLOG LEN              # Number of entries
SLOWLOG RESET            # Clear the log
CONFIG SET slowlog-log-slower-than 5000   # 5ms threshold
```

## Scaling Guidelines

### Single node

Moon's thread-per-core architecture scales vertically to the number of CPU cores. A single instance on an 8-core machine with monoio + io_uring can achieve:

- 4.8M+ GET/s at pipeline depth 64
- 3.6M+ SET/s at pipeline depth 64 (with AOF everysec)

### Horizontal scaling

For datasets larger than a single node's memory or for high availability:

1. **Read replicas** -- use `REPLICAOF <master-ip> <master-port>` for read scaling
2. **Client-side sharding** -- partition keyspace across multiple Moon instances
3. **Cluster mode** (experimental) -- `--cluster-enabled` with gossip-based slot migration

### Resource planning

| Metric | Guideline |
|--------|-----------|
| CPU | 1 core per shard; add 1 core for admin/monitoring overhead |
| Memory | Dataset size + 25% for jemalloc overhead and fragmentation |
| Disk | 2x dataset size for AOF rewrite headroom |
| Network | 10 Gbps for >1M ops/sec workloads |
| File descriptors | `ulimit -n 65536` for >1K concurrent clients |

## Backup and Restore

### RDB snapshot backup

```bash
# Trigger a background save
redis-cli -p 6379 BGSAVE

# Wait for completion
redis-cli -p 6379 LASTSAVE

# Copy the dump file
docker cp moon:/data/dump.rdb ./backup/dump.rdb
```

### AOF backup

```bash
# Trigger AOF rewrite to compact the file
redis-cli -p 6379 BGREWRITEAOF

# Copy persistence directory
docker cp moon:/data/ ./backup/
```

### Restore from backup

```bash
# Stop the server
docker compose down

# Replace persistence files
cp backup/dump.rdb /var/lib/moon/dump.rdb
# OR for AOF:
cp backup/appendonly.aof /var/lib/moon/appendonly.aof

# Start the server (will replay from persistence files)
docker compose up -d
```

### Automated backup with cron

```bash
# /etc/cron.d/moon-backup
0 */6 * * * root docker exec moon redis-cli BGSAVE && sleep 5 && docker cp moon:/data/dump.rdb /backup/moon/dump-$(date +\%Y\%m\%d-\%H\%M).rdb
```

## Security Checklist

Before deploying to production:

- [ ] Set `--requirepass` or use an ACL file -- never run without authentication on a network-accessible port
- [ ] Enable TLS (`--tls-port`) for encrypted connections; consider `--port 0` to disable plaintext
- [ ] Use mTLS (`--tls-ca-cert-file`) for zero-trust environments
- [ ] Set `--protected-mode yes` (default) when running outside Docker
- [ ] Restrict admin port access with `--console-auth-required` and `--console-auth-secret`
- [ ] Set `--console-cors-origin` to your specific domain (not wildcard)
- [ ] Run as non-root (the Docker image uses UID 65534 by default)
- [ ] Set resource limits (CPU, memory) to prevent noisy-neighbor issues
- [ ] Mount secrets (passwords, TLS keys) as Docker secrets or read-only volumes, not environment variables
- [ ] Review ACL file permissions: `chmod 600 users.acl`

## Troubleshooting

### "Connection refused" from outside Docker

Use `--bind 0.0.0.0` inside containers. The default `127.0.0.1` is only reachable from within the container.

### High latency with many shards

For small datasets (<100K keys), use `--shards 1`. Cross-shard dispatch overhead dominates local DashTable lookup for non-pipelined workloads.

### "Too many open files"

Set `ulimit -n 65536` on the host or use `ulimits` in Docker Compose. Testing with >1K concurrent clients requires this.

### io_uring not available

Moon falls back gracefully. Set `MOON_NO_URING=1` to explicitly disable io_uring, or use the tokio runtime:

```bash
docker build --build-arg FEATURES=runtime-tokio,jemalloc -t moon:tokio .
```

### WAL sync kills write throughput

`appendfsync=always` reduces write throughput by approximately 11x. Use `appendfsync=everysec` for the best durability/performance trade-off.

### Container OOM killed

Set `--maxmemory` to 75-80% of the container memory limit. Moon's eviction policies will keep memory in bounds. Without maxmemory, the dataset grows until the container is killed.
