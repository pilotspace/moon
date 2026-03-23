# Benchmark Results

## Test Environment

| Property | Value |
|----------|-------|
| **Date** | 2026-03-23 |
| **OS** | macOS 24.6.0 (Darwin arm64) |
| **CPU** | Apple M4 Pro (12 cores) |
| **Memory** | 24 GB |
| **Rust** | 1.94.0 (2026-03-02) |
| **Allocator** | jemalloc (tikv-jemallocator) |
| **Redis (baseline)** | Not installed -- install via `brew install redis` for comparison |

**Benchmark tool:** `redis-benchmark` (ships with Redis)
**Requests per test:** 100,000
**Key space:** 100,000 random keys

> **Note:** Benchmark results below are placeholder tables. Run `./bench.sh` to populate with actual numbers. Install Redis first: `brew install redis`.

---

## Throughput and Latency

The benchmark suite tests 13 commands across all combinations of:
- **Clients:** 1, 10, 50 concurrent connections
- **Pipeline depths:** 1 (no pipelining), 16, 64
- **Data sizes:** 3B (default), 256B, 1KB, 4KB

### SET -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 10 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 256B | _run bench.sh_ | - | - | - |
| 50 | 16 | 1024B | _run bench.sh_ | - | - | - |
| 50 | 16 | 4096B | _run bench.sh_ | - | - | - |

### GET -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 10 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 256B | _run bench.sh_ | - | - | - |
| 50 | 16 | 1024B | _run bench.sh_ | - | - | - |
| 50 | 16 | 4096B | _run bench.sh_ | - | - | - |

### INCR -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### LPUSH -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### RPUSH -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### LPOP -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### RPOP -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### SADD -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### HSET -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### SPOP -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### ZADD -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### ZRANGEBYSCORE -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### MSET (10 keys) -- rust-redis

| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---------|----------|-----------|---------|----------|----------|------------|
| 1 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 1 | 3B | _run bench.sh_ | - | - | - |
| 50 | 16 | 3B | _run bench.sh_ | - | - | - |
| 50 | 64 | 3B | _run bench.sh_ | - | - | - |

### Side-by-Side Comparison (rust-redis vs Redis 7.x)

> Install Redis (`brew install redis`) and run `./bench.sh` to generate comparison tables.
> The script runs identical workloads against both servers on separate ports (6399/6400)
> and produces a ratio column showing relative throughput.

| Command | rust-redis (ops/sec) | Redis 7.x (ops/sec) | Ratio | Notes |
|---------|---------------------|---------------------|-------|-------|
| SET | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16, 3B |
| GET | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16, 3B |
| INCR | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| LPUSH | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| RPUSH | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| LPOP | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| RPOP | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| SADD | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| HSET | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| SPOP | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| ZADD | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| ZRANGEBYSCORE | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |
| MSET | _run bench.sh_ | _run bench.sh_ | - | 50 clients, pipeline 16 |

---

## CPU Utilization

| Server | Benchmark Phase | Avg CPU % |
|--------|-----------------|-----------|
| rust-redis | Throughput suite | _run bench.sh_ |
| Redis 7.x | Throughput suite | _run bench.sh_ |

**Expected behavior:** Redis 7.x is single-threaded by design and will saturate a single core (~100% on one core). rust-redis uses Tokio's multi-threaded executor and should distribute load across cores, showing lower per-core utilization but potentially higher aggregate throughput under concurrent workloads.

**Observation notes:**
- rust-redis benefits from Tokio's work-stealing scheduler under high concurrency (50 clients)
- At pipeline depth 1 with 1 client, both servers are network-latency bound rather than CPU bound
- CPU spikes are expected during sorted set operations (ZADD, ZRANGEBYSCORE) due to BTreeMap maintenance

---

## Memory Usage (RSS)

| Server | RSS at rest (KB) | RSS with 100K keys (KB) | Delta (KB) |
|--------|------------------|------------------------|------------|
| rust-redis | _run bench.sh_ | _run bench.sh_ | - |
| Redis 7.x | _run bench.sh_ | _run bench.sh_ | - |

**Expected behavior:** rust-redis uses jemalloc (via tikv-jemallocator) which provides good fragmentation resistance. At rest, the Tokio runtime and jemalloc metadata contribute to a baseline RSS of approximately 2-5 MB. With 100K small keys, per-key overhead includes the HashMap entry, Bytes key/value buffers, and expiry metadata. Redis 7.x uses its own jemalloc build with custom arena configuration, optimized over 15+ years for this exact workload.

---

## Persistence Overhead

| Mode | SET ops/sec | GET ops/sec | SET Overhead vs None |
|------|-------------|-------------|---------------------|
| No persistence | _run bench.sh_ | _run bench.sh_ | baseline |
| AOF everysec | _run bench.sh_ | _run bench.sh_ | - |
| AOF always | _run bench.sh_ | _run bench.sh_ | - |

**Expected behavior:**
- **No persistence:** Maximum throughput -- all operations are in-memory only
- **AOF everysec:** Minimal overhead for GET (read-only, no AOF write). SET overhead should be <5% as writes are buffered and fsynced once per second via a background Tokio task
- **AOF always:** Significant SET overhead expected (potentially 50-80% reduction) because every write command is fsynced to disk before acknowledgment. GET performance should be unaffected since reads do not trigger AOF writes

---

## Top 3 Performance Bottlenecks

### 1. Global Database Lock Contention

The primary bottleneck in rust-redis is the per-database `Mutex` (or `RwLock`) that serializes all access to the key-value store. While Tokio distributes connection handling across threads, every command that reads or writes data must acquire the database lock. Under high concurrency (50 clients), this creates contention that limits throughput scaling. Redis 7.x avoids this entirely by being single-threaded -- there is no lock contention because only one thread accesses data. For rust-redis, the multi-threaded architecture provides benefits for connection parsing and protocol handling, but the data path is effectively serialized. Future optimization: sharding the keyspace across multiple independent lock domains (similar to ConcurrentHashMap segments) would reduce contention proportionally to the shard count.

### 2. Sorted Set Dual-Index Maintenance (ZADD, ZRANGEBYSCORE)

Sorted set operations maintain two data structures in lockstep: a `HashMap<member, score>` for O(1) lookups and a `BTreeMap<(score, member)>` for ordered range queries. Every ZADD must insert/update both structures, and every ZREM must remove from both. This dual-write overhead means sorted set mutations are roughly 2x the cost of simple key-value SET operations. Additionally, BTreeMap operations involve pointer chasing through a tree structure, which is less cache-friendly than HashMap's flat array layout. The p99.9 latency for ZADD is expected to show occasional spikes when BTreeMap rebalancing triggers. Future optimization: a skip list (as Redis uses) provides better cache locality for range operations and amortizes insertion cost.

### 3. Bytes Allocation on Copy-from-Slice

Throughout the codebase, the parser and command handlers use `Bytes::copy_from_slice` to extract owned frames from the network buffer. This was a deliberate Phase 1 decision to avoid lifetime infection across the codebase. However, each `copy_from_slice` allocates a new buffer and copies data, whereas a zero-copy approach using `Bytes::slice()` on the original read buffer would eliminate these allocations entirely. For small payloads (3B values), the allocation overhead is negligible. For larger payloads (4KB), the copy becomes a measurable fraction of per-command latency, especially under pipelining where many commands are parsed from a single buffer read. This is most visible as sublinear pipeline scaling -- doubling pipeline depth should nearly double throughput, but allocation overhead per command reduces the gains. Future optimization: implement zero-copy frame extraction using `BytesMut::freeze()` and `Bytes::slice()`.

---

## How to Run Benchmarks

```bash
# Install Redis (provides redis-benchmark and redis-server for baseline)
brew install redis

# Build the release binary
cargo build --release

# Run full benchmark suite (rust-redis + Redis 7.x comparison)
./bench.sh --output BENCHMARK.md

# Run rust-redis only (no Redis baseline)
./bench.sh --rust-only --output BENCHMARK.md

# Customize request count
./bench.sh --requests 50000 --output BENCHMARK.md
```

The `bench.sh` script manages the full server lifecycle:
1. Builds the release binary if needed
2. Starts rust-redis on port 6399
3. Runs the throughput/latency suite across all client/pipeline/datasize combinations
4. Measures memory (RSS at rest and with 100K keys)
5. Tests persistence overhead (no AOF, everysec, always)
6. Optionally starts Redis 7.x on port 6400 for baseline comparison
7. Generates this BENCHMARK.md with formatted tables

---

*Generated: 2026-03-23*
*Server: rust-redis v0.1.0*
*Benchmark script: bench.sh*
