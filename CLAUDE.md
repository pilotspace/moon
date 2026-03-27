# moon

High-performance Redis-compatible server in Rust. Dual-runtime: monoio (default, io_uring/kqueue) and tokio.

## Build

```bash
# Default (monoio + jemalloc, Linux/macOS)
cargo build --release

# Tokio runtime (required for macOS CI, CodeQL, GitHub Actions)
cargo build --release --no-default-features --features runtime-tokio

# Force jemalloc with tokio
cargo build --release --no-default-features --features runtime-tokio,jemalloc

# Native CPU optimizations (for benchmarking)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

## Run

```bash
# Start server (default port 6379, 4 shards)
./target/release/moon --port 6379 --shards 4

# --shards 0 auto-detects CPU core count
./target/release/moon --port 6379 --shards 0

# With AOF persistence
./target/release/moon --port 6379 --appendonly yes --appendfsync everysec

# With TLS
./target/release/moon --tls-port 6443 --tls-cert-file cert.pem --tls-key-file key.pem
```

## Test

```bash
# Unit + integration tests
cargo test --all-features

# Consistency tests (requires redis-server on PATH, compares moon vs Redis)
./scripts/test-consistency.sh
./scripts/test-consistency.sh --shards 4

# Skip rebuild
./scripts/test-consistency.sh --skip-build
```

## Benchmarks

```bash
# Criterion micro-benchmarks
cargo bench --bench resp_parsing
cargo bench --bench get_hotpath
cargo bench --bench entry_memory
cargo bench --bench bptree_memory
cargo bench --bench compact_key

# Production workload benchmark (compares vs Redis)
# Use redis-benchmark 8.x (parses \r in responses), use -r flag for unique keys
./scripts/bench-production.sh
./scripts/bench-production.sh --shards 4 --duration 60

# Resource/memory profiling
./scripts/bench-resources.sh
./scripts/profile.sh
```

## Architecture

```
src/
├── server/       # TCP listener, connection handling
├── io/           # Runtime-agnostic I/O abstractions
├── runtime/      # tokio/monoio dual-runtime shim
├── shard/        # Per-shard state, sharding mesh, VLL cross-shard coordinator
├── storage/      # Data types: string, hash, list, set, zset, stream
├── command/      # Command dispatch and parsing
├── protocol/     # RESP2/RESP3 parser (zero-copy for large payloads)
├── persistence/  # WAL/AOF (per-shard, no global lock)
├── replication/  # Leader/follower replication
├── cluster/      # Cluster mode (CRC16 slot routing)
├── pubsub/       # Pub/sub channels
├── scripting/    # Lua 5.4 scripting (mlua, lazy-initialized)
├── acl/          # Access control lists
├── blocking/     # Blocking commands (BLPOP, etc.)
└── tracking/     # Client-side caching invalidation
```

Primary KV engine: **DashTable** (segmented Swiss-table hash map with SIMD probing). B+tree is only used for SortedSet full encoding (`storage/bptree.rs`).

## Key Design Decisions

- **HeapString**: SSO at 23 bytes inline, heap beyond. Eliminates significant per-key overhead vs naive Arc<String>.
- **Per-shard WAL**: No global lock on writes. Low-latency append via in-memory buffer flushed on 1ms tick.
- **Lazy Lua/backlog init**: Reduces baseline memory. Lua sandbox initialized on first EVAL.
- **Lock-free channels (flume)**: Critical for pipeline throughput; avoids mutex contention on the hot path.
- **Timestamp caching**: Reduces syscall overhead by not calling `Instant::now()` per-key.
- **monoio default on Linux**: io_uring thread-per-core model; tokio for portability/CI.

## Gotchas

- **Multi-shard scaling:** Single-shard gives best throughput for non-pipelined workloads. Adding shards causes sub-linear scaling because most keys route cross-shard (SPSC dispatch overhead dominates local DashTable lookup). Use `--shards 1` unless testing pipeline/AOF benefits.
- **Hash tags for co-location:** `{tag}` in key names (e.g. `user:{1234}:name`) routes all tagged keys to the same shard, eliminating cross-shard dispatch for MGET/MSET operations.
- **High client counts:** Testing with >1K clients may require `ulimit -n 65536`; 5K clients with pipeline can cause connection drops without it.
- **AOF advantage grows with pipeline depth:** Per-shard WAL eliminates the global serialization bottleneck that Redis's single AOF file introduces at high pipeline depths.
- **Use `--shards 1` for fair per-key memory comparison** against Redis.

## Clippy

Many style lints are suppressed in `src/lib.rs` (`#![allow(...)]`). Correctness and performance lints remain enabled. Do not add new `#![allow(...)]` entries without justification.

## CI

- `cargo test --no-default-features --features runtime-tokio,jemalloc` — runs on ubuntu-latest
- `cargo clippy -- -D warnings` — zero warnings policy
- `cargo fmt --check` — enforced formatting
- CodeQL (Rust) — weekly + on push/PR
