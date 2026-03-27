# rust-redis

High-performance Redis-compatible server in Rust (~55K LOC, 96 files). Dual-runtime: monoio (default, io_uring/kqueue) and tokio. ~1.84–1.99x Redis throughput at p=8–16 parallelism.

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
# Start server (default port 6400, 1 shard)
./target/release/rust-redis --port 6400 --shards 4

# With AOF persistence
./target/release/rust-redis --port 6400 --appendonly yes --appendfsync everysec

# With TLS
./target/release/rust-redis --tls-port 6443 --tls-cert-file cert.pem --tls-key-file key.pem
```

## Test

```bash
# Unit + integration tests
cargo test --all-features

# Consistency tests (requires redis-server on PATH, compares rust-redis vs Redis)
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

# Production workload benchmark (compares vs Redis)
# Peak advantage at p=8–16; use redis-benchmark 8.x for \r parsing
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
├── shard/        # Per-shard state, sharding mesh, B+tree storage
├── storage/      # Data types: string, hash, list, set, zset, stream
├── command/      # Command dispatch and parsing
├── protocol/     # RESP2/RESP3 parser (zero-copy for large payloads)
├── persistence/  # WAL/AOF (per-shard, 5ns append, no global lock)
├── replication/  # Leader/follower replication
├── cluster/      # Cluster mode (CRC16 slot routing)
├── pubsub/       # Pub/sub channels
├── scripting/    # Lua 5.4 scripting (mlua, lazy-initialized)
├── acl/          # Access control lists
├── blocking/     # Blocking commands (BLPOP, etc.)
└── tracking/     # Client-side caching invalidation
```

## Key Design Decisions

- **HeapString**: SSO at 23 bytes inline, heap beyond. Eliminates ~62% per-key overhead vs naive Arc<String>.
- **Per-shard WAL**: No global lock on writes. 5ns append latency.
- **Lazy Lua/backlog init**: Reduces baseline memory. Lua sandbox initialized on first EVAL.
- **Lock-free channels (flume)**: Critical for pipeline throughput; saves ~12% CPU vs mutex channels.
- **Timestamp caching**: Saves ~4% CPU by not calling `Instant::now()` per-key.
- **monoio default on Linux**: io_uring thread-per-core model; tokio for portability/CI.

## Performance Notes

- Peak throughput advantage: **p=8–16** parallelism (network-bound below, CPU-bound above)
- With AOF everysec: advantage **grows** to 2.75x at p=64 (per-shard WAL vs Redis global lock)
- Memory: beats Redis per-key at **1KB+** value sizes
- Benchmark correctly: use `redis-benchmark 8.x` (parses `\r` in responses), use `-r` flag for unique keys
- Use `--shards 1` for fair per-key memory comparison

## Clippy

Many style lints are suppressed in `src/lib.rs` (`#![allow(...)]`). Correctness and performance lints remain enabled. Do not add new `#![allow(...)]` entries without justification.

## CI

- `cargo test --all-features` — runs on ubuntu-latest
- `cargo clippy -- -D warnings` — zero warnings policy
- `cargo fmt --check` — enforced formatting
- CodeQL (Rust) — weekly + on push/PR
