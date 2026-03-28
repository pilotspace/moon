# moon

High-performance Redis-compatible server in Rust. See [README.md](README.md) for build/run/test commands, configuration flags, architecture diagram, and command reference.

## MSRV

Rust **1.85** (edition 2024). Enforced in CI.

## Environment Variables

- `RUST_LOG=moon=debug` — enable tracing output (uses `tracing-subscriber` with `env-filter`)
- `MOON_NO_URING=1` — disable io_uring at runtime; used in CI/containers/WSL where io_uring is unavailable
- `RUSTFLAGS="-C target-cpu=native"` — enable CPU-specific optimizations for benchmarking

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

- `cargo test --no-default-features --features runtime-tokio,jemalloc` — runs on ubuntu-latest with `MOON_NO_URING=1`
- `cargo clippy -- -D warnings` — zero warnings policy (both default features and `runtime-tokio,jemalloc`)
- `cargo fmt --check` — enforced formatting
- MSRV check — `cargo build` with Rust 1.85 toolchain
- CodeQL (Rust) — weekly + on push/PR
- Claude Code Review — runs on PRs
