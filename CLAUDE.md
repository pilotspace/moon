# moon

High-performance Redis-compatible server in Rust. See [README.md](README.md) for build/run/test commands, configuration flags, architecture diagram, and command reference.

## MSRV

Rust **1.85** (edition 2024). Enforced in CI.

## Target Platform

**Linux only** (aarch64 primary, x86_64 secondary). macOS support is deferred to a future milestone.

All development, testing, and benchmarking MUST target Linux. On macOS hosts, use OrbStack (see below).

## OrbStack Development Environment

Moon requires Linux for io_uring, O_DIRECT, and production benchmarks. On macOS, use the `moon-dev` OrbStack machine.

### Machine: `moon-dev`

- **OS:** Ubuntu 24.04 (kernel 6.17+, full io_uring support)
- **Arch:** aarch64 (matches Apple Silicon host)
- **Rust:** 1.85.0 (MSRV-pinned)
- **Tools:** build-essential, pkg-config, libssl-dev, redis-server

OrbStack auto-mounts macOS `/Users/` into the VM — edit on macOS, compile on Linux. No rsync or Docker volumes needed.

### Commands

```bash
# Build (release)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && cargo build --release'

# Test (all)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && cargo test --release'

# Test (tokio runtime, CI parity)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && cargo test --no-default-features --features runtime-tokio,jemalloc'

# Clippy
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && cargo clippy -- -D warnings'

# Run server
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && ./target/release/moon --port 6399 --shards 4'

# Benchmark (redis-benchmark from macOS can reach moon-dev via OrbStack networking)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && cargo bench'

# Interactive shell
orb run -m moon-dev bash
```

### Recreating the Machine

If the machine is lost or corrupted:
```bash
orb delete moon-dev
orb create ubuntu moon-dev
orb run -m moon-dev bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.85.0'
orb run -m moon-dev bash -c 'sudo apt-get update -qq && sudo apt-get install -y -qq build-essential pkg-config libssl-dev redis-server'
```

### OrbStack Rules for Claude Code

- **Always build/test via `orb run -m moon-dev`** — never `cargo build` directly on macOS for final verification.
- `cargo check` on macOS is acceptable for fast iteration (syntax/type errors only).
- All benchmark numbers MUST come from the Linux VM.
- The VM path to the repo is the same as macOS: `/Users/tindang/workspaces/tind-repo/moon`.
- Use `source ~/.cargo/env &&` prefix in every `orb run` command.

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

## Coding Rules

### Unsafe Code
- Never introduce new `unsafe` blocks without explicit user approval.
- Every `unsafe` block MUST have a `// SAFETY:` comment explaining the invariant.
- Prefer safe abstractions. If unsafe is needed, isolate it in a dedicated module.
- When modifying existing unsafe code, verify all SAFETY comments remain accurate.
- Full policy, review checklist, approved patterns, and forbidden constructs:
  see [`UNSAFE_POLICY.md`](UNSAFE_POLICY.md).

### Allocations on Hot Paths
- No `Box::new()`, `Vec::new()`, `String::new()`, `Arc::new()`, `clone()`, `format!()`, or `to_string()` in:
  - Command dispatch (`src/command/`)
  - Protocol parsing (`src/protocol/`)
  - Shard event loops (`src/shard/event_loop.rs`)
  - I/O drivers (`src/io/`)
- Use `SmallVec`, `itoa`, `write!` to pre-allocated buffers, or borrow instead.
- `Vec::with_capacity()` is acceptable for result building at the end of a command path.

### Lock Handling
- Use `parking_lot::RwLock` / `parking_lot::Mutex` — never `std::sync` locks.
- Never hold a lock across `.await` points.
- Replace `.read().unwrap()` / `.write().unwrap()` with `.read()` / `.write()` (parking_lot doesn't poison).
- Per-shard locks only — no global locks on the write path.

### Error Handling
- All command errors return `Frame::Error(Bytes)` — no `Result` types in dispatch paths.
- No `unwrap()` or `expect()` in library code outside tests. Use pattern matching or `if let`.
- `anyhow` is only for `main.rs` and test code. Library code uses `thiserror` or `Frame::Error`.

### Feature Gates
- All runtime-specific code must compile under both `runtime-tokio` and `runtime-monoio`.
- Verify with: `cargo check --no-default-features --features runtime-tokio,jemalloc`
- Linux-only code (io_uring, O_DIRECT, `libc::` calls) must have `#[cfg(target_os = "linux")]` guards with a stub/fallback for non-Linux (compile guard is sufficient — runtime fallback not required until macOS milestone).
- New features use additive feature flags — never break the default feature set.

### New Commands
- Add to the `phf` dispatch table in the command registry.
- Include ACL category annotation.
- Add corresponding entries in `scripts/test-consistency.sh` and `scripts/test-commands.sh`.
- Benchmark new commands with `scripts/bench-compare.sh` if they touch hot paths.

### SIMD Code
- Always provide a scalar fallback for non-x86_64 architectures.
- Use `#[cfg(target_arch = "x86_64")]` with `#[target_feature(enable = "sse2")]` (baseline).
- AVX2/AVX-512 paths require runtime detection via `is_x86_feature_detected!`.
- Add unit tests for both SIMD and scalar paths.

### Performance Invariants
- Timestamp caching: never call `Instant::now()` per-key — use the shard-cached timestamp.
- Cross-shard dispatch: use `flume` channels, never `Arc<Mutex<>>` queues.
- Protocol parsing: zero-copy where possible — use `Bytes::slice()` not `to_vec()`.
- Response serialization: write directly to codec buffer, avoid intermediate `Vec<u8>`.

### File Size
- No single `.rs` file should exceed 1500 lines. Split into submodules if approaching this limit.
- Command implementations for a single Redis command group can be larger, but split read/write operations into separate files when exceeding 1000 lines.

### Testing
- Every new command needs at least one unit test and one consistency test entry.
- Integration tests use real server instances — no mocking.
- Benchmarks use Criterion with `black_box()` on inputs and outputs.

## GPU / CUDA Acceleration

### When to Use GPU
- Vector distance computation (L2, cosine, dot product) on batches > 1000 vectors.
- Bulk SIMD operations that exceed CPU SIMD width benefits (e.g., 10K+ float32 comparisons).
- Never for single-key operations — CPU + SIMD is always faster for individual lookups.

### CUDA Integration Pattern
- Use `cudarc` crate for safe Rust CUDA bindings (no raw FFI).
- Feature-gated: `--features gpu-cuda` — never in the default feature set.
- Kernels live in `src/gpu/kernels/` as `.cu` files, compiled at build time via `build.rs`.
- CPU fallback is mandatory — GPU path is an optimization, not a requirement.
- Device memory management: use pinned memory (`cuMemAllocHost`) for host-device transfers.
- Batch operations: accumulate work in a queue, dispatch to GPU when batch is full or timeout fires.

### GPU Memory Rules
- Never allocate GPU memory per-request — use a pre-allocated pool.
- Transfer data in batches (≥64KB) to amortize PCIe latency.
- Pin host memory for DMA transfers when throughput matters.
- Free GPU memory on shard shutdown, not per-operation.

### Vector Search (Future)
- Per-shard HNSW index — no cross-shard GPU sharing.
- Distance kernels: `f32` precision, SIMD on CPU, CUDA on GPU.
- Index building on GPU, serving on CPU (unless batch query mode).
- Use half-precision (`f16`) for storage, promote to `f32` for computation.

### Build Requirements
- CUDA Toolkit ≥ 12.0, compute capability ≥ 7.0 (Volta+).
- `build.rs` detects CUDA availability — graceful fallback to CPU if absent.
- CI runs CPU-only (`--no-default-features --features runtime-tokio,jemalloc`) — GPU tested separately.

## Clippy

Many style lints are suppressed in `src/lib.rs` (`#![allow(...)]`). Correctness and performance lints remain enabled. Do not add new `#![allow(...)]` entries without justification.

## CI

- `cargo test --no-default-features --features runtime-tokio,jemalloc` — runs on ubuntu-latest with `MOON_NO_URING=1`
- `cargo clippy -- -D warnings` — zero warnings policy (default features)
- `cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings` — zero warnings policy (tokio + jemalloc)
- `cargo fmt --check` — enforced formatting
- MSRV check — `cargo build` with Rust 1.85 toolchain
- CodeQL (Rust) — weekly + on push/PR
- Claude Code Review — runs on PRs

### Local CI Parity (via OrbStack)

Before pushing, run the full CI matrix locally:
```bash
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Users/tindang/workspaces/tind-repo/moon && cargo fmt --check && cargo clippy -- -D warnings && cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings && cargo test --release && cargo test --no-default-features --features runtime-tokio,jemalloc'
```
