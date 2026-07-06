# moon

High-performance Redis-compatible server in Rust. See [README.md](README.md) for build/run/test commands, configuration flags, architecture diagram, and command reference.

Load specific skill that best fit for each user task. Like /senior-rust-engineer to do task related to rust implement.

## MSRV

Rust **1.94** (edition 2024). Enforced in CI.

## Target Platform

**Linux** (aarch64 primary, x86_64 secondary) and **macOS** (aarch64 Apple Silicon, x86_64 Intel).

- **Linux**: Full feature set — io_uring (monoio), SO_REUSEPORT per-shard, O_DIRECT, connection migration.
- **macOS**: Full feature set minus io_uring — uses kqueue (monoio) or epoll (tokio), SO_REUSEPORT per-shard, no O_DIRECT (pread fallback), no connection migration.
- **Production benchmarks** MUST target Linux (OrbStack or GCloud). macOS numbers are for development only.
- Both `runtime-monoio` (default) and `runtime-tokio` are supported on macOS.

## OrbStack Development Environment

OrbStack is used for Linux-parity builds, production benchmarks, and io_uring testing on macOS hosts.

### Machine: `moon-dev`

- **OS:** Ubuntu 24.04 (kernel 6.17+, full io_uring support)
- **Arch:** aarch64 (matches Apple Silicon host)
- **Rust:** 1.94.1 (MSRV-pinned)
- **Tools:** build-essential, pkg-config, libssl-dev, redis-server

OrbStack auto-mounts the macOS filesystem (including `/Volumes/`) into the VM at the same paths — edit on macOS, compile on Linux. No rsync or Docker volumes needed. `orb run` preserves the caller's working directory, so commands run from the repo need no `cd` at all.

> **⚠ Stale checkout trap:** `/Users/tindang/workspaces/tind-repo/moon` is an OLD second checkout (stuck at hash-ttl era). The live repo is `/Volumes/Games/tindang-repo/moon` — never `cd` to the old path in VM commands, and pin `MOON_BIN` explicitly for integration tests that spawn a server binary (`find_moon_binary()` falls back to `target/release/moon`, whose provenance is unknown).

### Commands

```bash
# Build (release)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo build --release'

# Test (all)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo test --release'

# Test (tokio runtime, CI parity)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo test --no-default-features --features runtime-tokio,jemalloc'

# Clippy
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo clippy -- -D warnings'

# Run server
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && ./target/release/moon --port 6399 --shards 4'

# Benchmark (redis-benchmark from macOS can reach moon-dev via OrbStack networking)
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo bench'

# Interactive shell
orb run -m moon-dev bash
```

### Recreating the Machine

If the machine is lost or corrupted:
```bash
orb delete moon-dev
orb create ubuntu moon-dev
orb run -m moon-dev bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1'
orb run -m moon-dev bash -c 'sudo apt-get update -qq && sudo apt-get install -y -qq build-essential pkg-config libssl-dev redis-server'
```

### OrbStack Rules for Claude Code

- **`cargo build`/`cargo test` on macOS is now fully supported** — macOS is a first-class target.
- Use `orb run -m moon-dev` for Linux-specific testing (io_uring, O_DIRECT, connection migration).
- All **benchmark numbers** MUST come from the Linux VM (or GCloud instances).
- The VM path to the repo is the same as macOS: `/Volumes/Games/tindang-repo/moon`.
- Use `source ~/.cargo/env &&` prefix in every `orb run` command.
- Use `CARGO_TARGET_DIR=target-linux` for VM builds of the shared checkout so Linux ELF and macOS Mach-O artifacts never clobber each other (`/target-linux/` is gitignored).
- **Diskfull guard:** Moon pauses writes (`MOONERR diskfull`) when the data dir's filesystem has <5% free. `/Volumes/Games` hovers near that line — run server-spawning suites (e.g. `scripts/test-consistency.sh`) from a VM-local clone (`git clone --depth 1 file:///Volumes/Games/tindang-repo/moon ~/moon-consistency`) or pass a fresh `--dir` on VM /tmp.
- Don't edit sources on macOS while a VM build of the same checkout is compiling (shared fs → spurious compile errors).

## Scripts

- `scripts/bench-compare.sh` — Moon vs Redis side-by-side (all commands, pipeline 1–128, data 8B–64KB). Use `--requests 200000` for stable numbers.
- `scripts/bench-production.sh` — 10 production scenarios (session, ratelimit, leaderboard, cache, queue, hash, connections, datasizes, memory, pipeline).
- `scripts/bench-resources.sh` — RSS/memory bench. Starts a fresh server per row (RSS is a high-water mark; delta within one process is unreliable).
- `scripts/test-commands.sh` — 190 tests across 13 categories (correctness vs Redis + throughput). `--skip-bench` for fast correctness-only.
- `scripts/test-consistency.sh` — 132 data-consistency tests across 1/4/12 shard configs.
- All scripts run inside `moon-dev` and need `redis-server` / `redis-benchmark` on PATH.

## Environment Variables

- `RUST_LOG=moon=debug` — enable tracing output (uses `tracing-subscriber` with `env-filter`)
- `MOON_NO_URING=1` — force-disable io_uring everywhere (monoio runtime + tokio bridge); used in CI/containers/WSL where io_uring is unavailable. ⚠ Before 2026-07 this env was a silent NO-OP for the monoio driver (FusionDriver picked io_uring regardless); it now forces the epoll/kqueue LegacyDriver. CLI equivalent: `--io-driver epoll`. Empirically verify the driver via `ls -l /proc/<pid>/fd | grep io_uring` — monoio logs neither choice. **GCE ARM (c4a Axion): epoll beats io_uring by 2-4% at ALL pipeline depths for KV** (same-instance A/B 2026-07-03); other platforms favor io_uring — bench per platform.
- `MOON_URING=1` — opt **into** the tokio→io_uring bridge. The bridge is **default-off under the tokio runtime** (it floods errors under load and can hang the accept loop); tokio shards run plain epoll/kqueue unless this is set. No effect on the monoio runtime, which always uses io_uring unless `MOON_NO_URING` is set.
- `MOON_EPOLL_SPIN_US=<µs>` / CLI `--io-busy-poll-us <µs>` — poll-mode park (vendored-monoio patch, `vendor/monoio`, grep "moon patch"): the shard thread busy-loops zero-timeout readiness polls for the budget before blocking, deleting the per-op scheduler sleep+wake. Legacy (epoll/kqueue) driver only — the CLI flag forces it; io_uring CQEs are NOT observable from userspace without task participation (DEFER_TASKRUN posts only inside enter; TWA_SIGNAL kicks cost ~µs; SQPOLL doesn't run poll task_work on 6.17 — all measured 2026-07-03). **This is what flipped GCE p=1 c1 to a WIN vs Redis: ARM c4a 0.95→1.19/1.21, x86 c3 1.06→1.66** (same-instance A/Bs). Costs up to budget-µs CPU per idle park (~4%/core at 40µs vs 1ms timer parks). ⚠ Unpinned/shared-core environments (OrbStack default, laptop) show spin as a REGRESSION — only judge it on pinned disjoint cores.
- `MOON_URING_SPIN_US`, `MOON_URING_SQPOLL[_CPU]`, `MOON_URING_PLAIN` — io_uring-side experiment gates kept as documented diagnostics; all dead ends for the p=1 path (see `tmp/KV-FULLPROOF.md` Round 2).
- `MOON_XSHARD_SPIN_BUDGET` / `MOON_XSHARD_SPIN_GATE` / `MOON_XSHARD_SPIN_MAX_CONNS` — diagnostic overrides for the C2 reply-side spin (`src/shard/slice.rs`; defaults 4096 iters / gate 2 / **solo-conn 1**). Budget `0` disables the spin entirely (the same-instance A/B knob that proved the c8P1 convoy). ⚠ The solo-conn ceiling (spin only when the conn is ALONE on its shard thread) is the L1 convoy fix — raising `MAX_CONNS` re-creates the s4 c8P1 collapse (a spinning conn starves its sibling AND the shard's SPSC drain, 0.45× vs Redis; fixed = 2.75× better, see `tmp/MULTISHARD-REDESIGN.md`). Bench-only knobs: never set in production.
- `RUSTFLAGS="-C target-cpu=native"` — enable CPU-specific optimizations for benchmarking

## Key Design Decisions

- **Compact SSO types**: `CompactKey` stores keys up to 23 bytes inline; `CompactValue` stores values up to 12 bytes inline (heap beyond, via `HeapString(Vec<u8>)`). Eliminates significant per-key overhead vs naive `Arc<String>`.
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
- **WAL sync kills write throughput ~11x** (135K → 12K ops/s). Always benchmark writes with `appendonly=no` first. The WAL writer is created whenever `persistence_dir` is set, so explicitly disable it when measuring raw write speed.
- **Memory benchmarks need a fresh server + `redis-benchmark -r <N>`.** Without `-r`, all writes hit `__rand_key__` (1 real key). Delta measurement within one process is broken — RSS is a high-water mark, FLUSHALL does not return pages.
- **redis-benchmark 8.x uses `\r` for progress lines.** Pipe through `tr '\r' '\n' | grep "requests per second" | tail -1 | awk '{print $2}'` before grepping RPS, or you'll parse the command name instead.
- **`FT.COMPACT` is a silent no-op** when `mutable_len < compact_threshold`. Explicit user calls must route through `force_compact`, or set `COMPACT_THRESHOLD` ≥ expected dataset size so the one final compact happens.
- **Vector recall on random Gaussian is misleading** at high dimensions (concentration of distances). Validate recall with real semantic embeddings (e.g. MiniLM) — Moon hits 0.96+ there vs ~0.73 on random.

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
- **monoio cross-thread wakers:** `monoio::spawn` creates `!Send` tasks; `Waker::wake()` from another OS thread does NOT reach them. The cross-shard **reply** path therefore has the connection **await a `flume` oneshot directly** (the target shard sends the reply on it after executing). A `pending_wakers: Rc<RefCell<Vec<Waker>>>` relay is still swept each event-loop iteration (pub/sub + backpressure paths thread it), but it is **no longer the reply-wake mechanism** — the old "register your waker, event loop drains it after SPSC" design was retired when test `swf0` disproved its premise (see `event_loop.rs` ~1730). For cross-thread signalling, prefer `flume::bounded(1)` over custom atomic oneshots.

### Error Handling
- All command errors return `Frame::Error(Bytes)` — no `Result` types in dispatch paths.
- No `unwrap()` or `expect()` in library code outside tests. Use pattern matching, `if let`, or `let-else`.
- `anyhow` is only for `main.rs` and test code. Library code uses `thiserror` or `Frame::Error`.
- **Parser defensiveness:** `parse_frame_zerocopy` returns `Frame::Null` on ANY parse failure instead of panicking. Never add `.unwrap()` to protocol parsing code — malformed client input must never crash the server.
- If an unwrap is truly safe (post-insert get, atomic-invariant), add `#[allow(clippy::unwrap_used)]` with a one-line justification comment on the preceding line.

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
- Profile first, optimize second. Use `perf record -F 999 -g` + `objdump` on the stripped binary and verify the assembly actually changed (e.g. serial `vaddss xmm0` chain → parallel `vaddss xmm3..8`). Easy to unroll the wrong function.

### File Size
- No single `.rs` file should exceed 1500 lines. Split into submodules if approaching this limit.
- Command implementations for a single Redis command group can be larger, but split read/write operations into separate files when exceeding 1000 lines.
- **Split convention:** command files become directory modules (`src/command/hash/` with `mod.rs`, `hash_read.rs`, `hash_write.rs`). The `mod.rs` re-exports via `pub use hash_read::*; pub use hash_write::*;` and holds shared helpers + tests.

### Testing
- Every new command needs at least one unit test and one consistency test entry.
- Integration tests use real server instances — no mocking.
- Benchmarks use Criterion with `black_box()` on inputs and outputs.
- **Fuzzing:** 7 `cargo-fuzz` targets in `fuzz/fuzz_targets/`. Any new parser, decoder, or deserialization function MUST have a fuzz target. CI runs 15 min/target on PRs and 6h nightly.
- **Loom:** model tests in `tests/loom_response_slot.rs` for lock-free data structures. Any new atomic state machine MUST have a loom model.

### Module Structure
- Modules with subfiles use Rust directory-module convention: `src/command/hash/` → `mod.rs` + `hash_read.rs` + `hash_write.rs`.
- Re-exports in `mod.rs` maintain the same public API paths (`hash::hget` resolves via `pub use hash_read::*`).
- Submodule files use `crate::` imports (NOT `super::super::`).
- Test code stays in `mod.rs`, not in split subfiles.

## Vector Search (FT.*)

Moon ships a native HNSW + TurboQuant vector engine exposed via a RediSearch-compatible subset (`FT.CREATE`, `FT.DROPINDEX`, `FT.INFO`, `FT.SEARCH`, `FT.COMPACT`). Source: `src/vector/` and `src/command/vector_search/`.

- **Per-index knobs:** `EF_RUNTIME` (recall/QPS trade-off), `COMPACT_THRESHOLD` (when to flush mutable → immutable segment), `MERGE_RECALL_TOLERANCE` (FT.CONFIG; recall gate for unattended GraphUnion merges, default 0.70).
- **Segment lifecycle:** auto-indexed on HSET → mutable segment (brute force) → compact → immutable segment (HNSW graph + TQ codes). Immutable segments DO merge: `MERGE_MODE GRAPH_UNION` (the default) auto-merges at ≥16 segments or ≥20% dead, gated by a recall check (0.70 background / 0.90 manual FT.COMPACT). What is forbidden is *decode+re-encode* merging — re-quantizing accumulates error (tested, recall collapsed 0.73 → 0.0005); GraphUnion stitches graphs over the original codes instead. `MERGE_MODE KEEP_RAW` is rejected at FT.CREATE (unimplemented stub).
- **Key hash map:** `key_hash_to_key: Arc<HashMap<u64, Bytes>>` (copy-on-write via `Arc::make_mut`) must be populated in `auto_index_hset` and propagated via `SearchResult.key_hash`; otherwise multi-segment search returns synthetic `vec:<id>` instead of original keys.
- **FT.INFO `num_docs`** must sum across all segments (mutable + immutable), not just the mutable one — and across all shards: FT.INFO scatter-gathers via `scatter_ft_info` + `merge_ft_info_responses` (additive counters, per-field merge); a local-only answer under-reports by ~1/N.
- **Exact rerank sidecar (HQ-1):** immutable segments keep an f16 copy of each original vector (`raw_f16.bin` on disk; built from the mutable segment's always-retained f16 buffer); the top 4·k beam candidates are re-scored with true metric distances before truncation, via runtime-detected SIMD kernels (NEON integer-rescale on aarch64, F16C+FMA on x86_64, scalar fallback everywhere; +8.6% QPS from the kernels alone, ~+13% cumulative with the per-query fixed-cost cleanup — thread-cached search scratch, Arc'd MVCC treemap snapshot, ryu score formatting). Segments reloaded from pre-sidecar dirs (or rebuilt without raw data) silently fall back to quantized ADC distances — recall degrades, doesn't break. Merge propagation is all-or-nothing, and a drop that discards a real sidecar is now LOUD: `tracing::warn!` with source / sources-with-sidecar counts, plus additive `FT.INFO` counters `graph_segments` / `segments_with_exact_rerank` (merged across shards; coverage < total ⇒ some segments answer ADC-only).
- **Adaptive per-segment ef (AE-1):** at compact/GraphUnion time each immutable segment self-probes recall against its own exact f16 sidecar (leave-self-out, ef ladder 24..256); only a fully-saturated curve (flat ≈1.0 from the minimum rung) certifies the segment "trivially easy" and searches at min-ef (24) — every other segment keeps the full resolved ef. Applies only when `EF_RUNTIME` was heuristic-resolved (never overrides a user-pinned value) and is in-memory only (reloaded segments fall back to the full beam). A blanket `ef/√G` split (EF-SPLIT) was tried first and REJECTED — it silently cost gaussian 5-seg R@10 0.9915 → 0.9295; don't reintroduce it.
- **FLUSHALL/FLUSHDB/HDEL keyspace parity:** FLUSHALL and FLUSHDB clear every vector + text index's CONTENTS while KEEPING the FT.CREATE definition (matches restart semantics; indexes are keyspace-global so FLUSHDB of any db clears all index contents) — previously flushed hashes stayed searchable as ghosts until restart. `HDEL key <vector-field>` now tombstones that key in exactly the indexes whose vector field was removed (sibling indexes on other fields keep their entry). Known follow-ups: a multi-vector-field index tombstones the whole document when any one of its vector fields is removed; TEXT/TAG/NUMERIC field removal is not yet re-indexed.
- **TQ4 at 384d loses recall** (concentration of distances + quantization noise). Use **SQ8** or FP32 HNSW for ≤384d workloads; TQ4 shines at 768d+. (There is no TQ8 — SQ8 is the real 8-bit option: per-vector affine scalar quant, normalizes for the unit-sphere metrics Cosine + InnerProduct, validated across the full lifecycle — search/merge/persistence — at ~0.90 R@10 on real MiniLM 384d. PR #166.)

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
- MSRV check — `cargo build` with Rust 1.94 toolchain
- **Safety audit** — `scripts/audit-unsafe.sh` (100% SAFETY comment coverage on unsafe blocks) + `scripts/audit-unwrap.sh` (unwrap ratchet prevents new unannotated unwraps)
- **Fuzz** — `cargo-fuzz` 15 min/target on PR, 6h nightly (`resp_parse`, `resp_parse_differential`, `inline_parse`, `wal_v3_record`, `gossip_deser`, `acl_rule`). Uses nightly compiler; `rust-toolchain.toml` is removed in CI for this job.
- CodeQL (Rust) — weekly + on push/PR
- Claude Code Review — runs on PRs

### Local CI Parity (via OrbStack)

Before pushing, run the full CI matrix locally:
```bash
orb run -m moon-dev bash -c 'source ~/.cargo/env && cd /Volumes/Games/tindang-repo/moon && cargo fmt --check && cargo clippy -- -D warnings && cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings && cargo test --release && cargo test --no-default-features --features runtime-tokio,jemalloc'
```

<!-- ADD:BEGIN — managed by `add.py sync-guidelines`; do not edit inside -->
## ADD — how to work in this repo

This project uses **ADD (AI-Driven Development)**. The engine + book are installed.
To begin: run `python3 .add/tooling/add.py status` (the resume point), read
`.add/PROJECT.md`, then `python3 .add/tooling/add.py guide` for the current phase.

Open Claude Code and run `/add` — the skill drives intake -> milestone -> build.

This pointer is replaced by the full guideline block when `add.py sync-guidelines`
runs (at `/add`->init). Edit outside the markers, not inside.
<!-- ADD:END -->
