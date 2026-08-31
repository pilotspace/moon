# moon

High-performance Redis-compatible server in Rust. See [README.md](README.md) for build/run/test commands, configuration flags, architecture diagram, and the command reference.

Load the skill that best fits the task — e.g. `/senior-rust-engineer` for Rust implementation.

## Architecture map

Thread-per-core, shared-nothing — each shard owns a keyspace slice on its own thread. Diagram and data-structure detail: [`docs/architecture.md`](docs/architecture.md).

**Request path:** `server/` (listener, conn, codec, TLS, response slot) -> `protocol/` (RESP parse/serialize, zero-copy) -> `shard/` (`event_loop`, `dispatch`, `mesh` SPSC, `coordinator`) -> `command/` (`phf` tables, one module per command group) -> `storage/` (`dashtable`, `db`, `tiered`, CompactKey/CompactValue) -> `persistence/` (`wal_v3`, `aof`, `page_cache`, `checkpoint`, `dir_lock`).

**Engines:** `vector/` HNSW + TurboQuant (FT.*) · `graph/` Cypher + CSR segments · `text/` FST term dict + BM25 · `scripting/` Lua sandbox · `mq/` queues + triggers.

**Cross-cutting:** `acl/` · `pubsub/` + `notify*.rs` · `replication/` · `cluster/` · `blocking/` · `transaction/` · `tracking/` (client-side caching invalidation) · `cdc/` · `temporal/` · `workspace/` · `admin/` (HTTP console, metrics, memory treemap) · `monitor/` · `telemetry/`.

**Runtime & I/O:** `runtime/` (monoio <-> tokio abstraction) · `io/` (io_uring driver) · `config.rs` · `client_registry.rs` · `tls.rs` · `memory_ctl.rs` + `malloc_respawn.rs` (jemalloc conf re-exec).

## Platform & toolchain

- **MSRV:** Rust **1.94**, edition 2024. Enforced in CI.
- **Develop natively on macOS** (aarch64 Apple Silicon / x86_64) — a first-class target. Both `runtime-monoio` (default; kqueue here) and `runtime-tokio` build, test, and run on the host.
- **Ship on Linux** (aarch64 primary, x86_64 secondary). io_uring, O_DIRECT, connection migration, and the spin governor's `/proc` sampling sit behind `cfg(target_os = "linux")` — **no amount of native testing exercises them**. Every benchmark number MUST come from a Linux host, never from a native run.

## Build · test · run

```bash
cargo build --release
cargo test --release                                                 # default features (monoio)
cargo test --no-default-features --features runtime-tokio,jemalloc   # tokio leg, CI parity
cargo clippy --all-targets -- -D warnings
cargo bench
./target/release/moon --port 6399 --shards 4
```

- `cargo check` does **not** compile tests — pass `--all-targets` after touching `#[cfg(test)]`. Use `--no-fail-fast` when you need every failing binary, not just the first.
- Pin `MOON_BIN` for suites that spawn a server: `find_moon_binary()` falls back to `target/release/moon`, whose provenance is unknown.
- The tokio leg drops default features — `graph` and `text-index` are absent there; `cfg!`-gate anything depending on them.

## Scripts

Need `redis-server` / `redis-benchmark` on PATH (`brew install redis`).

- `scripts/test-commands.sh` — 504 correctness+throughput rows vs Redis (`--skip-bench` for correctness only). Honours `PORT_REDIS`/`PORT_RUST`; refuses to start if either port is held.
- `scripts/test-consistency.sh` — 132 data-consistency tests across 1/4/12 shard configs.
- `scripts/bench-compare.sh` — Moon vs Redis, all commands, pipeline 1–128, 8B–64KB (`--requests 200000` for stable numbers). `scripts/bench-production.sh` — 10 production scenarios. `scripts/bench-resources.sh` — RSS, fresh server per row.

## Environment variables

Measured detail — spin governor, THP soak, io_uring dead ends — in [`docs/internal/env-knobs.md`](docs/internal/env-knobs.md).

- `RUST_LOG=moon=debug` — tracing output. `RUSTFLAGS="-C target-cpu=native"` — benchmarking only.
- `MOON_NO_URING=1` / `--io-driver epoll` — force the epoll/kqueue driver. Bench per platform: GCE ARM c4a favours epoll by 2–4% at all pipeline depths.
- `MOON_URING=1` — opt **into** the tokio→io_uring bridge (default-off; it floods errors under load).
- `MOON_EPOLL_SPIN_US` / `--io-busy-poll-us <µs>` — poll-mode park, legacy driver only; what flipped p=1 c1 to a win vs Redis. Self-gating since O3 (`src/shard/spin_governor.rs`).
- `MOON_IDLE_PARK=0` — disable the adaptive idle park (same-binary A/B knob).
- `MOON_XSHARD_SPIN_*` — C2 reply-spin overrides. **Bench-only, never production**: raising `MAX_CONNS` past the solo-conn ceiling re-creates the s4 c8P1 convoy collapse.
- `_RJEM_MALLOC_CONF` — jemalloc conf (prefixed; plain `MALLOC_CONF` is inert). `--memory-arenas-cap N` and `--memory-thp` re-exec before jemalloc init. THP stays **permanently opt-in** — a soak measured +27% idle RSS drift, ~+31% same-data overhead.

## Key design decisions

- **Compact SSO types:** `CompactKey` inlines keys ≤23 bytes, `CompactValue` values ≤12 bytes.
- **Per-shard WAL:** no global lock on writes; in-memory buffer flushed on a 1ms tick.
- **Lock-free channels (flume):** critical for pipeline throughput. **Shard-cached timestamps** and **lazy Lua/backlog init** keep baseline cost down.
- **monoio default on Linux:** io_uring thread-per-core; tokio for portability/CI.

## Gotchas

- **Multi-shard scaling is sub-linear** for non-pipelined workloads — cross-shard SPSC dispatch dominates the local DashTable lookup. Use `--shards 1` unless testing pipeline/AOF benefits, and for any per-key memory comparison against Redis. `{tag}` in a key co-locates all tagged keys on one shard.
- **WAL sync costs ~11× write throughput** (135K → 12K ops/s). Benchmark writes with `appendonly=no` first — the WAL writer exists whenever `persistence_dir` is set.
- **Memory benchmarks need a fresh server + `redis-benchmark -r <N>`** — without `-r` every write hits `__rand_key__` (1 real key), and FLUSHALL does not return pages.
- **redis-benchmark 8.x uses `\r`** for progress lines: `tr '\r' '\n'` before grepping RPS, and match the number by position — `awk '{print $2}'` yields `summary:`.
- **>1K clients** may need `ulimit -n 65536` or connections drop.
- **`FT.COMPACT` is a silent no-op** below `compact_threshold` — user calls must route through `force_compact`.
- **Vector recall on random Gaussian misleads** at high dimensions. Validate with real embeddings (MiniLM): 0.96+ there vs ~0.73 on random.

## Coding rules

**Unsafe.** Never introduce a new `unsafe` block without explicit user approval; every block carries a `// SAFETY:` comment, isolated in a dedicated module. Full policy and checklist: [`UNSAFE_POLICY.md`](UNSAFE_POLICY.md).

**Hot-path allocations.** No `Box/Vec/String/Arc::new()`, `clone()`, `format!()`, or `to_string()` in `src/command/`, `src/protocol/`, `src/shard/event_loop.rs`, or `src/io/`. Use `SmallVec`, `itoa`, `write!` into pre-allocated buffers, or borrow. `Vec::with_capacity()` is fine for a terminal result.

**Locks.** `parking_lot` only — never `std::sync`, and no `.unwrap()` on `.read()`/`.write()` (it does not poison). Never hold a lock across `.await`. Per-shard locks only; no global lock on the write path. *monoio cross-thread wakers:* `monoio::spawn` tasks are `!Send`, so `Waker::wake()` from another OS thread never reaches them — the cross-shard reply path awaits a `flume` oneshot directly. Prefer `flume::bounded(1)` over custom atomic oneshots.

**Errors.** Command errors return `Frame::Error(Bytes)` — no `Result` in dispatch paths. No `unwrap()`/`expect()` in library code; `anyhow` only in `main.rs` and tests, `thiserror` elsewhere. `parse_frame_zerocopy` returns `Frame::Null` on ANY parse failure — never add `.unwrap()` to protocol parsing; malformed input must never crash the server. A provably-safe unwrap needs `#[allow(clippy::unwrap_used)]` plus a one-line justification above it.

**Feature gates.** Everything compiles under both runtimes — verify with `cargo check --no-default-features --features runtime-tokio,jemalloc`. Linux-only code needs a `#[cfg(target_os = "linux")]` guard and a non-Linux stub. New features are additive; never break the default set.

**New commands.** Register in the `phf` dispatch table with an ACL category annotation, and add rows to both `scripts/test-consistency.sh` and `scripts/test-commands.sh`. Hot-path commands get a `scripts/bench-compare.sh` run. **There are three dispatch paths** — `command::dispatch` and `command::dispatch_read` (both `src/command/mod.rs`) and `server::conn::try_inline_dispatch` (`src/server/conn/blocking.rs`). A command wired into only some of them is silently wrong on the others, and CI does not catch the missing arm.

**SIMD.** Always ship a scalar fallback. `#[cfg(target_arch = "x86_64")]` with `sse2` baseline; AVX2/AVX-512 behind `is_x86_feature_detected!`. Unit-test both paths.

**Performance invariants.** Shard-cached timestamps, never `Instant::now()` per key. `flume` for cross-shard dispatch, never `Arc<Mutex<>>`. `Bytes::slice()`, not `to_vec()`. Serialize straight into the codec buffer. Profile first — `perf record -F 999 -g` + `objdump` — and verify the assembly actually changed; it is easy to unroll the wrong function.

**Files & modules.** No `.rs` over 1500 lines (command groups may run larger, but split read/write past 1000). Split into a directory module — `src/command/hash/` = `mod.rs` + `hash_read.rs` + `hash_write.rs` — re-exported from `mod.rs`, `crate::` imports (not `super::super::`), tests staying in `mod.rs`.

**Testing.** Every new command: ≥1 unit test and 1 consistency-test row. Integration tests use real server instances, no mocking. Criterion benches `black_box()` inputs *and* outputs. *Fuzzing:* 20 targets in `fuzz/fuzz_targets/`; any new parser, decoder, or deserializer needs a target AND an entry in **both** matrices in `.github/workflows/fuzz.yml` — an unlisted target never runs. The Lint job type-checks the crate (`cargo check --manifest-path fuzz/Cargo.toml --all-targets`) because nothing else does: two targets rotted for months, one failing to BUILD on every nightly. Build a `ShardSlice` via `shard::slice::test_support::make_init` (moon's `fuzzing` feature), never a hand-copied struct literal — that is what rotted. Nightly budget is 5h (`-max_total_time=18000`); 6h hits the hosted job ceiling and the corpus is lost. *Loom:* any new atomic state machine needs a model in `tests/loom_response_slot.rs`.

**Clippy.** Many style lints are `#![allow(...)]`-ed in `src/lib.rs`; correctness and performance lints stay on. Do not add new allows without justification.

## Subsystem references

- Vector engine (FT.*) internals — segment lifecycle, exact-rerank sidecar, adaptive ef, quantization trade-offs: [`docs/internal/vector-engine-internals.md`](docs/internal/vector-engine-internals.md). User guide: [`docs/vector-search-guide.md`](docs/vector-search-guide.md).
- GPU / CUDA (`--features gpu-cuda`, never default): [`docs/internal/gpu-cuda.md`](docs/internal/gpu-cuda.md).
- Cross-shard performance — the per-park cost model, seven measured dead ends, five retracted claims: [`docs/internal/cross-shard-cost-model.md`](docs/internal/cross-shard-cost-model.md). **Read before proposing any cross-shard optimisation.**

## CI — the merge bar

`scripts/ci-local.sh` is the local gate and, since #732, the **only** gate for the monoio suite (*the runtime that ships*), client-compat, macOS, and the console feature until a change reaches main. Run it before every push.

```bash
scripts/ci-local.sh           # = --full: 13 legs, ~17 min warm — lint ×7 + both VM suites
                              #   (CONCURRENT) + client-compat + macOS + doctests
scripts/ci-local.sh --native  # NO VM: both suites + client-compat on the macOS host
scripts/ci-local.sh --fast    # lint + both VM suites — NOT the merge bar
scripts/ci-local.sh --quick   # host lint only — NOT the merge bar
```

**Exit 4 = the moon-dev VM is unreachable**, and the run refuses before doing anything:
with no VM every VM leg fails at 0s, and the old behaviour was to continue for ~20 more
minutes and then blame your tests. Recreate it per
[`docs/internal/orbstack-linux-parity.md`](docs/internal/orbstack-linux-parity.md) — and
reinstall the Actions runner, which dies with the machine and otherwise leaves the hosted
monoio and client-compat legs queued forever. `CI_LOCAL_VM_SEQUENTIAL=1` runs the two VM
suites one after another again; they are concurrent by default (measured -44.5%, zero
retries), and that knob is the rollback if a wall-clock-sensitive test starts failing.

`--native` is the fallback when the VM is unavailable. It is **not** equivalent: it ends by naming what it skipped — io_uring, Linux-only `cfg` code, Windows, the MSRV pin — instead of a bare PASS, and exits 2 with no `redis-server` oracle on PATH. The script captures every exit code directly (no piped gates) and fingerprints the tree at start and end: a branch switch or edit mid-run marks the result INVALID (exit 3) rather than a false green.

**Windows cannot run locally.** Before merging, always dispatch the hosted matrix — `gh workflow run ci.yml --ref <branch>` (add `console-integration.yml` / `crash-matrix.yml` when warranted). It runs only what no local gate can produce: Check (Windows), MSRV 1.94, memory steady-state.

**Per-PR (hosted, ~5–8m):** Lint (`cargo fmt --check`, unsafe/unwrap audits, CHANGELOG gate — `skip-changelog` is the escape hatch) · Check (clippy ×3 + tokio nextest under `MOON_NO_URING=1`) · MSRV · memory steady-state. Fuzz, console, and integration legs run only behind labels. **Main-push** (post-merge net, not a gate): monoio self-hosted with io_uring live, client-compat against a real redis-server, macOS, console feature. **Scheduled:** fuzz nightly 5h, crash matrix nightly + weekly soak, CodeQL and supply-chain weekly.

<!-- ADD:BEGIN — managed by `add.py sync-guidelines`; do not edit inside -->
## ADD — how to work in this repo

This project uses **ADD (AI-Driven Development)**. The engine + book are installed.
To begin: run `python3 .add/tooling/add.py status` (the resume point), read
`.add/PROJECT.md`, then `python3 .add/tooling/add.py guide` for the current phase.

Open Claude Code and run `/add` — the skill drives intake -> milestone -> build.

This pointer is replaced by the full guideline block when `add.py sync-guidelines`
runs (at `/add`->init). Edit outside the markers, not inside.
<!-- ADD:END -->
