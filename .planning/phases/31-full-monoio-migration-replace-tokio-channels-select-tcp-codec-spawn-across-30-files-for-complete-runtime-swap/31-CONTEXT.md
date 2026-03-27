# Phase 31: Full Monoio Migration — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Complete the Monoio runtime migration by replacing all remaining direct Tokio usage (channels, select!, TCP, codec, spawn, timers, signals, CancellationToken) so the binary compiles and runs with `--features runtime-monoio` only. Phase 29 defined the runtime traits; Phase 30 implemented Monoio backends for 6 traits. This phase handles the remaining 30+ files with 151 compile errors.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key constraints from deep dive analysis:

**151 compile errors when building with `runtime-monoio` (no `runtime-tokio`):**

The errors fall into these categories:

**1. Channels (11+ files, ~40 errors):**
- `tokio::sync::mpsc` — used for connection distribution, AOF messages, replica senders, pub/sub, tracking
- `tokio::sync::oneshot` — used for cross-shard replies, snapshot completion, blocking commands
- `tokio::sync::watch` — used for snapshot epoch triggers
- `tokio::sync::Notify` — used for SPSC wakeup
- **Strategy:** Use `flume` crate (Send+Sync, async+sync, mpsc+oneshot-like) or keep channels runtime-agnostic via crossbeam/flume behind cfg

**2. tokio::select! macro (11 sites, 8 files, ~30 errors):**
- Shard event loop (6 arms) — tightest coupling
- Listener accept loop, connection handler, AOF writer, auto-save, expiration, cluster bus, gossip
- **Strategy:** Use `futures::select!` from futures crate (runtime-agnostic) or abstract into a runtime-specific event loop trait

**3. TCP I/O (9 files, ~25 errors):**
- `tokio::net::TcpListener` / `TcpStream` — listener, connection, cluster bus, replication
- `tokio_util::codec::Framed` / `Decoder` / `Encoder`
- `AsyncReadExt` / `AsyncWriteExt`
- **Strategy:** Use monoio::net::TcpListener/TcpStream behind cfg, adapt codec to work with both

**4. Spawn (10 files, ~20 errors):**
- `tokio::spawn()` — background tasks (auto-save, cluster, replication)
- `tokio::task::spawn_local()` — connection handlers on shard
- `tokio::task::spawn_blocking()` — BGSAVE, eviction
- **Strategy:** Already have RuntimeSpawn trait; extend to cover all spawn variants

**5. Timers (8 files, ~15 errors):**
- `tokio::time::interval()` — periodic tasks (partially abstracted in Phase 29)
- `tokio::time::sleep()` — replication backoff, WAIT polling
- `tokio::time::Instant` — budget tracking
- **Strategy:** Extend RuntimeTimer trait or use std::time::Instant + monoio::time::sleep

**6. Signal handling (2 files, ~5 errors):**
- `tokio::signal::ctrl_c()` — graceful shutdown
- **Strategy:** Use ctrlc crate (runtime-agnostic) or signal-hook

**7. CancellationToken (9 files, ~10 errors):**
- `tokio_util::sync::CancellationToken` — shutdown coordination
- **Strategy:** Implement simple CancellationToken using AtomicBool + futures channel, or use tokio-util behind both features

**8. Async file I/O (3 files, ~6 errors):**
- `tokio::fs::*` — AOF file operations
- **Strategy:** Already have FileIo trait from Phase 29; extend or use std::fs (sync)

**Compile-time feature strategy:**
- `#[cfg(feature = "runtime-tokio")]` blocks for Tokio-specific code
- `#[cfg(feature = "runtime-monoio")]` blocks for Monoio-specific code
- Shared code uses runtime-agnostic types (std::time, flume channels, etc.)

**Risk:** This is a massive refactor touching 30+ files. Must maintain all 1134 tests passing under `runtime-tokio`. Tests under `runtime-monoio` may be limited (no Tokio test runtime).

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/runtime/traits.rs` — 6 trait definitions (extend as needed)
- `src/runtime/tokio_impl.rs` — Tokio implementations
- `src/runtime/monoio_impl.rs` — Monoio implementations (Phase 30)
- Phase 29/30 established the cfg pattern

### Established Patterns
- `#[cfg(feature = "runtime-tokio")]` guards
- Trait-based abstraction with re-exports in mod.rs
- Single-threaded per-shard (LocalSet/LocalExecutor) — both runtimes are !Send

### Key Files Requiring Changes (by error count)
- `src/shard/mod.rs` (~30 errors) — event loop, timers, channels, select!
- `src/server/connection.rs` (~25 errors) — TCP, codec, select!, channels
- `src/server/listener.rs` (~15 errors) — TCP, spawn, select!, signal
- `src/main.rs` (~10 errors) — runtime, spawn, channels, signal
- `src/persistence/aof.rs` (~10 errors) — channels, timers, async file
- `src/replication/master.rs` (~10 errors) — TCP, channels, spawn
- `src/replication/replica.rs` (~8 errors) — TCP, timers, sleep
- `src/cluster/bus.rs` (~8 errors) — TCP, spawn, select!
- `src/cluster/gossip.rs` (~8 errors) — timers, spawn, select!
- `src/blocking/mod.rs` (~5 errors) — oneshot channels

</code_context>

<specifics>
## Specific Ideas

No specific requirements — complete the migration so `cargo build --no-default-features --features runtime-monoio` produces a working binary.

</specifics>

<deferred>
## Deferred Ideas

- Monoio-native io_uring integration (bypass UringDriver, use monoio's built-in io_uring)
- Performance benchmark comparison Monoio vs Tokio
- DMA file I/O via monoio::fs for WAL/snapshots
- Monoio-specific connection pooling

</deferred>
