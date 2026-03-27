# Phase 29: Runtime Abstraction Layer — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract runtime-agnostic trait boundaries from the current Tokio-coupled codebase, enabling compile-time swapping between Tokio, Glommio, and Monoio runtimes via Cargo feature flags. Includes DMA persistence path abstraction for io_uring-native file I/O.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key constraints from deep dive analysis:
- Current codebase has ~30 files with direct Tokio dependencies
- ~98 async functions, 11 tokio::select! sites, 13 spawn locations
- Tokio primitives used: mpsc, oneshot, watch, Notify, CancellationToken, interval, sleep, TcpListener/TcpStream, Framed/Codec, spawn_local, spawn_blocking
- io_uring driver already operates semi-independently (polled in 1ms timer, not through Tokio I/O)
- Glommio is Linux-only and declining maintenance — abstraction layer should not commit to it
- Monoio is actively maintained alternative with macOS kqueue fallback
- Goal is trait boundaries, not full runtime migration

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/io/uring_driver.rs` (574 lines) — already abstracted from Tokio, uses raw io_uring
- `src/io/tokio_driver.rs` (71 lines) — thin Framed wrapper, easy to trait-abstract
- `src/io/mod.rs` — cfg guards for Linux/non-Linux already exist
- `src/shard/mod.rs` — event loop with tokio::select! is the tightest coupling point

### Established Patterns
- `#[cfg(target_os = "linux")]` guards for io_uring code
- `Rc<RefCell<>>` for shard-local state (already !Send compatible)
- SPSC ringbuf channels independent of async runtime
- Per-shard `current_thread` Tokio runtime (maps naturally to Glommio LocalExecutor)

### Integration Points
- `src/main.rs` — runtime creation (3 sites)
- `src/shard/mod.rs` — shard event loop (core coupling)
- `src/persistence/wal.rs` — file I/O (DMA candidate)
- `src/persistence/aof.rs` — async file I/O
- `src/server/listener.rs` — TCP accept path
- `src/server/connection.rs` — per-connection task lifecycle

</code_context>

<specifics>
## Specific Ideas

No specific requirements — infrastructure phase focused on trait extraction for future runtime flexibility.

</specifics>

<deferred>
## Deferred Ideas

- Full Glommio migration (blocked by Linux-only + declining maintenance)
- Monoio integration (separate phase after trait layer is stable)
- DMA file I/O for WAL writes (depends on runtime supporting DmaStreamWriter or equivalent)

</deferred>
