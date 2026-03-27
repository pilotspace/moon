# Phase 30: Monoio Runtime Migration — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement a Monoio backend for the runtime abstraction traits defined in Phase 29. Add `runtime-monoio` Cargo feature flag alongside existing `runtime-tokio`. Monoio provides thread-per-core LocalExecutor with native io_uring on Linux and kqueue fallback on macOS. This is NOT a replacement of Tokio — it's an alternative backend selectable at compile time.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key constraints from deep dive analysis:

**Phase 29 established these runtime traits:**
- `RuntimeTimer` — interval/sleep abstractions
- `RuntimeInterval` — periodic timer tick
- `RuntimeSpawn` — task spawning
- `RuntimeFactory` — per-shard runtime creation + block_on_local
- `FileIo` — file open/append operations
- `FileSync` — fsync operations

**Currently wired in 3 files:**
- `src/main.rs` — `TokioRuntimeFactory::block_on_local` for shard threads
- `src/persistence/wal.rs` — `TokioFileIo::open_append` for WAL files
- `src/shard/mod.rs` — `TokioTimer::interval` for 4 timer intervals

**Monoio characteristics (from deep dive):**
- ByteDance production (Monolake proxy framework)
- Actively maintained (unlike Glommio)
- macOS kqueue fallback (critical for dev workflow)
- `!Send` futures (same as current LocalSet model)
- GAT-based I/O traits (buffer ownership in type system)
- ~2x Tokio throughput at 4 cores, ~3x at 16 cores
- Latest version: monoio 0.2.x
- Thread-per-core via `RuntimeBuilder::new().build()`

**What remains Tokio-only (NOT abstracted in Phase 29):**
- `tokio::select!` macro (11 sites, 8 files) — must remain
- Channel types: mpsc, oneshot, watch, Notify (11+ files)
- CancellationToken (9 files)
- Codec/Framed (3 files)
- TCP I/O (9 files — separate from runtime traits)
- spawn/spawn_local (10 files)

**This means:** Monoio backend only implements the 6 traits. The rest of the codebase still uses Tokio primitives directly. A full migration would require a separate phase to replace channels/select!/TCP. This phase proves the trait layer works with a real alternative runtime.

**Compile-time selection:**
- `cargo build` → default `runtime-tokio` (zero regression)
- `cargo build --no-default-features --features runtime-monoio` → Monoio backend
- Both features cannot be active simultaneously (compile error)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/runtime/traits.rs` — 6 trait definitions (Phase 29)
- `src/runtime/tokio_impl.rs` — Reference Tokio implementation to mirror
- `src/runtime/mod.rs` — cfg-gated re-exports

### Established Patterns
- `#[cfg(feature = "runtime-tokio")]` guards in runtime/mod.rs
- Trait implementations are in separate files (tokio_impl.rs)
- Re-exports via `pub use` in mod.rs

### Integration Points
- `Cargo.toml` — add monoio dependency behind feature flag
- `src/runtime/mod.rs` — add cfg-gated monoio_impl module
- `src/runtime/monoio_impl.rs` — new file with Monoio implementations

</code_context>

<specifics>
## Specific Ideas

No specific requirements — implement Monoio backend matching existing Tokio trait implementations.

</specifics>

<deferred>
## Deferred Ideas

- Full Monoio migration (replace channels, select!, TCP — separate phase)
- Monoio DMA file I/O via monoio::fs
- Performance benchmarking Monoio vs Tokio backend
- Monoio-native io_uring integration (bypass current UringDriver)

</deferred>
