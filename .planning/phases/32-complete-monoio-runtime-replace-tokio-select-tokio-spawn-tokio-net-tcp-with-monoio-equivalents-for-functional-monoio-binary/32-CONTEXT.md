# Phase 32: Complete Monoio Runtime — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace all remaining direct Tokio usage (select!, spawn, TCP, AsyncRead/Write) with Monoio equivalents behind `#[cfg(feature = "runtime-monoio")]` blocks so the binary actually runs under Monoio — not just compiles.

Phase 31 made the monoio build compile (0 errors) but the TCP listener, event loops, and task spawning are stubbed. This phase fills those stubs with real Monoio implementations.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

**Remaining Tokio-specific call sites (65 total):**

1. **tokio::select! (19 sites, 8 files)** — the event loop multiplexer
   - `src/shard/mod.rs` — 6-arm shard event loop (MOST CRITICAL)
   - `src/server/connection.rs` — per-connection handler, pub/sub, blocking
   - `src/server/listener.rs` — accept loop
   - `src/persistence/aof.rs` — AOF writer loop
   - `src/persistence/auto_save.rs` — auto-save timer
   - `src/server/expiration.rs` — expiry background task
   - `src/cluster/bus.rs` — cluster bus listener
   - `src/cluster/gossip.rs` — gossip ticker
   - **Strategy:** `futures::select!` is NOT a drop-in (requires `Unpin + FusedFuture`). Use `monoio::select!` if available, or manual poll loop with `futures::future::poll_fn`.

2. **tokio::spawn / spawn_local (22 sites, 10 files)** — task creation
   - `src/main.rs` — background tasks (auto-save, cluster, signal)
   - `src/server/listener.rs` — connection handlers, Ctrl+C
   - `src/shard/mod.rs` — spawn_local for connections
   - `src/cluster/bus.rs` — peer handlers
   - `src/cluster/gossip.rs` — gossip send
   - `src/replication/master.rs` — replica sender
   - `src/command/persistence.rs` — BGSAVE spawn_blocking
   - **Strategy:** `monoio::spawn` for local tasks. `spawn_blocking` → `std::thread::spawn` wrapper.

3. **tokio::net (10 sites, 6 files)** — TCP networking
   - `TcpListener::bind` + `accept()` in listener, cluster bus
   - `TcpStream` in connection, replication, cluster
   - `OwnedWriteHalf` in replication master
   - **Strategy:** `monoio::net::TcpListener` + `monoio::net::TcpStream` behind cfg.

4. **tokio::io::AsyncReadExt/AsyncWriteExt (~14 sites)** — stream I/O
   - `read_buf`, `write_all`, `flush` in replication, cluster, AOF
   - **Strategy:** `monoio::io::AsyncReadRent`/`AsyncWriteRent` (GAT-based, different API). May need adapter layer.

5. **tokio::signal::ctrl_c (2 sites)** — graceful shutdown
   - **Strategy:** `signal-hook` crate or `ctrlc` crate (runtime-agnostic).

**Monoio API differences from Tokio:**
- `monoio::net::TcpStream` uses ownership-based I/O (GAT): `read(buf)` takes ownership of buffer, returns `(result, buf)`
- No `AsyncReadExt`/`AsyncWriteExt` traits — uses `AsyncReadRent`/`AsyncWriteRent`
- `monoio::select!` exists (similar to tokio::select!)
- `monoio::spawn` returns `JoinHandle` (drop = detach, like tokio::spawn)
- `monoio::net::TcpListener` has `accept()` returning `(TcpStream, SocketAddr)`

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/runtime/monoio_impl.rs` — 6 trait implementations (Phase 30)
- `src/runtime/channel.rs` — flume-based channels (Phase 31)
- `src/runtime/cancel.rs` — CancellationToken (Phase 31)
- All cfg-gating patterns established in Phases 29-31

### Established Patterns
- `#[cfg(feature = "runtime-tokio")]` / `#[cfg(feature = "runtime-monoio")]` dual blocks
- Trait-based abstraction in `src/runtime/` module
- flume channels work with any runtime

### Key Files to Modify
- `src/shard/mod.rs` (~1300 lines) — shard event loop, spawn_local, select!
- `src/server/connection.rs` (~2500 lines) — TCP stream, select!, codec
- `src/server/listener.rs` (~250 lines) — TcpListener, spawn, signal
- `src/main.rs` (~300 lines) — runtime creation, spawn, signal
- `src/persistence/aof.rs` (~300 lines) — async file I/O, select!
- `src/replication/master.rs` (~300 lines) — TCP, spawn, AsyncWrite
- `src/replication/replica.rs` (~270 lines) — TCP, AsyncRead/Write
- `src/cluster/bus.rs` (~130 lines) — TcpListener, spawn, select!
- `src/cluster/gossip.rs` (~500 lines) — spawn, select!, TCP

</code_context>

<specifics>
## Specific Ideas

End goal: `cargo run --no-default-features --features runtime-monoio -- --port 6400 --shards 1` starts a working server that handles redis-cli SET/GET.

</specifics>

<deferred>
## Deferred Ideas

- Monoio-native io_uring (bypass UringDriver, use monoio's built-in)
- Performance benchmark Monoio vs Tokio
- Full integration test suite under Monoio
- Monoio multi-shard with cross-shard channels

</deferred>
