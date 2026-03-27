# Phase 33: Monoio Multi-Shard and Persistence — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Enable multi-shard operation and persistence under Monoio runtime. Currently only single-shard works. This phase adds: cross-shard dispatch via SPSC channels, connection distribution across shards, AOF writer task, auto-save, expiration background task, and spawn_blocking replacements.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

**Files requiring monoio cfg blocks:**

1. `src/shard/mod.rs` — Multi-shard SPSC drain, cross-shard message handling (partially done in Phase 32)
2. `src/shard/mesh.rs` — ChannelMesh setup for N shards
3. `src/shard/coordinator.rs` — Cross-shard dispatch with oneshot replies
4. `src/shard/dispatch.rs` — ShardMessage types (already use flume channels)
5. `src/persistence/aof.rs` — AOF writer loop with select! + interval + channel recv
6. `src/persistence/auto_save.rs` — Auto-save timer loop with select! + watch recv
7. `src/server/expiration.rs` — Expiry background task with tokio::time::interval + select!
8. `src/command/key.rs` — `tokio::task::spawn_blocking(move || drop(entry))` → `std::thread::spawn`
9. `src/command/persistence.rs` — `tokio::task::spawn_blocking` for BGSAVE → `std::thread::spawn`

**Strategy:**
- `monoio::spawn` for local tasks (replaces `tokio::task::spawn_local`)
- `monoio::select!` for event loops (replaces `tokio::select!`)
- `monoio::time::interval` / `monoio::time::sleep` for timers
- `std::thread::spawn` + flume channel for blocking work (replaces `spawn_blocking`)
- SPSC ringbuf channels are already runtime-agnostic
- flume channels are already runtime-agnostic

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/runtime/channel.rs` — flume-based channels (runtime-agnostic)
- `src/runtime/cancel.rs` — CancellationToken (runtime-agnostic)
- Phase 32 shard event loop already has monoio::select! pattern
- SPSC ringbuf is runtime-agnostic (no async, just try_push/try_pop)

### Integration Points
- `src/main.rs` — already spawns shard threads with monoio cfg path
- `src/shard/mod.rs` — has monoio event loop, needs multi-shard wiring

</code_context>

<specifics>
## Specific Ideas

End goal: `cargo run --no-default-features --features runtime-monoio -- --port 6400 --shards 4` starts with 4 shards, cross-shard MGET works, AOF persistence works.

</specifics>

<deferred>
## Deferred Ideas
None — this phase covers all multi-shard + persistence requirements.
</deferred>
