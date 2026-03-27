# Phase 34: Monoio Pub/Sub, Blocking Commands, Transactions — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Enable Pub/Sub subscriber mode, blocking commands (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX), and MULTI/EXEC transactions under Monoio runtime. These features require select!-based multiplexing and timeout handling that are currently Tokio-only in the connection handler.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

**Tokio-specific code in connection.rs requiring monoio equivalents:**

1. **Pub/Sub subscriber mode** (~lines 148-400):
   - `tokio::select!` on mpsc receiver + stream for incoming messages
   - Subscriber enters a special loop receiving published messages
   - Needs: `monoio::select!` on flume receiver (already runtime-agnostic)

2. **Blocking commands** (~lines 2480-2640):
   - `tokio::select!` with `tokio::time::sleep` for timeout
   - `tokio::pin!(sleep)` for deadline tracking
   - Oneshot wakeup channel for data arrival
   - Needs: `monoio::time::sleep` + `monoio::select!`

3. **MULTI/EXEC transactions** (~lines 2200-2300):
   - `tokio::task::yield_now()` for fairness
   - Mostly sync logic (queue commands, execute atomically)
   - Needs: minimal changes (yield_now can be no-op under monoio)

4. **Replication spawn** (~line 1718):
   - `tokio::task::spawn_local(run_replica_task(...))` for REPLICAOF
   - Needs: `monoio::spawn` behind cfg

5. **Test macros** (pubsub/mod.rs):
   - `#[tokio::test]` — needs `#[cfg(feature = "runtime-tokio")]` gate

**Key insight from Phase 33:** `flume` channels already work under monoio with `sync` feature enabled. The main work is cfg-gating the select! + sleep patterns in connection.rs.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phase 33 cross-shard dispatch pattern in monoio handler
- `monoio::select!` already used in shard event loop (7 arms)
- flume channels with `recv_async()` work under monoio
- `monoio::time::sleep` available

### Key Files
- `src/server/connection.rs` — pub/sub, blocking, transactions (THE main file)
- `src/pubsub/mod.rs` — test-only tokio usage
- `src/blocking/mod.rs` — already uses flume channels

</code_context>

<specifics>
## Specific Ideas
End goal: SUBSCRIBE/PUBLISH, BLPOP with timeout, MULTI/EXEC all work under Monoio.
</specifics>

<deferred>
## Deferred Ideas
None.
</deferred>
