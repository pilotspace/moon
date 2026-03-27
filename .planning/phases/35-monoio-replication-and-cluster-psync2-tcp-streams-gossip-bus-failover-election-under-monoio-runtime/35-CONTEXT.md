# Phase 35: Monoio Replication and Cluster — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Enable replication (PSYNC2) and cluster mode (gossip, bus, failover) under Monoio runtime. These are the last Tokio-only subsystems. After this phase, the Monoio binary has full feature parity.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

**Remaining Tokio-specific code:**

1. **Replication master** (`src/replication/master.rs`):
   - `tokio::io::AsyncWriteExt` for RDB/WAL streaming to replicas
   - `tokio::net::tcp::OwnedWriteHalf` for replica writer
   - `tokio::spawn` for replica sender tasks
   - `tokio::sync::Mutex` for shared write half
   - Strategy: cfg-gate with monoio::net::TcpStream split + monoio::spawn

2. **Replication replica** (`src/replication/replica.rs`):
   - `tokio::io::{AsyncReadExt, AsyncWriteExt}` for handshake + streaming
   - `tokio::net::TcpStream` for master connection
   - `tokio::time::sleep` for reconnect backoff
   - Strategy: cfg-gate with monoio TCP + ownership I/O pattern from Phase 32

3. **Cluster bus** (`src/cluster/bus.rs`):
   - `tokio::net::{TcpListener, TcpStream}` for cluster bus port
   - `tokio::spawn` for peer handlers
   - `tokio::select!` for accept + shutdown
   - `tokio::io::{AsyncReadExt, AsyncWriteExt}` for gossip messages
   - Strategy: monoio::net + monoio::spawn + monoio::select!

4. **Cluster gossip** (`src/cluster/gossip.rs`):
   - `tokio::net::TcpStream` for peer connections
   - `tokio::spawn` for async gossip sends
   - `tokio::select!` for ticker + shutdown
   - `tokio::io::{AsyncReadExt, AsyncWriteExt}`
   - Strategy: monoio equivalents behind cfg

5. **Cluster failover** (`src/cluster/failover.rs`):
   - `tokio::io::AsyncWriteExt` for auth request sends
   - `tokio::net::TcpStream` for peer connections
   - Strategy: cfg-gate, reuse gossip TCP pattern

**Key challenge:** Monoio's ownership-based TCP I/O (`read(buf)` takes ownership) vs Tokio's borrow-based (`read_buf(&mut buf)`). Established pattern from Phase 32/33: read into Vec<u8>, copy to BytesMut.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phase 32 monoio TCP ownership pattern (connection.rs)
- Phase 33 cross-shard + flume channel patterns
- Phase 34 monoio::select! patterns for complex event loops
- monoio::spawn established pattern

### Key Files
- `src/replication/master.rs` (~300 lines)
- `src/replication/replica.rs` (~270 lines)
- `src/cluster/bus.rs` (~130 lines)
- `src/cluster/gossip.rs` (~500 lines)
- `src/cluster/failover.rs` (~200 lines)

</code_context>

<specifics>
## Specific Ideas
End goal: Full Redis Cluster with replication works under `runtime-monoio`. REPLICAOF, PSYNC, CLUSTER MEET, gossip PING/PONG, automatic failover all functional.
</specifics>

<deferred>
## Deferred Ideas
None — this is the final migration phase.
</deferred>
