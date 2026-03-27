# Phase 19: Replication with PSYNC2 Protocol - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement PSYNC2-compatible master-replica replication with per-shard WAL streaming, partial resynchronization for brief disconnections, and full resynchronization via RDB transfer. Replicas serve read-only queries during replication. Per-shard replication backlog enables shard-level partial resync (only catch up on shards that fell behind).

</domain>

<decisions>
## Implementation Decisions

### Replication protocol
- [auto] PSYNC2 compatible — replicas send `PSYNC <repl_id> <offset>` to master
- Master responds with `+FULLRESYNC <repl_id> <offset>` (full sync) or `+CONTINUE` (partial sync)
- Replication ID survives restarts (persisted in RDB/WAL metadata)
- Second replication ID (repl_id2) maintained for failover scenarios

### Full resynchronization
- [auto] Master sends RDB snapshot followed by buffered commands since snapshot epoch
- Per-shard: each shard streams its snapshot independently (parallel transfer)
- Replica loads per-shard snapshots, then applies per-shard command stream
- During full resync, replica can serve stale reads (configurable: allow or reject)

### Partial resynchronization
- [auto] Per-shard circular replication backlog (default 1MB per shard)
- On reconnection: replica sends last known offset; if within backlog, partial resync
- Per-shard granularity: only catch up on shards that fell behind — major improvement over Redis's single-backlog
- If offset too old (outside backlog), fall back to full resync for that shard

### Per-shard WAL streaming
- [auto] Each shard's WAL (from Phase 14) is the replication stream source
- Master streams WAL entries to connected replicas in real-time
- WAL format is RESP wire format — same format streamed to replicas
- Backlog is a view into the WAL circular buffer

### Replica behavior
- [auto] Replicas are read-only by default (`replica-read-only yes`)
- All write commands rejected with `READONLY` error on replicas
- Replicas execute expiration independently (lazy + active) to avoid serving expired data
- INFO replication reports: role (master/replica), connected replicas, replication offset, lag

### Commands
- [auto] REPLICAOF host port — initiate replication (SLAVEOF as alias)
- REPLICAOF NO ONE — promote replica to master
- REPLCONF — replication configuration handshake (listening-port, capa eof, capa psync2)
- WAIT numreplicas timeout — block until N replicas acknowledge offset (uses Phase 17 blocking)

### Claude's Discretion
- Replication backlog size per shard (1MB default, configurable)
- Heartbeat interval between master and replicas (default 10s)
- Whether to support multiple replicas simultaneously (yes, must handle)
- Replica priority for failover election (sentinel compatibility)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Cluster design and multi-key coordination — PSYNC2-compatible replication, per-shard backlog

### Phase dependencies
- `.planning/phases/14-forkless-compartmentalized-persistence/14-CONTEXT.md` — Per-shard WAL is the replication stream source
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Per-shard architecture for parallel replication
- `.planning/phases/17-blocking-commands/17-CONTEXT.md` — WAIT command uses blocking infrastructure

### Current implementation
- `src/persistence/rdb.rs` — RDB snapshot logic (668 lines) — used for full resync transfer
- `src/persistence/aof.rs` — AOF/WAL (726 lines) — WAL entries are the replication stream
- `src/server/listener.rs` — Server startup (175 lines) — add replica connection handling
- `src/command/connection.rs` — Connection commands (346 lines) — add REPLICAOF, REPLCONF

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- RDB snapshot serialization — used for full resync data transfer
- AOF/WAL RESP format — same format streamed to replicas
- TCP connection handling — replica connections are special-case connections

### Established Patterns
- BGSAVE mechanism — full resync triggers similar background snapshot
- Per-connection state management — replica connections have replication state

### Integration Points
- Server needs replica registry: track connected replicas, their offsets, and shard sync state
- Each shard must support streaming WAL to replica connections
- Listener must accept incoming replica connections (PSYNC handshake)
- INFO command updated with replication section

</code_context>

<specifics>
## Specific Ideas

- Per-shard partial resync is a unique advantage over Redis — failed replica only catches up on affected shards
- Blueprint: "This is a significant improvement over Redis's single-backlog design"
- WAIT command with per-shard awareness: ack when N replicas have caught up on all shards

</specifics>

<deferred>
## Deferred Ideas

- Sentinel compatibility (automatic failover) — separate future phase
- Active-active replication (multi-master, KeyDB-style) — out of scope
- Diskless replication (stream RDB directly without writing to disk) — future optimization

</deferred>

---

*Phase: 19-replication-with-psync2-protocol*
*Context gathered: 2026-03-24*
