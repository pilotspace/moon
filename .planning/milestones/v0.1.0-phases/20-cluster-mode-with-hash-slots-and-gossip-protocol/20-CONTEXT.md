# Phase 20: Cluster Mode with Hash Slots and Gossip Protocol - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement Redis Cluster compatibility with 16,384 hash slots, gossip-based cluster bus for node discovery and failure detection, MOVED/ASK redirections, live slot migration between nodes, and automatic failover (replica promotes to master on detected failure). Within a single node, slots map to local shards.

</domain>

<decisions>
## Implementation Decisions

### Hash slot scheme
- [auto] 16,384 hash slots using `CRC16(key) % 16384`
- Hash tags: `{tag}` — extract content between first `{` and first `}` as hash input for co-location
- Within a node: `slot_id % num_local_shards` maps slot to local shard
- Slot-to-node ownership tracked in cluster state (gossip-propagated)

### Cluster bus and gossip protocol
- [auto] Separate TCP port: base_port + 10000 (e.g., 16379 for data port 6379)
- Binary gossip protocol: PING/PONG messages carrying node state, slot assignments, epoch
- Gossip frequency: each node sends PING to random node every ~100ms
- Failure detection: node marked as PFAIL after ping_timeout (default 15s), FAIL after majority agrees

### MOVED/ASK redirections
- [auto] MOVED: client accessed key whose slot is permanently on another node
  - Response: `-MOVED <slot> <host>:<port>`
  - Client should update its slot map cache and retry
- ASK: slot is being migrated, key might be on target node
  - Response: `-ASK <slot> <host>:<port>`
  - Client sends ASKING to target node, then retries the command
  - ASKING flag valid for one command only

### Live slot migration
- [auto] CLUSTER SETSLOT <slot> IMPORTING <node-id> — target node accepts ASK-redirected commands
- CLUSTER SETSLOT <slot> MIGRATING <node-id> — source node redirects misses to target
- Migration coordinator iterates slot's keys, sends MIGRATE to target node
- MIGRATE: serialized key transfer (RDB format) with optional COPY/REPLACE flags
- After all keys migrated: CLUSTER SETSLOT <slot> NODE <target-node-id> on all nodes

### Automatic failover
- [auto] When master is marked FAIL by majority of masters:
  - Replica with lowest rank (smallest replication offset lag) initiates failover
  - Replica requests votes from other masters (FAILOVER_AUTH_REQUEST)
  - If majority grants votes: replica promotes itself, takes over master's slots
  - Epoch incremented on each failover for consistency
- Manual failover via CLUSTER FAILOVER (graceful, coordinated with master)

### Cluster commands
- [auto] CLUSTER MEET host port — add node to cluster
- CLUSTER ADDSLOTS/DELSLOTS — assign/remove slots
- CLUSTER INFO — cluster state (ok/fail, known nodes, slots, messages)
- CLUSTER NODES — full node list with slots and flags
- CLUSTER SLOTS — slot ranges mapped to node addresses
- CLUSTER MYID — this node's ID
- CLUSTER RESET HARD|SOFT — reset cluster state
- CLUSTER REPLICATE node-id — make this node a replica of given master

### Claude's Discretion
- Cluster state persistence (nodes.conf file format)
- Exact gossip message format (binary packing)
- Cluster bus TLS support (deferred or basic)
- CLUSTER KEYSLOT / CLUSTER COUNTKEYSINSLOT / CLUSTER GETKEYSINSLOT utility commands
- Whether to implement cluster proxy mode (route commands internally vs redirect)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Cluster design and multi-key coordination — 16384 hash slots, gossip, MOVED/ASK, slot migration, replication coordination

### Phase dependencies
- `.planning/phases/19-replication-with-psync2-protocol/19-CONTEXT.md` — Replication needed for cluster HA (replica promotion)
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Local shard mapping: slot_id % num_local_shards

### Current implementation
- `src/server/listener.rs` — TCP listener — add cluster bus listener on port+10000
- `src/command/mod.rs` — Command dispatch — add CLUSTER command family
- `src/main.rs` — Server startup — cluster initialization and node ID generation

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- TCP connection handling — cluster bus connections are TCP
- RDB serialization — MIGRATE uses RDB format for key transfer
- CRC32 — already have `crc32fast` crate; CRC16 implementation needed separately

### Established Patterns
- Command dispatch with subcommands (CONFIG GET/SET) — CLUSTER follows same pattern
- Server configuration via clap — add cluster-specific flags

### Integration Points
- Every command must check slot ownership before execution — add pre-dispatch slot routing
- Cross-slot commands (MGET with keys on different slots) return CROSSSLOT error
- Pub/Sub in cluster: publish propagated to all nodes in cluster (cluster bus)
- Replication (Phase 19) integrated with cluster for automatic failover

</code_context>

<specifics>
## Specific Ideas

- Blueprint warns: compare against Redis Cluster (40 shards) not single Redis — fair benchmarking
- Redis team showed 40-shard cluster achieves 4.74M ops/sec — our target is 5-7M single-process
- Cluster mode adds operational overhead — the single-process advantage is eliminating 40 separate processes

</specifics>

<deferred>
## Deferred Ideas

- Cluster proxy mode (route internally instead of redirect) — future optimization
- Redis Cluster bus encryption — future security phase
- Cross-slot transaction support — extremely complex, out of scope

</deferred>

---

*Phase: 20-cluster-mode-with-hash-slots-and-gossip-protocol*
*Context gathered: 2026-03-24*
