# Phase 26: Cluster Failover Completion — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Complete the cluster failover implementation: PFAIL→FAIL majority consensus, async election task with jittered delay and replica ranking, bus handler processing for FailoverAuthRequest/Ack messages, and CLUSTER FAILOVER [FORCE|TAKEOVER] command.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key constraints from deep dive analysis:

**Current state (what exists):**
- `check_and_initiate_failover()` — promotes replica, copies slots, but NEVER CALLED
- `failover_vote_eligible()` / `handle_failover_auth_request()` — vote guard works, NEVER CALLED
- Gossip PING/PONG/MEET fully working
- FailoverAuthRequest/Ack message types defined in GossipMsgType enum
- Bus handler: receives failover messages but logs debug NOOP
- PFAIL detection works (100ms gossip ticker)
- PFAIL→FAIL: single node marks, NO majority consensus
- CLUSTER FAILOVER command: stub returning OK unconditionally
- Per-shard replication offsets tracked (ready for replica ranking)
- 3 unit tests for failover basics pass

**What needs implementing:**
1. PFAIL→FAIL majority consensus (track pfail_reports per node, quorum = masters/2 + 1)
2. Election task (async, jittered delay, sends FailoverAuthRequest, collects votes)
3. Bus handler: process FailoverAuthRequest → call handle_failover_auth_request → send Ack
4. Bus handler: process FailoverAuthAck → forward to election task via channel
5. Gossip ticker: trigger election when replica detects master FAIL
6. CLUSTER FAILOVER [FORCE|TAKEOVER] command
7. Replica ranking by replication offset for tie-breaking

**Key files:**
- `src/cluster/failover.rs` (112 lines) — election logic
- `src/cluster/gossip.rs` (491 lines) — PFAIL detection, gossip ticker
- `src/cluster/bus.rs` (130 lines) — message handlers
- `src/cluster/command.rs` (590 lines) — CLUSTER subcommands
- `src/cluster/mod.rs` (384 lines) — ClusterState, NodeFlags
- `src/replication/state.rs` (204 lines) — per-shard offsets

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/cluster/failover.rs` — `check_and_initiate_failover()` and `handle_failover_auth_request()` already implemented
- `src/cluster/gossip.rs` — `GossipMessage` serialization/deserialization fully working
- `src/cluster/bus.rs` — TCP bus listener and peer handler framework
- `src/replication/state.rs` — `ReplicationState` with per-shard `AtomicU64` offsets

### Established Patterns
- Gossip ticker runs every 100ms on listener runtime
- Cluster bus on port+10000, length-prefixed framing
- ClusterState protected by `Arc<RwLock<ClusterState>>`
- NodeFlags enum: Master, Replica { master_id }, Pfail, Fail
- nodes.conf persistence via atomic .tmp+rename

### Integration Points
- `src/main.rs` — spawns `run_cluster_bus()` and `run_gossip_ticker()`
- `src/cluster/gossip.rs:check_failure_states()` — current PFAIL detection
- `src/cluster/bus.rs:handle_gossip_message()` — where failover messages need processing
- `src/cluster/command.rs` — CLUSTER FAILOVER stub at line 44

</code_context>

<specifics>
## Specific Ideas

No specific requirements — infrastructure phase. Follow Redis Cluster spec for failover protocol.

</specifics>

<deferred>
## Deferred Ideas

- Pre-vote phase (prevents disrupted nodes from triggering elections)
- CLUSTER COUNTKEYSINSLOT implementation (returns 0 currently)
- Slave-of-slave replication validation
- Failover state persistence across restarts

</deferred>
