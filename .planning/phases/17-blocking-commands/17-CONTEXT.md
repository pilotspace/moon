# Phase 17: Blocking Commands - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement blocking list and sorted-set commands: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX. These block the connection fiber/task until data becomes available or a configurable timeout expires. Fair FIFO wakeup ordering when new data arrives. Cross-shard blocking coordination for multi-key BLPOP.

</domain>

<decisions>
## Implementation Decisions

### Blocking mechanism
- [auto] In thread-per-core model: blocked connection's fiber yields to the shard scheduler
- Blocked fiber stored in a per-shard wait queue: HashMap<key, VecDeque<WaitingClient>>
- When LPUSH/RPUSH/ZADD adds data to a watched key, check wait queue and wake first blocked client
- Zero CPU consumption while blocked — fiber is suspended, not polling

### Wait queue structure
- [auto] Per-shard: `HashMap<Bytes, VecDeque<BlockedClient>>` where BlockedClient contains:
  - Client ID / fiber wakeup handle
  - Command type (BLPOP, BRPOP, etc.)
  - Timeout deadline (absolute timestamp)
  - Response channel (oneshot sender)

### FIFO fairness
- [auto] First client to block on a key is served first when data arrives (VecDeque maintains insertion order)
- BLPOP with multiple keys: client is registered in wait queues for ALL listed keys; first key with data wins
- On wakeup: remove client from all other wait queues to prevent double-service

### Timeout handling
- [auto] Timeout precision within 10ms (timer wheel or per-shard timer task)
- 0 timeout = block indefinitely (no timeout registration)
- On timeout: return nil response, remove from all wait queues
- Per-shard timer task scans for expired blocked clients periodically (every 10ms)

### Cross-shard blocking (multi-key BLPOP)
- [auto] BLPOP key1 key2 key3 — keys may be on different shards
- Coordinator on connection's shard registers interest in all target shards via SPSC channels
- Each target shard registers the client in its local wait queue for the relevant key
- First shard to have data wakes the coordinator, which cancels registration on other shards
- Complexity: moderate — but multi-key blocking is rare in production

### BLMOVE specifics
- [auto] BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
- Atomic pop from source + push to destination
- If source is empty, block until data available; then atomically move
- Source and destination may be on different shards — requires cross-shard coordination

### Claude's Discretion
- Timer implementation (tokio timer, custom timer wheel, or event loop deadline)
- Maximum number of blocked clients per key (to prevent memory unbounded growth)
- Whether WAIT command (blocking replication acknowledgment) is in scope here or Phase 19

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### REQUIREMENTS.md v2 requirements
- `BLOCK-01`: User can BLPOP/BRPOP for blocking list pops with timeout
- `BLOCK-02`: User can BZPOPMIN/BZPOPMAX for blocking sorted set pops

### Phase dependencies
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Fiber-per-connection model needed for non-blocking blocking (fiber yield)

### Current implementation
- `src/command/list.rs` — LPUSH/RPUSH (1325 lines) — wakeup hooks added after push operations
- `src/command/sorted_set.rs` — ZADD (2466 lines) — wakeup hooks after score additions
- `src/server/connection.rs` — Connection handler (819 lines) — blocking state management

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- LPOP/RPOP logic in list.rs — BLPOP/BRPOP use same pop logic, just with blocking wrapper
- ZPOPMIN/ZPOPMAX in sorted_set.rs — BZPOPMIN/BZPOPMAX use same logic with blocking wrapper

### Established Patterns
- Command dispatch returns Frame response — blocking commands return either immediate Frame or a "blocking" signal
- Per-connection state (transaction queue, subscription list) — add blocked state

### Integration Points
- LPUSH/RPUSH must check wait queue after successful push — new post-operation hook
- ZADD must check wait queue after successful add — new post-operation hook
- Connection handler needs "blocked" state that pauses normal command processing
- Shard event loop must support fiber suspension/wakeup

</code_context>

<specifics>
## Specific Ideas

- Blocking commands are critical for job queues (Sidekiq, Celery use BRPOP extensively)
- Fair FIFO ordering is non-negotiable — starvation would break queue semantics
- Cross-shard BLPOP is the hard case; single-shard blocking (single key) should be trivially efficient

</specifics>

<deferred>
## Deferred Ideas

- WAIT command (block until replicas acknowledge) — Phase 19 (replication)
- XREAD BLOCK (stream blocking reads) — Phase 18 (streams)

</deferred>

---

*Phase: 17-blocking-commands*
*Context gathered: 2026-03-24*
