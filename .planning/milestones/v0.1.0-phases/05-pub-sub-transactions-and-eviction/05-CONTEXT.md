# Phase 5: Pub/Sub, Transactions, and Eviction - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

Channel-based messaging (SUBSCRIBE/PUBLISH with pattern support), atomic multi-command transactions (MULTI/EXEC/DISCARD with WATCH optimistic locking), and memory-bounded operation with LRU/LFU eviction policies. Also CONFIG GET/SET for runtime parameter tuning. This is the final phase — completes the v1 feature set.

</domain>

<decisions>
## Implementation Decisions

### Pub/Sub Delivery Model
- Bounded mpsc channel per subscriber (capacity 256) — prevents OOM from slow clients
- Slow subscribers that fall behind are disconnected (channel full → drop subscriber)
- Subscriber enters special mode: only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/QUIT accepted — matches Redis behavior
- Pub/Sub handled at connection level, NOT through normal command dispatch
- Global `PubSubRegistry` struct: `HashMap<Bytes, Vec<Subscriber>>` for channel subscriptions + `Vec<(pattern, Vec<Subscriber>)>` for pattern subscriptions
- PUBLISH iterates exact channel subscribers + checks all patterns via `glob_match` (reuse from command/key.rs)
- Pub/Sub messages NOT logged to AOF — fire-and-forget, not persisted
- SUBSCRIBE response format: `["subscribe", channel_name, subscription_count]` as Array

### Transaction Semantics (MULTI/EXEC/DISCARD/WATCH)
- Per-connection transaction state: `in_multi: bool`, `command_queue: Vec<Frame>`, `watched_keys: HashMap<Bytes, u64>`
- MULTI sets `in_multi = true` — subsequent commands queued instead of executed
- EXEC runs all queued commands atomically under the database lock, returns Array of results
- DISCARD clears queue and exits multi mode
- Queued commands with syntax errors: queue them anyway, EXEC returns per-command error (matches Redis 2.6+)
- WATCH tracks key version numbers — before EXEC, check if any watched key's version changed since WATCH
- If WATCH detects modification: EXEC returns Null (transaction aborted), client retries
- Key version: add `version: u64` field to Entry, increment on every mutation
- WATCH state cleared after EXEC or DISCARD (regardless of success)

### Eviction Strategy
- Eviction triggers before every write command when `used_memory > maxmemory`
- Approximated LRU: sample `maxmemory-samples` (default 5) random keys, evict the one with oldest `last_access` time
- LFU: Morris counter (8-bit logarithmic frequency) + decay over time, sample and evict lowest frequency
- Per-key metadata additions to Entry: `last_access: Instant` (for LRU), `access_counter: u8` (for LFU, Morris counter)
- Eviction policies: `noeviction` (reject writes), `allkeys-lru`, `allkeys-lfu`, `volatile-lru` (only keys with TTL), `volatile-lfu`, `allkeys-random`, `volatile-random`, `volatile-ttl` (evict shortest TTL)
- `maxmemory` tracked via estimated memory usage (key size + value size approximation)
- `maxmemory = 0` means no limit (default)

### CONFIG GET/SET
- Runtime-configurable parameters: `maxmemory`, `maxmemory-policy`, `maxmemory-samples`, `save`, `appendonly`, `appendfsync`
- CONFIG GET returns matching parameters as alternating name-value pairs (Array)
- CONFIG SET updates the live ServerConfig — changes take effect immediately
- CONFIG GET supports glob patterns (e.g., `CONFIG GET max*`)
- Not persisting config changes to file (CONFIG REWRITE deferred to v2)

### Claude's Discretion
- PubSubRegistry internal locking strategy (separate Mutex or part of Database)
- Exact memory estimation formula for eviction threshold
- Morris counter parameters (probability factor, decay time constant)
- Whether MULTI/EXEC responses include "+QUEUED" for each queued command (they should, per Redis)
- CONFIG parameter name matching (case-insensitive per Redis)
- Transaction command queue size limit (if any)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing Server Code
- `src/server/connection.rs` — Connection handler loop, auth gate pattern (model for MULTI/SUBSCRIBE mode switching)
- `src/server/listener.rs` — Task spawning, shared state distribution
- `src/server/shutdown.rs` — CancellationToken pattern
- `src/command/mod.rs` — Command dispatch, DispatchResult enum
- `src/command/key.rs` — `glob_match` for Pub/Sub pattern matching and CONFIG GET
- `src/storage/entry.rs` — Entry struct (add version, last_access, access_counter fields)
- `src/storage/db.rs` — Database struct (add eviction methods)
- `src/config.rs` — ServerConfig (add maxmemory, eviction policy fields)

### Project Research
- `.planning/research/PITFALLS.md` — Pitfall #7 (Pub/Sub backpressure)
- `.planning/research/ARCHITECTURE.md` — Approximated LRU, sample-based eviction
- `.planning/research/FEATURES.md` — Pub/Sub, Transactions, eviction feature details

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `glob_match()` in command/key.rs — for PSUBSCRIBE patterns and CONFIG GET pattern matching
- `CancellationToken` — for Pub/Sub subscriber cleanup on shutdown
- Connection handler auth gate pattern — model for MULTI mode and SUBSCRIBE mode
- `DispatchResult` enum — extend for transaction queuing and pub/sub mode
- `Arc<Mutex<Vec<Database>>>` — shared state pattern
- AOF mpsc channel pattern — model for Pub/Sub message delivery

### Established Patterns
- Command handlers: pure functions `(&mut Database, &[Frame]) -> Frame`
- Connection-level interception (AUTH, BGSAVE) — model for MULTI/SUBSCRIBE
- Background tasks with CancellationToken
- Entry constructors for new fields

### Integration Points
- `Entry` struct — add `version: u64`, `last_access: Instant`, `access_counter: u8`
- `Database` — add eviction methods, version tracking on mutations
- `Connection` handler — add transaction state, pub/sub mode, eviction check before writes
- `ServerConfig` — add maxmemory, eviction policy, maxmemory-samples
- `command/mod.rs` — add CONFIG, MULTI/EXEC/DISCARD/WATCH dispatch

</code_context>

<specifics>
## Specific Ideas

- Pub/Sub should be a separate subsystem module (src/pubsub/) — not tangled with storage
- Transaction EXEC should hold the database lock for the entire batch — true atomicity
- Eviction check should be a single function call at the top of every write handler
- CONFIG GET/SET should be extensible — easy to add new parameters later

</specifics>

<deferred>
## Deferred Ideas

- Blocking commands (BLPOP/BRPOP) — v2 (requires client wake infrastructure)
- CONFIG REWRITE — v2
- RESP3 protocol — v2
- Memory-efficient encodings — v2
- CLIENT command (CLIENT LIST, CLIENT SETNAME) — v2

</deferred>

---

*Phase: 05-pub-sub-transactions-and-eviction*
*Context gathered: 2026-03-23*
