# Phase 11: Thread-per-Core Shared-Nothing Architecture - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Migrate from Tokio multi-threaded runtime with `Arc<Mutex<Database>>` shared across all tasks to a thread-per-core shared-nothing architecture. Each CPU core runs an independent shard with its own async event loop, DashTable partition, expiry index, and memory pool. Zero shared mutable state between shards. This is THE fundamental architectural change that determines the throughput ceiling — the single highest-impact phase in the entire roadmap.

</domain>

<decisions>
## Implementation Decisions

### Runtime selection
- [auto] Use Glommio as primary runtime — Seastar-heritage thread-per-core design with three io_uring rings per thread, cooperative scheduling with latency classes, NUMA-aware allocation
- Fallback: Monoio if Glommio's API surface proves too restrictive (ByteDance production-proven, GAT-based I/O traits)
- Both provide io_uring integration (Phase 12) naturally — choosing the runtime now avoids a second migration
- macOS development: Glommio has limited macOS support — may need conditional compilation with Tokio fallback for dev

### Shard architecture
- [auto] One shard per CPU core, each containing: DashTable (from Phase 10), expiry index, connection list, memory pool
- Shard struct is fully owned by its thread — no Arc, no Mutex, no shared references
- Each shard runs a single-threaded async event loop — all operations on shard data are sequential within the shard

### Keyspace partitioning
- [auto] `xxhash64(key) % num_shards` for key-to-shard mapping
- For future Redis Cluster compatibility: `CRC16(key) % 16384` → `slot % num_shards`
- Hash tags `{tag}` recognized: extract content between first `{` and first `}` as the hash input

### Connection handling
- [auto] Listener on core 0 accepts connections, assigns to shard via `hash(client_fd) % num_shards` or round-robin
- Each connection pinned to its assigned shard thread for lifetime
- Connection fiber/task handles all I/O for that connection within the shard's event loop

### Cross-shard communication
- [auto] SPSC (single-producer single-consumer) lock-free channels between each shard pair
- For cross-shard dispatch: connection sends request to target shard's SPSC channel, awaits response
- Channel implementation: `crossbeam-channel` SPSC or custom ring buffer (Glommio provides SharedChannel)

### Multi-key command coordination (VLL pattern)
- [auto] Multi-key commands (MGET, MSET, DEL with multiple keys, transactions) use the VLL (Very Lightweight Locking) pattern
- Coordinator fiber on the connection's shard identifies all involved shards
- Dispatches sub-operations to each shard via SPSC channels in ascending shard-ID order (prevents deadlocks)
- Collects results, assembles response
- Single-shard commands (>95% of workload) bypass coordination entirely — zero overhead on fast path

### Migration strategy
- [auto] Phased migration — NOT a big-bang rewrite:
  1. First: extract Shard struct containing Database + expiry + connections
  2. Then: run N shards on Tokio `current_thread` runtimes (one per OS thread) — validates the API without Glommio
  3. Finally: swap to Glommio runtime with io_uring
- All existing command handlers work unchanged — they operate on a `&mut Database` which is now per-shard
- Integration tests updated to connect to the new listener and exercise cross-shard commands

### Claude's Discretion
- Exact connection-to-shard assignment strategy (fd hash vs round-robin vs least-connections)
- SPSC channel buffer sizes and backpressure strategy
- Whether to implement a connection migration protocol (move connection to key's shard) or always use cross-shard dispatch
- Cooperative yield points within command execution (how often to yield to the event loop)
- Number of shards: auto-detect CPU count or configurable via CLI flag

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §The target architecture: thread-per-core shared-nothing — Full architecture diagram, shard layout, SPSC channels
- `.planning/architect-blue-print.md` §Cluster design and multi-key coordination — VLL pattern for cross-shard transactions
- `.planning/architect-blue-print.md` §What the competition proved is possible — Dragonfly EngineShard architecture reference

### Current implementation (to be restructured)
- `src/server/listener.rs` — Current TCP listener (175 lines) — will become the accept loop on core 0
- `src/server/connection.rs` — Connection handler (819 lines) — will run as fiber within shard
- `src/storage/db.rs` — Database struct (773 lines) — becomes per-shard owned data
- `src/storage/eviction.rs` — Eviction (437 lines) — becomes per-shard eviction
- `src/server/expiration.rs` — Active expiry task (139 lines) — becomes per-shard cooperative task
- `src/pubsub/mod.rs` — PubSubRegistry (359 lines) — must handle cross-shard pub/sub fan-out
- `src/main.rs` — Entry point (20 lines) — will bootstrap N shard threads

### Research
- `.planning/research/ARCHITECTURE.md` — Original architecture decisions
- `.planning/research/PITFALLS.md` — Known pitfalls (borrow checker, async lifetime issues)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- All command handlers in `src/command/*.rs` — operate on `&mut Database`, fully reusable per-shard
- `dispatch()` and `dispatch_read()` — command routing logic unchanged
- RESP parser and codec — reusable per-connection within any runtime
- Persistence (RDB/AOF) — needs adaptation for per-shard snapshots but core logic reusable

### Established Patterns
- `Arc<Vec<parking_lot::RwLock<Database>>>` — current 16-database model; becomes N shards × 16 databases per shard
- Pipeline batching (Phase 7) — `now_or_never()` pattern works within single-threaded event loop
- Auth/Pub/Sub/Transactions handled at connection level — connection fiber pattern preserves this

### Integration Points
- `main.rs` → shard bootstrap: spawn N OS threads, each with event loop
- Listener → connection assignment: accept loop distributes connections to shard threads
- Cross-shard dispatch: new SPSC channel infrastructure between all shard pairs
- Pub/Sub: message published on one shard must fan out to subscribers on other shards
- Persistence: each shard snapshots independently or coordinates for consistent point-in-time

</code_context>

<specifics>
## Specific Ideas

- Blueprint reference: Monoio achieves ~2x Tokio throughput at 4 cores, ~3x at 16 cores
- Dragonfly's EngineShard is the architectural template — each shard is an independent mini-Redis
- Critical: the fast path (single-key commands on local shard) must have ZERO cross-shard overhead
- VLL deadlock prevention via ascending shard-ID ordering is non-negotiable for correctness

</specifics>

<deferred>
## Deferred Ideas

- io_uring integration — Phase 12 (can start with epoll/kqueue within the new runtime)
- NUMA-aware memory placement — Phase 13
- Per-shard memory arenas — Phase 13

</deferred>

---

*Phase: 11-thread-per-core-shared-nothing-architecture*
*Context gathered: 2026-03-24*
