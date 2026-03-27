# Phase 18: Streams Data Type - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement Redis Streams with radix tree + listpack storage. Core commands: XADD, XLEN, XRANGE, XREVRANGE, XTRIM, XREAD (including BLOCK), XINFO. Consumer groups: XGROUP CREATE/DESTROY/SETID, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM. Streams are the most complex Redis data type — essentially an embedded message broker.

</domain>

<decisions>
## Implementation Decisions

### Storage structure
- [auto] Radix tree keyed by stream entry ID prefix (timestamp portion)
- Leaf nodes are listpacks (from Phase 15) containing multiple entries sharing the same timestamp prefix
- Entry ID format: `<ms-timestamp>-<sequence>` — auto-generated or explicit
- This structure enables efficient range queries and memory-compact storage

### Entry ID generation
- [auto] Auto-ID: current millisecond timestamp + auto-incrementing sequence within that millisecond
- Last generated ID tracked per stream to ensure monotonically increasing IDs
- Explicit IDs accepted if strictly greater than last generated ID
- Incomplete IDs (e.g., `*`, `1234-*`) auto-completed

### Core stream commands
- [auto] XADD: append entry with field-value pairs, optional MAXLEN/MINID trimming
- XLEN: entry count
- XRANGE/XREVRANGE: range query by ID with COUNT limit
- XTRIM: cap stream by MAXLEN (exact or approximate ~) or MINID
- XREAD: read from multiple streams, optionally BLOCK with timeout (uses Phase 17 blocking infra)
- XINFO: introspection (STREAM, GROUPS, CONSUMERS subcommands)

### Consumer groups
- [auto] Per-stream consumer group state:
  - Group name, last-delivered-id, consumer list
  - Pending entries list (PEL): entries delivered but not yet acknowledged
- XREADGROUP: deliver new entries to specific consumer, track in PEL
- XACK: mark entries as processed, remove from PEL
- XPENDING: report unacknowledged entries per consumer
- XCLAIM: transfer ownership of pending entries to different consumer (for stuck consumers)
- XAUTOCLAIM: auto-claim idle pending entries

### RedisValue extension
- [auto] Add `Stream` variant to RedisValue enum containing:
  - Radix tree (custom or BTreeMap<StreamId, ListPack> as initial implementation)
  - Last generated ID
  - Consumer groups: HashMap<Bytes, ConsumerGroup>
  - Entry count and metadata

### Claude's Discretion
- Whether to implement a proper radix tree or use BTreeMap<u128, Vec<u8>> as simpler initial storage
- Approximate trimming strategy (~ prefix on MAXLEN)
- XINFO output format details
- Whether radix tree nodes use listpack encoding from Phase 15 or a simpler Vec<Entry>

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Concrete implementation roadmap Phase 4 — mentions "streams (radix tree + listpack)"

### Phase dependencies
- `.planning/phases/15-b-plus-tree-sorted-sets-and-optimized-collection-encodings/15-CONTEXT.md` — Listpack encoding for stream entry storage
- `.planning/phases/17-blocking-commands/17-CONTEXT.md` — XREAD BLOCK uses blocking infrastructure

### Current implementation
- `src/storage/entry.rs` — RedisValue enum (line 28-36) — add Stream variant
- `src/command/mod.rs` — Command dispatch (421 lines) — add stream command routing
- `src/persistence/rdb.rs` — RDB serialization — add Stream type support

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- Listpack implementation from Phase 15 — leaf node storage for stream entries
- Blocking infrastructure from Phase 17 — XREAD BLOCK support
- Frame serialization — stream responses are standard RESP arrays

### Established Patterns
- RedisValue enum with type-checked command dispatch — add Stream variant with same pattern
- WRONGTYPE error for type mismatches — extend for Stream type
- SCAN-style cursor iteration — XRANGE uses entry IDs as cursors

### Integration Points
- New `src/command/stream.rs` module for all stream commands
- RDB/AOF persistence must serialize/deserialize Stream data
- Active expiration supports TTL on stream keys (same as other types)
- OBJECT ENCODING reports "stream" for stream keys

</code_context>

<specifics>
## Specific Ideas

- Streams are used for event sourcing, message queues, activity feeds — high-value feature
- Consumer groups enable at-least-once delivery semantics — critical for reliability
- Redis Streams process billions of entries/day at companies like Twitter and Pinterest

</specifics>

<deferred>
## Deferred Ideas

- XSETID command (set last generated ID) — low priority, implement if time permits
- Stream replication specifics — Phase 19

</deferred>

---

*Phase: 18-streams-data-type*
*Context gathered: 2026-03-24*
