# Phase 16: RESP3 Protocol Support - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement RESP3 protocol with new frame types (Map, Set, Double, Boolean, Verbatim String, Big Number, Push), HELLO command for per-connection protocol negotiation, and CLIENT TRACKING for server-push invalidation messages enabling client-side caching. RESP2 remains the default protocol; RESP3 is opt-in via HELLO.

</domain>

<decisions>
## Implementation Decisions

### RESP3 frame types
- [auto] Extend Frame enum with new variants:
  - `Map(Vec<(Frame, Frame)>)` — prefix `%` — for HGETALL, CONFIG GET
  - `Set(Vec<Frame>)` — prefix `~` — for SMEMBERS, SUNION
  - `Double(f64)` — prefix `,` — for ZSCORE, INCRBYFLOAT
  - `Boolean(bool)` — prefix `#` — for EXISTS, SISMEMBER
  - `VerbatimString { encoding: Bytes, data: Bytes }` — prefix `=` — for large text
  - `BigNumber(Bytes)` — prefix `(` — for arbitrary precision integers
  - `Push(Vec<Frame>)` — prefix `>` — for pub/sub and invalidation messages
  - `Null` — prefix `_` — distinct from BulkString null

### HELLO command
- [auto] `HELLO [protover] [AUTH username password] [SETNAME clientname]`
- Returns server info map: server name, version, proto (2 or 3), mode, role
- Connection starts in RESP2 by default; HELLO 3 upgrades to RESP3 for that connection
- HELLO 2 downgrades back to RESP2 if needed
- Optional AUTH during HELLO (avoids separate AUTH round-trip)

### Response format switching
- [auto] Per-connection `protocol_version: u8` field (2 or 3)
- Response serializers check protocol version:
  - RESP2: HGETALL returns flat Array [k1, v1, k2, v2, ...]
  - RESP3: HGETALL returns Map [(k1, v1), (k2, v2), ...]
- Only ~20 commands have different RESP3 representations — maintain a mapping table

### CLIENT TRACKING for invalidation
- [auto] `CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]`
- Server tracks which keys each client has accessed
- When a tracked key is modified, send Push frame `> invalidate [key1, key2, ...]` to the client
- Tracking table: per-shard HashMap<key, Vec<client_id>> — invalidation is shard-local, no cross-shard coordination
- BCAST mode: broadcast invalidation for all keys matching registered prefixes
- REDIRECT mode: send invalidations to a different connection (for pub/sub subscriber)

### Claude's Discretion
- Exact tracking table memory management (LRU eviction of tracking entries under memory pressure)
- Whether to implement CLIENT CACHING YES/NO (OPTIN/OPTOUT fine-grained control)
- Push frame delivery strategy (inline with response vs out-of-band)
- CLIENT ID assignment (atomic counter, currently not implemented)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` — RESP3 mentioned briefly; main reference is Redis RESP3 spec

### REQUIREMENTS.md v2 requirements
- `ADVP-01`: Server supports RESP3 protocol negotiation via HELLO command
- `ADVP-02`: Server returns rich types (maps, sets, booleans) in RESP3 mode

### Current implementation
- `src/protocol/frame.rs` — Frame enum (130 lines) — extend with RESP3 variants
- `src/protocol/parse.rs` — RESP2 parser (458 lines) — extend for RESP3 prefixes
- `src/protocol/serialize.rs` — Serializer (205 lines) — add RESP3 encoding
- `src/server/connection.rs` — Connection handler (819 lines) — add protocol_version state
- `src/command/connection.rs` — Connection commands (346 lines) — add HELLO command

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- Frame enum — extend with new variants (backward compatible)
- Parser's first-byte dispatch — add new RESP3 prefix bytes
- Serializer — add new Frame variant handlers

### Established Patterns
- Per-connection state (authenticated, db_index) — add protocol_version
- RESP2 response assembly via Frame::Array — RESP3 adds Frame::Map alternative

### Integration Points
- Every command's response path — check protocol_version to choose RESP2 vs RESP3 format
- Pub/Sub subscriber mode — Push frames for RESP3 clients
- CLIENT TRACKING — new per-connection tracking state + per-shard tracking table

</code_context>

<specifics>
## Specific Ideas

- RESP3 was introduced in Redis 6.0 — well-established, stable spec
- CLIENT TRACKING enables significant client-side performance gains (cache hits avoid round-trip)
- Push frames make pub/sub cleaner — no need for special subscriber mode parsing

</specifics>

<deferred>
## Deferred Ideas

- CLIENT CACHING YES/NO (fine-grained opt-in/opt-out) — implement with basic TRACKING first
- RESP3 streaming (fragmented large responses) — evaluate if needed

</deferred>

---

*Phase: 16-resp3-protocol-support*
*Context gathered: 2026-03-24*
