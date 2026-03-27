# Phase 42: Inline dispatch for single-shard and missing Monoio features (P1) - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Two goals: (1) Implement inline dispatch shortcut for single-shard mode to eliminate Frame allocation overhead, achieving >1.4× Redis at p=1. (2) Migrate all missing Tokio features to Monoio handler for production parity.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion.

**Part A — Inline dispatch for 1-shard GET/SET (~30-50% single-shard p=1 improvement):**
- When `num_shards == 1`, ALL commands are local — skip Frame construction entirely
- Parse command type + key directly from raw `read_buf` bytes (memchr for \r\n boundaries)
- For GET: hash key → DashTable lookup → serialize BulkString response directly into write_buf
- For SET: hash key → DashTable insert → serialize "+OK\r\n" static response
- Fallback to normal Frame-based path for complex commands (MULTI, blocking, etc.)
- This is the "parse-dispatch-serialize" shortcut that Redis uses natively

**Part B — Missing Monoio features (production parity):**
Priority order for migration from Tokio handler:
1. AUTH/requirepass gate (lines 1440-1494 → Monoio)
2. ACL permission checks (lines 2030-2076 → Monoio)
3. RESP3/HELLO protocol negotiation (lines 1462-1480 → Monoio)
4. apply_resp3_conversion calls on all response paths
5. Cluster routing: CLUSTER command, ASKING flag, slot routing, CROSSSLOT (lines 1513-1644)
6. Lua scripting: EVAL/EVALSHA/SCRIPT (lines 1540-1596)
7. REPLICAOF/SLAVEOF + REPLCONF + INFO replication (lines 1693-1765)
8. Read-only replica enforcement (lines 1768-1782)
9. BGSAVE sharded (lines 2020-2028)
10. Client tracking (CLIENT TRACKING, key invalidation) (lines 1810-1848, 2217-2236)
11. PUBLISH cross-shard fan-out (lines 1961-1997)
12. Replace blocking-wake string comparisons with command flags

</decisions>

<code_context>
## Existing Code Insights

### Tokio Handler Reference (complete implementation)
- `src/server/connection.rs:1333-2344` — has ALL features
- Each feature block is self-contained with clear line boundaries
- Can be ported block-by-block to Monoio handler

### Monoio Handler (gaps)
- `src/server/connection.rs:3104-3776` — missing 17+ feature blocks
- Parameters with `_` prefix indicate unused: `_tracking_table`, `_client_id`, `_repl_state`, `_cluster_state`, `_lua`, `_script_cache`, `_acl_table`
- Each `_` parameter maps to a missing feature

### DashTable Direct Access (for inline dispatch)
- `src/storage/dashtable/mod.rs` — `get()` returns `Option<&CompactEntry>`
- `src/storage/compact_value.rs` — `as_bytes()` returns the raw value bytes
- `src/protocol/mod.rs` — `serialize()` writes RESP format to BytesMut
- Key insight: For GET, we need: parse key from buffer → hash → segment lookup → serialize value

</code_context>

<specifics>
## Specific Ideas

For inline dispatch, the ideal flow for `GET key` is:
1. Detect `*2\r\n$3\r\nGET\r\n$N\r\n` pattern in read_buf (fixed prefix matching)
2. Extract key bytes directly from read_buf (zero-copy slice)
3. `xxhash64(key) → segment → ctrl scan → value`
4. `write_buf.extend_from_slice(b"$"); itoa(value.len()); write_buf.extend_from_slice(b"\r\n"); write_buf.extend_from_slice(value); write_buf.extend_from_slice(b"\r\n");`

This bypasses Frame construction, extract_command, dispatch table lookup, and response Frame construction.

</specifics>

<deferred>
## Deferred Ideas

None

</deferred>
