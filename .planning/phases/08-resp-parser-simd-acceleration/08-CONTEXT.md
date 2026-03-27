# Phase 8: RESP Parser SIMD Acceleration - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace the hand-written byte-by-byte RESP2 parser in `src/protocol/parse.rs` with memchr-accelerated SIMD scanning for CRLF detection, add fast integer parsing via the `atoi` crate, and introduce per-connection `bumpalo::Bump` arena allocation for O(1) bulk deallocation of parsing temporaries. The parser API and Frame enum remain unchanged — this is a pure performance optimization with zero behavioral changes.

</domain>

<decisions>
## Implementation Decisions

### SIMD scanning strategy
- [auto] Use `memchr` crate (v2.8+) for all CRLF scanning — it auto-detects SSE2/AVX2/NEON at runtime
- Replace the `Cursor`-based byte-by-byte scan in `parse_single_frame` with `memchr::memchr(b'\r', buf)` followed by `buf[pos+1] == b'\n'` verification
- Use `memchr::memchr2(b'\r', b'\n', buf)` where detecting either terminator is useful for validation
- Do NOT hand-roll AVX2 intrinsics — `memchr` already implements the optimal SIMD path internally

### Integer parsing
- [auto] Use `atoi` crate for parsing RESP length prefixes and integer frame values
- Replace the hand-written `parse_integer` function in parse.rs with `atoi::atoi::<i64>(slice)`
- `atoi` handles negative numbers, leading zeros, and overflow detection

### Arena allocation
- [auto] Add `bumpalo` crate as dependency; each connection gets a `Bump` arena
- Arena serves as infrastructure in Phase 8: per-connection allocation pool with O(1) reset after each pipeline batch
- Arena used for scratch allocations where lifetime permits (e.g., temporary token assembly buffers during inline parsing)
- Arena NOT used for `Frame`, `Bytes`, or batch vectors (`responses`, `dispatchable`, `aof_entries`) — research confirmed these owned types must outlive the arena reset (Frame::Array(Vec<Frame>) cannot use arena-allocated Vec without changing the public API contract, which is forbidden)
- `bump.reset()` called after each pipeline batch completes (O(1) deallocation)
- Phase 9+ will expand arena usage to per-request temporaries as more allocation-heavy patterns are introduced
- **Rationale for infrastructure-first:** The original intent was "parsed command argument vectors, temporary string slices during inline parsing." Research (RESEARCH.md Pattern 5, Anti-Patterns) proved that parsed command argument vectors cannot use the arena because they escape into Frame values that outlive the parse phase. The arena is established now so Phase 9+ can leverage it without re-threading the connection handler.

### Buffer management
- [auto] Keep existing `BytesMut` + `Bytes::split_to().freeze()` zero-copy pattern — it's already correct
- No changes to the Framed codec layer or connection handler — only parse.rs and inline.rs internals change

### Benchmark validation
- [auto] Extend existing Criterion bench (`benches/resp_parsing.rs`) with before/after comparison
- Capture pre-rewrite benchmark baseline before any code changes (required for SIMD-04 verification)
- Add pipelined workload benchmark: 32-command pipeline parse throughput
- Add single-command parse latency benchmark for GET/SET patterns

### Claude's Discretion
- Exact memchr call patterns (memchr vs memchr2 vs memmem for multi-byte sequences)
- Whether to replace Cursor with direct index arithmetic for further speedup
- Arena initial capacity sizing (recommend 4KB default)
- Whether inline parser also benefits from memchr (likely yes for space detection)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §RESP parsing at SIMD speed — memchr strategy, pipeline parsing, arena patterns

### Current implementation
- `src/protocol/parse.rs` — Current hand-written RESP2 parser (458 lines), the primary target
- `src/protocol/inline.rs` — Inline command parser (171 lines), secondary optimization target
- `src/protocol/frame.rs` — Frame enum and ParseConfig (130 lines), API contract that must NOT change
- `src/server/codec.rs` — Framed codec (122 lines), integration point
- `benches/resp_parsing.rs` — Existing Criterion benchmarks to extend

### Phase 7 context
- `.planning/phases/07-compare-architecture-resolve-bottlenecks-and-optimize-performance/07-02-PLAN.md` — Pipeline batching implementation that arena allocation must integrate with

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `bytes::BytesMut` / `Bytes` — Already used for zero-copy parsing; memchr operates directly on `&[u8]` slices from BytesMut
- `itoa` crate — Already a dependency for integer-to-string; `atoi` is the inverse
- `Criterion` bench harness — Already configured in Cargo.toml with html_reports

### Established Patterns
- Parser returns `Result<Option<Frame>, ParseError>` — None means incomplete, must be preserved
- `ParseConfig` with `max_bulk_length`, `max_array_depth`, `max_array_length` for DoS protection — must remain functional
- Inline parser dispatched by first-byte check — same pattern continues

### Integration Points
- `parse()` function in parse.rs — public API called by RespCodec::decode()
- `parse_inline()` in inline.rs — called when first byte is not a RESP prefix
- `Framed<TcpStream, RespCodec>` in connection.rs — no changes needed at this layer
- Arena would be owned per-connection in `handle_connection()` and passed to parse functions

</code_context>

<specifics>
## Specific Ideas

- Blueprint specifies ~32 bytes/cycle throughput on AVX2 for CRLF scanning via memchr
- `memchr` crate v2.8 already does runtime CPU feature detection — no unsafe needed
- Bumpalo arena reset pattern: allocate during parse → execute commands → reset arena → parse next batch

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 08-resp-parser-simd-acceleration*
*Context gathered: 2026-03-24*
