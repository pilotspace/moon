# Phase 1: Protocol Foundation - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

Complete, isolated RESP2 protocol library that parses all frame types from raw bytes and serializes responses back to valid RESP2 wire format. Uses zero-copy semantics via `bytes::Bytes`. Also handles inline (plain text) commands. This is a standalone library with zero dependencies on storage, networking, or command dispatch.

</domain>

<decisions>
## Implementation Decisions

### Frame Type Design
- Enum-based `Frame` type with `Bytes` payloads — idiomatic Rust, zero-copy via reference-counted buffers
- Variants: `SimpleString(Bytes)`, `Error(Bytes)`, `Integer(i64)`, `BulkString(Bytes)`, `Array(Vec<Frame>)`, `Null`
- `Frame::Null` is a distinct variant — Redis semantically distinguishes null from empty string
- Nested arrays use recursive `Frame::Array(Vec<Frame>)` — matches RESP2 spec
- Inline commands are parsed into the same `Frame::Array` format as RESP-framed commands — unified downstream handling
- RESP2 only for this phase — design allows future RESP3 extension but don't implement RESP3 types now
- No lifetime parameters on Frame — `Bytes` provides zero-copy without lifetime infection

### Parser API Surface
- Streaming parser operating on `BytesMut` — parse incrementally as TCP data arrives
- Return type: `Result<Option<Frame>, ParseError>` — `Ok(None)` means need-more-data (incomplete), `Ok(Some(frame))` means complete frame parsed, `Err` means protocol violation
- Parser and serializer in the same module as separate functions (`parse()` / `serialize()`) — they share the Frame type
- Parser advances the `BytesMut` cursor on successful parse (consumes bytes)
- Serializer writes to `BytesMut` for zero-copy output

### Error Handling Strategy
- Custom error enum with `Incomplete` and `Invalid` variants — `Incomplete` signals streaming need-more-data, `Invalid` signals protocol violation
- Malformed frames return error response to client, do NOT disconnect — matches Redis behavior, resilient to partial corruption
- Include byte offset in error context for debugging

### RESP2 Edge Case Coverage
- Configurable limits with sensible defaults: max nested array depth 8, max bulk string size 512MB — prevents DoS, matches Redis defaults
- Handle all RESP2 null representations (null bulk string `$-1\r\n`, null array `*-1\r\n`)
- Handle empty bulk strings (`$0\r\n\r\n`) and empty arrays (`*0\r\n`) correctly
- Handle CRLF-only lines and whitespace in inline commands

### Claude's Discretion
- Internal buffer sizing and growth strategy
- Exact module file layout within the protocol crate/module
- Test organization (unit tests inline vs separate test files)
- Benchmark setup details

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### RESP2 Protocol
- No local spec files — use the official Redis RESP2 specification as the canonical reference: https://redis.io/docs/latest/develop/reference/protocol-spec/

### Project Research
- `.planning/research/STACK.md` — Technology choices: bytes crate for zero-copy, no serde, hand-written parser
- `.planning/research/ARCHITECTURE.md` — Protocol layer design: self-contained, no dependencies on other components
- `.planning/research/PITFALLS.md` — Pitfall #2 (lifetime infection), Pitfall #9 (protocol edge cases), Pitfall #14 (not using Bytes)

### Reference Implementations
- Tokio mini-redis RESP parser — reference for streaming Bytes-based parsing pattern

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- None — greenfield project, no existing code

### Established Patterns
- Functional coding patterns per PROJECT.md — pure functions, minimal mutation, composition
- Rust stable toolchain
- jemalloc as global allocator (configure from day one per research recommendation)

### Integration Points
- Phase 2 will import this protocol library for TCP connection framing
- Frame type becomes the shared interface between networking and command dispatch

</code_context>

<specifics>
## Specific Ideas

- Parser should feel like a codec — plug into tokio-util's `Decoder`/`Encoder` traits for Phase 2 integration
- Exhaustive property-based testing: generate random valid RESP2 byte sequences, verify round-trip (parse → serialize → parse)
- Benchmark parse throughput to establish baseline before Phase 2 integration

</specifics>

<deferred>
## Deferred Ideas

- RESP3 protocol support — Phase 5+ (v2 requirement ADVP-01/ADVP-02)
- Tokio codec integration — Phase 2 (networking layer wraps this library)

</deferred>

---

*Phase: 01-protocol-foundation*
*Context gathered: 2026-03-23*
