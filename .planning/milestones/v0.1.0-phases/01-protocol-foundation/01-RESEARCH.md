# Phase 1: Protocol Foundation - Research

**Researched:** 2026-03-23
**Domain:** RESP2 protocol parsing and serialization in Rust
**Confidence:** HIGH

## Summary

Phase 1 implements a complete, self-contained RESP2 protocol library that parses all frame types from raw bytes and serializes frames back to valid RESP2 wire format. The library uses `bytes::Bytes` for zero-copy semantics without lifetime parameter infection. It also handles inline (plain text) commands used by telnet and redis-cli direct input.

The RESP2 protocol is well-specified and straightforward: 5 data types plus null representations, all CRLF-terminated, with type-prefix bytes. The primary implementation challenge is getting the streaming/incremental parse correct (handling partial data across TCP reads) and covering all edge cases (null bulk strings, null arrays, empty strings, nested arrays, binary data in bulk strings). The mini-redis project from the tokio team provides an excellent reference for the two-pass check-then-parse pattern using `Cursor` over `BytesMut`.

This is a greenfield project with no existing code. The protocol module has zero dependencies on other project components and will serve as the shared interface type for all downstream phases.

**Primary recommendation:** Follow the mini-redis two-pass pattern (check for completeness, then parse) with `Bytes`-based owned frames. Use `thiserror` for structured error types. Comprehensive test coverage is critical here -- every RESP2 edge case must be tested before building commands on top.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Enum-based `Frame` type with `Bytes` payloads -- idiomatic Rust, zero-copy via reference-counted buffers
- Variants: `SimpleString(Bytes)`, `Error(Bytes)`, `Integer(i64)`, `BulkString(Bytes)`, `Array(Vec<Frame>)`, `Null`
- `Frame::Null` is a distinct variant -- Redis semantically distinguishes null from empty string
- Nested arrays use recursive `Frame::Array(Vec<Frame>)` -- matches RESP2 spec
- Inline commands are parsed into the same `Frame::Array` format as RESP-framed commands -- unified downstream handling
- RESP2 only for this phase -- design allows future RESP3 extension but don't implement RESP3 types now
- No lifetime parameters on Frame -- `Bytes` provides zero-copy without lifetime infection
- Streaming parser operating on `BytesMut` -- parse incrementally as TCP data arrives
- Return type: `Result<Option<Frame>, ParseError>` -- `Ok(None)` means need-more-data (incomplete), `Ok(Some(frame))` means complete frame parsed, `Err` means protocol violation
- Parser and serializer in the same module as separate functions (`parse()` / `serialize()`) -- they share the Frame type
- Parser advances the `BytesMut` cursor on successful parse (consumes bytes)
- Serializer writes to `BytesMut` for zero-copy output
- Custom error enum with `Incomplete` and `Invalid` variants -- `Incomplete` signals streaming need-more-data, `Invalid` signals protocol violation
- Malformed frames return error response to client, do NOT disconnect -- matches Redis behavior
- Include byte offset in error context for debugging
- Configurable limits with sensible defaults: max nested array depth 8, max bulk string size 512MB
- Handle all RESP2 null representations (null bulk string `$-1\r\n`, null array `*-1\r\n`)
- Handle empty bulk strings (`$0\r\n\r\n`) and empty arrays (`*0\r\n`) correctly
- Handle CRLF-only lines and whitespace in inline commands
- Parser should feel like a codec -- plug into tokio-util's `Decoder`/`Encoder` traits for Phase 2 integration
- Exhaustive property-based testing: generate random valid RESP2 byte sequences, verify round-trip (parse then serialize then parse)
- Benchmark parse throughput to establish baseline before Phase 2 integration

### Claude's Discretion
- Internal buffer sizing and growth strategy
- Exact module file layout within the protocol crate/module
- Test organization (unit tests inline vs separate test files)
- Benchmark setup details

### Deferred Ideas (OUT OF SCOPE)
- RESP3 protocol support -- Phase 5+ (v2 requirement ADVP-01/ADVP-02)
- Tokio codec integration -- Phase 2 (networking layer wraps this library)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PROTO-01 | Server parses and serializes RESP2 protocol (Simple Strings, Errors, Integers, Bulk Strings, Arrays, Null) | RESP2 spec fully documented below; Frame enum design locked; two-pass parse pattern from mini-redis; serialization format for each type specified |
| PROTO-02 | Server handles inline commands (plain text commands from telnet/redis-cli) | Inline command format documented from RESP spec; detection via first-byte check (not `*`); parsed into same Frame::Array format |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| bytes | 1.10 | Zero-copy byte buffers (`Bytes`, `BytesMut`) | Reference-counted, cheaply cloneable. `Bytes::slice()` is O(1). Maintained by tokio team. Foundation for zero-copy RESP parsing. |
| thiserror | 2.0 | Structured error type derivation | Derive macro for clean `ParseError` enum. Callers match on `Incomplete` vs `Invalid` variants. |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tikv-jemallocator | 0.6 | Global memory allocator | Configure from day one per research recommendation. Reduces fragmentation for long-running server. |
| criterion | 0.8 | Benchmarking parse throughput | Dev-dependency for establishing baseline RESP parse performance. |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Hand-written parser | `nom` combinators | nom adds a dependency for ~200 lines of code; hand-written gives full control over error messages and streaming behavior |
| `thiserror` | Manual `impl Display + Error` | thiserror eliminates boilerplate; no runtime cost |
| `Bytes` payloads | `String` / `Vec<u8>` | String forces UTF-8 validation + copies; Vec forces copies. Bytes gives zero-copy with no lifetime infection. Locked decision. |

**Installation (Phase 1 only):**
```bash
cargo init rust-redis
cd rust-redis
cargo add bytes@1.10
cargo add thiserror@2.0
# jemalloc (non-MSVC only)
cargo add tikv-jemallocator@0.6
# Dev dependencies
cargo add --dev criterion@0.8 --features html_reports
```

## Architecture Patterns

### Recommended Module Structure
```
src/
├── main.rs              # Minimal entry point (jemalloc setup, placeholder)
├── lib.rs               # Public API re-exports
└── protocol/
    ├── mod.rs           # Re-exports Frame, ParseError, parse(), serialize()
    ├── frame.rs         # Frame enum definition, Display impl, helper methods
    ├── parse.rs         # Streaming parser: BytesMut -> Result<Option<Frame>, ParseError>
    ├── serialize.rs     # Frame -> BytesMut serializer
    └── inline.rs        # Inline command parser (plain text -> Frame::Array)
```

### Pattern 1: Two-Pass Check-Then-Parse (from mini-redis)

**What:** Separate frame completeness checking from frame parsing. First pass (`check`) peeks at the buffer using a `Cursor` without modifying it. If a complete frame exists, second pass (`parse`) extracts data and advances the buffer.

**When to use:** The public `parse()` function that operates on `BytesMut`.

**Why:** Separating validation from extraction means `parse()` can assume valid data after `check()` succeeds. The buffer is only modified on successful parse -- on incomplete data, nothing changes. This is the pattern used by mini-redis and tokio-util codecs.

**Example:**
```rust
use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;

/// Public API: attempt to parse one frame from the buffer.
/// On success, advances the buffer past the consumed bytes.
/// Returns Ok(None) if the buffer doesn't contain a complete frame.
pub fn parse(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    // Pass 1: Check if a complete frame exists (peek only)
    let mut cursor = Cursor::new(&buf[..]);
    match check(&mut cursor) {
        Ok(()) => {
            // Frame is complete. Record how many bytes it consumed.
            let len = cursor.position() as usize;

            // Pass 2: Parse the frame (re-read from start)
            let mut cursor = Cursor::new(&buf[..]);
            let frame = parse_frame(&mut cursor)?;

            // Advance the buffer past the consumed bytes
            buf.advance(len);

            Ok(Some(frame))
        }
        Err(ParseError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}
```

### Pattern 2: Inline Command Detection

**What:** Detect inline commands by checking if the first byte is NOT a RESP type prefix (`+`, `-`, `:`, `$`, `*`). If it is not a RESP prefix, parse as inline: split by whitespace, wrap each token as `Frame::BulkString(Bytes)`, return as `Frame::Array`.

**When to use:** At the top of the `parse()` function, before RESP parsing.

**Example:**
```rust
fn detect_and_parse(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' | b'-' | b':' | b'$' | b'*' => parse_resp(buf),
        _ => parse_inline(buf),
    }
}

fn parse_inline(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    // Find CRLF to determine if we have a complete line
    let crlf_pos = find_crlf(buf)?;
    // ... extract line, split by whitespace, wrap in Frame::Array
}
```

### Pattern 3: Configurable Limits via Constants with Override

**What:** Define default limits as constants. The parser accepts an optional config struct for overrides. Defaults match Redis behavior.

**Example:**
```rust
pub const DEFAULT_MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024; // 512 MB
pub const DEFAULT_MAX_ARRAY_DEPTH: usize = 8;
pub const DEFAULT_MAX_ARRAY_LENGTH: usize = 1024 * 1024; // 1M elements

pub struct ParseConfig {
    pub max_bulk_string_size: usize,
    pub max_array_depth: usize,
    pub max_array_length: usize,
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self {
            max_bulk_string_size: DEFAULT_MAX_BULK_STRING_SIZE,
            max_array_depth: DEFAULT_MAX_ARRAY_DEPTH,
            max_array_length: DEFAULT_MAX_ARRAY_LENGTH,
        }
    }
}
```

### Anti-Patterns to Avoid
- **Lifetime parameters on Frame:** If Frame has `<'a>`, the lifetime infects Command, Connection, and Server. Use `Bytes` instead -- locked decision from CONTEXT.md.
- **Scanning for CRLF in bulk strings:** Bulk strings are length-prefixed. Read exactly `len` bytes, then verify CRLF. Never scan the data portion for delimiters.
- **Allocating per-parse:** Each parsed frame should reference the original buffer via `Bytes::slice()`, not copy data to new allocations.
- **Modifying buffer on incomplete parse:** Only advance/consume bytes after successfully extracting a complete frame.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Byte buffer management | Custom ring buffer or Vec-based buffer | `bytes::BytesMut` | Handles growth, split, freeze, advance. Battle-tested in tokio ecosystem. |
| Error type boilerplate | Manual `impl Display + Error` | `thiserror` derive | Zero runtime cost, eliminates 20+ lines of boilerplate per error enum. |
| Integer parsing from bytes | Custom atoi | `str::parse::<i64>()` on the CRLF-terminated line | Standard library handles signs, overflow, edge cases correctly. |

**Key insight:** The RESP parser itself IS hand-rolled (project requirement). But the byte buffer management and error type derivation are solved problems that add no learning value.

## Common Pitfalls

### Pitfall 1: Incomplete Data Handling in Recursive Array Parsing
**What goes wrong:** When parsing arrays, the parser recursively parses elements. If element N is incomplete, the parser must return `Incomplete` without having consumed elements 1..N-1 from the buffer.
**Why it happens:** Naive recursive parsing advances the buffer on each successful element. When element N fails, the buffer is in an inconsistent state.
**How to avoid:** Use the two-pass pattern. The `check()` pass uses a `Cursor` (read-only view) to verify ALL elements are complete before `parse()` touches the buffer. The buffer is only advanced once, after the entire array is successfully parsed.
**Warning signs:** Tests pass for complete frames but fail when frames arrive in multiple TCP segments.

### Pitfall 2: Off-By-One in Bulk String Length
**What goes wrong:** The length prefix in `$5\r\nhello\r\n` is the byte count of the data, NOT including the trailing CRLF. After reading `len` bytes of data, you must consume the trailing `\r\n` as well.
**Why it happens:** Easy to forget the +2 bytes for the trailing CRLF after the data portion.
**How to avoid:** After reading the length prefix and CRLF, consume exactly `len + 2` bytes (data + CRLF). Verify the last two bytes are `\r` and `\n`.
**Warning signs:** Empty bulk strings (`$0\r\n\r\n`) parse correctly but strings of length 1+ are off by 2 bytes.

### Pitfall 3: Integer Overflow in Length Parsing
**What goes wrong:** A malicious client sends `$99999999999999999999\r\n` causing integer overflow or allocation of enormous buffers.
**Why it happens:** Length fields are parsed from ASCII digits without bounds checking.
**How to avoid:** Parse length into `usize` with overflow checking. Reject lengths exceeding `max_bulk_string_size` immediately, before attempting to read data. Same for array element counts exceeding `max_array_length`.
**Warning signs:** Panics or OOM on fuzz testing.

### Pitfall 4: Binary Data in Bulk Strings
**What goes wrong:** Bulk strings can contain arbitrary bytes including `\r\n`, `\0`, and non-UTF-8 sequences. Parsers that scan for CRLF within the data portion will split the string incorrectly.
**Why it happens:** Treating bulk strings like simple strings (scan for terminator) instead of respecting the length prefix.
**How to avoid:** For bulk strings, ALWAYS use the length prefix to determine where data ends. Never search for CRLF within the data region.
**Warning signs:** Binary values (images, compressed data) stored via SET are corrupted when retrieved.

### Pitfall 5: Inline Command Edge Cases
**What goes wrong:** Inline commands have subtleties: multiple spaces between arguments, leading/trailing whitespace, empty lines (CRLF-only), and quoted arguments (redis-cli supports quotes in inline mode).
**Why it happens:** The inline format looks trivially simple but has more edge cases than RESP.
**How to avoid:** Trim leading/trailing whitespace. Split by whitespace (one or more spaces/tabs). Handle empty lines gracefully (ignore or return empty array). For v1, quoted argument support is not required -- Redis itself parses quotes in inline mode, but most clients use RESP.
**Warning signs:** `PING` works but `SET  key  value` (double spaces) fails.

## Code Examples

### Frame Enum (verified against CONTEXT.md decisions)
```rust
use bytes::Bytes;

/// A RESP2 protocol frame.
///
/// All string payloads use `Bytes` for zero-copy semantics.
/// No lifetime parameters -- Bytes is reference-counted.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// +<string>\r\n -- Non-binary status reply
    SimpleString(Bytes),
    /// -<error>\r\n -- Error reply
    Error(Bytes),
    /// :<integer>\r\n -- Signed 64-bit integer
    Integer(i64),
    /// $<len>\r\n<data>\r\n -- Binary-safe string
    BulkString(Bytes),
    /// *<count>\r\n<elements...> -- Ordered collection of frames
    Array(Vec<Frame>),
    /// $-1\r\n or *-1\r\n -- Null value
    Null,
}
```

### ParseError Enum
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    /// Not enough data in the buffer to parse a complete frame.
    /// This is NOT a protocol error -- the caller should read more data.
    #[error("incomplete frame: need more data")]
    Incomplete,

    /// The data violates the RESP2 protocol specification.
    #[error("invalid frame at byte {offset}: {message}")]
    Invalid {
        message: String,
        offset: usize,
    },

    /// An I/O error occurred while reading from the buffer.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
```

### Serializer Pattern
```rust
use bytes::{BufMut, BytesMut};

/// Serialize a Frame into RESP2 wire format, appending to the buffer.
pub fn serialize(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.put_u8(b'-');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.put_u8(b':');
            // Use itoa or write! for efficient integer formatting
            let s = n.to_string();
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::BulkString(data) => {
            buf.put_u8(b'$');
            let len = data.len().to_string();
            buf.put_slice(len.as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Array(items) => {
            buf.put_u8(b'*');
            let len = items.len().to_string();
            buf.put_slice(len.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        Frame::Null => {
            buf.put_slice(b"$-1\r\n");
        }
    }
}
```

### Check Function Pattern (two-pass, cursor-based)
```rust
use std::io::Cursor;
use bytes::Buf;

/// Check if a complete RESP2 frame exists in the buffer.
/// Does NOT modify the buffer -- uses Cursor for read-only traversal.
fn check(cursor: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
    if !cursor.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    match get_u8(cursor)? {
        b'+' | b'-' => {
            // Simple string or error: find \r\n
            find_crlf(cursor)?;
            Ok(())
        }
        b':' => {
            // Integer: find \r\n
            find_crlf(cursor)?;
            Ok(())
        }
        b'$' => {
            // Bulk string: read length, then skip that many bytes + \r\n
            let len = read_decimal(cursor)?;
            if len == -1 {
                // Null bulk string
                return Ok(());
            }
            let len = len as usize;
            // Skip data + trailing \r\n
            skip(cursor, len + 2)?;
            Ok(())
        }
        b'*' => {
            // Array: read count, then check each element
            let count = read_decimal(cursor)?;
            if count == -1 {
                // Null array
                return Ok(());
            }
            for _ in 0..count {
                check(cursor)?;
            }
            Ok(())
        }
        byte => Err(ParseError::Invalid {
            message: format!("unknown type byte: 0x{:02x}", byte),
            offset: cursor.position() as usize - 1,
        }),
    }
}
```

### Inline Command Parser
```rust
/// Parse an inline command (non-RESP format) into Frame::Array.
/// Inline commands are space-separated tokens terminated by \r\n.
fn parse_inline(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    // Find the CRLF terminator
    let Some(crlf_pos) = find_crlf_position(buf) else {
        return Ok(None); // Incomplete -- need more data
    };

    // Extract the line (without CRLF)
    let line = &buf[..crlf_pos];

    // Split by whitespace into tokens
    let args: Vec<Frame> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|s| !s.is_empty())
        .map(|token| {
            let data = Bytes::copy_from_slice(token);
            Frame::BulkString(data)
        })
        .collect();

    // Advance buffer past the line + CRLF
    buf.advance(crlf_pos + 2);

    if args.is_empty() {
        // Empty line (CRLF only) -- ignore, try again
        return Ok(None);
    }

    Ok(Some(Frame::Array(args)))
}
```

## RESP2 Protocol Reference (from official spec)

### Wire Format Summary
| Type | Prefix | Format | Example |
|------|--------|--------|---------|
| Simple String | `+` | `+<text>\r\n` | `+OK\r\n` |
| Error | `-` | `-<error>\r\n` | `-ERR unknown\r\n` |
| Integer | `:` | `:[+-]<digits>\r\n` | `:1000\r\n`, `:-42\r\n` |
| Bulk String | `$` | `$<len>\r\n<data>\r\n` | `$5\r\nhello\r\n` |
| Null Bulk String | `$` | `$-1\r\n` | `$-1\r\n` |
| Empty Bulk String | `$` | `$0\r\n\r\n` | `$0\r\n\r\n` |
| Array | `*` | `*<count>\r\n<elements>` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |
| Null Array | `*` | `*-1\r\n` | `*-1\r\n` |
| Empty Array | `*` | `*0\r\n` | `*0\r\n` |

### Critical Edge Cases to Test
1. **Null bulk string** `$-1\r\n` -- must produce `Frame::Null`, not `Frame::BulkString`
2. **Null array** `*-1\r\n` -- must produce `Frame::Null`, not `Frame::Array(vec![])`
3. **Empty bulk string** `$0\r\n\r\n` -- must produce `Frame::BulkString(Bytes::new())`, not null
4. **Empty array** `*0\r\n` -- must produce `Frame::Array(vec![])`, not null
5. **Binary data in bulk strings** -- `$4\r\n\r\n\r\n\r\n` is valid (data is `\r\n\r\n`)
6. **Negative integers** -- `:-42\r\n` must parse correctly
7. **Nested arrays** -- `*1\r\n*1\r\n:1\r\n` is a 1-element array containing a 1-element array
8. **Null elements in arrays** -- `*3\r\n$3\r\nhey\r\n$-1\r\n$3\r\nfoo\r\n` has null as second element
9. **Inline commands** -- `PING\r\n` and `SET key value\r\n` without RESP framing
10. **Multiple spaces in inline** -- `SET  key  value\r\n` must handle correctly

### Inline Command Format
- No RESP prefix byte (first byte is NOT `+`, `-`, `:`, `$`, `*`)
- Arguments separated by spaces
- Terminated by `\r\n`
- Detection: if first byte of buffer is not a RESP type prefix, treat as inline
- Result: parsed into `Frame::Array(Vec<Frame::BulkString(...>>)` -- same as RESP-framed command

### Client Request Format
- Clients send commands as arrays of bulk strings ONLY (RESP format)
- Example: `SET foo bar` is sent as `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
- Exception: inline commands (telnet, redis-cli direct input)

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `&[u8]` lifetime-based parsing | `Bytes`-based owned frames | bytes crate maturity (~2020) | Eliminates lifetime infection. Standard in tokio ecosystem. |
| Single-pass parse (modify buffer immediately) | Two-pass check-then-parse | mini-redis pattern | Clean incomplete-data handling. No buffer corruption on partial frames. |
| `Vec<u8>` buffers | `BytesMut` with freeze-to-Bytes | bytes 1.x | Zero-copy slicing. Integrates with tokio I/O. |
| Custom error types with boilerplate | `thiserror` derive | thiserror 1.x/2.x | Less code, same performance. |

## Open Questions

1. **Integer formatting performance**
   - What we know: `i64::to_string()` allocates a temporary String for serialization
   - What's unclear: Whether this is a measurable bottleneck at protocol scale
   - Recommendation: Start with `to_string()`, add `itoa` crate later if benchmarks show it matters. The `itoa` crate provides allocation-free integer formatting.

2. **Inline command quoted arguments**
   - What we know: Redis supports quoted strings in inline mode (e.g., `SET key "hello world"`)
   - What's unclear: How many real clients rely on this feature
   - Recommendation: Defer quoted argument support. Real clients use RESP format. Only telnet users type inline commands, and quoting is rarely needed for basic testing.

3. **Codec trait integration**
   - What we know: CONTEXT.md says parser should "feel like a codec" for Phase 2 tokio-util integration
   - What's unclear: Whether to implement `Decoder`/`Encoder` traits now or keep as standalone functions
   - Recommendation: Keep as standalone `parse()`/`serialize()` functions in Phase 1. Phase 2 wraps them in a thin `Decoder`/`Encoder` impl. This keeps Phase 1 dependency-free from tokio-util.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test framework (cargo test) + criterion 0.8 for benchmarks |
| Config file | None needed -- Rust test framework is zero-config |
| Quick run command | `cargo test -p rust-redis --lib protocol` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PROTO-01 | Parse all RESP2 frame types from bytes | unit | `cargo test protocol::parse::tests -q` | Wave 0 |
| PROTO-01 | Serialize all frame types to valid RESP2 wire format | unit | `cargo test protocol::serialize::tests -q` | Wave 0 |
| PROTO-01 | Round-trip: parse(serialize(frame)) == frame | unit | `cargo test protocol::tests::roundtrip -q` | Wave 0 |
| PROTO-01 | Handle incomplete data (streaming) | unit | `cargo test protocol::parse::tests::incomplete -q` | Wave 0 |
| PROTO-01 | Handle null bulk string and null array | unit | `cargo test protocol::parse::tests::null -q` | Wave 0 |
| PROTO-01 | Reject malformed frames with ParseError | unit | `cargo test protocol::parse::tests::invalid -q` | Wave 0 |
| PROTO-01 | Enforce configurable limits (depth, size) | unit | `cargo test protocol::parse::tests::limits -q` | Wave 0 |
| PROTO-02 | Parse inline commands into Frame::Array | unit | `cargo test protocol::inline::tests -q` | Wave 0 |
| PROTO-02 | Handle edge cases (multiple spaces, empty lines, whitespace) | unit | `cargo test protocol::inline::tests::edge_cases -q` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib -q`
- **Per wave merge:** `cargo test -q`
- **Phase gate:** Full test suite green + benchmarks run before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/protocol/mod.rs` -- module structure
- [ ] `src/protocol/frame.rs` -- Frame enum and helpers
- [ ] `src/protocol/parse.rs` -- parser with inline tests
- [ ] `src/protocol/serialize.rs` -- serializer with inline tests
- [ ] `src/protocol/inline.rs` -- inline command parser with inline tests
- [ ] `src/lib.rs` -- re-exports
- [ ] `src/main.rs` -- jemalloc setup placeholder
- [ ] `Cargo.toml` -- project initialization with dependencies
- [ ] `benches/resp_parsing.rs` -- criterion benchmark harness

## Sources

### Primary (HIGH confidence)
- [Redis RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) -- Complete RESP2 wire format, all types, inline commands, edge cases
- [Tokio mini-redis source](https://github.com/tokio-rs/mini-redis) -- frame.rs (Frame enum, check/parse pattern), parse.rs (command argument extraction), connection.rs (BytesMut buffer management)
- `.planning/research/STACK.md` -- bytes 1.10, thiserror 2.0, criterion 0.8 versions verified
- `.planning/research/PITFALLS.md` -- Pitfall #2 (lifetime infection), #9 (RESP edge cases), #14 (use Bytes crate)

### Secondary (MEDIUM confidence)
- `.planning/research/ARCHITECTURE.md` -- Protocol layer design, project structure, zero-copy parsing pattern

### Tertiary (LOW confidence)
- None -- all findings verified against primary sources

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- bytes and thiserror are universally standard for this domain, versions verified from STACK.md research
- Architecture: HIGH -- two-pass parse pattern is proven in mini-redis, locked decisions from CONTEXT.md are clear
- Pitfalls: HIGH -- well-documented in project research and verified against RESP spec edge cases
- RESP2 spec: HIGH -- fetched from official Redis documentation

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, RESP2 protocol is frozen)
