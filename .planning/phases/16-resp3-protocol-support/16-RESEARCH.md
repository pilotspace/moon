# Phase 16: RESP3 Protocol Support - Research

**Researched:** 2026-03-24
**Domain:** RESP3 protocol parsing/serialization, protocol negotiation, client-side caching invalidation
**Confidence:** HIGH

## Summary

RESP3 extends RESP2 with eight new frame types (Map, Set, Double, Boolean, Null, VerbatimString, BigNumber, Push), each identified by a unique prefix byte. The protocol is backward-compatible: connections default to RESP2 and upgrade via the HELLO command. The core implementation extends the existing Frame enum, parser byte-dispatch, and serializer with new variants. The HELLO command combines protocol negotiation, optional authentication, and client naming in a single round-trip. CLIENT TRACKING enables server-assisted client-side caching by tracking read keys per connection and sending Push invalidation frames when those keys are modified.

The existing codebase has a clean architecture for this extension: the parser dispatches on first-byte prefix (currently 5 RESP2 prefixes, others route to inline parser), the serializer is a simple match on Frame variants, and the connection handler already carries per-connection state (authenticated, selected_db, pubsub). Adding protocol_version follows the same pattern. The main complexity lies in the response-format switching layer (~20 commands need RESP3-specific responses) and the CLIENT TRACKING invalidation table (per-shard tracking with Push frame delivery).

**Primary recommendation:** Implement in three waves: (1) Frame types + parser + serializer, (2) HELLO command + protocol_version + response switching, (3) CLIENT TRACKING with invalidation Push frames.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Frame enum extended with: Map(Vec<(Frame, Frame)>), Set(Vec<Frame>), Double(f64), Boolean(bool), VerbatimString { encoding: Bytes, data: Bytes }, BigNumber(Bytes), Push(Vec<Frame>), Null (distinct prefix `_`)
- HELLO [protover] [AUTH username password] [SETNAME clientname] command
- Connection starts RESP2 by default; HELLO 3 upgrades; HELLO 2 downgrades
- Per-connection protocol_version: u8 field (2 or 3)
- Response serializers check protocol_version for RESP2 vs RESP3 format
- CLIENT TRACKING ON|OFF with REDIRECT, PREFIX, BCAST, OPTIN, OPTOUT, NOLOOP
- Tracking table: per-shard HashMap<key, Vec<client_id>>
- BCAST mode: broadcast invalidation for all keys matching registered prefixes
- REDIRECT mode: send invalidations to a different connection

### Claude's Discretion
- Exact tracking table memory management (LRU eviction under pressure)
- Whether to implement CLIENT CACHING YES/NO (OPTIN/OPTOUT fine-grained control)
- Push frame delivery strategy (inline with response vs out-of-band)
- CLIENT ID assignment (atomic counter, currently not implemented)

### Deferred Ideas (OUT OF SCOPE)
- CLIENT CACHING YES/NO (fine-grained opt-in/opt-out) -- implement with basic TRACKING first
- RESP3 streaming (fragmented large responses) -- evaluate if needed
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| ADVP-01 | Server supports RESP3 protocol negotiation via HELLO command | HELLO command spec, protocol_version per-connection state, Frame enum extension |
| ADVP-02 | Server returns rich types (maps, sets, booleans) in RESP3 mode | RESP3 frame types, response format switching table, ~20 commands need RESP3 variants |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| bytes | 1.10 | Zero-copy byte buffers for frame data | Already in use; Bytes for all frame payloads |
| ordered-float | 5 | OrderedFloat for Double frame f64 comparison | Already in use for sorted sets; enables PartialEq on Frame with Double |
| itoa | 1 | Fast integer-to-string for serialization | Already in use in serializer |
| ryu | (new, optional) | Fast f64-to-string for Double serialization | Standard Rust float formatting; stdlib format!() also works |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| parking_lot | 0.12 | Per-shard tracking table Mutex | Already used for PubSubRegistry; same pattern for tracking |
| tokio::sync::mpsc | (bundled) | Push frame delivery channel | Already used for pub/sub subscriber channels; reuse for invalidation |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ordered-float for Double PartialEq | Custom Eq wrapper | ordered-float already a dependency; no reason to hand-roll |
| ryu for float formatting | format!("{}", f64) | ryu is faster but stdlib is adequate for this use case |

**Installation:**
No new dependencies required. All needed crates are already in Cargo.toml. The `ryu` crate is optional -- stdlib f64 formatting is sufficient.

## Architecture Patterns

### Recommended Project Structure
```
src/
  protocol/
    frame.rs          # Extended Frame enum with 8 new RESP3 variants
    parse.rs          # Extended parser with RESP3 prefix dispatch
    serialize.rs      # Extended serializer with RESP3 encoding + protocol-aware serialize fn
    resp3.rs          # (new) RESP3 response conversion helpers
  command/
    connection.rs     # HELLO command handler added
    client.rs         # (new) CLIENT TRACKING/ID/CACHING commands
  server/
    connection.rs     # protocol_version state, tracking state, Push delivery
    codec.rs          # Codec unchanged (Frame enum handles both)
  tracking/           # (new module)
    mod.rs            # TrackingTable, TrackingState per-connection
    invalidation.rs   # Key invalidation logic, Push frame construction
```

### Pattern 1: Frame Enum Extension (Backward Compatible)
**What:** Add new variants to the existing Frame enum. The Null variant already exists but gets a new RESP3 wire format (`_\r\n` instead of `$-1\r\n`).
**When to use:** All RESP3 frame types.
**Example:**
```rust
// Source: Redis RESP3 spec (redis.io/docs/latest/develop/reference/protocol-spec/)
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    // Existing RESP2 variants (unchanged)
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<Frame>),
    Null,
    // New RESP3 variants
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
    Double(f64),           // Wire: ,1.23\r\n  Special: ,inf\r\n ,-inf\r\n ,nan\r\n
    Boolean(bool),         // Wire: #t\r\n or #f\r\n
    VerbatimString {       // Wire: =15\r\ntxt:Some string\r\n
        encoding: Bytes,   // 3-byte encoding hint: "txt", "mkd"
        data: Bytes,
    },
    BigNumber(Bytes),      // Wire: (3492890328409238509324850943850943825024385\r\n
    Push(Vec<Frame>),      // Wire: >3\r\n$10\r\ninvalidate\r\n...
}
```

### Pattern 2: Parser First-Byte Dispatch Extension
**What:** The existing parser dispatches on `buf[0]` for RESP2 prefixes. Add RESP3 prefix bytes to the dispatch.
**When to use:** Parsing incoming RESP3 frames.
**Critical detail:** Currently, non-RESP prefix bytes (`!`, `#`, `%`, etc.) route to the inline parser. RESP3 prefix bytes MUST be added to the RESP dispatch match BEFORE the inline fallback.
```rust
// In parse.rs: extend the dispatch
match buf[0] {
    b'+' | b'-' | b':' | b'$' | b'*'                    // RESP2
    | b'%' | b'~' | b',' | b'#' | b'_' | b'=' | b'(' | b'>' | b'!'  // RESP3
    => { /* RESP parser */ }
    _ => return inline::parse_inline(buf),
}
```

### Pattern 3: Protocol-Aware Response Serialization
**What:** The serializer must emit RESP2 or RESP3 wire format based on protocol_version. For RESP2 clients, RESP3-only types must be downgraded.
**When to use:** Every response path.
**Key insight:** The serializer should handle RESP3 Frame variants even in RESP2 mode by downgrading them:
- Map -> flat Array [k1, v1, k2, v2, ...]
- Set -> Array
- Double -> BulkString (formatted float)
- Boolean -> Integer (1/0)
- Push -> Array (with "push" prefix for RESP2 pub/sub)
- Null -> `$-1\r\n` (RESP2) or `_\r\n` (RESP3)

```rust
// Two serialization functions:
pub fn serialize(frame: &Frame, buf: &mut BytesMut);           // RESP2 wire format (existing)
pub fn serialize_resp3(frame: &Frame, buf: &mut BytesMut);     // RESP3 wire format (new)

// Or a unified function with protocol version:
pub fn serialize_versioned(frame: &Frame, buf: &mut BytesMut, proto: u8);
```

### Pattern 4: Response Format Switching Table
**What:** A mapping of commands that return different types in RESP3 vs RESP2.
**When to use:** After command execution, before sending response.
**Key insight:** Only ~20 commands have different RESP3 representations. The conversion happens at the response level, NOT inside each command handler. Commands always return RESP2-compatible Frame values; the response layer converts to RESP3 types when protocol_version == 3.

```rust
// RESP3 response conversion table (applied post-dispatch)
// Commands returning Map in RESP3 (flat Array in RESP2):
//   HGETALL, CONFIG GET, XRANGE (partial), CLIENT INFO, MEMORY STATS
// Commands returning Set in RESP3 (Array in RESP2):
//   SMEMBERS, SINTER, SUNION, SDIFF, SSCAN (members part)
// Commands returning Boolean in RESP3 (Integer 0/1 in RESP2):
//   EXISTS (single key), SISMEMBER, HEXISTS, EXPIRE, PERSIST, MSETNX
// Commands returning Double in RESP3 (BulkString in RESP2):
//   ZSCORE, ZINCRBY, INCRBYFLOAT, HINCRBYFLOAT

// Conversion approach: wrap response after dispatch
fn maybe_convert_resp3(cmd: &[u8], response: Frame, proto: u8) -> Frame {
    if proto < 3 { return response; }
    match cmd_uppercase {
        "HGETALL" => array_to_map(response),
        "SMEMBERS" => array_to_set(response),
        "SISMEMBER" => integer_to_boolean(response),
        "ZSCORE" => bulk_to_double(response),
        _ => response,
    }
}
```

### Pattern 5: Per-Connection Tracking State
**What:** Each connection carries CLIENT TRACKING state alongside protocol_version.
**When to use:** Connection handler initialization.
```rust
// Per-connection state (added to handle_connection / handle_connection_sharded)
struct ConnectionState {
    protocol_version: u8,           // 2 or 3
    client_id: u64,                 // Unique connection ID
    tracking_enabled: bool,
    tracking_bcast: bool,
    tracking_optin: bool,
    tracking_optout: bool,
    tracking_noloop: bool,
    tracking_redirect: Option<u64>, // Target client_id
    tracking_prefixes: Vec<Bytes>,  // BCAST prefix filters
}
```

### Anti-Patterns to Avoid
- **Modifying every command handler for RESP3:** Do NOT add protocol_version to each command function. Instead, apply conversion at the response layer after dispatch.
- **Breaking RESP2 Null encoding:** The existing `Frame::Null` is used for both RESP2 `$-1\r\n` and RESP3 `_\r\n`. The serializer must check protocol_version to choose the right encoding.
- **Inline parser collision:** RESP3 prefix bytes (`#`, `%`, `~`, etc.) currently route to inline parser. Forgetting to update the dispatch causes RESP3 frames to be misinterpreted as inline commands.
- **Blocking Push delivery:** Push frames (invalidation) must be delivered non-blocking. Use try_send on the channel, same as pub/sub pattern.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Float serialization | Custom dtoa | ryu crate or stdlib `format!` | Edge cases: inf, -inf, nan, precision |
| Float parsing | Manual parse | `str::parse::<f64>()` or `fast-float` | Handles all IEEE 754 special values |
| Tracking table eviction | Custom LRU | Bounded HashMap with simple eviction | Redis uses fixed-size table with fake invalidation on overflow |
| Client ID generation | UUID or complex scheme | AtomicU64 counter | Redis uses simple incrementing counter |
| RESP3 type prefix detection | Manual byte checks | Match arm in existing dispatch | Already have the pattern established |

**Key insight:** The RESP3 protocol is straightforward wire encoding. The complexity is in the integration (response switching, tracking table, Push delivery), not in custom data structures.

## Common Pitfalls

### Pitfall 1: Inline Parser Collision with RESP3 Prefixes
**What goes wrong:** RESP3 prefix bytes (`#`, `%`, `~`, `,`, `_`, `=`, `(`, `>`, `!`) are currently NOT in the RESP prefix match, so they route to the inline parser, which treats them as inline command text.
**Why it happens:** The parser was written for RESP2 only. Non-RESP2 prefix bytes fall through to inline parsing.
**How to avoid:** Update the parse.rs dispatch match FIRST, before implementing any RESP3 parsing logic. Add all RESP3 prefixes to the RESP dispatch arm.
**Warning signs:** Test `test_parse_non_resp_prefix_routes_to_inline` currently asserts that `!foo\r\n` is parsed as inline. This test MUST be updated when `!` becomes a RESP3 Blob Error prefix.

### Pitfall 2: PartialEq for Frame with f64
**What goes wrong:** `f64` does not implement `Eq`, and `PartialEq` for f64 has NaN != NaN. The Frame enum currently derives `PartialEq`. Adding `Double(f64)` breaks equality semantics.
**Why it happens:** IEEE 754 float comparison rules.
**How to avoid:** Use `ordered_float::OrderedFloat<f64>` for the Double variant, or implement PartialEq manually for Frame. OrderedFloat is already a dependency (used in sorted sets).
**Warning signs:** Tests that compare Frame values with Double will behave unexpectedly if using raw f64.

### Pitfall 3: RESP2 Null vs RESP3 Null Wire Format
**What goes wrong:** `Frame::Null` currently serializes as `$-1\r\n` (RESP2). In RESP3, Null is `_\r\n`. Using a single Null variant means the serializer must know the protocol version.
**Why it happens:** Two wire formats for the same semantic.
**How to avoid:** The serializer (or codec) must accept protocol_version and switch encoding. One approach: `serialize_versioned(frame, buf, proto_version)`. The codec must carry protocol_version state.
**Warning signs:** RESP3 clients receiving `$-1\r\n` instead of `_\r\n`.

### Pitfall 4: Codec Must Be Protocol-Aware
**What goes wrong:** The RespCodec in codec.rs wraps parse/serialize without protocol awareness. For RESP3 responses, the codec must serialize using RESP3 wire format.
**Why it happens:** Codec was designed for RESP2 only.
**How to avoid:** Add `protocol_version: u8` to RespCodec. The Encoder impl checks this to select RESP2 vs RESP3 serialization. The Decoder always parses both (a server receives RESP2 commands from RESP2 clients and RESP2 commands from RESP3 clients -- clients always send commands in RESP2 array format regardless of protocol version).
**Warning signs:** RESP3 clients are always sending commands as RESP2 arrays. Only RESPONSES differ.

### Pitfall 5: CLIENT TRACKING in Sharded Architecture
**What goes wrong:** The tracking table must be per-shard (like PubSubRegistry). Cross-shard invalidation is complex.
**Why it happens:** Sharded architecture isolates data per shard.
**How to avoid:** Per CONTEXT.md decision: "Tracking table: per-shard HashMap<key, Vec<client_id>> -- invalidation is shard-local, no cross-shard coordination." Each shard tracks keys accessed by connections assigned to it. When a key is modified on a shard, only that shard's tracking table is consulted.
**Warning signs:** Attempting to implement a global tracking table requires cross-shard locking.

### Pitfall 6: HELLO Must Return Response in NEW Protocol Format
**What goes wrong:** When HELLO 3 switches the protocol, the response itself must be in RESP3 format (a Map). If the response is serialized in RESP2 format before the switch takes effect, the client gets a flat array instead of a map.
**Why it happens:** Protocol switch must happen before response serialization.
**How to avoid:** Set protocol_version = 3 BEFORE serializing the HELLO response. The HELLO handler should return the protocol version to apply, and the connection loop should update codec protocol_version before sending the response.
**Warning signs:** RESP3 clients receiving HELLO response as flat array.

### Pitfall 7: Push Frames Interleaved with Responses
**What goes wrong:** CLIENT TRACKING invalidation Push frames can arrive between a command's request and response if delivered synchronously.
**Why it happens:** Push frames are out-of-band by definition.
**How to avoid:** Use the same pattern as pub/sub: deliver Push frames via a channel, and the connection loop sends them between command responses (never mid-response). The tokio::select! pattern already handles this for pub/sub messages.
**Warning signs:** Client libraries receiving unexpected Push frames mid-pipeline.

## Code Examples

### RESP3 Frame Parsing (Double)
```rust
// Source: Redis protocol spec (redis.io/docs/latest/develop/reference/protocol-spec/)
// Prefix: ','
// Format: ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
// Special: ,inf\r\n  ,-inf\r\n  ,nan\r\n
b',' => {
    let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
    let line = &buf[*pos..crlf];
    let value = if line == b"inf" {
        f64::INFINITY
    } else if line == b"-inf" {
        f64::NEG_INFINITY
    } else if line.eq_ignore_ascii_case(b"nan") {
        f64::NAN
    } else {
        let s = std::str::from_utf8(line).map_err(|_| ParseError::Invalid {
            message: "invalid double encoding".into(),
            offset: *pos,
        })?;
        s.parse::<f64>().map_err(|_| ParseError::Invalid {
            message: format!("invalid double: {}", s),
            offset: *pos,
        })?
    };
    *pos = crlf + 2;
    Ok(Frame::Double(value))
}
```

### RESP3 Frame Serialization (Map)
```rust
// Source: Redis protocol spec
// Format: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
Frame::Map(entries) => {
    buf.put_u8(b'%');
    let mut itoa_buf = itoa::Buffer::new();
    buf.put_slice(itoa_buf.format(entries.len()).as_bytes());
    buf.put_slice(b"\r\n");
    for (key, value) in entries {
        serialize_resp3(key, buf);
        serialize_resp3(value, buf);
    }
}
```

### HELLO Command Response
```rust
// Source: redis.io/docs/latest/commands/hello/
fn hello(args: &[Frame], protocol_version: &mut u8, client_id: u64) -> Frame {
    // Parse protover, AUTH, SETNAME from args
    // Returns a Map with server info:
    Frame::Map(vec![
        (Frame::BulkString(Bytes::from_static(b"server")),
         Frame::BulkString(Bytes::from_static(b"rustredis"))),
        (Frame::BulkString(Bytes::from_static(b"version")),
         Frame::BulkString(Bytes::from_static(b"0.1.0"))),
        (Frame::BulkString(Bytes::from_static(b"proto")),
         Frame::Integer(*protocol_version as i64)),
        (Frame::BulkString(Bytes::from_static(b"id")),
         Frame::Integer(client_id as i64)),
        (Frame::BulkString(Bytes::from_static(b"mode")),
         Frame::BulkString(Bytes::from_static(b"standalone"))),
        (Frame::BulkString(Bytes::from_static(b"role")),
         Frame::BulkString(Bytes::from_static(b"master"))),
        (Frame::BulkString(Bytes::from_static(b"modules")),
         Frame::Array(vec![])),
    ])
}
```

### Invalidation Push Frame
```rust
// Source: redis.io/docs/latest/develop/reference/client-side-caching/
// Push frame format for invalidation:
// >2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n
fn invalidation_push(keys: &[Bytes]) -> Frame {
    let key_frames: Vec<Frame> = keys.iter()
        .map(|k| Frame::BulkString(k.clone()))
        .collect();
    Frame::Push(vec![
        Frame::BulkString(Bytes::from_static(b"invalidate")),
        Frame::Array(key_frames),
    ])
}
```

### Response Format Conversion (HGETALL)
```rust
// RESP2: HGETALL returns flat Array [k1, v1, k2, v2]
// RESP3: HGETALL returns Map [(k1, v1), (k2, v2)]
fn array_to_map(frame: Frame) -> Frame {
    match frame {
        Frame::Array(items) if items.len() % 2 == 0 => {
            let pairs: Vec<(Frame, Frame)> = items
                .chunks_exact(2)
                .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                .collect();
            Frame::Map(pairs)
        }
        other => other, // Pass through errors, empty arrays, etc.
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| RESP2 only | RESP2 + RESP3 dual support | Redis 6.0 (2020) | New frame types, protocol negotiation |
| Separate pub/sub connection | Push frames on same connection | Redis 6.0 | Eliminates need for dedicated subscriber connection |
| No client-side caching support | CLIENT TRACKING + invalidation | Redis 6.0 | Server-assisted caching reduces round-trips |
| HELLO not required | HELLO optional (RESP2 default) | Redis 6.2 | HELLO without protover reports connection context |

**Deprecated/outdated:**
- RESP3 streaming (`$?` chunked strings with `;` prefix): Not implemented by Redis server; specification only. OUT OF SCOPE per deferred decisions.
- Attribute type (`|` prefix): Auxiliary metadata; rarely used. Can defer.

## RESP3 Command-to-Type Mapping Table

Commands that return different types in RESP3 vs RESP2:

| Command | RESP2 Return | RESP3 Return | Conversion |
|---------|-------------|-------------|------------|
| HGETALL | Array [k,v,k,v...] | Map [(k,v)...] | array_to_map |
| HKEYS | Array | Array | (no change) |
| HVALS | Array | Array | (no change) |
| CONFIG GET | Array [k,v,k,v...] | Map [(k,v)...] | array_to_map |
| SMEMBERS | Array | Set | array_to_set |
| SINTER | Array | Set | array_to_set |
| SUNION | Array | Set | array_to_set |
| SDIFF | Array | Set | array_to_set |
| EXISTS (1 key) | Integer (0/1) | Boolean | int_to_bool (only single-key) |
| SISMEMBER | Integer (0/1) | Boolean | int_to_bool |
| HEXISTS | Integer (0/1) | Boolean | int_to_bool |
| EXPIRE | Integer (0/1) | Boolean | int_to_bool |
| PEXPIRE | Integer (0/1) | Boolean | int_to_bool |
| PERSIST | Integer (0/1) | Boolean | int_to_bool |
| SETNX | Integer (0/1) | Boolean | int_to_bool |
| MSETNX | Integer (0/1) | Boolean | int_to_bool |
| ZSCORE | BulkString | Double | bulk_to_double |
| ZINCRBY | BulkString | Double | bulk_to_double |
| INCRBYFLOAT | BulkString | Double | bulk_to_double |
| HINCRBYFLOAT | BulkString | Double | bulk_to_double |

## Claude's Discretion Recommendations

### Tracking Table Memory Management
**Recommendation:** Use a configurable `tracking-table-max-keys` limit (default 1,000,000 as Redis does). When the table exceeds this limit, evict the oldest entries and send fake invalidation messages to affected clients. Simple approach: no LRU needed for initial implementation -- just cap the total entries and invalidate+remove when full.

### CLIENT CACHING YES/NO
**Recommendation:** Defer per CONTEXT.md. OPTIN/OPTOUT modes work without CLIENT CACHING command -- they just don't track keys from the next command. The CLIENT CACHING command adds fine-grained per-command control which is a separate feature.

### Push Frame Delivery Strategy
**Recommendation:** Inline with response, same as pub/sub. Use tokio::select! in the connection loop with a dedicated mpsc channel for Push frames. When the connection is in RESP3 mode with tracking enabled, Push frames are interleaved between command responses. This is the exact same pattern used for pub/sub messages already in connection.rs.

### CLIENT ID Assignment
**Recommendation:** Global AtomicU64 counter, incremented on each new connection. Simple, monotonic, unique. Expose via `CLIENT ID` command. Add the counter as a static in the server module:
```rust
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
pub fn next_client_id() -> u64 { NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed) }
```

## Open Questions

1. **EXISTS with multiple keys**
   - What we know: EXISTS with 1 key returns Boolean in RESP3; EXISTS with multiple keys returns Integer (count)
   - What's unclear: Need to confirm the threshold -- is it based on arg count or return semantics?
   - Recommendation: Check arg count: if args.len() == 1, convert to Boolean; otherwise keep Integer

2. **RESP3 Blob Error (`!` prefix)**
   - What we know: RESP3 adds a Blob Error type (`!<length>\r\n<data>\r\n`) distinct from Simple Error (`-`)
   - What's unclear: Whether any Redis commands actually return Blob Errors
   - Recommendation: Parse it but don't generate it; existing Simple Error is sufficient for server responses

3. **Attribute type (`|` prefix)**
   - What we know: RESP3 spec defines Attribute as auxiliary metadata that precedes the actual reply
   - What's unclear: Whether Redis server actually uses this in practice
   - Recommendation: Defer -- no Redis command currently returns attributes. Can add later if needed.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + integration tests |
| Config file | Cargo.toml [[test]] sections |
| Quick run command | `cargo test --lib protocol::` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ADVP-01 | HELLO negotiates RESP2/RESP3 | unit + integration | `cargo test --lib hello` and `cargo test --test integration hello` | No - Wave 0 |
| ADVP-01 | HELLO with AUTH integration | integration | `cargo test --test integration hello_auth` | No - Wave 0 |
| ADVP-02 | RESP3 frame parsing (all 8 types) | unit | `cargo test --lib protocol::parse::tests` | Partial (RESP2 only) |
| ADVP-02 | RESP3 frame serialization (all 8 types) | unit | `cargo test --lib protocol::serialize::tests` | Partial (RESP2 only) |
| ADVP-02 | HGETALL returns Map in RESP3 | integration | `cargo test --test integration resp3_hgetall` | No - Wave 0 |
| ADVP-02 | SMEMBERS returns Set in RESP3 | integration | `cargo test --test integration resp3_smembers` | No - Wave 0 |
| ADVP-02 | Boolean responses for EXISTS etc | integration | `cargo test --test integration resp3_boolean` | No - Wave 0 |
| -- | CLIENT TRACKING ON/OFF | integration | `cargo test --test integration client_tracking` | No - Wave 0 |
| -- | Invalidation Push frame delivery | integration | `cargo test --test integration tracking_invalidation` | No - Wave 0 |
| -- | RESP2 backward compatibility | integration | `cargo test --test integration` (existing) | Yes - existing tests |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] RESP3 parse tests in `src/protocol/parse.rs` -- unit tests for all 8 new frame types
- [ ] RESP3 serialize tests in `src/protocol/serialize.rs` -- round-trip tests for all new types
- [ ] HELLO command unit tests in `src/command/connection.rs`
- [ ] Response conversion tests (array_to_map, array_to_set, int_to_bool, bulk_to_double)
- [ ] Integration tests for RESP3 negotiation via HELLO
- [ ] Integration tests for CLIENT TRACKING invalidation

## Sources

### Primary (HIGH confidence)
- [Redis protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) - All RESP3 type formats, prefix bytes, wire encoding
- [HELLO command docs](https://redis.io/docs/latest/commands/hello/) - Command syntax, response fields, protocol switching behavior
- [CLIENT TRACKING docs](https://redis.io/docs/latest/commands/client-tracking/) - All options, tracking modes, invalidation behavior
- [Client-side caching reference](https://redis.io/docs/latest/develop/reference/client-side-caching/) - Tracking table internals, invalidation message format, memory management

### Secondary (MEDIUM confidence)
- [Redis RESP3 specification (GitHub)](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md) - Full spec with streaming types, attribute type
- [antirez RESP3 repo](https://github.com/antirez/RESP3/blob/master/spec.md) - Original spec author's repository

### Tertiary (LOW confidence)
- Command-to-RESP3-type mapping table: assembled from multiple sources and Redis docs per-command pages; individual mappings should be verified against Redis 7+ behavior

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - no new dependencies needed; existing crates sufficient
- Architecture: HIGH - clean extension of existing Frame/parser/serializer pattern; response switching is well-defined
- RESP3 wire format: HIGH - verified against official Redis protocol spec
- HELLO command: HIGH - verified against official Redis command docs
- CLIENT TRACKING: HIGH - verified against official docs and client-side caching reference
- Command-to-RESP3 mapping: MEDIUM - assembled from multiple sources; individual mappings need verification
- Pitfalls: HIGH - derived from direct codebase analysis (inline parser collision, PartialEq, codec awareness)

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable -- RESP3 spec is finalized since Redis 6.0)
