# Phase 2: Working Server - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

A running TCP server that accepts redis-cli connections and executes string commands, key management, and lazy expiration. This is the first "working Redis" — redis-cli can connect, PING, SET/GET keys with TTL, and perform atomic integer operations. Covers ~30 commands across connection, strings, and key management.

</domain>

<decisions>
## Implementation Decisions

### Storage Engine Design
- Single `Database` struct wrapping `HashMap<Bytes, Entry>` where `Entry` holds a `RedisValue` enum + optional expiration metadata
- `RedisValue` enum starts with `String(Bytes)` variant — other types added in Phase 3
- Keys are `Bytes` (binary-safe, consistent with Frame type, avoids UTF-8 validation overhead)
- 16 pre-allocated databases in a `Vec<Database>` — `SELECT` switches index (matches Redis default)
- Concurrent access via `Arc<Mutex<Vec<Database>>>` using `std::sync::Mutex` (NOT `tokio::sync::Mutex`) for short critical sections — simple start, migrate to actor model if benchmarks show contention
- Entry struct contains: `value: RedisValue`, `expires_at: Option<Instant>`, `created_at: Instant`

### Command Dispatch Architecture
- Extract command name from first element of `Frame::Array`, convert to uppercase for case-insensitive matching
- Match command name string to handler functions — pure functions taking `(&mut Database, &[Frame]) -> Frame`
- Each handler validates its own arguments and returns `ERR wrong number of arguments for '{cmd}' command` on invalid arity (matches Redis error format)
- Unknown commands return `ERR unknown command '{cmd}'` with suggestion matching Redis behavior
- Command handlers return `Frame` directly — serialized by the connection layer

### Connection Lifecycle
- `Connection` struct holding: `TcpStream` (wrapped in tokio BufReader/Writer or codec), selected database index (default 0), write buffer
- tokio-util `Framed<TcpStream, RespCodec>` wrapping the Phase 1 parser/serializer as a codec — this is the deferred codec integration from Phase 1
- Per-connection tokio task: read frame → dispatch command → write response → loop
- Client disconnection: drop connection task on EOF or I/O error — tokio handles cleanup
- Graceful shutdown: `tokio::signal::ctrl_c()` with `tokio::select!` — stop accepting, drain existing connections with timeout

### Key Expiration (Lazy Only)
- Lazy expiration on every key read and write: check `expires_at` before returning, silently delete and return nil if expired
- Expiration stored as `Option<Instant>` in Entry — `None` means no TTL, `Instant` is monotonic
- EXPIRE/PEXPIRE set the `expires_at` field; TTL/PTTL compute remaining duration from `Instant`
- PERSIST removes the `expires_at` (sets to `None`)
- Expired keys are invisible — any access returns as if key doesn't exist

### Pipelining Support
- Falls out naturally from the codec + loop architecture — read all available frames, process sequentially, write all responses
- No special pipelining code needed — just don't require request-response lockstep in the read/write loop

### Server Configuration
- Configurable bind address and port via CLI args (clap) — default `127.0.0.1:6379`
- Configurable number of databases (default 16)
- Config struct passed to server on startup

### Claude's Discretion
- Exact module file layout for server, storage, and command modules
- tokio-util codec implementation details (Decoder/Encoder trait impls)
- Internal buffer sizing for connection read/write
- Test strategy (integration tests with actual TCP connections vs unit tests on handlers)
- Whether to use a `CommandRouter` struct or a simple match statement for dispatch
- Error message formatting details beyond the Redis-standard format

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Protocol Layer (built in Phase 1)
- `src/protocol/mod.rs` — Public API: `parse()`, `serialize()`, `parse_inline()`, `Frame`, `ParseConfig`, `ParseError`
- `src/protocol/frame.rs` — Frame enum definition with 6 variants, ParseError, ParseConfig
- `src/protocol/parse.rs` — Streaming two-pass parser (check then parse on BytesMut)
- `src/protocol/serialize.rs` — Frame serializer writing to BytesMut

### Project Research
- `.planning/research/STACK.md` — tokio, bytes, jemalloc stack choices
- `.planning/research/ARCHITECTURE.md` — Component boundaries, single-threaded data path, Arc<Mutex> starting approach
- `.planning/research/PITFALLS.md` — Pitfall #5 (wrong threading model), Pitfall #12 (command atomicity), Pitfall #13 (redis-cli compatibility)
- `.planning/research/FEATURES.md` — String commands cover 60-80% of Redis traffic, command list by priority

### Redis Command Reference
- No local spec — use official Redis command reference for exact command semantics: https://redis.io/docs/latest/commands/

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `protocol::Frame` — the shared interface type for all command input/output
- `protocol::parse()` — streaming parser on BytesMut, returns `Result<Option<Frame>, ParseError>`
- `protocol::serialize()` — writes Frame to BytesMut
- `protocol::ParseConfig` — configurable limits (max depth, max bulk string size)
- `protocol::parse_inline()` — inline command parser producing Frame::Array
- jemalloc already configured as global allocator in `src/main.rs`

### Established Patterns
- Functional coding patterns: pure functions, minimal mutation, composition
- `Bytes` for all string payloads (zero-copy, reference-counted)
- Inline tests within each module file
- `thiserror` for structured error types

### Integration Points
- Phase 2 wraps `protocol::parse()` and `protocol::serialize()` in a tokio-util codec (Decoder/Encoder)
- Frame type becomes the command input format — handlers receive `&[Frame]` args
- `Bytes::copy_from_slice` is the current pattern for payload extraction (from Phase 1 decision)

</code_context>

<specifics>
## Specific Ideas

- redis-cli compatibility is the key validation: `redis-cli -p 6379` should connect, PING, SET/GET, INCR work out of the box
- Command handlers should be dead-simple pure functions — easy to test in isolation without TCP
- The codec integration from Phase 1's deferred idea lands here — wrap parse/serialize in tokio-util Framed

</specifics>

<deferred>
## Deferred Ideas

- Actor model migration (channel-based single data owner) — defer to optimization if Mutex contention detected
- Zero-copy slicing in codec layer (avoid Bytes::copy_from_slice) — optimization for later
- Active expiration (background sweep) — Phase 3
- SCAN command — Phase 3
- AUTH — Phase 3

</deferred>

---

*Phase: 02-working-server*
*Context gathered: 2026-03-23*
