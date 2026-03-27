# Phase 2: Working Server - Research

**Researched:** 2026-03-23
**Domain:** Async TCP server, command dispatch, in-memory storage, key expiration
**Confidence:** HIGH

## Summary

Phase 2 transforms the Phase 1 protocol parser/serializer into a running Redis server. The work spans four major domains: (1) wrapping the existing parser/serializer in a tokio-util codec for framed TCP I/O, (2) building the storage engine (Database with HashMap, Entry with RedisValue enum, lazy expiration), (3) implementing ~30 command handlers across connection, string, and key management families, and (4) wiring it all together with a TCP listener, per-connection tasks, graceful shutdown, and CLI configuration.

The existing Phase 1 code provides a solid foundation: `protocol::parse()` already works on `BytesMut` with the exact signature the tokio-util `Decoder` trait expects (`&mut BytesMut -> Result<Option<Frame>, Error>`), and `protocol::serialize()` writes `Frame` to `BytesMut` matching the `Encoder` pattern. The codec integration is mechanical. The command handlers are pure functions (`(&Database, &[Frame]) -> Frame`) that are independently testable without networking.

The primary risk is redis-cli compatibility: redis-cli sends `COMMAND DOCS` on startup for tab-completion, and many clients send `COMMAND` to discover server capabilities. These must return non-error responses (even if minimal) or redis-cli will behave poorly. The CONTEXT.md decisions lock the architecture: `Arc<Mutex<Vec<Database>>>` with `std::sync::Mutex`, `Framed<TcpStream, RespCodec>`, and lazy-only expiration for this phase.

**Primary recommendation:** Build bottom-up -- storage engine first (testable without networking), then command handlers (testable against storage without networking), then codec + connection + server (integration layer). Validate with redis-cli at each integration milestone.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Single `Database` struct wrapping `HashMap<Bytes, Entry>` where `Entry` holds a `RedisValue` enum + optional expiration metadata
- `RedisValue` enum starts with `String(Bytes)` variant -- other types added in Phase 3
- Keys are `Bytes` (binary-safe, consistent with Frame type, avoids UTF-8 validation overhead)
- 16 pre-allocated databases in a `Vec<Database>` -- `SELECT` switches index (matches Redis default)
- Concurrent access via `Arc<Mutex<Vec<Database>>>` using `std::sync::Mutex` (NOT `tokio::sync::Mutex`) for short critical sections
- Entry struct contains: `value: RedisValue`, `expires_at: Option<Instant>`, `created_at: Instant`
- Extract command name from first element of `Frame::Array`, convert to uppercase for case-insensitive matching
- Match command name string to handler functions -- pure functions taking `(&mut Database, &[Frame]) -> Frame`
- Each handler validates its own arguments and returns `ERR wrong number of arguments for '{cmd}' command` on invalid arity
- Unknown commands return `ERR unknown command '{cmd}'` with suggestion matching Redis behavior
- Command handlers return `Frame` directly -- serialized by the connection layer
- `Connection` struct holding: `TcpStream` (wrapped in tokio BufReader/Writer or codec), selected database index (default 0), write buffer
- tokio-util `Framed<TcpStream, RespCodec>` wrapping the Phase 1 parser/serializer as a codec
- Per-connection tokio task: read frame -> dispatch command -> write response -> loop
- Client disconnection: drop connection task on EOF or I/O error
- Graceful shutdown: `tokio::signal::ctrl_c()` with `tokio::select!` -- stop accepting, drain existing connections with timeout
- Lazy expiration on every key read and write: check `expires_at` before returning, silently delete and return nil if expired
- Expiration stored as `Option<Instant>` in Entry -- `None` means no TTL, `Instant` is monotonic
- EXPIRE/PEXPIRE set the `expires_at` field; TTL/PTTL compute remaining duration from `Instant`
- PERSIST removes the `expires_at` (sets to `None`)
- Pipelining falls out naturally from codec + loop architecture
- Configurable bind address and port via CLI args (clap) -- default `127.0.0.1:6379`
- Configurable number of databases (default 16)

### Claude's Discretion
- Exact module file layout for server, storage, and command modules
- tokio-util codec implementation details (Decoder/Encoder trait impls)
- Internal buffer sizing for connection read/write
- Test strategy (integration tests with actual TCP connections vs unit tests on handlers)
- Whether to use a `CommandRouter` struct or a simple match statement for dispatch
- Error message formatting details beyond the Redis-standard format

### Deferred Ideas (OUT OF SCOPE)
- Actor model migration (channel-based single data owner) -- defer to optimization if Mutex contention detected
- Zero-copy slicing in codec layer (avoid Bytes::copy_from_slice) -- optimization for later
- Active expiration (background sweep) -- Phase 3
- SCAN command -- Phase 3
- AUTH -- Phase 3
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PROTO-03 | Command pipelining support | Falls out naturally from Framed codec + read-loop architecture; no special pipelining code needed |
| NET-01 | Concurrent TCP connections via Tokio | tokio::net::TcpListener + tokio::spawn per connection; Arc<Mutex<Vec<Database>>> for shared state |
| NET-02 | Graceful shutdown (SIGTERM/SIGINT) | tokio::signal::ctrl_c() + tokio_util::sync::CancellationToken for coordinated shutdown |
| NET-03 | Configurable bind address and port | clap derive macro for CLI args; Config struct with defaults |
| CONN-01 | PING/PONG | Return SimpleString("PONG") or echo argument as BulkString |
| CONN-02 | ECHO | Return argument as BulkString |
| CONN-03 | QUIT | Send +OK, then close connection |
| CONN-04 | SELECT database (0-15) | Per-connection database index; validate range, switch index |
| CONN-05 | COMMAND/COMMAND DOCS | Return empty array for COMMAND DOCS, integer count for COMMAND; must not error |
| CONN-07 | INFO with minimal sections | Return BulkString with key=value pairs in server/clients/memory/keyspace sections |
| STR-01 | GET and SET | Core handlers; SET stores Entry with RedisValue::String(Bytes), GET returns with expiry check |
| STR-02 | SET with EX/PX/NX/XX | Parse options from Frame args; compute expires_at from Duration; conditional set logic |
| STR-03 | MGET and MSET | Multi-key handlers; MSET is atomic (all-or-nothing under Mutex), MGET returns Array of values/nulls |
| STR-04 | INCR/DECR/INCRBY/DECRBY | Parse stored string as i64, apply delta, store back; handle overflow and non-integer errors |
| STR-05 | INCRBYFLOAT | Parse as f64, apply delta, format with Redis-compatible float output |
| STR-06 | APPEND | Concatenate bytes; create key if not exists |
| STR-07 | STRLEN | Return length of stored string; 0 if key missing |
| STR-08 | SETNX/SETEX/PSETEX | Legacy wrappers over SET with NX/EX/PX options |
| STR-09 | GETSET/GETDEL/GETEX | Atomic get-and-modify: return old value, apply modification |
| KEY-01 | DEL one or more keys | Remove entries, return count of deleted keys |
| KEY-02 | EXISTS | Count how many of the specified keys exist |
| KEY-03 | EXPIRE/PEXPIRE/TTL/PTTL | Set/query expiration; return -1 for no TTL, -2 for missing key |
| KEY-04 | PERSIST | Remove expiration from key |
| KEY-05 | TYPE | Return string name of value type ("string", "none") |
| KEY-06 | KEYS pattern | Pattern matching with glob-style wildcards over keyspace; return Array |
| KEY-08 | RENAME/RENAMENX | Move entry to new key; RENAMENX only if target does not exist |
| EXP-01 | Lazy expiration | Check expires_at on every key access; delete and return nil if expired |
</phase_requirements>

## Standard Stack

### Core (Phase 2 additions to Cargo.toml)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.50 | Async runtime, TCP, signals, task spawning | De facto async runtime for Rust; LTS releases; features: rt-multi-thread, net, io-util, macros, signal |
| tokio-util | 0.7.18 | Codec framework (Decoder/Encoder/Framed) | Wraps Phase 1 parser/serializer into stream-based framed I/O over TCP |
| clap | 4.6 | CLI argument parsing | Derive-based; handles --port, --bind, --databases |
| tracing | 0.1.44 | Structured logging | Standard for async Rust; spans for per-connection context |
| tracing-subscriber | 0.3.23 | Log output formatting | fmt subscriber with env-filter for level control |
| anyhow | 1.0.102 | Application-level errors in main.rs | Context chaining for startup/config errors; NOT in library code |

### Already in Cargo.toml (from Phase 1)

| Library | Version | Purpose |
|---------|---------|---------|
| bytes | 1.10 | Zero-copy byte buffers (Bytes, BytesMut) |
| thiserror | 2.0 | Structured error types for library code |
| tikv-jemallocator | 0.6 | Global memory allocator |

### Dev Dependencies (Phase 2 additions)

| Library | Version | Purpose |
|---------|---------|---------|
| tokio (test features) | 1.50 | `#[tokio::test]` for async integration tests |
| redis (client crate) | 0.27 | Integration testing with real Redis client |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| clap | std::env::args | clap adds ~200KB binary but gives help text, validation, derive macros for free |
| tracing | log + env_logger | tracing supports async spans and structured fields; log is simpler but less capable |
| anyhow | custom error types | anyhow is only for main.rs; library code uses thiserror |
| Arc<Mutex<>> | mpsc channels (actor) | Actor model is more faithful to Redis but more complex; Mutex is simpler for Phase 2 |

**Installation:**
```bash
cargo add tokio --features rt-multi-thread,net,io-util,macros,signal
cargo add tokio-util --features codec
cargo add clap --features derive
cargo add tracing
cargo add tracing-subscriber --features fmt,env-filter
cargo add anyhow
cargo add --dev redis --features tokio-comp
```

## Architecture Patterns

### Recommended Project Structure

```
src/
├── main.rs                 # Entry point: parse CLI, init tracing, start server
├── lib.rs                  # Re-exports: protocol, server, storage, command modules
├── protocol/               # [Phase 1 -- unchanged]
│   ├── mod.rs
│   ├── frame.rs
│   ├── parse.rs
│   ├── serialize.rs
│   └── inline.rs
├── server/
│   ├── mod.rs              # Re-exports
│   ├── listener.rs         # TcpListener accept loop, spawn per-connection tasks
│   ├── connection.rs       # Connection struct: Framed<TcpStream, RespCodec>, db index
│   ├── codec.rs            # RespCodec: Decoder<Item=Frame> + Encoder<Frame>
│   └── shutdown.rs         # CancellationToken-based graceful shutdown
├── storage/
│   ├── mod.rs              # Re-exports
│   ├── db.rs               # Database struct: HashMap<Bytes, Entry>, expiry check methods
│   └── entry.rs            # Entry struct, RedisValue enum
├── command/
│   ├── mod.rs              # dispatch() function: match command name -> handler
│   ├── connection.rs       # PING, ECHO, QUIT, SELECT, COMMAND, INFO
│   ├── string.rs           # GET, SET, MGET, MSET, INCR/DECR family, APPEND, STRLEN, etc.
│   └── key.rs              # DEL, EXISTS, EXPIRE/PEXPIRE, TTL/PTTL, PERSIST, TYPE, KEYS, RENAME
└── config.rs               # ServerConfig struct (bind, port, databases)
```

**Rationale:**
- `server/codec.rs` is separate from `protocol/` because the codec wraps the protocol parser, it does not modify it. Protocol module stays pure and dependency-free.
- `command/` handlers are pure functions: `fn handle_xyz(db: &mut Database, args: &[Frame]) -> Frame`. Testable without networking.
- `storage/` owns all state. The `Database` struct provides methods like `get_checked()` that perform lazy expiry before returning.

### Pattern 1: tokio-util Codec Wrapping Existing Parser

**What:** Implement `Decoder` and `Encoder` traits on a `RespCodec` struct that delegates to the existing `protocol::parse()` and `protocol::serialize()` functions.

**Why:** The Phase 1 parser already has the exact signature tokio-util expects. `parse(&mut BytesMut, &ParseConfig) -> Result<Option<Frame>, ParseError>` maps directly to `Decoder::decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>`.

**Example:**
```rust
use tokio_util::codec::{Decoder, Encoder};
use bytes::BytesMut;
use crate::protocol::{self, Frame, ParseConfig, ParseError};

pub struct RespCodec {
    config: ParseConfig,
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        match protocol::parse(src, &self.config) {
            Ok(frame) => Ok(frame),
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        protocol::serialize(&item, dst);
        Ok(())
    }
}
```

**Key detail:** The `Decoder::Error` type must implement `From<std::io::Error>`. Using `std::io::Error` directly is simplest. The `ParseError::Incomplete` case is already handled by the parser returning `Ok(None)` -- but the codec should also handle it defensively.

### Pattern 2: Command Dispatch via Match Statement

**What:** Simple match on uppercase command name string, delegating to handler functions.

**Why:** A match statement is clearer than a CommandRouter struct for ~30 commands. No vtable overhead, exhaustive matching, easy to add commands.

**Example:**
```rust
pub fn dispatch(db: &mut Database, frame: Frame) -> Frame {
    let args = match frame {
        Frame::Array(args) if !args.is_empty() => args,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid command format")),
    };

    // Extract command name, convert to uppercase
    let cmd_name = match &args[0] {
        Frame::BulkString(s) => s.to_ascii_uppercase(),
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid command name")),
    };

    let cmd_args = &args[1..];

    match cmd_name.as_ref() {
        b"PING" => connection::ping(cmd_args),
        b"ECHO" => connection::echo(cmd_args),
        b"SET" => string::set(db, cmd_args),
        b"GET" => string::get(db, cmd_args),
        // ... etc
        _ => Frame::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(&cmd_name)
        ))),
    }
}
```

### Pattern 3: Lazy Expiration on Every Access

**What:** Every method that reads or writes a key checks `expires_at` first. If expired, silently remove the entry and behave as if the key does not exist.

**Example:**
```rust
impl Database {
    /// Get a key, performing lazy expiration check.
    /// Returns None if key is missing OR expired.
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        // Check expiration first
        if self.is_expired(key) {
            self.data.remove(key);
            return None;
        }
        self.data.get(key)
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        self.data.get(key)
            .and_then(|e| e.expires_at)
            .map_or(false, |exp| Instant::now() >= exp)
    }
}
```

### Pattern 4: Graceful Shutdown with CancellationToken

**What:** Use `tokio_util::sync::CancellationToken` (part of tokio-util) to coordinate shutdown across the listener and all connection tasks.

**Example:**
```rust
let token = CancellationToken::new();
let shutdown_token = token.clone();

// Spawn signal handler
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    shutdown_token.cancel();
});

// Accept loop
loop {
    tokio::select! {
        result = listener.accept() => {
            let (stream, _) = result?;
            let conn_token = token.child_token();
            tokio::spawn(handle_connection(stream, db.clone(), conn_token));
        }
        _ = token.cancelled() => {
            break; // Stop accepting
        }
    }
}
```

### Anti-Patterns to Avoid

- **Using `tokio::sync::Mutex` for Database:** The lock is held for microsecond HashMap operations with no `.await` inside. `std::sync::Mutex` is faster for this pattern and cannot accidentally be held across await points.
- **Yielding inside command handlers:** Never `.await` inside a handler that modifies the database. This would break atomicity of multi-key commands (MSET, RENAME) and allow other tasks to observe partial state.
- **Parsing command args as `String`:** Keep everything as `Bytes`. Converting to `String` adds UTF-8 validation overhead and allocation. Only convert to string when needed (e.g., parsing integers with `std::str::from_utf8`).
- **Separate response serialization step:** Do not buffer Frame responses and serialize them later in a batch. Serialize immediately via the Encoder. Pipelining works because the Framed transport batches writes at the TCP level.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| TCP framing | Custom read loop with BytesMut | tokio-util `Framed<TcpStream, RespCodec>` | Handles partial reads, write batching, backpressure automatically |
| CLI parsing | Manual arg parsing | clap 4.6 with derive | Handles --help, validation, defaults, env vars |
| Signal handling | Manual signal() | `tokio::signal::ctrl_c()` | Cross-platform, async-safe |
| Shutdown coordination | Manual AtomicBool | `tokio_util::sync::CancellationToken` | Hierarchical cancellation, child tokens per connection |
| Structured logging | println! | tracing + tracing-subscriber | Structured fields, log levels, per-connection spans |
| Glob pattern matching for KEYS | Regex or hand-rolled | Implement a minimal glob matcher (only `*`, `?`, `[abc]`, `\x` needed) | Redis KEYS uses simplified glob, not full regex. A ~50 line matcher suffices. Do NOT pull in regex crate for this. |

**Key insight:** The codec layer is the biggest "don't hand-roll" win. Without tokio-util Framed, you would need to manually manage read buffers, handle partial reads, detect frame boundaries, manage write buffering, and handle backpressure. The Framed adapter does all of this.

## Common Pitfalls

### Pitfall 1: redis-cli Sends COMMAND DOCS on Connect
**What goes wrong:** redis-cli sends `COMMAND DOCS` on startup for tab-completion. If the server returns an error, redis-cli still works but shows error messages and loses tab-completion.
**Why it happens:** Developers test with custom TCP clients that skip the handshake.
**How to avoid:** Implement `COMMAND` returning `*0\r\n` (empty array) and `COMMAND DOCS` returning `*0\r\n` as stubs. These are enough to keep redis-cli happy.
**Warning signs:** redis-cli shows errors on connect or tab-completion does not work.

### Pitfall 2: INCR/DECR Integer Overflow
**What goes wrong:** Redis returns an error when INCR/DECR would overflow i64 range. Naive implementations either wrap (wrong) or panic.
**Why it happens:** Forgetting to check `checked_add`/`checked_sub` results.
**How to avoid:** Use `i64::checked_add()` / `i64::checked_sub()`. Return `ERR increment or decrement would overflow` on failure.
**Warning signs:** Large counter values producing unexpected results.

### Pitfall 3: SET with NX+XX is Always a No-Op
**What goes wrong:** If both NX and XX are specified, the command should still work (NX means "only if not exists", XX means "only if exists" -- they are mutually exclusive conditions, so one always fails). Redis 7.4+ returns an error for NX+XX combinations, but earlier versions silently returned nil.
**How to avoid:** Follow Redis behavior: treat NX and XX as mutually exclusive. If both specified, return nil (key cannot both exist and not exist).

### Pitfall 4: KEYS Pattern Matching Edge Cases
**What goes wrong:** The KEYS glob pattern must match Redis's exact glob semantics: `*` matches any sequence, `?` matches any single char, `[abc]` matches character class, `\` escapes special chars. Using regex or a different glob flavor produces incompatible results.
**How to avoid:** Write a dedicated Redis glob matcher. Test with: `h?llo` matches hello/hallo, `h*llo` matches hllo/heeeello, `h[ae]llo` matches hello/hallo, `h[^e]llo` matches hallo but not hello.

### Pitfall 5: RENAME to Same Key
**What goes wrong:** `RENAME key key` (renaming to itself) should succeed and be a no-op in Redis. Naive implementation might delete the key first then try to set it.
**How to avoid:** Check if source == destination early. If same, return OK immediately.

### Pitfall 6: TTL/PTTL Return Values
**What goes wrong:** TTL/PTTL have three distinct return values: the remaining time (positive integer), -1 (key exists but has no TTL), -2 (key does not exist). Returning -1 for non-existent keys is wrong.
**How to avoid:** Check key existence first. If key does not exist (or is expired via lazy check), return -2. If key exists but has no expiry, return -1. Otherwise return remaining duration.

### Pitfall 7: MGET Must Return Nil for Missing Keys (Not Skip Them)
**What goes wrong:** MGET returns an array of values in the same order as the requested keys. Missing keys must be represented as Null in the array, not omitted. If a response has fewer elements than requested keys, clients will map values to wrong keys.
**How to avoid:** Always return an array with exactly N elements where N is the number of requested keys. Use Frame::Null for missing/expired keys.

### Pitfall 8: Float Formatting for INCRBYFLOAT
**What goes wrong:** Redis returns floats in a specific format: no trailing zeros, no scientific notation, "inf"/"-inf" for infinity. Rust's default `format!("{}", f)` may produce scientific notation for very large/small values.
**How to avoid:** Format floats explicitly. Remove trailing zeros after the decimal point. Handle inf/-inf/nan as errors.

## Code Examples

### Complete Connection Handler Pattern
```rust
// Source: Derived from tokio-util Framed docs + CONTEXT.md decisions
async fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<Vec<Database>>>,
    shutdown: CancellationToken,
) {
    let codec = RespCodec::new(ParseConfig::default());
    let mut framed = Framed::new(stream, codec);
    let mut selected_db: usize = 0;

    loop {
        tokio::select! {
            result = framed.next() => {
                match result {
                    Some(Ok(frame)) => {
                        let response = {
                            let mut dbs = db.lock().unwrap();
                            dispatch(&mut dbs[selected_db], frame, &mut selected_db)
                        };
                        if framed.send(response).await.is_err() {
                            break; // Write error, client disconnected
                        }
                    }
                    Some(Err(_)) => break, // Protocol error
                    None => break, // EOF, client disconnected
                }
            }
            _ = shutdown.cancelled() => {
                // Graceful shutdown: send error and close
                let _ = framed.send(Frame::Error(
                    Bytes::from_static(b"ERR server shutting down")
                )).await;
                break;
            }
        }
    }
}
```

### SET Command Handler with All Options
```rust
// Source: Redis SET command spec (https://redis.io/docs/latest/commands/set/)
pub fn set(db: &mut Database, args: &[Frame]) -> Frame {
    // SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT tms|KEEPTTL]
    if args.len() < 2 {
        return err_wrong_args("SET");
    }
    let key = match &args[0] {
        Frame::BulkString(k) => k.clone(),
        _ => return err_wrong_type(),
    };
    let value = match &args[1] {
        Frame::BulkString(v) => v.clone(),
        _ => return err_wrong_type(),
    };

    // Parse options from remaining args
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut expiry: Option<Duration> = None;
    // ... parse args[2..] for EX/PX/NX/XX/GET/KEEPTTL/EXAT/PXAT

    // Condition check
    let exists = db.exists(&key);
    if nx && exists { return if get { /* return old value */ } else { Frame::Null }; }
    if xx && !exists { return if get { Frame::Null } else { Frame::Null }; }

    let old_value = if get { db.get(&key).map(|e| e.to_frame()) } else { None };

    // Compute expires_at
    let expires_at = expiry.map(|d| Instant::now() + d);

    db.set(key, Entry {
        value: RedisValue::String(value),
        expires_at,
        created_at: Instant::now(),
    });

    old_value.unwrap_or(Frame::SimpleString(Bytes::from_static(b"OK")))
}
```

### INCR Handler with Overflow Protection
```rust
pub fn incr(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("INCR");
    }
    let key = extract_key(&args[0]);

    // Get current value or default to 0
    let current = match db.get(&key) {
        Some(entry) => match entry.as_i64() {
            Ok(n) => n,
            Err(_) => return Frame::Error(
                Bytes::from_static(b"ERR value is not an integer or out of range")
            ),
        },
        None => 0,
    };

    match current.checked_add(1) {
        Some(new_val) => {
            db.set_string(&key, new_val.to_string().into());
            Frame::Integer(new_val)
        }
        None => Frame::Error(
            Bytes::from_static(b"ERR increment or decrement would overflow")
        ),
    }
}
```

### Minimal INFO Response
```rust
// Source: Redis INFO command reference
pub fn info(db: &Database, args: &[Frame]) -> Frame {
    let mut sections = String::new();

    sections.push_str("# Server\r\n");
    sections.push_str("redis_version:0.1.0\r\n");
    sections.push_str("rust_redis_version:0.1.0\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Keyspace\r\n");
    // db0:keys=N,expires=M,avg_ttl=0
    let key_count = db.len();
    let expires_count = db.expires_count();
    sections.push_str(&format!("db0:keys={},expires={},avg_ttl=0\r\n", key_count, expires_count));

    Frame::BulkString(Bytes::from(sections))
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Manual read/write loops | tokio-util Framed codec | tokio-util 0.7+ (2023) | Eliminates manual buffer management for TCP framing |
| `tokio::sync::Mutex` for shared state | `std::sync::Mutex` for short critical sections | Mini-redis pattern (2022+) | Avoids async lock overhead when no .await inside critical section |
| Manual signal handling | `tokio::signal::ctrl_c()` | tokio 1.0 (2021) | Cross-platform async signal handling |
| Separate CancellationToken crate | `tokio_util::sync::CancellationToken` | tokio-util 0.7 | Built into tokio-util, no separate dependency |

**Deprecated/outdated:**
- `tokio::io::BufReader`/`BufWriter` for protocol framing: Use `Framed<TcpStream, Codec>` instead
- `futures::StreamExt` for reading frames: tokio-util re-exports what is needed via `tokio_stream`

## Open Questions

1. **COMMAND DOCS exact response format**
   - What we know: redis-cli sends COMMAND DOCS on connect. An empty response works.
   - What is unclear: Whether returning an empty array vs a specific format affects redis-cli behavior.
   - Recommendation: Return `*0\r\n` (empty array) for both COMMAND and COMMAND DOCS. Test with redis-cli. Iterate if needed.

2. **KEYS glob pattern: character class ranges**
   - What we know: Redis supports `[a-z]` ranges and `[^x]` negation in KEYS patterns.
   - What is unclear: Full edge cases in Redis's glob implementation (e.g., does `[z-a]` work? Unicode behavior?).
   - Recommendation: Implement basic glob (`*`, `?`, `[chars]`, `[^chars]`, `\escape`). Test against Redis behavior. Redis keys are binary-safe but glob matching operates on byte values.

3. **INCRBYFLOAT precision and formatting**
   - What we know: Redis uses long double internally (80-bit on x86). Rust uses f64 (64-bit double).
   - What is unclear: Whether f64 precision differences will cause compatibility issues in practice.
   - Recommendation: Use f64. Format output to remove trailing zeros. This matches what most Redis clones do and is sufficient for practical use.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + tokio::test for async |
| Config file | none -- see Wave 0 |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PROTO-03 | Pipelining: multiple commands in one TCP write produce ordered responses | integration | `cargo test --test integration pipeline` | No -- Wave 0 |
| NET-01 | Multiple concurrent redis-cli sessions | integration | `cargo test --test integration concurrent` | No -- Wave 0 |
| NET-02 | Graceful shutdown on ctrl-c | integration | `cargo test --test integration shutdown` | No -- Wave 0 |
| NET-03 | Configurable port via --port flag | unit | `cargo test --lib config` | No -- Wave 0 |
| CONN-01 | PING returns PONG | unit | `cargo test --lib command::connection::test_ping` | No -- Wave 0 |
| CONN-02 | ECHO returns argument | unit | `cargo test --lib command::connection::test_echo` | No -- Wave 0 |
| CONN-04 | SELECT switches database | unit | `cargo test --lib command::connection::test_select` | No -- Wave 0 |
| CONN-05 | COMMAND DOCS returns non-error | unit | `cargo test --lib command::connection::test_command_docs` | No -- Wave 0 |
| CONN-07 | INFO returns BulkString with sections | unit | `cargo test --lib command::connection::test_info` | No -- Wave 0 |
| STR-01 | GET/SET round-trip | unit | `cargo test --lib command::string::test_get_set` | No -- Wave 0 |
| STR-02 | SET with EX/NX/XX options | unit | `cargo test --lib command::string::test_set_options` | No -- Wave 0 |
| STR-03 | MGET/MSET multi-key operations | unit | `cargo test --lib command::string::test_mget_mset` | No -- Wave 0 |
| STR-04 | INCR/DECR integer operations | unit | `cargo test --lib command::string::test_incr_decr` | No -- Wave 0 |
| STR-05 | INCRBYFLOAT precision | unit | `cargo test --lib command::string::test_incrbyfloat` | No -- Wave 0 |
| KEY-01 | DEL removes keys | unit | `cargo test --lib command::key::test_del` | No -- Wave 0 |
| KEY-02 | EXISTS counts existing keys | unit | `cargo test --lib command::key::test_exists` | No -- Wave 0 |
| KEY-03 | EXPIRE/TTL set and query expiration | unit | `cargo test --lib command::key::test_expire_ttl` | No -- Wave 0 |
| KEY-06 | KEYS glob pattern matching | unit | `cargo test --lib command::key::test_keys_pattern` | No -- Wave 0 |
| KEY-08 | RENAME moves key | unit | `cargo test --lib command::key::test_rename` | No -- Wave 0 |
| EXP-01 | Lazy expiration on access | unit | `cargo test --lib storage::test_lazy_expiry` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green + manual redis-cli smoke test before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/integration.rs` -- integration tests with TCP connections using redis client crate
- [ ] Unit test modules in each `command/*.rs` file -- inline `#[cfg(test)]` modules
- [ ] Unit test module in `storage/db.rs` -- Database operations and lazy expiry
- [ ] Unit test module in `server/codec.rs` -- Decoder/Encoder round-trip
- [ ] `Cargo.toml` dev-dependency: `redis = { version = "0.27", features = ["tokio-comp"] }`
- [ ] `Cargo.toml` dev-dependency: `tokio = { version = "1.50", features = ["test-util"] }`

## Sources

### Primary (HIGH confidence)
- [tokio-util codec docs](https://docs.rs/tokio-util/latest/tokio_util/codec/) - Decoder/Encoder trait signatures, Framed usage
- [Redis SET command](https://redis.io/docs/latest/commands/set/) - SET EX/PX/NX/XX/GET/EXAT/PXAT/KEEPTTL options
- [Redis Commands Reference](https://redis.io/docs/latest/commands/) - Authoritative command semantics
- Phase 1 source code (`src/protocol/`) - Existing parser/serializer API
- `.planning/research/STACK.md` - Verified library versions and choices
- `.planning/research/ARCHITECTURE.md` - Arc<Mutex> starting approach, project structure
- `.planning/research/PITFALLS.md` - Pitfalls #5, #12, #13 directly relevant

### Secondary (MEDIUM confidence)
- [tokio mini-redis](https://github.com/tokio-rs/mini-redis) - Reference implementation patterns (connection handling, shared state)
- `.planning/research/FEATURES.md` - Command priorities and dependency chains

### Tertiary (LOW confidence)
- redis-cli startup sequence specifics - Based on experience; exact commands sent may vary by version

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All crate versions verified against crates.io registry
- Architecture: HIGH - Decisions locked in CONTEXT.md, patterns well-established (mini-redis reference)
- Pitfalls: HIGH - Drawn from project research + Redis documentation
- Command semantics: MEDIUM - Redis docs are authoritative but edge cases need testing with real redis-cli

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, 30-day validity)
