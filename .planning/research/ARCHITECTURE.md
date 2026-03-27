# Architecture Research

**Domain:** Redis-compatible in-memory key-value store in Rust
**Researched:** 2026-03-23
**Confidence:** HIGH

## Standard Architecture

### System Overview

```
                         Client Connections (redis-cli, client libs)
                                    |
                                    v
┌──────────────────────────────────────────────────────────────────────┐
│                        Networking Layer                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │ IO Task  │  │ IO Task  │  │ IO Task  │  │ IO Task  │  (tokio)   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │
│       │              │              │              │                 │
│       └──────────────┴──────┬───────┴──────────────┘                │
│                             │ mpsc channel                          │
├─────────────────────────────┼────────────────────────────────────────┤
│                        Protocol Layer                                │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │           RESP Parser / Serializer (zero-copy)              │    │
│  │           Frame <-> Command translation                     │    │
│  └─────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│                    Command Execution Engine                          │
│         (single-threaded — no locks on data structures)             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ String   │  │ List     │  │ Hash     │  │ Set/ZSet │           │
│  │ Commands │  │ Commands │  │ Commands │  │ Commands │           │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘           │
│       └──────────────┴──────┬───────┴──────────────┘               │
├─────────────────────────────┼───────────────────────────────────────┤
│                         Data Store                                   │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │                   KeySpace (HashMap)                        │     │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐         │     │
│  │  │ String  │ │  List   │ │  Hash   │ │Set/ZSet │         │     │
│  │  │ values  │ │ (deque) │ │(hashmap)│ │(skiplist)│         │     │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘         │     │
│  ├────────────────────────────────────────────────────────────┤     │
│  │               Expiration Subsystem                         │     │
│  │  TTL HashMap  +  Active expiration timer                   │     │
│  ├────────────────────────────────────────────────────────────┤     │
│  │               Eviction Subsystem (LRU/LFU)                 │     │
│  └────────────────────────────────────────────────────────────┘     │
├─────────────────────────────────────────────────────────────────────┤
│                      Pub/Sub Subsystem                               │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Channel Registry: HashMap<ChannelName, Vec<Subscriber>>    │    │
│  │  Per-client subscription set for O(1) cleanup               │    │
│  └─────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│                     Persistence Layer                                 │
│  ┌─────────────┐           ┌─────────────┐                          │
│  │     RDB     │           │     AOF     │                          │
│  │  (snapshot) │           │  (write log)│                          │
│  └──────┬──────┘           └──────┬──────┘                          │
│         └──────────┬──────────────┘                                  │
│                    v                                                  │
│              Filesystem I/O                                          │
└──────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| Networking Layer | Accept TCP connections, read/write bytes, manage connection lifecycle | Tokio TcpListener + per-connection spawned tasks |
| RESP Parser | Parse RESP2/RESP3 wire format into structured frames, serialize responses | Zero-copy parser using `bytes::Bytes`, nom-style or hand-rolled state machine |
| Command Router | Translate parsed frames into typed command structs, dispatch to handlers | Match on command name, validate arity, delegate to command module |
| Command Handlers | Execute logic for each Redis command (GET, SET, LPUSH, etc.) | Pure functions: `fn execute(cmd: &Command, db: &mut Database) -> Response` |
| KeySpace / Database | Store all key-value data, manage type-tagged values | `HashMap<String, Entry>` where Entry contains value + metadata (TTL, access time) |
| Data Structures | Implement Redis types (String, List, Hash, Set, Sorted Set) | Hand-written for control: VecDeque for lists, HashMap for hashes, custom skip list for sorted sets |
| Expiration Subsystem | Track TTLs, remove expired keys via lazy + active strategies | Separate TTL hashmap + periodic sweep task sampling random keys |
| Eviction Subsystem | Enforce memory limits by removing keys per LRU/LFU policy | Approximated LRU (sampled, not true LRU) with per-key access metadata |
| Pub/Sub | Manage channel subscriptions, fan out published messages | Channel registry hashmap + per-client subscription tracking |
| RDB Persistence | Point-in-time snapshots of the full dataset | Fork-like background serialization (spawn blocking task), binary format |
| AOF Persistence | Append every write command for replay on restart | Write-after-log of RESP-formatted commands, configurable fsync policy |

## Recommended Project Structure

```
rust-redis/
├── Cargo.toml
├── src/
│   ├── main.rs                # Entry point: parse config, start server
│   ├── lib.rs                 # Public API re-exports
│   │
│   ├── server/                # Networking and connection management
│   │   ├── mod.rs
│   │   ├── listener.rs        # TCP listener, accept loop
│   │   ├── connection.rs      # Per-connection state, read/write frames
│   │   └── shutdown.rs        # Graceful shutdown coordination
│   │
│   ├── protocol/              # RESP wire protocol
│   │   ├── mod.rs
│   │   ├── frame.rs           # Frame enum (SimpleString, Error, Integer, Bulk, Array, Null)
│   │   ├── parser.rs          # Zero-copy RESP parser (bytes -> Frame)
│   │   └── serializer.rs      # Frame -> bytes serializer
│   │
│   ├── command/               # Command parsing and dispatch
│   │   ├── mod.rs             # Command enum, dispatch table
│   │   ├── string.rs          # GET, SET, MGET, MSET, INCR, DECR, APPEND, etc.
│   │   ├── list.rs            # LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, etc.
│   │   ├── hash.rs            # HGET, HSET, HDEL, HGETALL, etc.
│   │   ├── set.rs             # SADD, SREM, SMEMBERS, SINTER, SUNION, etc.
│   │   ├── sorted_set.rs      # ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, etc.
│   │   ├── key.rs             # DEL, EXISTS, EXPIRE, TTL, TYPE, RENAME, etc.
│   │   ├── pubsub.rs          # SUBSCRIBE, UNSUBSCRIBE, PUBLISH
│   │   ├── server_cmd.rs      # PING, INFO, CONFIG, DBSIZE, FLUSHDB, etc.
│   │   └── pipeline.rs        # Pipelining support (batch command execution)
│   │
│   ├── storage/               # Core data store
│   │   ├── mod.rs
│   │   ├── database.rs        # KeySpace: HashMap<String, Entry>, main DB struct
│   │   ├── entry.rs           # Entry type: value + TTL + access metadata
│   │   ├── value.rs           # RedisValue enum (String, List, Hash, Set, SortedSet)
│   │   ├── expiration.rs      # TTL tracking, lazy check, active sweep
│   │   └── eviction.rs        # LRU/LFU approximation, memory pressure handling
│   │
│   ├── types/                 # Redis data structure implementations
│   │   ├── mod.rs
│   │   ├── redis_string.rs    # String type (raw bytes, integer-optimized)
│   │   ├── redis_list.rs      # List (VecDeque-based)
│   │   ├── redis_hash.rs      # Hash (HashMap)
│   │   ├── redis_set.rs       # Set (HashSet)
│   │   └── redis_sorted_set.rs # Sorted Set (skip list + hashmap)
│   │
│   ├── pubsub/                # Pub/Sub subsystem
│   │   ├── mod.rs
│   │   ├── registry.rs        # Channel -> subscribers mapping
│   │   └── subscriber.rs      # Per-client subscription state
│   │
│   ├── persistence/           # RDB + AOF
│   │   ├── mod.rs
│   │   ├── rdb/
│   │   │   ├── mod.rs
│   │   │   ├── encoder.rs     # Serialize database to RDB binary format
│   │   │   └── decoder.rs     # Load RDB file on startup
│   │   └── aof/
│   │       ├── mod.rs
│   │       ├── writer.rs      # Append commands to AOF file
│   │       ├── reader.rs      # Replay AOF on startup
│   │       └── rewriter.rs    # AOF compaction / rewrite
│   │
│   ├── config/                # Server configuration
│   │   ├── mod.rs
│   │   └── config.rs          # Config struct, parsing, defaults
│   │
│   └── error.rs               # Unified error types
│
├── tests/
│   ├── integration/           # Full server integration tests
│   │   ├── string_test.rs
│   │   ├── list_test.rs
│   │   └── ...
│   └── protocol/              # RESP protocol round-trip tests
│       └── frame_test.rs
│
└── benches/                   # Performance benchmarks
    ├── parser_bench.rs
    ├── command_bench.rs
    └── throughput_bench.rs
```

### Structure Rationale

- **server/:** Isolates all TCP/networking concerns. Connection management, backpressure, and shutdown are networking problems, not database problems.
- **protocol/:** RESP parsing is a self-contained, heavily-tested module. Zero dependencies on the rest of the system. Can be extracted as a standalone crate.
- **command/:** Each Redis command family gets its own file. Commands are pure functions that take a database reference and return a response. This enables unit testing without networking.
- **storage/:** The core database state. This is the "single-threaded" domain -- all access goes through one owner. Expiration and eviction are storage-level concerns.
- **types/:** Hand-written data structures separate from storage logic. Each type is independently testable and benchmarkable.
- **persistence/:** RDB and AOF are independent persistence strategies. Kept separate from storage because they operate on snapshots/logs, not live data.

## Architectural Patterns

### Pattern 1: Single-Threaded Data Owner with Message Passing

**What:** One dedicated task owns all mutable database state. I/O tasks communicate with it via an mpsc channel, sending commands and receiving responses through oneshot channels.

**When to use:** This is the core architectural pattern for the entire server. It mirrors Redis's single-threaded model.

**Trade-offs:**
- Pro: Zero lock contention on data structures. Eliminates entire classes of bugs.
- Pro: Deterministic command ordering (important for consistency).
- Con: Data throughput bounded by single core. Acceptable for Redis-class workloads (operations are microsecond-fast).
- Con: Channel overhead per command (~50-100ns). Negligible compared to network RTT.

**Example:**
```rust
// The core event loop - single owner of all database state
async fn run_engine(mut rx: mpsc::Receiver<CommandRequest>, mut db: Database) {
    while let Some(req) = rx.recv().await {
        let response = db.execute(req.command);
        let _ = req.response_tx.send(response);
    }
}

// Each connection task sends commands through the channel
struct CommandRequest {
    command: Command,
    response_tx: oneshot::Sender<Response>,
}

// Connection handler (runs on tokio worker threads)
async fn handle_connection(
    stream: TcpStream,
    cmd_tx: mpsc::Sender<CommandRequest>,
) {
    let mut conn = Connection::new(stream);
    while let Some(frame) = conn.read_frame().await? {
        let command = Command::from_frame(frame)?;
        let (resp_tx, resp_rx) = oneshot::channel();
        cmd_tx.send(CommandRequest { command, response_tx: resp_tx }).await?;
        let response = resp_rx.await?;
        conn.write_frame(&response.to_frame()).await?;
    }
}
```

**Alternative approach (simpler, used by mini-redis):** Use `Arc<Mutex<HashMap>>` with `std::sync::Mutex`. This works when lock hold times are very short (just HashMap lookups/inserts). The single-owner channel approach is more faithful to Redis's architecture and avoids any contention under high concurrency, but the Mutex approach is simpler to implement initially. **Recommendation:** Start with `Arc<Mutex<Database>>` for Phase 1 simplicity, migrate to channel-based single-owner if benchmarks show contention.

### Pattern 2: Zero-Copy Frame Parsing

**What:** Parse RESP protocol directly from the read buffer without copying bytes. Use `bytes::Bytes` (reference-counted, shallow-clone) instead of `Vec<u8>`.

**When to use:** All protocol parsing. This is the hot path -- every single command goes through the parser.

**Trade-offs:**
- Pro: Eliminates allocation per parsed value. Significant at high throughput.
- Pro: `Bytes::slice()` is O(1), no memcpy.
- Con: Slightly more complex lifetime management than owned `String` values.
- Con: Reference counting adds small overhead per clone (~atomic increment).

**Example:**
```rust
use bytes::{Buf, Bytes, BytesMut};

#[derive(Debug, Clone)]
enum Frame {
    Simple(Bytes),       // +OK\r\n  -> Bytes pointing into original buffer
    Error(Bytes),        // -ERR msg\r\n
    Integer(i64),        // :1000\r\n
    Bulk(Bytes),         // $5\r\nhello\r\n -> Bytes slice, no copy
    Array(Vec<Frame>),   // *2\r\n...
    Null,                // $-1\r\n
}

fn parse_frame(buf: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
    // Check if complete frame is available (peek, don't consume)
    // If complete, advance buffer and return Frame with Bytes slices
    // If incomplete, return Ok(None) to signal "need more data"
}
```

### Pattern 3: Type-Tagged Value Entries

**What:** Each key maps to an `Entry` that wraps a type-tagged `RedisValue` enum plus metadata (TTL, last access time, creation time).

**When to use:** The database storage layer. Every key-value pair uses this structure.

**Trade-offs:**
- Pro: Type safety enforced at compile time. Wrong-type errors caught cleanly.
- Pro: Metadata co-located with value for cache-friendly access during expiration/eviction checks.
- Con: Enum overhead (discriminant tag + padding). Minimal in practice.

**Example:**
```rust
use std::time::Instant;

enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    SortedSet(SkipList),  // Custom implementation
}

struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
    last_accessed: Instant,    // For LRU
    access_frequency: u8,       // For LFU (Morris counter, logarithmic)
}

struct Database {
    keyspace: HashMap<String, Entry>,
    // Expiration: keys with TTL indexed by expiration time
    expiry_index: BTreeMap<Instant, Vec<String>>,
}
```

### Pattern 4: Command as Data (Functional Command Dispatch)

**What:** Parse RESP frames into strongly-typed Command enums, then dispatch through pure functions. Commands never hold references to database internals -- they describe what to do, and the engine applies them.

**When to use:** All command handling. Aligns with the project's functional coding patterns requirement.

**Trade-offs:**
- Pro: Commands are trivially testable -- construct a Command, call execute, assert on Response.
- Pro: Natural fit for AOF logging (commands are serializable).
- Pro: Pipeline support falls out naturally (just a Vec<Command>).
- Con: Enum with many variants can be unwieldy. Mitigated by grouping into sub-enums.

**Example:**
```rust
enum Command {
    // String commands
    Get { key: String },
    Set { key: String, value: Bytes, expiry: Option<Duration>, condition: SetCondition },
    // List commands
    LPush { key: String, values: Vec<Bytes> },
    LRange { key: String, start: i64, stop: i64 },
    // ... etc
}

enum SetCondition { Always, NX, XX }  // SET NX / SET XX

// Pure function: command + state -> response
fn execute(cmd: Command, db: &mut Database) -> Response {
    match cmd {
        Command::Get { key } => {
            // Lazy expiration check
            if db.is_expired(&key) {
                db.remove(&key);
                return Response::Null;
            }
            match db.get(&key) {
                Some(Entry { value: RedisValue::String(s), .. }) => Response::Bulk(s.clone()),
                Some(_) => Response::Error("WRONGTYPE".into()),
                None => Response::Null,
            }
        }
        // ...
    }
}
```

## Data Flow

### Request Flow (Command Execution)

```
Client sends: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    |
    v
[TCP Socket] -- raw bytes --> [Connection read buffer (BytesMut)]
    |
    v
[RESP Parser] -- zero-copy --> Frame::Array([Frame::Bulk("SET"), Frame::Bulk("foo"), Frame::Bulk("bar")])
    |
    v
[Command Router] -- validate & parse --> Command::Set { key: "foo", value: "bar", ... }
    |
    v
[Database.execute()] -- single-threaded --> Entry { value: String("bar"), ... } inserted
    |
    v
[Response] -- serialized --> Frame::Simple("+OK")
    |
    v
[Connection write buffer] -- flush --> Client receives: +OK\r\n
```

### Pipelining Flow

```
Client sends N commands without waiting for responses:
    |
    v
[Connection] reads all available bytes from socket
    |
    v
[Parser] extracts frames one by one from buffer
    |
    v
[Command Router] produces Vec<Command>
    |
    v
[Database] executes each command sequentially, collects Vec<Response>
    |
    v
[Connection] writes all responses back in order
    (single write syscall for entire batch)
```

### Expiration Flow (Dual Strategy)

```
LAZY EXPIRATION (on access):
  Client requests GET key
      |
      v
  [Database.get(key)] -> check entry.expires_at
      |                        |
      |                   expires_at < now?
      |                   /           \
      |                 YES            NO
      |                  |              |
      |            delete key      return value
      |            return Null
      v

ACTIVE EXPIRATION (periodic sweep):
  [Timer fires every 100ms]
      |
      v
  Sample 20 random keys with TTLs
      |
      v
  Delete expired ones
      |
      v
  If >25% were expired, repeat immediately
  Otherwise, wait for next timer tick
```

### Persistence Flow

```
RDB SNAPSHOT:
  [Trigger: timer or manual BGSAVE]
      |
      v
  [Spawn blocking task]
      |
      v
  [Serialize entire keyspace to temp file]  <-- reads consistent snapshot
      |
      v
  [Atomic rename temp -> dump.rdb]


AOF APPEND:
  [Every write command after execution]
      |
      v
  [Serialize command as RESP to AOF buffer]
      |
      v
  [Flush to file per fsync policy]
      |--- always: fsync after every command
      |--- everysec: fsync every second (default, recommended)
      |--- no: let OS decide

AOF REWRITE (compaction):
  [Trigger: AOF file too large]
      |
      v
  [Spawn blocking task]
      |
      v
  [Walk keyspace, write minimal command set to new AOF]
      |
      v
  [Append any commands received during rewrite]
      |
      v
  [Atomic swap old AOF -> new AOF]
```

### Pub/Sub Flow

```
SUBSCRIBE:
  Client sends SUBSCRIBE channel1
      |
      v
  [Registry] adds client's response_tx to channel1's subscriber list
  [Client state] marks connection as "subscriber mode"

PUBLISH:
  Another client sends PUBLISH channel1 "hello"
      |
      v
  [Registry] looks up channel1 -> Vec<subscriber_tx>
      |
      v
  For each subscriber_tx: send message frame
      |
      v
  Each subscriber's connection task writes to their TCP socket

UNSUBSCRIBE / DISCONNECT:
  [Client state] holds Set<ChannelName>
      |
      v
  For each channel: remove from registry's subscriber list
  (O(subscribers_on_channel) per channel)
```

### Key Data Flows

1. **Hot path (command execution):** TCP read -> parse -> execute -> serialize -> TCP write. Must be allocation-minimal. Target: sub-microsecond for simple commands excluding network.
2. **Warm path (expiration):** Periodic timer -> random sample -> delete expired. Runs on the same single thread as command execution to avoid synchronization.
3. **Cold path (persistence):** Background task reads a snapshot of state, writes to disk. Does not block command execution.

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| 0-10k concurrent connections | Single tokio runtime with default thread pool handles this easily. `Arc<Mutex<Database>>` or channel-based single owner both work fine. |
| 10k-100k connections | Ensure connection tasks are lightweight (minimal per-connection state). Use bounded channels for backpressure. Consider SO_REUSEPORT for multiple accept tasks. |
| 100k+ connections | I/O thread pool becomes the bottleneck before data throughput. Profile and tune tokio worker count. For data throughput beyond single-core, would need sharding (out of scope for v1). |

### Scaling Priorities

1. **First bottleneck: Network I/O and parsing.** The RESP parser and TCP read/write are the hottest paths. Zero-copy parsing and write batching (pipelining) are the highest-leverage optimizations.
2. **Second bottleneck: Memory allocator pressure.** High-throughput SET operations create allocation churn. Arena allocation for short-lived parse buffers and `Bytes` for zero-copy value storage mitigate this.
3. **Third bottleneck: Single-threaded data operations.** Only matters at extreme throughput (millions of ops/sec). Redis itself hits this ceiling around 100k-300k ops/sec on simple commands. Acceptable for v1.

## Anti-Patterns

### Anti-Pattern 1: Locking Data Structures Per-Operation with Tokio Mutex

**What people do:** Use `tokio::sync::Mutex` for the database, holding it across await points.
**Why it's wrong:** Tokio's async mutex is designed for locks held across `.await`. For a HashMap lookup/insert (sub-microsecond, no await), it adds unnecessary overhead. Worse, it can cause task starvation under high concurrency.
**Do this instead:** Use `std::sync::Mutex` for short critical sections (no `.await` while holding the lock), or the channel-based single-owner pattern for zero contention.

### Anti-Pattern 2: Cloning Values on Every Read

**What people do:** Store `String` / `Vec<u8>` values and `.clone()` on every GET response.
**Why it's wrong:** GET is the most common Redis operation. Cloning a 1KB value millions of times per second wastes CPU and memory bandwidth.
**Do this instead:** Store values as `bytes::Bytes`. Cloning `Bytes` is a cheap reference count increment (atomic add), not a memcpy.

### Anti-Pattern 3: True LRU with Doubly-Linked List

**What people do:** Implement textbook LRU with a doubly-linked list + HashMap for O(1) operations.
**Why it's wrong:** The linked list destroys cache locality. Every access touches scattered heap memory. Redis deliberately avoids this.
**Do this instead:** Approximated LRU: store a last-access timestamp per key, sample N random keys when evicting, evict the least-recently-used from the sample. Tunable accuracy via sample size (Redis default: 5).

### Anti-Pattern 4: Blocking the Event Loop with Persistence

**What people do:** Serialize the database to disk synchronously in the main data loop.
**Why it's wrong:** RDB serialization of a large dataset can take seconds. All client commands stall during this time.
**Do this instead:** Use `tokio::task::spawn_blocking` for persistence operations. For RDB, take a consistent snapshot (clone or COW-style) then serialize in background. For AOF, buffer writes and flush asynchronously.

### Anti-Pattern 5: One Allocation Per Parsed Frame Element

**What people do:** Parse each RESP bulk string into a new `String` allocation.
**Why it's wrong:** A typical command has 3-5 elements. At 100k commands/sec, that is 300k-500k allocations/sec just for parsing. Allocator becomes the bottleneck.
**Do this instead:** Parse into `Bytes` slices that reference the original read buffer. Zero allocations for the common case.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Filesystem (RDB/AOF) | `tokio::fs` for async file I/O, `spawn_blocking` for heavy serialization | Use atomic rename for crash safety. Never write directly to the live RDB/AOF file. |
| OS signals (SIGINT/SIGTERM) | `tokio::signal` | Trigger graceful shutdown: stop accepting, drain connections, flush AOF, save RDB |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| IO tasks <-> Database engine | mpsc channel (or Mutex) | Command requests flow in, responses flow back via oneshot. This is the central architectural boundary. |
| Database engine <-> Persistence | Snapshot/log interface | RDB: engine provides read-only snapshot. AOF: engine feeds write commands to log buffer. Neither blocks the engine. |
| Database engine <-> Expiration | Inline (same thread) | Active expiration runs as a periodic task within the engine's event loop, not as a separate thread/task. |
| Database engine <-> Pub/Sub | Inline + fan-out | PUBLISH executes inline, but message delivery fans out to subscriber connection tasks via their write channels. |
| Connection <-> Parser | Direct function call | Parser is a pure function: `&mut BytesMut -> Result<Option<Frame>>`. No async, no state beyond the buffer. |

## Build Order (Dependency Graph)

The architecture implies a clear bottom-up build order:

```
Phase 1: Foundation
  protocol/frame.rs + protocol/parser.rs + protocol/serializer.rs
  (No dependencies. Independently testable. Most critical to get right.)
      |
      v
Phase 2: Storage Core
  types/* + storage/entry.rs + storage/value.rs + storage/database.rs
  (Depends on nothing. Pure data structures. Heavily unit-testable.)
      |
      v
Phase 3: Command Engine
  command/* (all command families)
  (Depends on: protocol for Frame types, storage for Database)
  (Pure functions: Command + Database -> Response)
      |
      v
Phase 4: Networking
  server/listener.rs + server/connection.rs + server/shutdown.rs
  (Depends on: protocol for read/write frames, command for dispatch)
  (This is where it becomes a running server)
      |
      v
Phase 5: Expiration + Eviction
  storage/expiration.rs + storage/eviction.rs
  (Depends on: storage core. Modifies Database internals.)
  (Can be added after server works for basic operations)
      |
      v
Phase 6: Persistence
  persistence/rdb/* + persistence/aof/*
  (Depends on: storage for serialization, server for trigger hooks)
  (Most complex subsystem. Benefits from stable storage layer.)
      |
      v
Phase 7: Pub/Sub
  pubsub/*
  (Depends on: server for connection management, protocol for framing)
  (Somewhat independent. Can be built in parallel with persistence.)
      |
      v
Phase 8: Pipelining + Polish
  command/pipeline.rs + config/* + benchmarks
  (Depends on: everything working. Optimization layer.)
```

**Key dependency insight:** The protocol layer and storage layer have ZERO dependencies on each other. They can be built and tested completely independently, then composed through the command layer. This is the critical architectural boundary to maintain.

## Sources

- [Redis Deep Dive: In-Memory Architecture and Event Loop](https://thuva4.com/blog/part-1-the-core-of-redis/)
- [Redis Single-Threaded I/O Model](https://oneuptime.com/blog/post/2026-01-25-redis-single-threaded-io-model/view)
- [The Engineering Wisdom Behind Redis's Single-Threaded Design](https://riferrei.com/the-engineering-wisdom-behind-rediss-single-threaded-design/)
- [Redis Persistence Official Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Key Eviction Official Docs](https://redis.io/docs/latest/develop/reference/eviction/)
- [Redis Pub/Sub Official Docs](https://redis.io/docs/latest/develop/pubsub/)
- [Redis Pub/Sub Under the Hood](https://jameshfisher.com/2017/03/01/redis-pubsub-under-the-hood/)
- [Tokio Mini-Redis (reference implementation)](https://github.com/tokio-rs/mini-redis)
- [Tokio Shared State Tutorial](https://tokio.rs/tokio/tutorial/shared-state)
- [redis-protocol.rs (RESP2/RESP3 Rust implementation)](https://github.com/aembke/redis-protocol.rs)
- [Implementing a Copyless Redis Protocol in Rust](https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators/)
- [Redis LRU/LFU Eviction Configuration](https://oneuptime.com/blog/post/2026-01-25-redis-lru-lfu-eviction/view)
- [bumpalo - Arena Allocator for Rust](https://github.com/fitzgen/bumpalo)
- [Concurrent Servers Part 5: Redis Case Study](https://eli.thegreenplace.net/2017/concurrent-servers-part-5-redis-case-study/)

---
*Architecture research for: Redis-compatible in-memory key-value store in Rust*
*Researched: 2026-03-23*
