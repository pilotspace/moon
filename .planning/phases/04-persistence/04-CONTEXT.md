# Phase 4: Persistence - Context

**Gathered:** 2026-03-23
**Status:** Ready for planning

<domain>
## Phase Boundary

Data survives server restarts through RDB snapshots and AOF append-only logging. Users can trigger BGSAVE for manual snapshots and BGREWRITEAOF for AOF compaction. Server restores from persistence files on startup. Configurable fsync policies trade durability for performance.

</domain>

<decisions>
## Implementation Decisions

### RDB Snapshot Strategy
- Clone-on-snapshot: Arc-wrap database values, clone the key index, serialize in `spawn_blocking` — values are reference-counted so cloning is cheap
- No fork() — incompatible with tokio async runtime (per research Pitfall #4)
- BGSAVE does not block writes — snapshot reads a consistent clone while live database continues accepting commands
- Simplified custom binary format (NOT Redis-compatible RDB):
  - Header: magic bytes `RUSTREDIS` + format version u8
  - Per-key: type tag u8, key length u32 + key bytes, TTL (0 = none, or unix timestamp millis i64), value encoding per type
  - String: length u32 + bytes
  - Hash: field count u32 + (field_len u32 + field + value_len u32 + value) repeated
  - List: element count u32 + (elem_len u32 + elem) repeated
  - Set: member count u32 + (member_len u32 + member) repeated
  - SortedSet: member count u32 + (member_len u32 + member + score f64) repeated
  - Footer: EOF marker byte + CRC32 checksum
- File: `dump.rdb` in configurable directory

### AOF Logging Design
- Log raw RESP command bytes as received — replay is re-parsing and re-executing
- Write-after-log: command executes first, then appended to AOF (matches Redis)
- Three fsync policies: `always` (fsync every command), `everysec` (background fsync every second), `no` (OS decides) — default `everysec`
- AOF writer runs as a separate tokio task receiving commands via mpsc channel
- BGREWRITEAOF: generate new AOF by serializing current database state as synthetic SET/HSET/LPUSH/SADD/ZADD commands, then atomically replace old AOF
- File: `appendonly.aof` in configurable directory

### Startup Restore Behavior
- On startup: check for AOF first, then RDB — AOF takes priority (matches Redis)
- If persistence file exists, replay/load before accepting client connections
- Corrupt files: log error with details, start with empty database — don't crash
- Log restoration statistics (keys loaded, time taken) via tracing

### Persistence Configuration
- CLI flags matching Redis conventions:
  - `--appendonly yes/no` (default: no)
  - `--appendfsync always/everysec/no` (default: everysec)
  - `--save "900 1 300 10"` (RDB auto-save rules: save if N changes in M seconds)
  - `--dir ./data` (persistence directory, default: current directory)
  - `--dbfilename dump.rdb` (RDB filename)
  - `--appendfilename appendonly.aof` (AOF filename)
- Add all to ServerConfig with clap derive

### Claude's Discretion
- Exact Arc-wrapping strategy for clone-on-snapshot (Arc per Entry vs Arc per RedisValue)
- AOF channel buffer size
- CRC32 implementation choice (crc32fast crate or manual)
- RDB auto-save timer implementation details
- Whether BGREWRITEAOF blocks during the snapshot phase
- Exact error recovery behavior for partially corrupt files

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing Server Code
- `src/storage/entry.rs` — RedisValue enum (5 variants), Entry struct with TTL
- `src/storage/db.rs` — Database with HashMap<Bytes, Entry>, all get/set/remove methods
- `src/server/listener.rs` — Server startup, task spawning pattern (see expiration task)
- `src/server/expiration.rs` — Background task pattern with CancellationToken + Arc<Mutex>
- `src/config.rs` — ServerConfig with clap derive CLI args
- `src/command/mod.rs` — Dispatch for BGSAVE/BGREWRITEAOF commands
- `src/protocol/frame.rs` — Frame type for AOF command serialization
- `src/protocol/serialize.rs` — serialize() for writing RESP commands to AOF

### Project Research
- `.planning/research/PITFALLS.md` — Pitfall #4 (fork-based snapshots), Pitfall #8 (AOF rewrite complexity)
- `.planning/research/ARCHITECTURE.md` — Persistence layer design, background tasks via spawn_blocking
- `.planning/research/STACK.md` — crc crate for checksums

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `protocol::serialize()` — can serialize Frame to BytesMut for AOF command logging
- `protocol::parse()` — can parse AOF file contents during replay
- `server::expiration` — pattern for background tasks with CancellationToken + shared state
- `Arc<Mutex<Vec<Database>>>` — shared state pattern already established
- `ServerConfig` with clap derive — extend with persistence flags
- `Entry::expires_at` as `Option<Instant>` — need to convert to/from unix timestamps for persistence

### Established Patterns
- Background tasks spawned in `listener.rs` alongside TCP accept loop
- `CancellationToken` for graceful shutdown of background tasks
- `spawn_blocking` for CPU-intensive work off the event loop
- tracing for structured logging

### Integration Points
- `listener.rs` — spawn AOF writer task, load persistence on startup before accepting connections
- `connection.rs` — after command execution, send command bytes to AOF writer channel
- `config.rs` — add persistence configuration fields
- `command/mod.rs` — add BGSAVE and BGREWRITEAOF dispatch

</code_context>

<specifics>
## Specific Ideas

- AOF replay should reuse the exact same command dispatch path as live connections — ensures perfect consistency
- RDB save/load should be testable in isolation (serialize database, deserialize, compare)
- BGSAVE should return "Background saving started" immediately, not wait for completion
- Auto-save rules (e.g., "save after 900 seconds if at least 1 key changed") need a change counter in Database

</specifics>

<deferred>
## Deferred Ideas

- Redis-compatible RDB format — too complex for v1, custom format is sufficient
- AOF pipelining (batch fsync for multiple commands) — optimization
- Mixed RDB+AOF persistence (Redis 7 style) — v2
- LASTSAVE command — minor, add if time permits

</deferred>

---

*Phase: 04-persistence*
*Context gathered: 2026-03-23*
