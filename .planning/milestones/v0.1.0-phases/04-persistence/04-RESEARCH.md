# Phase 4: Persistence - Research

**Researched:** 2026-03-23
**Domain:** RDB snapshots, AOF logging, startup restoration, binary serialization
**Confidence:** HIGH

## Summary

Phase 4 adds data persistence through two complementary mechanisms: RDB point-in-time snapshots and AOF append-only logging. The project already has a well-defined architecture from the CONTEXT.md decisions: clone-on-snapshot for RDB (no fork), RESP-format AOF with mpsc channel writer, and configurable fsync policies.

The existing codebase provides strong foundations. The `protocol::serialize()` and `protocol::parse()` functions handle RESP wire format and can be reused directly for AOF. The `server::expiration` module establishes the background task pattern with `CancellationToken`. The `Database` struct exposes `data()` for read-only iteration needed by both RDB serialization and AOF rewrite.

**Primary recommendation:** Implement in order: (1) persistence config, (2) RDB save/load with BGSAVE command, (3) AOF writer/replay with BGREWRITEAOF, (4) startup restore logic, (5) auto-save timer. Each piece is independently testable.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Clone-on-snapshot: Arc-wrap database values, clone the key index, serialize in `spawn_blocking` -- values are reference-counted so cloning is cheap
- No fork() -- incompatible with tokio async runtime
- BGSAVE does not block writes -- snapshot reads a consistent clone while live database continues accepting commands
- Simplified custom binary format (NOT Redis-compatible RDB): Header magic `RUSTREDIS` + version u8, per-key type tag, key/value encoding, footer EOF marker + CRC32
- AOF logs raw RESP command bytes -- replay is re-parsing and re-executing
- Write-after-log: command executes first, then appended to AOF
- Three fsync policies: always/everysec/no, default everysec
- AOF writer runs as separate tokio task receiving commands via mpsc channel
- BGREWRITEAOF: generate new AOF by serializing current state as synthetic commands, atomically replace old AOF
- On startup: AOF takes priority over RDB (matches Redis behavior)
- Corrupt files: log error, start with empty database -- don't crash
- CLI flags: --appendonly, --appendfsync, --save, --dir, --dbfilename, --appendfilename

### Claude's Discretion
- Exact Arc-wrapping strategy for clone-on-snapshot (Arc per Entry vs Arc per RedisValue)
- AOF channel buffer size
- CRC32 implementation choice (crc32fast crate or manual)
- RDB auto-save timer implementation details
- Whether BGREWRITEAOF blocks during the snapshot phase
- Exact error recovery behavior for partially corrupt files

### Deferred Ideas (OUT OF SCOPE)
- Redis-compatible RDB format -- too complex for v1, custom format is sufficient
- AOF pipelining (batch fsync for multiple commands) -- optimization
- Mixed RDB+AOF persistence (Redis 7 style) -- v2
- LASTSAVE command -- minor, add if time permits
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PERS-01 | Server supports RDB persistence (point-in-time snapshots to disk) | Custom binary format with CRC32. Clone-on-snapshot via `spawn_blocking`. `Database::data()` provides read-only access for serialization. |
| PERS-02 | Server supports AOF persistence (append-only file with configurable fsync) | RESP-format log via mpsc channel to writer task. `protocol::serialize()` already writes RESP to BytesMut. Three fsync policies configurable via clap. |
| PERS-03 | Server can restore data from RDB or AOF on startup | AOF priority over RDB. `protocol::parse()` replays AOF. RDB loads custom binary format. Load in `listener.rs` before accept loop. |
| PERS-04 | User can trigger BGSAVE for manual RDB snapshot | New command in dispatch. Returns "Background saving started" immediately. Spawns `spawn_blocking` task. |
| PERS-05 | User can trigger BGREWRITEAOF for AOF compaction | Snapshot current state, generate synthetic SET/HSET/LPUSH/SADD/ZADD commands, write to temp file, atomic rename. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| crc32fast | 1.5.0 | CRC32 checksum for RDB footer | SIMD-accelerated, single purpose, zero-config. Standard choice for CRC32 in Rust. |
| tokio (existing) | 1.x | spawn_blocking for snapshot, mpsc channel for AOF, fs operations | Already in deps. Provides `tokio::sync::mpsc`, `tokio::task::spawn_blocking`, `tokio::fs`. |
| bytes (existing) | 1.10 | Buffer management for serialization | Already in deps. BytesMut for building RDB/AOF buffers. |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| clap (existing) | 4.x | CLI args for persistence config | Extend existing ServerConfig derive struct. |
| tracing (existing) | 0.1 | Structured logging for persistence events | Log save/load stats, errors, timing. |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| crc32fast | Manual CRC32 | No reason to hand-roll; crc32fast is 1 dependency, SIMD-optimized |
| tokio::sync::mpsc | crossbeam-channel | tokio mpsc integrates with async; crossbeam would need bridging |
| Custom binary RDB | serde + bincode | Overkill for a fixed schema; custom format is simpler and matches Redis mental model |

**Installation:**
```bash
cargo add crc32fast
```

## Architecture Patterns

### Recommended Project Structure
```
src/
├── persistence/
│   ├── mod.rs           # Module exports, PersistenceConfig struct
│   ├── rdb.rs           # RDB save/load (binary format)
│   ├── aof.rs           # AOF writer task, replay, rewrite
│   └── auto_save.rs     # Auto-save timer (background task)
├── storage/
│   ├── db.rs            # Add iter_entries() for snapshot, change counter
│   └── entry.rs         # No changes needed (already Clone)
├── server/
│   └── listener.rs      # Add persistence startup, spawn AOF writer
├── command/
│   └── persistence.rs   # BGSAVE, BGREWRITEAOF handlers
└── config.rs            # Extend with persistence fields
```

### Pattern 1: Clone-on-Snapshot for RDB
**What:** Clone the entire database state under the lock, then serialize outside the lock in `spawn_blocking`.
**When to use:** BGSAVE and auto-save triggers.
**Recommendation:** Arc per Entry is simpler than Arc per RedisValue. Since Entry is Clone and contains `Bytes` (which is already reference-counted via Arc internally), cloning is cheap for strings. For collections, the HashMap/VecDeque/HashSet/BTreeMap will deep-clone, but this matches Redis's copy-on-write fork behavior -- the clone cost is the price of consistency without fork().
**Example:**
```rust
// Snapshot the database: clone under lock, serialize outside lock
async fn save_rdb(
    db: Arc<Mutex<Vec<Database>>>,
    db_index: usize,  // or iterate all
    path: PathBuf,
) -> anyhow::Result<()> {
    // Clone under lock -- fast for Bytes-heavy data
    let snapshot: Vec<(Bytes, Entry)> = {
        let dbs = db.lock().unwrap();
        dbs.iter().enumerate().flat_map(|(idx, db)| {
            db.data().iter().map(move |(k, v)| (k.clone(), v.clone()))
        }).collect()
    };

    // Serialize off the event loop
    tokio::task::spawn_blocking(move || {
        let data = rdb::serialize_snapshot(&snapshot)?;
        // Write to temp file, then rename for atomicity
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }).await?
}
```

### Pattern 2: AOF Writer as Separate Task with mpsc
**What:** A dedicated tokio task that receives serialized RESP command bytes via an mpsc channel and appends them to the AOF file.
**When to use:** Every write command when AOF is enabled.
**Example:**
```rust
use tokio::sync::mpsc;
use tokio::io::AsyncWriteExt;

enum AofMessage {
    Append(Bytes),     // Serialized RESP command
    Rewrite,           // Trigger BGREWRITEAOF
    Shutdown,          // Graceful stop
}

async fn aof_writer_task(
    mut rx: mpsc::Receiver<AofMessage>,
    config: AofConfig,
    db: Arc<Mutex<Vec<Database>>>,
) {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true).append(true)
        .open(&config.path).await.unwrap();

    let mut last_fsync = Instant::now();

    while let Some(msg) = rx.recv().await {
        match msg {
            AofMessage::Append(data) => {
                file.write_all(&data).await.unwrap();
                match config.fsync_policy {
                    FsyncPolicy::Always => { file.sync_data().await.unwrap(); }
                    FsyncPolicy::EverySec => {
                        if last_fsync.elapsed() >= Duration::from_secs(1) {
                            file.sync_data().await.unwrap();
                            last_fsync = Instant::now();
                        }
                    }
                    FsyncPolicy::No => { /* OS decides */ }
                }
            }
            AofMessage::Rewrite => { /* trigger rewrite */ }
            AofMessage::Shutdown => break,
        }
    }
}
```

### Pattern 3: AOF Replay Using Existing Parser/Dispatch
**What:** Read AOF file, parse RESP frames using existing `protocol::parse()`, dispatch through `command::dispatch()`.
**When to use:** Startup restoration from AOF.
**Example:**
```rust
fn replay_aof(path: &Path, databases: &mut Vec<Database>) -> anyhow::Result<usize> {
    let data = std::fs::read(path)?;
    let mut buf = BytesMut::from(&data[..]);
    let config = ParseConfig::default();
    let mut count = 0;
    let mut selected_db = 0usize;

    while let Some(frame) = parse::parse(&mut buf, &config)? {
        let db_count = databases.len();
        dispatch(&mut databases[selected_db], frame, &mut selected_db, db_count);
        count += 1;
    }
    Ok(count)
}
```

### Pattern 4: Atomic File Writes
**What:** Write to a temp file, then rename to target path. Prevents corruption from interrupted writes.
**When to use:** RDB save and AOF rewrite.
**Example:**
```rust
// Write to temp, rename atomically
let tmp_path = path.with_extension("tmp");
std::fs::write(&tmp_path, &data)?;
std::fs::rename(&tmp_path, &path)?; // atomic on POSIX
```

### Pattern 5: Integrating AOF into Connection Loop
**What:** After command dispatch, if AOF is enabled and command is a write, serialize the original command frame and send to AOF channel.
**When to use:** Every write command in the connection handler.
**Key detail:** The AOF sender (`mpsc::Sender`) needs to be passed into the connection handler. Only write commands should be logged -- need a way to identify them (either a list of write commands, or a flag on DispatchResult).
**Example:**
```rust
// In connection.rs, after dispatch
if let Some(ref aof_tx) = aof_sender {
    if is_write_command(&cmd_name) {
        let mut buf = BytesMut::new();
        serialize(&original_frame, &mut buf);
        let _ = aof_tx.send(AofMessage::Append(buf.freeze())).await;
    }
}
```

### Anti-Patterns to Avoid
- **fork() for snapshots:** Incompatible with tokio. The async runtime's thread pool and IO driver cannot survive fork. Use clone-on-snapshot instead.
- **Holding mutex during file I/O:** Clone data under lock, release lock, then do I/O. Never hold the database lock while writing to disk.
- **Non-atomic file writes:** Always write to a temp file first, then rename. A crash during write would corrupt the persistence file otherwise.
- **Logging read commands to AOF:** Only log write commands. Logging reads wastes space and slows replay.
- **Using tokio::fs for RDB save:** Use `spawn_blocking` + `std::fs` instead. RDB serialization is CPU-bound work that would block the async runtime if done in an async context. `tokio::fs` is fine for the AOF append path since those are small writes.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CRC32 checksums | Manual bit-manipulation CRC | `crc32fast` crate | SIMD-accelerated, tested, one-liner API |
| RESP serialization | Custom AOF format | Existing `protocol::serialize()` | Already tested, round-trips with `parse()`, matches Redis AOF format |
| RESP parsing for replay | Custom AOF parser | Existing `protocol::parse()` | Handles all edge cases, already battle-tested |
| Async channel | Manual locking queue | `tokio::sync::mpsc` | Built for async, backpressure, proper wakeups |
| File atomicity | Manual fsync dance | Write-to-temp + rename | POSIX rename is atomic; no partial corruption possible |

**Key insight:** The existing protocol layer (serialize + parse) is the AOF format. RESP is already a self-describing, parseable, appendable format. This eliminates the need for any custom AOF format design.

## Common Pitfalls

### Pitfall 1: Instant vs SystemTime for TTL Persistence
**What goes wrong:** `Entry::expires_at` uses `std::time::Instant`, which is monotonic and process-local. It cannot be serialized to disk and loaded in a different process.
**Why it happens:** Instant is correct for runtime expiry checks but meaningless across restarts.
**How to avoid:** When serializing, convert `Instant` to a Unix timestamp (milliseconds since epoch). On load, convert back. Formula: `SystemTime::now() + (expires_at - Instant::now())` for future instants. Expired entries can be skipped during save.
**Warning signs:** TTLs that are wildly wrong after restore, or all keys expiring immediately.

### Pitfall 2: Holding Database Lock During I/O
**What goes wrong:** RDB save or AOF write holds the Mutex while doing disk I/O, blocking all client connections.
**Why it happens:** Natural to iterate the database and write simultaneously.
**How to avoid:** Clone data under the lock (fast -- Bytes is Arc-backed), release lock, then serialize/write. For AOF, use mpsc channel so the connection handler just sends a message and moves on.
**Warning signs:** High latency during BGSAVE, client timeouts.

### Pitfall 3: Non-Atomic File Replacement
**What goes wrong:** Server crashes mid-write, leaving a half-written or zero-length persistence file.
**Why it happens:** Writing directly to the target file.
**How to avoid:** Always write to a `.tmp` file, then `std::fs::rename()` (atomic on POSIX).
**Warning signs:** Corrupt files after crash, zero-length dump files.

### Pitfall 4: AOF Grows Without Bound
**What goes wrong:** AOF file grows forever because every write appends commands. A key SET 1000 times means 1000 entries in AOF.
**Why it happens:** Append-only by nature.
**How to avoid:** BGREWRITEAOF compacts by generating a minimal set of commands from current state. Could also add size-based auto-rewrite triggers (deferred for now).
**Warning signs:** AOF file size >> actual data size.

### Pitfall 5: Write Command Identification for AOF
**What goes wrong:** Logging read commands to AOF, or missing some write commands.
**Why it happens:** No central registry of which commands modify state.
**How to avoid:** Maintain an explicit list of write command names. Alternatively, return a "mutated" flag from dispatch. The explicit list is simpler and less error-prone:
```rust
const WRITE_COMMANDS: &[&[u8]] = &[
    b"SET", b"MSET", b"SETNX", b"SETEX", b"PSETEX",
    b"GETSET", b"GETDEL", b"GETEX",
    b"INCR", b"DECR", b"INCRBY", b"DECRBY", b"INCRBYFLOAT",
    b"APPEND", b"DEL", b"UNLINK",
    b"EXPIRE", b"PEXPIRE", b"PERSIST",
    b"RENAME", b"RENAMENX",
    b"HSET", b"HMSET", b"HDEL", b"HSETNX", b"HINCRBY", b"HINCRBYFLOAT",
    b"LPUSH", b"RPUSH", b"LPOP", b"RPOP", b"LSET", b"LINSERT", b"LREM", b"LTRIM",
    b"SADD", b"SREM", b"SPOP",
    b"SINTERSTORE", b"SUNIONSTORE", b"SDIFFSTORE",
    b"ZADD", b"ZREM", b"ZINCRBY", b"ZPOPMIN", b"ZPOPMAX",
    b"ZUNIONSTORE", b"ZINTERSTORE",
];
```

### Pitfall 6: SELECT in AOF Replay
**What goes wrong:** Multi-database AOF replay doesn't track which database a command targets.
**Why it happens:** AOF logs commands but not the database index.
**How to avoid:** Log SELECT commands to AOF when the connection switches databases. During replay, track `selected_db` state (already done -- dispatch takes `&mut selected_db`). Alternatively, if auto-save only saves one database at a time, prefix with SELECT.
**Warning signs:** Data restored to wrong database index.

### Pitfall 7: RDB Save During Auto-Save Overlap
**What goes wrong:** Multiple concurrent BGSAVE operations (manual + auto) writing to same file.
**Why it happens:** Auto-save timer fires while a manual BGSAVE is in progress.
**How to avoid:** Track "save in progress" state with an `AtomicBool` or similar flag. Skip auto-save if already saving. BGSAVE returns error if save already in progress.
**Warning signs:** Corrupt RDB files, temp file conflicts.

## Code Examples

### RDB Binary Format Serialization
```rust
use bytes::{BufMut, BytesMut};
use crc32fast::Hasher;

const RDB_MAGIC: &[u8] = b"RUSTREDIS";
const RDB_VERSION: u8 = 1;
const TYPE_STRING: u8 = 0;
const TYPE_HASH: u8 = 1;
const TYPE_LIST: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_SORTED_SET: u8 = 4;
const EOF_MARKER: u8 = 0xFF;

fn serialize_rdb(entries: &[(Bytes, Entry)]) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // Header
    buf.put_slice(RDB_MAGIC);
    buf.put_u8(RDB_VERSION);

    for (key, entry) in entries {
        // Skip expired entries
        if let Some(exp) = entry.expires_at {
            if Instant::now() >= exp {
                continue;
            }
        }

        // Type tag
        let type_tag = match &entry.value {
            RedisValue::String(_) => TYPE_STRING,
            RedisValue::Hash(_) => TYPE_HASH,
            RedisValue::List(_) => TYPE_LIST,
            RedisValue::Set(_) => TYPE_SET,
            RedisValue::SortedSet { .. } => TYPE_SORTED_SET,
        };
        buf.put_u8(type_tag);

        // Key
        buf.put_u32(key.len() as u32);
        buf.put_slice(key);

        // TTL: 0 = no expiry, otherwise unix timestamp millis
        match entry.expires_at {
            None => buf.put_i64(0),
            Some(exp) => {
                let remaining = exp.duration_since(Instant::now());
                let unix_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH).unwrap()
                    .as_millis() as i64 + remaining.as_millis() as i64;
                buf.put_i64(unix_ms);
            }
        }

        // Value encoding (per type)
        serialize_value(&entry.value, &mut buf);
    }

    // Footer
    buf.put_u8(EOF_MARKER);

    // CRC32 of everything so far
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let checksum = hasher.finalize();
    buf.put_u32(checksum);

    buf.to_vec()
}
```

### AOF BGREWRITEAOF Synthetic Commands
```rust
/// Generate synthetic RESP commands to recreate the current database state.
fn generate_rewrite_commands(
    entries: &[(Bytes, Entry)],
) -> BytesMut {
    let mut buf = BytesMut::new();

    for (key, entry) in entries {
        if let Some(exp) = entry.expires_at {
            if Instant::now() >= exp { continue; }
        }

        match &entry.value {
            RedisValue::String(val) => {
                // *3\r\n$3\r\nSET\r\n$K\r\n<key>\r\n$V\r\n<val>\r\n
                let cmd = Frame::Array(vec![
                    Frame::BulkString(Bytes::from_static(b"SET")),
                    Frame::BulkString(key.clone()),
                    Frame::BulkString(val.clone()),
                ]);
                serialize(&cmd, &mut buf);
            }
            RedisValue::Hash(map) => {
                // HSET key f1 v1 f2 v2 ...
                let mut args = vec![
                    Frame::BulkString(Bytes::from_static(b"HSET")),
                    Frame::BulkString(key.clone()),
                ];
                for (field, val) in map {
                    args.push(Frame::BulkString(field.clone()));
                    args.push(Frame::BulkString(val.clone()));
                }
                serialize(&Frame::Array(args), &mut buf);
            }
            // Similar for List (RPUSH), Set (SADD), SortedSet (ZADD)
            _ => { /* implement similarly */ }
        }

        // Add PEXPIREAT if TTL exists
        if let Some(exp) = entry.expires_at {
            let remaining = exp.duration_since(Instant::now());
            let unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap()
                .as_millis() as i64 + remaining.as_millis() as i64;
            let cmd = Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"PEXPIREAT")),
                Frame::BulkString(key.clone()),
                Frame::BulkString(Bytes::from(unix_ms.to_string())),
            ]);
            serialize(&cmd, &mut buf);
        }
    }

    buf
}
```

### ServerConfig Extension
```rust
#[derive(Parser, Debug, Clone)]
pub struct ServerConfig {
    // ... existing fields ...

    /// Enable AOF persistence
    #[arg(long, default_value = "no")]
    pub appendonly: String,  // "yes" or "no"

    /// AOF fsync policy
    #[arg(long, default_value = "everysec")]
    pub appendfsync: String,  // "always", "everysec", "no"

    /// RDB auto-save rules (e.g., "900 1 300 10")
    #[arg(long)]
    pub save: Option<String>,

    /// Directory for persistence files
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// RDB filename
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// AOF filename
    #[arg(long, default_value = "appendonly.aof")]
    pub appendfilename: String,
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| fork() for snapshots | Clone-on-snapshot in async context | Modern async Rust | No fork compatibility issues with tokio; slightly higher memory during snapshot |
| Redis RDB format v10 | Custom binary format | Project decision | Simpler implementation; not compatible with redis-cli but sufficient for self-restore |
| Sync file I/O in main thread | spawn_blocking + async channels | Tokio best practice | Non-blocking persistence; server stays responsive |

## Open Questions

1. **PEXPIREAT command for AOF rewrite TTLs**
   - What we know: BGREWRITEAOF needs to preserve TTLs. Using PEXPIREAT (absolute timestamp) is how Redis does it.
   - What's unclear: PEXPIREAT is not currently implemented as a command. AOF rewrite will generate it, and replay needs to handle it.
   - Recommendation: Implement PEXPIREAT in the persistence module (or as a command). Alternatively, use PEXPIRE with remaining milliseconds (simpler, but less accurate if rewrite takes time). Using PEXPIRE with the remaining duration calculated at rewrite time is the pragmatic choice for v1.

2. **Multi-database persistence**
   - What we know: Server has 16 databases. RDB/AOF need to handle all of them.
   - What's unclear: Should RDB save all databases or just non-empty ones?
   - Recommendation: Save all non-empty databases with a database index prefix. For AOF, log SELECT commands when the database index changes. The RDB format should include a database selector byte before each database's entries.

3. **Change counter for auto-save**
   - What we know: Auto-save rules are "save if N changes in M seconds". Need a change counter.
   - What's unclear: Where to put it -- Database struct or a separate counter.
   - Recommendation: Add an `AtomicU64` change counter alongside the database Vec, incremented after each write command dispatch. The auto-save timer reads and resets it periodically.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test + integration tests |
| Config file | Cargo.toml (test profile inherits) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PERS-01 | RDB serialize/deserialize round-trip | unit | `cargo test --lib persistence::rdb::tests -x` | No - Wave 0 |
| PERS-01 | RDB file written to disk by BGSAVE | integration | `cargo test --test integration rdb -x` | No - Wave 0 |
| PERS-02 | AOF appends write commands | unit | `cargo test --lib persistence::aof::tests -x` | No - Wave 0 |
| PERS-02 | AOF fsync policy configuration | unit | `cargo test --lib persistence::aof::tests::fsync -x` | No - Wave 0 |
| PERS-03 | RDB restore loads entries correctly | unit | `cargo test --lib persistence::rdb::tests::load -x` | No - Wave 0 |
| PERS-03 | AOF replay restores state | unit | `cargo test --lib persistence::aof::tests::replay -x` | No - Wave 0 |
| PERS-03 | Startup loads from AOF or RDB | integration | `cargo test --test integration restore -x` | No - Wave 0 |
| PERS-04 | BGSAVE command returns immediately | integration | `cargo test --test integration bgsave -x` | No - Wave 0 |
| PERS-05 | BGREWRITEAOF compacts AOF | integration | `cargo test --test integration bgrewriteaof -x` | No - Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/persistence/mod.rs` -- module declaration
- [ ] `src/persistence/rdb.rs` -- RDB save/load with unit tests
- [ ] `src/persistence/aof.rs` -- AOF writer/replay with unit tests
- [ ] Integration tests for BGSAVE, BGREWRITEAOF, startup restore

## Sources

### Primary (HIGH confidence)
- Project source code: `src/storage/entry.rs`, `src/storage/db.rs`, `src/server/listener.rs`, `src/server/expiration.rs`, `src/server/connection.rs`, `src/config.rs`, `src/protocol/serialize.rs`, `src/protocol/parse.rs`, `src/command/mod.rs`
- `04-CONTEXT.md` -- locked decisions from user discussion
- `crc32fast` crate v1.5.0 verified via `cargo search`
- Tokio documentation: `spawn_blocking`, `sync::mpsc`, `fs` module
- POSIX rename atomicity guarantee (well-established systems knowledge)

### Secondary (MEDIUM confidence)
- Redis persistence documentation (general AOF/RDB architecture patterns)
- Rust `std::time::Instant` vs `SystemTime` semantics (standard library docs)

### Tertiary (LOW confidence)
- None -- all findings verified against source code and standard library docs

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - only adding crc32fast (verified version), rest is existing deps
- Architecture: HIGH - patterns derived directly from existing codebase (expiration task, protocol layer)
- Pitfalls: HIGH - Instant vs SystemTime is a verified Rust-specific concern; lock-during-IO is a known async anti-pattern
- Code examples: MEDIUM - illustrative patterns based on API knowledge, will need refinement during implementation

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, custom format)
