# Phase 43: RDB Snapshot Hardening - Research

**Researched:** 2026-03-26
**Domain:** Persistence / RDB format / Async I/O / Replication streaming
**Confidence:** HIGH

## Summary

Phase 43 hardens the existing forkless async snapshot engine (Phase 14) for production readiness. The codebase already has a well-structured cooperative segment-by-segment snapshot (`SnapshotState` in snapshot.rs, 687 lines) using a custom RRDSHARD format, plus entry-level RDB serialization (`rdb.rs` with `write_entry`/`read_entry` for all 6 data types). The key gaps are: (1) no Redis-compatible RDB format writer for PSYNC2 full resync, (2) `finalize()` uses blocking `std::fs::write()`, (3) `bgsave_start_sharded()` clears `SAVE_IN_PROGRESS` immediately instead of tracking completion via oneshot channels, (4) replication sends files from disk rather than streaming on-the-fly, and (5) missing SAVE, LASTSAVE, and INFO persistence section.

The existing `rdb::write_entry()` already serializes all 6 data types (string, hash, list, set, sorted set, stream) with compact variant expansion. The main work for Redis RDB compatibility is wrapping these entries in the proper Redis RDB framing: `REDIS0010` magic, length-encoded strings, Redis type tags (0-5 for basic types), millisecond expiry opcode (0xFC), RESIZEDB (0xFB), AUX metadata (0xFA), and CRC64 checksum instead of CRC32.

**Primary recommendation:** Build a `redis_rdb` module that reuses existing `write_entry` data serialization but wraps it in Redis-compatible framing (length encoding, type tags, opcodes). Make `finalize()` async with cfg-gated tokio::fs::write vs monoio file ops. Fix BGSAVE completion tracking by collecting oneshot receivers and spawning a completion watcher task.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
None -- all implementation choices at Claude's discretion (pure infrastructure phase).

### Claude's Discretion
All implementation choices including:
- Redis RDB format version to target
- Async I/O approach for finalize()
- BGSAVE completion tracking mechanism
- Streaming RDB generation design
- SAVE/LASTSAVE/INFO persistence implementation

### Deferred Ideas (OUT OF SCOPE)
- Redis RDB format v10 full compatibility (only need enough for PSYNC2 bulk transfer)
- redis-check-rdb / rdbtools interop testing
- Point-in-time recovery from WAL + snapshot combination
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| crc32fast | 1.x | CRC32 for RRDSHARD format | Already in use |
| crc64 | 2.x | CRC64 for Redis RDB format | Redis RDB v5+ uses CRC64, not CRC32 |
| tokio (fs feature) | 1.x | Async file I/O for Tokio runtime | Already a dependency with `fs` feature enabled |
| lzf | 0.3.x | LZF compression for Redis RDB strings | Redis uses LZF for large string compression in RDB |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tempfile | 3.x | Temporary files for atomic writes in tests | Already in dev-dependencies |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| crc64 crate | Manual CRC64 implementation | Crate is tiny and correct; no reason to hand-roll |
| lzf crate | Skip LZF compression entirely | Redis clients expect LZF for large values; but for PSYNC2 transfer to our own replicas, LZF is optional since replicas understand our format |

**Installation:**
```bash
cargo add crc64
# lzf is optional -- only needed if writing RDB files that Redis itself must read
cargo add lzf  # optional
```

**Note on LZF:** For PSYNC2 full resync to our OWN replicas, we can continue sending RRDSHARD format (the replica code in `replica.rs` already handles it). Redis RDB export is only needed for migration/interop scenarios. For the initial implementation, skip LZF compression and emit uncompressed Redis RDB -- this is valid per the format spec.

## Architecture Patterns

### Recommended Project Structure
```
src/persistence/
  rdb.rs              # Existing: entry-level serialization (keep as-is)
  redis_rdb.rs        # NEW: Redis-compatible RDB format writer/reader
  snapshot.rs          # Modified: async finalize(), streaming support
  auto_save.rs         # Modified: track BGSAVE completion
  wal.rs               # Unchanged
src/command/
  persistence.rs       # Modified: SAVE, LASTSAVE, fix BGSAVE completion
src/replication/
  master.rs            # Modified: streaming RDB generation
```

### Pattern 1: Redis RDB Format Writer
**What:** A standalone module that produces byte-exact Redis RDB files using the existing `write_entry` data, rewrapped in Redis framing.
**When to use:** PSYNC2 full resync to Redis replicas, or RDB export for migration.
**Example:**
```rust
// src/persistence/redis_rdb.rs
use std::io::Write;

const REDIS_RDB_MAGIC: &[u8] = b"REDIS";
const REDIS_RDB_VERSION: &[u8] = b"0010"; // RDB version 10
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;

// Redis type tags (basic types only -- no compact encodings needed for export)
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET_2: u8 = 5; // ZSET v2: scores as binary doubles
const RDB_TYPE_HASH: u8 = 4;

/// Redis length encoding: variable-length integer
fn write_length(buf: &mut Vec<u8>, len: u64) {
    if len < 64 {
        buf.push(len as u8); // 00xxxxxx
    } else if len < 16384 {
        buf.push(0x40 | ((len >> 8) as u8)); // 01xxxxxx
        buf.push((len & 0xFF) as u8);
    } else if len <= u32::MAX as u64 {
        buf.push(0x80); // 10000000
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        buf.push(0x81); // 10000001
        buf.extend_from_slice(&len.to_be_bytes());
    }
}

/// Redis length-prefixed string
fn write_redis_string(buf: &mut Vec<u8>, s: &[u8]) {
    write_length(buf, s.len() as u64);
    buf.extend_from_slice(s);
}
```

### Pattern 2: Async Finalize with Runtime Abstraction
**What:** Replace `std::fs::write` in `finalize()` with async file write, cfg-gated for Tokio and Monoio.
**When to use:** All snapshot finalize paths.
**Example:**
```rust
// In snapshot.rs
pub async fn finalize_async(&mut self) -> anyhow::Result<()> {
    self.output_buf.push(EOF_MARKER);
    let mut hasher = Hasher::new();
    hasher.update(&self.output_buf);
    let global_crc = hasher.finalize();
    self.output_buf.extend_from_slice(&global_crc.to_le_bytes());

    let tmp_path = self.file_path.with_extension("rrdshard.tmp");

    #[cfg(feature = "runtime-tokio")]
    {
        tokio::fs::write(&tmp_path, &self.output_buf).await
            .context("Failed to write temporary snapshot file")?;
        tokio::fs::rename(&tmp_path, &self.file_path).await
            .context("Failed to rename temporary snapshot file")?;
    }

    #[cfg(feature = "runtime-monoio")]
    {
        // Monoio: use blocking write (thread-per-core, rare operation)
        std::fs::write(&tmp_path, &self.output_buf)
            .context("Failed to write temporary snapshot file")?;
        std::fs::rename(&tmp_path, &self.file_path)
            .context("Failed to rename temporary snapshot file")?;
    }

    Ok(())
}
```

### Pattern 3: BGSAVE Completion Tracking
**What:** Collect oneshot receivers in `bgsave_start_sharded()`, spawn a watcher task that awaits all completions before clearing `SAVE_IN_PROGRESS`.
**When to use:** Every BGSAVE invocation.
**Example:**
```rust
pub fn bgsave_start_sharded(
    producers: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    snapshot_dir: &str,
    num_shards: usize,
) -> Frame {
    if SAVE_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(b"ERR Background save already in progress"));
    }

    let epoch = SNAPSHOT_EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
    let snap_dir = PathBuf::from(snapshot_dir);
    let mut receivers = Vec::new();

    let mut prods = producers.borrow_mut();
    for prod in prods.iter_mut() {
        let (tx, rx) = crate::runtime::channel::oneshot();
        let _ = prod.try_push(ShardMessage::SnapshotBegin {
            epoch,
            snapshot_dir: snap_dir.clone(),
            reply_tx: tx,
        });
        receivers.push(rx);
    }
    drop(prods);

    // Spawn completion watcher
    crate::runtime::RuntimeImpl::spawn_local(async move {
        for rx in receivers {
            let _ = rx.await;
        }
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        LAST_SAVE_TIME.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    });

    Frame::SimpleString(Bytes::from_static(b"Background saving started"))
}
```

### Pattern 4: Streaming RDB for Replication
**What:** Instead of writing snapshot to disk then reading it back, generate Redis RDB bytes incrementally as each shard completes its snapshot, streaming directly to the replica socket.
**When to use:** Full resync in `handle_psync_on_master`.
**Example approach:**
```rust
// In master.rs full resync:
// 1. Trigger snapshots, collect output_buf references
// 2. As each shard completes, convert its RRDSHARD entries to Redis RDB format
// 3. Stream the combined Redis RDB directly to replica write_half
//
// For simplicity in v1: keep file-based transfer but use async file reads.
// Streaming generation is a performance optimization for later.
```

### Anti-Patterns to Avoid
- **Blocking I/O in event loop:** The current `std::fs::write` in `finalize()` blocks the shard event loop. Even though it runs cooperatively (one segment per tick), the final write of the entire buffer can block for large datasets.
- **Fire-and-forget BGSAVE:** The current `bgsave_start_sharded()` clears `SAVE_IN_PROGRESS` immediately, making concurrent BGSAVE invocations possible and LASTSAVE unreliable.
- **Full RDB compatibility overkill:** Don't implement ziplist/listpack/quicklist Redis compact encodings for RDB export. Emit basic types (type 0-5) which are universally supported. Redis can load RDB with basic types regardless of version.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CRC64 checksum | Custom CRC64 table | `crc64` crate | Polynomial must match Redis's CRC64-JONES exactly |
| Redis length encoding | Ad-hoc byte packing | Dedicated `write_length`/`read_length` functions | Off-by-one in 6/14/32/64-bit boundaries cause corrupt files |
| LZF compression | Custom compressor | `lzf` crate (or skip compression) | LZF decompression bugs are silent data corruption |
| Sorted set score encoding | String-based score | Binary f64 little-endian (ZSET_2 type) | Redis ZSET type 3 uses ASCII scores; type 5 uses binary doubles -- use type 5 |

**Key insight:** Redis RDB format has many subtle encoding rules (length encoding bit patterns, integer-as-string special cases, CRC64 polynomial). Use the basic uncompressed types (0-5) to avoid dealing with ziplist/listpack/quicklist binary formats entirely. Redis can always load basic types.

## Common Pitfalls

### Pitfall 1: CRC64 Polynomial Mismatch
**What goes wrong:** Redis uses a specific CRC64-JONES polynomial (0xAD93D23594C935A9). Using a different polynomial produces valid CRC64 values that Redis rejects.
**Why it happens:** Multiple CRC64 variants exist (ECMA, ISO, Jones).
**How to avoid:** Verify the crate uses the Jones polynomial, or implement the exact lookup table from Redis source.
**Warning signs:** Redis refuses to load the RDB file with "Wrong RDB checksum" error.

### Pitfall 2: Redis Length Encoding Big-Endian for 32/64-bit
**What goes wrong:** Redis uses big-endian for the 32-bit and 64-bit length values (after the type prefix byte), unlike the rest of the format which uses little-endian for timestamps and other values.
**Why it happens:** Historical Redis design inconsistency.
**How to avoid:** Use `to_be_bytes()` for length encoding 32/64-bit values, `to_le_bytes()` for expiry timestamps.
**Warning signs:** Garbage key lengths when reading RDB files.

### Pitfall 3: Forgetting AUX Fields for Redis Compatibility
**What goes wrong:** Redis 7+ expects certain AUX fields (redis-ver, redis-bits, ctime, used-mem) in the RDB header. Without them, redis-check-rdb warnings appear and some tools reject the file.
**Why it happens:** AUX fields were optional in early RDB versions but are now expected.
**How to avoid:** Always write minimal AUX fields: redis-ver, redis-bits, ctime, used-mem.
**Warning signs:** Warnings from redis-check-rdb or Redis log.

### Pitfall 4: SAVE_IN_PROGRESS Race in Sharded Mode
**What goes wrong:** Current code clears `SAVE_IN_PROGRESS` immediately after sending messages to shard producers, so another BGSAVE can start while the previous one is still running.
**Why it happens:** The `bgsave_start_sharded()` function was designed as fire-and-forget.
**How to avoid:** Keep `SAVE_IN_PROGRESS` set, collect oneshot receivers, spawn a completion watcher that clears the flag after all shards complete.
**Warning signs:** Multiple overlapping snapshots, WAL truncation during active snapshot, corrupt files.

### Pitfall 5: Async Finalize Lifetime Issues
**What goes wrong:** `finalize_async()` borrows `self.output_buf` across an await point. In Monoio's !Send context this is fine, but for Tokio, the snapshot state machine lives on a LocalSet, so it's also fine -- but moving the buffer into a spawned task requires ownership transfer.
**Why it happens:** Rust's borrow checker enforces that references don't cross await points in Send futures.
**How to avoid:** Since the shard event loop uses `spawn_local` (LocalSet), the future is !Send and can hold references across awaits. Alternatively, take ownership: `let buf = std::mem::take(&mut self.output_buf)` before the async write.
**Warning signs:** Compilation errors about `&mut self` across await points.

### Pitfall 6: RESIZEDB Opcode for Efficient Loading
**What goes wrong:** Without RESIZEDB hints, Redis pre-allocates small hash tables and resizes repeatedly during load, causing O(n) rehashes.
**Why it happens:** RESIZEDB (opcode 0xFB) tells Redis the hash table sizes before loading entries.
**How to avoid:** Write RESIZEDB after SELECTDB with db_size and expires_size as length-encoded integers.
**Warning signs:** Slow RDB loading in Redis, excessive memory allocations during restore.

## Code Examples

### Redis RDB File Header
```rust
fn write_rdb_header(buf: &mut Vec<u8>) {
    // Magic + version
    buf.extend_from_slice(b"REDIS0010"); // RDB version 10

    // AUX fields
    write_aux(buf, b"redis-ver", b"7.0.0");
    write_aux(buf, b"redis-bits", b"64");
    let ctime = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    write_aux(buf, b"ctime", ctime.to_string().as_bytes());
    write_aux(buf, b"used-mem", b"0"); // placeholder
}

fn write_aux(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buf.push(0xFA); // RDB_OPCODE_AUX
    write_redis_string(buf, key);
    write_redis_string(buf, value);
}
```

### Redis RDB Entry (String Example)
```rust
fn write_rdb_string_entry(buf: &mut Vec<u8>, key: &[u8], value: &[u8], expire_ms: Option<u64>) {
    // Expiry (if any)
    if let Some(ms) = expire_ms {
        buf.push(0xFC); // EXPIRETIME_MS
        buf.extend_from_slice(&ms.to_le_bytes());
    }
    // Type tag
    buf.push(0); // RDB_TYPE_STRING
    // Key
    write_redis_string(buf, key);
    // Value
    write_redis_string(buf, value);
}
```

### Redis RDB Footer
```rust
fn write_rdb_footer(buf: &mut Vec<u8>) {
    buf.push(0xFF); // EOF
    // CRC64 of everything so far
    let checksum = crc64::crc64(0, buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
}
```

### SAVE Command (Synchronous)
```rust
// Blocking save: iterate all shards, collect entries, write single RDB file
pub fn handle_save(databases: &[Database], path: &Path) -> Frame {
    match redis_rdb::save(databases, path) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(b"OK")),
        Err(e) => Frame::Error(Bytes::from(format!("ERR {}", e))),
    }
}
```

### LASTSAVE Command
```rust
pub static LAST_SAVE_TIME: AtomicU64 = AtomicU64::new(0);

pub fn handle_lastsave() -> Frame {
    let ts = LAST_SAVE_TIME.load(Ordering::Relaxed);
    Frame::Integer(ts as i64)
}
```

### INFO Persistence Section
```rust
fn info_persistence() -> String {
    format!(
        "# Persistence\r\n\
         loading:0\r\n\
         rdb_changes_since_last_save:{}\r\n\
         rdb_bgsave_in_progress:{}\r\n\
         rdb_last_save_time:{}\r\n\
         rdb_last_bgsave_status:ok\r\n\
         aof_enabled:{}\r\n\
         aof_rewrite_in_progress:0\r\n",
        changes_since_save,
        if SAVE_IN_PROGRESS.load(Ordering::Relaxed) { 1 } else { 0 },
        LAST_SAVE_TIME.load(Ordering::Relaxed),
        aof_enabled as u8,
    )
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| RDB type 3 (ZSET) with ASCII scores | RDB type 5 (ZSET_2) with binary f64 | Redis 3.2 / RDB v8 | Use type 5 for sorted sets -- more compact and faster |
| CRC32 checksum | CRC64 checksum | Redis 2.6 / RDB v5 | Redis RDB files require CRC64, not CRC32 |
| ziplist for small collections | listpack for small collections | Redis 7.0 / RDB v10 | For export, just use basic types (0-5) -- avoid encoding compact formats |
| Blocking fork-based snapshot | Forkless cooperative snapshot | Our Phase 14 | Already implemented -- just needs async finalize |

**Deprecated/outdated:**
- ZSET type 3 (ASCII scores): Still loadable but type 5 is standard since Redis 3.2
- Zipmap (type 9): Deprecated since Redis 2.6, replaced by ziplist then listpack

## Open Questions

1. **CRC64 Polynomial Verification**
   - What we know: Redis uses CRC64-JONES (polynomial 0xAD93D23594C935A9)
   - What's unclear: Whether the `crc64` Rust crate uses the exact same polynomial
   - Recommendation: Verify at implementation time; if mismatch, implement the lookup table from Redis source (redis/src/crc64.c) -- it's ~70 lines

2. **Streaming RDB vs File-Based Transfer for PSYNC2**
   - What we know: Current code writes .rrdshard to disk then reads back for replication
   - What's unclear: Whether streaming on-the-fly is worth the complexity for v1
   - Recommendation: For v1, keep file-based but switch to async reads. Add a `generate_redis_rdb_bytes()` function that converts RRDSHARD data to Redis RDB in-memory for the transfer. True streaming can be a follow-up.

3. **SAVE Command in Sharded Mode**
   - What we know: SAVE is synchronous and blocks. In sharded mode, the command handler thread doesn't own shard data.
   - What's unclear: How to implement blocking SAVE across shards without deadlock
   - Recommendation: SAVE in sharded mode sends SnapshotBegin to all shards and blocks on completion (same as replication full resync path). Document that SAVE is slow and prefer BGSAVE.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test + cargo test |
| Config file | Cargo.toml (features: runtime-tokio) |
| Quick run command | `cargo test --features runtime-tokio persistence` |
| Full suite command | `cargo test --features runtime-tokio` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| RDB-01 | Redis RDB format write produces valid header/footer/entries | unit | `cargo test --features runtime-tokio redis_rdb` | Wave 0 |
| RDB-02 | Redis RDB round-trip: write then load with redis_rdb::load | unit | `cargo test --features runtime-tokio redis_rdb::tests` | Wave 0 |
| RDB-03 | Async finalize writes file without blocking event loop | unit | `cargo test --features runtime-tokio snapshot::tests::test_finalize_async` | Wave 0 |
| RDB-04 | BGSAVE completion tracking: SAVE_IN_PROGRESS cleared only after all shards complete | unit | `cargo test --features runtime-tokio persistence::tests` | Wave 0 |
| RDB-05 | SAVE command produces valid RDB file | unit | `cargo test --features runtime-tokio test_save_command` | Wave 0 |
| RDB-06 | LASTSAVE returns timestamp after successful save | unit | `cargo test --features runtime-tokio test_lastsave` | Wave 0 |
| RDB-07 | INFO persistence section contains expected fields | unit | `cargo test --features runtime-tokio test_info_persistence` | Wave 0 |
| RDB-08 | Replication full resync uses async file reads | integration | Manual verification via replication test | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --features runtime-tokio persistence`
- **Per wave merge:** `cargo test --features runtime-tokio`
- **Phase gate:** Full suite green before verification

### Wave 0 Gaps
- [ ] `src/persistence/redis_rdb.rs` -- new module with unit tests for Redis RDB format
- [ ] Update `src/persistence/snapshot.rs` tests for async finalize
- [ ] Update `src/command/persistence.rs` tests for completion tracking and LASTSAVE

## Sources

### Primary (HIGH confidence)
- Redis source `rdb.h` (https://github.com/redis/redis/blob/unstable/src/rdb.h) -- RDB_VERSION=13, all type constants and opcodes verified
- Redis RDB file format specification (https://rdb.fnordig.de/file_format.html) -- length encoding, string encoding, opcodes
- Existing codebase: `src/persistence/snapshot.rs`, `src/persistence/rdb.rs`, `src/command/persistence.rs`, `src/replication/master.rs` -- all directly read and analyzed

### Secondary (MEDIUM confidence)
- RDB version history (https://rdb.fnordig.de/version_history.html) -- versions 2-7 documented, 8-10+ from Redis source
- Redis persistence docs (https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) -- general architecture

### Tertiary (LOW confidence)
- CRC64 polynomial compatibility between `crc64` Rust crate and Redis -- needs verification at implementation time

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all existing dependencies verified, only crc64 is new
- Architecture: HIGH -- patterns derived directly from existing codebase analysis
- Pitfalls: HIGH -- identified from Redis source code and format specification
- Redis RDB format: MEDIUM -- version 10 type constants verified from rdb.h, but compact encoding details (listpack/quicklist) not needed since we emit basic types only

**Research date:** 2026-03-26
**Valid until:** 2026-04-26 (stable domain, Redis RDB format changes rarely)
