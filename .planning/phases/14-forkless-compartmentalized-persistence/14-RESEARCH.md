# Phase 14: Forkless Compartmentalized Persistence - Research

**Researched:** 2026-03-24
**Domain:** Forkless async snapshotting, per-shard WAL, io_uring file I/O
**Confidence:** HIGH

## Summary

This phase replaces the current clone-on-snapshot RDB persistence and global AOF writer with a forkless compartmentalized snapshot system and per-shard WAL. The key architectural enabler is that DashTable's segmented structure provides natural checkpoint boundaries -- each of the ~N segments (60 entries max each) can be independently serialized while the shard continues serving requests. Modified-but-not-yet-serialized entries have their old values captured in a small per-segment overflow buffer (segment-level COW).

The current implementation (`rdb.rs` 730 lines, `aof.rs` 736 lines) clones ALL data under lock for snapshots and uses a single global AOF writer thread with tokio async I/O. Phase 14 fundamentally changes both: snapshots become cooperative tasks within each shard's event loop that iterate segments one at a time, and AOF becomes per-shard WAL files written via io_uring. This aligns with Dragonfly's proven DFS (Dragonfly File System) format approach.

**Primary recommendation:** Add a `segments()` iterator API to DashTable that yields per-segment references, implement a `SnapshotState` struct that tracks epoch + segment progress + overflow buffer within each shard, and replace the global AOF writer with per-shard WAL writers using the existing io_uring infrastructure from Phase 12.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Forkless snapshot mechanism: Mark snapshot epoch, iterate DashTable segments, serialize each segment's current state before modifications touch it. For modified-but-not-yet-serialized segments: capture old value before overwrite (copy-on-write at segment granularity). Serialization runs as low-priority cooperative task within shard's event loop, yielding between segments. Near-zero memory overhead.
- Per-shard parallel snapshots: Each shard snapshots independently, writing its own snapshot file. Parallel per-shard files (DFS-style format). Optional single interleaved RDB-compatible export. Global epoch counter for coordination.
- Segment-level COW strategy: Check segment's "snapshot pending" flag; if pending, copy old entry to per-segment overflow buffer before overwriting. Serializer reads from overflow buffer for modified entries, DashTable for unmodified.
- Per-shard WAL (replacing AOF): Each shard maintains its own WAL file. Writes via io_uring DMA file write path. Batched fsync: accumulate writes, fsync once per millisecond. WAL format: RESP wire format for compatibility.
- Snapshot format: Keep RUSTREDIS binary format with CRC32 footer, extend for per-shard files. Header includes shard ID, epoch number, segment count. Each segment serialized as self-contained block with own CRC32. Enables partial recovery.
- Startup restore: Load per-shard snapshot files in parallel. Then replay per-shard WAL from snapshot's epoch forward. Priority: WAL over snapshot.

### Claude's Discretion
- Exact snapshot scheduling (manual BGSAVE trigger, auto-save rules, or both)
- COW overflow buffer sizing strategy (pre-allocated vs dynamic)
- Whether to support mixing old RDB format and new per-shard format during transition
- Compression of snapshot segments (LZ4 for speed, or none for simplicity)

### Deferred Ideas (OUT OF SCOPE)
- Incremental snapshots (only serialize modified segments since last snapshot) -- future optimization
- Cloud storage backend for snapshots (S3, GCS) -- future feature
</user_constraints>

## Standard Stack

### Core (already in Cargo.toml)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `crc32fast` | 1.x | Per-segment CRC32 checksums | Already used for RDB footer |
| `io-uring` | 0.7 | WAL file writes via io_uring | Already used for networking in Phase 12 |
| `bytes` | 1.10 | Buffer management for serialization | Already used throughout |
| `tokio` | 1.x | Timer for batched fsync, spawn_blocking fallback | Already used |

### Supporting (no new dependencies needed)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `libc` | 0.2 | `O_DIRECT` flag for DMA writes on Linux | WAL writer with direct I/O |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| No compression | `lz4_flex` (pure Rust LZ4) | ~2x smaller snapshots, adds ~5% CPU; recommend NONE initially for simplicity |
| Per-segment CRC32 | xxhash checksum | CRC32 is already a dep and well-established for data integrity |

**No new dependencies required.** All needed crates are already in Cargo.toml.

## Architecture Patterns

### Recommended Project Structure
```
src/persistence/
  mod.rs               # re-exports
  rdb.rs               # MODIFY: add per-shard format, segment-level serialization
  aof.rs               # REPLACE INTERNALS: per-shard WAL writer (keep RESP format)
  auto_save.rs         # MODIFY: per-shard auto-save trigger via ShardMessage
  snapshot.rs           # NEW: SnapshotState, SnapshotCoordinator, segment COW logic
  wal.rs               # NEW: per-shard WAL writer with batched fsync
src/storage/dashtable/
  mod.rs               # ADD: segments() iterator, segment_count(), per-segment access
  segment.rs           # ADD: snapshot_pending flag, overflow buffer support
```

### Pattern 1: Cooperative Snapshot Task (within shard event loop)
**What:** Snapshot runs as a state machine within the shard's existing `spsc_interval` tick, serializing one segment per tick cycle.
**When to use:** Every snapshot operation.
**Example:**
```rust
/// Snapshot state machine -- lives inside the shard, progressed each tick.
pub struct SnapshotState {
    epoch: u64,
    current_segment: usize,
    total_segments: usize,
    output_buf: Vec<u8>,        // Accumulated serialized bytes
    segment_overflow: Vec<(Bytes, Entry)>, // COW buffer for current pending segment
    file_path: PathBuf,
}

impl SnapshotState {
    /// Advance snapshot by one segment. Returns true when complete.
    pub fn advance_one_segment(
        &mut self,
        table: &DashTable<Bytes, Entry>,
        base_ts: u32,
    ) -> bool {
        if self.current_segment >= self.total_segments {
            return true; // done
        }
        // Serialize segment: entries from overflow first, then unmodified from table
        let seg = table.segment(self.current_segment);
        self.serialize_segment(seg, base_ts);
        // Clear segment's "snapshot pending" flag
        self.current_segment += 1;
        self.current_segment >= self.total_segments
    }
}
```

### Pattern 2: Segment-Level COW (pre-write capture)
**What:** Before overwriting a key in a segment marked "snapshot pending", clone the old entry to an overflow buffer.
**When to use:** Every write operation during an active snapshot.
**Example:**
```rust
/// Called before any mutation to a key in the DashTable during snapshot.
/// Intercept at Database::set() level.
fn capture_if_snapshot_pending(
    snapshot: &mut Option<SnapshotState>,
    segment_idx: usize,
    key: &Bytes,
    old_entry: &Entry,
) {
    if let Some(ref mut state) = snapshot {
        if state.is_segment_pending(segment_idx) {
            state.segment_overflow.push((key.clone(), old_entry.clone()));
        }
    }
}
```

### Pattern 3: Per-Shard WAL Writer
**What:** Each shard writes its own WAL file. Serialized RESP commands are buffered in memory and flushed/fsynced on a 1ms timer via io_uring on Linux (tokio fallback on macOS).
**When to use:** Replaces the global AOF writer from Phase 11.
**Example:**
```rust
pub struct WalWriter {
    shard_id: usize,
    write_buf: Vec<u8>,          // Accumulate RESP bytes between fsyncs
    file_path: PathBuf,
    #[cfg(target_os = "linux")]
    fd: std::os::fd::RawFd,      // Direct fd for io_uring writes
    write_offset: u64,            // Current file position
    last_fsync: Instant,
    epoch: u64,                   // Current snapshot epoch for WAL truncation
}
```

### Pattern 4: Snapshot Coordination (global epoch)
**What:** A coordinator (on listener thread or via ShardMessage broadcast) increments a global epoch and sends SnapshotBegin messages to all shards. Each shard independently snapshots at that epoch.
**When to use:** BGSAVE command, auto-save triggers.
**Example:**
```rust
// In ShardMessage enum (dispatch.rs), extend:
pub enum ShardMessage {
    // ... existing variants ...
    SnapshotBegin {
        epoch: u64,
        reply_tx: oneshot::Sender<SnapshotComplete>,
    },
}
```

### Anti-Patterns to Avoid
- **Cloning entire dataset for snapshot:** Current approach clones ALL entries under lock (`SnapshotRequest` in shard/mod.rs lines 378-394). Phase 14 MUST NOT clone the whole dataset.
- **Blocking the shard event loop:** Snapshot serialization must yield between segments. Never serialize all segments in one tick cycle.
- **Global AOF writer:** Single writer thread creates contention. Each shard must own its WAL.
- **Fork-based snapshot:** No fork(), no process-level COW. The entire point of this phase.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CRC32 checksums | Custom checksum | `crc32fast` (already dep) | Hardware-accelerated, well-tested |
| RESP serialization for WAL | New format | Reuse `aof::serialize_command()` | RESP format already working, compatible |
| Atomic file write | Manual tmp+rename | Reuse pattern from `rdb::save()` | Crash-safety pattern already proven |
| io_uring file I/O submission | Custom syscall wrapper | `io-uring` crate (already dep) | Same crate used for networking |

**Key insight:** Most serialization logic from `rdb.rs` (type tags, value encoding, CRC32) is reusable. The change is in HOW data is accessed (segment-by-segment vs full clone) and WHERE it's written (per-shard files vs single file).

## Common Pitfalls

### Pitfall 1: Segment Index Instability During Snapshot
**What goes wrong:** DashTable segments can split during snapshot if new inserts trigger growth. A segment that was pending serialization splits into two, invalidating the snapshot state machine's segment index.
**Why it happens:** DashTable's `insert()` calls `split_segment()` when a segment reaches LOAD_THRESHOLD (51 entries).
**How to avoid:** During an active snapshot, the snapshot state must track segments by identity (store index at epoch start). If a split occurs on a pending segment, the overflow buffer captures the split's effects. Alternative: serialize segment data eagerly before it can split (preferred: mark segment indices at epoch time, track split events).
**Warning signs:** Segment count changes between snapshot begin and end.

### Pitfall 2: Overflow Buffer Memory Growth
**What goes wrong:** Under heavy write load during snapshot, the overflow buffer grows unboundedly, potentially exceeding the 5% memory overhead target.
**Why it happens:** Every write to a pending segment clones the old entry to overflow.
**How to avoid:** Bound overflow per segment. Since each segment holds max 60 entries, overflow per segment is bounded by 60 cloned entries. Process segments in order so only ONE segment is "pending" at a time. Worst case: 60 entries worth of cloned data (~60 * entry_size).
**Warning signs:** Monitor overflow buffer size during snapshot.

### Pitfall 3: WAL File Growth Without Truncation
**What goes wrong:** WAL files grow indefinitely, consuming disk space.
**Why it happens:** No truncation after successful snapshot.
**How to avoid:** After a snapshot completes at epoch N, truncate/rotate WAL to discard entries before epoch N. Use atomic rename: write new WAL to .wal.tmp, rename over old .wal.
**Warning signs:** WAL file size exceeds dataset size.

### Pitfall 4: Non-Atomic Per-Shard Snapshot Set
**What goes wrong:** If the server crashes mid-snapshot, some shard files are complete and some are missing/partial, creating an inconsistent restore point.
**Why it happens:** Each shard writes independently; no global commit point.
**How to avoid:** Write a manifest file AFTER all shard snapshots complete, listing all shard files and their epoch. On restore, only load snapshot sets with a complete manifest. Fall back to WAL replay if manifest is missing.
**Warning signs:** Missing shard snapshot files on restore.

### Pitfall 5: Borrow Conflicts with SnapshotState Inside Shard
**What goes wrong:** SnapshotState needs to read DashTable segments while mutation operations need `&mut` access to the same DashTable.
**Why it happens:** Rust's borrow checker prevents simultaneous `&` and `&mut` borrows.
**How to avoid:** SnapshotState is progressed in a dedicated code path (during spsc_interval tick) where it gets exclusive access to serialize one segment. During normal command processing, only the COW intercept runs (which writes to the overflow buffer, not the DashTable). The overflow buffer lives in SnapshotState, not in the DashTable.
**Warning signs:** Compile errors with multiple borrows of Database.

### Pitfall 6: io_uring File Write on macOS
**What goes wrong:** io_uring is Linux-only; macOS development/testing needs a fallback.
**Why it happens:** Phase 12 already handles this -- io_uring is optional with Tokio fallback.
**How to avoid:** WAL writer must have both io_uring path (Linux) and tokio::fs path (macOS/fallback). Same pattern as Phase 12's UringDriver.
**Warning signs:** Tests fail on macOS CI.

## Code Examples

### DashTable Segment Iteration API (NEW -- does not exist yet)
```rust
// In src/storage/dashtable/mod.rs -- add these methods:

impl<V> DashTable<Bytes, V> {
    /// Return the number of unique segments in the table.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Return an immutable reference to a segment by storage index.
    pub fn segment(&self, idx: usize) -> &Segment<Bytes, V> {
        &self.segments[idx]
    }

    /// Iterate over all unique segments (immutable).
    pub fn segments(&self) -> impl Iterator<Item = (usize, &Segment<Bytes, V>)> {
        self.segments.iter().enumerate().map(|(i, s)| (i, &**s))
    }
}
```

### Per-Shard Snapshot File Format (extending current RDB)
```rust
// Header: RUSTREDIS_SHARD magic + version + shard_id + epoch + segment_count
const SHARD_RDB_MAGIC: &[u8] = b"RRDSHARD";
const SHARD_RDB_VERSION: u8 = 1;

struct ShardSnapshotHeader {
    magic: [u8; 8],       // "RRDSHARD"
    version: u8,
    shard_id: u16,
    epoch: u64,
    segment_count: u32,
    db_index: u8,
}

// Per-segment block:
struct SegmentBlock {
    segment_idx: u32,
    entry_count: u32,
    // entries: [type_tag + key + ttl + value] * entry_count
    crc32: u32,           // CRC32 of this segment block only
}

// Footer:
struct ShardSnapshotFooter {
    eof_marker: u8,       // 0xFF
    global_crc32: u32,    // CRC32 of entire file
}
```

### COW Intercept at Database Level
```rust
// In Database::set() or a wrapper, before overwriting:
impl Database {
    pub fn set_with_snapshot_cow(
        &mut self,
        key: Bytes,
        entry: Entry,
        snapshot: &mut Option<SnapshotState>,
    ) {
        if let Some(ref mut snap) = snapshot {
            // Determine which segment this key lives in
            let hash = xxhash_rust::xxh64::xxh64(&key, 0);
            let seg_idx = self.data.segment_index_for_hash(hash);
            if snap.is_segment_pending(seg_idx) {
                // Capture old value before overwrite
                if let Some(old_entry) = self.data.get(&key) {
                    snap.capture_old_entry(seg_idx, key.clone(), old_entry.clone());
                }
            }
        }
        self.data.insert(key, entry);
    }
}
```

### WAL Writer with Batched Fsync
```rust
pub struct WalWriter {
    shard_id: usize,
    buf: Vec<u8>,
    fd: Option<std::os::fd::RawFd>, // Linux io_uring path
    file: Option<std::fs::File>,     // Fallback path
    last_fsync: Instant,
    offset: u64,
}

impl WalWriter {
    /// Append a RESP-encoded command to the write buffer.
    pub fn append(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Flush buffer to disk. Called on 1ms timer.
    pub fn flush_and_fsync(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // Write buffered data
        // On Linux: io_uring prep_write + prep_fsync
        // Fallback: std::fs::File::write_all + sync_data
        self.offset += self.buf.len() as u64;
        self.buf.clear();
        self.last_fsync = Instant::now();
        Ok(())
    }
}
```

## Design Decisions (Claude's Discretion)

### Snapshot Scheduling: Both BGSAVE and Auto-Save
**Recommendation:** Support BOTH manual BGSAVE trigger and auto-save rules. The current auto-save infrastructure (`auto_save.rs`) already has rule parsing. Adapt it to send `SnapshotBegin` messages to all shards instead of calling `bgsave_start()`.
**Rationale:** Backward compatibility with existing Redis config patterns.

### COW Overflow Buffer: Dynamic Vec (not pre-allocated)
**Recommendation:** Use a `Vec<(Bytes, Entry)>` per snapshot state, not pre-allocated. Since only ONE segment is pending at a time and segments hold max 60 entries, the worst-case overflow is 60 entries (~60 * 24 bytes = ~1.4KB for CompactEntry). This is negligible.
**Rationale:** Pre-allocation wastes memory for the common case (few modifications during snapshot window).

### Old RDB Format Compatibility: Maintain backward-compatible export
**Recommendation:** Keep the existing `rdb::save()` and `rdb::load()` functions working. Add new `shard_snapshot::save()` / `shard_snapshot::load()` alongside. The `BGSAVE` command uses the new per-shard format by default. Add a `SAVE` command or config option to produce a single merged RDB file using the existing merge logic (`rdb::merge_shard_snapshots()`).
**Rationale:** Gradual migration; no breaking changes for users with existing RDB files.

### Compression: None initially
**Recommendation:** No compression for Phase 14. Snapshot segments are already compact (CompactEntry is 24 bytes). LZ4 can be added as a future optimization.
**Rationale:** Simplicity. The primary goal is eliminating fork latency, not reducing file size.

## State of the Art

| Old Approach (Current) | New Approach (Phase 14) | Impact |
|------------------------|------------------------|--------|
| Clone entire dataset under lock | Iterate segments one at a time | <1ms stall vs ~60ms for 25GB |
| Single global AOF writer | Per-shard WAL with batched fsync | No cross-shard contention |
| tokio async file I/O | io_uring DMA writes (Linux) | Lower CPU overhead, batched syscalls |
| Single RDB file | Per-shard snapshot files + manifest | Parallel I/O, partial recovery |
| SnapshotRequest clones all entries | Cooperative segment-by-segment serialization | <5% memory overhead vs 2-3x |

**Dragonfly reference:** Dragonfly's DFS format uses this exact approach -- per-shard parallel snapshot files with compartmentalized serialization. Their production experience validates the architecture.

## Open Questions

1. **Segment split during snapshot**
   - What we know: Splits can happen during snapshot if writes trigger LOAD_THRESHOLD. The split redistributes entries between old and new segments.
   - What's unclear: Exact interplay between split and pending-segment tracking. Does the snapshot need to handle segments that were split after epoch?
   - Recommendation: Simplest approach: treat split as creating two new segments. If the original segment was pending, both halves are pending and their entries are already captured (the split itself reads all entries). Track segment count at epoch start; if splits occur, extend the snapshot to cover new segments.

2. **WAL truncation timing**
   - What we know: WAL should be truncated after snapshot completes.
   - What's unclear: Should truncation happen immediately after snapshot, or on next snapshot start?
   - Recommendation: Truncate on next snapshot start (rename old WAL, start fresh). This ensures WAL covers the full gap between snapshots for crash recovery.

3. **Multi-database snapshot coordination**
   - What we know: Each shard has 16 databases (SELECT 0-15). Current snapshot iterates all databases per shard.
   - What's unclear: Should each database have its own epoch, or one epoch per shard?
   - Recommendation: One epoch per shard. Snapshot all 16 databases of a shard in sequence during the same epoch. This matches the current behavior and simplifies coordination.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in `#[test]` + `cargo test` |
| Config file | Cargo.toml (test section) |
| Quick run command | `cargo test -p rust-redis --lib persistence` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SNAP-01 | Snapshot epoch marks point-in-time; segments iterated without blocking | unit | `cargo test snapshot_state_advance` | No -- Wave 0 |
| SNAP-02 | Segment COW captures old values during snapshot | unit | `cargo test cow_intercept` | No -- Wave 0 |
| SNAP-03 | Cooperative task yields between segments | unit | `cargo test snapshot_cooperative` | No -- Wave 0 |
| SNAP-04 | Memory overhead during snapshot < 5% | unit | `cargo test snapshot_memory_overhead` | No -- Wave 0 |
| WAL-01 | Per-shard WAL writes RESP format | unit | `cargo test wal_writer_resp` | No -- Wave 0 |
| WAL-02 | Batched fsync at 1ms interval | unit | `cargo test wal_batched_fsync` | No -- Wave 0 |
| SHARD-01 | Parallel per-shard snapshot files | integration | `cargo test shard_parallel_snapshot` | No -- Wave 0 |
| RESTORE-01 | Startup loads per-shard files + replays WAL | integration | `cargo test shard_restore` | No -- Wave 0 |
| FMT-01 | Per-segment CRC32 in snapshot format | unit | `cargo test segment_crc32` | No -- Wave 0 |
| COMPAT-01 | Old RDB format still loads | unit | `cargo test rdb_backward_compat` | Partially (existing rdb.rs tests) |

### Sampling Rate
- **Per task commit:** `cargo test -p rust-redis --lib persistence`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/persistence/snapshot.rs` -- SnapshotState unit tests (SNAP-01 through SNAP-04)
- [ ] `src/persistence/wal.rs` -- WalWriter unit tests (WAL-01, WAL-02)
- [ ] DashTable segment iteration API tests (in `src/storage/dashtable/mod.rs`)
- [ ] Integration tests for per-shard snapshot + WAL restore (SHARD-01, RESTORE-01)

## Sources

### Primary (HIGH confidence)
- Project codebase: `src/persistence/rdb.rs`, `src/persistence/aof.rs`, `src/storage/dashtable/` -- direct code analysis
- `.planning/architect-blue-print.md` section "Persistence without fork" -- architecture specification
- `14-CONTEXT.md` -- locked decisions from user

### Secondary (MEDIUM confidence)
- Dragonfly DFS format: referenced in blueprint, architecture proven in production at scale
- io_uring file I/O: Phase 12 already established the pattern with `io-uring` 0.7 crate

### Tertiary (LOW confidence)
- None -- all findings based on direct codebase analysis and architectural blueprint

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - no new dependencies, all crates already in use
- Architecture: HIGH - blueprint is detailed, Dragonfly proves the approach, codebase is well-understood
- Pitfalls: HIGH - derived from direct code analysis of DashTable segments and borrow patterns
- Code examples: MEDIUM - segment iteration API is new code that must be implemented and validated

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable architecture, no external API dependencies)
