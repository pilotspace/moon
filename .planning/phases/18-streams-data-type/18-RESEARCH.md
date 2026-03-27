# Phase 18: Streams Data Type - Research

**Researched:** 2026-03-24
**Domain:** Redis Streams (radix tree + listpack storage, consumer groups, blocking reads)
**Confidence:** HIGH

## Summary

Redis Streams is the most complex Redis data type -- essentially an embedded append-only message broker with consumer group semantics. The core storage model is a radix tree (rax) keyed by entry IDs where each leaf node is a listpack containing multiple entries that share a common ID prefix. Entry IDs are `<ms-timestamp>-<sequence>` format, monotonically increasing, enabling efficient range queries.

This phase builds on two completed prerequisites: Phase 15 (Listpack encoding) provides the leaf node storage, and Phase 17 (Blocking commands) provides the infrastructure for XREAD BLOCK. The implementation adds a new `Stream` variant to `RedisValue`, a new `src/command/stream.rs` module with ~15 commands, a new `HEAP_TAG_STREAM` in CompactValue, and RDB persistence support via a new `TYPE_STREAM` tag.

**Primary recommendation:** Use `BTreeMap<StreamId, Listpack>` as the initial storage (Claude's discretion area) rather than a custom radix tree. BTreeMap provides O(log N) range queries, ordered iteration, and correct semantics with zero custom data structure risk. The listpack leaves from Phase 15 store multiple field-value entries per node. This matches Redis's logical structure while being simpler to implement correctly.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Radix tree keyed by stream entry ID prefix (timestamp portion) with listpack leaf nodes
- Entry ID format: `<ms-timestamp>-<sequence>` with auto-generation and explicit ID support
- Last generated ID tracked per stream for monotonically increasing IDs
- XADD with optional MAXLEN/MINID trimming, XLEN, XRANGE/XREVRANGE with COUNT, XTRIM
- XREAD with optional BLOCK using Phase 17 blocking infrastructure
- XINFO with STREAM/GROUPS/CONSUMERS subcommands
- Per-stream consumer group state: group name, last-delivered-id, consumer list, PEL
- XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM
- Stream variant added to RedisValue enum with radix tree, last generated ID, consumer groups, entry count

### Claude's Discretion
- Whether to implement a proper radix tree or use BTreeMap<u128, Vec<u8>> as simpler initial storage
- Approximate trimming strategy (~ prefix on MAXLEN)
- XINFO output format details
- Whether radix tree nodes use listpack encoding from Phase 15 or a simpler Vec<Entry>

### Deferred Ideas (OUT OF SCOPE)
- XSETID command (low priority)
- Stream replication specifics (Phase 19)
</user_constraints>

## Standard Stack

### Core (all existing in project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| bytes | (existing) | Binary-safe keys and field values | Already used throughout |
| BTreeMap | std | Ordered storage keyed by StreamId | O(log N) range queries, ordered iteration |
| HashMap | std | Consumer groups by name, PEL by entry ID | O(1) lookup for ACK/claim operations |
| tokio::sync::oneshot | (existing) | XREAD BLOCK wakeup delivery | Same pattern as Phase 17 blocking |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| Listpack | (Phase 15) | Leaf node storage for stream entries | Each BTreeMap node stores entries in listpack |
| BlockingRegistry | (Phase 17) | XREAD BLOCK waiter management | When BLOCK timeout > 0 on XREAD/XREADGROUP |

**No new external dependencies required.** Everything builds on existing project infrastructure.

## Architecture Patterns

### Recommended Project Structure
```
src/
  storage/
    stream.rs          # StreamId, Stream struct, ConsumerGroup, PEL types
  command/
    stream.rs          # All XADD/XREAD/XGROUP/etc command handlers
  blocking/
    wakeup.rs          # Add try_wake_stream_waiter (extend existing)
```

### Pattern 1: StreamId as u128
**What:** Encode `<ms>-<seq>` as a single u128 for efficient comparison and BTreeMap keying.
**When to use:** All internal stream ID storage and comparison.
**Example:**
```rust
/// Stream entry ID: <milliseconds>-<sequence>
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    /// Pack into u128 for BTreeMap key (ms in upper 64, seq in lower 64).
    pub fn to_u128(self) -> u128 {
        ((self.ms as u128) << 64) | (self.seq as u128)
    }

    /// Parse from "ms-seq" or "ms" (seq defaults to 0 for range start, u64::MAX for range end).
    pub fn parse(s: &[u8], default_seq: u64) -> Result<Self, ()> { /* ... */ }

    /// Format as "ms-seq" bytes.
    pub fn to_bytes(self) -> Bytes { /* ... */ }
}
```

### Pattern 2: Stream Struct with BTreeMap + Metadata
**What:** Main stream data structure holding entries, metadata, and consumer groups.
**When to use:** The `RedisValue::Stream(Box<Stream>)` variant.
**Example:**
```rust
pub struct Stream {
    /// Entries ordered by ID. Each value is a Vec of (field, value) pairs.
    /// For simplicity, store field-value pairs directly rather than listpack
    /// encoding per BTreeMap node (discretion choice -- simpler, adequate perf).
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Total entry count (may differ from entries.len() if entries are trimmed).
    length: u64,
    /// Last generated entry ID (for monotonic guarantee).
    last_id: StreamId,
    /// Consumer groups keyed by group name.
    groups: HashMap<Bytes, ConsumerGroup>,
}

pub struct ConsumerGroup {
    /// Last delivered ID -- entries after this are "new" for the group.
    last_delivered_id: StreamId,
    /// Pending Entries List: entry_id -> PendingEntry.
    pel: BTreeMap<StreamId, PendingEntry>,
    /// Per-consumer state.
    consumers: HashMap<Bytes, Consumer>,
}

pub struct PendingEntry {
    /// Consumer who owns this pending entry.
    consumer: Bytes,
    /// Delivery timestamp (ms since epoch).
    delivery_time: u64,
    /// Number of times this entry has been delivered.
    delivery_count: u64,
}

pub struct Consumer {
    /// Consumer name.
    name: Bytes,
    /// Per-consumer PEL (subset of group PEL for this consumer).
    /// Stored as a set of entry IDs for O(1) membership check.
    pending: BTreeMap<StreamId, ()>,
    /// Last time this consumer was active (for idle time in XINFO).
    seen_time: u64,
}
```

### Pattern 3: RedisValue/CompactValue Extension
**What:** Add Stream variant following existing type patterns.
**When to use:** Integration with database, RDB, command dispatch.
**Example:**
```rust
// In entry.rs - RedisValue enum
pub enum RedisValue {
    // ... existing variants ...
    Stream(Box<Stream>),  // Box because Stream is large
}

// In compact_value.rs - new heap tag
const HEAP_TAG_STREAM: usize = 5;  // Next available after HEAP_TAG_ZSET=4

// In RedisValueRef
pub enum RedisValueRef<'a> {
    // ... existing variants ...
    Stream(&'a Stream),
}
```

### Pattern 4: XREAD BLOCK via BlockingRegistry
**What:** Extend Phase 17's blocking infrastructure for stream reads.
**When to use:** When XREAD or XREADGROUP has BLOCK option.
**Example:**
```rust
// Add to BlockedCommand enum
pub enum BlockedCommand {
    // ... existing ...
    XRead {
        /// (key, last_seen_id) pairs for each stream being read.
        streams: Vec<(Bytes, StreamId)>,
        count: Option<u64>,
    },
    XReadGroup {
        group: Bytes,
        consumer: Bytes,
        streams: Vec<(Bytes, StreamId)>,
        count: Option<u64>,
        noack: bool,
    },
}

// In wakeup.rs - new function
pub fn try_wake_stream_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    // Pop waiter, check if stream has entries > last_seen_id, deliver
}
```

### Pattern 5: Wakeup Hook for XADD
**What:** After XADD adds an entry, check for blocked XREAD/XREADGROUP waiters.
**When to use:** Post-dispatch hook in shard handler, same pattern as LPUSH waking BLPOP.
```rust
// In shard handler, after XADD dispatch:
if cmd.eq_ignore_ascii_case(b"XADD") {
    // key is args[0]
    try_wake_stream_waiter(&mut blocking_reg, &mut db, db_index, &key);
}
```

### Anti-Patterns to Avoid
- **Custom radix tree implementation:** Deceptive complexity, BTreeMap gives correct semantics with standard library quality. Save radix tree for a future optimization phase if needed.
- **Storing entry fields in Listpack for BTreeMap values:** Adds encoding/decoding overhead for every read. Use `Vec<(Bytes, Bytes)>` for field-value pairs. Listpack is better when you have a true radix tree with many entries sharing a prefix node.
- **Global stream lock for consumer groups:** Consumer group state is per-stream, accessed only under the database's existing single-writer model. No additional locking needed.
- **Mutable consumer group access through RedisValueRef:** Consumer group mutations (XACK, XCLAIM) need `&mut Stream`. Always use `as_redis_value_mut()` path.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Ordered ID storage | Custom radix tree | BTreeMap<StreamId, _> | Range queries, ordering, split/merge all built-in |
| ID monotonic guarantee | Manual clock tracking | Compare-and-set on last_id | Clock can go backward; always max(now_ms, last_id.ms) |
| PEL expiration scanning | Custom timer wheels | BTreeMap ordered by ID, linear scan with idle check | PEL entries are naturally ordered; XPENDING is a range query |
| Blocking wakeup | New channel system | Phase 17 BlockingRegistry + oneshot | Already proven, handles timeout, multi-key, cross-shard |

**Key insight:** Streams look like they need exotic data structures (radix tree), but BTreeMap + Vec provides the same semantics for this project's scale. The real complexity is in consumer group state management (PEL, delivery tracking, claim transfers), not in the storage layer.

## Common Pitfalls

### Pitfall 1: Entry ID Monotonic Guarantee Violation
**What goes wrong:** Clock goes backward (NTP adjustment), new XADD gets ID lower than last entry.
**Why it happens:** Using raw `SystemTime::now()` for millisecond component.
**How to avoid:** Always compute `ms = max(current_time_ms(), last_id.ms)`. If ms == last_id.ms, increment seq. If ms > last_id.ms, seq = 0.
**Warning signs:** Test with explicit IDs, then try auto-generated ID that should be higher.

### Pitfall 2: XREAD $ Semantics
**What goes wrong:** Using `$` as the initial stream ID reads from "now", missing entries added between XREAD calls.
**Why it happens:** `$` means "last ID in stream right now", not "beginning of time". After first XREAD, client must use the actual last-received ID.
**How to avoid:** `$` resolves to stream.last_id at parse time. Return the resolved ID to the client can track state.
**Warning signs:** Client reports missing messages.

### Pitfall 3: XREADGROUP > vs 0 Semantics
**What goes wrong:** Treating `>` and `0` the same way.
**Why it happens:** Both are special IDs but mean completely different things.
**How to avoid:** `>` means "new entries not yet delivered to any consumer in this group" (reads from last_delivered_id and adds to PEL). `0` (or any explicit ID) means "re-read my pending entries" (reads from PEL, does NOT add new entries).
**Warning signs:** Consumer group tests show entries delivered twice or PEL growing unbounded.

### Pitfall 4: XACK on Non-Pending Entry
**What goes wrong:** XACK called for an entry not in the PEL returns error or panics.
**Why it happens:** Entry may have been already ACKed, or claimed by another consumer.
**How to avoid:** XACK returns count of successfully acknowledged entries. Silently skip entries not in PEL (return 0 for that entry).
**Warning signs:** Error responses on valid XACK calls.

### Pitfall 5: Approximate Trimming (~) with MAXLEN
**What goes wrong:** Exact trimming on every XADD is O(N) and slow for large streams.
**Why it happens:** Must find and remove entries to meet exact MAXLEN.
**How to avoid:** With `~` prefix, trim in batches -- only remove when stream exceeds MAXLEN by more than ~10%. Redis uses radix tree node boundaries; with BTreeMap, trim when len > maxlen * 1.1 and remove entries until len <= maxlen.
**Warning signs:** XADD latency spikes on large streams with MAXLEN.

### Pitfall 6: Consumer Auto-Creation
**What goes wrong:** Consumer not found error on XREADGROUP.
**Why it happens:** Expecting consumers to be pre-created.
**How to avoid:** XREADGROUP auto-creates the consumer in the group if it doesn't exist. Only groups must be explicitly created with XGROUP CREATE.
**Warning signs:** "NOGROUP" errors when consumer name is new.

### Pitfall 7: XRANGE Exclusive/Inclusive Range Boundaries
**What goes wrong:** Returning entries at the boundary IDs incorrectly.
**Why it happens:** XRANGE uses inclusive ranges by default. The `-` and `+` special IDs mean minimum and maximum possible IDs.
**How to avoid:** `-` = StreamId::ZERO, `+` = StreamId::MAX. Both bounds are inclusive. COUNT limits the number of results returned.
**Warning signs:** Off-by-one in range query results.

### Pitfall 8: Empty Stream Deletion
**What goes wrong:** Deleting the key when stream has no entries but still has consumer groups.
**Why it happens:** Other data types (list, set) auto-delete on empty.
**How to avoid:** Streams persist even when empty (0 entries) because consumer groups and last_id metadata are valuable. Only explicit DEL removes a stream key.
**Warning signs:** Consumer group state lost after XTRIM removes all entries.

## Code Examples

### Auto-ID Generation
```rust
impl Stream {
    /// Generate the next auto-ID, ensuring monotonic increase.
    pub fn next_auto_id(&mut self) -> StreamId {
        let now_ms = current_time_ms();
        if now_ms > self.last_id.ms {
            let id = StreamId { ms: now_ms, seq: 0 };
            self.last_id = id;
            id
        } else {
            // Clock hasn't advanced (or went backward), increment sequence
            let id = StreamId { ms: self.last_id.ms, seq: self.last_id.seq + 1 };
            self.last_id = id;
            id
        }
    }

    /// Validate and accept an explicit ID (must be > last_id).
    pub fn validate_explicit_id(&self, id: StreamId) -> Result<StreamId, &'static str> {
        if id <= self.last_id {
            return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }
        Ok(id)
    }
}
```

### XADD Core Logic
```rust
pub fn xadd(db: &mut Database, args: &[Frame]) -> Frame {
    // Parse: XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] id field value [field value ...]
    // 1. Get or create stream
    // 2. Generate or validate ID
    // 3. Insert entry (field-value pairs)
    // 4. Apply trimming if MAXLEN/MINID specified
    // 5. Return the entry ID
}
```

### XRANGE Core Logic
```rust
pub fn xrange(db: &mut Database, args: &[Frame]) -> Frame {
    // Parse: XRANGE key start end [COUNT count]
    // start: "-" = StreamId::ZERO, or parse explicit ID
    // end: "+" = StreamId::MAX, or parse explicit ID
    // Use BTreeMap::range(start..=end) for efficient range query
    // Limit by COUNT if specified
    // Return array of [id, [field, value, ...]] entries
}
```

### Consumer Group Delivery (XREADGROUP with >)
```rust
impl Stream {
    pub fn read_group_new(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        count: Option<u64>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, Frame> {
        let group = self.groups.get_mut(group_name)
            .ok_or_else(|| Frame::Error(Bytes::from("NOGROUP ...")))?;

        // Auto-create consumer if needed
        let consumer = group.consumers
            .entry(consumer_name.clone())
            .or_insert_with(|| Consumer::new(consumer_name.clone()));
        consumer.seen_time = current_time_ms();

        // Read entries after last_delivered_id
        let start = StreamId { ms: group.last_delivered_id.ms, seq: group.last_delivered_id.seq + 1 };
        let entries: Vec<_> = self.entries.range(start..)
            .take(count.unwrap_or(u64::MAX) as usize)
            .map(|(id, fields)| (*id, fields.clone()))
            .collect();

        // Add to PEL and update last_delivered_id
        for (id, _) in &entries {
            group.pel.insert(*id, PendingEntry {
                consumer: consumer_name.clone(),
                delivery_time: current_time_ms(),
                delivery_count: 1,
            });
            consumer.pending.insert(*id, ());
            group.last_delivered_id = *id;
        }

        Ok(entries)
    }
}
```

### RDB Stream Serialization
```rust
// New type tag for persistence
pub const TYPE_STREAM: u8 = 5;

// Write: entry count, entries (id + field-value pairs), then consumer groups
// Read: reconstruct Stream struct with all metadata
pub fn write_stream(buf: &mut Vec<u8>, stream: &Stream) -> anyhow::Result<()> {
    // 1. Entry count (u64)
    // 2. Last generated ID (ms: u64, seq: u64)
    // 3. For each entry: id (ms: u64, seq: u64), field_count (u32), fields...
    // 4. Group count (u32)
    // 5. For each group: name, last_delivered_id, PEL entries, consumers
}
```

### Database Accessor Pattern
```rust
// In db.rs -- follow existing get_or_create pattern
impl Database {
    pub fn get_or_create_stream(&mut self, key: &[u8]) -> Result<&mut Stream, Frame> {
        // Expire check, create if missing, WRONGTYPE if wrong type
        // Return mutable reference to inner Stream
    }

    pub fn get_stream(&mut self, key: &[u8]) -> Result<Option<&Stream>, Frame> {
        // Read-only access with expire check
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| XADD without NOMKSTREAM | NOMKSTREAM option (Redis 6.2+) | 2021 | Prevents auto-creating stream key |
| XCLAIM manual scanning | XAUTOCLAIM (Redis 6.2+) | 2021 | SCAN-like auto-claim of idle pending entries |
| No MINID trimming | MINID trim strategy (Redis 6.2+) | 2021 | Trim by minimum ID instead of count |
| Exact trimming only | Approximate ~ trimming | Redis 5.0+ | Better XADD performance on large streams |

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) |
| Config file | Cargo.toml |
| Quick run command | `cargo test --lib stream` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| STREAM-01 | Stream entries in BTreeMap keyed by ID | unit | `cargo test --lib storage::stream` | Wave 0 |
| STREAM-02 | XADD/XLEN/XTRIM | unit+integration | `cargo test --lib command::stream::test_xadd` | Wave 0 |
| STREAM-03 | XRANGE/XREVRANGE with COUNT | unit | `cargo test --lib command::stream::test_xrange` | Wave 0 |
| STREAM-04 | XREAD with blocking | integration | `cargo test stream_blocking` | Wave 0 |
| STREAM-05 | Consumer groups XGROUP/XREADGROUP/XACK | unit+integration | `cargo test --lib command::stream::test_xreadgroup` | Wave 0 |
| STREAM-06 | XPENDING/XCLAIM for pending entries | unit | `cargo test --lib command::stream::test_xclaim` | Wave 0 |
| STREAM-07 | Stream persistence RDB+AOF | integration | `cargo test stream_persistence` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib stream`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/storage/stream.rs` -- Stream, StreamId, ConsumerGroup, PEL types + unit tests
- [ ] `src/command/stream.rs` -- all stream command handlers + unit tests
- [ ] Integration tests for XREAD BLOCK and persistence

## Open Questions

1. **Listpack vs Vec for entry field storage**
   - What we know: Redis uses listpack for delta-compressed entries within radix tree nodes. Our BTreeMap approach stores individual entries.
   - What's unclear: Whether memory overhead of Vec<(Bytes, Bytes)> per entry is acceptable.
   - Recommendation: Use Vec<(Bytes, Bytes)> for simplicity. Listpack encoding can be added later as optimization if memory profiling shows it's needed. The difference is small for typical stream entry sizes (few fields, short values).

2. **XDEL command**
   - What we know: Redis supports XDEL to remove specific entries from a stream. Not listed in CONTEXT.md commands.
   - What's unclear: Whether it should be included in this phase.
   - Recommendation: Include XDEL -- it's simple (BTreeMap::remove), and needed for PEL behavior (deleted entries return null in XREADGROUP pending replay).

3. **NOMKSTREAM option for XADD**
   - What we know: Redis 6.2+ supports NOMKSTREAM to prevent auto-creating the stream key.
   - What's unclear: Not explicitly listed but is part of modern XADD spec.
   - Recommendation: Include it -- trivial to implement (check key exists before creating).

## Sources

### Primary (HIGH confidence)
- Redis official documentation: [XADD](https://redis.io/docs/latest/commands/xadd/), [XREAD](https://redis.io/docs/latest/commands/xread/), [XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/), [XCLAIM](https://redis.io/docs/latest/commands/xclaim/), [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/)
- [Redis Streams documentation](https://redis.io/docs/latest/develop/data-types/streams/) -- stream data type overview, ID format, consumer groups
- Antirez blog: [Streams as pure data structure](https://antirez.com/news/128) -- internal architecture design decisions
- Project codebase: `src/storage/entry.rs`, `src/storage/compact_value.rs`, `src/storage/listpack.rs`, `src/blocking/mod.rs`, `src/blocking/wakeup.rs`

### Secondary (MEDIUM confidence)
- [Redis Internals - Streams](https://github.com/zpoint/Redis-Internals/blob/5.0/Object/streams/streams.md) -- detailed C implementation analysis
- [Dragonfly XREADGROUP docs](https://www.dragonflydb.io/docs/command-reference/stream/xreadgroup) -- behavioral reference

### Tertiary (LOW confidence)
- Memory usage estimates (17MB for 1M entries) -- from community sources, not verified independently

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - uses only existing project infrastructure, no new dependencies
- Architecture: HIGH - patterns follow established codebase conventions (RedisValue variant, get_or_create, CompactValue tags, wakeup hooks)
- Pitfalls: HIGH - based on official Redis documentation for command semantics
- Consumer groups: MEDIUM - complex state machine, verified against docs but implementation details require careful testing

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable domain, Redis Streams spec is mature)
