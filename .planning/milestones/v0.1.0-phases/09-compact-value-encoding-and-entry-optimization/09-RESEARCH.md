# Phase 9: Compact Value Encoding and Entry Optimization - Research

**Researched:** 2026-03-24
**Domain:** Low-level memory layout optimization in Rust (SSO, tagged pointers, bit packing)
**Confidence:** HIGH

## Summary

This phase transforms the `Entry` struct from its current ~56-byte layout (RedisValue enum + u64 expires_at_ms + u64 metadata) into a compact 24-byte struct using three techniques: small-string optimization (SSO) for values <= 12 bytes, tagged pointers encoding value type in low bits, and embedded TTL as a 4-byte delta from a per-database base timestamp.

The current `Entry` struct contains `value: RedisValue` (an enum with 5 variants, each holding heap-allocated collections -- the enum itself is 72 bytes due to the SortedSet variant carrying two collections), `expires_at_ms: u64`, and `metadata: u64`. The RedisValue enum discriminant plus its largest variant (SortedSet with HashMap + BTreeMap) dominates size. The compact encoding replaces this with a 16-byte union-based `CompactValue` (inline for <= 12 bytes, heap pointer for larger), 4-byte TTL delta, and 4-byte packed metadata.

**Primary recommendation:** Implement CompactEntry as a new struct alongside Entry, migrate Database to use CompactEntry via accessor methods, then remove the old Entry. Use Rust enum with `#[repr(C)]` for the SSO discriminant rather than raw `union` (safer, still compact with careful layout). Add a `base_timestamp_secs: u32` field to Database for TTL delta computation.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- SSO: Values <= 12 bytes stored inline in a 16-byte struct: 4-byte length + 12 bytes data
- Values > 12 bytes: 4-byte length + 4-byte inline prefix (for fast comparison) + 8-byte heap pointer
- Tagged pointers: Encode value type in low 3 bits of heap pointer; type tags: 0=string, 1=hash, 2=list, 3=set, 4=zset, 5=stream (reserved), 6=integer (special fast path), 7=reserved
- For inline SSO values, use the high nibble of the length field as the type tag
- Embedded TTL: Replace `expires_at_ms: u64` with `ttl_delta: u32` (seconds from per-database base timestamp)
- 0 = no expiry (sentinel value); base timestamp stored once per Database struct
- Target 24 bytes total per entry: 16-byte compact value + 4-byte TTL delta + 4-byte packed metadata
- Packed metadata: last_access:16 | version:8 | access_counter:8 (fit in u32)
- `#[repr(C)]` for predictable layout and no padding waste
- Incremental migration: new CompactEntry wraps old Entry initially, then refactor Database to use CompactEntry directly
- All command handlers access values through accessor methods (not direct field access)
- Benchmark with 1M keys to validate >= 40% memory reduction

### Claude's Discretion
- Exact bit layout of the packed metadata u32 (may differ from Phase 7's u64 packing)
- Whether to use Rust `union` vs enum for SSO discriminant
- Cache-line alignment of the entry struct
- Whether integer fast-path (type tag 6) is worth implementing now or deferred

### Deferred Ideas (OUT OF SCOPE)
- Compact collection encodings (listpack, intset) -- Phase 15
- DashTable inline entry storage -- Phase 10
</user_constraints>

<phase_requirements>
## Phase Requirements

This phase has no formal requirement IDs from REQUIREMENTS.md (it is a performance optimization phase). The success criteria from the phase description serve as requirements:

| ID | Description | Research Support |
|----|-------------|-----------------|
| CVE-01 | Values <= 12 bytes stored inline (no heap allocation) via SSO | SSO union/enum design, CompactValue struct layout |
| CVE-02 | Value type encoded in low 3 bits of pointer or dedicated nibble | Tagged pointer technique, type tag constants |
| CVE-03 | TTL stored as 4-byte delta from base timestamp | Database base_timestamp field, conversion helpers |
| CVE-04 | Per-key memory overhead <= 24 bytes via Criterion benchmarks | CompactEntry struct at 24 bytes, benchmark strategy |
| CVE-05 | All existing tests pass; memory usage reduced >= 40% on 1M key benchmark | Migration strategy, accessor method refactoring |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| bytes | 1.10 | Heap-allocated byte buffers (existing) | Already in use; Bytes provides refcounted heap storage for large values |
| criterion | 0.8 | Memory benchmarking | Already in dev-dependencies; needed for 1M key memory measurement |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| static_assertions | 1.1 | Compile-time size assertions | Verify CompactEntry is exactly 24 bytes at compile time |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Rust enum for SSO | Raw `union` | Union is more flexible but requires `unsafe` for all access; enum with `#[repr(C)]` achieves same size with safe discriminant checking |
| `static_assertions` | `const` assert blocks | Rust 1.85 supports `const { assert!(...) }` blocks natively; no extra dep needed |

**Installation:**
```bash
# No new dependencies required -- criterion already present
# Use Rust's built-in const assertions instead of static_assertions
```

## Architecture Patterns

### Recommended Project Structure
```
src/
├── storage/
│   ├── entry.rs          # Current Entry (keep temporarily), add CompactEntry
│   ├── compact_value.rs  # NEW: CompactValue union/enum with SSO
│   ├── db.rs             # Database updated with base_timestamp, uses CompactEntry
│   └── eviction.rs       # Updated to use CompactEntry accessors
├── command/              # Updated to use accessor methods (not direct field access)
└── persistence/
    └── rdb.rs            # Updated serialization for CompactEntry
```

### Pattern 1: CompactValue with SSO (16 bytes)
**What:** A union-like enum encoding values inline for <= 12 bytes or as a tagged heap pointer for larger values.
**When to use:** Every value stored in the database.
**Example:**
```rust
// Recommended: enum approach with #[repr(C)] for predictable layout
#[repr(C)]
pub struct CompactValue {
    // Byte 0-3: length field
    //   For inline: high nibble = type tag, lower 28 bits = length (max 12)
    //   For heap: high nibble = 0xF (heap marker), lower 28 bits = full length
    len_and_tag: u32,
    // Bytes 4-15: payload
    //   For inline (len <= 12): 12 bytes of data
    //   For heap: 4-byte inline prefix + 8-byte pointer (with type in low 3 bits)
    payload: [u8; 12],
}

// Size check
const _: () = assert!(std::mem::size_of::<CompactValue>() == 16);
```

### Pattern 2: Tagged Heap Pointer
**What:** Store value type in the low 3 bits of heap pointers, exploiting the fact that allocators return 8-byte-aligned pointers (low 3 bits are always zero).
**When to use:** Values > 12 bytes that require heap allocation.
**Example:**
```rust
impl CompactValue {
    // Pack type tag into low 3 bits of pointer
    fn pack_heap_ptr(ptr: *const u8, type_tag: u8) -> usize {
        (ptr as usize) | (type_tag as usize & 0x7)
    }

    // Extract raw pointer (clear low 3 bits)
    fn unpack_heap_ptr(packed: usize) -> *const u8 {
        (packed & !0x7) as *const u8
    }

    // Extract type tag from packed pointer
    fn type_tag_from_ptr(packed: usize) -> u8 {
        (packed & 0x7) as u8
    }
}
```

### Pattern 3: TTL Delta Encoding
**What:** Store TTL as seconds offset from a per-database base timestamp instead of absolute milliseconds.
**When to use:** All entries with expiration.
**Example:**
```rust
impl Database {
    pub fn base_timestamp_secs(&self) -> u32 {
        self.base_timestamp
    }

    /// Convert absolute milliseconds TTL to delta seconds from base.
    pub fn encode_ttl(&self, expires_at_ms: u64) -> u32 {
        if expires_at_ms == 0 { return 0; }
        let expires_secs = (expires_at_ms / 1000) as u32;
        expires_secs.saturating_sub(self.base_timestamp)
    }

    /// Convert delta seconds back to absolute milliseconds.
    pub fn decode_ttl(&self, delta: u32) -> u64 {
        if delta == 0 { return 0; }
        ((self.base_timestamp as u64) + (delta as u64)) * 1000
    }
}
```

### Pattern 4: Packed Metadata u32
**What:** Pack last_access, version, and access_counter into a single u32.
**When to use:** Every CompactEntry.
**Example:**
```rust
// Layout: [last_access:16 | version:8 | access_counter:8]
// last_access: 16-bit relative seconds (wraps every ~18 hours, sufficient for LRU comparison)
// version: 8-bit (wraps at 255, sufficient for WATCH optimistic locking)
// access_counter: 8-bit Morris counter (same as current)
#[inline]
fn pack_metadata_u32(last_access: u16, version: u8, access_counter: u8) -> u32 {
    ((last_access as u32) << 16) | ((version as u32) << 8) | (access_counter as u32)
}
```

### Pattern 5: CompactEntry (24 bytes total)
**What:** The final entry struct.
**Example:**
```rust
#[repr(C)]
pub struct CompactEntry {
    pub value: CompactValue,   // 16 bytes
    pub ttl_delta: u32,        // 4 bytes (0 = no expiry)
    pub metadata: u32,         // 4 bytes packed [last_access:16|version:8|counter:8]
}

const _: () = assert!(std::mem::size_of::<CompactEntry>() == 24);
```

### Anti-Patterns to Avoid
- **Direct field access to `entry.value` or `entry.expires_at_ms`:** Currently ~50 call sites access these directly. All must go through accessor methods to support the new encoding.
- **Storing absolute timestamps in the entry:** The whole point is to save 4 bytes by using a delta. Never store `u64` timestamps in the entry itself.
- **Using `unsafe` union without `ManuallyDrop`:** If using raw union for SSO, all heap-owning variants MUST use `ManuallyDrop<T>` to prevent double-free. Prefer enum approach.
- **Forgetting to update `estimate_memory()`:** The eviction engine relies on accurate memory estimates. CompactEntry must report its actual overhead (24 bytes base + heap allocation if any).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Memory benchmarking | Custom allocation tracking | Criterion + `std::alloc::GlobalAlloc` wrapper | Criterion provides statistical rigor; custom allocator wrapper counts actual allocations |
| Compile-time size checks | Runtime assertions | `const _: () = assert!(size_of::<T>() == N);` | Catches size regressions at compile time, zero runtime cost |
| Integer formatting in values | Custom integer parser | `atoi` crate or `str::parse` | For integer fast-path, parsing correctness matters more than micro-optimization here |

**Key insight:** The complexity in this phase is in the data layout transformation and the migration of ~50 call sites. The actual data structures are simple bit manipulation -- don't over-engineer the encoding.

## Common Pitfalls

### Pitfall 1: Rust Enum Size Inflation
**What goes wrong:** A naive Rust enum for CompactValue will be larger than 16 bytes because the compiler adds a discriminant and alignment padding. For example, `enum CompactValue { Inline([u8; 12], u32), Heap(*const u8, u32, u32) }` could be 24 bytes.
**Why it happens:** Rust's enum discriminant takes at least 1 byte, and alignment rules add padding.
**How to avoid:** Use `#[repr(C)]` on a struct with a single `len_and_tag: u32` + `payload: [u8; 12]` layout. Encode the inline/heap discriminant INTO the `len_and_tag` field (e.g., high nibble = 0xF means heap). Interpret `payload` differently based on the discriminant. This avoids a separate discriminant byte.
**Warning signs:** `std::mem::size_of::<CompactValue>() > 16` at compile time.

### Pitfall 2: Drop Implementation for Heap Values
**What goes wrong:** When a CompactValue with a heap pointer is dropped, the heap allocation must be freed. Missing this causes memory leaks. Implementing it incorrectly causes double-frees.
**Why it happens:** The struct looks like plain data (`[u8; 12]` payload) but hides a raw pointer.
**How to avoid:** Implement `Drop` for CompactValue that checks the discriminant; if it's a heap value, reconstruct the original heap-allocated type (e.g., `Box<[u8]>`, `Box<HashMap>`) from the raw pointer and drop it. Use `ManuallyDrop` in the conversion path.
**Warning signs:** Memory leaks in long-running tests; Miri violations.

### Pitfall 3: TTL Delta Overflow and Base Timestamp Drift
**What goes wrong:** If `base_timestamp` is far in the past and a new TTL is set far in the future, the delta could overflow u32. If base_timestamp is never updated, deltas grow large.
**Why it happens:** u32 seconds only covers ~136 years total, but the delta must cover the range `[base_timestamp, expires_at]`.
**How to avoid:** Periodically update `base_timestamp` (e.g., once per hour or at startup). When encoding a TTL, if the delta would overflow, either update the base timestamp and re-encode all entries' deltas, or fall back to storing a special "absolute" marker. In practice, since base_timestamp starts at server startup time and TTLs rarely exceed hours/days, overflow is not a real concern. Still, add a saturating_sub to be safe.
**Warning signs:** TTL returning wrong values after server runs for extended periods.

### Pitfall 4: Metadata u32 Reduces last_access and version Precision
**What goes wrong:** Current metadata packs last_access as 32-bit epoch seconds and version as 24-bit. The new u32 packing gives last_access only 16 bits (wraps every ~18 hours) and version only 8 bits (wraps at 255).
**Why it happens:** Fitting 3 fields into 4 bytes instead of 8 bytes halves the available bits.
**How to avoid:** For last_access: 16-bit relative seconds is fine for LRU comparison (only relative ordering matters; update base periodically). For version: 8-bit wrapping is acceptable for WATCH (version conflicts are detected within a single transaction, not across days). Document these reduced ranges clearly.
**Warning signs:** WATCH tests failing due to version wrap-around in pathological cases.

### Pitfall 5: Massive Migration Surface (50+ Call Sites)
**What goes wrong:** Changing Entry struct breaks every file that reads `entry.value`, `entry.expires_at_ms`, or constructs Entry directly.
**Why it happens:** The current code directly accesses pub fields across 10+ files.
**How to avoid:** Phase the migration: (1) Add CompactEntry alongside Entry with conversion methods. (2) Add accessor methods to Entry that mirror CompactEntry's API. (3) Migrate call sites to use accessors. (4) Swap Entry for CompactEntry. (5) Remove old Entry.
**Warning signs:** Compilation errors in 50+ locations at once.

### Pitfall 6: Collection Types Don't Benefit from SSO
**What goes wrong:** Hash, List, Set, SortedSet values are heap-allocated collections. SSO only benefits String values <= 12 bytes. Someone might expect 40% savings across all value types.
**Why it happens:** SSO is specifically for small strings. Collections always require heap pointers in CompactValue.
**How to avoid:** Set realistic expectations: the 40% savings target applies to a workload dominated by small string values (counters, flags, short tokens), which is the typical Redis workload. Benchmark with a realistic mix (80% strings, 20% collections).
**Warning signs:** Benchmark shows < 40% reduction when using only collection types.

## Code Examples

### Current Entry Size Analysis
```rust
// Current layout (verified from source):
// Entry {
//     value: RedisValue,        // 72 bytes (enum, sized to largest variant SortedSet)
//     expires_at_ms: u64,       // 8 bytes
//     metadata: u64,            // 8 bytes
// }
// Total: 88 bytes (with alignment)
// Plus HashMap overhead per key in Database: ~80 bytes
// Effective per-key: ~168 bytes

// Target CompactEntry: 24 bytes
// Plus compact HashMap overhead: ~80 bytes (unchanged, HashMap is in Database)
// Effective per-key: ~104 bytes (38% reduction on struct alone)
// With SSO avoiding Bytes heap alloc for small values: saves additional ~32 bytes per Bytes
// Effective for small strings: ~72 bytes vs ~168 = ~57% reduction
```

### CompactValue Implementation Skeleton
```rust
use std::ptr;

const HEAP_MARKER: u32 = 0xF0000000;
const TYPE_MASK: u32 = 0xF0000000;
const LEN_MASK: u32 = 0x0FFFFFFF;
const SSO_MAX_LEN: usize = 12;

// Type tags for inline (high nibble of len_and_tag)
const TAG_STRING: u32 = 0x00000000;  // 0 in high nibble
const TAG_INTEGER: u32 = 0x60000000; // 6 in high nibble

// Type tags for heap (low 3 bits of pointer)
const HEAP_TAG_STRING: usize = 0;
const HEAP_TAG_HASH: usize = 1;
const HEAP_TAG_LIST: usize = 2;
const HEAP_TAG_SET: usize = 3;
const HEAP_TAG_ZSET: usize = 4;

#[repr(C)]
pub struct CompactValue {
    len_and_tag: u32,
    payload: [u8; 12],
}

impl CompactValue {
    /// Create an inline string value (len <= 12).
    pub fn inline_string(data: &[u8]) -> Self {
        debug_assert!(data.len() <= SSO_MAX_LEN);
        let mut payload = [0u8; 12];
        payload[..data.len()].copy_from_slice(data);
        CompactValue {
            len_and_tag: TAG_STRING | (data.len() as u32),
            payload,
        }
    }

    /// Check if this value is stored inline (SSO).
    #[inline]
    pub fn is_inline(&self) -> bool {
        (self.len_and_tag & TYPE_MASK) != HEAP_MARKER
    }

    /// Get the value as bytes (for string values).
    pub fn as_bytes(&self) -> &[u8] {
        if self.is_inline() {
            let len = (self.len_and_tag & LEN_MASK) as usize;
            &self.payload[..len]
        } else {
            // Reconstruct from heap pointer
            let packed = usize::from_ne_bytes(self.payload[4..12].try_into().unwrap());
            let ptr = (packed & !0x7) as *const u8;
            let len = (self.len_and_tag & LEN_MASK) as usize;
            unsafe { std::slice::from_raw_parts(ptr, len) }
        }
    }
}
```

### TTL Delta Conversion
```rust
impl CompactEntry {
    /// Check if expired given the database base timestamp and current time.
    pub fn is_expired_at(&self, base_ts: u32, now_ms: u64) -> bool {
        if self.ttl_delta == 0 { return false; }
        let expires_ms = ((base_ts as u64) + (self.ttl_delta as u64)) * 1000;
        now_ms >= expires_ms
    }

    /// Get absolute expiry in milliseconds (for TTL/PTTL commands).
    pub fn expires_at_ms(&self, base_ts: u32) -> u64 {
        if self.ttl_delta == 0 { return 0; }
        ((base_ts as u64) + (self.ttl_delta as u64)) * 1000
    }

    /// Set expiry from absolute milliseconds.
    pub fn set_expires_at_ms(&mut self, base_ts: u32, ms: u64) {
        if ms == 0 {
            self.ttl_delta = 0;
        } else {
            let secs = (ms / 1000) as u32;
            self.ttl_delta = secs.saturating_sub(base_ts);
            // Ensure non-zero delta for non-zero expiry
            if self.ttl_delta == 0 && ms > 0 {
                self.ttl_delta = 1;
            }
        }
    }
}
```

### Memory Benchmark Pattern
```rust
// benches/entry_memory.rs
use criterion::{criterion_group, criterion_main, Criterion};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct TrackingAllocator;
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn bench_memory_1m_keys(c: &mut Criterion) {
    c.bench_function("1M small string entries memory", |b| {
        b.iter(|| {
            let before = ALLOCATED.load(Ordering::Relaxed);
            let mut entries = Vec::with_capacity(1_000_000);
            for i in 0..1_000_000u64 {
                // Small value: 8-byte counter string
                entries.push(CompactEntry::new_string(
                    format!("{}", i).as_bytes()
                ));
            }
            let after = ALLOCATED.load(Ordering::Relaxed);
            let per_entry = (after - before) / 1_000_000;
            assert!(per_entry <= 24, "Per-entry overhead {} > 24", per_entry);
            entries
        });
    });
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `Entry { value: RedisValue, expires_at_ms: u64, metadata: u64 }` | CompactEntry with SSO + tagged ptrs + delta TTL | Phase 9 | ~57-71% per-key reduction for small strings |
| u64 metadata packing (32+24+8 bits) | u32 metadata packing (16+8+8 bits) | Phase 9 | Halves metadata field, reduces precision (acceptable) |
| Absolute ms TTL (u64) | Delta seconds TTL from base (u32) | Phase 9 | Halves TTL field, loses millisecond precision (acceptable) |

**Key technique references:**
- Redis SDS (Simple Dynamic Strings): SSO for strings <= 44 bytes using sdshdr5
- Dragonfly CompactObject: 18-byte per-entry with inline small values
- Rust `smallvec` / `smallstr`: Prior art for Rust SSO patterns (stack-allocated small buffers)

## Open Questions

1. **Exact heap value representation for collections**
   - What we know: Collections (Hash, List, Set, SortedSet) always go through the heap path since they're never <= 12 bytes. The tagged pointer stores their type.
   - What's unclear: Should the heap pointer point to a `Box<HashMap<Bytes, Bytes>>`, or should we wrap in a new `HeapValue` enum? The former requires unsafe reconstruction from raw pointer. The latter adds indirection.
   - Recommendation: Use `Box<RedisValue>` for the heap path. The existing RedisValue enum already handles all collection types. Store `Box<RedisValue>` as raw pointer with type tag in low bits. On access, reconstruct `&RedisValue` via pointer arithmetic. This minimizes code changes in command handlers.

2. **Integer fast-path (type tag 6)**
   - What we know: Storing integers as native i64 in the 12-byte inline payload avoids string parsing for INCR/DECR.
   - What's unclear: How much performance gain this provides vs. complexity of dual representation.
   - Recommendation: Defer to a sub-task. Implement basic SSO first, add integer fast-path as an optional enhancement. The INCR/DECR hot path currently parses `Bytes` to i64 which is already fast with `atoi`.

3. **RDB format compatibility**
   - What we know: RDB serialization directly accesses `entry.value` and `entry.expires_at_ms`. The on-disk format must remain stable.
   - What's unclear: Whether to bump RDB_VERSION.
   - Recommendation: No RDB format change needed. The serialization layer converts CompactEntry back to RedisValue for serialization. The on-disk format is independent of in-memory layout.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test + Criterion 0.8 |
| Config file | Cargo.toml `[[bench]]` section |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CVE-01 | Inline SSO for <= 12 byte values | unit | `cargo test --lib storage::entry::tests::test_compact_value_inline -x` | Wave 0 |
| CVE-02 | Tagged pointer type encoding | unit | `cargo test --lib storage::entry::tests::test_tagged_pointer -x` | Wave 0 |
| CVE-03 | TTL delta encoding/decoding | unit | `cargo test --lib storage::entry::tests::test_ttl_delta -x` | Wave 0 |
| CVE-04 | CompactEntry is 24 bytes | unit (compile-time) | `cargo test --lib storage::entry::tests::test_compact_entry_size -x` | Wave 0 |
| CVE-05 | All existing tests pass | integration | `cargo test` | Existing |
| CVE-05 | >= 40% memory reduction on 1M keys | bench | `cargo bench --bench entry_memory` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green + memory benchmark showing <= 24 bytes/entry

### Wave 0 Gaps
- [ ] `src/storage/compact_value.rs` -- CompactValue struct with SSO (new file)
- [ ] `benches/entry_memory.rs` -- 1M key memory benchmark
- [ ] Compile-time size assertions for CompactEntry = 24 bytes

## Sources

### Primary (HIGH confidence)
- Project source code: `src/storage/entry.rs`, `src/storage/db.rs`, `src/storage/eviction.rs`, `src/command/string.rs`, `src/persistence/rdb.rs` -- direct analysis of current implementation
- `.planning/architect-blue-print.md` -- 16-byte compact value encoding spec, SSO details, tagged pointer scheme
- Rust Reference: `#[repr(C)]` layout guarantees, union semantics -- verified from Rust 1.85 edition 2024

### Secondary (MEDIUM confidence)
- Redis SDS implementation: SSO patterns in C string handling (well-documented in Redis source)
- Dragonfly CompactObject: 18-byte per-entry design (documented in Dragonfly source and design docs)

### Tertiary (LOW confidence)
- Exact memory overhead of `HashMap<Bytes, Entry>` per entry in hashbrown (estimated ~80 bytes based on load factor and bucket size; needs benchmarking to verify)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, using existing Rust features
- Architecture: HIGH -- SSO and tagged pointers are well-understood techniques; current code thoroughly analyzed
- Pitfalls: HIGH -- identified from direct code analysis of 50+ call sites and Rust-specific unsafe concerns

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable Rust techniques, no external dependency changes expected)
