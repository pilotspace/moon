# Phase 15: B+ Tree Sorted Sets and Optimized Collection Encodings - Research

**Researched:** 2026-03-24
**Domain:** Custom data structures (B+ tree, listpack, intset) for memory-efficient Redis collection encodings
**Confidence:** HIGH

## Summary

This phase replaces BTreeMap-based sorted sets with a custom B+ tree achieving 2-3 bytes per-entry amortized overhead, and introduces compact encodings (listpack, intset) for small collections across all four collection types (hash, list, set, sorted set). The key engineering challenge is maintaining the existing command interface (2466 lines of sorted set commands, 1259 hash, 1325 list, 1542 set) while transparently dispatching to compact or full-size backends.

The current codebase stores all collections as heap-allocated `RedisValue` variants behind `CompactValue`'s tagged pointer scheme. The `Database` accessor methods (`get_or_create_hash`, `get_or_create_sorted_set`, etc.) return mutable references to the inner `HashMap`/`BTreeMap`/`HashSet`/`VecDeque` directly. This means the encoding upgrade path must be handled at the `Database` level, replacing the entry's `CompactValue` when a compact encoding exceeds its threshold.

Redis's proven defaults (128 entries, 64 bytes max element for listpack; 512 entries for intset) have been stable for years and should be adopted as-is.

**Primary recommendation:** Implement B+ tree as a standalone module in `src/storage/bptree.rs`, listpack as `src/storage/listpack.rs`, intset as `src/storage/intset.rs`. Extend `RedisValue` enum with compact variants. Update `Database` accessor methods to handle encoding dispatch and automatic upgrade.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Custom B+ tree with configurable fanout (order ~128 for cache-line-friendly nodes)
- Leaf nodes store (score, member) pairs packed sequentially -- cache-friendly iteration
- Internal nodes store separator scores + child pointers only
- Per-entry overhead target: 2-3 bytes amortized (vs BTreeMap's ~37 bytes with pointer overhead)
- Dual-index maintained: B+ tree for score-ordered access + HashMap for O(1) member->score lookup (same as current)
- Listpack = compact byte-array storing entries sequentially (Redis 7+ replacement for ziplist)
- Entry format: [encoding-byte][data][backlen] -- backlen enables reverse traversal without cascade updates
- Used for: small hashes (< 128 entries), small lists (< 128 elements), small sets (< 128 members), small sorted sets (< 128 members)
- Configurable thresholds via CONFIG SET (matching Redis's hash-max-listpack-entries, etc.)
- Intset: sorted array of integers with configurable encoding width (int16, int32, int64)
- Automatic width upgrade when a wider integer is added
- Binary search for membership check
- Convert to HashSet when a non-integer member is added or set exceeds size threshold
- Listpack -> full data structure when element count > 128 or single element > 64 bytes
- Intset -> HashSet when non-integer element added or count > 512
- Upgrade is one-way (no downgrade) -- matches Redis behavior
- Extend RedisValue enum with compact variants; recommended: separate variants HashSmall(ListPack) / Hash(HashMap) etc.

### Claude's Discretion
- B+ tree node size (target: fit in 2-3 cache lines = 128-192 bytes)
- Whether listpack is implemented as a separate crate or module within storage/
- OBJECT ENCODING command support (report "listpack", "intset", "skiplist" etc.)
- Whether to implement listpack reverse iteration (needed for LRANGE with negative indices)

### Deferred Ideas (OUT OF SCOPE)
- Quicklist (linked list of listpacks for large lists) -- evaluate if VecDeque is insufficient
- Radix tree for Streams -- Phase 18
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| ordered-float | 5.x | Score ordering in B+ tree | Already a dependency, required for f64 in BTreeMap-like structures |
| bytes | 1.10 | Member/key storage | Already core dependency throughout project |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| (none new) | - | - | All data structures are hand-rolled per CONTEXT.md decisions |

No new crate dependencies needed. The B+ tree, listpack, and intset are all custom implementations (matching Redis's approach of hand-rolled data structures).

## Architecture Patterns

### Recommended Project Structure
```
src/storage/
  bptree.rs        # B+ tree for sorted sets (score-ordered)
  listpack.rs      # Listpack compact encoding (byte array)
  intset.rs        # Integer set encoding (sorted array)
  entry.rs         # Extended RedisValue enum with compact variants
  compact_value.rs # New heap tags for compact variants
  db.rs            # Updated accessor methods with encoding dispatch
```

### Pattern 1: Extended RedisValue Enum

**What:** Add compact variants alongside existing full-size variants.
**When to use:** Always -- this is the core change enabling encoding dispatch.

```rust
pub enum RedisValue {
    String(Bytes),
    // Full-size variants (existing)
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet {
        members: HashMap<Bytes, f64>,
        scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
    // Compact variants (new)
    HashListpack(Listpack),
    ListListpack(Listpack),
    SetListpack(Listpack),
    SetIntset(Intset),
    SortedSetBPTree {
        tree: BPTree,
        members: HashMap<Bytes, f64>,  // Keep HashMap for O(1) member->score lookup
    },
    SortedSetListpack(Listpack),
}
```

### Pattern 2: Database Accessor Abstraction

**What:** The `Database::get_or_create_*` methods must handle both encodings transparently. The approach is to check encoding on access and convert if needed.
**When to use:** Every command handler already calls these methods.

```rust
// In db.rs -- sorted set accessor becomes an enum return
pub enum SortedSetRef<'a> {
    Full {
        members: &'a mut HashMap<Bytes, f64>,
        scores: &'a mut BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
    BPTree {
        tree: &'a mut BPTree,
        members: &'a mut HashMap<Bytes, f64>,
    },
    Listpack(&'a mut Listpack),
}
```

However, this requires changing all 30+ sorted set command handlers. A simpler approach:

**Recommended:** Keep the existing accessor signatures but perform automatic upgrade from listpack to full/BPTree encoding inside the accessor when the collection exceeds the threshold. Command handlers only see the full-size type. New collections start as listpack and get upgraded transparently. This minimizes changes to the 2466-line sorted_set.rs.

For the B+ tree specifically, the accessor should return `(&mut BPTree, &mut HashMap<Bytes, f64>)` replacing the current `(&mut HashMap<Bytes, f64>, &mut BTreeMap<...>)`. This requires updating sorted_set.rs to use B+ tree APIs instead of BTreeMap APIs.

### Pattern 3: B+ Tree Internal Design

**What:** Cache-line-friendly B+ tree with ~128 fanout.
**When to use:** For all sorted sets beyond the listpack threshold (>128 elements).

```rust
pub struct BPTree {
    root: NodeId,
    nodes: Vec<Node>,      // Arena-allocated nodes
    len: usize,
    height: usize,
    // Free list for node reuse
    free_list: Vec<NodeId>,
}

// Target node size: 128-192 bytes (2-3 cache lines)
// With f64 scores (8 bytes each), ~16 scores per internal node fits in 128 bytes
// Leaf nodes: (f64, member_index) pairs packed sequentially
const INTERNAL_FANOUT: usize = 16;  // 16 scores + 17 child pointers
const LEAF_CAPACITY: usize = 14;    // 14 (score, member_ref) pairs per leaf

enum Node {
    Internal {
        len: u16,
        keys: [f64; INTERNAL_FANOUT],           // separator scores
        children: [NodeId; INTERNAL_FANOUT + 1], // child node indices
    },
    Leaf {
        len: u16,
        entries: [(f64, Bytes); LEAF_CAPACITY],  // (score, member) pairs
        next: Option<NodeId>,                     // linked-list for iteration
        prev: Option<NodeId>,                     // for reverse iteration
    },
}
```

The per-entry overhead calculation:
- Each leaf stores ~14 entries in a node of ~192 bytes
- Node overhead per entry: 192/14 = ~13.7 bytes
- But the member data (Bytes) is the same regardless of container
- Container overhead = node metadata + pointers / entries = ~2-3 bytes amortized at high occupancy
- vs BTreeMap: each entry has 2 pointers (16 bytes) + BTreeMap node overhead (~21 bytes) = ~37 bytes

### Pattern 4: Listpack Format

**What:** Compact byte array for small collections, following Redis 7+ listpack format.
**When to use:** Collections with <= 128 elements where each element is <= 64 bytes.

```rust
pub struct Listpack {
    data: Vec<u8>,  // [tot_bytes:4][num_elements:2][entries...][0xFF]
}

// Entry format: [encoding_byte(s)][data][backlen]
// Encoding types:
// 0xxxxxxx          = 7-bit unsigned int (0-127)
// 10xxxxxx + data    = string up to 63 bytes
// 110xxxxx + 1 byte  = 13-bit signed int
// 1110xxxx + 2 bytes = string up to 4095 bytes
// 11110000 + 4 bytes = string with 32-bit length
// 11110001 + 2 bytes = 16-bit signed int
// 11110010 + 3 bytes = 24-bit signed int
// 11110011 + 4 bytes = 32-bit signed int
// 11110100 + 8 bytes = 64-bit signed int
// 11111111           = terminator

// Backlen: variable-length, each byte uses 7 data bits + 1 continuation bit
```

For hashes: field-value pairs stored alternating (field1, value1, field2, value2, ...).
For sorted sets: score-member pairs alternating (score1, member1, score2, member2, ...).
For lists: elements stored sequentially.
For sets: members stored sequentially.

### Pattern 5: Intset Format

**What:** Sorted array of integers with automatic width upgrade.
**When to use:** Sets where all members are valid integers and count <= 512.

```rust
pub struct Intset {
    encoding: IntsetEncoding,  // Int16, Int32, Int64
    data: Vec<u8>,             // sorted integers packed at encoding width
}

enum IntsetEncoding {
    Int16 = 2,
    Int32 = 4,
    Int64 = 8,
}
```

### Anti-Patterns to Avoid
- **Trait-based polymorphism for collection backends:** Adding a trait like `SortedSetBackend` with dynamic dispatch adds vtable overhead on every operation. Use enum dispatch instead -- it compiles to a branch which the CPU can predict.
- **Storing member strings inline in B+ tree leaf nodes:** Variable-length data in fixed-size nodes causes fragmentation. Store `Bytes` handles (which are already 3 words = 24 bytes) in leaf nodes; the actual string data lives in the Bytes allocator.
- **Downgrade from full to compact encoding:** Redis never downgrades (e.g., from HashMap back to listpack after removing elements). One-way upgrade simplifies the code enormously.
- **Modifying sorted_set.rs to handle both BTreeMap and BPTree:** Instead, replace BTreeMap entirely with BPTree for the full-size encoding. The command handlers should only see one full-size type.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| f64 ordering | Custom Ord impl for f64 | `ordered-float` crate (already dep) | NaN handling, total ordering edge cases |
| CRC32 for persistence | Manual CRC | `crc32fast` (already dep) | SIMD-accelerated, well-tested |

**Key insight:** For this phase, most things ARE hand-rolled by design. B+ tree, listpack, and intset are the core deliverables. The point is to use existing crates for orthogonal concerns.

## Common Pitfalls

### Pitfall 1: B+ Tree Dual-Index Consistency
**What goes wrong:** B+ tree and HashMap for sorted sets get out of sync during ZADD/ZREM, especially with score updates.
**Why it happens:** The current `zadd_member`/`zrem_member` helpers enforce BTreeMap+HashMap consistency. When replacing BTreeMap with BPTree, these helpers must be updated atomically.
**How to avoid:** The BPTree must expose the same insert/remove/range API surface. Update `zadd_member` and `zrem_member` to use BPTree instead of BTreeMap. All sorted set mutations MUST go through these two helpers -- never direct B+ tree access from command handlers.
**Warning signs:** ZSCORE returns a score but ZRANK returns nil for the same member.

### Pitfall 2: Listpack Entry Size Overflow During Update
**What goes wrong:** An HSET on a listpack-encoded hash updates a field value from "a" to a 100-byte string, exceeding the 64-byte element threshold but not the 128-count threshold.
**Why it happens:** Only checking count threshold, forgetting the per-element size threshold.
**How to avoid:** Check BOTH count and element size on every mutation. If either threshold is exceeded, upgrade to full encoding before performing the operation.
**Warning signs:** Listpack with elements > 64 bytes, leading to excessive memory usage for the compact format.

### Pitfall 3: Encoding Upgrade Losing Data
**What goes wrong:** Converting listpack to HashMap/BPTree drops entries due to incorrect iteration.
**Why it happens:** Listpack iteration is byte-level; off-by-one in backlen parsing causes data corruption.
**How to avoid:** Write thorough roundtrip tests: create listpack, fill to threshold, trigger upgrade, verify all entries present in full encoding. Use property-based testing if possible.
**Warning signs:** Element count mismatch after encoding upgrade.

### Pitfall 4: CompactValue Heap Tags Running Out of Bits
**What goes wrong:** Current CompactValue uses 3 low bits of the heap pointer for type tagging (values 0-4 for string/hash/list/set/zset). Adding new variants like HashListpack, SetIntset, etc. exceeds 8 possible tags.
**Why it happens:** 3 bits = 8 values, current uses 5 (0-4).
**How to avoid:** The compact variants are still stored as `RedisValue` enum on the heap -- the same `HEAP_TAG_HASH` (1) covers both `Hash(HashMap)` and `HashListpack(Listpack)` because the tag identifies the Redis type, not the encoding. The encoding is determined by matching the `RedisValue` variant. This means NO changes to `CompactValue` tagging are needed.
**Warning signs:** None -- this is a design confirmation, not a pitfall.

### Pitfall 5: RDB Serialization Must Handle Both Encodings
**What goes wrong:** Persistence code writes listpack/intset raw bytes instead of expanding to standard format, causing load failures.
**Why it happens:** Temptation to serialize compact encoding directly for efficiency.
**How to avoid:** For RDB, always serialize in the existing format (expand listpack/intset to their element-level representation). This maintains backward compatibility. Compact encodings are in-memory optimizations only.
**Warning signs:** RDB files that can't be loaded after restart.

### Pitfall 6: RedisValueRef Must Match New Variants
**What goes wrong:** `CompactValue::as_redis_value()` returns `RedisValueRef` which currently has no compact variants. Command handlers matching on `RedisValueRef::Hash(map)` won't see `HashListpack`.
**Why it happens:** RedisValueRef mirrors RedisValue but the command handlers only handle full-size types.
**How to avoid:** Two options: (1) add compact variants to RedisValueRef and update all command handlers, or (2) make the Database accessor methods always return the correct mutable reference (upgrading to full encoding before returning if needed for write operations, or implementing read operations on compact encoding directly). Option 2 is recommended for reads on small collections; option 1 requires touching all command files.

## Code Examples

### B+ Tree Insert (Core Operation)
```rust
impl BPTree {
    pub fn insert(&mut self, score: f64, member: Bytes) -> bool {
        if self.len == 0 {
            // Create root leaf node
            let leaf = self.alloc_leaf();
            self.nodes[leaf.0].as_leaf_mut().push(score, member);
            self.root = leaf;
            self.len = 1;
            self.height = 1;
            return true;
        }
        // Find leaf node for insertion
        let leaf_id = self.find_leaf(score, &member);
        let leaf = self.nodes[leaf_id.0].as_leaf_mut();
        if leaf.len() < LEAF_CAPACITY {
            leaf.insert_sorted(score, member);
            self.len += 1;
            return true;
        }
        // Split leaf and propagate
        self.split_and_insert(leaf_id, score, member);
        self.len += 1;
        true
    }

    /// Range iteration: yields (score, &member) in score order
    pub fn range(&self, min: f64, max: f64) -> BPTreeIter<'_> {
        let start_leaf = self.find_leaf_lower_bound(min);
        BPTreeIter {
            tree: self,
            current_leaf: start_leaf,
            index: 0,
            max_score: max,
        }
    }
}
```

### Listpack Operations
```rust
impl Listpack {
    pub fn new() -> Self {
        let mut data = Vec::with_capacity(7);
        // Header: tot_bytes(4) + num_elements(2) + terminator(1)
        data.extend_from_slice(&7u32.to_le_bytes());
        data.extend_from_slice(&0u16.to_le_bytes());
        data.push(0xFF);
        Listpack { data }
    }

    pub fn push_back(&mut self, value: &[u8]) {
        let pos = self.data.len() - 1; // before terminator
        let entry_bytes = encode_entry(value);
        self.data.splice(pos..pos, entry_bytes.iter().copied());
        self.increment_count();
        self.update_total_bytes();
    }

    pub fn len(&self) -> usize {
        u16::from_le_bytes([self.data[4], self.data[5]]) as usize
    }

    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter { data: &self.data, pos: 6 } // skip 6-byte header
    }

    pub fn iter_rev(&self) -> ListpackRevIter<'_> {
        // Start from byte before terminator, read backlen to find previous entry
        ListpackRevIter { data: &self.data, pos: self.data.len() - 1 }
    }
}
```

### Intset Operations
```rust
impl Intset {
    pub fn new() -> Self {
        Intset { encoding: IntsetEncoding::Int16, data: Vec::new() }
    }

    pub fn insert(&mut self, value: i64) -> bool {
        let needed_enc = encoding_for_value(value);
        if needed_enc as u8 > self.encoding as u8 {
            self.upgrade_encoding(needed_enc);
        }
        // Binary search for insertion point
        match self.binary_search(value) {
            Ok(_) => false, // already exists
            Err(pos) => {
                self.insert_at(pos, value);
                true
            }
        }
    }

    pub fn contains(&self, value: i64) -> bool {
        if encoding_for_value(value) as u8 > self.encoding as u8 {
            return false; // can't be in a smaller encoding
        }
        self.binary_search(value).is_ok()
    }
}
```

### Encoding Upgrade Example
```rust
// In Database::get_or_create_hash()
pub fn get_or_create_hash(&mut self, key: &[u8]) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
    // ... expiry check ...
    if !self.data.contains_key(key) {
        // New hashes start as listpack
        let entry = Entry::new_hash_listpack();
        // ...
    }
    let entry = self.data.get_mut(key).unwrap();
    match entry.value.as_redis_value_mut() {
        Some(RedisValue::Hash(map)) => Ok(map),
        Some(RedisValue::HashListpack(lp)) => {
            // For mutation: if caller will exceed threshold, upgrade now
            // But we can't know ahead of time what the caller will do.
            // Solution: return the HashMap after upgrading.
            let map = lp.to_hash_map();
            *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
            match entry.value.as_redis_value_mut() {
                Some(RedisValue::Hash(map)) => Ok(map),
                _ => unreachable!(),
            }
        }
        _ => Err(Self::wrongtype_error()),
    }
}
```

Note: The above pattern always upgrades on mutable access to a listpack-encoded hash. A more efficient approach would check thresholds, but this requires passing size hints to the accessor. The simpler approach: always start with listpack for new small collections, eagerly upgrade to full encoding when the accessor is called for mutation (since most collections that get mutated frequently will exceed the threshold quickly anyway). Alternatively, implement the core hash commands (HSET, HGET, etc.) to operate directly on the listpack for small collections.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Ziplist (cascade updates) | Listpack (backlen, no cascades) | Redis 7.0 (2022) | Eliminates O(N^2) worst case on updates |
| Skip list for sorted sets | Still skip list in Redis | Unchanged | Redis chose not to change; Dragonfly uses B+ tree |
| BTreeMap in Rust std | Custom B+ tree with arena allocation | This phase | 12-18x memory reduction per sorted set entry |

**Deprecated/outdated:**
- Ziplist: replaced by listpack in Redis 7.0; do NOT implement ziplist
- Redis's `zsl` (skip list): 37 bytes per entry; our B+ tree targets 2-3 bytes

## Open Questions

1. **B+ tree node sizing for variable-length members**
   - What we know: Fixed-size nodes work well for numeric keys (the `f64` scores). Members (Bytes) are variable-length handles (24 bytes on stack).
   - What's unclear: Whether to store Bytes directly in leaf nodes (24 bytes each, limiting capacity to ~6-7 per 192-byte node) or store member indices into a separate arena.
   - Recommendation: Store `Bytes` directly. At 6-7 entries per leaf, the overhead is ~30 bytes per entry from node metadata. This is still better than BTreeMap's 37 bytes, and much simpler. If profiling shows this is too high, switch to a member arena later.

2. **Listpack direct-access vs always-upgrade for mutations**
   - What we know: Redis operates directly on listpack for small collections, only upgrading at threshold.
   - What's unclear: How much code change is needed to make all 30+ hash/list/set/sorted-set commands work with both encodings.
   - Recommendation: Implement direct listpack operations for the most common commands (HSET/HGET for hash, LPUSH/RPUSH/LPOP/RPOP for list, SADD/SISMEMBER for set). For less common commands, upgrade to full encoding first. This gives the memory benefit for the common case while limiting code changes.

3. **OBJECT ENCODING command support**
   - What we know: Redis reports "listpack", "intset", "skiplist", "hashtable", "linkedlist", etc.
   - What's unclear: Whether to add this command in this phase or defer.
   - Recommendation: Add it -- it's trivial (match on RedisValue variant, return string) and extremely useful for testing/debugging encoding behavior.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) + integration tests with redis crate |
| Config file | Cargo.toml [dev-dependencies] |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| P15-01 | B+ tree <= 3 bytes overhead per entry | unit | `cargo test bptree::tests::test_memory_overhead -x` | No - Wave 0 |
| P15-02 | B+ tree sequential cache-friendly iteration | unit | `cargo test bptree::tests::test_range_iteration -x` | No - Wave 0 |
| P15-03 | Listpack for small collections <= 128 elements | unit | `cargo test listpack::tests -x` | No - Wave 0 |
| P15-04 | Intset for integer-only sets | unit | `cargo test intset::tests -x` | No - Wave 0 |
| P15-05 | Automatic encoding upgrade compact -> full | unit + integration | `cargo test encoding_upgrade -x` | No - Wave 0 |
| P15-06 | ZADD/ZRANGE/etc maintain O(log N) | unit | `cargo test bptree::tests::test_operations_logn -x` | No - Wave 0 |
| P15-07 | Memory reduction >= 10x for 1M sorted set entries | benchmark | `cargo bench entry_memory` | Partial (bench exists) |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before verification

### Wave 0 Gaps
- [ ] `src/storage/bptree.rs` -- B+ tree implementation with unit tests
- [ ] `src/storage/listpack.rs` -- Listpack implementation with unit tests
- [ ] `src/storage/intset.rs` -- Intset implementation with unit tests
- [ ] Integration tests for encoding upgrade paths

## Sources

### Primary (HIGH confidence)
- [Redis listpack specification](https://github.com/antirez/listpack/blob/master/listpack.md) -- Complete format specification with encoding types and backlen format
- [Redis listpack.c](https://github.com/redis/redis/blob/unstable/src/listpack.c) -- Reference C implementation
- [Redis intset.c](https://github.com/redis/redis/blob/unstable/src/intset.c) -- Reference C implementation with upgrade mechanism
- Project codebase -- entry.rs, compact_value.rs, sorted_set.rs, db.rs (direct code analysis)

### Secondary (MEDIUM confidence)
- [Smolderingly fast B-trees](https://www.scattered-thoughts.net/writing/smolderingly-fast-btrees/) -- Node sizing analysis (512 bytes sweet spot), linear vs binary search, SIMD non-benefit
- [Redis intset blog](http://blog.wjin.org/posts/redis-internal-data-structure-intset.html) -- Intset upgrade mechanism details
- [Listpack encoding for sets PR](https://github.com/redis/redis/pull/11290) -- Redis's listpack adoption for sets

### Tertiary (LOW confidence)
- [indexset crate](https://github.com/brurucy/indexset) -- Rust B-tree with order-statistic lookup (reference only, not recommended for use)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - no new dependencies needed, all structures are hand-rolled
- Architecture: HIGH - based on direct analysis of 6000+ lines of command code and existing storage layer
- B+ tree design: MEDIUM - node sizing and per-entry overhead targets need implementation validation
- Listpack/intset: HIGH - Redis format is well-documented with reference implementations
- Pitfalls: HIGH - identified from direct codebase analysis of accessor patterns and CompactValue tagging

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable domain, no fast-moving dependencies)
