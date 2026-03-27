# Phase 3: Collection Data Types - Research

**Researched:** 2026-03-23
**Domain:** Redis collection data types (Hash, List, Set, Sorted Set), active expiration, SCAN, AUTH, UNLINK
**Confidence:** HIGH

## Summary

Phase 3 extends the existing single-variant `RedisValue::String` enum with four collection types (Hash, List, Set, Sorted Set) and adds infrastructure features (active expiration, SCAN cursor iteration, AUTH, UNLINK). The codebase already has a clean handler pattern (`pub fn cmd(db: &mut Database, args: &[Frame]) -> Frame`) with shared helpers (`err_wrong_args`, `extract_bytes`/`extract_key`, `parse_int`), a glob matcher for pattern filtering, and a `CancellationToken`-based shutdown mechanism. All new work follows established patterns.

The primary complexity is in sorted sets (dual data structure with `BTreeMap` + `HashMap`) and the active expiration background task (spawned in listener, uses `Arc<Mutex<Vec<Database>>>`). Two new crate dependencies are needed: `ordered-float` for `BTreeMap` key ordering and `rand` for probabilistic sampling. Everything else uses the standard library.

**Primary recommendation:** Implement data types bottom-up (entry.rs variants -> Database helpers -> command handlers -> dispatch wiring -> tests), with Hash and List first (simpler), then Set, then Sorted Set (most complex). Wire AUTH and active expiration as cross-cutting infrastructure. Reuse `glob_match` for all SCAN MATCH filtering.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Extend `RedisValue` enum with 4 new variants:
  - `Hash(HashMap<Bytes, Bytes>)` -- field-value pairs
  - `List(VecDeque<Bytes>)` -- O(1) push/pop at both ends, better cache locality than LinkedList
  - `Set(HashSet<Bytes>)` -- unique members
  - `SortedSet { members: HashMap<Bytes, f64>, scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()> }` -- dual structure for O(log n) range queries + O(1) score lookup
- Type checking enforced: commands on wrong type return `WRONGTYPE Operation against a key holding the wrong kind of value`
- Each collection type gets its own command module file (command/hash.rs, command/list.rs, command/set.rs, command/sorted_set.rs)
- BTreeMap keyed by `(OrderedFloat<f64>, Bytes)` for sorted sets
- Score ties broken by lexicographic ordering of member bytes
- Accept +inf/-inf as valid scores, reject NaN with error
- ZRANGE with BYSCORE/BYLEX/REV/LIMIT uses BTreeMap range queries
- OrderedFloat from the `ordered-float` crate
- Tokio spawned background task running every 100ms for active expiration
- Redis algorithm: sample 20 random keys, delete expired, repeat if >25% expired (1ms budget)
- Random sampling via `rand` crate
- Background task holds `Arc<Mutex<Vec<Database>>>` -- same shared state
- Task uses `tokio::time::interval` with graceful shutdown via CancellationToken
- Stateless SCAN cursor -- position index, client resends each call
- COUNT hint controls approximate batch size (default 10)
- MATCH pattern applied as post-filter; TYPE filter as post-filter
- HSCAN/SSCAN/ZSCAN follow same pattern
- Per-connection `authenticated: bool` in Connection (default true if no password set)
- AUTH command checks password, sets `authenticated = true`
- `requirepass: Option<String>` in ServerConfig and CLI args
- UNLINK removes key from HashMap immediately, defers value drop to `tokio::task::spawn_blocking`
- For small values, UNLINK behaves identically to DEL

### Claude's Discretion
- Threshold for UNLINK async vs sync drop (e.g., collections > 64 elements)
- Exact SCAN cursor encoding (integer index vs hash-based)
- Internal helper methods on Database for type-checked access
- Test organization for the 4 new command modules
- Whether to add `ordered-float` or implement a wrapper type

### Deferred Ideas (OUT OF SCOPE)
- Blocking commands (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX) -- Phase 5 or v2
- Memory-efficient encodings (ziplist/listpack equivalents) -- v2
- Skip list replacement for BTreeMap in sorted sets -- optimize if benchmarks warrant
- OBJECT ENCODING command -- v2
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CONN-06 | AUTH with single password (requirepass) | Add `requirepass` to ServerConfig, `authenticated` to Connection, intercept dispatch |
| KEY-07 | SCAN cursor-based key iteration | Stateless cursor over HashMap keys, glob_match for MATCH, TYPE post-filter |
| KEY-09 | UNLINK async non-blocking delete | Remove from HashMap immediately, spawn_blocking to drop large values |
| EXP-02 | Active expiration (background sampling) | Tokio task in listener, rand sampling, CancellationToken shutdown |
| HASH-01 | HSET/HGET/HDEL | New RedisValue::Hash variant, type-checked access helpers |
| HASH-02 | HMSET/HMGET | Batch operations on Hash variant |
| HASH-03 | HGETALL | Iterate HashMap, return alternating field/value array |
| HASH-04 | HEXISTS/HLEN | Check/count on Hash internals |
| HASH-05 | HKEYS/HVALS | Key/value iterators on Hash |
| HASH-06 | HINCRBY/HINCRBYFLOAT | Parse field value as number, increment, store back |
| HASH-07 | HSETNX | Conditional field insert |
| HASH-08 | HSCAN | Cursor iteration over hash fields, shared SCAN logic |
| LIST-01 | LPUSH/RPUSH/LPOP/RPOP | VecDeque push_front/push_back/pop_front/pop_back |
| LIST-02 | LLEN/LRANGE/LINDEX | VecDeque len, slice iteration, index access |
| LIST-03 | LSET/LINSERT | Index assignment, linear search + insert |
| LIST-04 | LREM | Linear scan removing matching elements |
| LIST-05 | LTRIM | VecDeque drain outside range |
| LIST-06 | LPOS | Linear search for element position |
| SET-01 | SADD/SREM/SMEMBERS/SCARD | HashSet insert/remove/iter/len |
| SET-02 | SISMEMBER/SMISMEMBER | HashSet contains, batch membership check |
| SET-03 | SINTER/SUNION/SDIFF | Set algebra using HashSet intersection/union/difference |
| SET-04 | SINTERSTORE/SUNIONSTORE/SDIFFSTORE | Store set algebra results in new key |
| SET-05 | SRANDMEMBER/SPOP | Random element from HashSet via rand |
| SET-06 | SSCAN | Cursor iteration over set members |
| ZSET-01 | ZADD/ZREM/ZSCORE/ZCARD | Dual structure insert/remove, HashMap lookup, len |
| ZSET-02 | ZRANGE with BYSCORE/BYLEX/REV/LIMIT | BTreeMap range queries with ordered iteration |
| ZSET-03 | ZRANGEBYSCORE/ZREVRANGEBYSCORE | Legacy range queries mapping to BTreeMap ranges |
| ZSET-04 | ZRANK/ZREVRANK | Linear count of elements before target in BTreeMap |
| ZSET-05 | ZINCRBY | Atomic score increment: remove old entry, insert new |
| ZSET-06 | ZCOUNT/ZLEXCOUNT | Count elements in score/lex range via BTreeMap |
| ZSET-07 | ZUNIONSTORE/ZINTERSTORE | Multi-key aggregation with SUM/MIN/MAX weights |
| ZSET-08 | ZPOPMIN/ZPOPMAX | BTreeMap first/last entry removal |
| ZSET-09 | ZSCAN | Cursor iteration over sorted set members |
</phase_requirements>

## Standard Stack

### Core (existing)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| bytes | 1.10 | Binary-safe string payloads | Already in use, Bytes for all values |
| tokio | 1.x | Async runtime, timers, spawn_blocking | Already in use |
| tokio-util | 0.7 | CancellationToken, codec | Already in use |
| clap | 4.x | CLI args (extend for requirepass) | Already in use |
| tracing | 0.1 | Logging for expiration stats | Already in use |

### New Dependencies
| Library | Version | Purpose | Why Needed |
|---------|---------|---------|------------|
| ordered-float | 5.1 | OrderedFloat<f64> for BTreeMap keys in sorted sets | f64 does not impl Ord; needed for `BTreeMap<(OrderedFloat<f64>, Bytes), ()>` |
| rand | 0.10 | Random sampling for active expiration + SRANDMEMBER/SPOP | Probabilistic key sampling, random set element selection |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ordered-float | Custom wrapper struct | More code, same result; crate is well-maintained, 0 transitive deps |
| rand | fastrand | Lighter but lacks slice sampling; rand 0.10 is standard |
| VecDeque (List) | LinkedList | LinkedList has poor cache locality, VecDeque is strictly better for Redis list ops |
| BTreeMap (SortedSet) | Skip list | BTreeMap is stdlib, proven; skip list only if benchmarks warrant (deferred) |

**Installation:**
```bash
cargo add ordered-float@5.1 rand@0.10
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  command/
    mod.rs            # Dispatch (extend match arms)
    connection.rs     # PING/ECHO/QUIT/SELECT/COMMAND/INFO + AUTH
    key.rs            # DEL/EXISTS/EXPIRE/TTL/KEYS/RENAME + SCAN/UNLINK
    string.rs         # (existing, unchanged)
    hash.rs           # NEW: HSET/HGET/HDEL/HMSET/HMGET/HGETALL/HEXISTS/HLEN/HKEYS/HVALS/HINCRBY/HINCRBYFLOAT/HSETNX/HSCAN
    list.rs           # NEW: LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX/LSET/LINSERT/LREM/LTRIM/LPOS
    set.rs            # NEW: SADD/SREM/SMEMBERS/SCARD/SISMEMBER/SMISMEMBER/SINTER/SUNION/SDIFF/SINTERSTORE/SUNIONSTORE/SDIFFSTORE/SRANDMEMBER/SPOP/SSCAN
    sorted_set.rs     # NEW: ZADD/ZREM/ZSCORE/ZCARD/ZRANGE/ZREVRANGE/ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANK/ZREVRANK/ZINCRBY/ZCOUNT/ZLEXCOUNT/ZUNIONSTORE/ZINTERSTORE/ZPOPMIN/ZPOPMAX/ZSCAN
  storage/
    entry.rs          # RedisValue enum (extend with 4 variants)
    db.rs             # Database (add type-checked accessors, expose data for SCAN/expiration)
    mod.rs
  server/
    listener.rs       # Spawn active expiration task, pass ServerConfig for requirepass
    connection.rs     # Add authenticated field, AUTH check before dispatch
    expiration.rs     # NEW: Active expiration background task
    mod.rs
  config.rs           # Add requirepass: Option<String>
```

### Pattern 1: Type-Checked Database Access
**What:** Helper methods on Database that extract a specific RedisValue variant with WRONGTYPE error handling.
**When to use:** Every collection command needs to get-or-create the right type.
**Example:**
```rust
// In db.rs -- add type-checked access helpers
impl Database {
    /// Get hash value, creating empty hash if key doesn't exist.
    /// Returns Err frame if key exists with wrong type.
    pub fn get_or_create_hash(&mut self, key: &[u8]) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
        // If key doesn't exist, create empty hash
        if self.get(key).is_none() {
            self.set(Bytes::copy_from_slice(key), Entry::new_hash());
        }
        match &mut self.get_mut(key).unwrap().value {
            RedisValue::Hash(h) => Ok(h),
            _ => Err(Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            ))),
        }
    }

    /// Get existing hash for read-only access.
    /// Returns None if key doesn't exist, Err if wrong type.
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Hash(h) => Ok(Some(h)),
                _ => Err(Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value"
                ))),
            }
        }
    }
    // Similar for List, Set, SortedSet...
}
```

### Pattern 2: Command Handler with WRONGTYPE Check
**What:** Every collection command must check the value type before operating.
**When to use:** All Hash/List/Set/SortedSet commands.
**Example:**
```rust
// In command/hash.rs
pub fn hset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 || args.len() % 2 == 0 {
        return err_wrong_args("HSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("HSET"),
    };

    let hash = match db.get_or_create_hash(&key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    let mut added = 0i64;
    for pair in args[1..].chunks(2) {
        let field = extract_bytes(&pair[0]).unwrap().clone();
        let value = extract_bytes(&pair[1]).unwrap().clone();
        if hash.insert(field, value).is_none() {
            added += 1;
        }
    }
    Frame::Integer(added)
}
```

### Pattern 3: Stateless SCAN Cursor
**What:** Cursor is an index into collected keys; 0 means start, 0 returned means done.
**When to use:** SCAN, HSCAN, SSCAN, ZSCAN.
**Example:**
```rust
// Shared scan logic (could be a helper function)
fn scan_iterate<I>(
    iter_source: I,
    cursor: usize,
    count: usize,
    pattern: Option<&[u8]>,
) -> (usize, Vec<Bytes>)
where
    I: Iterator<Item = Bytes>,
{
    let items: Vec<Bytes> = iter_source.collect();
    let total = items.len();
    if total == 0 || cursor >= total {
        return (0, vec![]);
    }

    let mut results = Vec::new();
    let mut pos = cursor;
    let end = (cursor + count).min(total);

    while pos < end {
        let item = &items[pos];
        if let Some(pat) = pattern {
            if glob_match(pat, item) {
                results.push(item.clone());
            }
        } else {
            results.push(item.clone());
        }
        pos += 1;
    }

    let next_cursor = if pos >= total { 0 } else { pos };
    (next_cursor, results)
}
```

### Pattern 4: Active Expiration Task
**What:** Tokio background task that probabilistically samples and deletes expired keys.
**When to use:** Spawned once at server startup in listener.rs.
**Example:**
```rust
// In server/expiration.rs
pub async fn run_active_expiration(
    db: Arc<Mutex<Vec<Database>>>,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut dbs = db.lock().unwrap();
                for db in dbs.iter_mut() {
                    expire_cycle(db);
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("Active expiration task shutting down");
                break;
            }
        }
    }
}

fn expire_cycle(db: &mut Database) {
    let start = Instant::now();
    let budget = Duration::from_millis(1);

    loop {
        let keys_with_ttl = db.keys_with_expiry(); // need to add this method
        if keys_with_ttl.is_empty() {
            break;
        }
        // Sample up to 20 random keys
        let sample_size = 20.min(keys_with_ttl.len());
        let mut rng = rand::rng();
        let sample: Vec<Bytes> = keys_with_ttl
            .choose_multiple(&mut rng, sample_size)
            .cloned()
            .collect();

        let mut expired = 0;
        for key in &sample {
            if db.is_key_expired(key) {
                db.remove(key);
                expired += 1;
            }
        }
        // If <25% expired or budget exceeded, stop
        if expired * 4 < sample.len() || start.elapsed() >= budget {
            break;
        }
    }
}
```

### Pattern 5: AUTH Interceptor
**What:** Check authentication state before dispatching commands.
**When to use:** In connection.rs handle_connection, before calling dispatch.
**Example:**
```rust
// In connection.rs -- modify handle_connection
pub async fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<Vec<Database>>>,
    shutdown: CancellationToken,
    requirepass: Option<String>,
) {
    let mut authenticated = requirepass.is_none(); // true if no password set
    // ... in the command loop:
    // If not authenticated, only allow AUTH command
    if !authenticated {
        if cmd_name != b"AUTH" {
            send(Frame::Error(Bytes::from_static(
                b"-NOAUTH Authentication required."
            ))).await;
            continue;
        }
    }
}
```

### Anti-Patterns to Avoid
- **Exhaustive match everywhere:** After adding 4 variants to RedisValue, every existing `match &entry.value` will break. Use wildcard `_ =>` for WRONGTYPE in existing handlers (GET returns WRONGTYPE if key holds a Hash).
- **Holding Mutex across await points:** The `db.lock().unwrap()` pattern already works because operations are sync. Active expiration must also lock, do work, and release quickly.
- **Cloning entire collections for SCAN:** Collect keys/members into a Vec, not clone the collection. Keys are `Bytes` (cheap clone via refcount).
- **Forgetting to update TYPE command:** The `type_cmd` handler in key.rs currently only matches `RedisValue::String`. Must add "hash", "list", "set", "zset" arms.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Float total ordering | Custom Ord wrapper for f64 | `ordered-float::OrderedFloat` | Handles NaN edge cases, well-tested, zero dependencies |
| Random sampling | Manual index generation | `rand::seq::IndexedRandom::choose_multiple` | Correct uniform sampling, Fisher-Yates-based |
| Glob pattern matching | New matcher | Existing `glob_match()` in key.rs | Already implemented and tested for KEYS command |
| Graceful task shutdown | Custom signal handling | Existing `CancellationToken` | Already wired through listener and connection |

**Key insight:** The codebase already has the hard pieces (glob_match, CancellationToken, Mutex-based DB sharing). Phase 3 is mostly additive -- extending existing enums and patterns.

## Common Pitfalls

### Pitfall 1: Borrow Checker Conflicts in Type-Checked Mutable Access
**What goes wrong:** Getting a mutable reference to the inner collection (e.g., `&mut HashMap`) from `Database::get_mut()` while also needing to call other Database methods.
**Why it happens:** Rust's borrow checker sees `db.get_mut(key)` as borrowing all of `db`, preventing subsequent calls like `db.remove()`.
**How to avoid:** Use a two-phase approach: check type first, then operate. Or add dedicated methods on Database that encapsulate the check+mutate in one call. The existing pattern of `Database::check_expired` as a static method shows how to work around borrow conflicts.
**Warning signs:** "cannot borrow `*db` as mutable more than once" errors.

### Pitfall 2: Sorted Set Consistency Between Dual Structures
**What goes wrong:** The `members` HashMap and `scores` BTreeMap get out of sync -- a member exists in one but not the other.
**Why it happens:** ZADD, ZREM, ZINCRBY all need to update BOTH structures atomically. It is easy to update one and forget the other, especially on score updates (must remove old BTreeMap entry before inserting new one).
**How to avoid:** Never expose the raw structures. Wrap all mutations in methods on a `SortedSet` struct or on Database. Every mutation method must update both structures or neither.
**Warning signs:** ZSCORE returns a score but ZRANGE doesn't include the member, or vice versa.

### Pitfall 3: SCAN Cursor Semantics -- Returning 0 Means Done
**What goes wrong:** Returning cursor 0 when there are still keys to iterate, or never returning 0.
**Why it happens:** Confusion about Redis SCAN contract: cursor 0 is both the start AND the termination signal.
**How to avoid:** Initial call uses cursor 0. If there are more items, return a non-zero cursor. Only return 0 when iteration is complete. The cursor should never be negative.
**Warning signs:** Client loops forever or misses keys.

### Pitfall 4: HSET Returns Count of NEW Fields, Not Updated
**What goes wrong:** HSET returns total fields set instead of only newly created fields.
**Why it happens:** Redis HSET returns the number of fields that were added (not updated). `HashMap::insert` returns `Some(old_value)` if the key existed, `None` if new.
**How to avoid:** Count only `insert()` calls that return `None`.
**Warning signs:** Integration tests with redis-cli show wrong return values.

### Pitfall 5: LPUSH/RPUSH Variadic Order
**What goes wrong:** `LPUSH mylist a b c` should result in list `[c, b, a]` (each element pushed to head in order), but implementation reverses the order.
**Why it happens:** Pushing `a`, then `b`, then `c` to the left results in `c` being at the head. This is correct Redis behavior but counterintuitive.
**How to avoid:** Iterate args left-to-right, calling `push_front` for each. The last arg ends up at the head.
**Warning signs:** `LRANGE mylist 0 -1` returns elements in wrong order after LPUSH.

### Pitfall 6: ZRANGEBYSCORE -inf +inf Parsing
**What goes wrong:** Failing to parse `-inf`, `+inf`, `inf` as valid score boundaries, or not handling the `(` exclusive prefix (e.g., `(1.5` means "greater than 1.5").
**Why it happens:** Naive float parsing with `str::parse::<f64>()` doesn't handle `(` prefix or `+inf`.
**How to avoid:** Custom score boundary parser that handles: `f64::NEG_INFINITY` for `-inf`, `f64::INFINITY` for `+inf`/`inf`, strip `(` for exclusive bounds, reject NaN.
**Warning signs:** ZRANGEBYSCORE fails with "not a valid float" on `-inf` or `+inf`.

### Pitfall 7: Active Expiration Mutex Contention
**What goes wrong:** Active expiration task holds the Mutex lock for too long, blocking client request handling.
**Why it happens:** The 1ms budget is per cycle, but if the Mutex acquisition itself is contended, clients wait.
**How to avoid:** Lock, do minimal work (one batch of 20 samples), unlock. Do not loop inside the lock. Re-acquire lock for next batch if needed.
**Warning signs:** Latency spikes every 100ms correlated with expiration cycles.

## Code Examples

### Extending RedisValue Enum
```rust
// In entry.rs
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use ordered_float::OrderedFloat;

#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet {
        members: HashMap<Bytes, f64>,
        scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl Entry {
    pub fn new_hash() -> Entry {
        Entry {
            value: RedisValue::Hash(HashMap::new()),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    pub fn new_list() -> Entry {
        Entry {
            value: RedisValue::List(VecDeque::new()),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    pub fn new_set() -> Entry {
        Entry {
            value: RedisValue::Set(HashSet::new()),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    pub fn new_sorted_set() -> Entry {
        Entry {
            value: RedisValue::SortedSet {
                members: HashMap::new(),
                scores: BTreeMap::new(),
            },
            expires_at: None,
            created_at: Instant::now(),
        }
    }
}
```

### Sorted Set ZADD with Dual Structure Update
```rust
// Core sorted set mutation -- always update both structures
fn zadd_member(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    member: Bytes,
    score: f64,
) -> bool {
    // Remove old entry if exists
    if let Some(old_score) = members.remove(&member) {
        scores.remove(&(OrderedFloat(old_score), member.clone()));
    }
    let is_new = !members.contains_key(&member);
    members.insert(member.clone(), score);
    scores.insert((OrderedFloat(score), member), ());
    is_new
}
```

### UNLINK with Async Drop Threshold
```rust
// In command/key.rs
pub fn unlink(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("UNLINK");
    }
    let mut count: i64 = 0;
    for arg in args {
        if let Some(key) = extract_key(arg) {
            if let Some(entry) = db.remove(key) {
                count += 1;
                // Defer drop for large collections
                if should_async_drop(&entry.value) {
                    tokio::task::spawn_blocking(move || drop(entry));
                }
                // Small values: dropped immediately when `entry` goes out of scope
            }
        }
    }
    Frame::Integer(count)
}

fn should_async_drop(value: &RedisValue) -> bool {
    match value {
        RedisValue::String(_) => false,
        RedisValue::Hash(h) => h.len() > 64,
        RedisValue::List(l) => l.len() > 64,
        RedisValue::Set(s) => s.len() > 64,
        RedisValue::SortedSet { members, .. } => members.len() > 64,
    }
}
```

### Updating TYPE Command
```rust
// In key.rs -- extend type_cmd match
pub fn type_cmd(db: &mut Database, args: &[Frame]) -> Frame {
    // ... existing arg validation ...
    match db.get(key) {
        None => Frame::SimpleString(Bytes::from_static(b"none")),
        Some(entry) => match &entry.value {
            RedisValue::String(_) => Frame::SimpleString(Bytes::from_static(b"string")),
            RedisValue::Hash(_) => Frame::SimpleString(Bytes::from_static(b"hash")),
            RedisValue::List(_) => Frame::SimpleString(Bytes::from_static(b"list")),
            RedisValue::Set(_) => Frame::SimpleString(Bytes::from_static(b"set")),
            RedisValue::SortedSet { .. } => Frame::SimpleString(Bytes::from_static(b"zset")),
        },
    }
}
```

### Set Algebra Operations
```rust
// SINTER needs to read multiple keys -- borrow checker requires collecting first
pub fn sinter(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SINTER");
    }
    // Collect all sets first (need to release borrows)
    let mut sets: Vec<HashSet<Bytes>> = Vec::new();
    for arg in args {
        let key = match extract_bytes(arg) {
            Some(k) => k,
            None => return err_wrong_args("SINTER"),
        };
        match db.get(key) {
            None => return Frame::Array(vec![]), // empty set intersected = empty
            Some(entry) => match &entry.value {
                RedisValue::Set(s) => sets.push(s.clone()),
                _ => return wrongtype_error(),
            },
        }
    }
    // Intersect
    let mut result = sets[0].clone();
    for other in &sets[1..] {
        result = result.intersection(other).cloned().collect();
    }
    Frame::Array(result.into_iter().map(Frame::BulkString).collect())
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| ZRANGEBYSCORE (deprecated) | ZRANGE BYSCORE | Redis 6.2 | Must support both for compatibility |
| HMSET (deprecated) | HSET (variadic) | Redis 4.0 | HSET accepts multiple field-value pairs |
| Lazy-only expiration | Lazy + active | Always in Redis | Phase 2 had lazy only, Phase 3 adds active |

**Deprecated/outdated:**
- HMSET: Still must be implemented for compatibility, but HSET is the modern equivalent (both do the same thing)
- ZRANGEBYSCORE/ZREVRANGEBYSCORE: Still must be implemented for compatibility, but ZRANGE BYSCORE is the modern way

## Open Questions

1. **SCAN cursor stability across mutations**
   - What we know: Redis SCAN may return duplicates and may miss keys added during iteration. This is by design.
   - What's unclear: With a simple index-based cursor over HashMap keys, mutations (inserts/deletes) may cause HashMap rehashing which invalidates all cursor positions.
   - Recommendation: Accept this limitation (matches Redis contract). Document that SCAN provides eventual consistency, not snapshot isolation. A hash-based cursor (using key hash as position) could be more stable but adds complexity -- start with index-based per Claude's discretion.

2. **ZRANK complexity**
   - What we know: ZRANK needs the rank (position) of a member by score. BTreeMap doesn't have an O(log n) rank operation.
   - What's unclear: Whether O(n) scan is acceptable or if we need an augmented tree.
   - Recommendation: Use O(n) BTreeMap iteration to count elements before the target. For the scope of this project (no skip list), this is acceptable. Redis itself uses a skip list with O(log n) rank, but BTreeMap was a conscious tradeoff decision.

3. **SRANDMEMBER with negative count**
   - What we know: `SRANDMEMBER key count` where count < 0 means return abs(count) elements WITH possible duplicates. count > 0 means return min(count, set_size) distinct elements.
   - What's unclear: N/A -- this is well-specified.
   - Recommendation: For positive count, use `choose_multiple`. For negative count, use a loop with `choose` (may return duplicates).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in `#[test]` + `#[tokio::test]` |
| Config file | Cargo.toml (test profile is default) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` (includes integration tests) |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CONN-06 | AUTH blocks unauthenticated commands | integration | `cargo test --test integration auth` | No -- Wave 0 |
| KEY-07 | SCAN iterates all keys via cursor | unit + integration | `cargo test --lib scan` | No -- Wave 0 |
| KEY-09 | UNLINK removes key, returns count | unit | `cargo test --lib unlink` | No -- Wave 0 |
| EXP-02 | Active expiration removes expired keys | unit + integration | `cargo test --lib expire_cycle` | No -- Wave 0 |
| HASH-01..08 | Hash command correctness | unit | `cargo test --lib hash` | No -- Wave 0 |
| LIST-01..06 | List command correctness | unit | `cargo test --lib list` | No -- Wave 0 |
| SET-01..06 | Set command correctness | unit | `cargo test --lib set` | No -- Wave 0 |
| ZSET-01..09 | Sorted set command correctness | unit | `cargo test --lib sorted_set` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test` (full suite including integration)
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `src/command/hash.rs` -- inline `#[cfg(test)] mod tests` for HASH-01..08
- [ ] `src/command/list.rs` -- inline tests for LIST-01..06
- [ ] `src/command/set.rs` -- inline tests for SET-01..06
- [ ] `src/command/sorted_set.rs` -- inline tests for ZSET-01..09
- [ ] `src/server/expiration.rs` -- inline tests for expire_cycle logic
- [ ] `tests/integration.rs` -- extend with AUTH, SCAN, collection type tests
- [ ] Framework install: `cargo add ordered-float@5.1 rand@0.10` -- new deps needed before implementation

## Sources

### Primary (HIGH confidence)
- Project source code -- entry.rs, db.rs, mod.rs, string.rs, key.rs, connection.rs, config.rs, listener.rs, shutdown.rs
- CONTEXT.md -- locked decisions and architecture from user discussion
- REQUIREMENTS.md -- phase requirement IDs and descriptions

### Secondary (MEDIUM confidence)
- [ordered-float 5.1.0 docs](https://docs.rs/crate/ordered-float/latest) -- OrderedFloat API
- [rand 0.10.0 docs](https://docs.rs/rand/0.10.0/rand/) -- rng(), choose_multiple, IndexedRandom trait
- [ordered-float crates.io](https://crates.io/crates/ordered-float) -- version 5.1.0 confirmed
- [rand crates.io](https://crates.io/crates/rand) -- version 0.10.0 confirmed

### Tertiary (LOW confidence)
- Redis command reference (not checked individually for all ~60 commands; verified key semantics for HSET return value, LPUSH order, SCAN cursor, ZRANGEBYSCORE syntax)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- existing crate ecosystem, versions verified on crates.io
- Architecture: HIGH -- patterns directly extend existing codebase patterns, all integration points read
- Pitfalls: HIGH -- informed by project's own PITFALLS.md research + Redis semantics knowledge
- Sorted set design: MEDIUM -- BTreeMap approach is sound but ZRANK is O(n); acceptable per deferred skip list decision

**Research date:** 2026-03-23
**Valid until:** 2026-04-23 (stable domain, well-established patterns)
