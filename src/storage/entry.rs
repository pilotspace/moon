use bytes::Bytes;
use ordered_float::OrderedFloat;
use rand::RngExt;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use super::bptree::BPTree;
use super::compact_value::{CompactValue, RedisValueRef};
use super::intset::Intset;
use super::listpack::Listpack;
use super::stream::Stream as StreamData;

// ── Thread-local cached clock ───────────────────────────────────────────
//
// Shard event loops tick at ~1 ms and call `tl_clock_set(...)` once per tick.
// Hot-path callers of `current_secs()` / `current_time_ms()` (e.g. every
// `Entry::new_*` constructor) read from this thread-local Cell and avoid the
// `clock_gettime` vDSO call entirely. A value of 0 means "never set on this
// thread" -- fall back to the real syscall for tests and cold init paths.
//
// Correctness: monoio is thread-per-core, so each shard owns its thread and
// its own thread-local. Tokio multi-thread is also safe because every call
// site here produces timestamps whose staleness budget is >= 1 ms.

thread_local! {
    static TL_NOW_SECS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static TL_NOW_MS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Update the per-thread cached clock. Call from shard event-loop ticks.
#[inline]
pub fn tl_clock_set(secs: u32, ms: u64) {
    TL_NOW_SECS.with(|c| c.set(secs));
    TL_NOW_MS.with(|c| c.set(ms));
}

/// RAII pin for the thread-local cached clock in tests; resets to 0 on drop
/// (panic-safe), restoring the syscall fallback. Single shared home so test
/// modules don't each carry their own copy or leak a pinned clock into the
/// next test on the same thread.
#[cfg(test)]
pub struct ClockPin;

#[cfg(test)]
impl ClockPin {
    pub fn set(secs: u32, ms: u64) -> Self {
        tl_clock_set(secs, ms);
        ClockPin
    }
}

#[cfg(test)]
impl Drop for ClockPin {
    fn drop(&mut self) {
        tl_clock_set(0, 0);
    }
}

#[cold]
fn current_secs_syscall() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

#[cold]
fn current_time_ms_syscall() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Return the current time as seconds since the Unix epoch, truncated to u32.
/// Reads the thread-local cache set by `tl_clock_set` -- no syscall on the
/// hot path. Falls back to `SystemTime::now()` only when the cache is zero
/// (tests, cold init). Wraps around in the year 2106 -- acceptable for
/// LRU/LFU relative comparisons.
#[inline]
pub fn current_secs() -> u32 {
    let cached = TL_NOW_SECS.with(|c| c.get());
    if cached != 0 {
        cached
    } else {
        current_secs_syscall()
    }
}

/// Return the current time as milliseconds since the Unix epoch.
/// Reads the thread-local cache set by `tl_clock_set`. See `current_secs`.
#[inline]
pub fn current_time_ms() -> u64 {
    let cached = TL_NOW_MS.with(|c| c.get());
    if cached != 0 {
        cached
    } else {
        current_time_ms_syscall()
    }
}

/// Shared cached clock updated once per shard event loop tick (1ms).
///
/// Stores seconds and milliseconds in two `AtomicU64` values behind `Arc`,
/// so cloning gives connection handlers a reference to the same shard clock.
/// Relaxed ordering is correct: expiry checks tolerate up to 1ms staleness,
/// and we avoid the cost of acquire/release fences on the hot path.
#[derive(Clone)]
pub struct CachedClock {
    secs: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl CachedClock {
    pub fn new() -> Self {
        let now_secs = current_secs() as u64;
        let now_ms = current_time_ms();
        Self {
            secs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(now_secs)),
            ms: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(now_ms)),
        }
    }

    /// Update the cached clock. Called once per shard tick (1ms).
    ///
    /// This function is the ONE place per shard that actually calls
    /// `clock_gettime`. It refreshes both the `Arc<AtomicU64>` used by
    /// cross-thread readers (e.g. `Database::refresh_now_from_cache`) AND
    /// the thread-local `TL_NOW_*` cells read by `current_secs` /
    /// `current_time_ms` on the hot path.
    #[inline]
    pub fn update(&self) {
        // One clock read serves both granularities — this runs every 1ms per
        // shard, so the second `SystemTime::now()` it used to make was a pure
        // duplicate `clock_gettime` (issue #373 idle-CPU work).
        let m = current_time_ms_syscall();
        let s = (m / 1000) as u32;
        self.secs
            .store(s as u64, std::sync::atomic::Ordering::Relaxed);
        self.ms.store(m, std::sync::atomic::Ordering::Relaxed);
        tl_clock_set(s, m);
    }

    /// Read cached seconds.
    #[inline]
    pub fn secs(&self) -> u32 {
        self.secs.load(std::sync::atomic::Ordering::Relaxed) as u32
    }

    /// Read cached milliseconds.
    #[inline]
    pub fn ms(&self) -> u64 {
        self.ms.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for CachedClock {
    fn default() -> Self {
        Self::new()
    }
}

/// The unordered-set value type behind `SET` keys.
///
/// `IndexSet`, not `HashSet`, for one reason: `SPOP` and `SRANDMEMBER` must
/// pick a member in O(1). A `HashSet` exposes no way to address the i-th
/// element, so both commands walked the whole set -- `SRANDMEMBER` on a
/// 100,000-member set ran at 506 ops/s against Redis's 129,032, because Redis
/// samples a random bucket (`dictGetRandomKey`) while moon materialized every
/// member to choose one. `IndexSet::get_index` makes that a single lookup.
///
/// The cost of the choice is that removal must use `swap_remove` (O(1),
/// reorders) rather than `shift_remove` (O(n), order-preserving). Redis set
/// iteration order is unspecified, so reordering is not observable behaviour --
/// but it does mean nothing may depend on set iteration order.
pub type SetValue = indexmap::IndexSet<Bytes>;

/// The type of value stored in a Redis key.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    // Full-size variants (existing)
    Hash(HashMap<Bytes, Bytes>),
    /// Hash with per-field TTL sidecar (phase 195 / issue #106).
    ///
    /// `fields` holds the field → value map (same as `Hash`); `ttls` is a
    /// sparse field → absolute-expiry-ms map. A field present in `fields` but
    /// absent in `ttls` has no expiration. Auto-promoted from `HashListpack`
    /// or `Hash` on first `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT`
    /// call; auto-downgrades back to `Hash` when the last TTL is removed.
    HashWithTtl {
        fields: HashMap<Bytes, Bytes>,
        /// Sparse field → absolute-expiry-ms map.  Changed from `BTreeMap` to
        /// `HashMap` (O(1) lookup vs O(log N)) in perf/hash-with-ttl-fast-path.
        /// Ordered iteration is not a requirement — active-expiry sweeps all
        /// entries regardless, and the BTreeMap overhead showed up in profiles.
        ttls: HashMap<Bytes, u64>,
        /// Cached minimum expiry across all entries in `ttls`.
        ///
        /// Invariant: `min_expiry_ms == ttls.values().copied().min().unwrap_or(u64::MAX)`.
        ///
        /// When `cached_now_ms < min_expiry_ms` **no** field has expired, so
        /// read commands (HGET, HLEN, HGETALL, …) can skip the per-field TTL
        /// probe entirely and return results directly.  This is a pure
        /// in-memory optimisation — not persisted to RDB/AOF; recomputed from
        /// the `ttls` map on every decode.
        ///
        /// Use `u64::MAX` as the "no TTLs" sentinel so the hot-path compare
        /// `now_ms < min_expiry_ms` works without an Option unwrap.
        min_expiry_ms: u64,
    },
    List(VecDeque<Bytes>),
    Set(SetValue),
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
        members: HashMap<Bytes, f64>,
    },
    SortedSetListpack(Listpack),
    Stream(Box<StreamData>),
}

impl RedisValue {
    /// Return the Redis type name string for this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::Hash(_) | RedisValue::HashListpack(_) | RedisValue::HashWithTtl { .. } => {
                "hash"
            }
            RedisValue::List(_) | RedisValue::ListListpack(_) => "list",
            RedisValue::Set(_) | RedisValue::SetListpack(_) | RedisValue::SetIntset(_) => "set",
            RedisValue::SortedSet { .. }
            | RedisValue::SortedSetBPTree { .. }
            | RedisValue::SortedSetListpack(_) => "zset",
            RedisValue::Stream(_) => "stream",
        }
    }

    /// Return the encoding name for OBJECT ENCODING command.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            RedisValue::String(s) => {
                if crate::storage::numeric::canonical_i64(s).is_some() {
                    "int"
                } else {
                    "embstr"
                }
            }
            RedisValue::Hash(_) => "hashtable",
            // HashWithTtl reports as `hashtable` — auto-promoted from listpack
            // or Hash on first HEXPIRE; matches Valkey OBJECT ENCODING.
            RedisValue::HashWithTtl { .. } => "hashtable",
            RedisValue::HashListpack(_) => "listpack",
            RedisValue::List(_) => "linkedlist",
            RedisValue::ListListpack(_) => "listpack",
            RedisValue::Set(_) => "hashtable",
            RedisValue::SetListpack(_) => "listpack",
            RedisValue::SetIntset(_) => "intset",
            RedisValue::SortedSet { .. } => "skiplist",
            RedisValue::SortedSetBPTree { .. } => "skiplist",
            RedisValue::SortedSetListpack(_) => "listpack",
            RedisValue::Stream(_) => "stream",
        }
    }

    /// Estimate memory usage of this value in bytes.
    pub fn estimate_memory(&self) -> usize {
        match self {
            RedisValue::String(b) => b.len(),
            RedisValue::Hash(map) => map.iter().map(|(k, v)| k.len() + v.len() + 64).sum(),
            // 64B/entry baseline + 32B per TTL'd field (HashMap entry overhead).
            RedisValue::HashWithTtl { fields, ttls, .. } => {
                let f: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 64).sum();
                let t: usize = ttls.iter().map(|(k, _)| k.len() + 8 + 32).sum();
                f + t
            }
            RedisValue::List(list) => list.iter().map(|elem| elem.len() + 24).sum(),
            RedisValue::Set(set) => set.iter().map(|member| member.len() + 24).sum(),
            RedisValue::SortedSet { members, .. } => {
                members.iter().map(|(member, _)| member.len() + 80).sum()
            }
            RedisValue::HashListpack(lp)
            | RedisValue::ListListpack(lp)
            | RedisValue::SetListpack(lp)
            | RedisValue::SortedSetListpack(lp) => lp.estimate_memory(),
            RedisValue::SetIntset(is) => is.estimate_memory(),
            RedisValue::SortedSetBPTree { tree, members } => {
                // BPTree nodes + member HashMap
                let tree_mem = tree.len() * 80; // approximate per-entry overhead
                let member_mem: usize = members.iter().map(|(member, _)| member.len() + 40).sum();
                tree_mem + member_mem
            }
            RedisValue::Stream(s) => s.estimate_memory(),
        }
    }
}

/// LFU initial counter value (per Redis convention).
const LFU_INIT_VAL: u8 = 5;

/// Probabilistic logarithmic increment for LFU counter (Morris counter).
pub fn lfu_log_incr(counter: u8, lfu_log_factor: u8) -> u8 {
    if counter == 255 {
        return 255;
    }
    let r: f64 = rand::rng().random();
    let base_val = (counter as f64 - LFU_INIT_VAL as f64).max(0.0);
    let p = 1.0 / (base_val * lfu_log_factor as f64 + 1.0);
    if r < p { counter + 1 } else { counter }
}

/// Time-based decay for LFU counter.
///
/// `last_access` is the full u32 epoch-seconds clock (same domain as
/// `current_secs()`). `saturating_sub` rather than `wrapping_sub`: around the
/// year-2106 clock wrap (or transient cross-thread cache skew) a "future"
/// last_access must yield zero decay, not a near-2^32 elapsed time that would
/// zero every counter.
pub fn lfu_decay(counter: u8, last_access: u32, lfu_decay_time: u64) -> u8 {
    if lfu_decay_time == 0 {
        return counter;
    }
    let now = current_secs();
    let elapsed_secs = now.saturating_sub(last_access) as u64;
    let elapsed_min = elapsed_secs / 60;
    let decay = (elapsed_min / lfu_decay_time).min(u8::MAX as u64) as u8;
    counter.saturating_sub(decay)
}

/// First version assigned to a newly created entry. Never 0: `get_version()`
/// returns 0 for a missing key, so WATCH distinguishes "key created between
/// WATCH and EXEC" from "key still absent" only if live entries never report 0.
pub const INITIAL_VERSION: u32 = 1;

/// Pack version (24 bits) and access_counter (8 bits) into a u32.
/// Layout: [version:24 | access_counter:8]
#[inline]
fn pack_metadata_u32(version: u32, access_counter: u8) -> u32 {
    ((version & 0xFF_FFFF) << 8) | (access_counter as u32)
}

/// A compact 32-byte entry in the database, wrapping a CompactValue with TTL and packed metadata.
///
/// Layout (repr(C)):
/// - `value: CompactValue` (16 bytes, offset 0) -- SSO for small strings, tagged heap pointer otherwise
/// - `ttl_ms: u64` (8 bytes, offset 16) -- 0 = no expiry, else absolute Unix milliseconds (u64
///   milliseconds reach year ~584 million; the field was widened u32→u64 for the year-2106
///   overflow fix, then W3 used the extra range to store milliseconds instead of seconds —
///   PEXPIRE/PEXPIREAT precision previously truncated to whole seconds, expiring keys up to
///   999ms EARLY and misreporting PTTL).
/// - `metadata: u32` (4 bytes, offset 24) -- packed [version:24 | counter:8]
/// - `last_access_secs: u32` (4 bytes, offset 28) -- full epoch-seconds LRU clock
///   (formerly alignment padding; repurposed so LRU/LFU/IDLETIME are exact
///   beyond the old 16-bit field's 18.2h wrap, at zero size cost). The freed
///   16 metadata bits widened `version` 8→24 bits, pushing the WATCH/EXEC ABA
///   window from 256 to 16.7M intervening writes.
///
/// NOTE: the size changed from 24 → 32 bytes when the TTL field was widened from u32 to u64.
/// Memory overhead per key increases by 8 bytes (1/3 overhead on a 100M-key dataset = ~800 MB).
/// The correctness gain (no silent TTL overflow, full ms fidelity) outweighs the cost for all
/// realistic key counts.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct CompactEntry {
    pub value: CompactValue,
    /// Absolute expiry time in Unix milliseconds. 0 = no expiry.
    pub ttl_ms: u64,
    /// Packed metadata: [version:24 | access_counter:8]
    pub metadata: u32,
    /// Last access time, full epoch seconds (`current_secs()` domain).
    last_access_secs: u32,
}

const _: () = assert!(std::mem::size_of::<CompactEntry>() == 32);

/// Type alias for backward compatibility during migration.
pub type Entry = CompactEntry;

impl CompactEntry {
    // --- Accessor methods for packed metadata ---

    /// Get the version (24-bit, wraps to [`INITIAL_VERSION`], never 0).
    #[inline]
    pub fn version(&self) -> u32 {
        (self.metadata >> 8) & 0xFF_FFFF
    }

    /// Get the last access time (full u32 epoch seconds).
    #[inline]
    pub fn last_access(&self) -> u32 {
        self.last_access_secs
    }

    /// Get the LFU access counter (8-bit Morris counter).
    #[inline]
    pub fn access_counter(&self) -> u8 {
        (self.metadata & 0xFF) as u8
    }

    /// Set the version (24-bit, truncated to lower 24 bits).
    #[inline]
    pub fn set_version(&mut self, v: u32) {
        self.metadata = (self.metadata & 0xFF) | ((v & 0xFF_FFFF) << 8);
    }

    /// Set the last access time (full u32 epoch seconds).
    #[inline]
    pub fn set_last_access(&mut self, t: u32) {
        self.last_access_secs = t;
    }

    /// Set the LFU access counter.
    #[inline]
    pub fn set_access_counter(&mut self, c: u8) {
        self.metadata = (self.metadata & !0xFF) | (c as u32);
    }

    /// Next version after `v`: 24-bit wrap that skips 0 (the WATCH
    /// "key absent" sentinel — see [`INITIAL_VERSION`]).
    #[inline]
    pub fn bump_version(v: u32) -> u32 {
        let n = (v + 1) & 0xFF_FFFF;
        if n == 0 { INITIAL_VERSION } else { n }
    }

    /// Increment version (24-bit wrap, skips 0).
    #[inline]
    pub fn increment_version(&mut self) {
        self.set_version(Self::bump_version(self.version()));
    }

    // --- Expiry helpers ---
    // TTL is stored as absolute Unix milliseconds (u64). 0 = no expiry.

    /// Check if this entry is expired at the given time (unix millis).
    #[inline]
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        self.ttl_ms != 0 && now_ms >= self.ttl_ms
    }

    /// Check if this entry has an expiry set.
    #[inline]
    pub fn has_expiry(&self) -> bool {
        self.ttl_ms != 0
    }

    /// Get the absolute expiry time in milliseconds (for serialization/TTL commands).
    #[inline]
    pub fn expires_at_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// Set the expiry from absolute milliseconds. Pass 0 to remove expiry.
    ///
    /// Full millisecond fidelity (W3): the value is stored as-is — the old
    /// `(ms / 1000)` truncation expired keys up to 999ms EARLY and made PTTL
    /// misreport. `0` remains the no-expiry sentinel, so a (nonsensical)
    /// literal `PEXPIREAT key 0` is "remove expiry", same as before.
    #[inline]
    pub fn set_expires_at_ms(&mut self, ms: u64) {
        self.ttl_ms = ms;
    }

    // --- Convenience value accessors ---

    /// Borrow the value as a RedisValueRef.
    #[inline]
    pub fn as_redis_value(&self) -> RedisValueRef<'_> {
        self.value.as_redis_value()
    }

    /// Get mutable access to the underlying heap RedisValue (None for inline SSO).
    #[inline]
    pub fn redis_value_mut(&mut self) -> Option<&mut RedisValue> {
        self.value.as_redis_value_mut()
    }

    /// Set a new string value, replacing the current value.
    pub fn set_string_value(&mut self, v: Bytes) {
        self.value = CompactValue::from_redis_value(RedisValue::String(v));
    }

    // --- Constructors ---

    /// Create a new string entry with no expiration.
    pub fn new_string(value: Bytes) -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::String(value)),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new string entry with an expiration time (unix millis).
    pub fn new_string_with_expiry(value: Bytes, expires_at_ms: u64) -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::String(value)),
            ttl_ms: expires_at_ms,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new string entry from a borrowed slice, with no expiration.
    ///
    /// The `Bytes`-taking constructor makes a caller that holds only bytes —
    /// `itoa` output, a stack buffer, a slice of a larger value — allocate one
    /// just to hand it over, and `CompactValue` then copies out of it and drops
    /// it. INCR did exactly that, once per operation.
    pub fn new_string_from_slice(value: &[u8]) -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_slice(value),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new string entry from a borrowed slice with an expiration (unix millis).
    pub fn new_string_from_slice_with_expiry(value: &[u8], expires_at_ms: u64) -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_slice(value),
            ttl_ms: expires_at_ms,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new hash entry with an empty HashMap.
    pub fn new_hash() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Hash(HashMap::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new list entry with an empty VecDeque.
    pub fn new_list() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::List(VecDeque::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new set entry with an empty HashSet.
    pub fn new_set() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Set(SetValue::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new sorted set entry with empty members and scores.
    pub fn new_sorted_set() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSet {
                members: HashMap::new(),
                scores: BTreeMap::new(),
            }),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new hash entry using compact listpack encoding.
    pub fn new_hash_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::HashListpack(Listpack::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new list entry using compact listpack encoding.
    pub fn new_list_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::ListListpack(Listpack::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new set entry using compact listpack encoding.
    pub fn new_set_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SetListpack(Listpack::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new set entry using compact intset encoding.
    pub fn new_set_intset() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SetIntset(Intset::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new sorted set entry using B+ tree encoding.
    pub fn new_sorted_set_bptree() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                tree: BPTree::new(),
                members: HashMap::new(),
            }),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new sorted set entry using compact listpack encoding.
    pub fn new_sorted_set_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSetListpack(Listpack::new())),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new stream entry.
    pub fn new_stream() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Stream(Box::new(StreamData::new()))),
            ttl_ms: 0,
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Update last_access for LRU tracking.
    pub fn touch_lru(&mut self) {
        self.set_last_access(current_secs());
    }

    /// Update last_access and probabilistically increment LFU counter.
    pub fn touch_lfu(&mut self, lfu_log_factor: u8) {
        self.set_last_access(current_secs());
        let new_counter = lfu_log_incr(self.access_counter(), lfu_log_factor);
        self.set_access_counter(new_counter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The borrowed-slice constructors must agree with the owned-`Bytes` ones on
    /// EVERY length, especially across the 12-byte SSO boundary where one side
    /// inlines and the other heap-allocates. INCR builds its reply value from
    /// `itoa` bytes rather than a per-operation `String`, and an off-by-one here
    /// would either trip `inline_string`'s `debug_assert` or silently truncate.
    #[test]
    fn new_string_from_slice_matches_owned_across_the_sso_boundary() {
        // 0..=32 covers 12 (last inline), 13 (first heap) and the 20 bytes an
        // i64 can print to, with room to spare.
        for len in 0..=32usize {
            let raw: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            let owned = Entry::new_string(Bytes::from(raw.clone()));
            let borrowed = Entry::new_string_from_slice(&raw);
            assert_eq!(
                borrowed.value.as_bytes().map(|v| v.to_vec()),
                owned.value.as_bytes().map(|v| v.to_vec()),
                "len {len} diverged"
            );
            assert_eq!(borrowed.value.type_name(), "string", "len {len}");
            assert!(!borrowed.has_expiry(), "len {len}");
        }

        // Every i64 an INCR can produce, printed exactly as Redis prints it.
        for n in [
            0i64,
            -1,
            9,
            -9,
            999_999_999_999,   // 12 digits — last inline
            -999_999_999_999,  // 13 bytes with the sign — first heap
            1_000_000_000_000, // 13 digits
            i64::MAX,
            i64::MIN,
        ] {
            let printed = n.to_string();
            let owned = Entry::new_string(Bytes::from(printed.clone()));
            let borrowed = Entry::new_string_from_slice(printed.as_bytes());
            assert_eq!(
                borrowed.value.as_bytes().map(|v| v.to_vec()),
                owned.value.as_bytes().map(|v| v.to_vec()),
                "{n} diverged"
            );
        }
    }

    #[test]
    fn new_string_from_slice_with_expiry_keeps_the_deadline() {
        let exp_ms = current_time_ms() + 60_000;
        let short = Entry::new_string_from_slice_with_expiry(b"7", exp_ms);
        let long = Entry::new_string_from_slice_with_expiry(b"-9223372036854775808", exp_ms);
        for entry in [&short, &long] {
            assert!(entry.has_expiry());
            assert!(
                (entry.expires_at_ms() as i64 - exp_ms as i64).unsigned_abs() < 1000,
                "expiry round-trip drifted"
            );
            assert_eq!(entry.version(), INITIAL_VERSION);
            assert_eq!(entry.access_counter(), LFU_INIT_VAL);
        }
        assert_eq!(short.value.as_bytes(), Some(&b"7"[..]));
        assert_eq!(long.value.as_bytes(), Some(&b"-9223372036854775808"[..]));
    }

    #[test]
    fn test_new_string_no_expiry() {
        let entry = Entry::new_string(Bytes::from_static(b"hello"));
        assert!(!entry.has_expiry());
        assert_eq!(entry.ttl_ms, 0);
        assert_eq!(entry.value.type_name(), "string");
        assert_eq!(entry.version(), INITIAL_VERSION);
        assert_eq!(entry.access_counter(), LFU_INIT_VAL);
    }

    #[test]
    fn test_new_string_with_expiry() {
        let exp_ms = current_time_ms() + 60_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"hello"), exp_ms);
        assert!(entry.has_expiry());
        assert!(entry.ttl_ms > 0);
        // Verify round-trip: expires_at_ms should be approximately exp_ms
        let recovered_ms = entry.expires_at_ms();
        // Allow 1 second tolerance due to integer division
        assert!((recovered_ms as i64 - exp_ms as i64).unsigned_abs() < 1000);
        assert_eq!(entry.version(), INITIAL_VERSION);
        assert_eq!(entry.access_counter(), LFU_INIT_VAL);
    }

    /// RED→GREEN (W3): TTLs must round-trip at millisecond fidelity.
    /// `CompactEntry` stored `(ms / 1000)` in a u64 — a `PEXPIREAT
    /// 10_000_500` key expired at 10_000_000 (up to 999ms EARLY, violating
    /// Redis's "key lives at least the requested TTL"), and PTTL read back
    /// the truncated value. The u64 is already paid for; store ms directly.
    #[test]
    fn ttl_millisecond_fidelity() {
        let exp_ms = 10_000_500u64;
        let mut entry = Entry::new_string(Bytes::from_static(b"v"));
        entry.set_expires_at_ms(exp_ms);
        assert_eq!(
            entry.expires_at_ms(),
            exp_ms,
            "PTTL must read back the exact expiry, not a second-truncated one"
        );
        assert!(
            !entry.is_expired_at(exp_ms - 300),
            "key expired 300ms early: sub-second TTL precision was truncated"
        );
        assert!(!entry.is_expired_at(exp_ms - 1));
        assert!(entry.is_expired_at(exp_ms));
    }

    #[test]
    fn test_new_hash() {
        let entry = Entry::new_hash();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "hash");
        assert_eq!(entry.version(), INITIAL_VERSION);
    }

    #[test]
    fn test_new_list() {
        let entry = Entry::new_list();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "list");
        assert_eq!(entry.version(), INITIAL_VERSION);
    }

    #[test]
    fn test_new_set() {
        let entry = Entry::new_set();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "set");
        assert_eq!(entry.version(), INITIAL_VERSION);
    }

    #[test]
    fn test_new_sorted_set() {
        let entry = Entry::new_sorted_set();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "zset");
        assert_eq!(entry.version(), INITIAL_VERSION);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(
            RedisValue::String(Bytes::from_static(b"")).type_name(),
            "string"
        );
        assert_eq!(RedisValue::Hash(HashMap::new()).type_name(), "hash");
        assert_eq!(RedisValue::List(VecDeque::new()).type_name(), "list");
        assert_eq!(RedisValue::Set(SetValue::new()).type_name(), "set");
        assert_eq!(
            RedisValue::SortedSet {
                members: HashMap::new(),
                scores: BTreeMap::new()
            }
            .type_name(),
            "zset"
        );
    }

    #[test]
    fn test_estimate_memory_string() {
        let val = RedisValue::String(Bytes::from_static(b"hello"));
        assert_eq!(val.estimate_memory(), 5);
    }

    #[test]
    fn test_estimate_memory_hash() {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        let val = RedisValue::Hash(map);
        // key(3) + val(3) + 64 = 70
        assert_eq!(val.estimate_memory(), 70);
    }

    #[test]
    fn test_estimate_memory_empty() {
        assert_eq!(RedisValue::Hash(HashMap::new()).estimate_memory(), 0);
        assert_eq!(RedisValue::List(VecDeque::new()).estimate_memory(), 0);
        assert_eq!(RedisValue::Set(SetValue::new()).estimate_memory(), 0);
    }

    #[test]
    fn test_lfu_log_incr_max() {
        assert_eq!(lfu_log_incr(255, 10), 255);
    }

    #[test]
    fn test_lfu_decay_zero_time() {
        assert_eq!(lfu_decay(100, current_secs(), 0), 100);
    }

    #[test]
    fn test_lfu_decay_recent() {
        // Just accessed, no decay expected
        assert_eq!(lfu_decay(100, current_secs(), 1), 100);
    }

    #[test]
    fn test_touch_lru() {
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        let before = entry.last_access();
        entry.touch_lru();
        assert!(entry.last_access() >= before);
    }

    #[test]
    fn test_metadata_packing_roundtrip() {
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        // Version is 24-bit: values beyond the old 8-bit range must survive.
        entry.set_version(123_456);
        assert_eq!(entry.version(), 123_456);
        // last_access is full u32 seconds — no 16-bit truncation.
        entry.set_last_access(1_754_000_000);
        assert_eq!(entry.last_access(), 1_754_000_000);
        // version preserved across last_access writes
        assert_eq!(entry.version(), 123_456);
        // Test access_counter
        entry.set_access_counter(42);
        assert_eq!(entry.access_counter(), 42);
        // Other fields preserved
        assert_eq!(entry.version(), 123_456);
        assert_eq!(entry.last_access(), 1_754_000_000);
    }

    #[test]
    fn test_increment_version_wraps_24bit_skipping_zero() {
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        entry.set_version(0xFF_FFFF);
        assert_eq!(entry.version(), 0xFF_FFFF);
        // Wrap must skip 0: version 0 is the WATCH "key absent" sentinel, so a
        // live entry may never report it.
        entry.increment_version();
        assert_eq!(entry.version(), 1);
    }

    #[test]
    fn test_new_entries_start_at_version_one() {
        // WATCH creation-detection: get_version() returns 0 for a missing key,
        // so a freshly created entry must NOT also report 0.
        let entry = Entry::new_string(Bytes::from_static(b"test"));
        assert_ne!(entry.version(), 0);
    }

    #[test]
    fn test_lfu_decay_full_domain_via_entry() {
        // Regression: lfu_decay used to receive the 16-bit-truncated
        // last_access and subtract it from the full u32 clock, making the
        // decay value effectively random. With the full-width field, a key
        // idle 2h decays by exactly 120 (minutes) at lfu_decay_time=1.
        let now = current_secs();
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        entry.set_last_access(now - 7200);
        assert_eq!(lfu_decay(200, entry.last_access(), 1), 200 - 120);
        // A key idle long enough saturates to 0 rather than wrapping.
        entry.set_last_access(now - 40_000);
        assert_eq!(lfu_decay(200, entry.last_access(), 1), 0);
    }

    #[test]
    fn test_last_access_survives_long_idle() {
        // Regression: an 18.2h+ idle time must not wrap (OBJECT IDLETIME).
        let now = current_secs();
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        entry.set_last_access(now - 100_000); // ~27.8h
        assert_eq!(now - entry.last_access(), 100_000);
    }

    #[test]
    fn test_is_expired_at() {
        let now_ms = current_time_ms();

        // Entry expired in the past
        let past_ms = now_ms - 1000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms);
        assert!(entry.is_expired_at(now_ms));

        // Entry expires in the future
        let future_ms = now_ms + 60_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms);
        assert!(!entry.is_expired_at(now_ms));

        // Entry with no expiry
        let entry = Entry::new_string(Bytes::from_static(b"v"));
        assert!(!entry.is_expired_at(now_ms));
    }

    #[test]
    fn test_compact_entry_size() {
        assert_eq!(std::mem::size_of::<CompactEntry>(), 32);
    }

    #[test]
    fn test_cached_clock_initial_values() {
        let clock = CachedClock::new();
        // Should be within 1 second of real time
        let real_secs = current_secs();
        assert!((clock.secs() as i64 - real_secs as i64).abs() <= 1);
        let real_ms = current_time_ms();
        assert!((clock.ms() as i64 - real_ms as i64).abs() <= 1000);
    }

    #[test]
    fn test_cached_clock_update() {
        let clock = CachedClock::new();
        let before = clock.ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        clock.update();
        let after = clock.ms();
        assert!(after >= before);
    }

    #[test]
    fn test_cached_clock_clone_shares_state() {
        let clock1 = CachedClock::new();
        let clock2 = clock1.clone();
        std::thread::sleep(std::time::Duration::from_millis(5));
        clock1.update();
        // Both see the same value because they share Arc
        assert_eq!(clock1.ms(), clock2.ms());
        assert_eq!(clock1.secs(), clock2.secs());
    }
}
