use bytes::Bytes;
use ordered_float::OrderedFloat;
use rand::RngExt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
        let s = current_secs_syscall();
        let m = current_time_ms_syscall();
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
                if s.len() <= 20
                    && std::str::from_utf8(s)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .is_some()
                {
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
pub fn lfu_decay(counter: u8, last_access: u32, lfu_decay_time: u64) -> u8 {
    if lfu_decay_time == 0 {
        return counter;
    }
    let now = current_secs();
    let elapsed_secs = now.wrapping_sub(last_access) as u64;
    let elapsed_min = elapsed_secs / 60;
    let decay = (elapsed_min / lfu_decay_time) as u8;
    counter.saturating_sub(decay)
}

/// Pack last_access (16 bits), version (8 bits), and access_counter (8 bits) into a u32.
/// Layout: [last_access:16 | version:8 | access_counter:8]
#[inline]
fn pack_metadata_u32(last_access: u16, version: u8, access_counter: u8) -> u32 {
    ((last_access as u32) << 16) | ((version as u32) << 8) | (access_counter as u32)
}

/// A compact 32-byte entry in the database, wrapping a CompactValue with TTL and packed metadata.
///
/// Layout (repr(C)):
/// - `value: CompactValue` (16 bytes, offset 0) -- SSO for small strings, tagged heap pointer otherwise
/// - `ttl_secs: u64` (8 bytes, offset 16) -- 0 = no expiry, else absolute Unix seconds (u64 supports
///   timestamps up to year ~584 billion; u32 was limited to year 2106 and silently overflowed for
///   timestamps like 9_999_999_999 used in EXPIREAT regression tests).
/// - `metadata: u32` (4 bytes, offset 24) -- packed [last_access:16 | version:8 | counter:8]
/// - _pad: u32 (4 bytes, offset 28) -- alignment padding
///
/// NOTE: the size changed from 24 → 32 bytes when `ttl_delta: u32` was widened to
/// `ttl_secs: u64`. Memory overhead per key increases by 8 bytes (1/3 overhead on a
/// 100M-key dataset = ~800 MB). The correctness gain (no silent TTL overflow past
/// year 2106) outweighs the cost for all realistic key counts.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct CompactEntry {
    pub value: CompactValue,
    /// Absolute expiry time in Unix seconds. 0 = no expiry.
    /// Widened from u32 to u64 to support timestamps beyond year 2106.
    pub ttl_secs: u64,
    /// Packed metadata: [last_access:16 | version:8 | access_counter:8]
    pub metadata: u32,
    _pad: u32,
}

const _: () = assert!(std::mem::size_of::<CompactEntry>() == 32);

/// Type alias for backward compatibility during migration.
pub type Entry = CompactEntry;

impl CompactEntry {
    // --- Accessor methods for packed metadata ---

    /// Get the version (8-bit, wraps at 0xFF).
    #[inline]
    pub fn version(&self) -> u32 {
        ((self.metadata >> 8) & 0xFF) as u32
    }

    /// Get the last access time (16-bit relative seconds).
    #[inline]
    pub fn last_access(&self) -> u32 {
        (self.metadata >> 16) as u32
    }

    /// Get the LFU access counter (8-bit Morris counter).
    #[inline]
    pub fn access_counter(&self) -> u8 {
        (self.metadata & 0xFF) as u8
    }

    /// Set the version (8-bit, truncated to lower 8 bits).
    #[inline]
    pub fn set_version(&mut self, v: u32) {
        let v8 = (v & 0xFF) as u8;
        self.metadata = (self.metadata & !(0xFF << 8)) | ((v8 as u32) << 8);
    }

    /// Set the last access time (truncated to 16 bits).
    #[inline]
    pub fn set_last_access(&mut self, t: u32) {
        let t16 = (t & 0xFFFF) as u16;
        self.metadata = (self.metadata & 0xFFFF) | ((t16 as u32) << 16);
    }

    /// Set the LFU access counter.
    #[inline]
    pub fn set_access_counter(&mut self, c: u8) {
        self.metadata = (self.metadata & !0xFF) | (c as u32);
    }

    /// Increment version, wrapping at 0xFF.
    #[inline]
    pub fn increment_version(&mut self) {
        let v = ((self.version() + 1) & 0xFF) as u32;
        self.set_version(v);
    }

    // --- Expiry helpers ---
    // TTL is stored as absolute Unix seconds (u64). 0 = no expiry.
    // The base_ts parameter is accepted for API consistency but not used.

    /// Check if this entry is expired at the given time (unix millis).
    #[inline]
    pub fn is_expired_at(&self, _base_ts: u32, now_ms: u64) -> bool {
        if self.ttl_secs == 0 {
            return false;
        }
        now_ms >= self.ttl_secs * 1000
    }

    /// Check if this entry has an expiry set.
    #[inline]
    pub fn has_expiry(&self) -> bool {
        self.ttl_secs != 0
    }

    /// Get the absolute expiry time in milliseconds (for serialization/TTL commands).
    #[inline]
    pub fn expires_at_ms(&self, _base_ts: u32) -> u64 {
        self.ttl_secs * 1000
    }

    /// Set the expiry from absolute milliseconds. Pass 0 to remove expiry.
    #[inline]
    pub fn set_expires_at_ms(&mut self, _base_ts: u32, ms: u64) {
        if ms == 0 {
            self.ttl_secs = 0;
        } else {
            // Store as absolute seconds; ensure non-zero for non-zero input.
            self.ttl_secs = (ms / 1000).max(1);
        }
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
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new string entry with an expiration time (unix millis).
    pub fn new_string_with_expiry(value: Bytes, expires_at_ms: u64, base_ts: u32) -> CompactEntry {
        let mut entry = CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::String(value)),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        };
        entry.set_expires_at_ms(base_ts, expires_at_ms);
        entry
    }

    /// Create a new hash entry with an empty HashMap.
    pub fn new_hash() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Hash(HashMap::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new list entry with an empty VecDeque.
    pub fn new_list() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::List(VecDeque::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new set entry with an empty HashSet.
    pub fn new_set() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Set(HashSet::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new sorted set entry with empty members and scores.
    pub fn new_sorted_set() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSet {
                members: HashMap::new(),
                scores: BTreeMap::new(),
            }),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new hash entry using compact listpack encoding.
    pub fn new_hash_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::HashListpack(Listpack::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new list entry using compact listpack encoding.
    pub fn new_list_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::ListListpack(Listpack::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new set entry using compact listpack encoding.
    pub fn new_set_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SetListpack(Listpack::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new set entry using compact intset encoding.
    pub fn new_set_intset() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SetIntset(Intset::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new sorted set entry using B+ tree encoding.
    pub fn new_sorted_set_bptree() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                tree: BPTree::new(),
                members: HashMap::new(),
            }),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new sorted set entry using compact listpack encoding.
    pub fn new_sorted_set_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSetListpack(Listpack::new())),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
        }
    }

    /// Create a new stream entry.
    pub fn new_stream() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Stream(Box::new(StreamData::new()))),
            ttl_secs: 0,
            metadata: pack_metadata_u32(current_secs() as u16, 0, LFU_INIT_VAL),
            _pad: 0,
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

    #[test]
    fn test_new_string_no_expiry() {
        let entry = Entry::new_string(Bytes::from_static(b"hello"));
        assert!(!entry.has_expiry());
        assert_eq!(entry.ttl_secs, 0);
        assert_eq!(entry.value.type_name(), "string");
        assert_eq!(entry.version(), 0);
        assert_eq!(entry.access_counter(), LFU_INIT_VAL);
    }

    #[test]
    fn test_new_string_with_expiry() {
        let base_ts = current_secs();
        let exp_ms = current_time_ms() + 60_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"hello"), exp_ms, base_ts);
        assert!(entry.has_expiry());
        assert!(entry.ttl_secs > 0);
        // Verify round-trip: expires_at_ms should be approximately exp_ms
        let recovered_ms = entry.expires_at_ms(base_ts);
        // Allow 1 second tolerance due to integer division
        assert!((recovered_ms as i64 - exp_ms as i64).unsigned_abs() < 1000);
        assert_eq!(entry.version(), 0);
        assert_eq!(entry.access_counter(), LFU_INIT_VAL);
    }

    #[test]
    fn test_new_hash() {
        let entry = Entry::new_hash();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "hash");
        assert_eq!(entry.version(), 0);
    }

    #[test]
    fn test_new_list() {
        let entry = Entry::new_list();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "list");
        assert_eq!(entry.version(), 0);
    }

    #[test]
    fn test_new_set() {
        let entry = Entry::new_set();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "set");
        assert_eq!(entry.version(), 0);
    }

    #[test]
    fn test_new_sorted_set() {
        let entry = Entry::new_sorted_set();
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "zset");
        assert_eq!(entry.version(), 0);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(
            RedisValue::String(Bytes::from_static(b"")).type_name(),
            "string"
        );
        assert_eq!(RedisValue::Hash(HashMap::new()).type_name(), "hash");
        assert_eq!(RedisValue::List(VecDeque::new()).type_name(), "list");
        assert_eq!(RedisValue::Set(HashSet::new()).type_name(), "set");
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
        assert_eq!(RedisValue::Set(HashSet::new()).estimate_memory(), 0);
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
        // Test version (8-bit: max 255)
        entry.set_version(123);
        assert_eq!(entry.version(), 123);
        // Test last_access (16-bit: max 65535)
        entry.set_last_access(54321);
        // last_access truncates to u16
        assert_eq!(entry.last_access(), 54321);
        // version should be preserved
        assert_eq!(entry.version(), 123);
        // Test access_counter
        entry.set_access_counter(42);
        assert_eq!(entry.access_counter(), 42);
        // Other fields preserved
        assert_eq!(entry.version(), 123);
        assert_eq!(entry.last_access(), 54321);
    }

    #[test]
    fn test_increment_version_wraps_at_8bit() {
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        entry.set_version(0xFF);
        assert_eq!(entry.version(), 0xFF);
        entry.increment_version();
        assert_eq!(entry.version(), 0); // wraps
    }

    #[test]
    fn test_is_expired_at() {
        let base_ts = current_secs();
        let now_ms = current_time_ms();

        // Entry expired in the past
        let past_ms = now_ms - 1000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms, base_ts);
        assert!(entry.is_expired_at(base_ts, now_ms));

        // Entry expires in the future
        let future_ms = now_ms + 60_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms, base_ts);
        assert!(!entry.is_expired_at(base_ts, now_ms));

        // Entry with no expiry
        let entry = Entry::new_string(Bytes::from_static(b"v"));
        assert!(!entry.is_expired_at(base_ts, now_ms));
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
