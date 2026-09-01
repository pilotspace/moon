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

/// The type of value stored in a Redis key.
///
/// # Why the fat variants are boxed
///
/// [`CompactValue::from_redis_value`] stores every collection as a
/// `Box<RedisValue>`, so the allocation charged to a container key is
/// `size_of::<RedisValue>()` — the size of the *widest* variant — no matter
/// which variant the key actually holds. A `HashListpack` carrying 24 bytes
/// of payload was billed for the 128 bytes that `SortedSetBPTree` needed.
///
/// jemalloc's 64-bit small classes are 8, 16, 32, 48, 64, 80, 96, 112, 128, …
/// so what matters is the class the enum lands in: 128 sat exactly on the
/// 128-byte class. Boxing the five fat payloads brings the enum to 40 bytes
/// and the 48-byte class — **80 bytes saved on every container key**.
///
/// The boxes are on the *fields*, not on newtype wrappers around each struct
/// variant, so every existing `match` pattern still binds the same names and
/// `Box<T>` derefs to `T` at each use. It also costs no extra heap: two boxes
/// of 48 and 24 land in the 48 and 32 classes, exactly what one box of 72
/// would (the 80 class), and `HashWithTtl` is strictly cheaper this way
/// (48 + 48 = 96 against a single 104-byte box's 112 class). What it does cost
/// is one extra `malloc` when a collection is promoted out of its listpack
/// encoding — a cold, once-per-key event.
///
/// # What deliberately stays inline
///
/// `String(Bytes)` is the hot path and the one dimension moon already wins on
/// (0.83x vs Redis); boxing it would spend that win. The three listpack
/// variants and `SetIntset` are the compact encodings small collections live
/// in, where a second indirection is pure loss. `HashWithTtl::min_expiry_ms`
/// stays inline too, so the "has anything expired?" fast path still reads a
/// plain `u64` with no pointer chase. `test_hot_variants_stay_inline` pins all
/// of this at compile time.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    // Full-size variants (existing)
    Hash(Box<HashMap<Bytes, Bytes>>),
    /// Hash with per-field TTL sidecar (phase 195 / issue #106).
    ///
    /// `fields` holds the field → value map (same as `Hash`); `ttls` is a
    /// sparse field → absolute-expiry-ms map. A field present in `fields` but
    /// absent in `ttls` has no expiration. Auto-promoted from `HashListpack`
    /// or `Hash` on first `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT`
    /// call; auto-downgrades back to `Hash` when the last TTL is removed.
    HashWithTtl {
        fields: Box<HashMap<Bytes, Bytes>>,
        /// Sparse field → absolute-expiry-ms map.  Changed from `BTreeMap` to
        /// `HashMap` (O(1) lookup vs O(log N)) in perf/hash-with-ttl-fast-path.
        /// Ordered iteration is not a requirement — active-expiry sweeps all
        /// entries regardless, and the BTreeMap overhead showed up in profiles.
        ttls: Box<HashMap<Bytes, u64>>,
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
    Set(Box<HashSet<Bytes>>),
    SortedSet {
        members: Box<HashMap<Bytes, f64>>,
        scores: Box<BTreeMap<(OrderedFloat<f64>, Bytes), ()>>,
    },
    // Compact variants (new)
    HashListpack(Listpack),
    ListListpack(Listpack),
    SetListpack(Listpack),
    SetIntset(Intset),
    SortedSetBPTree {
        tree: Box<BPTree>,
        members: Box<HashMap<Bytes, f64>>,
    },
    SortedSetListpack(Listpack),
    Stream(Box<StreamData>),
}

/// 40 bytes as measured: the widest inline payload is 32 (`String(Bytes)`,
/// `List(VecDeque<Bytes>)`, `SetIntset`) plus the 8-byte discriminant. It is
/// pinned at `<= 48` rather than `== 40` because what is actually being bought
/// is the jemalloc size class, and 40 and 48 are the same class — but 49 is
/// not, and the next stop above it is 64. See the type docs, and
/// `test_redis_value_fits_48_byte_size_class` for the runtime counterpart.
const _: () = assert!(std::mem::size_of::<RedisValue>() <= 48);

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

/// Bit 31 of `metadata`: this key has an expiry, recorded in the owning
/// [`crate::storage::Database`]'s `expires` map under the key.
///
/// The bit is what makes the no-TTL read path free. `is_expired`/`TTL` first
/// test this bit; only if it is SET does anything consult the map. A keyspace
/// with no TTLs therefore performs exactly the work it did when the entry
/// carried an inline `ttl_ms: u64` — one mask-and-test against a word that is
/// already in the same cache line as the value.
pub(crate) const HAS_EXPIRY_BIT: u32 = 0x8000_0000;

/// Width of the packed `version` field: bits 30..8 of `metadata`.
const VERSION_MASK: u32 = 0x7F_FFFF;
const VERSION_SHIFT: u32 = 8;

/// Pack version (23 bits) and access_counter (8 bits) into a u32.
/// Layout: [has_expiry:1 | version:23 | access_counter:8]
#[inline]
fn pack_metadata_u32(version: u32, access_counter: u8) -> u32 {
    ((version & VERSION_MASK) << VERSION_SHIFT) | (access_counter as u32)
}

/// A compact 24-byte entry in the database, wrapping a CompactValue with packed metadata.
///
/// Layout (repr(C)):
/// - `value: CompactValue` (16 bytes, offset 0) -- SSO for small strings, tagged heap pointer otherwise
/// - `metadata: u32` (4 bytes, offset 16) -- packed [has_expiry:1 | version:23 | counter:8]
/// - `last_access_secs: u32` (4 bytes, offset 20) -- full epoch-seconds LRU clock
///   (repurposed from alignment padding so LRU/LFU/IDLETIME are exact beyond
///   the old 16-bit field's 18.2h wrap, at zero size cost).
///
/// # Why the expiry is NOT here
///
/// The entry used to carry `ttl_ms: u64` at offset 16, documented `0 = no
/// expiry`. That field was charged to **every slot of every segment** --
/// `TOTAL_SLOTS` (60) of them per segment, occupied or not -- so a keyspace
/// with no TTLs at all still paid 8 B per slot, 480 B per segment, ~11.8 B per
/// stored key at the organic fill of ~40.5 keys/segment. Redis never pays it:
/// expiries live in a separate `db->expires` dict and a key without a TTL has
/// no entry in it.
///
/// moon now does the same. The absolute expiry (unix milliseconds, full
/// fidelity, unchanged range and semantics) lives in the owning
/// `Database`'s `expires: HashMap<CompactKey, u64>`, and [`HAS_EXPIRY_BIT`]
/// in `metadata` says whether to look. Every read of a key without a TTL
/// therefore does exactly what it did before -- test one bit of a word that
/// shares a cache line with the value -- and never hashes anything.
///
/// The 24-bit `version` gave up its top bit to [`HAS_EXPIRY_BIT`]: the
/// WATCH/EXEC ABA window is 8.4M intervening writes to the same key instead
/// of 16.7M, which is still four orders of magnitude past the 256 it was
/// before that field was widened.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct CompactEntry {
    pub value: CompactValue,
    /// Packed metadata: [has_expiry:1 | version:23 | access_counter:8]
    metadata: u32,
    /// Last access time, full epoch seconds (`current_secs()` domain).
    last_access_secs: u32,
}

const _: () = assert!(std::mem::size_of::<CompactEntry>() == 24);

/// Type alias for backward compatibility during migration.
pub type Entry = CompactEntry;

impl CompactEntry {
    // --- Accessor methods for packed metadata ---

    /// Get the version (23-bit, wraps to [`INITIAL_VERSION`], never 0).
    #[inline]
    pub fn version(&self) -> u32 {
        (self.metadata >> VERSION_SHIFT) & VERSION_MASK
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

    /// Set the version (23-bit, truncated to lower 23 bits).
    ///
    /// Preserves BOTH neighbours in the word: the LFU counter in bits 7..0 and
    /// [`HAS_EXPIRY_BIT`] in bit 31. Clobbering the latter here would strand a
    /// live entry in the `expires` map -- a key with a TTL that never expires.
    #[inline]
    pub fn set_version(&mut self, v: u32) {
        self.metadata = (self.metadata & !(VERSION_MASK << VERSION_SHIFT))
            | ((v & VERSION_MASK) << VERSION_SHIFT);
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

    /// Next version after `v`: 23-bit wrap that skips 0 (the WATCH
    /// "key absent" sentinel — see [`INITIAL_VERSION`]).
    #[inline]
    pub fn bump_version(v: u32) -> u32 {
        let n = (v + 1) & VERSION_MASK;
        if n == 0 { INITIAL_VERSION } else { n }
    }

    /// Increment version (24-bit wrap, skips 0).
    #[inline]
    pub fn increment_version(&mut self) {
        self.set_version(Self::bump_version(self.version()));
    }

    // --- Expiry helpers ---
    //
    // The expiry VALUE (absolute unix milliseconds, full fidelity) lives in
    // the owning `Database`'s `expires` map; see the struct docs. All the
    // entry itself knows is WHETHER there is one, and that answer is free.

    /// Check if this entry has an expiry recorded for it.
    ///
    /// The whole no-TTL fast path: one mask-and-test, no hashing, no map.
    /// A caller that needs the deadline itself asks the `Database`.
    #[inline]
    pub fn has_expiry(&self) -> bool {
        self.metadata & HAS_EXPIRY_BIT != 0
    }

    /// Record whether this entry has an expiry.
    ///
    /// `Database` owns this bit: it is set/cleared in the same statement that
    /// inserts into or removes from `expires`, so the bit and the map cannot
    /// disagree. Nothing outside the storage layer may call it.
    #[inline]
    pub(crate) fn set_has_expiry(&mut self, yes: bool) {
        if yes {
            self.metadata |= HAS_EXPIRY_BIT;
        } else {
            self.metadata &= !HAS_EXPIRY_BIT;
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
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new hash entry with an empty HashMap.
    pub fn new_hash() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Hash(Box::default())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new list entry with an empty VecDeque.
    pub fn new_list() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::List(VecDeque::new())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new set entry with an empty HashSet.
    pub fn new_set() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Set(Box::default())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new sorted set entry with empty members and scores.
    pub fn new_sorted_set() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSet {
                members: Box::default(),
                scores: Box::default(),
            }),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new hash entry using compact listpack encoding.
    pub fn new_hash_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::HashListpack(Listpack::new())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new list entry using compact listpack encoding.
    pub fn new_list_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::ListListpack(Listpack::new())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new set entry using compact listpack encoding.
    pub fn new_set_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SetListpack(Listpack::new())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new set entry using compact intset encoding.
    pub fn new_set_intset() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SetIntset(Intset::new())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new sorted set entry using B+ tree encoding.
    pub fn new_sorted_set_bptree() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                tree: Box::new(BPTree::new()),
                members: Box::default(),
            }),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new sorted set entry using compact listpack encoding.
    pub fn new_sorted_set_listpack() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::SortedSetListpack(Listpack::new())),
            metadata: pack_metadata_u32(INITIAL_VERSION, LFU_INIT_VAL),
            last_access_secs: current_secs(),
        }
    }

    /// Create a new stream entry.
    pub fn new_stream() -> CompactEntry {
        CompactEntry {
            value: CompactValue::from_redis_value(RedisValue::Stream(Box::new(StreamData::new()))),
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
    fn test_new_string_no_expiry() {
        let entry = Entry::new_string(Bytes::from_static(b"hello"));
        assert!(!entry.has_expiry());
        assert_eq!(entry.value.type_name(), "string");
        assert_eq!(entry.version(), INITIAL_VERSION);
        assert_eq!(entry.access_counter(), LFU_INIT_VAL);
    }

    /// The three packed fields share one `u32`, so every setter must leave the
    /// other two alone. `set_version` clobbering [`HAS_EXPIRY_BIT`] would
    /// strand a key in `expires`: a TTL that never fires. `set_access_counter`
    /// clobbering it would do the same on every LFU touch — i.e. on reads.
    #[test]
    fn packed_metadata_fields_are_independent() {
        let mut e = Entry::new_string(Bytes::from_static(b"v"));
        e.set_has_expiry(true);
        e.set_version(0x7F_FFFF);
        e.set_access_counter(200);
        assert!(
            e.has_expiry(),
            "set_version/set_access_counter dropped the bit"
        );
        assert_eq!(e.version(), 0x7F_FFFF);
        assert_eq!(e.access_counter(), 200);

        e.set_has_expiry(false);
        assert!(!e.has_expiry());
        assert_eq!(
            e.version(),
            0x7F_FFFF,
            "set_has_expiry corrupted the version"
        );
        assert_eq!(e.access_counter(), 200);

        // The version field is 23 bits: the 24th must not leak into the bit.
        e.set_version(u32::MAX);
        assert!(
            !e.has_expiry(),
            "an over-wide version reached HAS_EXPIRY_BIT"
        );
        assert_eq!(e.version(), 0x7F_FFFF);
    }

    /// `bump_version` must stay inside the 23-bit field and skip 0, or a wrap
    /// would set [`HAS_EXPIRY_BIT`] on a key with no TTL.
    #[test]
    fn bump_version_wraps_inside_23_bits() {
        assert_eq!(Entry::bump_version(0x7F_FFFE), 0x7F_FFFF);
        assert_eq!(Entry::bump_version(0x7F_FFFF), INITIAL_VERSION);
        let mut e = Entry::new_string(Bytes::from_static(b"v"));
        e.set_version(Entry::bump_version(0x7F_FFFF));
        assert!(!e.has_expiry());
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
        assert_eq!(RedisValue::Hash(Box::default()).type_name(), "hash");
        assert_eq!(RedisValue::List(VecDeque::new()).type_name(), "list");
        assert_eq!(RedisValue::Set(Box::default()).type_name(), "set");
        assert_eq!(
            RedisValue::SortedSet {
                members: Box::default(),
                scores: Box::default()
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
        let val = RedisValue::Hash(Box::new(map));
        // key(3) + val(3) + 64 = 70
        assert_eq!(val.estimate_memory(), 70);
    }

    #[test]
    fn test_estimate_memory_empty() {
        assert_eq!(RedisValue::Hash(Box::default()).estimate_memory(), 0);
        assert_eq!(RedisValue::List(VecDeque::new()).estimate_memory(), 0);
        assert_eq!(RedisValue::Set(Box::default()).estimate_memory(), 0);
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
    fn test_increment_version_wraps_23bit_skipping_zero() {
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        entry.set_version(0x7F_FFFF);
        assert_eq!(entry.version(), 0x7F_FFFF);
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

    /// `CompactValue::from_redis_value` stores every collection as a
    /// `Box<RedisValue>`, so `size_of::<RedisValue>()` is billed against EVERY
    /// container key regardless of which variant it holds — a 24-byte
    /// `HashListpack` paid for the widest variant in the enum.
    ///
    /// jemalloc's 64-bit small classes are 8, 16, 32, 48, 64, 80, 96, 112,
    /// 128, … so the number that matters is which class the enum lands in.
    /// At 128 it sat exactly on the 128-byte class; the fat variants have to
    /// be boxed to reach the 48-byte one, for 80 B/key.
    #[test]
    fn test_redis_value_fits_48_byte_size_class() {
        let sz = std::mem::size_of::<RedisValue>();
        assert!(
            sz <= 48,
            "size_of::<RedisValue>() is {sz}; it must be <= 48 so every \
             container key allocates from jemalloc's 48-byte class, not \
             the 128-byte one"
        );
    }

    /// The hot-path variants must stay INLINE. `String` is the path moon
    /// already WINS on (0.83x vs Redis) — boxing it would spend that win —
    /// and the three listpack encodings plus `SetIntset` are the compact
    /// representations small collections live in, where a second
    /// indirection is pure loss. Binding each payload to its exact,
    /// unboxed type makes this a COMPILE-time guarantee, not a runtime one.
    #[test]
    fn test_hot_variants_stay_inline() {
        let s = RedisValue::String(Bytes::from_static(b"v"));
        match &s {
            RedisValue::String(b) => {
                let _unboxed: &Bytes = b;
            }
            _ => unreachable!(),
        }

        let lp = RedisValue::HashListpack(Listpack::new());
        match &lp {
            RedisValue::HashListpack(l)
            | RedisValue::ListListpack(l)
            | RedisValue::SetListpack(l)
            | RedisValue::SortedSetListpack(l) => {
                let _unboxed: &Listpack = l;
            }
            _ => unreachable!(),
        }

        let is = RedisValue::SetIntset(Intset::new());
        match &is {
            RedisValue::SetIntset(i) => {
                let _unboxed: &Intset = i;
            }
            _ => unreachable!(),
        }

        let l = RedisValue::List(VecDeque::new());
        match &l {
            RedisValue::List(v) => {
                let _unboxed: &VecDeque<Bytes> = v;
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_compact_entry_size() {
        // 16 (CompactValue) + 4 (packed metadata) + 4 (LRU clock). The
        // 8-byte `ttl_ms` that used to sit at offset 16 now lives in the
        // Database's `expires` map, where only keys that HAVE a TTL pay for
        // it — see the struct docs. This number is multiplied by
        // `TOTAL_SLOTS` (60) in every segment, occupied or not.
        assert_eq!(std::mem::size_of::<CompactEntry>(), 24);
        // No 8-byte field left, so the type aligns to 4 and nothing is
        // wasted on tail padding.
        assert_eq!(std::mem::align_of::<CompactEntry>(), 4);
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
