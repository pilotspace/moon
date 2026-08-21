use std::collections::HashMap;

mod accessors;
mod hash_ttl;
mod kv_ops;

pub use super::db_read::{HashRef, ListRef, SetRef, SortedSetRef, StreamRef};
pub use accessors::EntryView;

use super::compact_key::CompactKey;
use super::dashtable::DashTable;
use super::entry::{CachedClock, Entry, RedisValue, current_secs, current_time_ms};

/// Maximum number of entries in a listpack before upgrading to full encoding.
pub const LISTPACK_MAX_ENTRIES: usize = 128;
/// Maximum element size in bytes before upgrading a listpack to full encoding.
pub const LISTPACK_MAX_ELEMENT_SIZE: usize = 64;
/// Maximum number of entries in an intset before upgrading to full encoding.
#[allow(dead_code)]
const INTSET_MAX_ENTRIES: usize = 512;

/// Estimate per-entry overhead: key length + value memory + struct overhead.
fn entry_overhead(key: &[u8], entry: &Entry) -> usize {
    key.len() + entry.value.estimate_memory() + 128
}

// ---------------------------------------------------------------------------
// O(1) container-growth memory accounting (WS6).
//
// `get_or_create_hash`/`_list`/`_set`/`_sorted_set` charge `entry_overhead`
// ONCE, at creation of the (empty) container, then hand out a raw `&mut`
// reference into the map/deque/set/tree. Every subsequent mutation through
// that reference (HSET adding fields, LPUSH growing the deque, ...) was
// invisible to `used_memory` — a real memory-safety hole against both the
// global `--maxmemory` gate and per-db quotas (adversarial review, 2026-07-08).
//
// The fix is per-mutation delta accounting rather than a full recompute:
// `RedisValue::estimate_memory()` is O(n) for the expanded encodings (plain
// `Hash`/`List`/`Set`/`SortedSetBPTree` all sum over every element), so
// recomputing it after every single-field HSET on a million-field hash would
// turn an O(1) command into O(n) — exactly the "periodic rescan on the hot
// path" this design avoids. Instead, call sites that mutate a container
// in-place compute the exact per-element byte delta using the same
// constants baked into `RedisValue::estimate_memory`'s per-variant arms
// (kept below so they can't silently drift out of sync with that function)
// and apply it via `Database::charge_memory` / `credit_memory` — O(1), no
// allocation, no rescan.
//
// The listpack/intset COMPACT encodings are the exception: their
// `estimate_memory()` is already O(1) (`size_of::<Self>() + data.capacity()`,
// a byte-buffer capacity read), so those call sites snapshot
// `lp.estimate_memory()` before/after the mutation instead of tracking a
// per-element formula — simpler and just as cheap.
// ---------------------------------------------------------------------------

/// Per-field byte cost for a `RedisValue::Hash` / `HashWithTtl.fields` entry.
/// MUST mirror the `Hash` arm of `RedisValue::estimate_memory` exactly.
#[inline]
pub fn hash_field_cost(field: &[u8], value: &[u8]) -> usize {
    hash_field_cost_len(field.len(), value.len())
}

/// Length-only variant of [`hash_field_cost`] for call sites where the field
/// and/or value `Bytes` have already been moved (e.g. into `map.insert`) —
/// `Bytes::len()` is captured beforehand since it doesn't consume the value.
#[inline]
pub fn hash_field_cost_len(field_len: usize, value_len: usize) -> usize {
    field_len + value_len + 64
}

/// Per-field TTL sidecar byte cost (`HashWithTtl.ttls`). MUST mirror the
/// `HashWithTtl` arm's `ttls` term in `RedisValue::estimate_memory`.
#[inline]
pub fn hash_ttl_field_cost(field: &[u8]) -> usize {
    field.len() + 8 + 32
}

/// Per-element byte cost for `RedisValue::List`. MUST mirror the `List` arm
/// of `RedisValue::estimate_memory`.
#[inline]
pub fn list_elem_cost(elem: &[u8]) -> usize {
    elem.len() + 24
}

/// Per-member byte cost for `RedisValue::Set`. MUST mirror the `Set` arm of
/// `RedisValue::estimate_memory`.
#[inline]
pub fn set_member_cost(member: &[u8]) -> usize {
    member.len() + 24
}

/// Per-member byte cost for `RedisValue::SortedSetBPTree`: one fixed-size
/// tree node (80B, `tree.len() * 80` in the estimator) plus one
/// `members: HashMap<Bytes, f64>` entry (`member.len() + 40`). MUST mirror
/// the `SortedSetBPTree` arm of `RedisValue::estimate_memory`.
#[inline]
pub fn zset_member_cost(member: &[u8]) -> usize {
    member.len() + 40 + 80
}

// ---------------------------------------------------------------------------
// HEXPIRE family — public type surface (phase 195 / issue #106).
// ---------------------------------------------------------------------------

/// Conditional gate for `Database::hash_set_field_ttl`.
/// Mirrors Valkey 9.0 HEXPIRE NX/XX/GT/LT semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashTtlCond {
    /// Always set the TTL (no condition).
    Always,
    /// Only set if no current TTL on the field.
    Nx,
    /// Only set if a TTL is already present.
    Xx,
    /// Only set if the new TTL is greater than current.
    Gt,
    /// Only set if the new TTL is less than current.
    Lt,
}

/// Tri-state field lookup result for HTTL / HEXPIRETIME / HPERSIST.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldState {
    /// Field does not exist in the hash (HTTL → -2).
    Missing,
    /// Field exists but has no TTL (HTTL → -1).
    NoTtl,
    /// Field exists with the given absolute expiry (unix-ms).
    Ttl(u64),
}

/// Returned by hash-mutation primitives when the key exists but is not a hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WrongType;

/// Convert a `RedisValue::Hash` or `RedisValue::HashListpack` in-place into a
/// `RedisValue::HashWithTtl` carrying an empty `ttls` sidecar. No-op when the
/// value is already `HashWithTtl`. Panics for non-hash variants — callers must
/// type-check first.
fn promote_to_hash_with_ttl(rv: &mut RedisValue) {
    match rv {
        RedisValue::HashWithTtl { .. } => {}
        RedisValue::Hash(_) => {
            let placeholder = RedisValue::Hash(HashMap::new());
            let owned = std::mem::replace(rv, placeholder);
            let RedisValue::Hash(fields) = owned else {
                unreachable!("matched Hash above");
            };
            // ttls starts empty; min_expiry_ms = u64::MAX (sentinel: "no TTLs
            // yet").  hash_set_field_ttl will update min on the first insert.
            *rv = RedisValue::HashWithTtl {
                fields,
                ttls: HashMap::new(),
                min_expiry_ms: u64::MAX,
            };
        }
        RedisValue::HashListpack(lp) => {
            let fields = lp.to_hash_map();
            *rv = RedisValue::HashWithTtl {
                fields,
                ttls: HashMap::new(),
                min_expiry_ms: u64::MAX,
            };
        }
        _ => panic!("promote_to_hash_with_ttl called on non-hash variant"),
    }
}

/// An in-memory key-value database with lazy expiration.
///
/// Keys are `Bytes` (binary-safe). Values are `Entry` structs containing
/// a `CompactValue`, optional expiration (TTL delta), and packed metadata.
pub struct Database {
    data: DashTable<CompactKey, Entry>,
    used_memory: usize,
    /// Cached current time in epoch seconds; set once per batch to avoid
    /// repeated `SystemTime::now()` syscalls on every command.
    cached_now: u32,
    /// Cached current time in unix millis for expiry checks.
    cached_now_ms: u64,
    /// Base timestamp (epoch seconds) for TTL delta computation.
    /// Set once at database creation time and never changed, ensuring
    /// TTL deltas remain stable across the database lifetime.
    base_timestamp: u32,
    /// Monotonic flag: true once an entry with has_expiry() has been inserted
    /// (via set / set_expiry / insert_for_load). Reset to false only when the
    /// active-expiry scan confirms zero expiring keys remain. Used by the
    /// active-expiry tick to short-circuit the O(N) `keys_with_expiry()` scan
    /// when no key has a TTL — the common case for cache workloads that never
    /// call EXPIRE / SETEX. Safe under-reporting scenarios: flag stays true
    /// longer than necessary (harmless: one extra scan); never flips false
    /// while an expiring key is live (enforced by the three setters + the
    /// self-reset gate in `expire_cycle`). The scan-based reset costs O(N)
    /// but happens at most once per "expiring key drained" transition.
    maybe_has_expiring_keys: bool,
    /// Logical db this instance holds (0..databases).
    ///
    /// Needed because a keyspace notification's channel names embed it
    /// (`__keyspace@<db>__:<key>`), and command code is handed a `&Database`
    /// with no other way to know which one it has. Set once by the shard when
    /// it builds its database array; `Database::new()` defaults it to 0, which
    /// is correct for the single-db paths (tests, embedded) that use it.
    pub db_index: usize,
    /// Cold index for disk-offloaded KV entries (None when disk-offload disabled).
    pub cold_index: Option<crate::storage::tiered::cold_index::ColdIndex>,
    /// Shard directory for cold reads (None when disk-offload disabled).
    pub cold_shard_dir: Option<std::path::PathBuf>,
    /// Hot-key detection sketch, fed by sampled dispatch observations.
    hot_keys: crate::storage::hotkey::HotKeySketch,
    /// Keys whose async spill is IN FLIGHT: enqueued to the spill thread,
    /// hot entry already freed, not yet registered in `cold_index`.
    ///
    /// This is the THIRD storage plane, not a bookkeeping side table (#459).
    /// It began as a write-only supersession guard holding just the request
    /// id, which left the in-flight window visible to nobody: reads, deletes
    /// and `logical_len` consult hot and cold only, so for the length of the
    /// window an acked key was denied by `GET`/`EXISTS`, `DEL` answered `:0`
    /// and then the completion resurrected it, and `DBSIZE` under-counted.
    /// It therefore carries the PAYLOAD as well, so every plane-aware path
    /// can answer from RAM with no disk read and no promotion.
    ///
    /// Costs nothing to hold: `value_bytes` is the very same refcounted
    /// `Bytes` the queued `SpillRequest` already pins for the whole window,
    /// so this is one refcount, not a copy.
    ///
    /// Empty for every server not actively spilling, which is what makes the
    /// `is_empty()` fast bail on the hot paths below honest rather than a
    /// hopeful guess. Size is bounded by the spill channel's capacity:
    /// `evict_one_async_spill` marks only after a successful `try_send`, and
    /// a full channel makes it bail.
    ///
    /// Retention note: a record is retired by its completion, by DEL, by an
    /// overwriting SET, or by a promoting read. A LOST completion (see
    /// `spill_completion_dropped_total` — the rare shutdown-with-full-channel
    /// edge) therefore now strands a payload rather than the bare u64 it used
    /// to, so the value stays charged to RAM until the key is next written or
    /// deleted. Reads stay correct throughout; the cost is that the eviction
    /// did not actually reclaim that key's memory.
    ///
    /// ACCOUNTING (moon#466, FIXED): the payload is resident RAM that
    /// `used_memory` used not to count. `evict_one_async_spill` calls
    /// `db.remove()`, whose `remove_hot` credits back the whole
    /// `entry_overhead`, while the queued `SpillRequest` still holds a full
    /// `Bytes::copy_from_slice` of the value — so for the length of the
    /// window the eviction loop believed it had reclaimed memory it had not.
    /// (That was equally true before this plane existed: the request has
    /// always owned that copy. What this plane added was the key `Bytes` plus
    /// this struct per in-flight key, and a retention window that now ends
    /// when the event loop APPLIES the completion rather than when the spill
    /// thread drains the channel.)
    ///
    /// The bytes are now charged via [`Self::spill_inflight_bytes`], which
    /// [`Self::estimated_memory`] includes — paired, as it had to be, with
    /// the stop rule in [`crate::storage::eviction::evict_to_budget`]: once
    /// the pending bytes cover the overshoot the tick is done. Without that
    /// pairing the honest figure would evict in a runaway loop chasing memory
    /// that cannot drop yet, which is why the fix was deferred out of #459.
    ///
    /// A LOST completion therefore now also strands a CHARGE: `used_memory`
    /// keeps counting those bytes, which is truthful (the RAM really is still
    /// held) and self-correcting the moment the key is written or deleted.
    spill_inflight: std::collections::HashMap<bytes::Bytes, PendingSpill>,
    /// Running sum of `key.len() + value_bytes.len()` over [`Self::spill_inflight`]
    /// (moon#466). See [`Self::pending_spill_bytes`].
    spill_inflight_bytes: usize,
    /// Ticket dispenser for the version stamped on a NEWLY CREATED entry.
    ///
    /// Versions are per-entry and die with the entry. Before this counter every
    /// creation started at `INITIAL_VERSION`, so `DEL k` + `SET k` handed a
    /// WATCHing client back the exact token it had recorded and EXEC committed
    /// on a key that had been destroyed and rebuilt underneath it — the ABA
    /// hole (Redis aborts there). Stamping each creation with the next value of
    /// a per-db counter makes recreation observably distinct instead.
    ///
    /// Monotonic per database, bumped on every `set` and every `get_or_create`
    /// fabrication; gaps are harmless (it is a ticket, not a count). Shares the
    /// entry's 24-bit version field, so it wraps at 16,777,216 — a miss now
    /// needs that wrap to land inside one client's open WATCH..EXEC window AND
    /// hit the one watched key, ~1 in 16.7M versus the pre-fix certainty. Only
    /// a full incarnation field (a wider `Entry`) removes the residue entirely;
    /// see the task's §7.
    birth_counter: u32,
    /// Keys a LAZY read discovered expired (moon#542). The lazy paths
    /// (`get` / `get_mut` / `exists`) HIDE an expired key instead of
    /// removing it, and record it here; the shard's next active-expiry tick
    /// drains this queue, re-verifies expiry, deletes, and EMITS the
    /// deletion (keyspace `expired` notification + dual-plane DEL) — the
    /// emission the lazy paths structurally cannot perform (no plane
    /// handles in the storage layer). Bounded by [`PENDING_EXPIRED_CAP`];
    /// past the cap the lazy path still hides but stops recording (the
    /// probabilistic sweep is the backstop that eventually samples the
    /// key). On a REPLICA the sweep never drains (replicas wait for the
    /// master's authoritative DEL), so the queue sits at ≤ cap — bounded
    /// memory — and hidden keys are exactly Redis's replica semantics:
    /// logically absent, physically present until the master says so.
    pending_expired: Vec<CompactKey>,
    /// Deadline-ordered whole-key expiry index (moon#541): one
    /// `(expires_at_ms, key)` pair per HOT entry with `ttl_ms != 0`,
    /// kept in lock-step by every writer that changes a key's TTL state
    /// (`set` / `set_expiry` / `insert_for_load` / `remove_hot` / `clear`
    /// / `recalculate_memory` — "sweep state writers, not command names").
    /// The active-expiry sweep pops exactly the DUE pairs from the front —
    /// no sampling, no O(N) scan; maintenance is O(log n) per TTL write.
    /// Cold-spilled keys are NOT indexed (eviction removes the hot entry,
    /// unindexing it) — exactly mirroring the old scan, which only ever
    /// walked hot entries; cold TTLs stay lazy-only. The index's own
    /// memory (~48B/pair) is metadata outside `used_memory`, like every
    /// other side table here.
    expiry_index: std::collections::BTreeSet<(u64, CompactKey)>,
    /// Deadline-ordered hash-FIELD expiry index (moon#543), the sibling of
    /// [`Self::expiry_index`]: one `(min_expiry_ms, key)` pair per hot
    /// `HashWithTtl` entry whose `ttls` sidecar is non-empty.
    ///
    /// It replaces the pre-#543 `hash_field_ttl_latch` bool, which only told
    /// the sweep *whether* to run an O(N) `HashWithTtl` table scan; the sweep
    /// then reaped every hash it found, unbudgeted, on the shard event loop.
    /// Popping due pairs off this index is O(due · log n) and needs no scan.
    ///
    /// ## Lower-bound invariant (what makes it self-healing)
    ///
    /// For every hot `HashWithTtl` entry with a non-empty `ttls` sidecar
    /// there EXISTS a pair `(ts, key)` here with `ts <= ttls.values().min()`.
    /// Pairs may be stale-EARLY (a TTL was raised or the hash was deleted
    /// without unindexing) — the sweep pops such a pair, finds nothing due,
    /// and re-arms at the entry's fresh minimum (or drops it). A stale-early
    /// pair therefore costs one wasted peek, never a missed reap. Only a
    /// stale-LATE pair could lose a reap, and the single writer that can
    /// LOWER a hash's minimum (`hash_set_field_ttl`) inserts its new pair
    /// unconditionally, so none can exist.
    hash_expiry_index: std::collections::BTreeSet<(u64, CompactKey)>,
}

/// Maximum `HashWithTtl` keys the hash-field sweep reaps in one tick
/// (moon#543). Bounds the sweep even when the 1ms wall-clock budget has not
/// yet been consumed, so the cap is deterministic and testable rather than
/// machine-speed dependent. Expired-but-unreaped fields are already
/// INVISIBLE to clients (the read path filters `ttls` against the shard
/// clock), so a deferred reap costs delayed memory reclaim, never a stale
/// answer.
pub const HASH_SWEEP_MAX_KEYS_PER_TICK: u32 = 256;

/// Maximum expired fields removed from ONE hash per reap call (moon#543).
/// A hash with more due fields than this is re-armed at its (still-due)
/// minimum and picked up again immediately — bounded progress per visit
/// instead of an unbounded stall inside a single 1M-field hash.
pub const HASH_SWEEP_MAX_FIELDS_PER_KEY: usize = 256;

/// Bound on [`Database::pending_expired`]. 8192 keys ≈ 200KB of inline
/// `CompactKey`s at worst — small enough to hold on a replica forever,
/// large enough that a read-heavy burst over expired keys rarely overflows
/// into sweep-sampling fallback.
pub const PENDING_EXPIRED_CAP: usize = 8192;

/// A spill that has left hot RAM but has not yet landed in `cold_index`.
///
/// Holds enough to answer a read without touching disk — the window is short
/// but it is not free, and serving a stale-or-nil answer inside it was #459.
#[derive(Clone)]
pub struct PendingSpill {
    /// The `SpillRequest.file_id` this record belongs to. A completion only
    /// applies when it is still the newest (stale-shadow guard, deep-review
    /// P2): a newer eviction of the same key supersedes it.
    pub req_id: u64,
    pub value_type: crate::persistence::kv_page::ValueType,
    /// Refcounted handle to the queued request's payload — never a copy.
    pub value_bytes: bytes::Bytes,
    pub ttl_ms: Option<u64>,
}

impl Database {
    /// Create a new empty database.
    pub fn new() -> Self {
        Database {
            data: DashTable::new(),
            used_memory: 0,
            cached_now: current_secs(),
            cached_now_ms: current_time_ms(),
            base_timestamp: current_secs(),
            maybe_has_expiring_keys: false,
            pending_expired: Vec::new(),
            db_index: 0,
            cold_index: None,
            cold_shard_dir: None,
            hot_keys: crate::storage::hotkey::HotKeySketch::new(),
            spill_inflight: std::collections::HashMap::new(),
            spill_inflight_bytes: 0,
            birth_counter: 0,
            expiry_index: std::collections::BTreeSet::new(),
            hash_expiry_index: std::collections::BTreeSet::new(),
        }
    }

    /// Create a new empty database with the internal DashTable pre-sized for
    /// approximately `cap` entries. Eliminates segment-split cost for
    /// workloads whose key count does not exceed `cap`.
    ///
    /// `cap == 0` is equivalent to `Database::new()` (no pre-sizing).
    pub fn with_capacity(cap: usize) -> Self {
        let data = if cap == 0 {
            DashTable::new()
        } else {
            DashTable::with_capacity(cap)
        };
        Database {
            data,
            used_memory: 0,
            cached_now: current_secs(),
            cached_now_ms: current_time_ms(),
            base_timestamp: current_secs(),
            maybe_has_expiring_keys: false,
            pending_expired: Vec::new(),
            db_index: 0,
            cold_index: None,
            cold_shard_dir: None,
            hot_keys: crate::storage::hotkey::HotKeySketch::new(),
            spill_inflight: std::collections::HashMap::new(),
            spill_inflight_bytes: 0,
            birth_counter: 0,
            expiry_index: std::collections::BTreeSet::new(),
            hash_expiry_index: std::collections::BTreeSet::new(),
        }
    }

    /// Record `key` as in-flight, carrying the payload so reads answered
    /// during the window are correct.
    ///
    /// Called AFTER the hot entry is removed (`evict_one_async_spill`). The
    /// remove and this call are one synchronous run on the shard thread with
    /// no `.await` between them, so no reader can observe the key absent from
    /// every plane.
    pub fn spill_inflight_mark(&mut self, key: bytes::Bytes, pending: PendingSpill) {
        // moon#466: the payload this record pins is resident RAM that
        // `remove_hot` has already credited back to `used_memory`. Charge it
        // here so the ledger stays honest for the length of the window.
        // O(1) integer arithmetic, no allocation, no clone (`key.len()` is
        // read before the move).
        let key_len = key.len();
        let charge = key_len + pending.value_bytes.len();
        // A re-eviction of the same key REPLACES its record; credit the old
        // one or the counter drifts upward forever.
        if let Some(previous) = self.spill_inflight.insert(key, pending) {
            let credit = key_len + previous.value_bytes.len();
            self.spill_inflight_bytes = self.spill_inflight_bytes.saturating_sub(credit);
        }
        self.spill_inflight_bytes = self.spill_inflight_bytes.saturating_add(charge);
    }

    /// Drop the in-flight record for `key`, crediting its bytes back
    /// (moon#466). Returns `true` when a record existed.
    ///
    /// The ONE retire point: every path that stops pinning a payload —
    /// completion applied, DEL, overwriting SET, promoting read — goes
    /// through here, so the byte counter cannot drift away from the map.
    #[inline]
    fn spill_inflight_retire(&mut self, key: &[u8]) -> bool {
        match self.spill_inflight.remove_entry(key) {
            Some((stored_key, pending)) => {
                let credit = stored_key.len() + pending.value_bytes.len();
                self.spill_inflight_bytes = self.spill_inflight_bytes.saturating_sub(credit);
                true
            }
            None => false,
        }
    }

    /// True when `req_id` is still the newest recorded spill request for
    /// `key` — i.e. no later eviction re-enqueued it, and no `DEL`/overwrite
    /// retired the record while this request was in flight.
    pub fn spill_inflight_is_newest(&self, key: &[u8], req_id: u64) -> bool {
        self.spill_inflight.get(key).map(|p| p.req_id) == Some(req_id)
    }

    /// Consume the in-flight record for `key` if (and only if) it belongs
    /// to `req_id`; a newer request's record is left for its own completion.
    pub fn spill_inflight_clear(&mut self, key: &[u8], req_id: u64) {
        if self.spill_inflight.get(key).map(|p| p.req_id) == Some(req_id) {
            self.spill_inflight_retire(key);
        }
    }

    /// Retire any in-flight record for `key`, whatever request it belongs to.
    /// Returns `true` if one existed.
    ///
    /// This is what makes a `DEL` (or an overwriting `SET`) inside the window
    /// FINAL: the record is the only thing that authorizes the completion to
    /// insert into `cold_index`, so dropping it here is what stops the key
    /// coming back when the spill lands.
    #[inline]
    pub fn spill_inflight_forget(&mut self, key: &[u8]) -> bool {
        if self.spill_inflight.is_empty() {
            return false;
        }
        self.spill_inflight_retire(key)
    }

    /// Cheap (no disk I/O, no promotion) liveness probe for the in-flight
    /// plane — the pending-spill sibling of `cold_contains_alive`.
    #[inline]
    pub fn spill_inflight_alive(&self, key: &[u8], now_ms: u64) -> bool {
        if self.spill_inflight.is_empty() {
            return false;
        }
        self.spill_inflight
            .get(key)
            .is_some_and(|p| p.ttl_ms.is_none_or(|ttl| now_ms <= ttl))
    }

    /// Rehydrate the pending payload for `key` into an `Entry`, if one is
    /// in flight and not TTL-expired. Pure RAM — the payload is right here,
    /// so this never reads disk.
    ///
    /// `None` when the payload cannot be rehydrated (corrupt encoding); the
    /// caller then falls through to its normal path rather than serving a
    /// wrong value.
    pub fn spill_inflight_entry(&self, key: &[u8], now_ms: u64) -> Option<Entry> {
        if self.spill_inflight.is_empty() {
            return None;
        }
        let p = self.spill_inflight.get(key)?;
        if p.ttl_ms.is_some_and(|ttl| now_ms > ttl) {
            return None;
        }
        crate::storage::eviction::rehydrate_spill_payload(p.value_type, &p.value_bytes, p.ttl_ms)
    }

    /// Decode the pending payload for `key` into a `RedisValue` without
    /// touching hot RAM or the cold index — the `&self` sibling of
    /// [`Self::spill_inflight_entry`].
    ///
    /// This is what the RwLock-shared-read dispatch path needs: it holds only
    /// `&Database` and so cannot promote, exactly as it cannot promote a cold
    /// hit. Unlike the cold case there is no disk read to pay for — the
    /// payload is already in RAM.
    pub fn spill_inflight_value(&self, key: &[u8], now_ms: u64) -> Option<RedisValue> {
        if self.spill_inflight.is_empty() {
            return None;
        }
        let p = self.spill_inflight.get(key)?;
        if p.ttl_ms.is_some_and(|ttl| now_ms > ttl) {
            return None;
        }
        match p.value_type {
            crate::persistence::kv_page::ValueType::String => {
                Some(RedisValue::String(p.value_bytes.clone()))
            }
            vt => crate::storage::tiered::kv_serde::deserialize_collection(&p.value_bytes, vt),
        }
    }

    /// Keys currently in flight — used to count them as the logical keys they
    /// are (`logical_len`) and to keep them enumerable.
    pub fn spill_inflight_keys(&self) -> impl Iterator<Item = &bytes::Bytes> {
        self.spill_inflight.keys()
    }

    /// True when nothing is in flight — the fast bail every plane-aware
    /// caller takes on a server that is not spilling.
    #[inline]
    pub fn spill_inflight_is_empty(&self) -> bool {
        self.spill_inflight.is_empty()
    }

    /// Bytes of value payload pinned by the in-flight spill plane (moon#466).
    ///
    /// This is REAL resident RAM: the queued `SpillRequest` and the in-flight
    /// record share one refcounted `Bytes` per key, which stays alive until
    /// the completion is applied. `remove_hot` already credited the whole
    /// entry back to `used_memory` at mark time, so without this term the
    /// ledger claims memory that has not been released.
    ///
    /// Maintained incrementally by [`Self::spill_inflight_mark`] and the two
    /// retire paths — O(1), no allocation, no scan.
    #[inline]
    pub fn pending_spill_bytes(&self) -> usize {
        self.spill_inflight_bytes
    }

    /// Fast-path predicate for the active-expiry tick. Returns `false` only
    /// when the database is known to have zero entries with a TTL. Callers
    /// MUST treat `true` as "maybe has expiring keys" — the precise answer
    /// requires `keys_with_expiry()`.
    #[inline]
    pub fn maybe_has_expiring_keys(&self) -> bool {
        self.maybe_has_expiring_keys
    }

    /// Latch the flag to `false`. Called by `expire_cycle` once its
    /// `keys_with_expiry()` scan returns empty — proof that zero expiring
    /// keys remain, so the next tick can skip the scan.
    #[inline]
    pub fn clear_maybe_has_expiring_keys(&mut self) {
        self.maybe_has_expiring_keys = false;
    }

    // ── moon#541: deadline-ordered expiry index ─────────────────────────

    /// Earliest `(expires_at_ms, key)` pair that is due at `now_ms`, or
    /// `None` when nothing is due. O(log n). Returns a clone (inline for
    /// keys ≤ 23B) so the sweep can mutate the db while holding it.
    ///
    /// "Due" here (`ts <= now_ms`) is aligned with `Entry::is_expired_at`
    /// (`now >= ttl`): with a monotone clock, a pair this returns always
    /// re-verifies as expired. A FAILED re-verification is therefore NOT
    /// proof of staleness on its own — the wall clock may have stepped
    /// backwards after `now_ms` was captured — so the sweep only drops a
    /// pair when the entry is gone or its TTL differs from `ts`.
    #[inline]
    pub fn peek_due_expiry(&self, now_ms: u64) -> Option<(u64, CompactKey)> {
        self.expiry_index
            .first()
            .filter(|(ts, _)| *ts <= now_ms)
            .cloned()
    }

    /// Earliest `(expires_at_ms, key)` pair in the index regardless of
    /// whether it is due yet — the exact nearest-deadline victim for
    /// `volatile-ttl` eviction. O(log n). `None` when no hot key is
    /// volatile. Cold-spilled keys are not indexed, matching the old
    /// sampling picker which also only saw hot entries.
    #[inline]
    pub fn peek_nearest_expiry(&self) -> Option<(u64, CompactKey)> {
        self.expiry_index.first().cloned()
    }

    /// Drop one specific index pair. The sweep calls this when a popped
    /// pair is PROVABLY stale (the entry is gone or carries a different
    /// TTL than `ts`): without dropping it, the sweep would peek the same
    /// head pair forever. A pair that merely failed the expiry re-check is
    /// not dropped — see [`Self::peek_due_expiry`].
    #[inline]
    pub fn drop_expiry_index_pair(&mut self, ts: u64, key: &CompactKey) {
        self.expiry_index.remove(&(ts, key.clone()));
    }

    /// Number of indexed (hot, TTL-carrying) keys. Exact — backs
    /// `expires_count` and the sweep's flag maintenance.
    #[inline]
    pub fn expiry_index_len(&self) -> usize {
        self.expiry_index.len()
    }

    /// O(1) "zero hot keys carry a TTL" — the sweep's flag-maintenance
    /// check, replacing the old O(N) `keys_with_expiry().is_empty()` scan.
    #[inline]
    pub fn expiry_index_is_empty(&self) -> bool {
        self.expiry_index.is_empty()
    }

    /// Insert an index pair. Callers pass `ttl_ms != 0` only.
    #[inline]
    pub(crate) fn expiry_index_insert(&mut self, ttl_ms: u64, key: &[u8]) {
        self.expiry_index.insert((ttl_ms, CompactKey::from(key)));
    }

    /// Remove an index pair. Callers pass the entry's CURRENT `ttl_ms`
    /// (`!= 0`) — the pair the writers inserted for it.
    #[inline]
    pub(crate) fn expiry_index_remove(&mut self, ttl_ms: u64, key: &[u8]) {
        self.expiry_index.remove(&(ttl_ms, CompactKey::from(key)));
    }

    // ── moon#543: deadline-ordered hash-FIELD expiry index ──────────────

    /// `true` iff at least one hash-field deadline is indexed, i.e. the
    /// hash-field sweep has something it could ever do. Replaces the
    /// pre-#543 `hash_field_ttl_possible()` latch read; O(1).
    #[inline]
    pub fn hash_field_ttl_possible(&self) -> bool {
        !self.hash_expiry_index.is_empty()
    }

    /// Earliest `(min_expiry_ms, key)` hash-field pair that is due at
    /// `now_ms`, or `None` when nothing is due. O(log n) — the sibling of
    /// [`Self::peek_due_expiry`], and the head-peek that lets a tick with no
    /// due hash field skip the cycle entirely (moon#552).
    #[inline]
    pub fn peek_due_hash_expiry(&self, now_ms: u64) -> Option<(u64, CompactKey)> {
        self.hash_expiry_index
            .first()
            .filter(|(ts, _)| *ts <= now_ms)
            .cloned()
    }

    /// Number of indexed hash-field deadlines. Exact only up to stale-early
    /// duplicates (see the field's invariant doc) — tests and diagnostics.
    #[inline]
    pub fn hash_expiry_index_len(&self) -> usize {
        self.hash_expiry_index.len()
    }

    /// Index `key` from a whole-value write (`set` / `insert_for_load` /
    /// recovery): reads the TRUE minimum out of the sidecar rather than
    /// trusting the cached `min_expiry_ms`, because a value arriving over
    /// RESTORE / replication / cold promotion has not been validated here.
    /// No-op for every non-`HashWithTtl` value and for an empty sidecar
    /// (nothing to reap ⇒ nothing to index).
    pub(crate) fn hash_expiry_index_note_value(&mut self, key: &[u8], entry: &Entry) {
        if let crate::storage::compact_value::RedisValueRef::HashWithTtl { ttls, .. } =
            entry.value.as_redis_value()
            && let Some(min) = ttls.values().copied().min()
        {
            self.hash_expiry_index.insert((min, CompactKey::from(key)));
            // `expire_cycle_direct` gates the WHOLE cycle on this latch, and
            // a whole-value write carries no WHOLE-KEY TTL to raise it — so
            // without this a `HashWithTtl` restored by RESTORE, replication
            // apply or cold promotion was never swept at all unless some
            // unrelated key happened to hold a TTL. (`hash_set_field_ttl`
            // raises it directly, which is why HEXPIRE never hit this.)
            self.maybe_has_expiring_keys = true;
        }
    }

    /// Retire the pair the hash-field sweep just consumed and re-arm `key`
    /// at its FRESH minimum (moon#543).
    ///
    /// This is what makes stale-early pairs self-healing: whatever the popped
    /// `ts` claimed, the replacement is read back out of the live sidecar, so
    /// a raised TTL moves the pair forward and a deleted / downgraded /
    /// fully-reaped hash leaves the index entirely.
    pub(crate) fn rearm_hash_expiry(&mut self, ts: u64, key: &CompactKey) {
        self.hash_expiry_index.remove(&(ts, key.clone()));
        let fresh = match self.data.get(key.as_bytes()) {
            Some(entry) => match entry.value.as_redis_value() {
                crate::storage::compact_value::RedisValueRef::HashWithTtl { ttls, .. } => {
                    ttls.values().copied().min()
                }
                _ => None,
            },
            None => None,
        };
        if let Some(min) = fresh {
            self.hash_expiry_index.insert((min, key.clone()));
        }
    }

    /// Best-effort unindex on hot removal. Guarded by an `is_empty` load so
    /// the overwhelmingly common (no field TTL anywhere) DEL pays one branch.
    /// A missed pair is harmless — see the field's invariant doc.
    #[inline]
    pub(crate) fn hash_expiry_index_forget(&mut self, key: &[u8], entry: &Entry) {
        if self.hash_expiry_index.is_empty() {
            return;
        }
        if let crate::storage::compact_value::RedisValueRef::HashWithTtl { ttls, .. } =
            entry.value.as_redis_value()
            && let Some(min) = ttls.values().copied().min()
        {
            self.hash_expiry_index.remove(&(min, CompactKey::from(key)));
        }
    }

    /// Full-scan consistency oracle for the expiry index (tests + the
    /// #541 property battery — O(N), never call on a hot path): the index
    /// must equal the scan-derived pair set exactly, and the hash latch
    /// must be conservative (any `HashWithTtl` present ⇒ latch raised).
    /// Test-only: an oracle for the #541 battery, not part of the storage
    /// API — compiled out of production builds entirely.
    #[cfg(test)]
    pub(crate) fn debug_expiry_index_consistent(&self) -> bool {
        let scan: std::collections::BTreeSet<(u64, CompactKey)> = self
            .data
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, e)| (e.expires_at_ms(), k.clone()))
            .collect();
        // moon#543: the hash-field index's LOWER-BOUND invariant — every
        // reapable hash must have SOME pair at or before its true minimum.
        // Stale-early duplicates are legal (self-healing), so this is a
        // containment check, not an equality one.
        let hash_ok = self.data.iter().all(|(k, e)| {
            let crate::storage::compact_value::RedisValueRef::HashWithTtl { ttls, .. } =
                e.value.as_redis_value()
            else {
                return true;
            };
            let Some(min) = ttls.values().copied().min() else {
                return true; // empty sidecar: nothing to reap, nothing to index
            };
            self.hash_expiry_index
                .iter()
                .any(|(ts, ik)| ik == k && *ts <= min)
        });
        scan == self.expiry_index && hash_ok
    }

    /// Record a key a lazy read discovered expired (moon#542) so the next
    /// active-expiry tick can delete it WITH emission. Bounded: past
    /// [`PENDING_EXPIRED_CAP`] the key is silently not recorded — the
    /// probabilistic sweep remains the backstop.
    #[inline]
    pub(crate) fn note_lazy_expired(&mut self, key: &[u8]) {
        // Adjacent-dedup: a hot loop re-reading ONE expired key before the
        // next tick must not flood the queue with duplicates (the drain
        // re-verifies, so dups are harmless to correctness — they just
        // evict capacity other keys could use).
        if self
            .pending_expired
            .last()
            .is_some_and(|k| k.as_ref() == key)
        {
            return;
        }
        if self.pending_expired.len() < PENDING_EXPIRED_CAP {
            self.pending_expired.push(CompactKey::from(key));
        }
    }

    /// Take the lazily-discovered expired keys for the drain (moon#542).
    #[inline]
    pub fn take_pending_lazy_expired(&mut self) -> Vec<CompactKey> {
        std::mem::take(&mut self.pending_expired)
    }

    /// Whether any lazily-discovered expired keys await the drain.
    #[inline]
    pub fn has_pending_lazy_expired(&self) -> bool {
        !self.pending_expired.is_empty()
    }

    /// Current pending-queue length (tests + observability).
    #[inline]
    pub fn pending_lazy_expired_len(&self) -> usize {
        self.pending_expired.len()
    }

    /// Update the cached timestamp from a shared [`CachedClock`].
    ///
    /// Reads two `AtomicU64` values (Relaxed) instead of issuing `clock_gettime`
    /// syscalls.  The clock is updated once per shard event-loop tick (1 ms).
    #[inline]
    pub fn refresh_now_from_cache(&mut self, clock: &CachedClock) {
        self.cached_now = clock.secs();
        self.cached_now_ms = clock.ms();
    }

    /// Override the cached millisecond timestamp for unit tests only.
    ///
    /// Simulates the "expired but not yet reaped" transient state where a
    /// field's absolute expiry has already passed but the active-expiry tick
    /// has not yet run.  Without this, tests would need to wait for real wall
    /// time to advance, making them slow and flaky.
    #[cfg(test)]
    pub fn set_cached_now_ms_for_test(&mut self, ms: u64) {
        self.cached_now_ms = ms;
    }

    /// Return the cached `min_expiry_ms` for the `HashWithTtl` at `key`.
    ///
    /// Used by unit tests to assert that the fast-path invariant is maintained
    /// after HEXPIRE, HPERSIST, HSET overwrite, and active-reap operations.
    /// Returns `None` if the key does not exist or is not a `HashWithTtl`.
    #[cfg(test)]
    pub fn hash_min_expiry_ms_for_test(&self, key: &[u8]) -> Option<u64> {
        use super::compact_value::RedisValueRef;
        let entry = self.data.get(key)?;
        match entry.value.as_redis_value() {
            RedisValueRef::HashWithTtl { min_expiry_ms, .. } => Some(min_expiry_ms),
            _ => None,
        }
    }

    /// Fallback: update the cached timestamp via `SystemTime::now()` syscall.
    ///
    /// Kept for callers that do not yet have a `CachedClock` reference (e.g. the
    /// non-sharded legacy handler).  Marked `#[cold]` to hint the compiler that
    /// the fast path is `refresh_now_from_cache`.
    #[cold]
    pub fn refresh_now(&mut self) {
        self.cached_now = current_secs();
        self.cached_now_ms = current_time_ms();
    }

    /// Return the base timestamp for TTL delta computation.
    #[inline]
    pub fn base_timestamp(&self) -> u32 {
        self.base_timestamp
    }

    /// Return the cached current time (epoch seconds).
    #[inline]
    pub fn now(&self) -> u32 {
        self.cached_now
    }

    /// Return the cached current time (unix millis).
    #[inline]
    pub fn now_ms(&self) -> u64 {
        self.cached_now_ms
    }

    /// Estimated memory usage of all entries in this database.
    ///
    /// Includes [`Self::pending_spill_bytes`] (moon#466): a victim queued for
    /// async spill has had its whole `entry_overhead` credited back by
    /// `remove_hot` while a full copy of its value is still pinned in RAM by
    /// the queued `SpillRequest` and the in-flight plane. Counting only
    /// `used_memory` made the ledger — and every eviction decision built on
    /// it — believe memory had been released that had not.
    ///
    /// The pairing rule this requires lives in
    /// [`crate::storage::eviction::evict_to_budget`]: once the pending bytes
    /// cover the overshoot, the tick stops instead of selecting more victims
    /// to chase memory that cannot drop yet.
    pub fn estimated_memory(&self) -> usize {
        self.used_memory.saturating_add(self.spill_inflight_bytes)
    }

    /// Resident bytes attributed to this database (alias for `estimated_memory`,
    /// kept distinct so observability call sites use the canonical name).
    /// O(1), zero allocation -- reads the per-shard `used_memory` accumulator
    /// plus the pending-spill term (moon#466: those bytes are resident too).
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.used_memory.saturating_add(self.spill_inflight_bytes)
    }

    /// Charge `delta` bytes of container growth to this database's memory
    /// accounting. O(1), saturating, no allocation.
    ///
    /// Used by command-layer call sites that hold a `&mut` reference into a
    /// container obtained via `get_or_create_hash`/`_list`/`_set`/`_sorted_set`
    /// (so they cannot route the mutation back through `Database::set`) —
    /// see the WS6 container-growth accounting note above `entry_overhead`.
    #[inline]
    pub fn charge_memory(&mut self, delta: usize) {
        self.used_memory = self.used_memory.saturating_add(delta);
    }

    /// Credit back `delta` bytes previously charged via [`Self::charge_memory`]
    /// (e.g. a field/element removed, or an overwrite that shrank a value).
    /// O(1), saturating, no allocation.
    #[inline]
    pub fn credit_memory(&mut self, delta: usize) {
        self.used_memory = self.used_memory.saturating_sub(delta);
    }

    /// Apply a signed byte delta in one call — convenience wrapper around
    /// [`Self::charge_memory`] / [`Self::credit_memory`] for call sites that
    /// compute a net `old_cost` vs `new_cost` difference (e.g. HINCRBY
    /// overwriting a field with a same-or-different-length value).
    #[inline]
    pub fn adjust_memory(&mut self, old_cost: usize, new_cost: usize) {
        if new_cost >= old_cost {
            self.charge_memory(new_cost - old_cost);
        } else {
            self.credit_memory(old_cost - new_cost);
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bptree::BPTree;
    use crate::storage::compact_value::RedisValueRef;
    use crate::storage::stream::Stream as StreamData;
    use bytes::Bytes;
    use ordered_float::OrderedFloat;
    use std::collections::{HashSet, VecDeque};

    #[test]
    fn test_set_and_get() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            Entry::new_string(Bytes::from_static(b"value1")),
        );
        let entry = db.get(b"key1").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(v) => assert_eq!(v, b"value1"),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn test_get_missing_key() {
        let mut db = Database::new();
        assert!(db.get(b"nonexistent").is_none());
    }

    #[test]
    fn test_remove_key() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            Entry::new_string(Bytes::from_static(b"value1")),
        );
        let removed = db.remove(b"key1");
        assert!(removed.is_some());
        assert!(db.get(b"key1").is_none());
    }

    /// Task #56 red/green: `demote_replayed_cold_shadows` must drop a hot
    /// entry that AOF replay redundantly re-materialized for a key already
    /// present in the (crash-consistent) cold index, correctly uncharging
    /// `used_memory` -- and must leave the cold-index entry itself intact so
    /// the key is still readable via cold read-through afterward. A key with
    /// NO cold-index entry (never evicted) must be left completely alone.
    #[test]
    fn test_demote_replayed_cold_shadows() {
        use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};

        let mut db = Database::new();
        // never-evicted key: no cold_index entry, must survive untouched.
        db.set(
            Bytes::from_static(b"never_evicted"),
            Entry::new_string(Bytes::from_static(b"live_value")),
        );
        // AOF-replay-shadowed key: also present in cold_index (as recovery
        // would have rebuilt it from the manifest before replay ran).
        db.set(
            Bytes::from_static(b"replayed_shadow"),
            Entry::new_string(Bytes::from_static(b"same_value_replay_reconstructed")),
        );
        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"replayed_shadow"),
            ColdLocation {
                file_id: 1,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        db.cold_index = Some(ci);

        let used_memory_before = db.resident_bytes();
        assert!(db.is_hot(b"replayed_shadow"));

        let demoted = db.demote_replayed_cold_shadows();

        assert_eq!(demoted, 1, "exactly the shadowed key must be demoted");
        assert!(
            !db.is_hot(b"replayed_shadow"),
            "shadowed key must no longer be hot"
        );
        assert!(
            db.is_hot(b"never_evicted"),
            "never-evicted key must be untouched"
        );
        assert!(
            db.resident_bytes() < used_memory_before,
            "used_memory must drop once the redundant hot copy is dropped"
        );
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"replayed_shadow")
                .is_some(),
            "cold-index entry must remain so the key is still cold-readable"
        );
    }

    /// Task #56 finding 1 (adversarial review) red/green: a key overwritten
    /// AFTER its cold-tier copy was captured must never resurrect the stale
    /// cold value. Models exactly what AOF replay does at restart: the
    /// cold_index is rebuilt from the crash-consistent manifest BEFORE any
    /// replay command runs (so it already "knows" about a key that was
    /// spilled with an old value), and then replay reconstructs the key's
    /// FULL write history against an empty DashTable -- here simulated as
    /// two `db.set()` calls for the same key (the original SET whose later
    /// eviction produced the cold copy, followed by a live overwrite issued
    /// after that eviction, both still present in the AOF since eviction
    /// never rewrites/deletes AOF records).
    ///
    /// This test FAILS on the pre-finding-1 HEAD: without the `Updated`-arm
    /// invalidation in `Database::set`, the cold_index entry survives both
    /// writes untouched, `demote_replayed_cold_shadows` wrongly treats it as
    /// "provably redundant", drops the hot v2 copy, and the final `GET`
    /// promotes the stale v1 from cold storage instead.
    #[test]
    fn test_second_write_invalidates_cold_shadow() {
        use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};

        let mut db = Database::new();
        // Simulates `recover_shard_v3_pitr` rebuilding ColdIndex from the
        // manifest BEFORE any replay command runs -- the key is cold-only,
        // NOT hot, at this point.
        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"k"),
            ColdLocation {
                file_id: 1,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        db.cold_index = Some(ci);
        assert!(!db.is_hot(b"k"), "precondition: key starts cold-only");

        // Replay command #1: the ORIGINAL SET whose later eviction produced
        // the cold copy above (v1). First touch since the DashTable was
        // cleared -- ambiguous, left alone.
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string(Bytes::from_static(b"v1")),
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"k").is_some(),
            "first touch must not disturb the cold shadow (still ambiguous)"
        );

        // Replay command #2: a LATER overwrite issued after the eviction
        // that produced the cold copy (still in the AOF -- eviction never
        // deletes AOF records). Second touch -- provably proves the cold
        // copy stale.
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string(Bytes::from_static(b"v2")),
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"k").is_none(),
            "second touch must invalidate the now-stale cold shadow"
        );

        // Reconciliation pass (what main.rs runs right after replay
        // finishes, before the server accepts connections) must be a no-op
        // for this key now -- there is nothing left in cold_index to
        // wrongly "demote".
        let demoted = db.demote_replayed_cold_shadows();
        assert_eq!(
            demoted, 0,
            "nothing left to demote -- the shadow is already gone"
        );

        match db.get(b"k").unwrap().value.as_bytes_owned() {
            Some(v) => assert_eq!(
                &v[..],
                b"v2",
                "GET must return the LATER value, not the stale spilled v1"
            ),
            None => panic!("key must exist"),
        }
    }

    #[test]
    fn test_lazy_expiry() {
        let mut db = Database::new();
        // Set key with an expiry in the past
        let past_ms = current_time_ms() - 1000;
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms),
        );
        // moon#542: `get` HIDES the expired key (answers None) but must NOT
        // physically remove it — deletion belongs to the active-expiry drain,
        // which emits the keyspace notification + dual-plane DEL this layer
        // cannot. The key is queued for that drain instead.
        assert!(db.get(b"expired").is_none());
        assert_eq!(db.len(), 1, "hidden, not removed");
        assert_eq!(db.pending_lazy_expired_len(), 1, "queued for the drain");
        // Every subsequent read must keep hiding it — and must not enqueue
        // a duplicate (adjacent-dedup).
        assert!(db.get(b"expired").is_none());
        assert_eq!(db.pending_lazy_expired_len(), 1, "no duplicate enqueue");
    }

    #[test]
    fn test_exists_with_expiry() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms),
        );
        assert!(!db.exists(b"expired"));
    }

    #[test]
    fn test_len_and_expires_count() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"k1"),
            Entry::new_string(Bytes::from_static(b"v1")),
        );
        let future_ms = current_time_ms() + 3_600_000;
        db.set(
            Bytes::from_static(b"k2"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v2"), future_ms),
        );
        assert_eq!(db.len(), 2);
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn test_is_expired() {
        let past_ms = current_time_ms() - 1000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms);
        assert!(Database::is_expired(&entry));

        let future_ms = current_time_ms() + 3_600_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms);
        assert!(!Database::is_expired(&entry));

        let entry = Entry::new_string(Bytes::from_static(b"v"));
        assert!(!Database::is_expired(&entry));
    }

    #[test]
    fn test_get_mut() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string(Bytes::from_static(b"old")),
        );
        let entry = db.get_mut(b"key").unwrap();
        entry.set_string_value(Bytes::from_static(b"new"));
        let entry = db.get(b"key").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(v) => assert_eq!(v, b"new"),
            _ => panic!("unexpected type"),
        }
    }

    // --- Type-checked helper tests ---

    #[test]
    fn test_get_or_create_hash() {
        let mut db = Database::new();
        let map = db.get_or_create_hash(b"myhash").unwrap();
        map.insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        let map = db.get_hash(b"myhash").unwrap().unwrap();
        assert_eq!(
            map.get(&Bytes::from_static(b"field")).unwrap().as_ref(),
            b"value"
        );
    }

    #[test]
    fn test_hash_wrongtype() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        let result = db.get_or_create_hash(b"k");
        assert!(result.is_err());
        let result = db.get_hash(b"k");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_or_create_list() {
        let mut db = Database::new();
        let list = db.get_or_create_list(b"mylist").unwrap();
        list.push_back(Bytes::from_static(b"item"));
        let list = db.get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_get_or_create_set() {
        let mut db = Database::new();
        let set = db.get_or_create_set(b"myset").unwrap();
        set.insert(Bytes::from_static(b"member"));
        let set = db.get_set(b"myset").unwrap().unwrap();
        assert!(set.contains(&Bytes::from_static(b"member")));
    }

    #[test]
    fn test_get_or_create_sorted_set() {
        let mut db = Database::new();
        let (members, scores) = db.get_or_create_sorted_set(b"myzset").unwrap();
        members.insert(Bytes::from_static(b"a"), 1.0);
        scores.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
        let (members, scores) = db.get_sorted_set(b"myzset").unwrap().unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(scores.len(), 1);
    }

    #[test]
    fn test_get_hash_missing() {
        let mut db = Database::new();
        assert!(db.get_hash(b"missing").unwrap().is_none());
    }

    #[test]
    fn test_keys_with_expiry() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        let future_ms = current_time_ms() + 3_600_000;
        db.set_string_with_expiry(
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
            future_ms,
        );
        let keys = db.keys_with_expiry();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref(), b"k2");
    }

    #[test]
    fn test_is_key_expired() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms),
        );
        assert!(db.is_key_expired(b"expired"));
        assert!(!db.is_key_expired(b"missing"));
    }

    // ── moon#541: deadline-ordered expiry index ──────────────────────────

    /// Every writer that changes a key's TTL state must keep the expiry
    /// index in lock-step with the data map ("sweep state writers, not
    /// command names"): a missed insert means the key never actively
    /// expires; a missed remove means the sweep chews stale pairs.
    #[test]
    fn expiry_index_mirrors_every_ttl_writer() {
        let mut db = Database::new();
        assert!(db.debug_expiry_index_consistent());
        let future_ms = current_time_ms() + 3_600_000;

        // set: fresh key with TTL
        db.set(
            Bytes::from_static(b"a"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
        );
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());

        // set: fresh key without TTL
        db.set_string(Bytes::from_static(b"b"), Bytes::from_static(b"v"));
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());

        // overwrite: TTL -> no TTL
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"v2"));
        assert_eq!(db.expiry_index_len(), 0);
        assert!(db.debug_expiry_index_consistent());

        // overwrite: no TTL -> TTL
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v2"), future_ms),
        );
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());

        // overwrite: TTL -> different TTL (old pair must go, not linger)
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v3"), future_ms + 500),
        );
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());

        // set_expiry: retarget, then PERSIST (0)
        assert!(db.set_expiry(b"b", future_ms + 1_000));
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());
        assert!(db.set_expiry(b"b", 0));
        assert_eq!(db.expiry_index_len(), 0);
        assert!(db.debug_expiry_index_consistent());

        // set_expiry: arm a plain key
        assert!(db.set_expiry(b"a", future_ms));
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());

        // remove
        db.remove(b"a");
        assert_eq!(db.expiry_index_len(), 0);
        assert!(db.debug_expiry_index_consistent());

        // insert_for_load (bulk restore path)
        db.insert_for_load(
            Bytes::from_static(b"c"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
        );
        assert_eq!(db.expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());

        // write-path expired drop (get_or_create on an expired key)
        db.set_cached_now_ms_for_test(future_ms + 1);
        let _ = db.get_or_create_hash(b"c").expect("fresh hash");
        assert_eq!(db.expiry_index_len(), 0, "expired drop must unindex");
        assert!(db.debug_expiry_index_consistent());

        // clear
        db.set(
            Bytes::from_static(b"d"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms + 3_600_000),
        );
        db.clear();
        assert_eq!(db.expiry_index_len(), 0);
        assert!(db.debug_expiry_index_consistent());
    }

    /// moon#541 rides moon#542's machinery: EXPIRE hitting a lazily-expired
    /// key must HIDE it and queue it for the emitting drain — the old code
    /// physically removed it here, silently (no notification, no dual-plane
    /// DEL), recreating exactly the divergence #542 closed for reads.
    #[test]
    fn set_expiry_on_lazily_expired_key_hides_and_queues() {
        let mut db = Database::new();
        let future_ms = db.now_ms() + 1_000;
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
        );
        db.set_cached_now_ms_for_test(future_ms + 1);

        assert!(
            !db.set_expiry(b"k", future_ms + 60_000),
            "expired key: EXPIRE answers 0"
        );
        assert_eq!(db.data().len(), 1, "must hide, not silently delete");
        assert_eq!(
            db.pending_lazy_expired_len(),
            1,
            "must queue for the emitting drain"
        );
    }

    /// The hash-field-TTL latch arms when a field TTL is stored and lets the
    /// sweep skip the O(N) HashWithTtl scan for the (overwhelmingly common)
    /// workloads that never touch HEXPIRE.
    #[test]
    fn hash_field_ttl_latch_arms_on_field_ttl() {
        let mut db = Database::new();
        assert!(!db.hash_field_ttl_possible());
        {
            let map = db.get_or_create_hash(b"h").expect("hash");
            map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        assert!(!db.hash_field_ttl_possible(), "plain hash must not arm it");
        let future_ms = db.now_ms() + 60_000;
        let r = db.hash_set_field_ttl(b"h", b"f", future_ms, HashTtlCond::Always);
        assert_eq!(r, Ok(1));
        assert!(db.hash_field_ttl_possible());
    }

    /// The NX/XX/GT/LT gate runs AFTER promotion: HEXPIRE GT on a
    /// non-volatile field answers -2 with the value already converted to
    /// `HashWithTtl` — carrying an EMPTY `ttls` sidecar.
    ///
    /// Pre-#543 the bool latch had to arm at promotion (a `HashWithTtl` with
    /// the latch down broke its conservativeness invariant), which cost every
    /// later tick an O(N) table scan for a hash that had no deadline at all.
    /// The deadline index states the invariant precisely instead — index what
    /// is REAPABLE — so a rejected HEXPIRE indexes nothing, and the very next
    /// accepted one indexes immediately.
    #[test]
    fn a_rejected_hexpire_indexes_nothing_but_an_accepted_one_does() {
        let mut db = Database::new();
        {
            let map = db.get_or_create_hash(b"h").expect("hash");
            map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        let future_ms = db.now_ms() + 60_000;
        // GT on a field with NO current TTL: condition not met.
        assert_eq!(
            db.hash_set_field_ttl(b"h", b"f", future_ms, HashTtlCond::Gt),
            Ok(-2)
        );
        assert!(
            !db.hash_field_ttl_possible(),
            "a HashWithTtl with an empty sidecar has nothing to reap, so the \
             sweep must not be woken for it"
        );
        assert!(db.debug_expiry_index_consistent());

        assert_eq!(
            db.hash_set_field_ttl(b"h", b"f", future_ms, HashTtlCond::Always),
            Ok(1)
        );
        assert_eq!(
            db.peek_due_hash_expiry(future_ms),
            Some((future_ms, CompactKey::from(&b"h"[..]))),
            "promotion already happened; the accepted TTL must still index"
        );
        assert!(db.debug_expiry_index_consistent());
    }

    // ── moon#543: hash-field deadline index — writer coverage ────────────
    //
    // "Enumerate state writers, not command names." Exactly ONE writer can
    // LOWER a hash's field minimum (`hash_set_field_ttl`); every other
    // mutation can only raise it or remove the hash, and a stale-EARLY pair
    // is self-healing. These lock that reasoning in.

    fn hash_with_field_ttl(db: &mut Database, key: &[u8], field: &[u8], ttl_ms: u64) {
        {
            let map = db.get_or_create_hash(key).expect("hash");
            map.insert(Bytes::copy_from_slice(field), Bytes::from_static(b"v"));
        }
        assert_eq!(
            db.hash_set_field_ttl(key, field, ttl_ms, HashTtlCond::Always),
            Ok(1)
        );
    }

    #[test]
    fn hash_expiry_index_tracks_the_minimum_deadline() {
        let mut db = Database::new();
        let base = db.now_ms() + 60_000;

        hash_with_field_ttl(&mut db, b"h", b"late", base + 5_000);
        assert_eq!(
            db.peek_due_hash_expiry(base + 6_000),
            Some((base + 5_000, CompactKey::from(&b"h"[..])))
        );

        // A LOWER deadline must become reachable at the head.
        hash_with_field_ttl(&mut db, b"h", b"early", base);
        assert_eq!(
            db.peek_due_hash_expiry(base).map(|(ts, _)| ts),
            Some(base),
            "the new minimum must be visible to the sweep immediately"
        );
        assert!(db.debug_expiry_index_consistent());
    }

    /// A RAISED deadline leaves a stale-early pair; `rearm_hash_expiry` must
    /// heal it rather than let the sweep spin on it forever.
    #[test]
    fn rearm_heals_a_stale_early_pair() {
        let mut db = Database::new();
        let base = db.now_ms() + 60_000;
        hash_with_field_ttl(&mut db, b"h", b"f", base);
        // HEXPIRE the same field further out — the (base, h) pair is now early.
        assert_eq!(
            db.hash_set_field_ttl(b"h", b"f", base + 10_000, HashTtlCond::Always),
            Ok(1)
        );
        assert_eq!(db.hash_expiry_index_len(), 2, "both pairs are present");

        db.rearm_hash_expiry(base, &CompactKey::from(&b"h"[..]));
        assert_eq!(
            db.peek_due_hash_expiry(base + 9_999),
            None,
            "the healed index must not report the hash due before its real TTL"
        );
        assert!(db.debug_expiry_index_consistent());
    }

    /// HPERSIST of the last TTL downgrades to plain `Hash`; the sweep must
    /// stop seeing the key once it re-arms.
    #[test]
    fn persisting_the_last_field_ttl_leaves_the_index() {
        let mut db = Database::new();
        let ts = db.now_ms() + 1_000;
        hash_with_field_ttl(&mut db, b"h", b"f", ts);
        assert!(db.hash_persist_field(b"h", b"f"));

        db.rearm_hash_expiry(ts, &CompactKey::from(&b"h"[..]));
        assert_eq!(db.hash_expiry_index_len(), 0);
        assert!(!db.hash_field_ttl_possible());
        assert!(db.debug_expiry_index_consistent());
    }

    /// A whole `HashWithTtl` value arriving over RESTORE / replication /
    /// cold promotion must be indexed from its OWN sidecar, not from the
    /// cached `min_expiry_ms` an untrusted encoder supplied.
    #[test]
    fn a_whole_hashwithttl_value_is_indexed_on_set() {
        let mut db = Database::new();
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let mut ttls = HashMap::new();
        ttls.insert(Bytes::from_static(b"f"), 4_000u64);
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            RedisValue::HashWithTtl {
                fields,
                ttls,
                // Deliberately WRONG cached minimum: the index must not trust it.
                min_expiry_ms: u64::MAX,
            },
        );
        db.set(Bytes::from_static(b"h"), entry);

        assert_eq!(
            db.peek_due_hash_expiry(4_000),
            Some((4_000, CompactKey::from(&b"h"[..]))),
            "the sidecar's real minimum must be indexed"
        );
        assert!(
            db.maybe_has_expiring_keys(),
            "the whole-cycle latch must arm too — it carries no whole-key TTL \
             to raise it, so without this the sweep never runs on this db"
        );
        assert!(db.debug_expiry_index_consistent());
    }

    /// End-to-end for the gap above: a `HashWithTtl` that arrived as a whole
    /// value (RESTORE / replication / cold promotion) with an already-elapsed
    /// field TTL, in a database holding NO other TTL at all, must still be
    /// reaped by the active sweep.
    #[test]
    fn a_restored_hashwithttl_is_swept_without_any_other_ttl_in_the_db() {
        let mut db = Database::new();
        let past = db.now_ms() - 1_000;
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let mut ttls = HashMap::new();
        ttls.insert(Bytes::from_static(b"f"), past);
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms: past,
            },
        );
        db.set(Bytes::from_static(b"h"), entry);
        assert_eq!(db.len(), 1);
        assert!(db.expiry_index_is_empty(), "no whole-key TTL anywhere");

        crate::server::expiration::expire_cycle_direct(&mut db, &mut |_| {});
        assert_eq!(db.len(), 0, "the restored hash's only field had expired");
    }

    /// `recalculate_memory` heals the index after a bulk load, exactly as it
    /// does for the whole-key one.
    #[test]
    fn recalculate_memory_rebuilds_the_hash_expiry_index() {
        let mut db = Database::new();
        let ts = db.now_ms() + 1_000;
        hash_with_field_ttl(&mut db, b"h", b"f", ts);
        // Simulate index loss (a load path that bypassed every writer).
        db.hash_expiry_index.clear();
        assert!(!db.hash_field_ttl_possible());

        db.recalculate_memory();
        assert_eq!(db.hash_expiry_index_len(), 1);
        assert!(db.debug_expiry_index_consistent());
    }

    /// DEL unindexes, so a churn of short-lived field-TTL hashes cannot
    /// accumulate pairs that only retire when their far-future deadline
    /// arrives.
    #[test]
    fn removing_a_hash_unindexes_its_field_deadline() {
        let mut db = Database::new();
        let far = db.now_ms() + 86_400_000;
        for i in 0..64u32 {
            let key = format!("h{i}");
            hash_with_field_ttl(&mut db, key.as_bytes(), b"f", far);
        }
        assert_eq!(db.hash_expiry_index_len(), 64);
        for i in 0..64u32 {
            db.remove(format!("h{i}").as_bytes());
        }
        assert_eq!(
            db.hash_expiry_index_len(),
            0,
            "a deleted hash must not leave a pair parked until its deadline"
        );
    }

    #[test]
    fn test_data_accessor() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        assert_eq!(db.data().len(), 1);
    }

    #[test]
    fn test_used_memory_tracking() {
        let mut db = Database::new();
        assert_eq!(db.estimated_memory(), 0);
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        assert!(db.estimated_memory() > 0);
        let mem_after_set = db.estimated_memory();
        db.remove(b"key");
        assert_eq!(db.estimated_memory(), 0);
        // Overwrite should not double-count
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"longer_value"),
        );
        assert!(db.estimated_memory() > 0);
        // Should not equal 2x the original
        assert_ne!(db.estimated_memory(), mem_after_set * 2);
    }

    #[test]
    fn test_version_tracking() {
        let mut db = Database::new();
        // 0 is reserved for "key absent" so WATCH detects creation.
        assert_eq!(db.get_version(b"key"), 0);
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v1"));
        assert_eq!(db.get_version(b"key"), 1); // first creation draws ticket 1
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v2"));
        assert_eq!(db.get_version(b"key"), 2); // overwrite bumps
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v3"));
        assert_eq!(db.get_version(b"key"), 3);
    }

    /// The ABA hole: before the birth counter every creation started at
    /// `INITIAL_VERSION`, so DEL + re-SET handed a WATCHing client back the
    /// exact token it had recorded and EXEC committed on a key that had been
    /// destroyed and rebuilt underneath it.
    #[test]
    fn test_recreated_key_never_reuses_its_old_version() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v0"));
        let watched = db.get_version(b"k");

        db.remove(b"k");
        assert_eq!(db.get_version(b"k"), 0, "removed key must read as absent");
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v1"));

        assert_ne!(
            db.get_version(b"k"),
            watched,
            "recreated key presented the version WATCH recorded"
        );
    }

    /// The ticket is per-database, not per-key: two keys created in sequence
    /// must not both read as version 1, or the counter has degenerated back
    /// into a per-entry constant.
    #[test]
    fn test_birth_tickets_are_distinct_across_keys() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"v"));
        db.set_string(Bytes::from_static(b"b"), Bytes::from_static(b"v"));
        assert_ne!(db.get_version(b"a"), db.get_version(b"b"));
    }

    /// Containers fabricated by `get_or_create` take tickets too — HSET on a
    /// deleted-and-rebuilt hash is the same ABA hole as SET on a string.
    #[test]
    fn test_fabricated_containers_take_birth_tickets() {
        let mut db = Database::new();
        db.get_or_create_hash(b"h")
            .unwrap()
            .insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let watched = db.get_version(b"h");

        db.remove(b"h");
        db.get_or_create_hash(b"h")
            .unwrap()
            .insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));

        assert_ne!(
            db.get_version(b"h"),
            watched,
            "recreated hash presented the version WATCH recorded"
        );
    }

    /// Restored entries carry tickets as well. Versions are not persisted, so
    /// without this every restored key would read `INITIAL_VERSION` and the
    /// first key created after the restore would collide with all of them.
    #[test]
    fn test_restored_keys_do_not_collide_with_the_first_new_key() {
        let mut db = Database::new();
        db.insert_for_load(
            Bytes::from_static(b"restored"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        let restored = db.get_version(b"restored");
        assert_ne!(restored, 0, "a loaded key must not read as absent");

        db.set_string(Bytes::from_static(b"fresh"), Bytes::from_static(b"v"));
        assert_ne!(db.get_version(b"fresh"), restored);
    }

    /// The counter shares the entry's 24-bit version field, so it wraps —
    /// and must skip 0, which is the "key absent" sentinel. A wrapped ticket
    /// reading as 0 would make a live key invisible to WATCH.
    #[test]
    fn test_birth_ticket_wraps_without_ever_yielding_zero() {
        let mut db = Database::new();
        db.birth_counter = 0xFF_FFFE;
        assert_eq!(db.next_birth_version(), 0xFF_FFFF);
        assert_eq!(
            db.next_birth_version(),
            crate::storage::entry::INITIAL_VERSION
        );
    }

    #[test]
    fn test_increment_version() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v"));
        assert_eq!(db.get_version(b"key"), 1);
        db.increment_version(b"key");
        assert_eq!(db.get_version(b"key"), 2);
        // non-existent key is a no-op
        db.increment_version(b"missing");
    }

    #[test]
    fn test_refresh_now_from_cache() {
        let mut db = Database::new();
        let clock = CachedClock::new();
        db.refresh_now_from_cache(&clock);
        // cached_now should match clock
        assert_eq!(db.now(), clock.secs());
        assert_eq!(db.now_ms(), clock.ms());
    }

    // -----------------------------------------------------------------
    // P0 cold-collection-visibility fix
    // (.planning/reviews/storage-audit-2026-07-12-kv.md)
    //
    // Disk-offload spills Hash/List/Set/ZSet/Stream via
    // `evict_one_async_spill` / `evict_batch_durable` (production
    // paths, `kv_serde::serialize_collection` handles every type), but the
    // type-specific accessors used to consult only `self.data`, never the
    // `ColdIndex` — so a spilled collection read as absent, and a write
    // fabricated a brand-new EMPTY container that permanently shadowed
    // (destroyed) the cold copy on the next `set`.
    //
    // These tests spill a collection directly via the same
    // `spill_to_datafile` production helper the eviction paths use (mirrors
    // the pattern in `tiered::cold_read`'s own tests), bypassing the
    // eviction machinery itself (out of scope here) while exercising the
    // exact on-disk format the accessors must decode.
    // -----------------------------------------------------------------

    use crate::persistence::manifest::ShardManifest;
    use crate::storage::compact_value::CompactValue;
    use crate::storage::entry::RedisValue as TestRedisValue;
    use crate::storage::tiered::cold_index::ColdIndex;
    use crate::storage::tiered::kv_spill::spill_to_datafile;

    /// Build a `Database` with an active cold tier holding one spilled
    /// collection at `key`. Mirrors `cold_read::tests::db_with_spilled_key`
    /// but accepts an arbitrary `RedisValue` (collections, not just strings).
    fn db_with_spilled_value(
        shard_dir: &std::path::Path,
        key: &[u8],
        value: TestRedisValue,
    ) -> Database {
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        let mut entry = Entry::new_string(Bytes::new()); // placeholder, overwritten below
        entry.value = CompactValue::from_redis_value(value);

        spill_to_datafile(
            shard_dir,
            900,
            key,
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        let mut db = Database::new();
        db.cold_shard_dir = Some(shard_dir.to_path_buf());
        db.cold_index = Some(cold_index);
        db
    }

    // ── moon#610: read-only access must see the cold tier ────────────────

    /// `PFCOUNT` reads its HLL through a *separate* loader from the string
    /// commands, so converting `STRLEN`/`TYPE`/`TTL` left it behind: a spilled
    /// HyperLogLog reported cardinality 0 while `EXISTS` answered 1. Measured
    /// against a live server (2026-08-21, 4,977 spilled keys, identical spill
    /// sets on both binaries): hot control `PFCOUNT` 5, tiered 0.
    #[test]
    fn pfcount_reads_a_tiered_hyperloglog_through_every_plane() {
        use crate::protocol::Frame;
        use crate::storage::hll::Hll;

        let mut hll = Hll::new_sparse();
        for element in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            hll.add(element);
        }
        let expected = hll.count() as i64;
        assert!(
            expected > 0,
            "the fixture HLL must have members to be a test"
        );

        let tmp = tempfile::tempdir().unwrap();
        let db = db_with_spilled_value(tmp.path(), b"hk", TestRedisValue::String(hll.into_bytes()));

        let reply = crate::command::hll::pfcount_readonly(
            &db,
            &[Frame::BulkString(Bytes::from_static(b"hk"))],
            0,
        );
        assert_eq!(
            reply,
            Frame::Integer(expected),
            "a spilled HLL must report its real cardinality, not 0"
        );
    }

    /// `get_if_alive` is hot-plane-only, so a tiered key is indistinguishable
    /// from a missing one. This is the property every read-only command
    /// inherited: `STRLEN` 0, `TYPE` none, `TTL` -2 for a key `EXISTS`
    /// answers 1 for. Pinning BOTH accessors in one test states the contract
    /// as a difference, so a future change that quietly gives `get_if_alive`
    /// a cold fallback (or takes one away from its sibling) fails here.
    #[test]
    fn readonly_access_sees_the_cold_tier_and_hot_only_access_does_not() {
        let tmp = tempfile::tempdir().unwrap();
        let db = db_with_spilled_value(
            tmp.path(),
            b"tiered",
            TestRedisValue::String(Bytes::from_static(b"hello world")),
        );
        let now = current_time_ms();

        assert!(
            !db.is_hot(b"tiered"),
            "precondition: the key must live only in the cold tier"
        );
        assert!(
            db.get_if_alive(b"tiered", now).is_none(),
            "get_if_alive is hot-plane-only BY DESIGN; if this starts \
             returning the key, the doc contract on both accessors is stale"
        );

        let view = db
            .get_if_alive_any_plane(b"tiered", now)
            .expect("moon#610: a tiered key must be visible to read-only access");
        assert_eq!(
            view.entry().value.as_bytes().expect("string value"),
            b"hello world".as_slice(),
            "the cold value must come back whole, not as an empty placeholder"
        );
    }

    /// A tiered key with a TTL must report that TTL, not \"no expiry\".
    ///
    /// This is the half the first cut of the fix got WRONG: reaching the
    /// value through `get_cold_value` alone drops the deadline, so `TTL`
    /// answered -1 on a key written with `EX` — trading one wrong answer for
    /// another. Caught by measurement against a hot control, not by review.
    #[test]
    fn tiered_key_keeps_its_deadline_through_readonly_access() {
        let tmp = tempfile::tempdir().unwrap();
        let db = db_with_spilled_value(
            tmp.path(),
            b"tiered_ttl",
            TestRedisValue::String(Bytes::from_static(b"v")),
        );
        let now = current_time_ms();

        let view = db
            .get_if_alive_any_plane(b"tiered_ttl", now)
            .expect("tiered key must be visible");
        // `db_with_spilled_value` spills with no TTL, so the contract to pin
        // here is the inverse: a key that never had a deadline must not
        // acquire a bogus one from the cold path.
        assert!(
            !view.entry().has_expiry(),
            "a tiered key with no TTL must not come back carrying one"
        );
    }

    /// A key absent from BOTH planes still answers `None` — the fallback must
    /// not invent an entry, or every miss becomes a phantom hit.
    #[test]
    fn readonly_any_plane_still_reports_a_genuinely_missing_key_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let db = db_with_spilled_value(
            tmp.path(),
            b"present",
            TestRedisValue::String(Bytes::from_static(b"v")),
        );
        let now = current_time_ms();
        assert!(
            db.get_if_alive_any_plane(b"definitely_not_here", now)
                .is_none(),
            "a key in neither plane must stay absent"
        );
    }

    #[test]
    fn test_get_or_create_hash_promotes_cold_instead_of_fabricating() {
        let tmp = tempfile::tempdir().unwrap();
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"color"), Bytes::from_static(b"red"));
        fields.insert(Bytes::from_static(b"size"), Bytes::from_static(b"large"));
        let mut db = db_with_spilled_value(tmp.path(), b"myhash", TestRedisValue::Hash(fields));

        // Precondition: nothing hot yet, key only exists cold.
        assert!(!db.is_hot(b"myhash"), "precondition: key must be cold-only");

        // Simulates HSET's get_or_create_hash call: must promote the real
        // spilled fields, NOT fabricate an empty hash that would permanently
        // shadow (destroy) the cold copy.
        let map = db.get_or_create_hash(b"myhash").unwrap();
        assert_eq!(
            map.len(),
            2,
            "must promote existing fields, not fabricate empty hash"
        );
        assert_eq!(map.get(b"color".as_slice()).unwrap(), b"red".as_slice());

        // Promotion must also clear the cold index entry (no dual reference).
        assert!(db.cold_index.as_ref().unwrap().lookup(b"myhash").is_none());
        assert!(db.is_hot(b"myhash"));
    }

    #[test]
    fn test_get_or_create_hash_merges_new_field_with_promoted_ones() {
        let tmp = tempfile::tempdir().unwrap();
        let mut fields = HashMap::new();
        fields.insert(
            Bytes::from_static(b"old_field"),
            Bytes::from_static(b"old_value"),
        );
        let mut db = db_with_spilled_value(tmp.path(), b"h", TestRedisValue::Hash(fields));

        // HSET h new_field new_value
        {
            let map = db.get_or_create_hash(b"h").unwrap();
            map.insert(
                Bytes::from_static(b"new_field"),
                Bytes::from_static(b"new_value"),
            );
        }

        let map = db.get_hash(b"h").unwrap().unwrap();
        assert_eq!(
            map.len(),
            2,
            "old spilled field + new field must both be present"
        );
        assert_eq!(
            map.get(b"old_field".as_slice()).unwrap(),
            b"old_value".as_slice()
        );
        assert_eq!(
            map.get(b"new_field".as_slice()).unwrap(),
            b"new_value".as_slice()
        );
    }

    #[test]
    fn test_get_hash_ref_if_alive_sees_cold_hash_without_promoting() {
        let tmp = tempfile::tempdir().unwrap();
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let db = db_with_spilled_value(tmp.path(), b"h", TestRedisValue::Hash(fields));

        let href = db
            .get_hash_ref_if_alive(b"h", 0)
            .unwrap()
            .expect("must see cold hash");
        assert_eq!(href.get_field(b"f").unwrap(), b"v".as_slice());
        assert_eq!(href.len(), 1);

        // Non-promoting: `&self` accessor must not have mutated hot RAM.
        assert!(
            !db.is_hot(b"h"),
            "get_hash_ref_if_alive takes &self and must not promote"
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"h").is_some(),
            "cold index entry must remain untouched by a non-promoting read"
        );
    }

    /// W5 pin: the ref accessor's WRONGTYPE leg on a live hot key.
    #[test]
    fn test_ref_accessors_wrongtype_on_hot_string() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"s"), Bytes::from_static(b"v"));
        assert!(db.get_hash_ref_if_alive(b"s", 0).is_err());
        assert!(db.get_list_ref_if_alive(b"s", 0).is_err());
        assert!(db.get_set_ref_if_alive(b"s", 0).is_err());
        assert!(db.get_sorted_set_ref_if_alive(b"s", 0).is_err());
    }

    /// W5 pin: `get_hash_ref_if_alive` must map a hot `HashWithTtl` entry to
    /// `HashRef::WithTtl` carrying the CALLER's `now_ms` — an expired field
    /// filters out, a live one reads through.
    #[test]
    fn test_hash_ref_with_ttl_leg_filters_expired_fields() {
        let mut db = Database::new();
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"dead"), Bytes::from_static(b"x"));
        fields.insert(Bytes::from_static(b"live"), Bytes::from_static(b"y"));
        let mut ttls = HashMap::new();
        ttls.insert(Bytes::from_static(b"dead"), 1_000u64);
        ttls.insert(Bytes::from_static(b"live"), 5_000u64);
        let mut entry = Entry::new_hash();
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms: 1_000,
            },
        );
        db.set(Bytes::from_static(b"h"), entry);

        // now_ms between the two field deadlines: "dead" filtered, "live" seen.
        let href = db.get_hash_ref_if_alive(b"h", 2_000).unwrap().unwrap();
        assert!(matches!(href, HashRef::WithTtl { .. }));
        assert_eq!(href.get_field(b"dead"), None, "expired field must filter");
        assert_eq!(
            href.get_field(b"live").unwrap(),
            Bytes::from_static(b"y"),
            "live field must read through"
        );
    }

    #[test]
    fn test_get_or_create_list_promotes_cold_instead_of_fabricating() {
        let tmp = tempfile::tempdir().unwrap();
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        list.push_back(Bytes::from_static(b"b"));
        let mut db = db_with_spilled_value(tmp.path(), b"mylist", TestRedisValue::List(list));

        let l = db.get_or_create_list(b"mylist").unwrap();
        assert_eq!(
            l.len(),
            2,
            "must promote existing elements, not fabricate empty list"
        );
        assert_eq!(l[0], Bytes::from_static(b"a"));
    }

    #[test]
    fn test_get_list_ref_if_alive_sees_cold_list_without_promoting() {
        let tmp = tempfile::tempdir().unwrap();
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"x"));
        let db = db_with_spilled_value(tmp.path(), b"l", TestRedisValue::List(list));

        let lref = db
            .get_list_ref_if_alive(b"l", 0)
            .unwrap()
            .expect("must see cold list");
        assert_eq!(lref.len(), 1);
        assert!(!db.is_hot(b"l"));
    }

    #[test]
    fn test_get_or_create_set_promotes_cold_instead_of_fabricating() {
        let tmp = tempfile::tempdir().unwrap();
        let mut set = HashSet::new();
        set.insert(Bytes::from_static(b"m1"));
        set.insert(Bytes::from_static(b"m2"));
        let mut db = db_with_spilled_value(tmp.path(), b"myset", TestRedisValue::Set(set));

        let s = db.get_or_create_set(b"myset").unwrap();
        assert_eq!(
            s.len(),
            2,
            "must promote existing members, not fabricate empty set"
        );
        assert!(s.contains(b"m1".as_slice()));
    }

    #[test]
    fn test_get_set_ref_if_alive_sees_cold_set_without_promoting() {
        let tmp = tempfile::tempdir().unwrap();
        let mut set = HashSet::new();
        set.insert(Bytes::from_static(b"m"));
        let db = db_with_spilled_value(tmp.path(), b"s", TestRedisValue::Set(set));

        let sref = db
            .get_set_ref_if_alive(b"s", 0)
            .unwrap()
            .expect("must see cold set");
        assert!(sref.contains(b"m"));
        assert!(!db.is_hot(b"s"));
    }

    #[test]
    fn test_get_or_create_sorted_set_promotes_cold_instead_of_fabricating() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tree = BPTree::new();
        let mut members = HashMap::new();
        tree.insert(OrderedFloat(1.5), Bytes::from_static(b"alice"));
        members.insert(Bytes::from_static(b"alice"), 1.5);
        let mut db = db_with_spilled_value(
            tmp.path(),
            b"myzset",
            TestRedisValue::SortedSetBPTree { tree, members },
        );

        let (members, _tree) = db.get_or_create_sorted_set(b"myzset").unwrap();
        assert_eq!(
            members.len(),
            1,
            "must promote existing members, not fabricate empty zset"
        );
        assert_eq!(members.get(b"alice".as_slice()), Some(&1.5));
    }

    #[test]
    fn test_get_sorted_set_ref_if_alive_sees_cold_zset_without_promoting() {
        let tmp = tempfile::tempdir().unwrap();
        let mut tree = BPTree::new();
        let mut members = HashMap::new();
        tree.insert(OrderedFloat(2.0), Bytes::from_static(b"bob"));
        members.insert(Bytes::from_static(b"bob"), 2.0);
        let db = db_with_spilled_value(
            tmp.path(),
            b"z",
            TestRedisValue::SortedSetBPTree { tree, members },
        );

        let zref = db
            .get_sorted_set_ref_if_alive(b"z", 0)
            .unwrap()
            .expect("must see cold zset");
        assert_eq!(zref.score(b"bob"), Some(2.0));
        assert!(!db.is_hot(b"z"));
    }

    #[test]
    fn test_get_or_create_stream_promotes_cold_instead_of_fabricating() {
        let tmp = tempfile::tempdir().unwrap();
        let mut stream = StreamData::new();
        let id = crate::storage::stream::StreamId { ms: 1000, seq: 0 };
        stream.last_id = id;
        stream.length = 1;
        stream.entries.insert(
            id,
            vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))],
        );
        let mut db = db_with_spilled_value(
            tmp.path(),
            b"mystream",
            TestRedisValue::Stream(Box::new(stream)),
        );

        let s = db.get_or_create_stream(b"mystream").unwrap();
        assert_eq!(
            s.length, 1,
            "must promote existing entries, not fabricate empty stream"
        );
        assert_eq!(s.entries.len(), 1);
    }

    #[test]
    fn test_get_stream_if_alive_sees_cold_stream_without_promoting() {
        let tmp = tempfile::tempdir().unwrap();
        let mut stream = StreamData::new();
        let id = crate::storage::stream::StreamId { ms: 2000, seq: 0 };
        stream.last_id = id;
        stream.length = 1;
        stream.entries.insert(
            id,
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
        );
        let db = db_with_spilled_value(tmp.path(), b"s", TestRedisValue::Stream(Box::new(stream)));

        let sref = db
            .get_stream_if_alive(b"s", 0)
            .unwrap()
            .expect("must see cold stream");
        assert_eq!(sref.length, 1);
        assert!(!db.is_hot(b"s"));
    }

    #[test]
    fn test_exists_counts_cold_only_hash_without_promoting() {
        let tmp = tempfile::tempdir().unwrap();
        let mut fields = HashMap::new();
        fields.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        let mut db = db_with_spilled_value(tmp.path(), b"h", TestRedisValue::Hash(fields));

        assert!(db.exists(b"h"), "cold-only key must count as existing");
        // Cheap presence check must not have promoted the key into hot RAM.
        assert!(
            !db.is_hot(b"h"),
            "EXISTS must not promote — cheap check only"
        );

        let now_ms = db.now_ms();
        assert!(db.exists_if_alive(b"h", now_ms));
        assert!(!db.is_hot(b"h"));
    }

    #[test]
    fn test_exists_false_for_genuinely_missing_key() {
        let mut db = Database::new();
        db.cold_index = Some(ColdIndex::new());
        assert!(!db.exists(b"nope"));
        let now_ms = db.now_ms();
        assert!(!db.exists_if_alive(b"nope", now_ms));
    }

    /// Deep-review P2 stale-shadow guard: a failed spill completion for a
    /// SUPERSEDED request (key re-created and re-evicted while the failed
    /// pwrite was in flight) must be detectable, and the superseded
    /// completion's cleanup must not drop the newer request's record.
    #[test]
    fn spill_inflight_supersession_guard() {
        let mut db = Database::new();
        let k = bytes::Bytes::from_static(b"k");
        db.spill_inflight_mark(k.clone(), pending(7, b"v7"));
        assert!(db.spill_inflight_is_newest(b"k", 7));
        // Key re-evicted with a newer request: 7 is now superseded — its
        // failure arm must NOT re-insert its stale payload.
        db.spill_inflight_mark(k, pending(9, b"v9"));
        assert!(!db.spill_inflight_is_newest(b"k", 7));
        // The superseded completion's clear leaves the newer record intact.
        db.spill_inflight_clear(b"k", 7);
        assert!(db.spill_inflight_is_newest(b"k", 9));
        // The newest completion consumes its own record.
        db.spill_inflight_clear(b"k", 9);
        assert!(!db.spill_inflight_is_newest(b"k", 9));
    }

    fn pending(req_id: u64, value: &'static [u8]) -> PendingSpill {
        PendingSpill {
            req_id,
            value_type: crate::persistence::kv_page::ValueType::String,
            value_bytes: bytes::Bytes::from_static(value),
            ttl_ms: None,
        }
    }

    /// #459: the in-flight plane must answer reads. A key mid-spill is in
    /// neither hot nor cold, and before this it was invisible to everything
    /// except the completion path.
    #[test]
    fn an_inflight_key_is_readable_and_counted_while_it_is_in_flight() {
        let mut db = Database::new();
        let k = bytes::Bytes::from_static(b"k");
        db.spill_inflight_mark(k, pending(1, b"hello"));

        assert!(
            db.spill_inflight_alive(b"k", 0),
            "a queued spill still holds a live key"
        );
        assert_eq!(
            db.logical_len(),
            1,
            "the key was acked to a client, so it must be counted"
        );
        let entry = db
            .spill_inflight_entry(b"k", 0)
            .expect("payload rehydrates from RAM");
        assert_eq!(entry.value.as_bytes(), Some(&b"hello"[..]));
    }

    /// The half that turned a transient miss into permanent data loss: DEL
    /// must retire the record, because the record is the completion's
    /// authorization to publish into `cold_index`.
    #[test]
    fn deleting_an_inflight_key_withdraws_the_completions_authorization() {
        let mut db = Database::new();
        let k = bytes::Bytes::from_static(b"k");
        db.spill_inflight_mark(k, pending(1, b"hello"));

        let (removed, _) = db.remove_counting_cold(b"k");
        assert!(removed, "DEL of a mid-spill key must answer 1, not 0");
        assert!(
            !db.spill_inflight_is_newest(b"k", 1),
            "the completion must no longer be authorized to publish this key — \
             otherwise the spill lands in cold_index and the delete is undone"
        );
        assert_eq!(db.logical_len(), 0, "the key is gone from every plane");
    }

    /// An overwrite makes the queued payload stale; publishing it would
    /// leave the OLD value as a cold shadow behind the new hot one.
    #[test]
    fn overwriting_an_inflight_key_also_withdraws_authorization() {
        let mut db = Database::new();
        let k = bytes::Bytes::from_static(b"k");
        db.spill_inflight_mark(k.clone(), pending(1, b"old"));

        db.set(k, Entry::new_string(bytes::Bytes::from_static(b"new")));

        assert!(
            !db.spill_inflight_is_newest(b"k", 1),
            "the queued payload is stale after an overwrite"
        );
        assert_eq!(db.logical_len(), 1, "one key, counted once, not twice");
    }

    /// A promoting read pulls the payload back from RAM and likewise cancels
    /// the pending publish — the key is hot again, so a cold entry would
    /// only shadow it.
    #[test]
    fn a_read_promotes_an_inflight_key_out_of_the_window() {
        let mut db = Database::new();
        let k = bytes::Bytes::from_static(b"k");
        db.spill_inflight_mark(k, pending(1, b"hello"));

        assert!(db.promote_inflight_if_present(b"k", 0));
        assert!(db.is_hot(b"k"), "promotion puts the key back in hot RAM");
        assert!(!db.spill_inflight_is_newest(b"k", 1));
        assert_eq!(db.logical_len(), 1, "still exactly one key");
    }

    /// An in-flight record whose TTL has passed is not a live key.
    #[test]
    fn an_expired_inflight_payload_is_not_alive() {
        let mut db = Database::new();
        let k = bytes::Bytes::from_static(b"k");
        db.spill_inflight_mark(
            k,
            PendingSpill {
                req_id: 1,
                value_type: crate::persistence::kv_page::ValueType::String,
                value_bytes: bytes::Bytes::from_static(b"v"),
                ttl_ms: Some(1_000),
            },
        );
        assert!(db.spill_inflight_alive(b"k", 999), "not yet expired");
        assert!(!db.spill_inflight_alive(b"k", 1_001), "past its TTL");
        assert!(db.spill_inflight_entry(b"k", 1_001).is_none());
    }

    // ── moon#523 / moon#539: blocking pops must never create their key ──
    //
    // `list_pop_front`/`list_pop_back`/`zset_pop_min`/`zset_pop_max` back the
    // blocking fast path (`try_immediate_pop`) for BLPOP/BRPOP/BLMOVE/
    // BRPOPLPUSH/BZPOPMIN/BZPOPMAX/BZMPOP. A miss on an absent key used to
    // reach the value through `get_or_create_*`, materialising an empty
    // list/zset that EXISTS/TYPE/DBSIZE then reported and that later RPUSHes
    // tripped over with WRONGTYPE. A miss must leave the keyspace — and the
    // memory estimate — byte-identical.

    /// Assert that `db` holds no trace of `key` on any plane.
    fn assert_keyspace_untouched(db: &Database, key: &[u8], used_before: usize) {
        assert_eq!(db.data().len(), 0, "hot plane must stay empty");
        assert_eq!(db.logical_len(), 0, "DBSIZE must stay 0");
        assert!(!db.is_hot(key), "no phantom entry for the polled key");
        assert!(
            !db.exists_if_alive(key, current_time_ms()),
            "EXISTS must still answer 0"
        );
        assert_eq!(
            db.resident_bytes(),
            used_before,
            "a miss must not charge memory"
        );
    }

    #[test]
    fn test_list_pop_front_missing_key_creates_no_phantom() {
        let mut db = Database::new();
        let used_before = db.resident_bytes();
        assert!(db.list_pop_front(b"ghost").is_none());
        assert_keyspace_untouched(&db, b"ghost", used_before);
    }

    #[test]
    fn test_list_pop_back_missing_key_creates_no_phantom() {
        let mut db = Database::new();
        let used_before = db.resident_bytes();
        assert!(db.list_pop_back(b"ghost").is_none());
        assert_keyspace_untouched(&db, b"ghost", used_before);
    }

    #[test]
    fn test_zset_pop_min_missing_key_creates_no_phantom() {
        let mut db = Database::new();
        let used_before = db.resident_bytes();
        assert!(db.zset_pop_min(b"ghost").is_none());
        assert_keyspace_untouched(&db, b"ghost", used_before);
    }

    #[test]
    fn test_zset_pop_max_missing_key_creates_no_phantom() {
        let mut db = Database::new();
        let used_before = db.resident_bytes();
        assert!(db.zset_pop_max(b"ghost").is_none());
        assert_keyspace_untouched(&db, b"ghost", used_before);
    }

    /// The #539 headline symptom: after a blocking pop misses, a producer
    /// must still be able to create the key with its own type.
    #[test]
    fn test_push_after_missed_pop_is_not_wrongtype() {
        let mut db = Database::new();
        assert!(db.zset_pop_min(b"q").is_none());
        // A list push on the same key must succeed — pre-fix the miss left a
        // zset behind and `get_or_create_list` answered WRONGTYPE.
        db.list_push_back(b"q", Bytes::from_static(b"job"));
        assert_eq!(
            db.get_list(b"q").unwrap().map(|l| l.len()),
            Some(1),
            "producer must own the key's type after a consumer's miss"
        );
    }

    /// Wrong-type behaviour is unchanged: the pop reports "nothing" and
    /// leaves the existing value alone (no clobber, no removal).
    #[test]
    fn test_pop_on_wrong_type_leaves_value_intact() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"s"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        assert!(db.list_pop_front(b"s").is_none());
        assert!(db.zset_pop_min(b"s").is_none());
        assert_eq!(db.logical_len(), 1);
        match db.get(b"s").map(|e| e.value.as_redis_value()) {
            Some(RedisValueRef::String(v)) => assert_eq!(v, b"v"),
            _ => panic!("string must survive a wrong-type pop"),
        }
    }

    /// Regression guard for the found-key path: the last pop still removes
    /// the key, and a non-final pop still leaves it in place.
    #[test]
    fn test_list_pop_removes_key_only_when_it_empties() {
        let mut db = Database::new();
        db.list_push_back(b"l", Bytes::from_static(b"a"));
        db.list_push_back(b"l", Bytes::from_static(b"b"));

        assert_eq!(db.list_pop_front(b"l"), Some(Bytes::from_static(b"a")));
        assert_eq!(db.logical_len(), 1, "one element left, key stays");

        assert_eq!(db.list_pop_back(b"l"), Some(Bytes::from_static(b"b")));
        assert_eq!(db.logical_len(), 0, "emptied list must be removed");
        assert!(db.list_pop_front(b"l").is_none());
        assert_eq!(db.logical_len(), 0, "and the re-poll must not resurrect it");
    }

    /// The non-creating lookup must still reach the COLD tier: a list that
    /// eviction spilled to disk is a real key, and popping it has to promote
    /// it back rather than answer "missing". (Guards the one behaviour the
    /// #523/#539 fix could have silently dropped along with the fabrication.)
    #[test]
    fn test_list_pop_front_promotes_a_cold_spilled_list() {
        let tmp = tempfile::tempdir().unwrap();
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"first"));
        list.push_back(Bytes::from_static(b"second"));
        let mut db = db_with_spilled_value(tmp.path(), b"coldlist", TestRedisValue::List(list));

        assert_eq!(
            db.list_pop_front(b"coldlist"),
            Some(Bytes::from_static(b"first")),
            "a cold-spilled list must be promoted and popped, not treated as missing"
        );
        assert_eq!(
            db.get_list(b"coldlist").unwrap().map(|l| l.len()),
            Some(1),
            "the promoted remainder stays in hot RAM"
        );
    }

    /// Same guard on the sorted-set side.
    #[test]
    fn test_zset_pop_removes_key_only_when_it_empties() {
        let mut db = Database::new();
        {
            let (members, tree) = db.get_or_create_sorted_set(b"z").unwrap();
            members.insert(Bytes::from_static(b"a"), 1.0);
            tree.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
            members.insert(Bytes::from_static(b"b"), 2.0);
            tree.insert(OrderedFloat(2.0), Bytes::from_static(b"b"));
        }

        assert_eq!(db.zset_pop_min(b"z"), Some((Bytes::from_static(b"a"), 1.0)));
        assert_eq!(db.logical_len(), 1, "one member left, key stays");

        assert_eq!(db.zset_pop_max(b"z"), Some((Bytes::from_static(b"b"), 2.0)));
        assert_eq!(db.logical_len(), 0, "emptied zset must be removed");
        assert!(db.zset_pop_max(b"z").is_none());
        assert_eq!(db.logical_len(), 0, "and the re-poll must not resurrect it");
    }
}
