use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

pub use super::db_read::{HashRef, ListRef, SetRef, SortedSetRef, StreamRef};

use super::bptree::BPTree;
use super::compact_key::CompactKey;
use super::compact_value::{CompactValue, RedisValueRef};
use super::dashtable::{DashTable, InsertOrUpdate};
use super::entry::{CachedClock, Entry, RedisValue, current_secs, current_time_ms};
use super::intset::Intset;
use super::stream::Stream as StreamData;
use crate::protocol::Frame;

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
    /// Cold index for disk-offloaded KV entries (None when disk-offload disabled).
    pub cold_index: Option<crate::storage::tiered::cold_index::ColdIndex>,
    /// Shard directory for cold reads (None when disk-offload disabled).
    pub cold_shard_dir: Option<std::path::PathBuf>,
    /// Hot-key detection sketch, fed by sampled dispatch observations.
    hot_keys: crate::storage::hotkey::HotKeySketch,
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
            cold_index: None,
            cold_shard_dir: None,
            hot_keys: crate::storage::hotkey::HotKeySketch::new(),
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
            cold_index: None,
            cold_shard_dir: None,
            hot_keys: crate::storage::hotkey::HotKeySketch::new(),
        }
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

    // -- HEXPIRE family (phase 195 / issue #106) ------------------------------
    //
    // Storage primitives for Valkey 9.0 HEXPIRE-family parity. Consumed by
    // phases 196 (write commands), 198 (read commands), 199 (atomic compound).
    //
    // Storage strategy: auto-promote to `RedisValue::HashWithTtl` on first
    // per-field TTL touch. Downgrade back to plain `Hash` when the last TTL
    // is removed via `hash_persist_field`. No promotion of HashListpack
    // happens unless an actual TTL is being set — pure reads stay cheap.

    /// Set per-field TTL (absolute unix-ms). Returns the per-field result code:
    /// `0` = no such field, `1` = TTL set, `2` = expired during this call,
    /// `-2` = NX/XX/GT/LT condition not met. `Err(WrongType)` if key exists and
    /// is not a hash.
    pub fn hash_set_field_ttl(
        &mut self,
        key: &[u8],
        field: &[u8],
        ts_ms: u64,
        cond: HashTtlCond,
    ) -> Result<i64, WrongType> {
        // 1. Key existence + type check.
        let Some(entry) = self.data.get_mut(key) else {
            return Ok(0);
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            // Inline string or heap string — wrong type.
            return Err(WrongType);
        };
        match rv {
            RedisValue::Hash(_) | RedisValue::HashListpack(_) | RedisValue::HashWithTtl { .. } => {}
            _ => return Err(WrongType),
        }

        // 2. Field-existence pre-check (avoids unnecessary promotion).
        let field_exists = match rv {
            RedisValue::Hash(map) => map.contains_key(field),
            RedisValue::HashListpack(lp) => lp.iter_pairs().any(|(f, _)| f.as_bytes() == field),
            RedisValue::HashWithTtl { fields, .. } => fields.contains_key(field),
            _ => unreachable!("type-checked above"),
        };
        if !field_exists {
            return Ok(0);
        }

        // 3. Past-expiry short-circuit: delete the field, return code 2.
        if ts_ms <= self.cached_now_ms {
            // O(1) credit for the field removed below (see the WS6 accounting
            // note above `entry_overhead`); the `HashListpack` branch instead
            // diffs `estimate_memory()` before/after since it also changes
            // encoding (listpack -> Hash), an O(n) transform it already pays.
            let mut credit: usize = 0;
            match rv {
                RedisValue::Hash(map) => {
                    if let Some(v) = map.remove(field) {
                        credit = hash_field_cost(field, &v);
                    }
                }
                RedisValue::HashListpack(lp) => {
                    let before = lp.estimate_memory();
                    // Promote to Hash to delete (listpack delete-by-key is awkward).
                    let mut map = lp.to_hash_map();
                    map.remove(field);
                    *rv = RedisValue::Hash(map);
                    let after = rv.estimate_memory();
                    credit = before.saturating_sub(after);
                }
                RedisValue::HashWithTtl {
                    fields,
                    ttls,
                    min_expiry_ms,
                } => {
                    let old_ttl = ttls.remove(field);
                    if let Some(v) = fields.remove(field) {
                        credit = hash_field_cost(field, &v);
                    }
                    if ttls.is_empty() {
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                    } else if old_ttl == Some(*min_expiry_ms) {
                        // The removed field held the min; recompute.
                        *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                    }
                }
                _ => unreachable!(),
            }
            if credit > 0 {
                self.credit_memory(credit);
            }
            return Ok(2);
        }

        // 4. Promote to HashWithTtl if needed.
        promote_to_hash_with_ttl(rv);
        let RedisValue::HashWithTtl {
            ttls,
            min_expiry_ms,
            ..
        } = rv
        else {
            unreachable!("just promoted")
        };

        // 5. Apply NX/XX/GT/LT conditional gate.
        // Valkey semantics: a non-volatile field is treated as +∞ for GT/LT.
        let current = ttls.get(field).copied();
        let pass = match cond {
            HashTtlCond::Always => true,
            HashTtlCond::Nx => current.is_none(),
            HashTtlCond::Xx => current.is_some(),
            HashTtlCond::Gt => current.is_some_and(|c| ts_ms > c),
            HashTtlCond::Lt => current.is_some_and(|c| ts_ms < c) || current.is_none(),
        };
        if !pass {
            return Ok(-2);
        }

        // 6. Set the TTL and maintain the cached minimum.
        ttls.insert(Bytes::copy_from_slice(field), ts_ms);
        if ts_ms < *min_expiry_ms {
            *min_expiry_ms = ts_ms;
        }
        self.maybe_has_expiring_keys = true;
        Ok(1)
    }

    /// Read absolute expiry ms for a field. `None` for missing field or no TTL.
    pub fn hash_get_field_ttl_ms(&self, key: &[u8], field: &[u8]) -> Option<u64> {
        let entry = self.data.get(key)?;
        match entry.value.as_redis_value() {
            RedisValueRef::HashWithTtl { ttls, .. } => ttls.get(field).copied(),
            _ => None,
        }
    }

    /// Tri-state field-existence + TTL state lookup used by HTTL / HEXPIRETIME
    /// and the HEXPIRE conditional gates.
    pub fn hash_field_state(&self, key: &[u8], field: &[u8], _now_ms: u64) -> FieldState {
        let Some(entry) = self.data.get(key) else {
            return FieldState::Missing;
        };
        match entry.value.as_redis_value() {
            RedisValueRef::Hash(map) => {
                if map.contains_key(field) {
                    FieldState::NoTtl
                } else {
                    FieldState::Missing
                }
            }
            RedisValueRef::HashListpack(lp) => {
                if lp.iter_pairs().any(|(f, _)| f.as_bytes() == field) {
                    FieldState::NoTtl
                } else {
                    FieldState::Missing
                }
            }
            RedisValueRef::HashWithTtl { fields, ttls, .. } => {
                if !fields.contains_key(field) {
                    FieldState::Missing
                } else if let Some(&ms) = ttls.get(field) {
                    FieldState::Ttl(ms)
                } else {
                    FieldState::NoTtl
                }
            }
            _ => FieldState::Missing,
        }
    }

    /// Remove the TTL from a field. Returns `true` if the field had a TTL
    /// (and it was removed); `false` if the field was missing or had no TTL.
    /// Downgrades `HashWithTtl` back to plain `Hash` when the last TTL is
    /// removed.
    pub fn hash_persist_field(&mut self, key: &[u8], field: &[u8]) -> bool {
        let Some(entry) = self.data.get_mut(key) else {
            return false;
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return false;
        };
        let RedisValue::HashWithTtl {
            fields,
            ttls,
            min_expiry_ms,
        } = rv
        else {
            return false;
        };
        let removed = ttls.remove(field);
        let had_ttl = removed.is_some();
        if had_ttl {
            if ttls.is_empty() {
                let m = std::mem::take(fields);
                *rv = RedisValue::Hash(m);
            } else if removed == Some(*min_expiry_ms) {
                // Removed field held the minimum; recompute from remaining entries.
                *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
            }
        }
        had_ttl
    }

    /// Clear the TTL sidecar entries for every field in `fields`. For plain
    /// `Hash` and `HashListpack` this is a no-op (they carry no TTLs).
    /// For `HashWithTtl`, removes the sidecar entry for each field; downgrades
    /// to plain `Hash` when the last TTL is removed.
    ///
    /// Called by HSET / HMSET when they overwrite a field: the new write
    /// unconditionally persists the field (Valkey semantics — HSET clears TTL).
    pub fn hash_clear_field_ttls<F>(&mut self, key: &[u8], fields: &[F])
    where
        F: AsRef<[u8]>,
    {
        let Some(entry) = self.data.get_mut(key) else {
            return;
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return;
        };
        let RedisValue::HashWithTtl {
            fields: _,
            ttls,
            min_expiry_ms,
        } = rv
        else {
            // Plain Hash or HashListpack carries no per-field TTLs.
            return;
        };
        // Track whether any removed TTL equaled the cached minimum.
        // If so, recompute after all removals rather than re-scanning per step.
        let mut min_invalidated = false;
        for f in fields {
            if let Some(t) = ttls.remove(f.as_ref()) {
                if t == *min_expiry_ms {
                    min_invalidated = true;
                }
            }
        }
        if ttls.is_empty() {
            // Downgrade: borrow ends, then re-borrow to swap variant.
            let Some(rv2) = entry.value.as_redis_value_mut() else {
                return;
            };
            if let RedisValue::HashWithTtl { fields: fmap, .. } = rv2 {
                let m = std::mem::take(fmap);
                *rv2 = RedisValue::Hash(m);
            }
        } else if min_invalidated {
            *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
        }
    }

    /// Remove a single field from a hash, cleaning up the TTL sidecar when
    /// present.  Returns `(removed, hash_now_empty)`:
    /// - `removed = true` if the field existed and was deleted.
    /// - `hash_now_empty = true` if the hash has no more fields after deletion.
    ///
    /// Returns `Err(wrongtype_error())` if the key is not a hash.
    /// Returns `Ok((false, false))` for a missing key.
    pub fn hash_delete_field(&mut self, key: &[u8], field: &[u8]) -> Result<(bool, bool), Frame> {
        let Some(entry) = self.data.get_mut(key) else {
            return Ok((false, false));
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return Err(Self::wrongtype_error());
        };
        // Accumulated O(1) credit for this call; applied to `used_memory`
        // once at the end (single field mutated per call, so this is exactly
        // the byte delta — see the WS6 accounting note above `entry_overhead`).
        let mut credit: usize = 0;
        let result = match rv {
            RedisValue::Hash(map) => {
                if let Some(v) = map.remove(field) {
                    credit = hash_field_cost(field, &v);
                    let empty = map.is_empty();
                    Ok((true, empty))
                } else {
                    Ok((false, false))
                }
            }
            RedisValue::HashListpack(lp) => {
                // Listpack cost is O(1) (capacity-based) — snapshot before/after
                // instead of tracking a per-element formula.
                let before = lp.estimate_memory();
                // Locate field among pairs, remove both field + value entries.
                let mut found_idx: Option<usize> = None;
                for (i, (f, _)) in lp.iter_pairs().enumerate() {
                    if f.as_bytes() == field {
                        found_idx = Some(i);
                        break;
                    }
                }
                if let Some(i) = found_idx {
                    // Each pair occupies two listpack slots: field at 2*i, value at 2*i+1.
                    // Remove value first (higher index) then field to keep indices stable.
                    lp.remove_at(i * 2 + 1);
                    lp.remove_at(i * 2);
                    let after = lp.estimate_memory();
                    credit = before.saturating_sub(after);
                    let empty = lp.is_empty();
                    Ok((true, empty))
                } else {
                    Ok((false, false))
                }
            }
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            } => {
                if let Some(v) = fields.remove(field) {
                    credit = hash_field_cost(field, &v);
                    let old_ttl = ttls.remove(field);
                    if ttls.is_empty() && !fields.is_empty() {
                        // All TTLs gone but fields remain — downgrade to plain Hash.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                        Ok((true, false))
                    } else if ttls.is_empty() && fields.is_empty() {
                        // Both maps empty — signal caller to delete the key.
                        // We leave the (now-empty) HashWithTtl in place; the
                        // caller will call db.remove() to drop the key.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                        Ok((true, true))
                    } else {
                        // TTLs remain; recompute min if the removed field held it.
                        if old_ttl == Some(*min_expiry_ms) {
                            *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                        }
                        let empty = fields.is_empty();
                        Ok((true, empty))
                    }
                } else {
                    Ok((false, false))
                }
            }
            _ => Err(Self::wrongtype_error()),
        };
        if credit > 0 {
            self.credit_memory(credit);
        }
        result
    }

    // -- HGETDEL / HGETEX family (phase 199 / issue #110) ----------------------
    //
    // Atomic compound get-and-mutate primitives for the two Valkey 9.1 commands.
    // Atomicity is guaranteed by the per-shard single-threaded execution model —
    // no explicit locking is required.

    /// Atomically read and remove a single field from a hash.
    ///
    /// Returns `Ok(Some(value))` when the field existed and was removed.
    /// Returns `Ok(None)` when the key is missing or the field does not exist.
    /// Returns `Err(WrongType)` when the key exists but is not a hash.
    ///
    /// For `HashWithTtl` the TTL sidecar entry is removed together with the
    /// field. When the last TTL sidecar entry is removed the encoding is
    /// downgraded back to a plain `Hash` (mirrors `hash_delete_field`).
    ///
    /// Callers **must** call [`Database::cleanup_empty_hash`] after processing
    /// all fields — this method intentionally does NOT delete the key when the
    /// hash becomes empty, so that the caller can accumulate results first.
    pub fn hash_get_and_delete_field(
        &mut self,
        key: &[u8],
        field: &[u8],
    ) -> Result<Option<Bytes>, WrongType> {
        let Some(entry) = self.data.get_mut(key) else {
            return Ok(None);
        };
        let Some(rv) = entry.value.as_redis_value_mut() else {
            return Err(WrongType);
        };
        // O(1) credit accumulator — one field removed per call (see the WS6
        // accounting note above `entry_overhead`).
        let mut credit: usize = 0;
        let result = match rv {
            RedisValue::Hash(map) => {
                let v = map.remove(field);
                if let Some(ref val) = v {
                    credit = hash_field_cost(field, val);
                }
                Ok(v)
            }
            RedisValue::HashListpack(lp) => {
                // Listpack cost is O(1) (capacity-based) — snapshot before/after.
                let before = lp.estimate_memory();
                // Locate the field-value pair by linear scan then remove both
                // listpack slots (value first so the field index stays valid).
                let mut found: Option<(usize, Bytes)> = None;
                for (i, (f, v)) in lp.iter_pairs().enumerate() {
                    if f.as_bytes() == field {
                        found = Some((i, v.to_bytes()));
                        break;
                    }
                }
                if let Some((i, v)) = found {
                    lp.remove_at(i * 2 + 1); // value first (higher index)
                    lp.remove_at(i * 2); // then field
                    let after = lp.estimate_memory();
                    credit = before.saturating_sub(after);
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            } => {
                let v = fields.remove(field);
                if let Some(ref val) = v {
                    credit = hash_field_cost(field, val);
                    let old_ttl = ttls.remove(field);
                    if ttls.is_empty() && !fields.is_empty() {
                        // All TTLs gone, live fields remain — downgrade to Hash.
                        let m = std::mem::take(fields);
                        *rv = RedisValue::Hash(m);
                    } else if ttls.is_empty() && fields.is_empty() {
                        // Both maps empty — leave an empty Hash shell; caller
                        // must call cleanup_empty_hash to remove the key.
                        *rv = RedisValue::Hash(HashMap::new());
                    } else if old_ttl == Some(*min_expiry_ms) {
                        // Removed field held the minimum; recompute.
                        *min_expiry_ms = ttls.values().copied().min().unwrap_or(u64::MAX);
                    }
                }
                Ok(v)
            }
            _ => Err(WrongType),
        };
        if credit > 0 {
            self.credit_memory(credit);
        }
        result
    }

    /// Remove the key when its hash value has become empty.
    ///
    /// No-op if the key is absent, is not a hash, or still has live fields.
    /// Intended to be called after a series of `hash_get_and_delete_field`
    /// calls to clean up the key when all fields were consumed.
    pub fn cleanup_empty_hash(&mut self, key: &[u8]) {
        let should_delete = self
            .data
            .get(key)
            .is_some_and(|e| match e.value.as_redis_value() {
                super::compact_value::RedisValueRef::Hash(m) => m.is_empty(),
                super::compact_value::RedisValueRef::HashListpack(lp) => lp.is_empty(),
                super::compact_value::RedisValueRef::HashWithTtl { fields, .. } => {
                    fields.is_empty()
                }
                _ => false,
            });
        if should_delete {
            self.data.remove(key);
        }
    }

    /// Estimated memory usage of all entries in this database.
    pub fn estimated_memory(&self) -> usize {
        self.used_memory
    }

    /// Resident bytes attributed to this database (alias for `estimated_memory`,
    /// kept distinct so observability call sites use the canonical name).
    /// O(1), zero allocation -- reads the per-shard `used_memory` accumulator.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        self.used_memory
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

    /// Get an entry by key, performing lazy expiration.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: a single immutable probe classifies the key as live,
    /// expired, or absent. NLL forces one re-probe on the live path
    /// (the cold fallback below mutates `self`, so the first borrow cannot
    /// be returned directly): 2 probes on a live hit, 1 on a miss — down
    /// from 3/2 with the previous expiry-check + `is_some()` + get chain.
    /// No LRU touch on reads (callers requiring LRU updates use `get_mut()`).
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        enum KeyState {
            Live,
            Expired,
            Absent,
        }
        let state = match self.data.get(key) {
            Some(e) if e.is_expired_at(base_ts, now_ms) => KeyState::Expired,
            Some(_) => KeyState::Live,
            None => KeyState::Absent,
        };
        match state {
            // Hot path: single re-probe, borrow returned to caller.
            KeyState::Live => self.data.get(key),
            KeyState::Expired => {
                let removed = self.data.remove(key)?;
                self.used_memory = self
                    .used_memory
                    .saturating_sub(entry_overhead(key, &removed));
                None
            }
            KeyState::Absent => {
                // Cold fallback: promote from disk into hot RAM if spilled
                // there by eviction, then re-probe. `promote_cold_if_present`
                // also owns reclaiming a stale (TTL-expired) cold-index entry.
                self.promote_cold_if_present(key, now_ms);
                self.data.get(key)
            }
        }
    }

    /// If `key` is missing from hot RAM but present in the cold tier
    /// (spilled there by eviction), read it from disk and promote it into
    /// hot RAM, removing the cold-index entry — this is the cold-fallback
    /// branch [`Self::get`] has always used, factored out so every mutable
    /// accessor can share it.
    ///
    /// Returns `true` if `key` is present in hot RAM after this call returns
    /// (either because it already was, or because promotion just happened).
    /// Returns `false` for a genuine miss (absent from both tiers), or when
    /// the cold entry's TTL had already passed — the stale index entry is
    /// reclaimed as a byproduct in that case, same as the expired-hot branch
    /// above.
    ///
    /// Cheap on the common case callers actually care about (key already
    /// hot): a single `contains_key` probe short-circuits before ever
    /// touching the cold tier. Only a genuine miss pays for the `ColdIndex`
    /// lookup + a blocking disk `pread`.
    ///
    /// P0 fix (`.planning/reviews/storage-audit-2026-07-12-kv.md`):
    /// `get_or_create_hash`/`_list`/`_set`/`_sorted_set`/`_stream` (and their
    /// compact-encoding siblings `get_or_create_hash_listpack`,
    /// `get_or_create_list_listpack`, `get_or_create_intset`) used to skip
    /// straight to fabricating a brand-new EMPTY container whenever
    /// `self.data` missed the key — even when the real value was sitting
    /// right there in the cold tier. The fabricated container then got
    /// written back over the cold copy on the next `set`/mutation,
    /// permanently destroying it. Calling this method first closes that gap.
    ///
    /// Correctness (task #59 review): this is a plain, ORIGINAL synchronous
    /// blocking disk read — no timeout, no possibility of returning "not
    /// found" for a key that actually exists on disk. A prior revision of
    /// this method routed through a bounded/timeout off-thread pool that
    /// could time out and silently answer `Miss` for an existing spilled
    /// key; that was a correctness regression (a GET on an existing key
    /// could return nil under disk backlog) and has been removed. See
    /// `tmp/task59-design.md` for the corrected design: the shard-thread
    /// stall is instead addressed by [`Self::promote_cold_outcome`] +
    /// `storage::tiered::cold_read_pool::read_cold_entry_async`, used from
    /// the async connection-handler layer for the read-only single-key
    /// commands that can afford to `.await` (currently GET); MULTI/EXEC
    /// bodies and Lua `redis.call` keep calling this original synchronous
    /// method unchanged.
    pub fn promote_cold_if_present(&mut self, key: &[u8], now_ms: u64) -> bool {
        if self.data.contains_key(key) {
            return true;
        }
        // Look up the location first (cheap, in-memory) so the outcome below
        // can be paired with the location it was read from -- required by
        // `promote_cold_outcome`'s revalidation (see its doc comment). This
        // path never awaits between lookup and use, so the location can't
        // actually change out from under it, but sharing one code path with
        // the async caller keeps the invariant enforced in exactly one
        // place instead of two.
        let Some((location, shard_dir)) = self.cold_lookup_location(key) else {
            return false;
        };
        let outcome =
            crate::storage::tiered::cold_read::read_cold_entry(&shard_dir, location, now_ms, None);
        self.promote_cold_outcome(key, now_ms, location, outcome)
    }

    /// Apply an already-computed [`cold_read::ColdReadOutcome`] to hot RAM +
    /// the cold index, without performing any disk I/O itself.
    ///
    /// Factored out of [`Self::promote_cold_if_present`] (task #59) so a
    /// caller that can `.await` an off-shard-thread disk read (see
    /// `storage::tiered::cold_read_pool::read_cold_entry_async`) can do the
    /// slow part outside any lock/borrow of `self`, then come back and apply
    /// the *real* outcome here — never a timed-out placeholder. Same
    /// return-value contract as `promote_cold_if_present`: `true` iff `key`
    /// is present in hot RAM after this call.
    ///
    /// TOCTOU fix (task #59 review round 2): `outcome` was read from disk at
    /// `expected_location`, possibly across an `.await` on the caller's
    /// side. Anything can happen to `key` while that await was suspended —
    /// most importantly `DEL`/`FLUSHDB`/`UNLINK` on the SAME shard thread
    /// (single-threaded event loop; no lock protects this window). If we
    /// blindly promoted `outcome` here, a deleted key would come back to
    /// life: `self.data.contains_key(key)` is false (DEL removed the hot
    /// copy, and there never was one for a cold-only key), so the guard
    /// above doesn't catch it, and `self.set(...)` would resurrect the
    /// key permanently. So: before applying a `Hit`/`Expired` outcome,
    /// re-read the CURRENT cold-index location for `key` and only proceed
    /// if it's still exactly `expected_location`. If the entry is gone
    /// (DEL/FLUSHDB) or now points somewhere else (SET-then-re-evict during
    /// the await), discard `outcome` entirely and let the caller's normal
    /// dispatch answer from current state instead. This re-check and the
    /// mutation that follows it both run synchronously with no `.await`
    /// between them, so they're atomic from every other task's perspective
    /// on this single-threaded shard — the race window closes here.
    pub fn promote_cold_outcome(
        &mut self,
        key: &[u8],
        _now_ms: u64,
        expected_location: crate::storage::tiered::cold_index::ColdLocation,
        outcome: crate::storage::tiered::cold_read::ColdReadOutcome,
    ) -> bool {
        use crate::storage::tiered::cold_read::ColdReadOutcome;
        if self.data.contains_key(key) {
            return true;
        }
        if matches!(outcome, ColdReadOutcome::Hit(..) | ColdReadOutcome::Expired) {
            let still_valid = self
                .cold_index
                .as_ref()
                .and_then(|ci| ci.lookup(key))
                .is_some_and(|current| current == expected_location);
            if !still_valid {
                // The cold entry this outcome was read from is gone (DEL/
                // FLUSHDB/UNLINK/expiry-sweep) or has been replaced (a fresh
                // SET-then-re-evict landed a NEW cold entry for this key)
                // since we started reading it. Applying `outcome` now would
                // either resurrect a deleted key or clobber a newer cold
                // entry with stale bytes. Discard it and report "not
                // promoted" — the caller's normal dispatch path re-reads
                // current state and answers correctly (nil for a deleted
                // key, the new value for a re-spilled one).
                return false;
            }
        }
        match outcome {
            ColdReadOutcome::Hit(redis_value, ttl_ms) => {
                let key_bytes = Bytes::copy_from_slice(key);
                // Build an entry from the RedisValue (works for strings and collections).
                let mut entry = Entry::new_string(Bytes::new()); // placeholder
                entry.value =
                    crate::storage::compact_value::CompactValue::from_redis_value(redis_value);
                if let Some(ttl) = ttl_ms {
                    entry.set_expires_at_ms(self.base_timestamp, ttl);
                }
                self.set(key_bytes, entry);
                if let Some(ref mut ci) = self.cold_index {
                    ci.remove(key);
                }
                true
            }
            ColdReadOutcome::Expired => {
                // Expired on disk: reclaim the index entry now, or it leaks
                // (the orphan sweep only checks hot-shadowing, never TTL).
                // Safe to remove unconditionally here -- the revalidation
                // above already confirmed the index still points at
                // `expected_location`, so this can't be clobbering a newer
                // entry.
                if let Some(ref mut ci) = self.cold_index {
                    ci.remove(key);
                }
                false
            }
            ColdReadOutcome::Miss => false,
        }
    }

    /// Cheap (no disk I/O, no promotion) check for whether `key` is present
    /// in the cold tier and not yet TTL-expired.
    ///
    /// Used by `EXISTS`: a spilled key logically exists even though it has
    /// no in-RAM `Entry`, but `EXISTS` doesn't need the *value* — paying for
    /// a disk read (or, worse, promoting the key into RAM) just to answer a
    /// boolean would be wasteful. A single `HashMap` probe against the
    /// in-RAM `ColdIndex` (which caches `ttl_ms` at insert time, see
    /// [`crate::storage::tiered::cold_index::ColdLocation::ttl_ms`]) is
    /// enough. Does not reclaim an expired entry (that needs `&mut self`
    /// and disk access to do safely) — the proactive sweep and the
    /// promoting paths handle reclamation.
    #[inline]
    fn cold_contains_alive(&self, key: &[u8], now_ms: u64) -> bool {
        let Some(ci) = self.cold_index.as_ref() else {
            return false;
        };
        match ci.lookup(key) {
            Some(loc) => loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl),
            None => false,
        }
    }

    /// Non-promoting cold read-through for the `&self` "*_ref_if_alive"
    /// accessors: decodes a spilled value from disk WITHOUT touching hot RAM
    /// or the cold index. Thin wrapper over [`Self::get_cold_value`] using
    /// this `Database`'s own cached clock semantics is left to the caller
    /// (they already have `now_ms` in hand); kept as a private alias so the
    /// call sites below read as "cold read" rather than repeating the
    /// `cold_shard_dir`/`cold_index` plumbing.
    ///
    /// These accessors back BOTH the exclusive-dispatch path (`&mut
    /// Database`, which downgrades to `&self` for the call) AND the
    /// RwLock-shared-read dispatch path (`&Database` only — see
    /// `dispatch_read` / `*_readonly` command handlers). The latter cannot
    /// mutate `self` to promote a cold hit into hot RAM, so the safe fix is
    /// "decode it from disk every time it's cold" rather than "silently
    /// report the key absent" (same P0 as [`Self::promote_cold_if_present`]).
    #[inline]
    fn cold_read_only(&self, key: &[u8], now_ms: u64) -> Option<RedisValue> {
        self.get_cold_value(key, now_ms)
    }

    /// Get a mutable reference to an entry by key, performing lazy expiration and access tracking.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: immutable check for expiry (rare path), then single get_mut
    /// for LRU touch + return. Reduces from 3 lookups to 2 for non-expired keys.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let now = self.cached_now;
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        // Immutable check for expiry (avoids get_mut + remove + get_mut triple lookup)
        let expired = self
            .data
            .get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms));
        if expired {
            let removed = self.data.remove(key)?;
            self.used_memory = self
                .used_memory
                .saturating_sub(entry_overhead(key, &removed));
            return None;
        }
        // Single get_mut: touch LRU + return
        let entry = self.data.get_mut(key)?;
        entry.set_last_access(now);
        Some(entry)
    }

    /// Insert or replace an entry, tracking memory and version.
    ///
    /// Uses `DashTable::insert_or_update` for a single SIMD probe on both hit
    /// and miss paths. The old `get_mut` + `insert` pattern ran two probes on
    /// miss (PERF-08).
    pub fn set(&mut self, key: Bytes, entry: Entry) {
        let new_cost = entry_overhead(&key, &entry);
        let has_expiry = entry.has_expiry();
        let mut old_cost: usize = 0;

        // Cell enables interior mutability through shared references, allowing
        // both closures to capture &entry_cell without conflicting &mut borrows.
        // Exactly one closure runs (FnOnce), so the take() is safe.
        let entry_cell = std::cell::Cell::new(Some(entry));

        // `insert_or_update` invariant: exactly one of the two closures fires
        // exactly once per call, so the `Cell::take()` below cannot observe
        // a None value. Annotated for the hot-path unwrap ratchet.
        #[allow(clippy::expect_used)]
        let result = self.data.insert_or_update(
            CompactKey::from(key.as_ref()), // borrow: CompactKey copies the bytes either way
            |existing: &mut Entry| {
                // Hit path: replace existing entry, bump version.
                let new_entry = entry_cell.take().expect("update closure called once");
                old_cost = entry_overhead(&key, existing);
                let new_version = existing.version() + 1;
                *existing = new_entry;
                existing.set_version(new_version);
            },
            || {
                // Miss path: insert entry as-is (version defaults to 0).
                entry_cell.take().expect("make closure called once on miss")
            },
        );

        match result {
            InsertOrUpdate::Updated(_) => {
                self.used_memory = self.used_memory.saturating_sub(old_cost) + new_cost;
                // Task #56 finding 1 (adversarial review, stale-data
                // resurrection): a SECOND-OR-LATER write to a key that ALSO
                // carries a cold-tier shadow proves that shadow is stale.
                //
                // Crash-consistency proof: AOF replay reconstructs a key's
                // entire write history against an EMPTY DashTable (`db.clear()`
                // runs in `main.rs` right before every replay branch), so the
                // first replayed write to a given key is always `Inserted`
                // (ambiguous -- it may be exactly the write whose later
                // eviction produced this cold entry, so it's left alone) and
                // every SUBSEQUENT write to the SAME key during that same
                // replay is `Updated` -- which can only happen if the AOF
                // recorded a write to this key AFTER the one that got
                // spilled, i.e. the cold copy is provably stale relative to
                // the value now hot. Clearing the shadow here, synchronously,
                // the moment a second touch is observed (live traffic or
                // replay -- same code path, same proof) closes the gap:
                // without this, a crash between a live overwrite of a
                // cold-shadowed key and the next eviction/orphan-sweep left
                // the on-disk manifest's cold entry pointing at the OLD
                // value; `demote_replayed_cold_shadows` (`src/main.rs`,
                // running after replay finishes) would then treat that
                // untouched-looking cold entry as "provably redundant" and
                // use it to discard the newer hot value on restart --
                // silently resurrecting stale data. See
                // `tests/cold_shadow_overwrite_resurrection.rs` for the
                // end-to-end regression guard and
                // `storage::db::tests::test_second_write_invalidates_cold_shadow`
                // for the unit-level proof.
                if let Some(ci) = self.cold_index.as_mut() {
                    ci.remove(&key);
                }
            }
            InsertOrUpdate::Inserted(_) => {
                self.used_memory += new_cost;
            }
        }
        if has_expiry {
            self.maybe_has_expiring_keys = true;
        }
    }

    /// Clear all entries and reset memory accounting.
    ///
    /// Used during multi-part AOF recovery to wipe any state populated by
    /// earlier recovery phases (per-shard WAL replay, legacy appendonly.aof)
    /// before loading the authoritative base RDB + incr log. Without this,
    /// non-idempotent commands from pre-existing state would be double-applied.
    pub fn clear(&mut self) {
        self.data = DashTable::new();
        self.used_memory = 0;
        self.maybe_has_expiring_keys = false;
        self.hot_keys.clear();
        // D1: FLUSH must clear the cold tier too, or flushed keys stay
        // readable via cold read-through. Files are queued for unlink and
        // reclaimed by the orphan sweep (which holds the manifest handle).
        if let Some(ci) = self.cold_index.as_mut() {
            ci.clear_all();
        }
    }

    /// The hot-key sketch (sampling hooks, HOTKEYS command, coordinator
    /// merge). Interior mutability — `&self` suffices for observe/top/clear.
    #[inline]
    pub fn hot_keys(&self) -> &crate::storage::hotkey::HotKeySketch {
        &self.hot_keys
    }

    /// Bulk-load insert: skip duplicate check, version tracking, and per-key memory accounting.
    ///
    /// Used exclusively during RDB/AOF restore where keys are guaranteed unique and
    /// we recalculate `used_memory` once after the entire load completes.
    #[inline]
    pub fn insert_for_load(&mut self, key: Bytes, entry: Entry) {
        if entry.has_expiry() {
            self.maybe_has_expiring_keys = true;
        }
        self.data.insert(CompactKey::from(key), entry);
    }

    /// Recalculate `used_memory` by scanning all entries. Call once after bulk load.
    pub fn recalculate_memory(&mut self) {
        let mut total = 0usize;
        let mut any_expiring = false;
        for (key, entry) in self.data.iter() {
            total += entry_overhead(key.as_bytes(), entry);
            if entry.has_expiry() {
                any_expiring = true;
            }
        }
        self.used_memory = total;
        self.maybe_has_expiring_keys = any_expiring;
    }

    /// Pre-size the internal hash table for an expected key count.
    ///
    /// WARNING: This REPLACES the internal hash table with a fresh one sized for
    /// `additional` entries. It MUST be called on an empty `Database` (typically
    /// immediately after `Database::new()` during RDB/AOF bulk load). Calling it
    /// on a populated database silently discards all entries.
    ///
    /// Named `reserve` rather than `reset_with_capacity` to match the plan
    /// nomenclature, but the debug assertion guards the misuse case.
    pub fn reserve(&mut self, additional: usize) {
        debug_assert!(
            self.data.is_empty(),
            "Database::reserve() must only be called on an empty database (bulk-load pre-sizing); called with {} existing entries",
            self.data.len()
        );
        if additional > self.data.len() {
            let new_table = DashTable::with_capacity(additional);
            self.data = new_table;
            self.maybe_has_expiring_keys = false;
        }
    }

    /// Remove a key and return its entry. No expiry check needed (DEL removes regardless).
    ///
    /// Also removes any COLD (spilled) copy of the key — without this, a DEL
    /// of an evicted key is a no-op and the next GET resurrects the value via
    /// cold read-through (D1, tmp/OFFLOAD-COMPRESSION-REVIEW.md). A cold-only
    /// key returns `None` (no in-RAM entry exists); callers that must COUNT
    /// cold-only removals (DEL/UNLINK) use [`Self::remove_counting_cold`].
    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let _ = self.remove_cold_only(key);
        self.remove_hot(key)
    }

    /// Remove hot + cold copies; returns `true` when EITHER existed, so
    /// DEL/UNLINK count spilled keys as removed (Redis semantics: the key
    /// logically exists). A cold entry that is already TTL-expired (judged
    /// from the cached `ColdLocation::ttl_ms`, no disk read) is reclaimed
    /// but NOT counted — DEL of a logically-expired key answers 0. The
    /// removed hot entry, when present, is also returned so UNLINK can
    /// size its async-drop decision.
    pub fn remove_counting_cold(&mut self, key: &[u8]) -> (bool, Option<Entry>) {
        let now_ms = self.cached_now_ms;
        let cold_alive = self
            .cold_index
            .as_ref()
            .and_then(|ci| ci.lookup(key))
            .is_some_and(|loc| loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl));
        let had_cold = self.remove_cold_only(key);
        let hot = self.remove_hot(key);
        (hot.is_some() || (had_cold && cold_alive), hot)
    }

    #[inline]
    fn remove_cold_only(&mut self, key: &[u8]) -> bool {
        self.cold_index.as_mut().is_some_and(|ci| ci.remove(key))
    }

    #[inline]
    fn remove_hot(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(entry) = self.data.remove(key) {
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            Some(entry)
        } else {
            None
        }
    }

    /// Task #56 (used_memory truthful under disk-offload + AOF restart):
    /// drop hot DashTable copies that are redundant with an already-cold key.
    ///
    /// AOF replay has no knowledge of the disk-offload cold tier -- a key
    /// evicted-and-spilled to cold storage BEFORE a crash gets no
    /// corresponding DEL record in the AOF (a spilled entry stays
    /// cold-readable, not deleted, so `record_reason_del` never fires for
    /// it -- see its doc comment). Replaying the AOF's full command history
    /// therefore unconditionally re-applies that key's original SET and
    /// lands it back in hot RAM, inflating `used_memory` well past
    /// `--maxmemory` immediately after every restart until later eviction
    /// ticks claw it back down (task #56 diagnosis; observed ~6x the
    /// steady-state ledger in a 40k-key repro, `tests/used_memory_offload_truthful.rs`).
    ///
    /// Call this ONCE per shard, after AOF replay finishes and before the
    /// server starts accepting connections. Any key present in
    /// `cold_index` at that point was cold-and-untouched as of the crash:
    /// the index was rebuilt from the crash-consistent manifest *before*
    /// AOF replay ran, and replay only ever writes hot -- it cannot
    /// re-cold a key. If AOF replay *also* landed that key in the hot
    /// DashTable, both copies hold the identical value. Proof sketch: the
    /// AOF's last command touching a crash-time-cold key must be the same
    /// SET the eviction path later spilled -- any *later* live write would
    /// have made the key hot again and required a fresh eviction to become
    /// cold once more, which the manifest (and thus `cold_index`) would
    /// already reflect as its current entry. It is therefore always safe
    /// to drop the redundant hot copy and let the existing cold-index
    /// entry keep serving reads -- no cold-tier file is touched, only the
    /// in-memory accounting is corrected.
    ///
    /// This is the mirror image of the live-traffic orphan sweeper
    /// ([`crate::storage::tiered::cold_index::ColdIndex::orphan_sweep`]):
    /// that one deletes a *stale cold* copy shadowed by a *newer hot*
    /// write seen during live traffic (hot wins). This one deletes a
    /// *redundant hot* copy manufactured by replaying an *old* write whose
    /// crash-time fate was already cold (cold wins) -- the two precedences
    /// only coexist because this method runs strictly before the server is
    /// reachable; never call it once live traffic may have run.
    pub fn demote_replayed_cold_shadows(&mut self) -> usize {
        let Some(ci) = self.cold_index.as_ref() else {
            return 0;
        };
        let shadow_keys: Vec<Bytes> = ci
            .iter()
            .filter(|(key, _)| self.is_hot(key))
            .map(|(key, _)| key.clone())
            .collect();
        let mut demoted = 0usize;
        for key in &shadow_keys {
            if self.remove_hot(key).is_some() {
                demoted += 1;
            }
        }
        demoted
    }

    /// Check if a key exists, performing lazy expiration.
    /// Optimized: single lookup via get instead of check_expired + contains_key.
    ///
    /// A cold-only key (spilled by eviction, no in-RAM `Entry`) still counts
    /// as existing — checked via the cheap in-RAM [`Self::cold_contains_alive`]
    /// (no disk I/O, no promotion; P0 cold-collection-visibility fix).
    #[allow(clippy::unwrap_used)] // remove() after get() returned Some — key guaranteed present
    pub fn exists(&mut self, key: &[u8]) -> bool {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => self.cold_contains_alive(key, now_ms),
            Some(entry) => {
                if entry.is_expired_at(base_ts, now_ms) {
                    let removed = self.data.remove(key).unwrap();
                    self.used_memory = self
                        .used_memory
                        .saturating_sub(entry_overhead(key, &removed));
                    self.cold_contains_alive(key, now_ms)
                } else {
                    true
                }
            }
        }
    }

    /// Number of entries (including potentially expired ones).
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Number of LOGICAL keys: hot entries plus disk-offloaded (cold) keys,
    /// counting a key that exists in both planes exactly once (issue #355 —
    /// a spilled-but-readable key is still a key; DBSIZE / INFO `# Keyspace`
    /// previously reported the resident set only, an ~86% under-report in
    /// the 2026-07-16 G2 re-run).
    ///
    /// Hot∩cold overlap is transient but real: a fresh SET over a cold-only
    /// key lands on `set()`'s `Inserted` arm, which deliberately leaves the
    /// cold shadow (removing it there would defeat restart-as-cold — every
    /// first replayed write is `Inserted` — see the ambiguity proof in
    /// [`Self::set`]). The overlap is therefore subtracted with an O(cold)
    /// probe pass instead of a maintained counter: `cold_index` is a `pub`
    /// field mutated directly by the spill-completion path, so an
    /// incremental counter would have unfenceable drift risk, while INFO
    /// already tolerates an O(hot) scan per call in
    /// [`Self::expires_count`]. Like `len()`, potentially-expired entries
    /// (hot or cold) are counted.
    pub fn logical_len(&self) -> usize {
        let hot = self.data.len();
        match &self.cold_index {
            None => hot,
            Some(ci) => {
                let overlap = ci
                    .iter()
                    .filter(|(key, _)| self.data.contains_key(key))
                    .count();
                hot + ci.len() - overlap
            }
        }
    }

    /// Iterator over all keys (caller does glob filtering).
    ///
    /// Hot plane only: under disk-offload, spilled keys have no in-RAM
    /// `Entry` and are NOT yielded here — keyspace enumerators (SCAN /
    /// KEYS / RANDOMKEY) must union this with [`Self::cold_only_keys`]
    /// or spilled keys silently vanish from enumeration (#364).
    pub fn keys(&self) -> impl Iterator<Item = &CompactKey> {
        self.data.keys()
    }

    /// Keys visible ONLY via the cold plane at `now_ms`: present in the
    /// in-RAM cold index, not TTL-expired (judged from the cached
    /// [`crate::storage::tiered::cold_index::ColdLocation::ttl_ms`] — no
    /// disk I/O), and NOT shadowed by a live hot-plane entry.
    ///
    /// Together with the hot-alive subset of [`Self::keys`] this
    /// partitions the logical keyspace with no overlap, so keyspace
    /// enumerators (SCAN / KEYS / RANDOMKEY, #364) can take the union of
    /// the two planes without any dedup pass. A key present in BOTH
    /// planes (hot shadow over a stale cold entry, e.g. after AOF replay)
    /// is classified hot; a hot entry that is TTL-expired above a live
    /// cold entry is classified cold.
    pub fn cold_only_keys(&self, now_ms: u64) -> impl Iterator<Item = &Bytes> + '_ {
        let base_ts = self.base_timestamp;
        self.cold_index.as_ref().into_iter().flat_map(move |ci| {
            ci.iter().filter_map(move |(key, loc)| {
                let cold_alive = loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl);
                if !cold_alive {
                    return None;
                }
                let hot_alive = self
                    .data
                    .get(key.as_ref())
                    .is_some_and(|e| !e.is_expired_at(base_ts, now_ms));
                if hot_alive { None } else { Some(key) }
            })
        })
    }

    /// Return a random non-expired key from the database, or None if empty.
    ///
    /// Samples the LOGICAL keyspace: hot-alive entries plus cold-only
    /// spilled keys ([`Self::cold_only_keys`]) — an all-spilled database
    /// must not answer "empty" (#364).
    ///
    /// Two passes — count, then walk to the selected position — so only
    /// the winning key is ever cloned (the previous single-pass version
    /// materialized a `Bytes` copy of EVERY live key per call). Stable
    /// across the passes: `&self` is held throughout and each shard's
    /// database is single-threaded, so neither plane can mutate between
    /// the count and the walk.
    pub fn random_key(&self) -> Option<Bytes> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        let hot_live = self
            .data
            .iter()
            .filter(|(_, e)| !e.is_expired_at(base_ts, now_ms))
            .count();
        let cold_live = self.cold_only_keys(now_ms).count();
        let total = hot_live + cold_live;
        if total == 0 {
            return None;
        }
        let idx = (current_time_ms() as usize) % total;
        if idx < hot_live {
            self.data
                .iter()
                .filter(|(_, e)| !e.is_expired_at(base_ts, now_ms))
                .nth(idx)
                .map(|(k, _)| Bytes::copy_from_slice(k.as_ref()))
        } else {
            self.cold_only_keys(now_ms).nth(idx - hot_live).cloned()
        }
    }

    /// Set or remove expiration on an existing key.
    ///
    /// Performs lazy expiry check first. Returns `false` if the key does not
    /// exist (or has already expired). Pass 0 to remove expiry.
    pub fn set_expiry(&mut self, key: &[u8], expires_at_ms: u64) -> bool {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
            return false;
        }
        match self.data.get_mut(key) {
            Some(entry) => {
                entry.set_expires_at_ms(base_ts, expires_at_ms);
                // Latch the fast-path flag once a TTL is set. Persist (0) may
                // clear the per-entry bit but we don't flip the DB-level flag
                // down — the active-expiry scan's self-reset gate is the only
                // authoritative downgrade path (avoids racy decrement logic).
                if expires_at_ms != 0 {
                    self.maybe_has_expiring_keys = true;
                }
                true
            }
            None => false,
        }
    }

    /// Count entries that have an expiration set.
    pub fn expires_count(&self) -> usize {
        self.data.values().filter(|e| e.has_expiry()).count()
    }

    /// Check if an entry is expired without requiring &mut self.
    fn check_expired(
        data: &DashTable<CompactKey, Entry>,
        key: &[u8],
        base_ts: u32,
        now_ms: u64,
    ) -> bool {
        data.get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms))
    }

    /// Convenience: set a string value with no expiry.
    pub fn set_string(&mut self, key: Bytes, value: Bytes) {
        self.set(key, Entry::new_string(value));
    }

    /// Convenience: set a string value with an expiry (unix millis).
    pub fn set_string_with_expiry(&mut self, key: Bytes, value: Bytes, expires_at_ms: u64) {
        let base_ts = self.base_timestamp;
        self.set(
            key,
            Entry::new_string_with_expiry(value, expires_at_ms, base_ts),
        );
    }

    /// Check if a single entry is expired.
    #[allow(dead_code)]
    fn is_expired(entry: &Entry, base_ts: u32) -> bool {
        entry.is_expired_at(base_ts, current_time_ms())
    }

    /// WRONGTYPE error frame.
    fn wrongtype_error() -> Frame {
        Frame::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }

    /// Get the version of a key. Returns 0 if not found. No expiry check (WATCH needs raw version).
    pub fn get_version(&self, key: &[u8]) -> u32 {
        self.data.get(key).map(|e| e.version()).unwrap_or(0)
    }

    /// Increment version of a key if it exists.
    pub fn increment_version(&mut self, key: &[u8]) {
        if let Some(entry) = self.data.get_mut(key) {
            entry.increment_version();
        }
    }

    /// Touch access time of a key for LRU tracking (for reads).
    pub fn touch_access(&mut self, key: &[u8]) {
        let now = self.cached_now;
        if let Some(entry) = self.data.get_mut(key) {
            entry.set_last_access(now);
        }
    }

    /// Get or create a hash entry. Returns mutable ref to inner HashMap.
    /// Returns Err(WRONGTYPE) if key exists with wrong type.
    ///
    /// New keys start with compact listpack encoding and are upgraded to
    /// full HashMap on first mutable access (eager upgrade).
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present; as_redis_value_mut() on known type
    pub fn get_or_create_hash(&mut self, key: &[u8]) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        // Expire check
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: a hash may have been spilled to the cold tier by
            // eviction. Promote it back to hot RAM before fabricating an
            // empty container, or HSET on an evicted hash would silently
            // destroy the cold copy (see `Self::promote_cold_if_present`).
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_hash();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => {
                // This should not happen — insert was just called above.
                // Log and return an error instead of panicking.
                tracing::error!(
                    "get_or_create_hash: get_mut returned None after insert for key len={}",
                    key.len()
                );
                return Err(Frame::Error(bytes::Bytes::from_static(
                    b"ERR internal: hash lookup failed after insert",
                )));
            }
        };
        // Upgrade compact listpack to full HashMap if needed
        let needs_upgrade = matches!(
            entry.value.as_redis_value_mut(),
            Some(RedisValue::HashListpack(_))
        );
        if needs_upgrade {
            if let Some(RedisValue::HashListpack(lp)) = entry.value.as_redis_value_mut() {
                let map = lp.to_hash_map();
                if let Some(v) = entry.value.as_redis_value_mut() {
                    *v = RedisValue::Hash(map);
                }
            }
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Hash(map)) => Ok(map),
            // HashWithTtl: the fields sub-map is a plain HashMap<Bytes, Bytes>.
            // HSET/HMSET/HINCRBY must be able to mutate it directly; they never
            // touch the ttls sidecar, which is managed by the HEXPIRE family.
            Some(RedisValue::HashWithTtl { fields, .. }) => Ok(fields),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a hash entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encoding to full HashMap if found.
    ///
    /// Promotes a cold-spilled hash back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`, so
    /// unlike the enum-based `get_hash_ref_if_alive` it can promote directly
    /// instead of decoding a throwaway copy on every call.
    #[allow(clippy::unwrap_used)] // as_redis_value_mut() on known compact type during upgrade
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        // Upgrade compact encoding if present
        if let Some(entry) = self.data.get_mut(key) {
            if let Some(RedisValue::HashListpack(lp)) = entry.value.as_redis_value_mut() {
                let map = lp.to_hash_map();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Hash(map) => Ok(Some(map)),
                // HashWithTtl: expose the fields sub-map for reads.  Callers
                // (HGET, HMGET, HKEYS, HVALS, HGETALL, etc.) see the HashMap
                // directly; expired-field filtering happens in hash_field_state
                // or via the active-expiry tick, not on every read.
                RedisValueRef::HashWithTtl { fields, .. } => Ok(Some(fields)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a list entry. Returns mutable ref to inner VecDeque.
    /// New keys start with full encoding. Upgrades compact listpack on access.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present; as_redis_value_mut() on known type
    pub fn get_or_create_list(&mut self, key: &[u8]) -> Result<&mut VecDeque<Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled list before fabricating an empty
            // one (see `Self::promote_cold_if_present`).
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_list();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        // Upgrade compact listpack to full VecDeque if needed
        if let Some(RedisValue::ListListpack(lp)) = entry.value.as_redis_value_mut() {
            let list = lp.to_vec_deque();
            *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::List(list)) => Ok(list),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a list entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encoding to full VecDeque if found.
    ///
    /// Promotes a cold-spilled list back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    #[allow(clippy::unwrap_used)] // as_redis_value_mut() on known compact type during upgrade
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        // Upgrade compact encoding if present
        if let Some(entry) = self.data.get_mut(key) {
            if let Some(RedisValue::ListListpack(lp)) = entry.value.as_redis_value_mut() {
                let list = lp.to_vec_deque();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::List(list) => Ok(Some(list)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a set entry. Returns mutable ref to inner HashSet.
    /// New keys start with full encoding. Upgrades compact encodings on access.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present; as_redis_value_mut() on known type
    pub fn get_or_create_set(&mut self, key: &[u8]) -> Result<&mut HashSet<Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled set before fabricating an empty
            // one (see `Self::promote_cold_if_present`).
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_set();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        // Upgrade compact encodings to full HashSet
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetListpack(lp)) => {
                let set = lp.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            Some(RedisValue::SetIntset(is)) => {
                let set = is.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            _ => {}
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Set(set)) => Ok(set),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a set entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encodings to full HashSet if found.
    ///
    /// Promotes a cold-spilled set back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    #[allow(clippy::unwrap_used)] // as_redis_value_mut() on known compact type during upgrade
    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&HashSet<Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        // Upgrade compact encodings if present
        if let Some(entry) = self.data.get_mut(key) {
            match entry.value.as_redis_value_mut() {
                Some(RedisValue::SetListpack(lp)) => {
                    let set = lp.to_hash_set();
                    *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
                }
                Some(RedisValue::SetIntset(is)) => {
                    let set = is.to_hash_set();
                    *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
                }
                _ => {}
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Set(set) => Ok(Some(set)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create an intset entry. Creates a new SetIntset if the key doesn't exist.
    /// Returns Err if the key exists but holds a non-set type.
    /// Returns Ok(None) if the key exists but is not an intset (caller should use get_or_create_set).
    /// Returns Ok(Some(&mut Intset)) if the key holds or was created as an intset.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_intset(&mut self, key: &[u8]) -> Result<Option<&mut Intset>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled set before fabricating an empty
            // intset — a promoted value always decodes as `RedisValue::Set`
            // (cold storage never persists the intset compact encoding), so
            // it naturally falls into the `Ok(None)` "not an intset, caller
            // should use get_or_create_set" arm below, which already routes
            // callers (e.g. SADD) to `get_or_create_set` — no fabrication.
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_set_intset();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => Ok(Some(is)),
            Some(RedisValue::Set(_)) | Some(RedisValue::SetListpack(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade an intset entry to a full HashSet and return mutable ref.
    /// Panics if the key doesn't exist or isn't a SetIntset.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is SetIntset; upgrade is infallible
    pub fn upgrade_intset_to_set(&mut self, key: &[u8]) -> &mut HashSet<Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => {
                let set = is.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Set(set)) => set,
            _ => unreachable!("upgrade_intset_to_set: expected Set after upgrade"),
        }
    }

    /// Get or create a hash entry as listpack. Creates new keys as HashListpack.
    /// Returns Ok(Some(&mut Listpack)) if the key is a HashListpack.
    /// Returns Ok(None) if the key already holds a full Hash (caller should fall through).
    /// Returns Err(WRONGTYPE) if the key holds a non-hash type.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_hash_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut super::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled hash before fabricating an
            // empty listpack — a promoted value always decodes as
            // `RedisValue::Hash` (cold storage never persists the listpack
            // compact encoding), so it naturally falls into the `Ok(None)`
            // "not a listpack, fall through" arm below, which already routes
            // callers (e.g. HSET) to `get_or_create_hash` — no fabrication.
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_hash_listpack();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::HashListpack(lp)) => Ok(Some(lp)),
            // Plain HashMap or TTL-extended hash: caller falls through to the
            // HashMap path.  TTL'd hashes never compact back to listpack.
            Some(RedisValue::Hash(_)) | Some(RedisValue::HashWithTtl { .. }) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a HashListpack to full Hash. Returns mutable ref to the HashMap.
    /// Panics if the key doesn't exist or isn't a HashListpack.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is HashListpack; upgrade is infallible
    pub fn upgrade_hash_listpack_to_hash(&mut self, key: &[u8]) -> &mut HashMap<Bytes, Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::HashListpack(lp)) => {
                let map = lp.to_hash_map();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Hash(map)) => map,
            _ => unreachable!("upgrade_hash_listpack_to_hash: expected Hash after upgrade"),
        }
    }

    /// Get or create a list entry as listpack. Creates new keys as ListListpack.
    /// Returns Ok(Some(&mut Listpack)) if the key is a ListListpack.
    /// Returns Ok(None) if the key already holds a full List (caller should fall through).
    /// Returns Err(WRONGTYPE) if the key holds a non-list type.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_list_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut super::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled list before fabricating an
            // empty listpack — a promoted value always decodes as
            // `RedisValue::List` (cold storage never persists the listpack
            // compact encoding), so it naturally falls into the `Ok(None)`
            // "not a listpack, fall through" arm below, which already routes
            // callers (e.g. LPUSH) to `get_or_create_list` — no fabrication.
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_list_listpack();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::ListListpack(lp)) => Ok(Some(lp)),
            Some(RedisValue::List(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a ListListpack to full List. Returns mutable ref to the VecDeque.
    /// Panics if the key doesn't exist or isn't a ListListpack.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is ListListpack; upgrade is infallible
    pub fn upgrade_list_listpack_to_list(&mut self, key: &[u8]) -> &mut VecDeque<Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::ListListpack(lp)) => {
                let list = lp.to_vec_deque();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::List(list)) => list,
            _ => unreachable!("upgrade_list_listpack_to_list: expected List after upgrade"),
        }
    }

    /// Get or create a sorted set entry. Returns mutable refs to both inner structures.
    ///
    /// New keys start with SortedSetBPTree encoding. Legacy SortedSet (BTreeMap)
    /// entries are upgraded to SortedSetBPTree on access for backward compatibility.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present; legacy upgrade is infallible
    pub fn get_or_create_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<(&mut HashMap<Bytes, f64>, &mut BPTree), Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled sorted set before fabricating
            // an empty one (see `Self::promote_cold_if_present`).
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_sorted_set_bptree();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        // Upgrade old SortedSet (BTreeMap) to SortedSetBPTree on access
        let entry = self.data.get_mut(key).unwrap();
        if let Some(RedisValue::SortedSet { members, scores }) = entry.value.as_redis_value_mut() {
            let mut tree = BPTree::new();
            let new_members = std::mem::take(members);
            let old_scores = std::mem::take(scores);
            for ((score, member), ()) in old_scores {
                tree.insert(score, member);
            }
            entry.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                tree,
                members: new_members,
            });
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SortedSetBPTree { members, tree }) => Ok((members, tree)),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a sorted set entry (read-only). Returns None if key missing, Err if wrong type.
    ///
    /// Promotes a cold-spilled sorted set back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    #[allow(clippy::unwrap_used)] // get_mut() after confirmed existence; legacy BTreeMap upgrade is infallible
    pub fn get_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(&HashMap<Bytes, f64>, &BPTree)>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        // Upgrade old SortedSet (BTreeMap) to SortedSetBPTree on access
        if let Some(entry) = self.data.get(key) {
            if matches!(
                entry.value.as_redis_value(),
                RedisValueRef::SortedSet { .. }
            ) {
                let entry = self.data.get_mut(key).unwrap();
                if let Some(RedisValue::SortedSet { members, scores }) =
                    entry.value.as_redis_value_mut()
                {
                    let mut tree = BPTree::new();
                    let new_members = std::mem::take(members);
                    let old_scores = std::mem::take(scores);
                    for ((score, member), ()) in old_scores {
                        tree.insert(score, member);
                    }
                    entry.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                        tree,
                        members: new_members,
                    });
                }
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::SortedSetBPTree { tree, members } => Ok(Some((members, tree))),
                RedisValueRef::SortedSet { .. } => unreachable!("should have been upgraded"),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Collect keys that have an expiration set.
    pub fn keys_with_expiry(&self) -> Vec<CompactKey> {
        self.data
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Collect keys whose value is a `HashWithTtl` (hash with per-field TTLs).
    ///
    /// Used by the active-expiry tick to find hashes that may have expired
    /// fields ready for reaping.  Returns an owned `Vec` so the caller can
    /// iterate and mutate via `get_mut` without holding a borrow on `self.data`.
    pub fn hashes_with_field_expiry(&self) -> Vec<CompactKey> {
        self.data
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e.value.as_redis_value(),
                    super::compact_value::RedisValueRef::HashWithTtl { .. }
                )
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a key exists and its expiry is in the past.
    pub fn is_key_expired(&self, key: &[u8]) -> bool {
        let now_ms = current_time_ms();
        let base_ts = self.base_timestamp;
        self.data
            .get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms))
    }

    /// Read-only access to the data map (for SCAN iteration).
    pub fn data(&self) -> &DashTable<CompactKey, Entry> {
        &self.data
    }

    // ---- Read-only methods for RwLock read path ----
    // These take `now_ms` as a parameter and do NOT mutate state:
    // no expired-key removal, no LRU touch.

    /// Read-only get: checks expiry, returns None if expired, but does NOT
    /// remove expired keys or touch LRU. Used with RwLock read path.
    pub fn get_if_alive(&self, key: &[u8], now_ms: u64) -> Option<&Entry> {
        let base_ts = self.base_timestamp;
        let entry = self.data.get(key)?;
        if entry.is_expired_at(base_ts, now_ms) {
            return None;
        }
        Some(entry)
    }

    /// Read-only cold storage lookup for evicted keys.
    ///
    /// When `get_if_alive` returns None, call this to check if the key was
    /// spilled to disk by the eviction path. Returns the value as owned Bytes
    /// (read from disk file). Does NOT promote the entry back to RAM.
    ///
    /// WARNING: this method performs synchronous disk I/O. Callers on the
    /// hot path must release any shard read/write guard *before* invoking it.
    /// Use [`Self::cold_lookup_location`] under the guard, then drop the guard,
    /// then call [`crate::storage::tiered::cold_read::read_cold_entry_at`].
    pub fn get_cold_value(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Option<crate::storage::entry::RedisValue> {
        let shard_dir = self.cold_shard_dir.as_ref()?;
        let ci = self.cold_index.as_ref()?;
        let (value, _ttl) =
            crate::storage::tiered::cold_read::cold_read_through(ci, shard_dir, key, now_ms)?;
        Some(value)
    }

    /// Cheap, in-memory cold-index lookup. Returns the disk location plus a
    /// cloned shard dir path so the caller can drop the shard guard before
    /// performing the disk read.
    pub fn cold_lookup_location(
        &self,
        key: &[u8],
    ) -> Option<(
        crate::storage::tiered::cold_index::ColdLocation,
        std::path::PathBuf,
    )> {
        let shard_dir = self.cold_shard_dir.as_ref()?;
        let ci = self.cold_index.as_ref()?;
        let location = ci.lookup(key)?;
        Some((location, shard_dir.clone()))
    }

    /// Returns `true` if the key is present in the hot in-memory DashTable,
    /// regardless of expiry status.
    ///
    /// Used by the cold-tier orphan sweeper to detect "hot shadow" entries:
    /// a cold entry whose key was later overwritten by a hot `SET` is an
    /// orphan — the hot copy shadows it and the cold file is reclaimable.
    ///
    /// This intentionally does NOT check expiry (an expired hot key still
    /// shadows the cold entry; the hot expiry path will clean it up on next
    /// access). Using the expired-included check avoids a TOCTOU race where
    /// we decide "not hot" and then a concurrent read promotes the expired key.
    #[inline]
    pub fn is_hot(&self, key: &[u8]) -> bool {
        self.data.get(key).is_some()
    }

    /// Read-only existence check: returns false if expired.
    ///
    /// Also counts a cold-only key (spilled by eviction) as existing — see
    /// [`Self::cold_contains_alive`] (P0 cold-collection-visibility fix).
    /// Used by the RwLock-shared-read `EXISTS` dispatch path, which cannot
    /// mutate `self` to promote.
    pub fn exists_if_alive(&self, key: &[u8], now_ms: u64) -> bool {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            Some(e) if !e.is_expired_at(base_ts, now_ms) => true,
            _ => self.cold_contains_alive(key, now_ms),
        }
    }

    /// Read-only hash access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_hash_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Hash(map) => Ok(Some(map)),
                RedisValueRef::HashListpack(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only list access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_list_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::List(list) => Ok(Some(list)),
                RedisValueRef::ListListpack(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only set access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_set_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&HashSet<Bytes>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Set(set) => Ok(Some(set)),
                RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only sorted set access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_sorted_set_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<(&HashMap<Bytes, f64>, &BPTree)>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::SortedSetBPTree { tree, members } => Ok(Some((members, tree))),
                RedisValueRef::SortedSet { .. } | RedisValueRef::SortedSetListpack(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    // ---- Enum-based readonly accessors that handle compact encodings ----

    /// Read-only hash access via HashRef enum.
    ///
    /// Handles `Hash` (HashMap), `HashListpack` (compact Listpack), and
    /// `HashWithTtl` (HashMap with per-field TTL sidecar).  For `HashWithTtl`
    /// returns a `HashRef::WithTtl` that filters expired fields on every
    /// field-level operation without mutating the database (lazy expiry).
    ///
    /// Takes `&self` because this backs BOTH the exclusive-dispatch path
    /// (`&mut Database`, reborrowed) AND the RwLock-shared-read dispatch
    /// path (`&Database` only — `hget_readonly`/`hgetall_readonly`/etc.).
    /// The latter cannot promote a cold hit into hot RAM, so a hot miss
    /// falls back to a non-promoting cold read-through returning
    /// `HashRef::Owned`/`OwnedWithTtl` (P0 cold-collection-visibility fix) —
    /// the fast path (key present hot) still costs exactly one probe.
    pub fn get_hash_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<HashRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(base_ts, now_ms) {
                return Ok(None);
            }
            return match entry.value.as_redis_value() {
                RedisValueRef::Hash(map) => Ok(Some(HashRef::Map(map))),
                RedisValueRef::HashListpack(lp) => Ok(Some(HashRef::Listpack(lp))),
                RedisValueRef::HashWithTtl {
                    fields,
                    ttls,
                    min_expiry_ms,
                } => Ok(Some(HashRef::WithTtl {
                    fields,
                    ttls,
                    now_ms,
                    min_expiry_ms,
                })),
                _ => Err(Self::wrongtype_error()),
            };
        }
        match self.cold_read_only(key, now_ms) {
            Some(RedisValue::Hash(map)) => Ok(Some(HashRef::Owned(map))),
            Some(RedisValue::HashWithTtl {
                fields,
                ttls,
                min_expiry_ms,
            }) => Ok(Some(HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            })),
            Some(_) => Err(Self::wrongtype_error()),
            None => Ok(None),
        }
    }

    /// Read-only list access via ListRef enum. Handles both VecDeque and Listpack.
    ///
    /// See [`Self::get_hash_ref_if_alive`] for why this consults the cold
    /// tier without promoting on a hot miss (P0 cold-collection-visibility
    /// fix).
    pub fn get_list_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<ListRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(base_ts, now_ms) {
                return Ok(None);
            }
            return match entry.value.as_redis_value() {
                RedisValueRef::List(list) => Ok(Some(ListRef::Deque(list))),
                RedisValueRef::ListListpack(lp) => Ok(Some(ListRef::Listpack(lp))),
                _ => Err(Self::wrongtype_error()),
            };
        }
        match self.cold_read_only(key, now_ms) {
            Some(RedisValue::List(list)) => Ok(Some(ListRef::Owned(list))),
            Some(_) => Err(Self::wrongtype_error()),
            None => Ok(None),
        }
    }

    /// Read-only set access via SetRef enum. Handles HashSet, Listpack, and Intset.
    ///
    /// See [`Self::get_hash_ref_if_alive`] for why this consults the cold
    /// tier without promoting on a hot miss (P0 cold-collection-visibility
    /// fix).
    pub fn get_set_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<SetRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(base_ts, now_ms) {
                return Ok(None);
            }
            return match entry.value.as_redis_value() {
                RedisValueRef::Set(set) => Ok(Some(SetRef::Hash(set))),
                RedisValueRef::SetListpack(lp) => Ok(Some(SetRef::Listpack(lp))),
                RedisValueRef::SetIntset(is) => Ok(Some(SetRef::Intset(is))),
                _ => Err(Self::wrongtype_error()),
            };
        }
        match self.cold_read_only(key, now_ms) {
            Some(RedisValue::Set(set)) => Ok(Some(SetRef::Owned(set))),
            Some(_) => Err(Self::wrongtype_error()),
            None => Ok(None),
        }
    }

    /// Read-only sorted set access via SortedSetRef enum. Handles BPTree, Listpack, and Legacy.
    ///
    /// See [`Self::get_hash_ref_if_alive`] for why this consults the cold
    /// tier without promoting on a hot miss (P0 cold-collection-visibility
    /// fix).
    pub fn get_sorted_set_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<SortedSetRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(base_ts, now_ms) {
                return Ok(None);
            }
            return match entry.value.as_redis_value() {
                RedisValueRef::SortedSetBPTree { tree, members } => {
                    Ok(Some(SortedSetRef::BPTree { tree, members }))
                }
                RedisValueRef::SortedSetListpack(lp) => Ok(Some(SortedSetRef::Listpack(lp))),
                RedisValueRef::SortedSet { members, scores } => {
                    Ok(Some(SortedSetRef::Legacy { members, scores }))
                }
                _ => Err(Self::wrongtype_error()),
            };
        }
        match self.cold_read_only(key, now_ms) {
            Some(RedisValue::SortedSetBPTree { tree, members }) => {
                Ok(Some(SortedSetRef::Owned { tree, members }))
            }
            Some(_) => Err(Self::wrongtype_error()),
            None => Ok(None),
        }
    }

    // ---- Low-level helpers for blocking wakeup hooks ----

    /// Pop the front element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    pub fn list_pop_front(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_or_create_list(key).ok()?;
        let val = list.pop_front()?;
        let empty = list.is_empty();
        // `list`'s borrow of `self` ends above.
        if empty {
            // Whole-key removal recomputes the (now-empty) entry cost via
            // `entry_overhead` -- no separate credit needed for the popped
            // element itself.
            self.remove(key);
        } else {
            self.credit_memory(list_elem_cost(&val));
        }
        Some(val)
    }

    /// Pop the back element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    pub fn list_pop_back(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_or_create_list(key).ok()?;
        let val = list.pop_back()?;
        let empty = list.is_empty();
        if empty {
            self.remove(key);
        } else {
            self.credit_memory(list_elem_cost(&val));
        }
        Some(val)
    }

    /// Push an element to the front of a list. Creates the list if it does not exist.
    pub fn list_push_front(&mut self, key: &[u8], value: Bytes) {
        // get_or_create_list creates the key if missing
        let cost = list_elem_cost(&value);
        if let Ok(list) = self.get_or_create_list(key) {
            list.push_front(value);
            self.charge_memory(cost);
        }
    }

    /// Push an element to the back of a list. Creates the list if it does not exist.
    pub fn list_push_back(&mut self, key: &[u8], value: Bytes) {
        let cost = list_elem_cost(&value);
        if let Ok(list) = self.get_or_create_list(key) {
            list.push_back(value);
            self.charge_memory(cost);
        }
    }

    /// Pop the minimum element from a sorted set. Returns (member, score) or None.
    /// Removes the key if the sorted set becomes empty.
    pub fn zset_pop_min(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self.get_or_create_sorted_set(key).ok()?;
        let first = tree.iter().next().map(|(s, m)| (s, m.clone()))?;
        let (score, member) = first;
        tree.remove(score, &member);
        members.remove(&member);
        let result = (member, score.0);
        if members.is_empty() {
            self.remove(key);
        }
        Some(result)
    }

    /// Pop the maximum element from a sorted set. Returns (member, score) or None.
    /// Removes the key if the sorted set becomes empty.
    pub fn zset_pop_max(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self.get_or_create_sorted_set(key).ok()?;
        let last = tree.iter_rev().next().map(|(s, m)| (s, m.clone()))?;
        let (score, member) = last;
        tree.remove(score, &member);
        members.remove(&member);
        let result = (member, score.0);
        if members.is_empty() {
            self.remove(key);
        }
        Some(result)
    }

    /// Get or create a stream at the given key. Returns WRONGTYPE if key holds another type.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_stream(&mut self, key: &[u8]) -> Result<&mut StreamData, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled stream before fabricating an
            // empty one (see `Self::promote_cold_if_present`).
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_stream();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Stream(s)) => Ok(s.as_mut()),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Read-only stream access for the shared-lock read path.
    ///
    /// Checks expiry using `now_ms` but does NOT remove the expired key or
    /// touch LRU (mirrors `get_if_alive` semantics).  Returns `Ok(None)` for
    /// missing or expired keys, `Err(WRONGTYPE)` for non-stream keys.
    ///
    /// Returns `StreamRef` rather than `&StreamData`: this backs both the
    /// exclusive-dispatch path and the RwLock-shared-read dispatch path
    /// (`&Database` only, cannot promote). A hot miss falls back to a
    /// non-promoting cold read-through returning `StreamRef::Owned` (P0
    /// cold-collection-visibility fix) — `StreamRef` derefs to `&StreamData`
    /// so existing call sites (`stream.length`, `stream.range(..)`, ...) are
    /// unaffected. The fast path (key present hot) still costs one probe.
    pub fn get_stream_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<StreamRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(base_ts, now_ms) {
                return Ok(None);
            }
            return match entry.value.as_redis_value() {
                RedisValueRef::Stream(s) => Ok(Some(StreamRef::Borrowed(s))),
                _ => Err(Self::wrongtype_error()),
            };
        }
        match self.cold_read_only(key, now_ms) {
            Some(RedisValue::Stream(s)) => Ok(Some(StreamRef::Owned(s))),
            Some(_) => Err(Self::wrongtype_error()),
            None => Ok(None),
        }
    }

    /// Get a read-only reference to a stream. Returns Ok(None) if key doesn't exist.
    /// Returns WRONGTYPE error if key holds another type.
    ///
    /// Promotes a cold-spilled stream back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_stream(&mut self, key: &[u8]) -> Result<Option<&StreamData>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Stream(s) => Ok(Some(s)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get a mutable reference to an existing stream. Returns Ok(None) if key doesn't exist.
    ///
    /// Promotes a cold-spilled stream back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_stream_mut(&mut self, key: &[u8]) -> Result<Option<&mut StreamData>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        match self.data.get_mut(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value_mut() {
                Some(RedisValue::Stream(s)) => Ok(Some(s.as_mut())),
                Some(_) => Err(Self::wrongtype_error()),
                None => Err(Self::wrongtype_error()),
            },
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
    use ordered_float::OrderedFloat;

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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms, base_ts),
        );
        // get should return None and remove the key
        assert!(db.get(b"expired").is_none());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_exists_with_expiry() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms, base_ts),
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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"k2"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v2"), future_ms, base_ts),
        );
        assert_eq!(db.len(), 2);
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn test_is_expired() {
        let base_ts = current_secs();
        let past_ms = current_time_ms() - 1000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms, base_ts);
        assert!(Database::is_expired(&entry, base_ts));

        let future_ms = current_time_ms() + 3_600_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms, base_ts);
        assert!(!Database::is_expired(&entry, base_ts));

        let entry = Entry::new_string(Bytes::from_static(b"v"));
        assert!(!Database::is_expired(&entry, base_ts));
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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms, base_ts),
        );
        assert!(db.is_key_expired(b"expired"));
        assert!(!db.is_key_expired(b"missing"));
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
        assert_eq!(db.get_version(b"key"), 0);
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v1"));
        assert_eq!(db.get_version(b"key"), 0); // first set, version 0
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v2"));
        assert_eq!(db.get_version(b"key"), 1); // second set, version 0+1=1
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v3"));
        assert_eq!(db.get_version(b"key"), 2);
    }

    #[test]
    fn test_increment_version() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v"));
        assert_eq!(db.get_version(b"key"), 0);
        db.increment_version(b"key");
        assert_eq!(db.get_version(b"key"), 1);
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
    // `evict_one_async_spill` / `evict_batch_durable_no_aof` (production
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
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        let mut db = Database::new();
        db.cold_shard_dir = Some(shard_dir.to_path_buf());
        db.cold_index = Some(cold_index);
        db
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
}
