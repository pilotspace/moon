use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use super::bptree::BPTree;
use super::intset::Intset;
use super::listpack::Listpack;
use super::stream::Stream as StreamData;

// ---------------------------------------------------------------------------
// Read-only Ref enums for immutable access to compact and full encodings
// ---------------------------------------------------------------------------

/// Read-only reference to a hash (full HashMap, compact Listpack, or TTL-enabled HashMap).
///
/// The `WithTtl` variant carries `now_ms` so that `get_field`, `len`, and
/// `entries` can filter expired fields without requiring `&mut Database`.
pub enum HashRef<'a> {
    Map(&'a HashMap<Bytes, Bytes>),
    Listpack(&'a Listpack),
    /// Live-filtered view of a `HashWithTtl` entry.  Fields absent from `ttls`
    /// are immortal; fields present in `ttls` are expired when `ttl <= now_ms`.
    ///
    /// `min_expiry_ms` is the cached minimum across all TTL values.  When
    /// `now_ms < min_expiry_ms` no field has expired, so all three methods can
    /// take the O(1) fast path and skip per-field TTL probes entirely.
    WithTtl {
        fields: &'a HashMap<Bytes, Bytes>,
        ttls: &'a HashMap<Bytes, u64>,
        now_ms: u64,
        min_expiry_ms: u64,
    },
    /// A value read fresh from the cold tier with no backing hot `Entry` to
    /// borrow from (P0 cold-collection-visibility fix, 2026-07-12).
    ///
    /// `get_hash_ref_if_alive` takes `&self` because some callers (the
    /// RwLock-shared-read dispatch path) hold only a shared read guard and
    /// cannot promote a cold hit into hot RAM. Rather than report the key
    /// absent (silent data loss to the caller), the value is decoded fresh
    /// from disk on every such access and carried here by value.
    Owned(HashMap<Bytes, Bytes>),
    /// Owned counterpart of `WithTtl` for a cold `HashWithTtl` read.
    OwnedWithTtl {
        fields: HashMap<Bytes, Bytes>,
        ttls: HashMap<Bytes, u64>,
        now_ms: u64,
        min_expiry_ms: u64,
    },
}

impl<'a> HashRef<'a> {
    /// Look up a single field, respecting per-field TTLs.
    /// Linear scan for Listpack, O(1) for Map/WithTtl.
    pub fn get_field(&self, field: &[u8]) -> Option<Bytes> {
        match self {
            HashRef::Map(map) => map.get(field).cloned(),
            HashRef::Listpack(lp) => {
                // ONE borrowed scan: only the value that actually matches is
                // materialized, instead of every field walked past -- and the
                // listpack is not re-walked from the head to reach a position
                // the lookup had already found (moon#799).
                lp.pair_value(field).map(|v| v.to_bytes())
            }
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                // Fast path: if now_ms < min_expiry_ms, no field has expired.
                // Invariant: min_expiry_ms = min(ttls.values()), so every
                // individual TTL is also > now_ms.  Skip the HashMap probe.
                if *now_ms < *min_expiry_ms {
                    return fields.get(field).cloned();
                }
                // Slow path: at least one field may have expired.
                let expired = ttls.get(field).is_some_and(|&t| t <= *now_ms);
                if expired {
                    None
                } else {
                    fields.get(field).cloned()
                }
            }
            HashRef::Owned(map) => map.get(field).cloned(),
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                if *now_ms < *min_expiry_ms {
                    return fields.get(field).cloned();
                }
                let expired = ttls.get(field).is_some_and(|&t| t <= *now_ms);
                if expired {
                    None
                } else {
                    fields.get(field).cloned()
                }
            }
        }
    }

    /// Number of live fields in the hash.
    ///
    /// O(1) for `Map` and `Listpack`.  O(N) for `WithTtl` — acceptable since
    /// TTL-enabled hashes are rare and HLEN is documented as O(N) for them.
    pub fn len(&self) -> usize {
        match self {
            HashRef::Map(map) => map.len(),
            HashRef::Listpack(lp) => lp.len() / 2,
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                // Fast path: no fields have expired → return length directly.
                if *now_ms < *min_expiry_ms {
                    return fields.len();
                }
                // Slow path: at least one field may have expired; scan all.
                fields
                    .keys()
                    .filter(|f| ttls.get(*f).map_or(true, |&t| t > *now_ms))
                    .count()
            }
            HashRef::Owned(map) => map.len(),
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                if *now_ms < *min_expiry_ms {
                    return fields.len();
                }
                fields
                    .keys()
                    .filter(|f| ttls.get(*f).map_or(true, |&t| t > *now_ms))
                    .count()
            }
        }
    }

    /// Return all live (field, value) pairs.
    pub fn entries(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            HashRef::Map(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            // Borrowed walk, one copy per entry (moon#1174 §3): `iter_pairs`
            // decoded each into a `Vec` and `to_bytes` cloned it again.
            HashRef::Listpack(lp) => lp
                .iter_pair_refs()
                .map(|(f, v)| (f.to_bytes(), v.to_bytes()))
                .collect(),
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                // Fast path: no fields have expired → collect all entries.
                if *now_ms < *min_expiry_ms {
                    return fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                }
                // Slow path: filter expired entries individually.
                fields
                    .iter()
                    .filter(|(f, _)| ttls.get(*f).map_or(true, |&t| t > *now_ms))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
            HashRef::Owned(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                if *now_ms < *min_expiry_ms {
                    return fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                }
                fields
                    .iter()
                    .filter(|(f, _)| ttls.get(*f).map_or(true, |&t| t > *now_ms))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
        }
    }
}

/// Whether a field of a TTL-carrying hash is still live at `now_ms` -- the
/// rule `HashRef::get_field` applies, shared by the field-only and
/// length-only reads so they cannot disagree with it. `min_expiry_ms` is the
/// cached minimum TTL: below it nothing has expired and the probe is skipped.
#[inline]
fn hash_field_live(
    ttls: &HashMap<Bytes, u64>,
    field: &[u8],
    now_ms: u64,
    min_expiry_ms: u64,
) -> bool {
    now_ms < min_expiry_ms || ttls.get(field).is_none_or(|&t| t > now_ms)
}

impl HashRef<'_> {
    /// Whether `field` is present and live -- `HEXISTS` -- without
    /// materializing its value (moon#1174 §3: it used to go through
    /// `get_field`, which copies the value out of a listpack just to be
    /// dropped).
    pub fn contains_field(&self, field: &[u8]) -> bool {
        match self {
            HashRef::Map(map) => map.contains_key(field),
            HashRef::Owned(map) => map.contains_key(field),
            HashRef::Listpack(lp) => lp.pair_value(field).is_some(),
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                fields.contains_key(field) && hash_field_live(ttls, field, *now_ms, *min_expiry_ms)
            }
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => {
                fields.contains_key(field) && hash_field_live(ttls, field, *now_ms, *min_expiry_ms)
            }
        }
    }

    /// Byte length of `field`'s value, if present and live -- `HSTRLEN` --
    /// without materializing it (moon#1174 §3). A listpack integer measures
    /// its canonical spelling, the bytes `HGET` would return.
    pub fn field_len(&self, field: &[u8]) -> Option<usize> {
        match self {
            HashRef::Map(map) => map.get(field).map(Bytes::len),
            HashRef::Owned(map) => map.get(field).map(Bytes::len),
            HashRef::Listpack(lp) => lp.pair_value(field).map(|v| v.byte_len()),
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => fields
                .get(field)
                .filter(|_| hash_field_live(ttls, field, *now_ms, *min_expiry_ms))
                .map(Bytes::len),
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => fields
                .get(field)
                .filter(|_| hash_field_live(ttls, field, *now_ms, *min_expiry_ms))
                .map(Bytes::len),
        }
    }

    /// An upper bound on the live field count that costs O(1) on every
    /// encoding -- a reply's capacity, not an answer (`len` is O(n) on a TTL
    /// hash past its first expiry).
    pub fn len_hint(&self) -> usize {
        match self {
            HashRef::Map(map) => map.len(),
            HashRef::Owned(map) => map.len(),
            HashRef::Listpack(lp) => lp.len() / 2,
            HashRef::WithTtl { fields, .. } => fields.len(),
            HashRef::OwnedWithTtl { fields, .. } => fields.len(),
        }
    }

    /// Hand every live FIELD name to `f` -- `HKEYS` -- without materializing
    /// a single value (moon#1174 §3: it used to call `entries()`, which copied
    /// every value just to drop it, twice per entry on a listpack).
    pub fn for_each_field(&self, mut f: impl FnMut(Bytes)) {
        match self {
            HashRef::Map(map) => map.keys().for_each(|k| f(k.clone())),
            HashRef::Owned(map) => map.keys().for_each(|k| f(k.clone())),
            HashRef::Listpack(lp) => lp.iter_pair_refs().for_each(|(k, _)| f(k.to_bytes())),
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => fields
                .keys()
                .filter(|k| hash_field_live(ttls, k, *now_ms, *min_expiry_ms))
                .for_each(|k| f(k.clone())),
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => fields
                .keys()
                .filter(|k| hash_field_live(ttls, k, *now_ms, *min_expiry_ms))
                .for_each(|k| f(k.clone())),
        }
    }

    /// Hand every live VALUE to `f` -- `HVALS` -- without materializing a
    /// single field name (moon#1174 §3).
    pub fn for_each_value(&self, mut f: impl FnMut(Bytes)) {
        match self {
            HashRef::Map(map) => map.values().for_each(|v| f(v.clone())),
            HashRef::Owned(map) => map.values().for_each(|v| f(v.clone())),
            HashRef::Listpack(lp) => lp.iter_pair_refs().for_each(|(_, v)| f(v.to_bytes())),
            HashRef::WithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => fields
                .iter()
                .filter(|(k, _)| hash_field_live(ttls, k, *now_ms, *min_expiry_ms))
                .for_each(|(_, v)| f(v.clone())),
            HashRef::OwnedWithTtl {
                fields,
                ttls,
                now_ms,
                min_expiry_ms,
            } => fields
                .iter()
                .filter(|(k, _)| hash_field_live(ttls, k, *now_ms, *min_expiry_ms))
                .for_each(|(_, v)| f(v.clone())),
        }
    }
}

/// Read-only reference to a list (full VecDeque or compact Listpack).
pub enum ListRef<'a> {
    Deque(&'a VecDeque<Bytes>),
    Listpack(&'a Listpack),
    /// Owned counterpart for a value decoded fresh from the cold tier — see
    /// `HashRef::Owned` for the rationale (P0 cold-collection-visibility fix).
    Owned(VecDeque<Bytes>),
}

impl<'a> ListRef<'a> {
    /// Number of elements.
    pub fn len(&self) -> usize {
        match self {
            ListRef::Deque(d) => d.len(),
            ListRef::Listpack(lp) => lp.len(),
            ListRef::Owned(d) => d.len(),
        }
    }

    /// Whether the list holds no elements.
    ///
    /// moon#832: the blocking-move gate needs "is there anything to move?"
    /// WITHOUT `&mut Database` — asking through `Database::get_list` used to
    /// flatten the list's compact encoding just to answer it.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get element at index.
    ///
    /// A listpack is entered from its NEARER end and the entry is borrowed
    /// until the one copy the caller keeps (moon#1174 §2).
    pub fn get(&self, index: usize) -> Option<Bytes> {
        match self {
            ListRef::Deque(d) => d.get(index).cloned(),
            ListRef::Listpack(lp) => lp.get_ref(index).map(|e| e.to_bytes()),
            ListRef::Owned(d) => d.get(index).cloned(),
        }
    }

    /// Hand every element of `[start..=end]` to `f`, in order. Caller clamps.
    ///
    /// moon#1174 §2: on a listpack this is ONE seek, from the nearer end, then
    /// a walk (`Listpack::range_refs`). It used to be `get_at(i)` per index,
    /// each walking from the head: `LRANGE 0 -1` on 128 entries decoded 8,256.
    pub fn for_each_in_range(&self, start: usize, end: usize, mut f: impl FnMut(Bytes)) {
        let len = self.len();
        if start > end || start >= len {
            return;
        }
        let end = end.min(len - 1);
        match self {
            ListRef::Deque(d) => d.range(start..=end).for_each(|b| f(b.clone())),
            ListRef::Owned(d) => d.range(start..=end).for_each(|b| f(b.clone())),
            ListRef::Listpack(lp) => lp
                .range_refs(start, end - start + 1)
                .for_each(|e| f(e.to_bytes())),
        }
    }

    /// Get a range of elements [start..=end]. Caller must clamp bounds.
    pub fn range(&self, start: usize, end: usize) -> Vec<Bytes> {
        let mut out =
            Vec::with_capacity(end.saturating_sub(start).saturating_add(1).min(self.len()));
        self.for_each_in_range(start, end, |b| out.push(b));
        out
    }

    /// Visit the indices of the elements equal to `element`, among the first
    /// `limit` elements scanned from the head -- or from the tail when
    /// `from_tail` is set. `on_match(index)` returns `false` to stop.
    ///
    /// `LPOS` is this call (moon#1173). It used to be `iter_bytes()` -- a clone
    /// of EVERY element into a fresh `Vec`, two allocations each on a listpack
    /// -- before it looked at `MAXLEN`, `RANK` or `COUNT`, so `LPOS l x MAXLEN
    /// 10` on a million-element list copied a million elements to compare ten.
    /// This borrows: the scan stops at `limit` or when the caller says so, and
    /// nothing is copied. Listpack equality is [`ListpackRef::eq_bytes`], the
    /// canonical-integer rule every other listpack lookup uses (moon#795).
    ///
    /// [`ListpackRef::eq_bytes`]: super::listpack::ListpackRef::eq_bytes
    pub fn for_each_match(
        &self,
        element: &[u8],
        from_tail: bool,
        limit: usize,
        mut on_match: impl FnMut(usize) -> bool,
    ) {
        let len = self.len();
        let deque = match self {
            ListRef::Deque(d) => *d,
            ListRef::Owned(d) => d,
            ListRef::Listpack(lp) => {
                if from_tail {
                    // The window is the last `limit` entries. A listpack is
                    // never walked backwards (`listpack::list_ops` docs), so
                    // the window's matches are gathered in one forward walk
                    // and handed out tail first. Bounded by the policy's entry
                    // count; the buffer spills to the heap only past 32 hits.
                    let first = len - limit.min(len);
                    let mut hits: smallvec::SmallVec<[usize; 32]> = smallvec::SmallVec::new();
                    for (k, e) in lp.range_refs(first, len - first).enumerate() {
                        if e.eq_bytes(element) {
                            hits.push(first + k);
                        }
                    }
                    for &i in hits.iter().rev() {
                        if !on_match(i) {
                            return;
                        }
                    }
                } else {
                    for (i, e) in lp.iter_refs().take(limit).enumerate() {
                        if e.eq_bytes(element) && !on_match(i) {
                            return;
                        }
                    }
                }
                return;
            }
        };
        if from_tail {
            for (k, v) in deque.iter().rev().take(limit).enumerate() {
                if v.as_ref() == element && !on_match(len - 1 - k) {
                    return;
                }
            }
        } else {
            for (i, v) in deque.iter().take(limit).enumerate() {
                if v.as_ref() == element && !on_match(i) {
                    return;
                }
            }
        }
    }

    /// Iterate all elements (for LPOS).
    pub fn iter_bytes(&self) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => d.iter().cloned().collect(),
            // Borrowed walk, one copy per element (moon#1174 §3).
            ListRef::Listpack(lp) => lp.iter_refs().map(|e| e.to_bytes()).collect(),
            ListRef::Owned(d) => d.iter().cloned().collect(),
        }
    }
}

/// Read-only reference to a set (full HashSet, compact Listpack, or Intset).
pub enum SetRef<'a> {
    Hash(&'a crate::storage::entry::SetValue),
    Listpack(&'a Listpack),
    Intset(&'a Intset),
    /// Owned counterpart for a value decoded fresh from the cold tier — see
    /// `HashRef::Owned` for the rationale (P0 cold-collection-visibility fix).
    Owned(crate::storage::entry::SetValue),
}

impl<'a> SetRef<'a> {
    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            SetRef::Hash(s) => s.len(),
            SetRef::Listpack(lp) => lp.len(),
            SetRef::Intset(is) => is.len(),
            SetRef::Owned(s) => s.len(),
        }
    }

    /// Check if member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetRef::Hash(s) => s.contains(member),
            SetRef::Listpack(lp) => lp.find(member).is_some(),
            SetRef::Intset(is) => {
                // Canonical forms only: `000000012345` is a *different* member
                // from `12345`, so it must not match one stored in the intset.
                match crate::storage::numeric::canonical_i64(member) {
                    Some(v) => is.contains(v),
                    None => false,
                }
            }
            SetRef::Owned(s) => s.contains(member),
        }
    }

    /// The member at index `idx`, without materializing the whole set.
    ///
    /// This is the primitive `SPOP` and `SRANDMEMBER` need. Before it existed
    /// they called `members()`, which clones EVERY member into a fresh `Vec`,
    /// then indexed into that -- so picking one member out of 100,000 cost
    /// 100,000 clones and ran at 506 ops/s against Redis's 129,032.
    ///
    /// Cost per representation:
    ///   - `Hash`/`Owned`: O(1). This is why the set is an `IndexSet`.
    ///   - `Intset`: O(1) -- a sorted `Vec<i64>` indexes directly.
    ///   - `Listpack`: O(idx), but a listpack set is capped at
    ///     `set-max-listpack-entries` (128) members, so the walk is bounded.
    ///
    /// Returns `None` when `idx >= len()`.
    pub fn nth(&self, idx: usize) -> Option<Bytes> {
        match self {
            SetRef::Hash(s) => s.get_index(idx).cloned(),
            SetRef::Owned(s) => s.get_index(idx).cloned(),
            SetRef::Intset(is) => is.get(idx).map(|v| Bytes::from(v.to_string())),
            SetRef::Listpack(lp) => lp.get_at(idx).map(|e| e.into_bytes()),
        }
    }

    /// Return all members as Bytes.
    pub fn members(&self) -> Vec<Bytes> {
        match self {
            SetRef::Hash(s) => s.iter().cloned().collect(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.into_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
            SetRef::Owned(s) => s.iter().cloned().collect(),
        }
    }

    /// Convert to an owned HashSet for set-algebra operations.
    pub fn to_hash_set(&self) -> HashSet<Bytes> {
        match self {
            // The stored set is an `IndexSet`; set algebra wants a plain
            // `HashSet`, so this boundary re-collects rather than clones.
            SetRef::Hash(s) => s.iter().cloned().collect(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.into_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
            SetRef::Owned(s) => s.iter().cloned().collect(),
        }
    }
}

// Test-only: how many times a sorted-set read materialized the whole set
// with `entries_sorted` — the O(n log n) decode + parse + sort moon#1174 §4
// removed from ZRANK/ZREVRANK/ZCOUNT/ZLEXCOUNT on a listpack. Plain `//`
// comments: a doc comment on the macro trips `unused_doc_comments`.
#[cfg(test)]
thread_local! {
    static ENTRIES_SORTED_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Read and reset the per-thread `entries_sorted` call counter (test-only).
#[cfg(test)]
pub(crate) fn take_entries_sorted_calls() -> u32 {
    ENTRIES_SORTED_CALLS.with(|c| c.replace(0))
}

/// Read-only reference to a sorted set.
pub enum SortedSetRef<'a> {
    BPTree {
        tree: &'a BPTree,
        members: &'a HashMap<Bytes, f64>,
    },
    Listpack(&'a Listpack),
    #[allow(dead_code)]
    Legacy {
        members: &'a HashMap<Bytes, f64>,
        scores: &'a BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
    /// Owned counterpart of `BPTree` for a value decoded fresh from the cold
    /// tier — see `HashRef::Owned` for the rationale (P0
    /// cold-collection-visibility fix). `members_map`/`bptree` return `None`
    /// for this variant (same as `Listpack`); callers already fall back to
    /// the generic `score`/`entries_sorted` methods in that case.
    Owned {
        tree: BPTree,
        members: HashMap<Bytes, f64>,
    },
}

impl<'a> SortedSetRef<'a> {
    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            SortedSetRef::BPTree { members, .. } => members.len(),
            SortedSetRef::Listpack(lp) => lp.len() / 2,
            SortedSetRef::Legacy { members, .. } => members.len(),
            SortedSetRef::Owned { members, .. } => members.len(),
        }
    }

    /// Get score for a member.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            SortedSetRef::BPTree { members, .. } => members.get(member).copied(),
            SortedSetRef::Listpack(lp) => {
                // ONE borrowed scan, and the score is read straight out of the
                // borrowed view -- no second walk to an index the lookup had
                // already reached, and nothing materialized at all (moon#799).
                // `as_score` is the same rule `Listpack::update_pair_value`
                // applies on the write side (moon#942 replaced the local
                // `listpack_zset_find` + `replace_at` pair with it), so ZSCORE
                // and ZADD cannot disagree.
                lp.pair_value(member)?.as_score()
            }
            SortedSetRef::Legacy { members, .. } => members.get(member).copied(),
            SortedSetRef::Owned { members, .. } => members.get(member).copied(),
        }
    }

    /// Get all (member, score) pairs sorted by score then member.
    pub fn entries_sorted(&self) -> Vec<(Bytes, f64)> {
        #[cfg(test)]
        ENTRIES_SORTED_CALLS.with(|c| c.set(c.get() + 1));
        match self {
            SortedSetRef::BPTree { tree, .. } => tree
                .iter()
                .map(|(score, member)| (member.clone(), score.0))
                .collect(),
            SortedSetRef::Listpack(lp) => {
                let mut pairs: Vec<(Bytes, f64)> = Vec::new();
                for (m, s) in lp.iter_score_member_pairs() {
                    let score = match s {
                        super::listpack::ListpackEntry::Integer(i) => i as f64,
                        super::listpack::ListpackEntry::String(ref b) => std::str::from_utf8(b)
                            .ok()
                            .and_then(|ss| ss.parse().ok())
                            .unwrap_or(0.0),
                    };
                    pairs.push((m.into_bytes(), score));
                }
                pairs.sort_by(|a, b| {
                    OrderedFloat(a.1)
                        .cmp(&OrderedFloat(b.1))
                        .then_with(|| a.0.cmp(&b.0))
                });
                pairs
            }
            SortedSetRef::Legacy { scores, .. } => {
                scores.keys().map(|(s, m)| (m.clone(), s.0)).collect()
            }
            SortedSetRef::Owned { tree, .. } => tree
                .iter()
                .map(|(score, member)| (member.clone(), score.0))
                .collect(),
        }
    }

    /// Get members HashMap reference (for BPTree and Legacy variants).
    /// For listpack, returns None (callers should use entries_sorted or score).
    pub fn members_map(&self) -> Option<&'a HashMap<Bytes, f64>> {
        match self {
            SortedSetRef::BPTree { members, .. } => Some(members),
            SortedSetRef::Legacy { members, .. } => Some(members),
            SortedSetRef::Listpack(_) => None,
            SortedSetRef::Owned { .. } => None,
        }
    }

    /// Get BPTree reference (for BPTree variant only).
    pub fn bptree(&self) -> Option<&'a BPTree> {
        match self {
            SortedSetRef::BPTree { tree, .. } => Some(tree),
            _ => None,
        }
    }

    /// The B+tree behind this view, borrowed from the view itself — so,
    /// unlike [`Self::bptree`], it also reaches the tree an `Owned` cold-tier
    /// decode carries. For readers that only need the tree's order
    /// statistics and never outlive the view (moon#1171).
    pub fn any_tree(&self) -> Option<&BPTree> {
        match self {
            SortedSetRef::BPTree { tree, .. } => Some(tree),
            SortedSetRef::Owned { tree, .. } => Some(tree),
            SortedSetRef::Listpack(_) | SortedSetRef::Legacy { .. } => None,
        }
    }

    /// Every `(member, score)` pair in STORAGE order, unsorted: insertion
    /// order for a listpack (whose length `zset-max-listpack-entries`
    /// bounds), `(score, member)` order for the tree forms. For callers that
    /// pick entries by position and never need a rank — ZRANDMEMBER — so a
    /// listpack is decoded once with no sort (moon#1171). Scores decode with
    /// the same `as_score` rule `score()` uses.
    pub fn entries_unordered(&self) -> Vec<(Bytes, f64)> {
        match self {
            SortedSetRef::Listpack(lp) => lp
                .iter_pair_refs()
                .map(|(m, s)| {
                    let member = match m {
                        super::listpack::ListpackRef::Str(b) => Bytes::copy_from_slice(b),
                        super::listpack::ListpackRef::Integer(v) => {
                            let mut buf = itoa::Buffer::new();
                            Bytes::copy_from_slice(buf.format(v).as_bytes())
                        }
                    };
                    (member, s.as_score().unwrap_or(0.0))
                })
                .collect(),
            _ => self.entries_sorted(),
        }
    }
}

/// Read-only reference to a stream: either borrowed from a hot `Entry`, or
/// owned after a fresh cold-tier decode (P0 cold-collection-visibility fix,
/// 2026-07-12 — see `HashRef::Owned` for the general rationale).
///
/// `Deref`s to `&StreamData` so existing call sites (`stream.length`,
/// `stream.entries.range(..)`, etc.) work unchanged for both variants.
pub enum StreamRef<'a> {
    Borrowed(&'a StreamData),
    Owned(Box<StreamData>),
}

impl<'a> std::ops::Deref for StreamRef<'a> {
    type Target = StreamData;

    fn deref(&self) -> &StreamData {
        match self {
            StreamRef::Borrowed(s) => s,
            StreamRef::Owned(s) => s,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SetRef::contains` parsed the *query* with a bare `parse::<i64>()`, so
    /// `SISMEMBER s 000000012345` matched a stored intset member 12345.
    /// Redis treats the two as different members and answers 0.
    #[test]
    fn intset_contains_rejects_non_canonical_query() {
        let mut is = Intset::new();
        is.insert(12345);
        let sref = SetRef::Intset(&is);

        assert!(sref.contains(b"12345"), "exact member must be found");
        for probe in [&b"000000012345"[..], b"+12345"] {
            assert!(
                !sref.contains(probe),
                "{:?} must not match a stored 12345",
                std::str::from_utf8(probe).unwrap()
            );
        }
    }
}

#[cfg(test)]
mod set_nth_tests {
    use super::*;
    use crate::storage::entry::SetValue;
    use crate::storage::intset::Intset;
    use crate::storage::listpack::Listpack;

    /// `nth` must enumerate exactly the same multiset as `members()` on every
    /// representation -- it is a cheaper route to the same answer, not a
    /// different one. If it ever disagreed, SPOP would return members the set
    /// does not contain, or miss members it does.
    #[test]
    fn nth_enumerates_the_same_members_as_members() {
        let vals: Vec<Bytes> = vec![
            Bytes::from_static(b"10"),
            Bytes::from_static(b"20"),
            Bytes::from_static(b"30"),
        ];

        let mut hash = SetValue::new();
        for v in &vals {
            hash.insert(v.clone());
        }
        let mut lp = Listpack::new();
        for v in &vals {
            lp.push_back(v);
        }
        let mut is = Intset::new();
        for v in &vals {
            let n: i64 = std::str::from_utf8(v).unwrap().parse().unwrap();
            is.insert(n);
        }

        let owned = hash.clone();
        let refs: Vec<SetRef<'_>> = vec![
            SetRef::Hash(&hash),
            SetRef::Listpack(&lp),
            SetRef::Intset(&is),
            SetRef::Owned(owned),
        ];

        for (i, r) in refs.iter().enumerate() {
            assert_eq!(r.len(), 3, "repr {i}: wrong len");
            let mut by_nth: Vec<Vec<u8>> =
                (0..r.len()).map(|k| r.nth(k).unwrap().to_vec()).collect();
            let mut by_members: Vec<Vec<u8>> =
                r.members().into_iter().map(|b| b.to_vec()).collect();
            by_nth.sort();
            by_members.sort();
            assert_eq!(by_nth, by_members, "repr {i}: nth() and members() disagree");
        }
    }

    /// Out-of-range must be `None`, not a panic and not a wrap-around. SPOP
    /// picks `rng.random_range(0..len)`, so an off-by-one in either direction
    /// would be a live crash or a silently skewed distribution.
    #[test]
    fn nth_out_of_range_is_none() {
        let mut hash = SetValue::new();
        hash.insert(Bytes::from_static(b"only"));
        let mut lp = Listpack::new();
        lp.push_back(b"only");
        let mut is = Intset::new();
        is.insert(7);

        for (i, r) in [
            SetRef::Hash(&hash),
            SetRef::Listpack(&lp),
            SetRef::Intset(&is),
        ]
        .iter()
        .enumerate()
        {
            assert!(r.nth(0).is_some(), "repr {i}: index 0 should exist");
            assert!(r.nth(1).is_none(), "repr {i}: index 1 must be None");
            assert!(r.nth(usize::MAX).is_none(), "repr {i}: MAX must be None");
        }

        let empty = SetValue::new();
        assert!(
            SetRef::Hash(&empty).nth(0).is_none(),
            "empty set has no nth(0)"
        );
    }

    /// Every index in 0..len must be reachable and distinct. A representation
    /// that returned the same member for two indices would make SPOP with a
    /// count return duplicates of a member it had already removed.
    #[test]
    fn nth_covers_every_index_exactly_once() {
        let mut hash = SetValue::new();
        for i in 0..64u32 {
            hash.insert(Bytes::from(format!("m{i}")));
        }
        let r = SetRef::Hash(&hash);
        let mut seen: Vec<Vec<u8>> = (0..r.len()).map(|k| r.nth(k).unwrap().to_vec()).collect();
        let total = seen.len();
        seen.sort();
        seen.dedup();
        assert_eq!(total, 64, "not every index yielded a member");
        assert_eq!(seen.len(), 64, "two indices returned the same member");
    }

    /// `swap_remove` is what keeps removal O(1); it reorders, and `nth` must
    /// stay consistent with the set AFTER a removal -- no stale index, no
    /// hole, and the removed member must be gone from every index.
    #[test]
    fn nth_stays_consistent_across_swap_remove() {
        let mut hash = SetValue::new();
        for i in 0..16u32 {
            hash.insert(Bytes::from(format!("m{i}")));
        }
        let victim = Bytes::from_static(b"m7");
        assert!(hash.swap_remove(&victim));

        let r = SetRef::Hash(&hash);
        assert_eq!(r.len(), 15);
        let all: Vec<Vec<u8>> = (0..r.len()).map(|k| r.nth(k).unwrap().to_vec()).collect();
        assert_eq!(all.len(), 15, "a hole appeared after swap_remove");
        assert!(
            !all.iter().any(|m| m.as_slice() == b"m7"),
            "the removed member is still reachable by index"
        );
        let mut d = all.clone();
        d.sort();
        d.dedup();
        assert_eq!(d.len(), 15, "swap_remove left a duplicate reachable");
    }
}
