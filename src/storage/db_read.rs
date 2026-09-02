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
                // Borrowed scan: only the value that actually matches is
                // materialized, instead of every field walked past.
                match lp.find_pair_index(field) {
                    Some(idx) => lp.get_at(idx * 2 + 1).map(|v| v.to_bytes()),
                    None => None,
                }
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
            HashRef::Listpack(lp) => lp
                .iter_pairs()
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

    /// Get element at index.
    pub fn get(&self, index: usize) -> Option<Bytes> {
        match self {
            ListRef::Deque(d) => d.get(index).cloned(),
            ListRef::Listpack(lp) => lp.get_at(index).map(|e| e.to_bytes()),
            ListRef::Owned(d) => d.get(index).cloned(),
        }
    }

    /// Get a range of elements [start..=end]. Caller must clamp bounds.
    pub fn range(&self, start: usize, end: usize) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => (start..=end).filter_map(|i| d.get(i).cloned()).collect(),
            ListRef::Listpack(lp) => (start..=end)
                .filter_map(|i| lp.get_at(i).map(|e| e.to_bytes()))
                .collect(),
            ListRef::Owned(d) => (start..=end).filter_map(|i| d.get(i).cloned()).collect(),
        }
    }

    /// Iterate all elements (for LPOS).
    pub fn iter_bytes(&self) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => d.iter().cloned().collect(),
            ListRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
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
    ///     `LISTPACK_MAX_ENTRIES` (128) members, so the walk is bounded.
    ///
    /// Returns `None` when `idx >= len()`.
    pub fn nth(&self, idx: usize) -> Option<Bytes> {
        match self {
            SetRef::Hash(s) => s.get_index(idx).cloned(),
            SetRef::Owned(s) => s.get_index(idx).cloned(),
            SetRef::Intset(is) => is.get(idx).map(|v| Bytes::from(v.to_string())),
            SetRef::Listpack(lp) => lp.get_at(idx).map(|e| e.to_bytes()),
        }
    }

    /// Return all members as Bytes.
    pub fn members(&self) -> Vec<Bytes> {
        match self {
            SetRef::Hash(s) => s.iter().cloned().collect(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
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
            SetRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
            SetRef::Owned(s) => s.iter().cloned().collect(),
        }
    }
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
                // Borrowed scan: walk members without materializing each one,
                // then decode only the score that belongs to the match.
                let idx = lp.find_pair_index(member)?;
                match lp.get_at(idx * 2 + 1)? {
                    super::listpack::ListpackEntry::Integer(i) => Some(i as f64),
                    super::listpack::ListpackEntry::String(ref b) => {
                        std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                    }
                }
            }
            SortedSetRef::Legacy { members, .. } => members.get(member).copied(),
            SortedSetRef::Owned { members, .. } => members.get(member).copied(),
        }
    }

    /// Get all (member, score) pairs sorted by score then member.
    pub fn entries_sorted(&self) -> Vec<(Bytes, f64)> {
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
                    pairs.push((m.to_bytes(), score));
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
