//! Bucketed copy-on-write key-hash map (RSS/CPU wave 4, defect 1 —
//! `Arc<HashMap>` CoW amplification).
//!
//! ## Problem
//!
//! `VectorIndex` keeps three `key_hash -> V` maps (`key_hash_to_key`,
//! `key_hash_to_global_id`, `key_hash_to_vec_checksum`) that used to be a
//! single flat `Arc<HashMap<u64, V>>`. Every FT.SEARCH snapshot
//! (`SearchSnapshot::key_hash_to_key`) clones the `Arc` cheaply (a refcount
//! bump), but `ft-search-off-eventloop` lets the shard event loop keep
//! processing writes while that search `.await`s. The next HSET/HDEL calls
//! `Arc::make_mut` on the map — which sees refcount > 1 and deep-clones the
//! ENTIRE map (tens of MB at scale) SYNCHRONOUSLY on the shard thread, once
//! per write, for the whole lifetime of the live search.
//!
//! ## Fix
//!
//! Shard the map into [`NUM_BUCKETS`] independent `Arc<HashMap<u64, V>>`
//! buckets, keyed by the top bits of the (already well-distributed) xxh64
//! key hash. A search snapshot still clones the whole structure in O(1) —
//! [`NUM_BUCKETS`] refcount bumps, no data copy — but a concurrent
//! insert/remove only calls `Arc::make_mut` on the ONE bucket the key hashes
//! into. Worst case, a write during a live snapshot clones ~1/256th of the
//! map instead of all of it.
//!
//! Persistence (`keymap.bin`) is unaffected: [`BucketedKeyMap::iter`] walks
//! every bucket and the on-disk format is a flat, hash-keyed entry list that
//! was never order-sensitive (recovery re-inserts by key_hash into fresh
//! buckets — see `crate::vector::persistence::recover_v2`).

use std::collections::HashMap;
use std::sync::Arc;

/// Number of independent CoW buckets. Bucket index is the top 8 bits of the
/// `u64` key hash (`hash >> 56`), which is uniformly distributed for xxh64
/// output — no separate mixing step needed.
pub const NUM_BUCKETS: usize = 256;

/// A `key_hash -> V` map, internally sharded into [`NUM_BUCKETS`]
/// independently copy-on-write buckets.
///
/// Cloning a `BucketedKeyMap` is O(`NUM_BUCKETS`) — a fixed number of `Arc`
/// refcount bumps, never a data copy. This is what makes search-snapshot
/// capture cheap (see `SearchSnapshot::key_hash_to_key`) while keeping
/// concurrent-write CoW cost bucket-scoped instead of map-wide.
pub struct BucketedKeyMap<V> {
    buckets: Box<[Arc<HashMap<u64, V>>]>,
}

impl<V> BucketedKeyMap<V> {
    /// Create an empty map with all `NUM_BUCKETS` buckets allocated (each an
    /// `Arc` around an empty `HashMap` — no per-bucket heap allocation until
    /// the first insert into that bucket, since an empty `HashMap` doesn't
    /// allocate).
    pub fn new() -> Self {
        let buckets: Vec<Arc<HashMap<u64, V>>> =
            (0..NUM_BUCKETS).map(|_| Arc::new(HashMap::new())).collect();
        Self {
            buckets: buckets.into_boxed_slice(),
        }
    }

    /// Bucket index for a given key hash: top 8 bits.
    #[inline]
    fn bucket_of(key_hash: u64) -> usize {
        (key_hash >> 56) as usize
    }

    /// Look up a value by key hash.
    #[inline]
    pub fn get(&self, key_hash: &u64) -> Option<&V> {
        self.buckets[Self::bucket_of(*key_hash)].get(key_hash)
    }

    /// Whether `key_hash` is present.
    #[inline]
    pub fn contains_key(&self, key_hash: &u64) -> bool {
        self.buckets[Self::bucket_of(*key_hash)].contains_key(key_hash)
    }

    /// Insert a value, returning the previous value (if any). Only the
    /// bucket `key_hash` maps into is touched — `Arc::make_mut` there clones
    /// that ONE bucket's `HashMap` if (and only if) a snapshot is
    /// concurrently holding a reference to it.
    pub fn insert(&mut self, key_hash: u64, value: V) -> Option<V>
    where
        V: Clone,
    {
        let idx = Self::bucket_of(key_hash);
        Arc::make_mut(&mut self.buckets[idx]).insert(key_hash, value)
    }

    /// Remove a value by key hash, returning it if present. Bucket-scoped,
    /// same CoW cost profile as [`Self::insert`].
    pub fn remove(&mut self, key_hash: &u64) -> Option<V>
    where
        V: Clone,
    {
        let idx = Self::bucket_of(*key_hash);
        Arc::make_mut(&mut self.buckets[idx]).remove(key_hash)
    }

    /// Entry-style get-or-insert: returns the existing value for `key_hash`,
    /// or computes `default()` and inserts it. `default` is only invoked when
    /// the key is absent (mirrors `HashMap::entry(..).or_insert_with`).
    /// Bucket-scoped CoW, same as [`Self::insert`].
    pub fn get_or_insert_with(&mut self, key_hash: u64, default: impl FnOnce() -> V) -> &V
    where
        V: Clone,
    {
        let idx = Self::bucket_of(key_hash);
        Arc::make_mut(&mut self.buckets[idx])
            .entry(key_hash)
            .or_insert_with(default)
    }

    /// Total number of entries across all buckets. O(`NUM_BUCKETS`).
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    /// Whether the map has no entries. O(`NUM_BUCKETS`).
    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.is_empty())
    }

    /// Iterate all `(key_hash, value)` pairs across every bucket. Used by the
    /// persistence snapshot writer (order is not significant — the on-disk
    /// keymap format is keyed by `key_hash`, not by insertion/iteration
    /// order) and by admin paths that need every key (e.g. `FT.DROPINDEX
    /// DD`).
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &V)> {
        self.buckets.iter().flat_map(|b| b.iter())
    }

    /// Iterate all key hashes across every bucket.
    pub fn keys(&self) -> impl Iterator<Item = &u64> {
        self.iter().map(|(k, _)| k)
    }

    /// Iterate all values across every bucket.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    /// Cheap O(`NUM_BUCKETS`) snapshot: clones the bucket-`Arc` array (refcount
    /// bumps only, zero data copy). Semantically identical to [`Clone::clone`]
    /// — this named alias documents intent at capture sites (e.g.
    /// `SearchSnapshot` construction) that want to read "take an isolated
    /// view", not "duplicate the whole map".
    pub fn snapshot(&self) -> Self {
        self.clone()
    }
}

impl<V> Default for BucketedKeyMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

// Manual `Clone` impl (rather than `#[derive(Clone)]`) so cloning a
// `BucketedKeyMap<V>` never requires `V: Clone` — only `Arc<HashMap<u64,
// V>>: Clone` is needed, which holds unconditionally (Arc's Clone impl has
// no bound on its inner type). `#[derive(Clone)]` would add a spurious `V:
// Clone` bound to the generated impl.
impl<V> Clone for BucketedKeyMap<V> {
    fn clone(&self) -> Self {
        Self {
            buckets: self.buckets.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    /// Deterministic LCG (matches the style already used for pseudo-random
    /// test vectors elsewhere in the vector module, e.g.
    /// `persistence::recover_v2::f32_blob`) — no external `rand` dependency
    /// needed for a property-style test, and results are reproducible.
    struct Lcg(u64);
    impl Lcg {
        fn next_u64(&mut self) -> u64 {
            // Fixed-point LCG constants (Numerical Recipes).
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            self.0
        }
    }

    #[test]
    fn new_map_is_empty() {
        let m: BucketedKeyMap<u32> = BucketedKeyMap::new();
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        assert_eq!(m.get(&42), None);
    }

    #[test]
    fn insert_get_remove_single_key() {
        let mut m: BucketedKeyMap<u32> = BucketedKeyMap::new();
        assert_eq!(m.insert(100, 7), None);
        assert_eq!(m.get(&100), Some(&7));
        assert!(m.contains_key(&100));
        assert_eq!(m.len(), 1);

        // Overwrite returns the previous value.
        assert_eq!(m.insert(100, 9), Some(7));
        assert_eq!(m.get(&100), Some(&9));
        assert_eq!(m.len(), 1);

        assert_eq!(m.remove(&100), Some(9));
        assert_eq!(m.get(&100), None);
        assert!(!m.contains_key(&100));
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
    }

    #[test]
    fn get_or_insert_with_only_computes_on_vacant() {
        let mut m: BucketedKeyMap<u32> = BucketedKeyMap::new();
        let mut calls = 0;
        assert_eq!(
            *m.get_or_insert_with(1, || {
                calls += 1;
                5
            }),
            5
        );
        assert_eq!(calls, 1);
        // Second call on the same (now-occupied) key must NOT invoke the closure.
        assert_eq!(
            *m.get_or_insert_with(1, || {
                calls += 1;
                999
            }),
            5
        );
        assert_eq!(calls, 1);
    }

    /// Property test: thousands of random insert/remove ops against a
    /// `BucketedKeyMap` must match a plain reference `HashMap` at every step
    /// (len, get, contains_key) and in final full-iteration content.
    #[test]
    fn property_matches_reference_hashmap_over_random_ops() {
        let mut rng = Lcg(0xC0FFEE_u64);
        let mut reference: StdHashMap<u64, u64> = StdHashMap::new();
        let mut bucketed: BucketedKeyMap<u64> = BucketedKeyMap::new();

        // Keep the key space small enough that removes/overwrites actually
        // exercise existing entries, not just fresh inserts.
        const KEY_SPACE: u64 = 500;

        for step in 0..5000u64 {
            let raw = rng.next_u64();
            let key = raw % KEY_SPACE;
            // Spread the low-space keys across the full u64 range (and thus
            // across buckets) by mixing in a hashed key_hash instead of using
            // `key` directly as the map key — mirrors real key_hash usage
            // (xxh64 output), not a small dense integer.
            let key_hash = key_hash_for_test(key);
            let op = raw % 3;
            match op {
                0 => {
                    // insert
                    let value = step;
                    let expected_prev = reference.insert(key_hash, value);
                    let actual_prev = bucketed.insert(key_hash, value);
                    assert_eq!(actual_prev, expected_prev, "insert mismatch at step {step}");
                }
                1 => {
                    // remove
                    let expected = reference.remove(&key_hash);
                    let actual = bucketed.remove(&key_hash);
                    assert_eq!(actual, expected, "remove mismatch at step {step}");
                }
                _ => {
                    // get / contains_key (read-only)
                    assert_eq!(
                        bucketed.get(&key_hash),
                        reference.get(&key_hash),
                        "get mismatch at step {step}"
                    );
                    assert_eq!(
                        bucketed.contains_key(&key_hash),
                        reference.contains_key(&key_hash),
                        "contains_key mismatch at step {step}"
                    );
                }
            }
            assert_eq!(
                bucketed.len(),
                reference.len(),
                "len mismatch at step {step}"
            );
        }

        assert_eq!(bucketed.len(), reference.len());
        assert_eq!(bucketed.is_empty(), reference.is_empty());
        for (k, v) in &reference {
            assert_eq!(
                bucketed.get(k),
                Some(v),
                "final content mismatch for key {k}"
            );
        }
        let mut bucketed_sorted: Vec<(u64, u64)> = bucketed.iter().map(|(k, v)| (*k, *v)).collect();
        bucketed_sorted.sort_unstable();
        let mut reference_sorted: Vec<(u64, u64)> =
            reference.iter().map(|(k, v)| (*k, *v)).collect();
        reference_sorted.sort_unstable();
        assert_eq!(bucketed_sorted, reference_sorted);
    }

    /// Deterministic mixer so a small dense key space still spreads across
    /// the full u64 range (hence across all 256 buckets), matching how real
    /// `key_hash` values (xxh64 of a Redis key) are distributed.
    fn key_hash_for_test(key: u64) -> u64 {
        key.wrapping_mul(0x9E3779B97F4A7C15).rotate_left(31)
    }

    #[test]
    fn snapshot_isolation_mutations_after_snapshot_are_invisible() {
        let mut m: BucketedKeyMap<u32> = BucketedKeyMap::new();
        m.insert(1, 10);
        m.insert(2, 20);

        let snap = m.snapshot();
        assert_eq!(snap.get(&1), Some(&10));
        assert_eq!(snap.get(&2), Some(&20));

        // Mutate the LIVE map after the snapshot was taken.
        m.insert(1, 999); // update
        m.insert(3, 30); // new key
        m.remove(&2); // delete

        // Snapshot must be unaffected by any of the above.
        assert_eq!(
            snap.get(&1),
            Some(&10),
            "snapshot must not see post-snapshot update"
        );
        assert_eq!(
            snap.get(&3),
            None,
            "snapshot must not see post-snapshot insert"
        );
        assert_eq!(
            snap.get(&2),
            Some(&20),
            "snapshot must not see post-snapshot remove"
        );
        assert_eq!(snap.len(), 2);

        // Live map reflects all three mutations.
        assert_eq!(m.get(&1), Some(&999));
        assert_eq!(m.get(&3), Some(&30));
        assert_eq!(m.get(&2), None);
        assert_eq!(m.len(), 2);
    }

    /// THE key property (spec, "Fix: bucketed CoW keymap"): after
    /// `let snap = map.snapshot()`, a single `insert` on the live map must
    /// leave at least 255 of the 256 buckets `Arc::ptr_eq` between `snap`
    /// and the live map — proof the CoW clone triggered by that insert was
    /// bucket-scoped, not map-wide.
    #[test]
    fn insert_under_live_snapshot_clones_only_one_bucket() {
        let mut m: BucketedKeyMap<u32> = BucketedKeyMap::new();
        // Populate every bucket with at least one entry so a real HashMap
        // clone (if it happened) would be observable — an empty bucket
        // cloning is a degenerate case that wouldn't prove anything.
        for b in 0..NUM_BUCKETS as u64 {
            let key_hash = b << 56; // top byte = bucket index, rest zero
            m.insert(key_hash, b as u32);
        }

        let snap = m.snapshot();

        // One insert into bucket 0's key range.
        let target_key_hash = 0u64; // bucket_of(0) == 0
        m.insert(target_key_hash, 12345);

        let mut unchanged_buckets = 0usize;
        for i in 0..NUM_BUCKETS {
            if Arc::ptr_eq(&snap.buckets[i], &m.buckets[i]) {
                unchanged_buckets += 1;
            }
        }
        assert!(
            unchanged_buckets >= NUM_BUCKETS - 1,
            "expected at most 1 bucket to have cloned (bucket-scoped CoW), \
             but only {unchanged_buckets}/{NUM_BUCKETS} buckets stayed pointer-equal"
        );
        // The touched bucket (0) must actually have diverged (proves the
        // write really happened, not that make_mut was a no-op).
        assert!(
            !Arc::ptr_eq(&snap.buckets[0], &m.buckets[0]),
            "bucket 0 should have cloned since the snapshot pinned its old Arc"
        );

        // Sanity: the live map has the new value, the snapshot has the old one.
        assert_eq!(m.get(&target_key_hash), Some(&12345));
        assert_eq!(snap.get(&target_key_hash), Some(&0));
    }

    #[test]
    fn clone_does_not_require_value_clone_bound() {
        // A type that is NOT Clone — proves BucketedKeyMap::clone() (used by
        // `snapshot()`) never requires `V: Clone`.
        struct NotClone(#[allow(dead_code)] u8);

        let m: BucketedKeyMap<NotClone> = BucketedKeyMap::new();
        let cloned = m.clone(); // must compile without `V: Clone`
        assert_eq!(cloned.len(), 0);
    }

    #[test]
    fn default_is_empty() {
        let m: BucketedKeyMap<u32> = BucketedKeyMap::default();
        assert!(m.is_empty());
    }
}
