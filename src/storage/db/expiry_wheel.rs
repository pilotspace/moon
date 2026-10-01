//! Time-bucket wheel for the whole-key expiry index (moon#1298, PROTOTYPE).
//!
//! Behind `MOON_EXPIRY_WHEEL=1` (default OFF = the sorted-set index in
//! `expiry_index.rs`). One binary does the A/B.
//!
//! ## Design
//!
//! * **Bucket granularity: 256 ms.** A deadline `ts` lives in bucket
//!   `ts >> 8`; buckets are an ordered `BTreeMap<bucket_id, Bucket>`, so the
//!   head (smallest deadline) is the first bucket's first element — exact
//!   order, no scan, no cascade, no promotion stall.
//! * **A reference is ONE `u64`, not a key.** `entry = (ts & 0xFF) << 56 |
//!   key_hash >> 8`: the high byte is the deadline's offset inside its
//!   bucket (so numeric order is `(deadline, hash)` order), the low 56 bits
//!   are `xxh64(key) >> 8`. The DashTable derives the segment from the TOP
//!   bits and the home buckets from bits >= 8 (`(h >> 8) % 56`,
//!   `(h >> 16) % 56`), so those 56 bits are enough to find the segment a key
//!   lives in; nothing reads the low 8 bits. The `CompactKey` copy (24 B, plus
//!   a heap `Box<[u8]>` for keys over 23 B) and the B-tree node overhead
//!   around it are gone: ~12 B per TTL key in a dense bucket.
//! * **Resolution.** A reference is turned back into a key by scanning the
//!   ONE segment its hash selects for an entry whose own `expires_at_ms()`
//!   equals the reference's deadline and whose `xxh64 >> 8` matches. The
//!   entry's own deadline stays authoritative: a 56-bit-hash collision that
//!   resolves to a different key can only name a key whose real deadline is
//!   also `ts`, i.e. one that is due exactly when the reference says so, and
//!   whose own reference then resolves to its colliding twin. Expiry stays
//!   correct; at 56 bits it is also vanishingly rare.
//! * **Inserts / removes are exact and eager** — the writers pass the deadline
//!   they are retiring, so a TTL change or a delete removes the one `u64` it
//!   inserted (no tombstones, no stale-entry leak, `len` is exact).
//! * **Sparse buckets.** A bucket holding one reference is stored as
//!   `Bucket::One(u64)`, not a one-element `BTreeSet`, which bounds the
//!   worst case (random TTLs over weeks, ~1 key per 256 ms) at the cost of
//!   the `BTreeMap` slot. A real adoption would need coarser far levels with
//!   a cascade; that is exactly the part this prototype does not build.
//!
//! Same-millisecond ties are ordered by hash, not by key bytes (the sorted
//! set orders by bytes). Nothing observable depends on it: the sweep drains
//! every due key, and `volatile-ttl` only promises the nearest DEADLINE.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::{DashTable, hash_key};
use crate::storage::entry::Entry;

/// Bucket width exponent: 2^8 = 256 ms, so the in-bucket offset is one byte.
const BUCKET_SHIFT: u32 = 8;
const OFFSET_MASK: u64 = (1 << BUCKET_SHIFT) - 1;
/// Bits of the hash kept in a reference (`hash >> 8`).
const HASH_BITS: u32 = 56;

/// Most unresolvable (provably stale) references one `peek_nearest` call will
/// retire before giving up. A latency bound, not a correctness one.
const PEEK_STALE_REAP_LIMIT: usize = 64;

#[inline]
fn pack(ts: u64, hash: u64) -> u64 {
    ((ts & OFFSET_MASK) << HASH_BITS) | (hash >> 8)
}

/// The hash bits a reference carries, in the position `hash_key` produces
/// them (low byte zero).
#[inline]
fn hash_of(entry: u64) -> u64 {
    entry << 8
}

#[inline]
fn deadline_of(bucket: u64, entry: u64) -> u64 {
    (bucket << BUCKET_SHIFT) | (entry >> HASH_BITS)
}

/// `Many` is boxed so the enum is 16 bytes: a sparse wheel (one key per
/// bucket) pays 24 B per `BTreeMap` slot, not 40.
#[derive(Debug)]
enum Bucket {
    One(u64),
    Many(Box<BTreeSet<u64>>),
}

/// The wheel. `len` is the exact number of references held.
#[derive(Debug, Default)]
pub(crate) struct ExpiryWheel {
    buckets: BTreeMap<u64, Bucket>,
    len: usize,
}

impl ExpiryWheel {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn clear(&mut self) {
        self.buckets.clear();
        self.len = 0;
    }

    /// Insert the reference for `(ts, key)`. A duplicate is a no-op, like the
    /// sorted set's.
    pub(crate) fn insert(&mut self, ts: u64, key: &[u8]) {
        self.insert_hash(ts, hash_key(key));
    }

    fn insert_hash(&mut self, ts: u64, hash: u64) {
        use std::collections::btree_map::Entry as MapEntry;
        let e = pack(ts, hash);
        match self.buckets.entry(ts >> BUCKET_SHIFT) {
            MapEntry::Vacant(v) => {
                v.insert(Bucket::One(e));
                self.len += 1;
            }
            MapEntry::Occupied(mut o) => match o.get_mut() {
                Bucket::One(x) => {
                    if *x != e {
                        let mut set = BTreeSet::new();
                        set.insert(*x);
                        set.insert(e);
                        *o.get_mut() = Bucket::Many(Box::new(set));
                        self.len += 1;
                    }
                }
                Bucket::Many(set) => {
                    if set.insert(e) {
                        self.len += 1;
                    }
                }
            },
        }
    }

    /// Remove the reference for `(ts, key)`. Returns whether it was present.
    pub(crate) fn remove(&mut self, ts: u64, key: &[u8]) -> bool {
        self.remove_hash(ts, hash_key(key))
    }

    fn remove_hash(&mut self, ts: u64, hash: u64) -> bool {
        let e = pack(ts, hash);
        let id = ts >> BUCKET_SHIFT;
        let Some(b) = self.buckets.get_mut(&id) else {
            return false;
        };
        let (removed, drop_bucket) = match b {
            Bucket::One(x) => (*x == e, *x == e),
            Bucket::Many(set) => {
                let removed = set.remove(&e);
                if removed && set.len() == 1 {
                    // Collapse: a one-element BTreeSet is a whole leaf node.
                    if let Some(only) = set.first().copied() {
                        *b = Bucket::One(only);
                    }
                }
                (removed, false)
            }
        };
        if drop_bucket {
            self.buckets.remove(&id);
        }
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// Whether the reference for `(ts, key)` is present.
    #[cfg(test)]
    pub(crate) fn contains(&self, ts: u64, key: &[u8]) -> bool {
        let e = pack(ts, hash_key(key));
        match self.buckets.get(&(ts >> BUCKET_SHIFT)) {
            Some(Bucket::One(x)) => *x == e,
            Some(Bucket::Many(set)) => set.contains(&e),
            None => false,
        }
    }

    /// The head reference `(deadline, hash-with-low-byte-zero)`, if any.
    #[inline]
    fn first(&self) -> Option<(u64, u64)> {
        let (id, b) = self.buckets.first_key_value()?;
        let e = match b {
            Bucket::One(x) => *x,
            Bucket::Many(set) => *set.first()?,
        };
        Some((deadline_of(*id, e), hash_of(e)))
    }

    /// Earliest deadline held, without resolving it to a key.
    #[inline]
    pub(crate) fn first_ts(&self) -> Option<u64> {
        self.first().map(|(ts, _)| ts)
    }

    #[cfg(test)]
    fn pop_first(&mut self) -> Option<(u64, u64)> {
        self.pop_first_through(u64::MAX)
    }

    /// Pop the head reference if its deadline is `<= limit`: ONE descent of
    /// the bucket map and at most one of the bucket's set.
    fn pop_first_through(&mut self, limit: u64) -> Option<(u64, u64)> {
        let mut slot = self.buckets.first_entry()?;
        let id = *slot.key();
        let (e, drop_bucket) = match slot.get_mut() {
            Bucket::One(x) => {
                if deadline_of(id, *x) > limit {
                    return None;
                }
                (*x, true)
            }
            Bucket::Many(set) => {
                let head = *set.first()?;
                if deadline_of(id, head) > limit {
                    return None;
                }
                set.pop_first();
                if set.len() == 1 {
                    if let Some(only) = set.first().copied() {
                        *slot.get_mut() = Bucket::One(only);
                    }
                }
                (head, false)
            }
        };
        if drop_bucket {
            slot.remove();
        }
        self.len -= 1;
        Some((deadline_of(id, e), hash_of(e)))
    }

    /// Every `(deadline, hash)` held, in order (tests / O(N) diagnostics).
    pub(crate) fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.buckets.iter().flat_map(|(id, b)| {
            let it: Box<dyn Iterator<Item = u64> + '_> = match b {
                Bucket::One(x) => Box::new(std::iter::once(*x)),
                Bucket::Many(set) => Box::new(set.iter().copied()),
            };
            it.map(move |e| (deadline_of(*id, e), hash_of(e)))
        })
    }

    /// Pop the head if it is due at `now_ms` and resolve it to its key.
    ///
    /// A head that resolves to NO live entry (the entry is gone or carries a
    /// different deadline) is provably stale: it is dropped and the next head
    /// is tried, exactly as the sweep treats a stale sorted-set pair.
    pub(crate) fn pop_due(
        &mut self,
        now_ms: u64,
        data: &DashTable<CompactKey, Entry>,
    ) -> Option<(u64, CompactKey)> {
        while let Some((ts, h)) = self.pop_first_through(now_ms) {
            if let Some(key) = resolve(data, ts, h) {
                return Some((ts, key.clone()));
            }
        }
        None
    }

    /// The head resolved to its key, due or not (the `volatile-ttl` victim).
    /// Retires up to [`PEEK_STALE_REAP_LIMIT`] unresolvable heads on the way.
    pub(crate) fn peek_nearest(
        &mut self,
        data: &DashTable<CompactKey, Entry>,
    ) -> Option<(u64, CompactKey)> {
        for _ in 0..PEEK_STALE_REAP_LIMIT {
            let (ts, h) = self.first()?;
            if let Some(key) = resolve(data, ts, h) {
                return Some((ts, key.clone()));
            }
            self.remove_hash(ts, h);
        }
        None
    }

    /// The head resolved to its key if it is due (read-only; an unresolvable
    /// head reads as "nothing due" here and is retired by `pop_due`).
    pub(crate) fn peek_due(
        &self,
        now_ms: u64,
        data: &DashTable<CompactKey, Entry>,
    ) -> Option<(u64, CompactKey)> {
        let (ts, h) = self.first()?;
        if ts > now_ms {
            return None;
        }
        resolve(data, ts, h).map(|k| (ts, k.clone()))
    }
}

/// Find the live key a reference names: scan the one segment the hash selects
/// for an entry whose OWN deadline is `ts` and whose hash agrees on the 56
/// bits a reference carries. Cheap field test first, hash only on a match.
pub(crate) fn resolve(
    data: &DashTable<CompactKey, Entry>,
    ts: u64,
    hash: u64,
) -> Option<&CompactKey> {
    data.segment(data.segment_index_for_hash(hash))
        .iter_occupied()
        .find(|(k, e)| e.expires_at_ms() == ts && (hash_key(k.as_bytes()) & !0xFF) == hash)
        .map(|(k, _)| k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_orders_by_offset_then_hash_and_round_trips() {
        let a = pack(0x1234_5601, 0xAAAA_AAAA_AAAA_AA00);
        let b = pack(0x1234_5602, 0x1111_1111_1111_1100);
        assert!(a < b, "offset dominates the hash in the order");
        assert_eq!(deadline_of(0x1234_5601 >> BUCKET_SHIFT, a), 0x1234_5601);
        assert_eq!(hash_of(a), 0xAAAA_AAAA_AAAA_AA00);
    }

    #[test]
    fn insert_remove_len_and_collapse() {
        let mut w = ExpiryWheel::default();
        w.insert(1000, b"a");
        w.insert(1000, b"a"); // duplicate: no-op
        assert_eq!(w.len(), 1);
        w.insert(1001, b"b");
        w.insert(5000, b"c");
        assert_eq!(w.len(), 3);
        assert!(w.contains(1000, b"a") && w.contains(1001, b"b"));
        assert!(!w.remove(1000, b"zz"));
        assert!(w.remove(1000, b"a"));
        assert!(!w.remove(1000, b"a"));
        assert_eq!(w.len(), 2);
        assert_eq!(w.first_ts(), Some(1001));
        assert!(w.remove(1001, b"b"));
        assert_eq!(w.first_ts(), Some(5000));
        assert!(w.remove(5000, b"c"));
        assert_eq!(w.len(), 0);
        assert!(w.buckets.is_empty(), "no empty bucket is left behind");
    }

    #[test]
    fn head_is_the_exact_smallest_deadline_across_and_within_buckets() {
        let mut w = ExpiryWheel::default();
        // Same 256 ms bucket, different ms; and a neighbouring bucket.
        for (ts, k) in [(1300u64, "x"), (1281, "y"), (1290, "z"), (1100, "w")] {
            w.insert(ts, k.as_bytes());
        }
        let order: Vec<u64> = w.iter().map(|(ts, _)| ts).collect();
        assert_eq!(order, vec![1100, 1281, 1290, 1300]);
        assert_eq!(w.first_ts(), Some(1100));
        assert_eq!(w.pop_first().map(|p| p.0), Some(1100));
        assert_eq!(w.pop_first().map(|p| p.0), Some(1281));
        assert_eq!(w.len(), 2);
    }
}
