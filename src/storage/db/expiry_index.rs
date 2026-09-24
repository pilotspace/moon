//! The whole-key deadline index's element type (moon#1189).
//!
//! `Database::expiry_index` holds one [`ExpiryPair`] per hot key with a TTL,
//! ordered by `(deadline_ms, key bytes)` — exactly the order the old
//! `(u64, CompactKey)` tuple had, since `CompactKey` orders by its bytes.
//!
//! The newtype exists so the index can be SEARCHED without owning a key.
//! A tuple can only be looked up by another tuple, so every unindex
//! (`remove_hot`, every TTL change in `set` / `set_expiry`) built a
//! `CompactKey::from(key)` — a heap copy for keys over 23 bytes — just to
//! find and drop the pair. [`ExpiryPair`] also borrows as
//! `dyn ExpiryLookup`, which `(u64, &[u8])` implements, so a lookup is
//! `set.remove(&(ts, key) as &dyn ExpiryLookup)`: no allocation.

use std::borrow::Borrow;
use std::cmp::Ordering;

use crate::storage::compact_key::CompactKey;

/// One `(deadline, key)` pair of the whole-key expiry index.
///
/// Field order matters: the derived `Ord` compares `ts` first, then `key`
/// (bytewise), and [`ExpiryLookup`]'s `Ord` below must agree with it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ExpiryPair {
    pub(crate) ts: u64,
    pub(crate) key: CompactKey,
}

/// A borrowed view of an index pair, for allocation-free lookups.
pub(crate) trait ExpiryLookup {
    fn ts(&self) -> u64;
    fn key(&self) -> &[u8];
}

impl ExpiryLookup for ExpiryPair {
    #[inline]
    fn ts(&self) -> u64 {
        self.ts
    }
    #[inline]
    fn key(&self) -> &[u8] {
        self.key.as_bytes()
    }
}

impl ExpiryLookup for (u64, &[u8]) {
    #[inline]
    fn ts(&self) -> u64 {
        self.0
    }
    #[inline]
    fn key(&self) -> &[u8] {
        self.1
    }
}

impl<'a> Borrow<dyn ExpiryLookup + 'a> for ExpiryPair {
    #[inline]
    fn borrow(&self) -> &(dyn ExpiryLookup + 'a) {
        self
    }
}

impl PartialEq for dyn ExpiryLookup + '_ {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.ts() == other.ts() && self.key() == other.key()
    }
}

impl Eq for dyn ExpiryLookup + '_ {}

impl PartialOrd for dyn ExpiryLookup + '_ {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn ExpiryLookup + '_ {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts()
            .cmp(&other.ts())
            .then_with(|| self.key().cmp(other.key()))
    }
}

/// Look `(ts, key)` up in an index without building a key.
#[inline]
pub(crate) fn lookup(ts: u64, key: &[u8]) -> (u64, &[u8]) {
    (ts, key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn pair(ts: u64, k: &[u8]) -> ExpiryPair {
        ExpiryPair {
            ts,
            key: CompactKey::from(k),
        }
    }

    /// The borrowed order must be the owned order, or `BTreeSet` lookups
    /// through `dyn ExpiryLookup` silently miss.
    #[test]
    fn borrowed_order_agrees_with_owned_order() {
        let long = b"a-key-longer-than-twenty-three-bytes-lives-on-the-heap";
        let pairs = [
            pair(5, b"b"),
            pair(5, b"a"),
            pair(4, b"zzz"),
            pair(5, long),
            pair(6, b""),
            pair(5, b"ab"),
        ];
        for x in &pairs {
            for y in &pairs {
                let bx: &dyn ExpiryLookup = x;
                let by: &dyn ExpiryLookup = y;
                assert_eq!(x.cmp(y), bx.cmp(by), "{x:?} vs {y:?}");
            }
        }
        let mut set: BTreeSet<ExpiryPair> = pairs.iter().cloned().collect();
        assert!(set.contains(&lookup(5, long) as &dyn ExpiryLookup));
        assert!(set.remove(&lookup(5, b"a") as &dyn ExpiryLookup));
        assert!(!set.remove(&lookup(5, b"a") as &dyn ExpiryLookup));
        assert!(!set.remove(&lookup(7, b"b") as &dyn ExpiryLookup));
        assert_eq!(set.len(), pairs.len() - 1);
        assert_eq!(set.first(), Some(&pair(4, b"zzz")));
    }
}
