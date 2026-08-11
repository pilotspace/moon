//! Fast non-cryptographic hasher for graph-internal keys.
//!
//! `NodeKey`/`EdgeKey` are slotmap keys (u64 POD) that only enter maps and
//! sets we control (visited sets, distance maps, frontier dedup) — SipHash's
//! HashDoS resistance buys nothing there and costs several times more per
//! op. This is the FxHash multiply-fold (rustc's own internal hasher),
//! inlined here to avoid a new dependency.
//!
//! NOT DoS-resistant: never key these maps by attacker-controlled bytes
//! (client key names, query strings). Slotmap keys and label ids only.

use std::hash::{BuildHasherDefault, Hasher};

/// FxHash-style multiplicative hasher (see module docs for the safety
/// contract on key provenance).
#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add_to_hash(&mut self, i: u64) {
        self.hash = (self.hash.rotate_left(5) ^ i).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            self.add_to_hash(u64::from_le_bytes(buf));
        }
    }
    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add_to_hash(u64::from(i));
    }
    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.add_to_hash(u64::from(i));
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add_to_hash(u64::from(i));
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add_to_hash(i);
    }
    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add_to_hash(i as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// BuildHasher for [`FxHasher`].
pub type FxBuildHasher = BuildHasherDefault<FxHasher>;
/// HashMap keyed with [`FxHasher`].
pub type FxHashMap<K, V> = std::collections::HashMap<K, V, FxBuildHasher>;
/// HashSet keyed with [`FxHasher`].
pub type FxHashSet<T> = std::collections::HashSet<T, FxBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fx_map_and_set_basic() {
        let mut m: FxHashMap<u64, u32> = FxHashMap::default();
        for i in 0..1000u64 {
            m.insert(i, (i * 2) as u32);
        }
        assert_eq!(m.len(), 1000);
        assert_eq!(m.get(&500), Some(&1000));

        let mut s: FxHashSet<u64> = FxHashSet::default();
        assert!(s.insert(7));
        assert!(!s.insert(7));
        assert!(s.contains(&7));
    }

    #[test]
    fn test_fx_hasher_distributes() {
        // Sequential keys must not collapse to a few buckets: distinct
        // hashes for distinct inputs (sanity, not a statistical test).
        use std::hash::BuildHasher;
        let bh = FxBuildHasher::default();
        let mut hashes: FxHashSet<u64> = FxHashSet::default();
        for i in 0..10_000u64 {
            hashes.insert(bh.hash_one(i));
        }
        assert_eq!(hashes.len(), 10_000, "no collisions on 10k sequential u64");
    }
}
