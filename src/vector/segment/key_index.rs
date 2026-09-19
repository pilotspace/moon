//! Per-segment `key_hash -> row positions` membership index.
//!
//! A steady-state tombstone names a KEY (`key_hash`), but it kills a ROW: the
//! copy of that key one segment holds. Before this index a segment could not
//! tell whether it held the key at all, so every segment recorded every
//! tombstone. That cost memory (one set entry per DEL per segment) and made
//! the live count wrong in both directions: HOT segments never counted a
//! steady-state tombstone, and WARM/COLD segments counted it once per segment
//! whether they held the key or not.
//!
//! The index is the segment's row positions sorted by key_hash (4 bytes per
//! row), built once when the segment is constructed -- on the compaction or
//! merge worker, or on the boot/reload thread, never on the shard thread's
//! delete path. Lookup is two binary searches.

/// Row positions sorted by the key_hash of the row they name.
///
/// The key_hash itself is not stored: the owning segment already has one per
/// row, and passes the accessor in. That keeps the index at 4 bytes per row.
#[derive(Debug, Default)]
pub struct KeyHashIndex {
    order: Box<[u32]>,
}

impl KeyHashIndex {
    /// Index `rows` rows, reading each row's key_hash through `key_hash_at`.
    pub fn build(rows: usize, key_hash_at: impl Fn(u32) -> u64) -> Self {
        let rows = u32::try_from(rows).unwrap_or(u32::MAX);
        // Sort (key_hash, pos) pairs read in one sequential pass rather than
        // positions compared through `key_hash_at`: the comparator then never
        // chases a row pointer, which is what makes boot-time recovery of a
        // large segment pay for this index in a single sort.
        let mut pairs: Vec<(u64, u32)> = (0..rows).map(|pos| (key_hash_at(pos), pos)).collect();
        pairs.sort_unstable();
        Self {
            order: pairs.into_iter().map(|(_, pos)| pos).collect(),
        }
    }

    /// Every row position whose key_hash is `key_hash` (empty when the segment
    /// holds no row for the key). `key_hash_at` must be the accessor the index
    /// was built with.
    pub fn positions(&self, key_hash: u64, key_hash_at: impl Fn(u32) -> u64) -> &[u32] {
        let start = self
            .order
            .partition_point(|&pos| key_hash_at(pos) < key_hash);
        let len = self.order[start..].partition_point(|&pos| key_hash_at(pos) == key_hash);
        &self.order[start..start + len]
    }

    /// Heap bytes held by the index.
    pub fn resident_bytes(&self) -> usize {
        self.order.len() * std::mem::size_of::<u32>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_finds_every_row_of_a_key_and_nothing_else() {
        let hashes = [30u64, 10, 20, 10, 40, 30];
        let at = |p: u32| hashes[p as usize];
        let idx = KeyHashIndex::build(hashes.len(), at);
        let mut ten = idx.positions(10, at).to_vec();
        ten.sort_unstable();
        assert_eq!(ten, vec![1, 3]);
        assert_eq!(idx.positions(20, at), &[2]);
        assert!(idx.positions(25, at).is_empty(), "absent key");
        assert!(idx.positions(5, at).is_empty(), "below every key");
        assert!(idx.positions(50, at).is_empty(), "above every key");
        assert_eq!(idx.resident_bytes(), hashes.len() * 4);
    }

    #[test]
    fn empty_index_answers_empty() {
        let idx = KeyHashIndex::build(0, |_| 0);
        assert!(idx.positions(0, |_| 0).is_empty());
    }
}
