//! Chunked node arena for [`super::BPTree`] (moon#1189).
//!
//! Loaded via `#[path]` from `bptree.rs`, as a child module.
//!
//! # Why chunks
//!
//! The tree's nodes used to live in one doubling `Vec`. At 1M members that
//! is a 75–200 MB allocation whose capacity is up to 2x its use (the
//! capacity is what the ledger and the allocator both pay), and whose every
//! doubling copies the whole arena on the shard thread — a multi-millisecond
//! `ZADD`.
//!
//! Here the FIRST chunk is an ordinary `Vec` that grows by doubling up to
//! `1 << SHIFT` slots, so a small tree costs exactly what it did. Every
//! later chunk is allocated once at exactly `1 << SHIFT` slots and never
//! reallocated: no slot ever moves again, a growth step allocates one chunk
//! (tens of KiB) instead of copying the arena, and the unused tail is at
//! most one chunk.
//!
//! # Accounting
//!
//! [`Arena::bytes`] is O(1): the first chunk's capacity is read, the full
//! chunks are summed incrementally as they are added (their capacity is
//! fixed at construction — `Clone` preserves it), plus the chunk directory.

use crate::storage::mem_size::vec_bytes;

#[derive(Debug)]
pub(super) struct Arena<T, const SHIFT: u32> {
    chunks: Vec<Vec<T>>,
    len: usize,
    /// `vec_bytes` of every chunk after the first, maintained on push.
    full_chunk_bytes: usize,
}

impl<T, const SHIFT: u32> Arena<T, SHIFT> {
    const CHUNK: usize = 1 << SHIFT;
    const MASK: usize = Self::CHUNK - 1;

    pub(super) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
            full_chunk_bytes: 0,
        }
    }

    /// Slots in use (live + free-listed).
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Slots allocated (the first chunk's capacity plus every full chunk).
    pub(super) fn capacity(&self) -> usize {
        match self.chunks.first() {
            None => 0,
            Some(first) => first.capacity() + (self.chunks.len() - 1) * Self::CHUNK,
        }
    }

    /// Exact bytes the allocator holds for this arena. O(1).
    pub(super) fn bytes(&self) -> usize {
        let first = self
            .chunks
            .first()
            .map_or(0, |c| vec_bytes(c.capacity(), std::mem::size_of::<T>()));
        first
            + self.full_chunk_bytes
            + vec_bytes(self.chunks.capacity(), std::mem::size_of::<Vec<T>>())
    }

    /// Append a slot and return its index.
    pub(super) fn push(&mut self, value: T) -> usize {
        let idx = self.len;
        let chunk = idx >> SHIFT;
        if chunk == self.chunks.len() {
            if chunk == 0 {
                self.chunks.push(Vec::new());
            } else {
                // Exactly one chunk, allocated once and never grown.
                self.chunks.push(Vec::with_capacity(Self::CHUNK));
                self.full_chunk_bytes += vec_bytes(Self::CHUNK, std::mem::size_of::<T>());
            }
        }
        self.chunks[chunk].push(value);
        self.len += 1;
        idx
    }

    #[inline]
    pub(super) fn get(&self, idx: usize) -> &T {
        &self.chunks[idx >> SHIFT][idx & Self::MASK]
    }

    #[inline]
    pub(super) fn get_mut(&mut self, idx: usize) -> &mut T {
        &mut self.chunks[idx >> SHIFT][idx & Self::MASK]
    }

    /// Consume the arena into its chunks (moon#1190's lazy free): O(1), the
    /// chunk vector moves and nothing is walked.
    pub(super) fn into_chunks(self) -> Vec<Vec<T>> {
        self.chunks
    }

    /// Two distinct slots, both mutably (possibly in different chunks).
    pub(super) fn get2_mut(&mut self, a: usize, b: usize) -> (&mut T, &mut T) {
        assert_ne!(a, b, "get2_mut needs two distinct slots");
        let (ca, oa) = (a >> SHIFT, a & Self::MASK);
        let (cb, ob) = (b >> SHIFT, b & Self::MASK);
        if ca == cb {
            let chunk = &mut self.chunks[ca];
            if oa < ob {
                let (lo, hi) = chunk.split_at_mut(ob);
                (&mut lo[oa], &mut hi[0])
            } else {
                let (lo, hi) = chunk.split_at_mut(oa);
                (&mut hi[0], &mut lo[ob])
            }
        } else if ca < cb {
            let (lo, hi) = self.chunks.split_at_mut(cb);
            (&mut lo[ca][oa], &mut hi[0][ob])
        } else {
            let (lo, hi) = self.chunks.split_at_mut(ca);
            (&mut hi[0][oa], &mut lo[cb][ob])
        }
    }

    /// Drop every slot. The first chunk keeps its capacity (as `Vec::clear`
    /// did for the old single arena); the full chunks are released.
    pub(super) fn clear(&mut self) {
        self.chunks.truncate(1);
        if let Some(first) = self.chunks.first_mut() {
            first.clear();
        }
        self.len = 0;
        self.full_chunk_bytes = 0;
    }
}

impl<T: Clone, const SHIFT: u32> Clone for Arena<T, SHIFT> {
    /// Chunk-shape preserving: every full chunk of the clone is allocated at
    /// exactly `1 << SHIFT` slots like the original, so `bytes()` stays exact
    /// and later pushes into the last chunk never reallocate it.
    fn clone(&self) -> Self {
        let chunks = self
            .chunks
            .iter()
            .enumerate()
            .map(|(k, c)| {
                let mut v = Vec::with_capacity(if k == 0 { c.len() } else { Self::CHUNK });
                v.extend(c.iter().cloned());
                v
            })
            .collect::<Vec<_>>();
        Self {
            chunks,
            len: self.len,
            full_chunk_bytes: self.full_chunk_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_never_move_and_accounting_is_exact() {
        let mut a: Arena<u64, 3> = Arena::new();
        assert_eq!(a.bytes(), 0);
        for i in 0..100u64 {
            assert_eq!(a.push(i * 10), i as usize);
        }
        // A full chunk's buffer address is stable across later pushes.
        let p = a.get(9) as *const u64;
        for i in 100..200u64 {
            a.push(i * 10);
        }
        assert_eq!(p, a.get(9) as *const u64, "a slot in a full chunk moved");
        for i in 0..200usize {
            assert_eq!(*a.get(i), i as u64 * 10);
        }
        let (x, y) = a.get2_mut(3, 150);
        std::mem::swap(x, y);
        assert_eq!((*a.get(3), *a.get(150)), (1500, 30));
        let (x, y) = a.get2_mut(12, 13);
        std::mem::swap(x, y);
        assert_eq!((*a.get(12), *a.get(13)), (130, 120));
        fn exact(a: &Arena<u64, 3>) -> usize {
            a.chunks
                .iter()
                .map(|c| vec_bytes(c.capacity(), 8))
                .sum::<usize>()
                + vec_bytes(a.chunks.capacity(), std::mem::size_of::<Vec<u64>>())
        }
        assert_eq!(a.bytes(), exact(&a));
        assert_eq!(
            a.capacity(),
            a.chunks.iter().map(|c| c.capacity()).sum::<usize>()
        );
        // A clone keeps the chunk shape (full chunks at exactly 1 << SHIFT)
        // and its own accounting exact.
        let mut b = a.clone();
        assert_eq!(b.bytes(), exact(&b));
        assert_eq!(b.capacity(), a.capacity());
        assert!(b.chunks[1..].iter().all(|c| c.capacity() == 8));
        b.push(1);
        assert_eq!(b.bytes(), exact(&b));
        a.clear();
        assert_eq!(a.len(), 0);
        assert_eq!(a.push(7), 0);
    }
}
