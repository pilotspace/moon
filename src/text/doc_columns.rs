//! Dense per-document side tables of a [`TextIndex`](crate::text::store::TextIndex) (moon#1194).
//!
//! Text doc ids are dense: `next_doc_id` hands them out sequentially and an upsert reuses its id,
//! so a document's side data is a `Vec` slot indexed by its id rather than a `HashMap` entry —
//! no hash, no bucket padding, no load-factor slack. The only holes are ids removed by
//! `remove_doc_by_doc_id` (`FT.INVALIDATE_RANGE`, the boot deletion probe); a hole costs one empty
//! slot per column (see [`DocKeys::footprint`] and friends for the exact sizes).
//!
//! Each column reports its REAL footprint (`capacity × slot size`), which is what the index bills
//! (`TextIndex::resident_bytes`). The APIs mirror the `HashMap` methods callers used, so call sites
//! outside the text module compile unchanged.

use bytes::Bytes;
use roaring::RoaringBitmap;

/// `doc_id -> key` plus the bitmap of ids that resolve to a key.
///
/// The bitmap is kept in lock-step by `insert` / `remove`: query membership restricts to
/// resolvable documents with one `&=` (moon#1191) and `*` borrows it directly.
#[derive(Default)]
pub struct DocKeys {
    slots: Vec<Option<Bytes>>,
    live: RoaringBitmap,
}

impl DocKeys {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    #[must_use]
    pub fn get(&self, doc_id: &u32) -> Option<&Bytes> {
        self.slots.get(*doc_id as usize)?.as_ref()
    }

    #[inline]
    #[must_use]
    pub fn contains_key(&self, doc_id: &u32) -> bool {
        self.get(doc_id).is_some()
    }

    /// Set `doc_id`'s key, returning the previous one (HashMap semantics).
    pub fn insert(&mut self, doc_id: u32, key: Bytes) -> Option<Bytes> {
        let i = doc_id as usize;
        if i >= self.slots.len() {
            self.slots.resize(i + 1, None);
        }
        self.live.insert(doc_id);
        self.slots[i].replace(key)
    }

    pub fn remove(&mut self, doc_id: &u32) -> Option<Bytes> {
        let old = self.slots.get_mut(*doc_id as usize)?.take();
        if old.is_some() {
            self.live.remove(*doc_id);
        }
        old
    }

    /// Number of documents with a key.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.live.len() as usize
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.live.is_empty()
    }

    /// Doc ids with a key, ascending.
    pub fn keys(&self) -> impl Iterator<Item = u32> + '_ {
        self.live.iter()
    }

    /// Keys in ascending doc-id order.
    pub fn values(&self) -> impl Iterator<Item = &Bytes> + '_ {
        self.slots.iter().flatten()
    }

    /// `(doc_id, key)` pairs, ascending.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &Bytes)> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, k)| k.as_ref().map(|k| (i as u32, k)))
    }

    /// Exactly the ids with a key.
    #[inline]
    #[must_use]
    pub fn live(&self) -> &RoaringBitmap {
        &self.live
    }

    /// Bytes held by the slot array (key bytes are billed per key by the owner).
    #[inline]
    #[must_use]
    pub fn footprint(&self) -> usize {
        self.slots.capacity() * std::mem::size_of::<Option<Bytes>>()
    }
}

impl PartialEq for DocKeys {
    /// Same `(doc_id, key)` set — capacity and trailing holes are irrelevant.
    fn eq(&self, other: &Self) -> bool {
        self.live == other.live && self.iter().eq(other.iter())
    }
}

impl std::fmt::Debug for DocKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// `doc_id -> u64` where `0` means "absent" — the convention of both users (an insert LSN of 0
/// is a pre-MVCC doc, a content checksum is never 0). Reads therefore match the former
/// `HashMap::get(..).copied().unwrap_or(0)` exactly.
#[derive(Default, Clone)]
pub struct DocU64s {
    vals: Vec<u64>,
    len: usize,
}

impl DocU64s {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    #[must_use]
    pub fn get(&self, doc_id: &u32) -> Option<&u64> {
        self.vals.get(*doc_id as usize).filter(|v| **v != 0)
    }

    #[inline]
    #[must_use]
    pub fn contains_key(&self, doc_id: &u32) -> bool {
        self.get(doc_id).is_some()
    }

    /// Set `doc_id`'s value (`0` clears it), returning the previous one.
    pub fn insert(&mut self, doc_id: u32, value: u64) -> Option<u64> {
        if value == 0 {
            return self.remove(&doc_id);
        }
        let i = doc_id as usize;
        if i >= self.vals.len() {
            self.vals.resize(i + 1, 0);
        }
        let old = std::mem::replace(&mut self.vals[i], value);
        if old == 0 {
            self.len += 1;
            None
        } else {
            Some(old)
        }
    }

    pub fn remove(&mut self, doc_id: &u32) -> Option<u64> {
        let slot = self.vals.get_mut(*doc_id as usize)?;
        let old = std::mem::take(slot);
        if old == 0 {
            None
        } else {
            self.len -= 1;
            Some(old)
        }
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `(doc_id, value)` pairs with a value, ascending.
    pub fn iter(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        self.vals
            .iter()
            .enumerate()
            .filter(|(_, v)| **v != 0)
            .map(|(i, v)| (i as u32, *v))
    }

    #[inline]
    #[must_use]
    pub fn footprint(&self) -> usize {
        self.vals.capacity() * std::mem::size_of::<u64>()
    }
}

impl PartialEq for DocU64s {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl std::fmt::Debug for DocU64s {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// Per-document token length of every TEXT field, one flat `u32` row of `stride` fields per doc
/// id (a missing row reads as zeros — exactly what every former `HashMap<u32, Vec<u32>>` reader
/// did with a missing entry).
#[derive(Clone, Default)]
pub struct DocLengths {
    stride: usize,
    vals: Vec<u32>,
}

impl DocLengths {
    #[must_use]
    pub fn new(stride: usize) -> Self {
        Self {
            stride,
            vals: Vec::new(),
        }
    }

    /// Length of `field` in `doc_id` (`0` when unknown).
    #[inline]
    #[must_use]
    pub fn get(&self, doc_id: u32, field: usize) -> u32 {
        if field >= self.stride {
            return 0;
        }
        self.vals
            .get(doc_id as usize * self.stride + field)
            .copied()
            .unwrap_or(0)
    }

    /// The row of `doc_id` (zeros when unknown); `None` only past the stored rows.
    #[must_use]
    pub fn row(&self, doc_id: u32) -> Option<&[u32]> {
        let start = doc_id as usize * self.stride;
        self.vals.get(start..start + self.stride)
    }

    /// Overwrite `doc_id`'s row (`lengths` shorter than the stride is zero-padded).
    pub fn set(&mut self, doc_id: u32, lengths: &[u32]) {
        if self.stride == 0 {
            return;
        }
        let start = doc_id as usize * self.stride;
        if self.vals.len() < start + self.stride {
            self.vals.resize(start + self.stride, 0);
        }
        let row = &mut self.vals[start..start + self.stride];
        row.fill(0);
        let n = lengths.len().min(self.stride);
        row[..n].copy_from_slice(&lengths[..n]);
    }

    /// Zero `doc_id`'s row.
    pub fn clear(&mut self, doc_id: u32) {
        let start = doc_id as usize * self.stride;
        if let Some(row) = self.vals.get_mut(start..start + self.stride) {
            row.fill(0);
        }
    }

    /// Σ over every doc of `field`'s length.
    #[must_use]
    pub fn sum_field(&self, field: usize) -> u64 {
        if field >= self.stride {
            return 0;
        }
        self.vals
            .iter()
            .skip(field)
            .step_by(self.stride)
            .map(|&v| u64::from(v))
            .sum()
    }

    #[inline]
    #[must_use]
    pub fn footprint(&self) -> usize {
        self.vals.capacity() * std::mem::size_of::<u32>()
    }
}

/// `doc_id -> boxed slice` column for small per-document lists (TAG / NUMERIC entries). A doc with
/// no entries holds `None`; an entry list is one exact-size allocation.
pub struct DocSlices<T> {
    slots: Vec<Option<Box<[T]>>>,
    len: usize,
}

impl<T> Default for DocSlices<T> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            len: 0,
        }
    }
}

impl<T> DocSlices<T> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    #[must_use]
    pub fn get(&self, doc_id: u32) -> Option<&[T]> {
        self.slots.get(doc_id as usize)?.as_deref()
    }

    /// Remove and return `doc_id`'s entries.
    pub fn take(&mut self, doc_id: u32) -> Option<Box<[T]>> {
        let old = self.slots.get_mut(doc_id as usize)?.take();
        if old.is_some() {
            self.len -= 1;
        }
        old
    }

    /// Set `doc_id`'s entries (an empty list clears the slot).
    pub fn set(&mut self, doc_id: u32, entries: Vec<T>) {
        if entries.is_empty() {
            self.take(doc_id);
            return;
        }
        let i = doc_id as usize;
        if i >= self.slots.len() {
            self.slots.resize_with(i + 1, || None);
        }
        if self.slots[i].replace(entries.into_boxed_slice()).is_none() {
            self.len += 1;
        }
    }

    /// Number of docs with entries.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `(doc_id, entries)` for docs with entries, ascending.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &[T])> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_deref().map(|s| (i as u32, s)))
    }

    /// Bytes held by the slot array (entry slices are billed per doc by the owner).
    #[inline]
    #[must_use]
    pub fn footprint(&self) -> usize {
        self.slots.capacity() * std::mem::size_of::<Option<Box<[T]>>>()
    }

    /// Heap bytes of one doc's entry slice — its exact allocation request.
    #[inline]
    #[must_use]
    pub fn entries_bytes(entries: &[T]) -> usize {
        std::mem::size_of_val(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_keys_behave_like_a_map_and_track_live_ids() {
        let mut k = DocKeys::new();
        assert!(k.is_empty());
        assert_eq!(k.insert(3, Bytes::from_static(b"c")), None);
        assert_eq!(k.insert(0, Bytes::from_static(b"a")), None);
        assert_eq!(
            k.insert(3, Bytes::from_static(b"cc")),
            Some(Bytes::from_static(b"c"))
        );
        assert_eq!(k.len(), 2);
        assert_eq!(k.get(&3).map(|b| b.as_ref()), Some(&b"cc"[..]));
        assert_eq!(k.get(&1), None);
        assert_eq!(k.get(&99), None);
        assert_eq!(k.keys().collect::<Vec<_>>(), vec![0, 3]);
        assert_eq!(k.live().iter().collect::<Vec<_>>(), vec![0, 3]);
        assert_eq!(k.remove(&0), Some(Bytes::from_static(b"a")));
        assert_eq!(k.remove(&0), None);
        assert_eq!(k.remove(&77), None);
        assert_eq!(k.len(), 1);
        assert_eq!(k.values().count(), 1);
        let mut other = DocKeys::new();
        other.insert(3, Bytes::from_static(b"cc"));
        assert_eq!(k, other, "equality ignores holes and capacity");
    }

    #[test]
    fn doc_u64s_zero_is_absent() {
        let mut c = DocU64s::new();
        assert_eq!(c.insert(5, 50), None);
        assert_eq!(c.insert(5, 51), Some(50));
        assert_eq!(c.insert(2, 0), None, "inserting 0 clears");
        assert_eq!(c.get(&2), None);
        assert_eq!(c.get(&5).copied(), Some(51));
        assert_eq!(c.len(), 1);
        assert_eq!(c.remove(&5), Some(51));
        assert!(c.is_empty());
        assert_eq!(c.get(&1000), None);
    }

    #[test]
    fn doc_lengths_rows_default_to_zero() {
        let mut l = DocLengths::new(2);
        l.set(4, &[7, 9]);
        l.set(1, &[3]);
        assert_eq!(l.get(4, 1), 9);
        assert_eq!(l.get(1, 0), 3);
        assert_eq!(l.get(1, 1), 0);
        assert_eq!(l.get(0, 0), 0);
        assert_eq!(l.get(99, 0), 0);
        assert_eq!(l.get(4, 2), 0, "out-of-stride field reads 0");
        assert_eq!(l.sum_field(0), 10);
        assert_eq!(l.sum_field(1), 9);
        l.clear(4);
        assert_eq!(l.sum_field(0), 3);
        assert_eq!(l.row(1), Some(&[3u32, 0][..]));
    }

    #[test]
    fn doc_slices_set_take_and_count() {
        let mut s: DocSlices<(u16, u32)> = DocSlices::new();
        s.set(2, vec![(0, 1), (1, 2)]);
        s.set(0, vec![]);
        assert_eq!(s.len(), 1);
        assert_eq!(s.get(2), Some(&[(0u16, 1u32), (1, 2)][..]));
        assert_eq!(s.get(0), None);
        s.set(2, vec![(0, 9)]);
        assert_eq!(s.len(), 1);
        assert_eq!(s.take(2).as_deref(), Some(&[(0u16, 9u32)][..]));
        assert!(s.is_empty());
        assert_eq!(s.take(2), None);
    }
}
