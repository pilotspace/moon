//! Iterator types for DashTable.
//!
//! Iterators scan through unique segments (deduplicating directory entries that
//! may point to the same segment due to extendible hashing), then within each
//! segment scan control bytes for FULL slots.

use super::segment::{Segment, TOTAL_SLOTS};

/// Immutable iterator over `(&K, &V)` pairs in a DashTable.
pub struct Iter<'a, K, V> {
    /// Deduplicated list of segment references.
    segments: Vec<&'a Segment<K, V>>,
    seg_idx: usize,
    slot_idx: usize,
    remaining: usize,
}

impl<'a, K, V> Iter<'a, K, V> {
    pub(crate) fn new(segments: Vec<&'a Segment<K, V>>, total_len: usize) -> Self {
        Iter {
            segments,
            seg_idx: 0,
            slot_idx: 0,
            remaining: total_len,
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        while self.seg_idx < self.segments.len() {
            let seg = self.segments[self.seg_idx];
            while self.slot_idx < TOTAL_SLOTS {
                let slot = self.slot_idx;
                self.slot_idx += 1;

                if Segment::<K, V>::is_full_ctrl_pub(seg.ctrl_byte(slot)) {
                    self.remaining -= 1;
                    // SAFETY: ctrl byte is FULL, so key and value are initialized.
                    unsafe {
                        return Some((seg.key_ref(slot), seg.value_ref(slot)));
                    }
                }
            }
            self.seg_idx += 1;
            self.slot_idx = 0;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<K, V> ExactSizeIterator for Iter<'_, K, V> {}

/// Mutable iterator over `(&K, &mut V)` pairs in a DashTable.
pub struct IterMut<'a, K, V> {
    segments: Vec<&'a mut Segment<K, V>>,
    seg_idx: usize,
    slot_idx: usize,
    remaining: usize,
}

impl<'a, K, V> IterMut<'a, K, V> {
    pub(crate) fn new(segments: Vec<&'a mut Segment<K, V>>, total_len: usize) -> Self {
        IterMut {
            segments,
            seg_idx: 0,
            slot_idx: 0,
            remaining: total_len,
        }
    }
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        while self.seg_idx < self.segments.len() {
            while self.slot_idx < TOTAL_SLOTS {
                let slot = self.slot_idx;
                self.slot_idx += 1;

                let seg = &mut self.segments[self.seg_idx];
                if Segment::<K, V>::is_full_ctrl_pub(seg.ctrl_byte(slot)) {
                    self.remaining -= 1;
                    // SAFETY: ctrl byte is FULL, so key and value are initialized.
                    // We extend the lifetime from the segment borrow to 'a because
                    // each slot is yielded exactly once and the iterator holds
                    // exclusive access to the segment.
                    unsafe {
                        let k = &*(seg.key_ref(slot) as *const K);
                        let v = &mut *(seg.value_mut(slot) as *mut V);
                        return Some((k, v));
                    }
                }
            }
            self.seg_idx += 1;
            self.slot_idx = 0;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<K, V> ExactSizeIterator for IterMut<'_, K, V> {}

/// Iterator over keys (`&K`) in a DashTable.
pub struct Keys<'a, K, V>(pub(crate) Iter<'a, K, V>);

impl<'a, K, V> Iterator for Keys<'a, K, V> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, _)| k)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> ExactSizeIterator for Keys<'_, K, V> {}

/// Iterator over values (`&V`) in a DashTable.
pub struct Values<'a, K, V>(pub(crate) Iter<'a, K, V>);

impl<'a, K, V> Iterator for Values<'a, K, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, v)| v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> ExactSizeIterator for Values<'_, K, V> {}

#[cfg(test)]
mod tests {
    // Iterator tests are in mod.rs alongside DashTable (they need the full API).
}
