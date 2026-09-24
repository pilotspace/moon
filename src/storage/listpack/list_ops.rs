//! List-shaped walks and bulk mutations on a [`Listpack`] (moon#1173, moon#1174).
//!
//! These live in a child module only because `listpack.rs` is already past
//! CLAUDE.md's 1500-line ceiling. As a descendant this module sees the parent's
//! private buffer and helpers, so nothing here re-derives an entry width: every
//! walk goes through `decode_entry_ref_at` (forward) or `decode_backlen`
//! (backward), the same two functions every other scan in the parent uses.
//!
//! The shape of every operation below is Redis's: work on the flat byte buffer
//! in place, move each kept byte at most once, and never decode an entry into an
//! owned `Vec` just to look at it or step over it.

use super::{
    LP_TERMINATOR, Listpack, ListpackRef, decode_backlen, decode_entry_ref_at, encode_entry,
    seek_to,
};

/// Byte offset of the first entry: `total_bytes: u32` + `num_elements: u16`.
const LP_FIRST_ENTRY: usize = 6;

// Test-only count of walks that started at the TAIL -- the backward half of
// [`Listpack::offset_of`]. Paired with the parent's `HEAD_SEEKS`, it pins "one
// seek per operation" whichever end the seek started from. Thread-local for the
// same reason `HEAD_SEEKS` is: unit tests run in parallel in one process.
#[cfg(test)]
thread_local! {
    static TAIL_SEEKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Walks from either end so far on this thread: `HEAD_SEEKS + TAIL_SEEKS`.
#[cfg(test)]
pub(crate) fn seeks_from_either_end() -> usize {
    super::head_seeks() + TAIL_SEEKS.with(std::cell::Cell::get)
}

/// Reverse iterator yielding BORROWED entries, tail first — the borrowed twin of
/// [`super::ListpackRevIter`].
///
/// Steps backwards over each entry through its backlen, so reaching the `k`-th
/// entry from the tail decodes `k` backlens and nothing else.
pub struct ListpackRevRefIter<'a> {
    data: &'a [u8],
    /// One past the last byte of the next entry to yield (its backlen's end):
    /// the terminator position before the first `next`.
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ListpackRevRefIter<'a> {
    type Item = ListpackRef<'a>;

    fn next(&mut self) -> Option<ListpackRef<'a>> {
        if self.remaining == 0 || self.pos <= LP_FIRST_ENTRY {
            return None;
        }
        let (entry_len, backlen_size) = decode_backlen(self.data, self.pos);
        let start = self.pos - backlen_size - entry_len;
        let (entry, _) = decode_entry_ref_at(self.data, start);
        self.pos = start;
        self.remaining -= 1;
        Some(entry)
    }
}

impl Listpack {
    /// Reverse iterator yielding BORROWED entries -- no allocation per entry.
    ///
    /// `LPOS` with a negative `RANK` scans from the tail; before moon#1173 it
    /// got there by cloning the whole list into a `Vec` first.
    pub fn iter_rev_refs(&self) -> ListpackRevRefIter<'_> {
        ListpackRevRefIter {
            data: &self.data,
            pos: self.data.len() - 1,
            remaining: self.len(),
        }
    }

    /// Remove up to `max` entries equal to `value` in ONE pass over the buffer,
    /// scanning from the head, or from the tail when `from_tail` is set.
    /// Returns how many were removed.
    ///
    /// This is `LREM` on the compact encoding (moon#1173 / moon#1174 §1). Kept
    /// entries are moved at most once each, as raw bytes -- nothing is decoded
    /// into a `Vec` and nothing is re-encoded -- and the scan stops as soon as
    /// `max` matches are gone, the untouched remainder moving in a single
    /// `copy_within`. Equality is [`ListpackRef::eq_bytes`], the canonical-
    /// integer rule every other lookup uses, so a stored `7` answers to `b"7"`
    /// and never to `b"07"` (moon#795).
    pub fn remove_matches(&mut self, value: &[u8], from_tail: bool, max: usize) -> usize {
        if max == 0 || self.is_empty() {
            return 0;
        }
        let removed = if from_tail {
            self.remove_matches_from_tail(value, max)
        } else {
            self.remove_matches_from_head(value, max)
        };
        if removed > 0 {
            // `removed <= len()`, and `len()` is the header's own u16.
            let removed = u16::try_from(removed).unwrap_or(u16::MAX);
            self.update_header_sub(removed);
        }
        removed
    }

    /// Head-first compaction: `[FIRST..w)` holds the kept entries in order,
    /// `[w..r)` is the gap left by the removed ones.
    fn remove_matches_from_head(&mut self, value: &[u8], max: usize) -> usize {
        let mut remaining = self.len();
        let mut r = LP_FIRST_ENTRY;
        let mut w = LP_FIRST_ENTRY;
        let mut removed = 0usize;
        while remaining > 0 && r < self.data.len() - 1 && self.data[r] != LP_TERMINATOR {
            let (hit, next) = {
                let (entry, next) = decode_entry_ref_at(&self.data, r);
                (entry.eq_bytes(value), next)
            };
            remaining -= 1;
            if hit {
                removed += 1;
                r = next;
                if removed == max {
                    break;
                }
            } else {
                if w != r {
                    self.data.copy_within(r..next, w);
                }
                w += next - r;
                r = next;
            }
        }
        if w != r {
            // Everything from `r` on -- the unscanned entries and the
            // terminator -- closes the gap in one move.
            let end = self.data.len();
            self.data.copy_within(r..end, w);
            self.data.truncate(end - (r - w));
        }
        removed
    }

    /// Tail-first compaction, the mirror image: kept entries are packed
    /// against the terminator, `[w..terminator)`, the gap is `[r..w)`, and the
    /// untouched prefix `[FIRST..r)` stays where it is. Closing the gap is one
    /// `drain`, i.e. one move of the kept suffix.
    fn remove_matches_from_tail(&mut self, value: &[u8], max: usize) -> usize {
        let mut remaining = self.len();
        let mut r = self.data.len() - 1; // the terminator: one past the last entry
        let mut w = r;
        let mut removed = 0usize;
        while remaining > 0 && r > LP_FIRST_ENTRY {
            let (entry_len, backlen_size) = decode_backlen(&self.data, r);
            let start = r - backlen_size - entry_len;
            let hit = {
                let (entry, _) = decode_entry_ref_at(&self.data, start);
                entry.eq_bytes(value)
            };
            remaining -= 1;
            if hit {
                removed += 1;
                r = start;
                if removed == max {
                    break;
                }
            } else {
                let width = r - start;
                if w != r {
                    self.data.copy_within(start..r, w - width);
                }
                w -= width;
                r = start;
            }
        }
        if w != r {
            self.data.drain(r..w);
        }
        removed
    }

    /// Byte offset of entry `index` -- `index == len()` names the terminator --
    /// walking from whichever END of the buffer is nearer.
    ///
    /// From the head this is [`seek_to`], counted by `HEAD_SEEKS`; from the tail
    /// it steps back over `len() - index` entries by their backlens alone, never
    /// decoding a payload. `LTRIM k 0 99` on a 101-entry list therefore walks
    /// ONE entry, not a hundred.
    pub(super) fn offset_of(&self, index: usize) -> Option<usize> {
        let len = self.len();
        if index > len {
            return None;
        }
        if index == len {
            return Some(self.data.len() - 1);
        }
        if index <= len - index {
            return seek_to(&self.data, index).map(|(pos, _)| pos);
        }
        #[cfg(test)]
        TAIL_SEEKS.with(|c| c.set(c.get() + 1));
        let mut pos = self.data.len() - 1;
        for _ in 0..len - index {
            let (entry_len, backlen_size) = decode_backlen(&self.data, pos);
            pos -= backlen_size + entry_len;
        }
        Some(pos)
    }

    /// Stamp both header fields: `total_bytes` from the buffer, the element
    /// count from the caller.
    fn stamp_header(&mut self, count: usize) {
        let total = u32::try_from(self.data.len()).unwrap_or(u32::MAX);
        self.data[0..4].copy_from_slice(&total.to_le_bytes());
        let count = u16::try_from(count).unwrap_or(u16::MAX);
        self.data[4..6].copy_from_slice(&count.to_le_bytes());
    }

    /// Keep only the entries `start..=end`, dropping everything outside that
    /// range in ONE move of the kept bytes (`LTRIM`, moon#1174 §1).
    ///
    /// Both edges are found by [`Listpack::offset_of`], each from its nearer
    /// end, so the common `LTRIM k 0 N` on a list one entry past `N` steps over
    /// a single backlen. An empty or out-of-range window (`start > end`, or
    /// `start >= len()`) empties the listpack; `end` past the tail is clamped.
    pub fn retain_range(&mut self, start: usize, end: usize) {
        let len = self.len();
        if start > end || start >= len {
            self.data.truncate(LP_FIRST_ENTRY);
            self.data.push(LP_TERMINATOR);
            self.stamp_header(0);
            return;
        }
        let end = end.min(len - 1);
        let (Some(from), Some(to)) = (self.offset_of(start), self.offset_of(end + 1)) else {
            return;
        };
        let kept = to - from;
        if from != LP_FIRST_ENTRY {
            self.data.copy_within(from..to, LP_FIRST_ENTRY);
        }
        let terminator = LP_FIRST_ENTRY + kept;
        self.data[terminator] = LP_TERMINATOR;
        self.data.truncate(terminator + 1);
        self.stamp_header(end - start + 1);
    }

    /// Insert `value` immediately before -- or, with `before == false`, after
    /// -- the FIRST entry equal to `pivot`, found in one borrowed walk
    /// (`LINSERT`, moon#1174 §1). Returns `false`, touching nothing, when no
    /// entry equals `pivot`.
    ///
    /// The pivot match is [`ListpackRef::eq_bytes`] and the new entry goes
    /// through the same canonical-integer encoder `push_back` uses, so bytes go
    /// in and come back out exactly (moon#795).
    pub fn insert_relative(&mut self, pivot: &[u8], value: &[u8], before: bool) -> bool {
        let mut pos = LP_FIRST_ENTRY;
        let mut remaining = self.len();
        let at = loop {
            if remaining == 0 || pos >= self.data.len() - 1 || self.data[pos] == LP_TERMINATOR {
                return false;
            }
            let (entry, next) = decode_entry_ref_at(&self.data, pos);
            if entry.eq_bytes(pivot) {
                break if before { pos } else { next };
            }
            pos = next;
            remaining -= 1;
        };
        let encoded = encode_entry(value);
        self.write_entry(at..at, &encoded);
        self.update_header();
        true
    }

    /// Give back buffer capacity after a BULK removal, when the buffer has
    /// become sparse: capacity more than twice the bytes in use.
    ///
    /// `LTRIM`/`LREM` can drop most of a listpack in one command, and
    /// `estimate_memory` bills the jemalloc size class of the CAPACITY
    /// (moon#788), so without this a list trimmed from 128 entries to 3 keeps
    /// paying for 128 until the key dies. Redis `lp_realloc`s after every
    /// delete. The factor of two is hysteresis, not tuning: the steady-state
    /// capped-list idiom (`LPUSH` one, `LTRIM` one) never gets near it, so it
    /// never pays a realloc per command, and a buffer that genuinely emptied
    /// out still returns its memory.
    pub fn shrink_if_sparse(&mut self) {
        if self.data.capacity() > 2 * self.data.len() {
            self.data.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Naive oracle: the LREM Redis documents, on a plain `Vec`.
    fn lrem_oracle(list: &[Vec<u8>], value: &[u8], count: i64) -> (Vec<Vec<u8>>, usize) {
        let max = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let mut hit_positions: Vec<usize> = list
            .iter()
            .enumerate()
            .filter(|(_, v)| v.as_slice() == value)
            .map(|(i, _)| i)
            .collect();
        if count < 0 {
            hit_positions.reverse();
        }
        hit_positions.truncate(max);
        let kept: Vec<Vec<u8>> = list
            .iter()
            .enumerate()
            .filter(|(i, _)| !hit_positions.contains(i))
            .map(|(_, v)| v.clone())
            .collect();
        (kept, hit_positions.len())
    }

    fn build(values: &[Vec<u8>]) -> Listpack {
        let mut lp = Listpack::new();
        for v in values {
            lp.push_back(v);
        }
        lp
    }

    fn contents(lp: &Listpack) -> Vec<Vec<u8>> {
        lp.iter_refs().map(|e| e.to_vec()).collect()
    }

    /// Tiny deterministic xorshift, so the sweep needs no RNG crate and
    /// replays identically on every run.
    struct XorShift(u64);
    impl XorShift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// The alphabet mixes every encoding family with the canonical-integer
    /// edge: `7` is integer-encoded, `07`/`+7`/`-0` are strings that must not
    /// answer to it, and a 70-byte value takes the 12-bit string head.
    fn alphabet() -> Vec<Vec<u8>> {
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"7".to_vec(),
            b"07".to_vec(),
            b"+7".to_vec(),
            b"-0".to_vec(),
            b"-4096".to_vec(),
            b"9223372036854775807".to_vec(),
            b"".to_vec(),
            vec![b'z'; 70],
        ]
    }

    /// moon#1173: `remove_matches` must agree with the naive LREM on every
    /// count sign and magnitude, on random lists over every encoding width --
    /// both the contents and the byte layout (the result must be exactly the
    /// listpack a fresh build of the kept values would be).
    #[test]
    fn remove_matches_agrees_with_the_naive_oracle() {
        let alpha = alphabet();
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        for round in 0..600 {
            let n = rng.below(40) as usize;
            let list: Vec<Vec<u8>> = (0..n)
                .map(|_| alpha[rng.below(alpha.len() as u64) as usize].clone())
                .collect();
            let probe = alpha[rng.below(alpha.len() as u64) as usize].clone();
            let count = rng.below(9) as i64 - 4; // -4..=4, zero included
            let (want, want_removed) = lrem_oracle(&list, &probe, count);

            let mut lp = build(&list);
            let removed = lp.remove_matches(
                &probe,
                count < 0,
                if count == 0 {
                    usize::MAX
                } else {
                    count.unsigned_abs() as usize
                },
            );
            assert_eq!(removed, want_removed, "round {round}: removed count");
            assert_eq!(contents(&lp), want, "round {round}: surviving elements");
            assert_eq!(lp.len(), want.len(), "round {round}: header count");
            let fresh = build(&want);
            assert_eq!(
                lp.data, fresh.data,
                "round {round}: byte layout differs from a fresh build"
            );
        }
    }

    /// The reverse iterator must be the forward one, reversed, on every
    /// encoding width.
    #[test]
    fn iter_rev_refs_mirrors_iter_refs() {
        let lp = build(&alphabet());
        let mut fwd: Vec<Vec<u8>> = lp.iter_refs().map(|e| e.to_vec()).collect();
        fwd.reverse();
        let rev: Vec<Vec<u8>> = lp.iter_rev_refs().map(|e| e.to_vec()).collect();
        assert_eq!(rev, fwd);
        assert_eq!(Listpack::new().iter_rev_refs().count(), 0);
    }

    #[test]
    fn remove_matches_edges() {
        // max == 0 and an empty listpack are no-ops.
        let mut lp = build(&[b"a".to_vec(), b"a".to_vec()]);
        let before = lp.data.clone();
        assert_eq!(lp.remove_matches(b"a", false, 0), 0);
        assert_eq!(lp.data, before);
        let mut empty = Listpack::new();
        assert_eq!(empty.remove_matches(b"a", true, 5), 0);
        assert_eq!(empty.len(), 0);

        // Removing everything leaves a valid empty listpack.
        let mut lp = build(&[b"x".to_vec(), b"x".to_vec(), b"x".to_vec()]);
        assert_eq!(lp.remove_matches(b"x", true, usize::MAX), 3);
        assert_eq!(lp.data, Listpack::new().data);

        // Stopping early from the tail keeps the untouched prefix in place.
        let mut lp = build(&[b"x".to_vec(), b"y".to_vec(), b"x".to_vec(), b"x".to_vec()]);
        assert_eq!(lp.remove_matches(b"x", true, 2), 2);
        assert_eq!(contents(&lp), vec![b"x".to_vec(), b"y".to_vec()]);
    }

    #[test]
    fn shrink_if_sparse_only_shrinks_a_sparse_buffer() {
        let values: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("elem-{i:04}").into_bytes())
            .collect();
        let mut lp = build(&values);
        let cap = lp.data.capacity();
        // Dense: nothing happens.
        lp.shrink_if_sparse();
        assert_eq!(lp.data.capacity(), cap);
        // Remove almost everything: the buffer is handed back.
        for v in &values[3..] {
            lp.remove_matches(v, false, 1);
        }
        assert_eq!(lp.len(), 3);
        lp.shrink_if_sparse();
        assert!(
            lp.data.capacity() <= 2 * lp.data.len(),
            "a sparse buffer must shrink (cap {} len {})",
            lp.data.capacity(),
            lp.data.len()
        );
        assert_eq!(contents(&lp), values[..3].to_vec());
    }

    /// `offset_of` must land on the same byte the head walk lands on, from
    /// either end, for every index including the terminator -- and it must
    /// take the NEARER end.
    #[test]
    fn offset_of_agrees_with_seek_to_from_either_end() {
        let lp = build(&alphabet());
        let len = lp.len();
        for i in 0..len {
            let (want, _) = seek_to(&lp.data, i).expect("in range");
            let mark = seeks_from_either_end();
            assert_eq!(lp.offset_of(i), Some(want), "index {i}");
            assert_eq!(seeks_from_either_end() - mark, 1, "index {i}: one seek");
        }
        assert_eq!(lp.offset_of(len), Some(lp.data.len() - 1), "terminator");
        assert_eq!(lp.offset_of(len + 1), None);
        // The last entry is reached from the tail, not the head.
        let head = super::super::head_seeks();
        lp.offset_of(len - 1);
        assert_eq!(
            super::super::head_seeks(),
            head,
            "tail index walked from the head"
        );
    }

    /// `retain_range` against slicing, on every window of a list that spans
    /// every encoding width, and the byte layout of a fresh build.
    #[test]
    fn retain_range_agrees_with_slicing() {
        let values = alphabet();
        let n = values.len();
        for start in 0..n + 2 {
            for end in 0..n + 2 {
                let mut lp = build(&values);
                lp.retain_range(start, end);
                let want: Vec<Vec<u8>> = if start > end || start >= n {
                    Vec::new()
                } else {
                    values[start..=end.min(n - 1)].to_vec()
                };
                assert_eq!(contents(&lp), want, "window {start}..={end}");
                assert_eq!(lp.data, build(&want).data, "bytes, window {start}..={end}");
            }
        }
    }

    /// The capped-list shape: `LTRIM 0 99` on 101 entries walks ONE backlen.
    #[test]
    fn retain_range_on_a_capped_list_seeks_once_from_the_tail() {
        let values: Vec<Vec<u8>> = (0..101)
            .map(|i| format!("item:{i:06}").into_bytes())
            .collect();
        let mut lp = build(&values);
        let head = super::super::head_seeks();
        let both = seeks_from_either_end();
        lp.retain_range(0, 99);
        assert_eq!(contents(&lp), values[..100].to_vec());
        assert_eq!(seeks_from_either_end() - both, 2, "one seek per edge");
        // The start edge is index 0 (a zero-length head seek); the end edge
        // must come from the tail.
        assert_eq!(super::super::head_seeks() - head, 1);
    }

    /// `insert_relative` against `Vec::insert` at the first pivot, both sides,
    /// including a pivot that is integer-encoded and one that is absent.
    #[test]
    fn insert_relative_agrees_with_vec_insert() {
        let values = alphabet();
        let mut probes = values.clone();
        probes.push(b"absent".to_vec());
        probes.push(b"0007".to_vec());
        for pivot in &probes {
            for before in [true, false] {
                for new in [&b"new"[..], b"12", b"+12", &[b'w'; 90][..]] {
                    let mut lp = build(&values);
                    let found = lp.insert_relative(pivot, new, before);
                    let mut want = values.clone();
                    match values.iter().position(|v| v == pivot) {
                        Some(i) => {
                            assert!(found);
                            want.insert(if before { i } else { i + 1 }, new.to_vec());
                        }
                        None => assert!(!found),
                    }
                    assert_eq!(contents(&lp), want);
                    assert_eq!(lp.data, build(&want).data);
                }
            }
        }
        let mut empty = Listpack::new();
        assert!(!empty.insert_relative(b"x", b"y", true));
        assert_eq!(empty.data, Listpack::new().data);
    }
}
