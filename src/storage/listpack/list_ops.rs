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

use super::{LP_TERMINATOR, Listpack, ListpackRef, decode_backlen, decode_entry_ref_at};

/// Byte offset of the first entry: `total_bytes: u32` + `num_elements: u16`.
const LP_FIRST_ENTRY: usize = 6;

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
}
