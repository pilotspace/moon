//! `LREM`'s one-pass compaction of the full (`VecDeque`) encoding (moon#1173).
//!
//! Out of `list_write.rs` because that file sits at the 1500-line ceiling.

use bytes::Bytes;

/// Remove up to `max_remove` elements equal to `element` from `list` in ONE
/// pass, scanning from the head -- or from the tail when `from_tail` -- and
/// return how many went (moon#1173).
///
/// Read cursor `r`, write cursor `w`: a kept element is swapped down into the
/// write slot, so the kept elements stay in order on one side of the cursors
/// and the removed ones collect between them. The scan stops the moment the
/// `max_remove`-th match is gone -- the unscanned remainder is already in
/// place -- and one `drain` of the gap closes it, moving whichever side of it
/// is shorter. O(scanned + min(prefix, suffix)), against `VecDeque::remove`
/// per match, which is O(len) EACH.
pub(super) fn lrem_deque(
    list: &mut std::collections::VecDeque<Bytes>,
    element: &[u8],
    from_tail: bool,
    max_remove: usize,
) -> usize {
    let len = list.len();
    let mut removed = 0usize;
    if max_remove == 0 {
        return 0;
    }
    if !from_tail {
        // [0..w) kept, [w..r) removed, [r..len) unscanned.
        let (mut r, mut w) = (0usize, 0usize);
        while r < len {
            if list[r] == element {
                removed += 1;
                r += 1;
                if removed == max_remove {
                    break;
                }
            } else {
                if w != r {
                    list.swap(w, r);
                }
                w += 1;
                r += 1;
            }
        }
        if w != r {
            list.drain(w..r);
        }
    } else {
        // [0..r) unscanned, [r..w) removed, [w..len) kept.
        let (mut r, mut w) = (len, len);
        while r > 0 {
            r -= 1;
            if list[r] == element {
                removed += 1;
                if removed == max_remove {
                    break;
                }
            } else {
                w -= 1;
                if w != r {
                    list.swap(w, r);
                }
            }
        }
        if w != r {
            list.drain(r..w);
        }
    }
    removed
}
