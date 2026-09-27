//! SSCAN over the full (`IndexSet`) set encoding: the cursor and its two page
//! walks (moon#1287, and its wave-1 review finding F1).
//!
//! # Position mode — the common case, O(COUNT) per page
//!
//! The cursor counts the positions still to visit: a page walks DOWN from
//! position `pos - 1` (`len - 1` for the initial cursor 0) and answers the
//! lowest position it did not reach. That keeps the SCAN guarantee — every
//! member present for the whole scan is returned at least once — only while
//! nothing moves a member UP. SREM / SPOP / SMOVE `swap_remove` (the LAST
//! member moves down into the hole) and SADD appends, so a member not yet
//! visited sits below the cursor and can only move further down; one already
//! visited may move below it and be returned again (allowed), and one added
//! mid-scan lands above it (may be missed — allowed).
//!
//! A write that REPLACES the whole value breaks that: `SUNIONSTORE s s …`,
//! `SINTERSTORE s …`, `SDIFFSTORE`, `RENAME`/`COPY … REPLACE`/`RESTORE` onto
//! the key, a cold-tier round trip. The new `IndexSet` holds the members in a
//! different order, and a position cursor minted against the old layout
//! skipped 100 of 2000 members that were present for the whole scan (review
//! F1; redis returns them, its cursor is a hash-bucket index).
//!
//! # Detecting a replacement — zero bytes per set
//!
//! Every `IndexSet` owns its own `RandomState`, and std seeds each new one
//! with distinct keys. A set's hasher therefore IS its identity: every way a
//! value is rebuilt (`IndexSet::new`, `with_capacity`, `collect`, a decode)
//! creates a new one, while the operations the position walk tolerates
//! (`insert`, `swap_remove`) keep it. The cursor carries a 30-bit tag of
//! `hasher().hash_one(TAG_PROBE)`; a page whose set answers a different tag
//! knows its positions mean nothing. No field is added to the set (the
//! review's budget was ~8 bytes; this spends none), and no command that
//! replaces a set has to remember to bump a counter — including the ones
//! nobody lists.
//!
//! A `clone()` shares the hasher AND the order, so a cloned set is correctly
//! treated as the same layout. The one blind spot: `COPY s t`, then writes to
//! BOTH copies, then `t` moved back onto `s` mid-scan — two layouts that
//! diverged from one ancestor under one tag. Documented in the WS30 summary.
//!
//! # Hash mode — the fallback, correct under every mutation
//!
//! On a tag mismatch (or for a value decoded fresh from the cold tier on
//! every call, or a cursor this module never minted) the scan continues by
//! member HASH: a fixed-seed `xxh64` of the member, 61 bits. The cursor is a
//! threshold `T` — "members hashing below `T` are still to visit" — and a
//! page returns the members with the largest hashes below `T`. A member's
//! hash depends on nothing but its bytes, so no rebuild, insertion or
//! removal can carry an unvisited member past the cursor, and a hash-mode
//! scan terminates like redis's even when the set keeps being rebuilt.
//! Entering hash mode restarts from the top (`T = 2^61`), so members
//! returned before the switch may come back again — allowed.
//!
//! A hash-mode page costs O(N) (every member is hashed), so its size is
//! `max(COUNT, ceil(N / HASH_MODE_PAGES))`: the rest of the scan is at most
//! `HASH_MODE_PAGES` pages, O(N · HASH_MODE_PAGES) in all, paid only after a
//! whole-value replacement that itself cost O(N). COUNT is a hint in redis
//! too. Members sharing a hash at the page boundary are never split across
//! pages (see [`hash_page`]).
//!
//! # Cursor layout (every cursor stays below 2^63, for clients that parse a
//! signed 64-bit integer)
//!
//! ```text
//!   0                          start / done
//!   tag(30) << 32 | pos(32)    position mode, pos in 1..2^32
//!   1 << 62 | T                hash mode, T in 1..=2^61
//! ```
//!
//! Anything else — bit 63 set (`-1`), a hash cursor out of range, a position
//! cursor whose tag does not match — restarts in hash mode from the top.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::BuildHasher;

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::entry::SetValue;

use super::glob_match;

/// Position bits of a position-mode cursor.
const POS_BITS: u32 = 32;
const POS_MASK: u64 = (1 << POS_BITS) - 1;
/// Tag bits of a position-mode cursor: bits 32..62.
const TAG_BITS: u32 = 30;
/// Hash-mode flag.
const HASH_MODE: u64 = 1 << 62;
/// Exclusive upper bound of a member hash (61 bits); the initial threshold.
const HASH_TOP: u64 = 1 << 61;
/// Bit 63: never minted here (`-1` parses to `u64::MAX`).
const FOREIGN: u64 = 1 << 63;
/// A hash-mode scan takes at most this many pages.
pub(super) const HASH_MODE_PAGES: usize = 256;
/// Hashed by the set's own `RandomState` to fingerprint the instance.
const TAG_PROBE: u64 = 0x5353_4341_4E5F_7461;
/// Fixed seed of the member hash (any value: it only has to be the same for
/// every page of a scan, and it is the same for the process's lifetime).
const MEMBER_SEED: u64 = 0x6D6F_6F6E_2331_3238;

/// The 30-bit identity tag of this `IndexSet` instance (see the module doc).
fn layout_tag(set: &SetValue) -> u64 {
    set.hasher().hash_one(TAG_PROBE) >> (64 - TAG_BITS)
}

/// A member's 61-bit scan hash.
fn member_hash(member: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(member, MEMBER_SEED) >> 3
}

/// One SSCAN page over the full encoding.
///
/// `stable_layout` is false for a value decoded fresh from the cold tier on
/// every call: its hasher is new each time, so it scans in hash mode from
/// the start rather than minting a position cursor the next call would
/// reject.
pub(super) fn sscan_page(
    set: &SetValue,
    cursor: u64,
    count: usize,
    pattern: Option<&[u8]>,
    stable_layout: bool,
) -> Frame {
    let len = set.len();
    let positional = stable_layout && (len as u64) <= POS_MASK;
    if positional && cursor & (FOREIGN | HASH_MODE) == 0 {
        let tag = layout_tag(set);
        let pos = cursor & POS_MASK;
        if cursor == 0 {
            return position_page(set, len, count, pattern, tag);
        }
        if pos != 0 && cursor >> POS_BITS == tag {
            // A cursor past the end (the set shrank) clamps: every position
            // below it is still unvisited.
            let end = usize::try_from(pos).map_or(len, |p| p.min(len));
            return position_page(set, end, count, pattern, tag);
        }
    }
    let below = if cursor & FOREIGN == 0 && cursor & HASH_MODE != 0 {
        match cursor & !HASH_MODE {
            t @ 1..=HASH_TOP => t,
            _ => HASH_TOP,
        }
    } else {
        HASH_TOP
    };
    hash_page(set, below, count, pattern)
}

/// Position mode: walk down from position `end - 1`.
fn position_page(
    set: &SetValue,
    end: usize,
    count: usize,
    pattern: Option<&[u8]>,
    tag: u64,
) -> Frame {
    let start = end.saturating_sub(count.max(1));
    let mut results = Vec::with_capacity(end - start);
    if let Some(page) = set.as_slice().get_range(start..end) {
        for member in page.iter().rev() {
            if pattern.is_none_or(|p| glob_match(p, member)) {
                results.push(Frame::BulkString(member.clone()));
            }
        }
    }
    let next = if start == 0 {
        0
    } else {
        (tag << POS_BITS) | start as u64
    };
    scan_reply(next, results)
}

/// Hash mode: the members whose hash is the largest below `below`.
///
/// A bounded min-heap keeps the `page` largest hashes seen; `left_out` is
/// the largest hash below `below` that is NOT in it. The heap's minimum `b`
/// is non-decreasing, so `left_out <= b` at the end:
/// - nothing left out: the page holds every remaining member, cursor 0;
/// - `left_out < b`: the next page continues below `b`;
/// - `left_out == b` (members tied on `b` straddle the page): return only
///   the members above `b` and continue below `b + 1`, so the tied ones are
///   all in the next page; if EVERY member of the page hashes to `b` (more
///   than `page` members on one 61-bit hash), return all of them with a
///   second pass and continue below `b`.
///
/// The heap is scratch of `page` entries — this is the fallback after a
/// whole-value replacement, not the steady-state path.
fn hash_page(set: &SetValue, below: u64, count: usize, pattern: Option<&[u8]>) -> Frame {
    hash_page_by(set, below, count, pattern, member_hash)
}

/// [`hash_page`] over any member hash below `HASH_TOP` — the tests force
/// ties with a coarse one.
fn hash_page_by(
    set: &SetValue,
    below: u64,
    count: usize,
    pattern: Option<&[u8]>,
    member_hash: impl Fn(&[u8]) -> u64,
) -> Frame {
    let page = count.max(1).max(set.len().div_ceil(HASH_MODE_PAGES));
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> =
        BinaryHeap::with_capacity(page.min(set.len()));
    let mut left_out: Option<u64> = None;
    for (idx, member) in set.iter().enumerate() {
        let h = member_hash(member);
        if h >= below {
            continue;
        }
        if heap.len() < page {
            heap.push(Reverse((h, idx)));
            continue;
        }
        let Some(mut min) = heap.peek_mut() else {
            continue;
        };
        let min_h = min.0.0;
        let out = if h > min_h {
            *min = Reverse((h, idx));
            min_h
        } else {
            h
        };
        left_out = Some(left_out.map_or(out, |l| l.max(out)));
    }
    let boundary = heap.peek().map_or(0, |min| min.0.0);
    let (keep_above, next_below) = match left_out {
        None => (None, 0),
        Some(l) if l < boundary => (None, boundary),
        Some(_) if heap.iter().any(|e| e.0.0 > boundary) => (Some(boundary), boundary + 1),
        Some(_) => {
            // Every member of the page shares one hash: return all of them.
            let mut results = Vec::with_capacity(page);
            for member in set.iter() {
                if member_hash(member) == boundary && pattern.is_none_or(|p| glob_match(p, member))
                {
                    results.push(Frame::BulkString(member.clone()));
                }
            }
            return scan_reply(hash_cursor(boundary), results);
        }
    };
    let mut results = Vec::with_capacity(heap.len());
    for Reverse((h, idx)) in heap.into_sorted_vec() {
        if keep_above.is_some_and(|b| h <= b) {
            continue;
        }
        if let Some(member) = set.get_index(idx)
            && pattern.is_none_or(|p| glob_match(p, member))
        {
            results.push(Frame::BulkString(member.clone()));
        }
    }
    scan_reply(hash_cursor(next_below), results)
}

/// The hash-mode cursor that continues below `below` (0: nothing below).
fn hash_cursor(below: u64) -> u64 {
    if below == 0 { 0 } else { HASH_MODE | below }
}

/// `[cursor, [items...]]` — the SSCAN reply shape.
pub(super) fn scan_reply(next_cursor: u64, results: Vec<Frame>) -> Frame {
    let cursor = if next_cursor == 0 {
        Bytes::from_static(b"0")
    } else {
        let mut buf = itoa::Buffer::new();
        Bytes::copy_from_slice(buf.format(next_cursor).as_bytes())
    };
    Frame::Array(framevec![
        Frame::BulkString(cursor),
        Frame::Array(results.into()),
    ])
}

#[cfg(test)]
pub(super) mod test_hooks {
    //! Cursor introspection for `sscan_tests`.
    pub fn is_hash_mode(cursor: u64) -> bool {
        cursor & super::HASH_MODE != 0 && cursor & super::FOREIGN == 0
    }
    /// One hash-mode page with a caller-chosen member hash; `below` is the
    /// threshold (`None`: from the top). Answers (next threshold, members).
    pub fn hash_page_by(
        set: &super::SetValue,
        below: Option<u64>,
        count: usize,
        hash: impl Fn(&[u8]) -> u64,
    ) -> (Option<u64>, Vec<bytes::Bytes>) {
        let frame = super::hash_page_by(set, below.unwrap_or(super::HASH_TOP), count, None, hash);
        let crate::protocol::Frame::Array(outer) = frame else {
            panic!("not an array")
        };
        let (crate::protocol::Frame::BulkString(c), crate::protocol::Frame::Array(items)) =
            (&outer[0], &outer[1])
        else {
            panic!("bad reply shape")
        };
        let cursor: u64 = std::str::from_utf8(c).unwrap().parse().unwrap();
        let next = (cursor != 0).then(|| {
            assert!(is_hash_mode(cursor));
            cursor & !super::HASH_MODE
        });
        let items = items
            .iter()
            .map(|f| match f {
                crate::protocol::Frame::BulkString(b) => b.clone(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        (next, items)
    }
    /// [`super::sscan_page`] for a value with an unstable layout (cold).
    pub fn sscan_page_unstable(
        set: &super::SetValue,
        cursor: u64,
        count: usize,
    ) -> crate::protocol::Frame {
        super::sscan_page(set, cursor, count, None, false)
    }
}
