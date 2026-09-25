//! moon#1226 (storage core): the split test gap.
//!
//! `Segment::split` recomputes `has_non_home_keys` from the STAYERS'
//! positions (moon#1159 follow-up): a stayer the "any free slot" fallback once
//! placed outside its home groups must keep the flag set, or `find` skips the
//! fallback scan (PERF-09) and that key becomes unreachable. The existing
//! split tests never had a key off home, so a split that simply cleared the
//! flag passed them. These fixtures FORCE the fallback placement and then
//! split, on both sides of the routing bit, and in the new segment too.
//!
//! Keys are their own hashes (`u64`, identity hasher), chosen so both home
//! buckets fall in group 0; filling group 0 and the stash leaves the next
//! group-0 key only the fallback path.

use super::*;

/// The segment's routing bit for the depth 0 -> 1 split.
const TOP: u64 = 1 << 63;

/// `n` distinct keys whose two home buckets are both in group 0, with the
/// routing bit set as asked. Deterministic search over a mixed counter.
fn group0_keys(n: usize, top: bool, salt: u64) -> Vec<u64> {
    let mut out = Vec::with_capacity(n);
    let mut x = salt;
    while out.len() < n {
        // splitmix64
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let h = if top { z | TOP } else { z & !TOP };
        let (a, b) = home_buckets(h);
        if a / 16 == 0 && b / 16 == 0 && !out.contains(&h) {
            out.push(h);
        }
    }
    out
}

fn insert(seg: &mut Segment<u64, u64>, k: u64) {
    let (a, b) = home_buckets(k);
    assert!(
        matches!(seg.insert(h2(k), k, !k, a, b), InsertResult::Inserted),
        "fixture insert of {k:#x}"
    );
}

fn find(seg: &Segment<u64, u64>, k: u64) -> Option<u64> {
    let (a, b) = home_buckets(k);
    seg.get(h2(k), &k, a, b).copied()
}

/// Fill group 0 (16) and the stash (4) with `fill`, then insert `extra`:
/// it has no home slot and no stash slot left, so the fallback places it in
/// another group and raises the flag.
fn segment_with_one_off_home(fill: &[u64], extra: u64) -> Segment<u64, u64> {
    assert_eq!(fill.len(), 16 + STASH_SLOTS);
    let mut seg: Segment<u64, u64> = Segment::new(0);
    for &k in fill {
        insert(&mut seg, k);
    }
    assert!(
        !seg.has_non_home_keys(),
        "home + stash placements are not off home"
    );
    insert(&mut seg, extra);
    let (a, b) = home_buckets(extra);
    assert_eq!(
        seg.is_in_non_home_group(h2(extra), &extra, a, b),
        Some(true),
        "fixture: the extra key must have taken the fallback placement"
    );
    assert!(
        seg.has_non_home_keys(),
        "the fallback placement raises the flag"
    );
    seg
}

/// A stayer placed off home keeps `has_non_home_keys` set across the split,
/// so `find` still scans for it; every other key moves out.
#[test]
fn a_stayer_placed_off_home_keeps_the_fallback_flag_through_a_split() {
    let fill = group0_keys(16 + STASH_SLOTS, true, 1); // all movers
    let stayer = group0_keys(1, false, 2)[0];
    let mut seg = segment_with_one_off_home(&fill, stayer);
    let new_seg = seg.split(&|k: &u64| *k);
    assert!(
        seg.has_non_home_keys(),
        "split cleared has_non_home_keys while an off-home stayer remains: \
         find() would skip the fallback scan and lose the key"
    );
    assert_eq!(find(&seg, stayer), Some(!stayer), "the off-home stayer");
    assert_eq!(seg.count(), 1);
    for &k in &fill {
        assert_eq!(find(&new_seg, k), Some(!k), "mover {k:#x}");
        assert_eq!(find(&seg, k), None);
    }
    // The movers landed at home (group 0 + stash) in an empty segment.
    assert!(!new_seg.has_non_home_keys());
}

/// When the only off-home key MOVES, the old segment's flag is lowered (no
/// off-home key left) and the key is found at a home slot in the new one.
#[test]
fn the_flag_is_lowered_when_the_off_home_key_moves_out() {
    let fill = group0_keys(16 + STASH_SLOTS, false, 3); // all stayers
    let mover = group0_keys(1, true, 4)[0];
    let mut seg = segment_with_one_off_home(&fill, mover);
    let new_seg = seg.split(&|k: &u64| *k);
    assert!(
        !seg.has_non_home_keys(),
        "no off-home key remains; find() may skip the fallback scan again"
    );
    for &k in &fill {
        assert_eq!(find(&seg, k), Some(!k), "stayer {k:#x}");
    }
    assert_eq!(find(&new_seg, mover), Some(!mover));
    assert!(!new_seg.has_non_home_keys());
}

/// A split whose movers overflow their home group and the stash in the NEW
/// segment places the rest through the fallback and raises the new
/// segment's flag (`insert_during_split`'s last resort).
#[test]
fn movers_overflowing_home_in_the_new_segment_raise_its_flag() {
    let fill = group0_keys(16 + STASH_SLOTS, true, 5);
    let extra = group0_keys(1, true, 6)[0];
    let mut seg = segment_with_one_off_home(&fill, extra);
    let new_seg = seg.split(&|k: &u64| *k);
    assert_eq!(seg.count(), 0);
    assert!(!seg.has_non_home_keys());
    assert!(
        new_seg.has_non_home_keys(),
        "21 movers homed to group 0 cannot all fit home + stash in the new segment"
    );
    for &k in fill.iter().chain([&extra]) {
        assert_eq!(find(&new_seg, k), Some(!k), "mover {k:#x}");
    }
}
