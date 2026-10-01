//! Whole-segment control-byte bitmasks (moon#1298 prototype).
//!
//! The expiry wheel names a key by its hash alone, so turning a reference
//! back into a key means asking "which slots of THIS segment carry this 7-bit
//! tag?" without a key to compare. [`Segment::tag_masks`] answers that for all
//! 64 control bytes at once, in plain safe SWAR (8 control bytes per `u64`):
//! no `unsafe`, no per-slot branch, ~100 instructions instead of ~1500 for a
//! slot-by-slot walk.

use super::{NUM_GROUPS, Segment};

const LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F;
const HI: u64 = 0x8080_8080_8080_8080;
const ONES: u64 = 0x0101_0101_0101_0101;

/// Gather the high bit of each byte of `m` (bytes are `0x80` or `0x00`) into
/// an 8-bit mask: bit `i` = byte `i`. The multiply routes byte `i`'s LSB to
/// bit `56 + i` with no colliding partial products, hence no carries.
#[inline]
fn gather(m: u64) -> u64 {
    ((m >> 7).wrapping_mul(0x0102_0408_1020_4080)) >> 56
}

#[inline]
fn word(g: &[u8; 16], half: usize) -> u64 {
    let b = &g[half * 8..half * 8 + 8];
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// `(full, matching)` masks over 64 slots: bit `s` of `full` is set iff slot
/// `s`'s control byte is FULL (`0x00..=0x7F`); bit `s` of `matching` iff it is
/// FULL and equals `tag` (`tag <= 0x7F`). EMPTY (`0xFF`) and DELETED (`0x80`)
/// have the high bit set, so they can match neither.
#[inline]
pub(super) fn masks_from_ctrl(ctrl: [&[u8; 16]; NUM_GROUPS], tag: u8) -> (u64, u64) {
    let (mut full, mut matching) = (0u64, 0u64);
    let tag8 = ONES.wrapping_mul(u64::from(tag));
    for (g, bytes) in ctrl.iter().enumerate() {
        for half in 0..2 {
            let w = word(bytes, half);
            let shift = (g * 16 + half * 8) as u32;
            full |= gather(!w & HI) << shift;
            // Exact zero-byte detect of `w ^ tag`: 0x80 where the byte is 0.
            let x = w ^ tag8;
            let y = (x & LO7).wrapping_add(LO7);
            matching |= gather(!(y | x | LO7)) << shift;
        }
    }
    (full, matching)
}

impl<K, V> Segment<K, V> {
    /// FULL-slot and tag-match bitmasks over this segment's control bytes
    /// (bit `s` = slot `s`; slots 60..64 are padding and always clear).
    #[inline]
    pub fn tag_masks(&self, tag: u8) -> (u64, u64) {
        masks_from_ctrl(
            [
                &self.ctrl[0].0,
                &self.ctrl[1].0,
                &self.ctrl[2].0,
                &self.ctrl[3].0,
            ],
            tag,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SWAR masks equal a slot-by-slot reference over random control
    /// bytes drawn from {EMPTY, DELETED, FULL(tag), FULL(other)}.
    #[test]
    fn swar_masks_equal_the_naive_walk() {
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        for _ in 0..20_000 {
            let tag = (next() % 0x80) as u8;
            let mut ctrl = [[0xFFu8; 16]; NUM_GROUPS];
            for g in ctrl.iter_mut() {
                for b in g.iter_mut() {
                    *b = match next() % 5 {
                        0 => 0xFF,
                        1 => 0x80,
                        2 => tag,
                        3 => (next() % 0x80) as u8,
                        _ => ((next() % 0x80) as u8) ^ 1,
                    };
                }
            }
            let (mut full, mut matching) = (0u64, 0u64);
            for slot in 0..64usize {
                let c = ctrl[slot / 16][slot % 16];
                if c & 0x80 == 0 {
                    full |= 1 << slot;
                    if c == tag {
                        matching |= 1 << slot;
                    }
                }
            }
            let got = masks_from_ctrl([&ctrl[0], &ctrl[1], &ctrl[2], &ctrl[3]], tag);
            assert_eq!(got, (full, matching), "tag {tag:#x} ctrl {ctrl:?}");
        }
    }
}
