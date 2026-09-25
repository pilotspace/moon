//! moon#1212: a listpack's buffer grows to the allocator's size class of its
//! new length — what redis's `lp_realloc` gets back from jemalloc — instead of
//! `Vec`'s doubling, which left a capped list carrying up to 2x its bytes.

use super::*;
use crate::storage::mem_size::size_class;

/// Growing push by push, the capacity is always exactly the size class of
/// the bytes in use (no slack past what the allocator hands out anyway), and
/// the buffer is reallocated once per class entered — not once per push.
///
/// Red on `ae21476` (doubling: 1,600 B of capacity under a 925 B listpack)
/// and on bare exact growth (a reallocation on every one of the 120 pushes).
#[test]
fn growth_takes_the_size_class_and_reallocates_once_per_class() {
    let mut lp = Listpack::new();
    let mut cap = lp.data.capacity();
    let mut reallocs = 0usize;
    let mut off_class = 0usize;
    let mut classes = std::collections::BTreeSet::new();
    for i in 0..120u32 {
        lp.push_back(format!("recent-item-{i:04}").as_bytes());
        let now = lp.data.capacity();
        if now != cap {
            reallocs += 1;
            cap = now;
        }
        let class = size_class(lp.data.len());
        if now != class {
            off_class += 1;
        }
        classes.insert(class);
    }
    assert_eq!(
        (off_class, reallocs),
        (0, classes.len()),
        "of 120 pushes {off_class} left the capacity off the size class of the bytes in use, \
         and the buffer was reallocated {reallocs} times across {} size classes",
        classes.len()
    );
}

/// The same rule on the in-place REPLACE path (HSET / ZADD / LSET of a wider
/// value), which shares `write_entry`.
#[test]
fn a_wider_replacement_grows_to_the_size_class_too() {
    let mut lp = Listpack::new();
    for i in 0..40u32 {
        lp.push_back(format!("f{i}").as_bytes());
    }
    let wide = vec![b'x'; 60];
    for i in (0..40).step_by(2) {
        lp.replace_at(i, &wide);
        assert_eq!(lp.data.capacity(), size_class(lp.data.len()));
    }
}
