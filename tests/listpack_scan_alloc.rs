//! moon#799: a listpack index walk must not allocate per entry it STEPS OVER.
//!
//! `remove_at` / `replace_at` discard every entry they pass on the way to the
//! target, yet they walked with `decode_entry_at`, which materializes each
//! string entry into a fresh `Vec`. One HSET onto an existing field of a
//! 128-field hash therefore ran ~129 malloc/free pairs inside `src/command/`,
//! which CLAUDE.md forbids from allocating at all.
//!
//! The claim under test is *scaling*, not an absolute count: whatever a single
//! `replace_at` costs, it must cost the SAME on a 128-pair listpack as on an
//! 8-pair one. A per-entry allocation cannot survive that assertion, and no
//! amount of unrelated allocator noise in the fixture can fake it away,
//! because both legs pay the same noise.
//!
//! The instrument is a counting `GlobalAlloc` around `System`, the same shape
//! as `tests/compact_value_alloc_accounting.rs` and
//! `tests/shard_idle_alloc_attribution.rs`. It is a whole-process counter, so
//! this file deliberately holds exactly ONE `#[test]`: a second one would run
//! concurrently on another thread and its allocations would land in these
//! counters.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use moon::storage::listpack::Listpack;

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards to `System` with the caller's own layout
// unchanged, so `System`'s contract is upheld verbatim; the counter is an
// atomic and never allocates, so there is no re-entrancy.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: same layout the caller passed; `System::alloc` has no
        // precondition beyond a non-zero-size layout, which `GlobalAlloc`'s
        // caller contract already guarantees.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` are the pair the caller obtained from
        // this allocator's `alloc`, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs() -> usize {
    ALLOCS.load(Ordering::Relaxed)
}

/// `pairs` field/value pairs, values long enough to take the 6-bit STRING
/// encoding (the arm that used to `to_vec()`), fields likewise.
fn build(pairs: usize) -> Listpack {
    let mut lp = Listpack::new();
    for i in 0..pairs {
        lp.push_back(format!("field:{i:08}").as_bytes());
        lp.push_back(format!("value-payload-{i:08}").as_bytes());
    }
    lp
}

/// Allocations charged to one `f(lp)`, measured on a listpack of `pairs`
/// pairs. The fixture is built OUTSIDE the measured window.
fn cost<F: FnOnce(&mut Listpack)>(pairs: usize, f: F) -> usize {
    let mut lp = build(pairs);
    // Touch the last pair first so any lazy growth has already happened.
    std::hint::black_box(lp.len());
    let before = allocs();
    f(&mut lp);
    let after = allocs();
    std::hint::black_box(&lp);
    after - before
}

#[test]
fn listpack_index_walk_does_not_allocate_per_entry_stepped_over() {
    // Target the LAST pair every time: the walk is maximal, so a per-entry
    // allocation shows up at full strength.
    let small = 8usize;
    let large = 128usize;

    // --- replace_at: the HSET-on-existing-field write ---
    let r_small = cost(small, |lp| {
        lp.replace_at((small - 1) * 2 + 1, b"replacement-value-0000");
    });
    let r_large = cost(large, |lp| {
        lp.replace_at((large - 1) * 2 + 1, b"replacement-value-0000");
    });
    // --- remove_at: the HDEL write ---
    let d_small = cost(small, |lp| {
        lp.remove_at((small - 1) * 2 + 1);
    });
    let d_large = cost(large, |lp| {
        lp.remove_at((large - 1) * 2 + 1);
    });
    // --- get_at: exactly ONE allocation, for the entry actually returned ---
    let g_small = cost(small, |lp| {
        std::hint::black_box(lp.get_at((small - 1) * 2 + 1));
    });
    let g_large = cost(large, |lp| {
        std::hint::black_box(lp.get_at((large - 1) * 2 + 1));
    });
    // --- find_pair_index: the borrowed lookup, already allocation-free.
    // The `format!` inside each closure allocates; the two legs pay it
    // identically, so only the SCALING is asserted.
    let f_small = cost(small, |lp| {
        std::hint::black_box(lp.find_pair_index(format!("field:{:08}", small - 1).as_bytes()));
    });
    let f_large = cost(large, |lp| {
        std::hint::black_box(lp.find_pair_index(format!("field:{:08}", large - 1).as_bytes()));
    });

    // Every number is taken BEFORE the first assertion, and printed, so one
    // run reports the whole table instead of stopping at the first failure.
    // `cargo test --test listpack_scan_alloc -- --nocapture`.
    eprintln!(
        "allocations per call  ({small} pairs -> {large} pairs)\n\
         \x20 replace_at       {r_small} -> {r_large}\n\
         \x20 remove_at        {d_small} -> {d_large}\n\
         \x20 get_at           {g_small} -> {g_large}\n\
         \x20 find_pair_index  {f_small} -> {f_large}"
    );

    assert_eq!(
        r_small, r_large,
        "replace_at allocated {r_small} on {small} pairs but {r_large} on {large} pairs \
         — the walk is still materializing entries it only steps over (moon#799)"
    );

    assert_eq!(
        d_small, d_large,
        "remove_at allocated {d_small} on {small} pairs but {d_large} on {large} pairs (moon#799)"
    );
    assert_eq!(
        d_large, 0,
        "remove_at keeps nothing — it must not allocate at all, got {d_large}"
    );

    assert_eq!(
        g_small, g_large,
        "get_at allocated {g_small} on {small} pairs but {g_large} on {large} pairs (moon#799)"
    );
    assert!(
        g_large <= 1,
        "get_at must allocate at most the one entry it returns, got {g_large}"
    );

    assert_eq!(
        f_small, f_large,
        "find_pair_index must not scale with entry count (moon#799)"
    );
}
