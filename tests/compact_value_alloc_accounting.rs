//! `CompactValue` heap strings: what the allocator is asked for must be what
//! `estimate_memory` bills (moon perf campaign, G1 §5; the billing seam is
//! moon#788 / #810).
//!
//! `used_memory` is the ledger behind `--maxmemory`. For a heap string it is
//! `mem_size::size_class(len)`, which models jemalloc's rounding of ONE
//! request of exactly `len` bytes. That formula is only right if the value
//! really is one allocation of `len` bytes — a second wrapper allocation, or
//! a buffer with stranded capacity, would be memory the ledger never sees.
//!
//! The instrument is a counting `GlobalAlloc` wrapped around `System`, the
//! same shape as `tests/shard_idle_alloc_attribution.rs`: it records how
//! many requests were made and how many bytes each asked for, independent of
//! which allocator is live. `size_class` itself is checked against the
//! published jemalloc class table in `mem_size`'s own tests, so the two facts
//! together — one request of `len` bytes, billed `size_class(len)` — pin the
//! ledger to the allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use moon::storage::compact_value::CompactValue;
use moon::storage::mem_size::size_class;

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards to `System` with the caller's own layout
// unchanged, so `System`'s contract is upheld verbatim; the counters are
// atomics and never allocate, so there is no re-entrancy.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: same layout the caller passed; `System::alloc` has no
        // precondition beyond a non-zero-size layout, which `GlobalAlloc`'s
        // caller contract already guarantees.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: `ptr` and `layout` are the pair the caller obtained from
        // this allocator's `alloc`, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Snapshot {
    allocs: usize,
    deallocs: usize,
    alloc_bytes: usize,
    dealloc_bytes: usize,
}

fn snapshot() -> Snapshot {
    Snapshot {
        allocs: ALLOCS.load(Ordering::Relaxed),
        deallocs: DEALLOCS.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn delta(before: Snapshot, after: Snapshot) -> Snapshot {
    Snapshot {
        allocs: after.allocs - before.allocs,
        deallocs: after.deallocs - before.deallocs,
        alloc_bytes: after.alloc_bytes - before.alloc_bytes,
        dealloc_bytes: after.dealloc_bytes - before.dealloc_bytes,
    }
}

/// Lengths straddling every jemalloc small-class boundary the fixture can
/// reach, plus the SSO edge and two large sizes.
const SIZES: &[usize] = &[
    13,
    15,
    16,
    17,
    31,
    32,
    33,
    47,
    48,
    49,
    63,
    64,
    65,
    79,
    80,
    81,
    95,
    96,
    100,
    127,
    128,
    129,
    159,
    160,
    161,
    255,
    256,
    257,
    4_095,
    4_096,
    4_097,
    65_536,
    1 << 20,
];

/// The counters are process-global, so the three scenarios run back to back
/// on ONE thread: the libtest harness runs `#[test]` functions in parallel,
/// and a sibling test's allocations would land in this test's deltas.
#[test]
fn heap_string_allocation_accounting() {
    heap_string_is_one_allocation_of_len_bytes_billed_as_its_size_class();
    owned_vec_is_adopted_without_a_wrapper_allocation();
    overwrite_is_one_free_and_one_alloc_with_an_exact_ledger_delta();
}

/// Building a heap string from a borrowed slice is exactly ONE allocation of
/// exactly `len` bytes, billed as `size_class(len)`; dropping it returns that
/// same single allocation.
fn heap_string_is_one_allocation_of_len_bytes_billed_as_its_size_class() {
    for &len in SIZES {
        let data: Vec<u8> = (0..len).map(|i| (i as u8) ^ 0x5A).collect();

        let before = snapshot();
        let cv = CompactValue::from_slice(&data);
        let built = delta(before, snapshot());

        assert!(!cv.is_inline(), "len={len} must be a heap string");
        assert_eq!(
            built.allocs, 1,
            "len={len}: a heap string is ONE allocation"
        );
        assert_eq!(
            built.alloc_bytes, len,
            "len={len}: the one allocation must ask for exactly the data"
        );
        assert_eq!(
            cv.estimate_memory(),
            size_class(len),
            "len={len}: billing must be the size class of that one request"
        );
        assert_eq!(cv.as_bytes(), Some(&data[..]));

        let before = snapshot();
        drop(cv);
        let freed = delta(before, snapshot());
        assert_eq!(freed.deallocs, 1, "len={len}: drop frees ONE allocation");
        assert_eq!(
            freed.dealloc_bytes, len,
            "len={len}: drop must hand back the same layout it was given"
        );
    }
}

/// The owned-`Vec` constructor (RDB load fast path) must not allocate at all
/// when the vector is exactly sized: the buffer is adopted, not copied. When
/// it is over-capacity the excess is trimmed, so nothing is stranded.
fn owned_vec_is_adopted_without_a_wrapper_allocation() {
    for &len in SIZES {
        let exact: Vec<u8> = vec![b'v'; len];
        assert_eq!(exact.capacity(), len);
        let before = snapshot();
        let cv = CompactValue::heap_string_vec_direct(exact);
        let built = delta(before, snapshot());
        assert_eq!(
            built.allocs, 0,
            "len={len}: an exact-capacity Vec is adopted with no new allocation"
        );
        assert_eq!(cv.estimate_memory(), size_class(len));
        drop(cv);
    }

    // Over-capacity: one realloc to trim, then the same single ownership.
    let mut fat: Vec<u8> = Vec::with_capacity(1_024);
    fat.extend_from_slice(&[b'f'; 100]);
    let cv = CompactValue::heap_string_vec_direct(fat);
    assert_eq!(cv.estimate_memory(), size_class(100));
    let before = snapshot();
    drop(cv);
    let freed = delta(before, snapshot());
    assert_eq!(freed.deallocs, 1);
    assert_eq!(
        freed.dealloc_bytes, 100,
        "the trimmed buffer is what is freed"
    );
}

/// Overwrite — the path the campaign measured: dropping the old value and
/// installing the new one is one free plus one allocation, and the ledger
/// delta is exactly the difference of the two size classes.
fn overwrite_is_one_free_and_one_alloc_with_an_exact_ledger_delta() {
    let old_len = 64;
    let new_len = 100;
    let old_data = vec![b'o'; old_len];
    let new_data = vec![b'n'; new_len];
    let mut slot = CompactValue::from_slice(&old_data);
    let old_bill = slot.estimate_memory();

    let before = snapshot();
    slot = CompactValue::from_slice(&new_data);
    let d = delta(before, snapshot());

    assert_eq!(d.allocs, 1, "the new value is one allocation");
    assert_eq!(d.deallocs, 1, "the old value is one free");
    assert_eq!(d.alloc_bytes, new_len);
    assert_eq!(d.dealloc_bytes, old_len);
    assert_eq!(
        slot.estimate_memory() as isize - old_bill as isize,
        size_class(new_len) as isize - size_class(old_len) as isize
    );
}
