//! moon#942: writing one listpack entry must not touch the allocator.
//!
//! `encode_entry` built a `Vec<u8>` per entry written and dropped it as soon
//! as the bytes had been copied into the listpack -- and `Vec::new()` starts
//! at capacity 0, so appending the encoding and then the backlen charged the
//! allocator more than once. HSET, LPUSH, SADD's listpack path and ZADD's
//! listpack path all pay it, which is four of the five families in moon#942,
//! including the two the dispatch-path tax does not reach. Redis encodes into
//! a stack `intenc[LP_MAX_INT_ENCODING_LEN]` and never allocates.
//!
//! Note what moon#861 does and does not say. It ruled the allocator out for
//! SADD in the HASHTABLE regime -- where `encode_entry` is never called at
//! all. That exclusion does not transfer here.
//!
//! The claim under test is an ABSOLUTE count, not a scaling one: a
//! same-width replacement neither grows nor shrinks the listpack's buffer, so
//! after the rewrite the only correct answer is ZERO allocations. The control
//! at the end proves the counter is not simply stuck at zero.
//!
//! The instrument is a counting `GlobalAlloc` around `System`, the same shape
//! as `tests/listpack_scan_alloc.rs`. It is a whole-process counter, so this
//! file deliberately holds exactly ONE `#[test]`: a second would run
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

const PAIRS: usize = 64;
const ROUNDS: usize = 100;

#[test]
fn writing_a_listpack_entry_does_not_allocate() {
    // Fixture, probes and their replacements are all built OUTSIDE the
    // measured windows -- `format!` allocates, and that is the point of the
    // instrument, not noise it should have to tolerate.
    let mut lp = Listpack::new();
    for i in 0..PAIRS {
        lp.push_back(format!("field:{i:08}").as_bytes());
        lp.push_back(format!("value-payload-{i:08}").as_bytes());
    }
    let field = format!("field:{:08}", PAIRS - 1).into_bytes();
    // Byte-for-byte the width already stored there, so the buffer neither
    // grows nor shrinks and `Vec` itself has no reason to reallocate.
    let same_width = format!("value-payload-{:08}", PAIRS - 1).into_bytes();
    assert_eq!(same_width.len(), 22, "fixture width drifted");

    // One call outside the window, so anything lazily initialised on the
    // first write is paid for before the counter is read.
    assert!(lp.replace_pair_value(&field, &same_width));

    let mark = allocs();
    for _ in 0..ROUNDS {
        assert!(lp.replace_pair_value(&field, &same_width));
    }
    let replace_pair_value = allocs() - mark;

    // `replace_at(1, ..)` is pair 0's value: the ZADD/LSET shape.
    let mark = allocs();
    for _ in 0..ROUNDS {
        lp.replace_at(1, &same_width);
    }
    let replace_at = allocs() - mark;

    // The integer arm, which carries no payload at all.
    let int_field = b"field:00000000".to_vec();
    let int_value = b"1234567".to_vec();
    assert!(lp.replace_pair_value(&int_field, &int_value));
    let mark = allocs();
    for _ in 0..ROUNDS {
        assert!(lp.replace_pair_value(&int_field, &int_value));
    }
    let integer_entry = allocs() - mark;

    // Control: the counter must be able to move, or every assertion above is
    // vacuous.
    let mark = allocs();
    for _ in 0..ROUNDS {
        std::hint::black_box(Vec::<u8>::with_capacity(64));
    }
    let control = allocs() - mark;

    println!(
        "allocations over {ROUNDS} same-width writes:\n\
         \x20 replace_pair_value (string entry)  {replace_pair_value}\n\
         \x20 replace_at         (string entry)  {replace_at}\n\
         \x20 replace_pair_value (integer entry) {integer_entry}\n\
         \x20 control (Vec::with_capacity)       {control}"
    );

    assert!(
        control >= ROUNDS,
        "the allocation counter never moved ({control} over {ROUNDS} \
         Vec::with_capacity calls) -- the instrument is broken, so the \
         assertions below prove nothing"
    );
    assert_eq!(
        replace_pair_value, 0,
        "replace_pair_value allocated {replace_pair_value} times over \
         {ROUNDS} same-width writes; encoding an entry must use a stack \
         buffer (moon#942)"
    );
    assert_eq!(
        replace_at, 0,
        "replace_at allocated {replace_at} times over {ROUNDS} same-width \
         writes (moon#942)"
    );
    assert_eq!(
        integer_entry, 0,
        "the integer encoding arm allocated {integer_entry} times over \
         {ROUNDS} same-width writes (moon#942)"
    );
}
