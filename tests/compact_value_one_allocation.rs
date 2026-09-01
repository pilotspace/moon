//! Allocation-shape harness for `CompactValue`'s heap string representation.
//!
//! # Why this file exists
//!
//! Memory is the only dimension where moon still loses to Redis. Redis packs
//! `robj + sds header + data` into ONE allocation for string values up to 44
//! bytes (`embstr`). moon used to need TWO: the data buffer, plus a
//! `Box<HeapString>` wrapper holding the `Box<[u8]>` fat pointer. That wrapper
//! is 16 bytes and lands in jemalloc's 16-byte size class, so it costs a flat
//! **16 bytes on every key** whose value exceeds the SSO cutoff.
//!
//! `CompactValue` already stores the string length in its own 16 bytes, so the
//! wrapper carried no information the entry did not already have. This harness
//! pins the resulting invariant: **one allocation, of exactly `len` bytes, per
//! heap string** — and pins the memory-safety consequences of owning that
//! allocation through a raw thin pointer (no leak, no double free, correct
//! clone / overwrite / drop).
//!
//! # What is host-independent here, and what is not
//!
//! Allocation **counts and requested layout sizes are structural** — they come
//! from the same `to_vec()` / `Box::new()` calls on every platform, and this
//! file asserts only those. It asserts **no RSS number**: RSS depends on the
//! allocator's retention policy (jemalloc's `background_thread` does not exist
//! on macOS) and must be confirmed on Linux.
//!
//! The mapping from a requested size to a jemalloc size class is likewise
//! structural (a function of `LG_QUANTUM`, which is 4 on both x86_64 and
//! aarch64), and is measured with `nallocx` outside this crate.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use moon::storage::compact_value::CompactValue;
use moon::storage::entry::RedisValue;

// ── Recording allocator ───────────────────────────────────────────────────────

const CAP: usize = 64;

thread_local! {
    /// Armed only inside [`record`]; the allocator is otherwise a pure forward.
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<[usize; CAP]> = const { Cell::new([0; CAP]) };
    static N_ALLOC: Cell<usize> = const { Cell::new(0) };
    static DEALLOCS: Cell<[usize; CAP]> = const { Cell::new([0; CAP]) };
    static N_DEALLOC: Cell<usize> = const { Cell::new(0) };
}

fn push(
    slot: &'static std::thread::LocalKey<Cell<[usize; CAP]>>,
    n: &'static std::thread::LocalKey<Cell<usize>>,
    size: usize,
) {
    let i = n.get();
    if i < CAP {
        let mut arr = slot.get();
        arr[i] = size;
        slot.set(arr);
    }
    n.set(i + 1);
}

struct Recording;

// SAFETY: every method forwards verbatim to `System`, which is a sound
// `GlobalAlloc`. The only added work is bookkeeping in `Cell`-typed
// thread-locals declared with `const` initialisers, so recording neither
// allocates (no re-entrancy) nor produces, consumes, or reinterprets any
// pointer. The contract this impl must uphold is therefore exactly `System`'s,
// and it is discharged by delegating unchanged.
unsafe impl GlobalAlloc for Recording {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged to the system allocator,
        // whose contract is identical to the one our caller already upheld.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && ARMED.get() {
            push(&ALLOCS, &N_ALLOC, layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.get() {
            push(&DEALLOCS, &N_DEALLOC, layout.size());
        }
        // SAFETY: `ptr`/`layout` are forwarded unchanged; the caller already
        // guarantees the pair came from this allocator with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as `alloc` — unchanged forward to the system allocator.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() && ARMED.get() {
            push(&ALLOCS, &N_ALLOC, layout.size());
        }
        p
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.get() {
            push(&DEALLOCS, &N_DEALLOC, layout.size());
        }
        // SAFETY: forwarded unchanged; the caller upholds `System`'s contract.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() && ARMED.get() {
            push(&ALLOCS, &N_ALLOC, new_size);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Recording = Recording;

/// Requested allocation sizes, and freed sizes, observed while `f` ran.
///
/// `f`'s return value is dropped *after* the samples are taken, so the record
/// describes construction only. Use [`record_full`] to include the drop.
fn record<T>(f: impl FnOnce() -> T) -> (Vec<usize>, Vec<usize>, T) {
    N_ALLOC.set(0);
    N_DEALLOC.set(0);
    ARMED.set(true);
    let v = f();
    ARMED.set(false);
    let (a, d) = drain();
    (a, d, v)
}

/// As [`record`], but `f`'s value is dropped *inside* the window, so the record
/// covers the whole lifecycle. Used for the leak / double-free assertions.
fn record_full<T>(f: impl FnOnce() -> T) -> (Vec<usize>, Vec<usize>) {
    N_ALLOC.set(0);
    N_DEALLOC.set(0);
    ARMED.set(true);
    drop(f());
    ARMED.set(false);
    drain()
}

fn drain() -> (Vec<usize>, Vec<usize>) {
    let na = N_ALLOC.get();
    let nd = N_DEALLOC.get();
    assert!(
        na <= CAP && nd <= CAP,
        "recorder overflowed: {na} allocs, {nd} deallocs"
    );
    let a = ALLOCS.get()[..na].to_vec();
    let d = DEALLOCS.get()[..nd].to_vec();
    (a, d)
}

/// Every heap-string size the memory campaign benchmarks, plus the cutoff edge.
const SIZES: [usize; 6] = [13, 24, 40, 64, 96, 4096];

// ── The invariant this change exists to create ────────────────────────────────

/// A heap string must cost exactly ONE allocation, of exactly `len` bytes.
///
/// Before this change it cost two — `[len, 16]` — and the 16-byte wrapper is
/// billed against every key above the SSO cutoff. This is the whole saving.
#[test]
fn heap_string_is_exactly_one_allocation_of_exactly_len_bytes() {
    for n in SIZES {
        let data = vec![b'x'; n];
        let (allocs, _, cv) = record(|| CompactValue::from_slice(&data));
        assert!(!cv.is_inline(), "{n} B must not be inline");
        assert_eq!(
            allocs,
            vec![n],
            "a {n}-byte heap string must be ONE allocation of exactly {n} bytes, \
             got {allocs:?}"
        );
        assert_eq!(cv.as_bytes().expect("string"), &data[..]);
    }
}

/// The same for the owned-`Bytes` constructor the SET path actually takes.
///
/// The `Bytes` is deliberately a *slice of a larger shared buffer* — exactly
/// what a value parsed out of a connection read buffer is — so
/// `Bytes -> Vec<u8>` must copy rather than take the refcount-1 fast path.
/// The shared buffer and the slice are both built outside the recording
/// window, so the only allocations recorded are the ones the value itself owns.
#[test]
fn heap_string_from_redis_value_is_one_allocation() {
    for n in SIZES {
        let backing = bytes::Bytes::from(vec![b'x'; n + 8]);
        let data = backing.slice(0..n);
        assert_eq!(data.len(), n);
        let (allocs, _, cv) =
            record(move || CompactValue::from_redis_value(RedisValue::String(data)));
        assert_eq!(
            allocs,
            vec![n],
            "a {n}-byte heap string must be ONE allocation of exactly {n} bytes, \
             got {allocs:?}"
        );
        assert_eq!(cv.as_bytes().expect("string").len(), n);
        drop(backing);
    }
}

// ── Memory safety of owning the buffer through a raw thin pointer ─────────────

#[test]
fn heap_string_drop_frees_everything_it_allocated() {
    for n in SIZES {
        let data = vec![b'x'; n];
        let (allocs, deallocs) = record_full(|| CompactValue::from_slice(&data));
        assert_eq!(
            allocs, deallocs,
            "{n} B: drop must free exactly what construction allocated"
        );
    }
}

#[test]
fn clone_allocates_an_independent_buffer_and_both_are_freed() {
    for n in SIZES {
        let data = vec![b'x'; n];
        let (allocs, deallocs) = record_full(|| {
            let a = CompactValue::from_slice(&data);
            let b = a.clone();
            assert_eq!(a.as_bytes(), b.as_bytes());
            assert_ne!(
                a.as_bytes().expect("s").as_ptr(),
                b.as_bytes().expect("s").as_ptr(),
                "{n} B: clone must be a deep copy, not a shared pointer"
            );
            (a, b)
        });
        assert_eq!(
            allocs,
            vec![n, n],
            "{n} B: clone must allocate its own buffer"
        );
        assert_eq!(
            deallocs.len(),
            2,
            "{n} B: both buffers must be freed exactly once, got {deallocs:?}"
        );
    }
}

#[test]
fn overwriting_a_value_frees_the_old_buffer_and_not_the_new_one() {
    // Fixtures built outside the window: only the value's own allocations
    // are recorded.
    let a = vec![b'a'; 40];
    let b = vec![b'b'; 64];
    let (allocs, deallocs) = record_full(|| {
        let mut cv = CompactValue::from_slice(&a);
        assert_eq!(cv.as_bytes().expect("string"), &a[..]);
        cv = CompactValue::from_slice(&b);
        assert_eq!(cv.as_bytes().expect("string"), &b[..]);
        cv
    });
    assert_eq!(
        allocs,
        vec![40, 64],
        "each stored value owns exactly one buffer, got {allocs:?}"
    );
    assert_eq!(
        deallocs,
        vec![40, 64],
        "the overwritten 40 B buffer must be freed once, at the assignment, and \
         the 64 B buffer once at drop; got {deallocs:?}"
    );
}

#[test]
fn into_redis_value_moves_the_buffer_without_leak_or_double_free() {
    for n in SIZES {
        let data = vec![b'x'; n];
        let (allocs, deallocs) = record_full(|| {
            let cv = CompactValue::from_slice(&data);
            match cv.into_redis_value() {
                RedisValue::String(s) => {
                    assert_eq!(s.len(), n);
                    s
                }
                other => panic!("expected string, got {other:?}"),
            }
        });
        let a: usize = allocs.iter().sum();
        let d: usize = deallocs.iter().sum();
        assert_eq!(
            a, d,
            "{n} B: into_redis_value must balance ({allocs:?} vs {deallocs:?})"
        );
    }
}

// ── Edges ─────────────────────────────────────────────────────────────────────

/// Empty and cutoff-length strings stay inline: zero allocations, ever.
#[test]
fn sso_edges_allocate_nothing() {
    for n in [0usize, 1, 11, 12] {
        let data = vec![b'x'; n];
        let (allocs, deallocs) = record_full(|| {
            let cv = CompactValue::from_slice(&data);
            assert!(cv.is_inline(), "{n} B must stay inline");
            assert_eq!(cv.as_bytes().expect("string"), &data[..]);
            cv
        });
        assert!(
            allocs.is_empty(),
            "{n} B inline string allocated {allocs:?}"
        );
        assert!(deallocs.is_empty());
    }
}

/// One byte past the cutoff is the first heap string, and must round-trip.
#[test]
fn first_heap_length_round_trips() {
    let data = vec![b'z'; 13];
    let cv = CompactValue::from_slice(&data);
    assert!(!cv.is_inline());
    assert_eq!(cv.as_bytes().expect("string"), &data[..]);
    assert_eq!(cv.type_name(), "string");
    assert_eq!(cv.type_tag(), 0);
}

/// A length far wider than the 12-byte payload can hold inline, and wider than
/// any value the SSO path could mask, must survive the round trip byte-exactly.
#[test]
fn multi_megabyte_string_round_trips_byte_exactly() {
    let n = 3 * 1024 * 1024 + 7;
    let mut data = vec![0u8; n];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let cv = CompactValue::from_slice(&data);
    assert_eq!(cv.as_bytes().expect("string").len(), n);
    assert_eq!(cv.as_bytes().expect("string"), &data[..]);
    let back = cv.into_redis_value();
    match back {
        RedisValue::String(s) => assert_eq!(&s[..], &data[..]),
        other => panic!("expected string, got {other:?}"),
    }
}

/// Collections keep their own `Box<RedisValue>` representation and must still
/// be freed exactly once now that the type tag no longer lives in the pointer.
#[test]
fn collections_still_round_trip_and_free() {
    let (allocs, deallocs) = record_full(|| {
        let mut m = std::collections::HashMap::new();
        m.insert(
            bytes::Bytes::from_static(b"field"),
            bytes::Bytes::from_static(b"value"),
        );
        let cv = CompactValue::from_redis_value(RedisValue::Hash(m));
        assert_eq!(cv.type_name(), "hash");
        assert!(cv.as_bytes().is_none(), "a hash is not a string");
        cv
    });
    assert_eq!(
        allocs.iter().sum::<usize>(),
        deallocs.iter().sum::<usize>(),
        "hash construction/drop must balance"
    );
}
