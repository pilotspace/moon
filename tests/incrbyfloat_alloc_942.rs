//! moon#942: `INCRBYFLOAT` must touch the allocator once, not four times.
//!
//! The handler rendered its result through `format_float`, which builds a
//! `String` with `format!` and then rebuilds it with `to_string()` after
//! trimming; the handler then `clone()`d that `String` to have one copy for
//! the stored `Entry` and one for the reply. `format!`, `to_string()` and
//! `clone()` are all three banned outright in `src/command/` by CLAUDE.md, and
//! this is one command paying all three per call.
//!
//! # What the budget is, and why it is 2 and not 1
//!
//! Two allocations are what the current renderer can reach, and the ceiling is
//! set there deliberately rather than at the floor:
//!
//! 1. `format!("{}", f64)` — `String` growth. Rust's `Display` for `f64` is
//!    the only renderer that produces moon's exact output (positional, shortest
//!    round-trip), so removing this means rendering into a stack buffer, which
//!    is written up as a proposal rather than done here.
//! 2. `Bytes::from(String)` — `Vec::into_boxed_slice` reallocates whenever the
//!    `String`'s capacity exceeds its length, which for `format!` output it
//!    normally does.
//!
//! What this ratchet locks out is everything that WAS on top of those: the
//! second `String` `format_float` built with `to_string()` after trimming, and
//! the `formatted.clone()` the handler made so the `Entry` and the reply could
//! each have one. Both are banned outright in `src/command/` by CLAUDE.md, and
//! both were free to remove: `truncate` trims in place, and a counter that
//! renders to <= 12 bytes inlines into `CompactValue`'s SSO payload, so the
//! `Entry` never needed a buffer of its own at all.
//!
//! The genuine floor is 1 for a narrow value (the reply `Bytes`) and 2 for one
//! wider than the 12-byte SSO window (reply + the entry's `Box<[u8]>`).
//!
//! The instrument is a counting `GlobalAlloc` around `System`, the same shape
//! as `tests/listpack_encode_alloc_942.rs`. It is a whole-process counter, so
//! this file deliberately holds exactly ONE `#[test]`: a second would run
//! concurrently on another thread and its allocations would land in these
//! counters.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use moon::protocol::Frame;
use moon::storage::Database;

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

/// One `INCRBYFLOAT key delta` against a warm database, measured.
fn incrbyfloat_allocs(db: &mut Database, key: &'static [u8], delta: &'static [u8]) -> (Frame, usize) {
    let args = [
        Frame::BulkString(Bytes::from_static(key)),
        Frame::BulkString(Bytes::from_static(delta)),
    ];
    let before = allocs();
    let reply = moon::command::string::incrbyfloat(db, &args);
    (reply, allocs() - before)
}

#[test]
fn incrbyfloat_allocates_once_per_call() {
    let mut db = Database::new();

    // Warm every lazily-initialised structure the FIRST write to a database
    // touches, so the measured call below bills only its own work. Without
    // this the first row would carry the DashTable's segment growth and the
    // ledger's first-touch costs and the number would mean nothing.
    for _ in 0..64 {
        let _ = incrbyfloat_allocs(&mut db, b"warm", b"1.5");
    }

    // ── The shapes the renderer takes ─────────────────────────────────────
    //
    // `format_float` has two arms: an integral result renders through the
    // `i64` path, a fractional one renders `f64` Display and then trims. Both
    // must hold the budget, because the trim arm is where the second `String`
    // used to be built.
    let cases: [(&'static [u8], &'static [u8], &'static str); 4] = [
        // integral arm: 2.0 + 1.0 == 3
        (b"i", b"1.0", "integral"),
        // fractional arm, needs trimming
        (b"f", b"0.1", "fractional"),
        // negative, fractional
        (b"n", b"-0.25", "negative fractional"),
        // large magnitude: the rendered payload exceeds CompactValue's
        // 12-byte SSO window, so the entry really does need a heap buffer and
        // this is the worst case of the four.
        (b"b", b"123456789012345.5", "wide"),
    ];

    // Measured first, asserted after, so a failure reports every arm rather
    // than only the first one to break.
    let mut measured: Vec<(&'static str, usize)> = Vec::with_capacity(cases.len());
    for (key, delta, label) in cases {
        // Create the key first; the measured call is then a pure update, the
        // same shape a benchmark's steady state takes.
        let _ = incrbyfloat_allocs(&mut db, key, delta);
        let (reply, n) = incrbyfloat_allocs(&mut db, key, delta);
        assert!(
            matches!(reply, Frame::BulkString(_)),
            "{label}: expected a bulk reply, got {reply:?}"
        );
        measured.push((label, n));
    }
    const BUDGET: usize = 2;
    let over: Vec<_> = measured.iter().filter(|(_, n)| *n > BUDGET).collect();
    assert!(
        over.is_empty(),
        "INCRBYFLOAT allocated more than {BUDGET} times: {over:?} (all arms: \
         {measured:?}). Only the `format!` growth and the `Bytes::from(String)` \
         shrink are in budget — see this file's module docs. A third means the \
         `to_string()` in `format_float` or the `formatted.clone()` in the \
         handler has come back."
    );

    // ── Control ───────────────────────────────────────────────────────────
    //
    // A counter that can only ever read zero proves nothing. This shows the
    // instrument moves: one deliberate heap allocation is seen as one.
    let before = allocs();
    let boxed = vec![0u8; 4096];
    let after = allocs();
    assert!(
        after > before,
        "the allocation counter is stuck; every assertion above is vacuous"
    );
    drop(boxed);
}
