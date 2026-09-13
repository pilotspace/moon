//! moon#942: popping one element off a listpack-encoded list must touch the
//! allocator exactly ONCE.
//!
//! `LPOP`/`RPOP` on the compact encoding go through `listpack_pop_end`
//! (`src/command/list/list_write.rs`), which materialised the entry TWICE:
//! `Listpack::get_at` decodes into an owning `ListpackEntry::String(Vec<u8>)`
//! — allocation one — and `ListpackEntry::to_bytes` then goes through
//! `as_bytes`, whose `String` arm is `s.clone()` — allocation two — before
//! `Bytes::from` takes the second `Vec` over. The first `Vec` is dropped
//! having been copied and never read.
//!
//! ONE is the floor, not zero: the reply owns its bytes and the listpack's
//! buffer is about to be mutated out from under them, so exactly one copy has
//! to happen. The same pop off the FULL (`VecDeque<Bytes>`) encoding is a
//! `Bytes` move and allocates nothing at all — which is the control that says
//! the surplus belongs to the listpack path and not to the reply.
//!
//! Why this is not a micro-optimisation nobody can see: `src/command/` is
//! forbidden from allocating on the hot path at all (CLAUDE.md), `LPOP` is the
//! queue primitive, and moon#897 made the listpack encoding SURVIVE a pop —
//! so every small work queue in the tree now takes this path on every drain,
//! where before #897 it flattened once and paid zero thereafter.
//!
//! The instrument is a counting `GlobalAlloc` around `System`, the same shape
//! as `tests/listpack_encode_alloc_942.rs`. It is a whole-process counter, so
//! this file deliberately holds exactly ONE `#[test]`: a second would run
//! concurrently on another thread and land in these counters.

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

fn bulk(s: &str) -> Frame {
    Frame::BulkString(Bytes::from(s.to_owned()))
}

/// Small enough that a push-only window never crosses `list-max-listpack-size`
/// (128) and promotes the very encoding under test out from under itself.
const ROUNDS: usize = 50;

/// Starting length for every listpack fixture, chosen so `LEN + ROUNDS` stays
/// under the 128-element threshold.
const LEN: usize = 20;

/// A list of `len` elements under `key`, left on the encoding `len` implies.
fn seeded(key: &str, element: &str, len: usize) -> Database {
    let mut db = Database::new();
    let args = [bulk(key), bulk(element)];
    for _ in 0..len {
        moon::command::list::lpush(&mut db, &args);
    }
    db
}

/// A listpack list of [`LEN`] elements whose buffer has ALREADY been grown to
/// hold 120, so no window below can catch a `Vec` reallocation.
///
/// Nothing in moon shrinks a listpack's buffer on removal, which is what makes
/// this work: push it wide, drain it back, keep the capacity.
fn preallocated_listpack(key: &str, element: &str) -> Database {
    let mut db = seeded(key, element, 120);
    let pop = [bulk(key)];
    for _ in 0..(120 - LEN) {
        moon::command::list::lpop(&mut db, &pop);
    }
    db
}

#[test]
fn popping_one_listpack_element_allocates_once() {
    // Every fixture, key and element is built OUTSIDE the measured windows —
    // `format!` and `Bytes::from(String)` both allocate, and that is the
    // instrument working, not noise it should have to tolerate.
    const KEY: &str = "list:000000000042";
    const ELEM: &str = "xxxxxxxx";
    let push = [bulk(KEY), bulk(ELEM)];
    let pop = [bulk(KEY)];

    // Each window is a PUSH/POP PAIR, so the list's length — and therefore
    // the listpack buffer's capacity — is stationary and `Vec` never has a
    // reason to reallocate inside one. The push half is measured on its own
    // first and asserted at zero, so a pair's count is the POP's count.
    //
    // Draining instead of pairing is how the first cut of this test came out
    // wrong: a window longer than the list spends most of its iterations
    // answering `Frame::Null` off an EMPTY key, which allocates nothing and
    // reads as a pass.

    // ── the LISTPACK arm ────────────────────────────────────────────────
    let mut db = preallocated_listpack(KEY, ELEM);
    // One of each outside the window: anything lazily initialised on a first
    // call is paid for before the counter is read.
    moon::command::list::lpush(&mut db, &push);
    moon::command::list::lpop(&mut db, &pop);

    let mark = allocs();
    for _ in 0..ROUNDS {
        moon::command::list::lpush(&mut db, &push);
    }
    let listpack_push = allocs() - mark;
    for _ in 0..ROUNDS {
        moon::command::list::lpop(&mut db, &pop);
    }

    let mark = allocs();
    for _ in 0..ROUNDS {
        moon::command::list::lpush(&mut db, &push);
        assert!(matches!(
            moon::command::list::lpop(&mut db, &pop),
            Frame::BulkString(_)
        ));
    }
    let listpack_lpop = allocs() - mark - listpack_push;

    let mark = allocs();
    for _ in 0..ROUNDS {
        moon::command::list::lpush(&mut db, &push);
        assert!(matches!(
            moon::command::list::rpop(&mut db, &pop),
            Frame::BulkString(_)
        ));
    }
    let listpack_rpop = allocs() - mark - listpack_push;

    // ── the LISTPACK arm, INTEGER element ───────────────────────────────
    // The integer encoding carries no payload at all: the stored form is an
    // `i64` and the only allocation the reply needs is its decimal
    // rendering. Pinning it separately stops a fix that only covers the
    // string arm.
    const IKEY: &str = "list:000000000043";
    let ipush = [bulk(IKEY), bulk("12345")];
    let ipop = [bulk(IKEY)];
    let mut idb = preallocated_listpack(IKEY, "12345");
    moon::command::list::lpush(&mut idb, &ipush);
    moon::command::list::lpop(&mut idb, &ipop);
    let mark = allocs();
    for _ in 0..ROUNDS {
        moon::command::list::lpush(&mut idb, &ipush);
        assert!(matches!(
            moon::command::list::lpop(&mut idb, &ipop),
            Frame::BulkString(_)
        ));
    }
    let listpack_integer = allocs() - mark - listpack_push;

    // ── CONTROL A: the FULL encoding pops with no allocation at all ─────
    // A `VecDeque<Bytes>` pop is a move. This is what says the surplus above
    // belongs to the listpack decode and not to building the reply.
    let mut fdb = seeded(KEY, ELEM, 400);
    moon::command::list::lpush(&mut fdb, &push);
    moon::command::list::lpop(&mut fdb, &pop);
    let mark = allocs();
    for _ in 0..ROUNDS {
        moon::command::list::lpush(&mut fdb, &push);
        assert!(matches!(
            moon::command::list::lpop(&mut fdb, &pop),
            Frame::BulkString(_)
        ));
    }
    let full_pair = allocs() - mark;

    // ── CONTROL B: the counter can move ─────────────────────────────────
    let mark = allocs();
    for _ in 0..ROUNDS {
        std::hint::black_box(Vec::<u8>::with_capacity(64));
    }
    let control = allocs() - mark;

    println!(
        "allocations over {ROUNDS} rounds:\n\
         \x20 listpack LPUSH (baseline)  {listpack_push}\n\
         \x20 listpack LPOP  (string)    {listpack_lpop}\n\
         \x20 listpack RPOP  (string)    {listpack_rpop}\n\
         \x20 listpack LPOP  (integer)   {listpack_integer}\n\
         \x20 full     PUSH+POP pair     {full_pair}\n\
         \x20 Vec::with_capacity         {control}"
    );

    assert!(
        control >= ROUNDS,
        "the allocation counter never moved ({control} over {ROUNDS} \
         Vec::with_capacity calls) — the instrument is broken, so every \
         assertion below proves nothing"
    );
    assert_eq!(
        listpack_push, 0,
        "LPUSH onto a listpack of stationary length allocated \
         {listpack_push} times over {ROUNDS} pushes; `encode_entry` writes \
         through a stack buffer (moon#942, `tests/listpack_encode_alloc_942.rs`) \
         and the listpack's `Vec` has no reason to grow. If this is not zero \
         the pair windows below are not measuring the pop"
    );
    assert_eq!(
        full_pair, 0,
        "a PUSH/POP pair on the FULL encoding allocated {full_pair} times \
         over {ROUNDS} rounds; a `VecDeque<Bytes>` pop is a move. If this is \
         not zero the counter is picking up something other than the pop and \
         the listpack numbers mean nothing"
    );
    assert_eq!(
        listpack_lpop, ROUNDS,
        "LPOP off a listpack allocated {listpack_lpop} times over {ROUNDS} \
         pops — moon#942. The floor is ONE per pop: the reply owns its bytes \
         and the listpack buffer is mutated straight afterwards, so exactly \
         one copy is unavoidable. Two means `listpack_pop_end` is still \
         decoding through the OWNING `ListpackEntry` — `Listpack::get_at` \
         allocates the `Vec`, `ListpackEntry::to_bytes` goes through \
         `as_bytes`, whose `String` arm CLONES it — instead of through the \
         borrowed `ListpackRef` that `Listpack::iter_refs` already hands out"
    );
    assert_eq!(
        listpack_rpop, ROUNDS,
        "RPOP off a listpack allocated {listpack_rpop} times over {ROUNDS} \
         pops — the other end of the same `listpack_pop_end` (moon#942)"
    );
    assert_eq!(
        listpack_integer, ROUNDS,
        "LPOP of an INTEGER-encoded listpack element allocated \
         {listpack_integer} times over {ROUNDS} pops — moon#942"
    );
}
