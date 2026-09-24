//! moon#1187: building one AOF / replication record costs ONE allocation.
//!
//! Every durable write is serialized by `serialize_effect_for_log` on the
//! shard thread that executed it. At HEAD `935c555` a plain `SET k v` paid
//! three allocations for that (the expire rewrite cloned every argument into
//! a `Vec` it then discarded, the record buffer started at 64 bytes, and a
//! `BytesMut` frozen with spare capacity costs `Bytes` a second, shared
//! header), and `SET k v EX 100` about six (the `Vec`, two copied literals,
//! the `FrameVec` box, the buffer and its growth). The floor is one: the
//! record's own bytes. This file pins that floor for the no-rewrite path and
//! the rewrite path alike, plus the bytes themselves.
//!
//! The instrument is a counting `GlobalAlloc` around `System` — the same
//! shape as `tests/incrbyfloat_alloc_942.rs` — with a THREAD-LOCAL counter
//! (`const`-initialized, so reading it never allocates) so the tests in this
//! file can run concurrently without seeing each other's allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::Bytes;
use moon::persistence::aof::serialize_effect_for_log;
use moon::protocol::{Frame, FrameVec};

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: every method forwards to `System` with the caller's own layout
// unchanged, so `System`'s contract is upheld verbatim; the counter is a
// const-initialized thread-local `Cell` that never allocates, so there is no
// re-entrancy.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        // SAFETY: same layout the caller passed; `System::alloc` has no
        // precondition beyond a non-zero-size layout, which `GlobalAlloc`'s
        // caller contract already guarantees.
        unsafe { System.alloc(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        // SAFETY: `ptr`/`layout` are the pair the caller obtained from this
        // allocator, and `new_size` is the caller's, forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
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
    ALLOCS.with(Cell::get)
}

fn cmd(parts: &[&[u8]]) -> Frame {
    Frame::Array(FrameVec::from_vec(
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
            .collect(),
    ))
}

/// Allocations of one `serialize_effect_for_log` call, and its output.
fn measure(frame: &Frame, reply: &Frame) -> (usize, Vec<Bytes>) {
    // Warm the thread-local clock and anything lazily initialised.
    drop(serialize_effect_for_log(frame, reply));
    let before = allocs();
    let out = serialize_effect_for_log(frame, reply);
    let n = allocs() - before;
    (n, out.into_iter().collect())
}

fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// The no-rewrite path — `SET k v`, `HSET`, `SET … KEEPTTL`, an absolute
/// `PXAT` — is one allocation per record (HEAD: 3).
#[test]
fn verbatim_record_costs_one_allocation() {
    for (frame, reply, wire) in [
        (
            cmd(&[b"SET", b"key:000001", b"value"]),
            ok(),
            &b"*3\r\n$3\r\nSET\r\n$10\r\nkey:000001\r\n$5\r\nvalue\r\n"[..],
        ),
        (
            cmd(&[b"HSET", b"h", b"field", b"v"]),
            Frame::Integer(1),
            &b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$5\r\nfield\r\n$1\r\nv\r\n"[..],
        ),
        (
            cmd(&[b"SET", b"k", b"v", b"KEEPTTL"]),
            ok(),
            &b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$7\r\nKEEPTTL\r\n"[..],
        ),
        (
            cmd(&[b"SET", b"k", b"v", b"PXAT", b"9999999999999"]),
            ok(),
            &b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPXAT\r\n$13\r\n9999999999999\r\n"[..],
        ),
    ] {
        let (n, out) = measure(&frame, &reply);
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][..], wire, "bytes for {frame:?}");
        assert_eq!(n, 1, "allocations for {frame:?}");
    }
    // A value larger than any small-buffer guess: still exactly one.
    let big = vec![b'z'; 100_000];
    let frame = cmd(&[b"SET", b"big", &big]);
    let (n, out) = measure(&frame, &ok());
    assert_eq!(out[0].len(), 4 + 9 + 9 + 9 + 100_000 + 2);
    assert_eq!(n, 1, "large value");
}

/// The expire-rewrite path serializes the absolute form straight from the
/// borrowed arguments: one allocation (HEAD: ~6), bytes as before.
#[test]
fn relative_expiry_rewrite_costs_one_allocation() {
    // Outside a shard event loop the clock can tick between the test's read
    // and the encoder's: build the expectation from the deadline the encoder
    // chose, after checking it lies in [before, after] + ttl.
    let before = moon::storage::entry::current_time_ms();
    for (frame, expected) in [
        (
            cmd(&[b"SET", b"k", b"v", b"EX", b"100"]),
            ["SET", "k", "v", "PXAT", "{ABS:100000}"].join("|"),
        ),
        (
            cmd(&[b"SET", b"k", b"v", b"NX", b"PX", b"250", b"GET"]),
            ["SET", "k", "v", "NX", "PXAT", "{ABS:250}", "GET"].join("|"),
        ),
        (
            cmd(&[b"SETEX", b"k", b"100", b"v"]),
            ["SET", "k", "v", "PXAT", "{ABS:100000}"].join("|"),
        ),
        (
            cmd(&[b"PSETEX", b"k", b"500", b"v"]),
            ["SET", "k", "v", "PXAT", "{ABS:500}"].join("|"),
        ),
        (
            cmd(&[b"GETEX", b"k", b"EX", b"7"]),
            ["PEXPIREAT", "k", "{ABS:7000}"].join("|"),
        ),
    ] {
        let reply = if matches!(&frame, Frame::Array(a) if a.len() == 4 && matches!(&a[0], Frame::BulkString(b) if b.as_ref() == b"GETEX"))
        {
            Frame::BulkString(Bytes::from_static(b"v"))
        } else {
            ok()
        };
        let (n, out) = measure(&frame, &reply);
        let after = moon::storage::entry::current_time_ms();
        assert_eq!(out.len(), 1);
        let got = decode(&out[0]);
        // Substitute the deadline the encoder chose once it is proven in range.
        let (head, tail) = expected.split_once("{ABS:").expect("placeholder");
        let (ttl, rest) = tail.split_once('}').expect("placeholder end");
        let ttl: u64 = ttl.parse().unwrap();
        let abs_str = &got[head.len()..got.len() - rest.len()];
        let abs: u64 = abs_str.parse().expect("deadline");
        assert!(
            abs >= before + ttl && abs <= after + ttl,
            "deadline {abs} outside [{}, {}]",
            before + ttl,
            after + ttl
        );
        assert_eq!(got, format!("{head}{abs}{rest}"), "rewrite of {frame:?}");
        assert_eq!(n, 1, "allocations for {frame:?}");
    }
}

/// Reply-gated effect rewrites (`EXPIRE` → `PEXPIREAT`) still build a frame,
/// but no longer copy their literals or grow their buffer: ratchet at the
/// new count (HEAD: 6).
#[test]
fn effect_rewrite_stays_within_budget() {
    let frame = cmd(&[b"EXPIRE", b"k", b"100"]);
    let (n, out) = measure(&frame, &Frame::Integer(1));
    assert_eq!(out.len(), 1);
    assert!(decode(&out[0]).starts_with("PEXPIREAT|k|"));
    assert!(n <= 4, "EXPIRE effect rewrite allocations: {n}");
}

/// Minimal RESP array-of-bulk decoder for assertions: `a|b|c`.
fn decode(wire: &[u8]) -> String {
    let s = std::str::from_utf8(wire).expect("utf8");
    let mut lines = s.split("\r\n");
    let head = lines.next().expect("header");
    let n: usize = head[1..].parse().expect("count");
    let mut parts = Vec::with_capacity(n);
    for _ in 0..n {
        let _len = lines.next().expect("bulk len");
        parts.push(lines.next().expect("bulk").to_string());
    }
    parts.join("|")
}
