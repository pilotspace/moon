# Phase 41: Eliminate double memory copies and batch optimizations (P0 hot-path) - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Eliminate all unnecessary memory copies and allocation overhead in the Monoio connection handler's hot path. These are the bottlenecks keeping single-shard pipeline throughput at 0.55× Redis instead of >1.0×.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure performance optimization phase. The fixes are precisely identified from architectural analysis:

**Fix 1 — Eliminate write-path `.to_vec()` copy (CRITICAL, ~15-25% pipeline improvement):**
- `connection.rs:3748`: `write_buf.split().to_vec()` copies entire serialized response into fresh Vec
- Fix: Replace with zero-copy approach. Options:
  - (a) `write_buf.split().freeze()` → `Bytes` which can impl `IoBuf` for monoio
  - (b) Use `Vec<u8>` directly as write buffer instead of BytesMut, avoiding split+to_vec
  - (c) Write directly from BytesMut slice using unsafe IoBuf impl
- Monoio's `write_all` requires ownership of an `IoBuf` — `Vec<u8>` already impls it, `Bytes` does too

**Fix 2 — Eliminate read-path double copy (~10-15% improvement):**
- `connection.rs:3363-3369`: Data goes kernel→tmp_buf→read_buf (two copies)
- `tmp_buf.resize(8192, 0)` zero-fills unnecessarily
- Fix: Read directly into a `Vec<u8>` that serves as the parse buffer, or impl IoBufMut wrapper for BytesMut
- Alternative: Use unsafe `set_len` after `reserve` instead of `resize` to skip zero-fill

**Fix 3 — Hoist `borrow_mut()` outside frame loop (~5-10% pipeline improvement):**
- `connection.rs:3605`: Each local command individually acquires/releases RefCell
- Fix: Acquire once before the loop, process all local commands under single borrow, drop after loop
- Must handle the case where remote commands are interleaved (process locals in batch, then remotes)

**Fix 4 — Pre-allocate `frames` Vec outside loop (~2-5%):**
- `connection.rs:3375`: `let mut frames = Vec::new();` allocated fresh every read cycle
- Fix: Move outside the main `loop {}`, use `.clear()` like responses/remote_groups

**Fix 5 — Call `refresh_now()` once per batch (~2-3%):**
- `connection.rs:3606`: Called per-command inside the frame loop
- Fix: Call once before the frame loop

**Fix 6 — Add MAX_BATCH=1024 limit (stability):**
- Monoio handler parses unlimited frames. Tokio caps at 1024.
- Fix: Add `if frames.len() >= 1024 { break; }` in parse loop

</decisions>

<code_context>
## Existing Code Insights

### Critical Files
- `src/server/connection.rs:3104-3776` — Monoio sharded handler (hot path)
- `src/server/connection.rs:1333-2344` — Tokio handler (reference for borrow_mut batching)

### Monoio Ownership I/O Model
- `monoio::io::AsyncReadRent::read(buf)` takes ownership of buf, returns `(Result, buf)`
- `monoio::io::AsyncWriteRentExt::write_all(buf)` takes ownership of buf
- `Vec<u8>` implements both `IoBuf` and `IoBufMut` natively
- `bytes::Bytes` implements `IoBuf` (read-only) — can be used for write_all
- `BytesMut` does NOT implement `IoBuf`/`IoBufMut` for monoio

### Key Insight for Write Path
`write_buf.split()` returns `BytesMut`. `.freeze()` converts to `Bytes` (zero-copy, just Arc refcount).
`Bytes` impls `monoio::buf::IoBuf` via the `bytes` feature in monoio. Check if monoio re-exports this.
If not, `.to_vec()` on `Bytes` is still a copy. The real fix may be to use `Vec<u8>` as the write buffer directly.

### Key Insight for Read Path
Instead of tmp_buf → read_buf copy, read directly into the tail of a `Vec<u8>` and parse from it.
Or: keep BytesMut as parse buffer but read into it via a raw pointer (unsafe but zero-copy).

</code_context>

<specifics>
## Specific Ideas

The single biggest win is eliminating the write-path `.to_vec()` at line 3748. For a pipeline of 16 GET 256B responses, this copies ~4.3KB per batch. At 1M batches/sec, that's 4.3GB/sec of memcpy that Redis doesn't do.

</specifics>

<deferred>
## Deferred Ideas

None — all fixes are in scope

</deferred>
