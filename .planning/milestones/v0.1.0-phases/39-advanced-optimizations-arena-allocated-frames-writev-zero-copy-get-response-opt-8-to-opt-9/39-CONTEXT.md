# Phase 39: Advanced Optimizations — Context

**Gathered:** 2026-03-26
**Status:** Ready for planning
**Source:** .planning/OPTIMIZATION-ANALYSIS.md (OPT-8 and OPT-9)

<domain>
## Phase Boundary

2 advanced optimizations: arena-allocated frames (eliminate Vec heap alloc per command) and writev zero-copy GET response (write value directly from DashTable to socket via io_uring scatter-gather).

</domain>

<decisions>
## Implementation Decisions

### OPT-8: Arena-Allocated Frames
**Files:** `src/protocol/parse.rs`, `src/protocol/frame.rs`, dispatch path
**What:** Replace `Frame::Array(Vec<Frame>)` with `ArenaFrame<'a>` using BumpVec from per-connection bumpalo arena. Arena reset after each pipeline batch (O(1) bulk dealloc).
**Expected:** -18ns per command (Vec alloc 20ns → bump alloc 2ns)
**Risk:** Medium — lifetime parameter propagation through dispatch path. BUT: can be simplified by just replacing Vec with BumpVec in the existing Frame enum using an arena reference, without full ArenaFrame rewrite.
**Simpler alternative:** Use `SmallVec<[Frame; 4]>` for Frame::Array — most Redis commands have 2-4 args, avoiding heap alloc entirely without lifetime changes. This is lower risk and captures most of the benefit.

### OPT-9: writev Zero-Copy GET Response
**Files:** `src/io/uring_driver.rs`, `src/shard/mod.rs`
**What:** For GET responses on io_uring path, use 3-iovec writev: header ($len\r\n) from stack, value data DIRECT from DashTable segment memory, trailer (\r\n) from static. The Frame::PreSerialized infrastructure from Phase 38 enables this.
**Expected:** -14ns per GET (eliminate 256B memcpy to response buffer)
**Risk:** Medium — must hold DashTable reference until CQE. WritevGuard pattern from Phase 12 handles this.

### Claude's Discretion
All implementation details at Claude's discretion. Read OPTIMIZATION-ANALYSIS.md for exact code examples. Prefer SmallVec approach for OPT-8 over full ArenaFrame if it achieves similar results with lower risk.

</decisions>

<code_context>
## Existing Code Insights

### Canonical Reference
- `.planning/OPTIMIZATION-ANALYSIS.md` — exact code changes

### Reusable Assets
- Phase 12 WritevGuard for io_uring buffer lifetime management
- Phase 8 bumpalo arena per connection (already exists, underutilized)
- Phase 38 Frame::PreSerialized variant (infrastructure ready)

### Key Files
- `src/protocol/frame.rs` — Frame::Array(Vec<Frame>)
- `src/protocol/parse.rs` — frame construction
- `src/io/uring_driver.rs` — writev support, WritevGuard
- `src/shard/mod.rs` — io_uring event handling

</code_context>

<specifics>
## Specific Ideas
Consider SmallVec<[Frame; 4]> as simpler OPT-8 alternative.
</specifics>

<deferred>
## Deferred Ideas
None — final optimization phase.
</deferred>
