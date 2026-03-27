# Phase 37: Quick-Win Optimizations — Context

**Gathered:** 2026-03-26
**Status:** Ready for planning
**Source:** .planning/OPTIMIZATION-ANALYSIS.md (OPT-1 through OPT-4)

<domain>
## Phase Boundary

4 quick-win optimizations from the Principal Architect profiling analysis. Each is low-risk, high-ROI, 1-2 day implementation.

</domain>

<decisions>
## Implementation Decisions

### OPT-1: DashTable Software Prefetch
**File:** `src/storage/dashtable/mod.rs`
**What:** Insert `_mm_prefetch` (x86) / `__prefetch` (aarch64) after segment index computation, before home bucket calculation. Overlaps prefetch latency with hash computation.
**Expected:** -8ns on cache-miss lookups
**Acceptance:** `cargo bench -- 3_dashtable_get_hit` shows improvement

### OPT-2: Branchless CompactKey Equality
**File:** `src/storage/compact_key.rs`
**What:** For inline keys (90%+), compare raw 24-byte `data` array directly (`self.data == other.data`) instead of branching to `as_bytes()` + `memcmp`.
**Expected:** -5ns for inline key comparisons
**Acceptance:** `cargo bench -- eq_inline_match` shows improvement

### OPT-3: jemalloc as Default Allocator
**Files:** `Cargo.toml`, `src/main.rs`
**What:** Change default feature from mimalloc to jemalloc. jemalloc has tighter size-class granularity (~8B metadata vs mimalloc ~32B).
**Expected:** -35B/key RSS
**Acceptance:** Profile shows <250B/key at 1M strings

### OPT-4: No-Arc Local Command Responses
**Files:** `src/shard/mod.rs`, `src/server/connection.rs`
**What:** Return `Frame` directly for local commands (95%+), `Arc<Frame>` only for cross-shard dispatch. Eliminates 2 atomic refcount ops per local command.
**Expected:** -4% CPU, ~10ns per command
**Acceptance:** `cargo bench -- 9_full_pipeline_get_256b` shows improvement

### Claude's Discretion
All implementation details at Claude's discretion. Read OPTIMIZATION-ANALYSIS.md for exact code examples.

</decisions>

<code_context>
## Existing Code Insights

### Canonical Reference
- `.planning/OPTIMIZATION-ANALYSIS.md` — **MUST READ** — contains exact code changes, before/after examples, acceptance criteria for each optimization

### Key Files
- `src/storage/dashtable/mod.rs` — get() lookup method
- `src/storage/compact_key.rs` — PartialEq implementation
- `Cargo.toml` — feature flags and default allocator
- `src/shard/mod.rs` — command dispatch, Arc<Frame> usage
- `src/server/connection.rs` — response handling

</code_context>

<specifics>
## Specific Ideas
Run `cargo bench` before and after each optimization to measure actual improvement.
</specifics>

<deferred>
## Deferred Ideas
None — all 4 optimizations are in scope.
</deferred>
