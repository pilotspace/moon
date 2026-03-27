# Phase 38: Core Optimizations — Context

**Gathered:** 2026-03-26
**Status:** Ready for planning
**Source:** .planning/OPTIMIZATION-ANALYSIS.md (OPT-5 through OPT-7)

<domain>
## Phase Boundary

3 core optimizations from the Principal Architect analysis. These are medium-risk, high-impact changes targeting the value extraction bottleneck (54ns) and memory overhead (280B/key).

</domain>

<decisions>
## Implementation Decisions

### OPT-5: Direct GET Serialization (Skip Frame Intermediary)
**Files:** `src/command/string.rs`, `src/server/connection.rs`
**What:** For GET responses, serialize directly from `CompactValue` to output buffer. Skip `as_bytes_owned()` → `Frame::BulkString` → `serialize()`. Borrow value from DashTable during serialization (safe: single-threaded, hold borrow_mut for entire dispatch-serialize phase).
**Expected:** -24ns per GET response (54→30ns)
**Acceptance:** `cargo bench -- 9_full_pipeline_get_256b` drops below 140ns

### OPT-6: Zero-Copy Argument Slicing
**File:** `src/protocol/parse.rs`
**What:** Use `buf.split_to(len).freeze()` (Arc bump, ~5ns) for arguments ≥10 bytes instead of `Bytes::copy_from_slice` (~18ns). Keep copy for small command names (<10B).
**Expected:** -13ns per key/value argument
**Acceptance:** `cargo bench -- 1_resp_parse_get_cmd` drops below 65ns

### OPT-7: Slab Segment Allocator
**File:** `src/storage/dashtable/mod.rs`
**What:** Pre-allocate segments in contiguous Vec slabs instead of individual heap allocations. Eliminates per-segment allocator metadata.
**Expected:** -25B/key RSS
**Acceptance:** Profile shows <230B/key at 1M with jemalloc+slab

### Claude's Discretion
All implementation details at Claude's discretion. Read OPTIMIZATION-ANALYSIS.md for exact code examples.

</decisions>

<code_context>
## Existing Code Insights

### Canonical Reference
- `.planning/OPTIMIZATION-ANALYSIS.md` — **MUST READ** — contains exact code changes

### Key Files
- `src/command/string.rs` — `get()` function returns Frame
- `src/protocol/parse.rs` — `parse_bulk_string()` uses copy_from_slice
- `src/storage/dashtable/mod.rs` — segment allocation in split/insert

</code_context>

<specifics>
## Specific Ideas
OPT-5 and OPT-6 are independent. OPT-7 is independent. All 3 can run in parallel.
</specifics>

<deferred>
## Deferred Ideas
None.
</deferred>
