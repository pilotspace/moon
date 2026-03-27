---
phase: 37-quick-win-optimizations
plan: 01
subsystem: storage
tags: [prefetch, simd, dashtable, compact-key, cache-optimization, branchless]

requires:
  - phase: 10-dashtable
    provides: DashTable segmented hash table with SIMD probing
  - phase: 11-compact-key
    provides: CompactKey 24-byte SSO key representation
provides:
  - Software prefetch hints in DashTable get/get_mut/insert/remove/remove_entry
  - Branchless inline PartialEq for CompactKey (3 QWORD compare)
affects: [storage, dashtable, compact-key, benchmarks]

tech-stack:
  added: []
  patterns: [software-prefetch-before-hash-computation, branchless-inline-comparison]

key-files:
  created: []
  modified:
    - src/storage/dashtable/mod.rs
    - src/storage/compact_key.rs

key-decisions:
  - "Used inline asm (prfm pldl1keep) for aarch64 prefetch instead of unstable core::arch::aarch64::_prefetch intrinsic"
  - "Prefetch targets dereferenced Segment data (via &**Box), not Box pointer itself"

patterns-established:
  - "Prefetch pattern: insert prefetch_segment() between segment index lookup and h2/home_bucket computation"
  - "Branchless equality: OR both discriminant bytes, mask with flag, single branch for fast path"

requirements-completed: [OPT-1, OPT-2]

duration: 4min
completed: 2026-03-26
---

# Phase 37 Plan 01: DashTable Prefetch + Branchless CompactKey Equality Summary

**Software prefetch in DashTable lookup path (-3.4% latency) and branchless 24-byte array compare for inline CompactKey equality (802ps per comparison)**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-26T05:06:16Z
- **Completed:** 2026-03-26T05:10:16Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Added software prefetch hints to all 6 DashTable lookup methods (get, get_mut, contains_key, insert, remove, remove_entry)
- Replaced CompactKey PartialEq with branchless fast path that compiles to 3 QWORD comparisons for inline keys
- DashTable get_hit benchmark improved 3.4% (from ~53ns to ~51ns)
- CompactKey eq_inline_match benchmark: 802 picoseconds per comparison

## Task Commits

Each task was committed atomically:

1. **Task 1: DashTable software prefetch on segment lookup (OPT-1)** - `45b39e5` (perf)
2. **Task 2: Branchless CompactKey inline equality (OPT-2)** - `63caaea` (perf)

## Files Created/Modified
- `src/storage/dashtable/mod.rs` - Added prefetch_segment() function and calls in get/get_mut/insert/remove/remove_entry; uses x86_64 _mm_prefetch and aarch64 prfm inline asm
- `src/storage/compact_key.rs` - Replaced PartialEq with branchless fast path comparing raw 24-byte data arrays for inline keys

## Decisions Made
- Used `core::arch::asm!("prfm pldl1keep, [{ptr}]")` for aarch64 instead of unstable `core::arch::aarch64::_prefetch` intrinsic (requires nightly `stdarch_aarch64_prefetch` feature gate)
- Prefetch function takes `&Segment<K, V>` reference (dereferenced Box) to prefetch actual segment data, not the Box's stack pointer
- Used `#[inline(always)]` on both prefetch_segment and PartialEq::eq to ensure zero call overhead

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] aarch64 __prefetch intrinsic unavailable on stable Rust**
- **Found during:** Task 1 (DashTable prefetch implementation)
- **Issue:** Plan specified `core::arch::aarch64::__prefetch()` but this is `_prefetch` (single underscore) and requires unstable feature `stdarch_aarch64_prefetch`
- **Fix:** Used inline assembly `core::arch::asm!("prfm pldl1keep, [{ptr}]")` which works on stable Rust
- **Files modified:** src/storage/dashtable/mod.rs
- **Verification:** cargo test passes, benchmark runs successfully
- **Committed in:** 45b39e5 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Essential fix for stable Rust compatibility. No scope creep.

## Issues Encountered
None beyond the aarch64 intrinsic deviation documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Storage layer prefetch and branchless equality optimizations complete
- Ready for OPT-3 (jemalloc default) and OPT-4 (no-Arc local responses) in plan 37-02

---
*Phase: 37-quick-win-optimizations*
*Completed: 2026-03-26*
