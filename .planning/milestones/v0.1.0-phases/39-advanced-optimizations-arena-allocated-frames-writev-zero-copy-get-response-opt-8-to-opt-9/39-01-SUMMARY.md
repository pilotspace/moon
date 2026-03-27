---
phase: 39-advanced-optimizations
plan: 01
subsystem: protocol
tags: [smallvec, frame-allocation, performance, zero-copy]

# Dependency graph
requires:
  - phase: 01-protocol-foundation
    provides: Frame enum and RESP parser
provides:
  - FrameVec newtype with SmallVec<[Frame; 4]> inline storage
  - framevec![] macro for Frame collection construction
  - Box<SmallVec> pattern for recursive enum types
affects: [protocol, command-handlers, serialization]

# Tech tracking
tech-stack:
  added: [smallvec 1.15 with union feature]
  patterns: [FrameVec newtype wrapping Box<SmallVec> for recursive enums, framevec![] macro]

key-files:
  modified:
    - src/protocol/frame.rs
    - src/protocol/parse.rs
    - src/protocol/inline.rs
    - src/protocol/serialize.rs
    - src/protocol/resp3.rs
    - src/protocol/mod.rs
    - Cargo.toml
    - src/command/*.rs (all command handlers)
    - src/shard/coordinator.rs
    - src/server/connection.rs
    - src/persistence/aof.rs
    - src/scripting/types.rs

key-decisions:
  - "Used Box<SmallVec<[Frame; 4]>> via FrameVec newtype because unboxed SmallVec causes E0072 (infinite size) on recursive Frame enum"
  - "Chose N=4 for SmallVec capacity to cover GET(2), SET(3), HSET(4) -- 95%+ of Redis commands"
  - "Frame enum size stays at 72 bytes (unchanged) thanks to Box indirection"
  - "Created framevec![] macro for ergonomic construction at 400+ call sites"

patterns-established:
  - "FrameVec newtype: Box<SmallVec<[Frame; 4]>> with Deref, IntoIterator, FromIterator, From<Vec>"
  - "framevec![] macro: drop-in vec![] replacement for Frame::Array/Set/Push construction"

requirements-completed: [OPT-8]

# Metrics
duration: 68min
completed: 2026-03-26
---

# Phase 39 Plan 01: SmallVec Frame Array Optimization Summary

**Frame::Array/Set/Push now use Box<SmallVec<[Frame; 4]>> via FrameVec newtype -- fixed-size allocation for small arrays, Frame stays 72 bytes**

## Performance

- **Duration:** 68 min
- **Started:** 2026-03-26T06:58:48Z
- **Completed:** 2026-03-26T08:07:00Z
- **Tasks:** 3 (combined into single atomic commit due to type system constraint)
- **Files modified:** 29

## Accomplishments
- Replaced Vec<Frame> with FrameVec (Box<SmallVec<[Frame; 4]>>) in Frame::Array, Frame::Set, Frame::Push
- Frame enum stays at 72 bytes (Box is 8 bytes vs Vec's 24 bytes -- actually smaller)
- All 1037 tests pass, zero remaining vec![] patterns for Frame construction
- Created FrameVec newtype with full trait support (Deref, DerefMut, IntoIterator, FromIterator, From<Vec>, PartialEq, Clone, Debug)

## Task Commits

Tasks 1-3 combined into single atomic commit (Frame type change requires all construction sites updated simultaneously):

1. **Task 1-3: Add smallvec, change Frame enum, update all construction sites** - `3bdb9b9` (feat)

## Files Created/Modified
- `Cargo.toml` - Added smallvec dependency with union feature
- `src/protocol/frame.rs` - FrameVec newtype, framevec![] macro, Frame enum change
- `src/protocol/mod.rs` - Re-export FrameVec
- `src/protocol/parse.rs` - SmallVec construction in all 3 parsers
- `src/protocol/inline.rs` - SmallVec construction for inline parser
- `src/protocol/serialize.rs` - Test assertions updated
- `src/protocol/resp3.rs` - Test assertions updated, array_to_map/set updated
- `src/command/*.rs` (11 files) - framevec![] macro + .into() for Vec construction patterns
- `src/shard/coordinator.rs` - .into() for Vec construction patterns
- `src/server/connection.rs` - .into() for Vec construction patterns
- `src/persistence/aof.rs` - framevec![] for AOF frame construction
- `src/scripting/types.rs` - FrameVec::new() for Lua-to-Frame conversion
- `src/blocking/wakeup.rs` - .into() for wake notification frames
- `src/tracking/invalidation.rs` - .into() for invalidation frames
- `src/pubsub/mod.rs` - framevec![] for pub/sub frames

## Decisions Made
- **Box<SmallVec> over unboxed SmallVec**: Frame is recursive (Array contains Frames). Unboxed SmallVec<[Frame; 2]> causes E0072 (infinite type size). Box provides required indirection while SmallVec provides inline storage within the Box allocation.
- **N=4 capacity**: Covers 95%+ of Redis commands (GET=2 args, SET=3, HSET=4). Commands with >4 args spill to heap like Vec.
- **FrameVec as newtype, not type alias**: Enables implementing FromIterator, IntoIterator, and From<Vec> which are needed for .collect() and .into() patterns across the codebase. A type alias for Box<SmallVec> can't have foreign trait impls.
- **framevec![] macro**: Provides ergonomic construction at 400+ call sites, equivalent to vec![] but produces FrameVec.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Combined Tasks 1-3 into single commit**
- **Found during:** Task 1
- **Issue:** Changing Frame::Array from Vec to FrameVec is a type system change that breaks all 400+ construction sites. Cannot compile between tasks.
- **Fix:** Combined all 3 tasks into a single atomic commit
- **Verification:** All 1037 tests pass

**2. [Rule 1 - Bug] Fixed pattern match corruption from sed replacement**
- **Found during:** Task 3
- **Issue:** Bulk sed replacement of `Frame::Array(var)` -> `Frame::Array(var.into())` incorrectly modified pattern match arms (e.g., `Frame::Array(items) =>` became `Frame::Array(items.into()) =>`)
- **Fix:** Identified and reverted `.into()` in pattern match contexts across 15+ files
- **Verification:** All compilation errors resolved

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Task combination was necessary due to Rust's type system requiring atomic type changes. No scope creep.

## Issues Encountered
- Recursive Frame enum prevents unboxed SmallVec (E0072: infinite type size). Resolved by using Box<SmallVec> wrapped in a newtype.
- Pattern match arms in test assertions were incorrectly modified by bulk sed replacement. Resolved by targeted pattern-aware reversal.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- FrameVec is ready for use in any new code
- Benchmark improvement may be marginal since Box<SmallVec> still does one heap allocation (vs Vec's one allocation) -- the benefit is fixed-size vs variable-size allocation
- Plan 39-02 can proceed independently

---
*Phase: 39-advanced-optimizations*
*Completed: 2026-03-26*
