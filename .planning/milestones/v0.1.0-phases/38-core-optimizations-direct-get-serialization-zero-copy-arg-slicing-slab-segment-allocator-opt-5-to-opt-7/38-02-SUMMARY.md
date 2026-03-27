---
phase: 38-core-optimizations
plan: 02
subsystem: protocol
tags: [resp, serialization, performance, frame, preserialized]

requires:
  - phase: 01-protocol-foundation
    provides: Frame enum and RESP serialize functions

provides:
  - Frame::PreSerialized(Bytes) variant for direct RESP output
  - Cross-variant PartialEq between PreSerialized and BulkString
  - Scripting bridge PreSerialized payload extraction
  - Infrastructure for future writev zero-copy (OPT-9)

affects: [38-core-optimizations, protocol, serialization, scripting]

tech-stack:
  added: []
  patterns: [PreSerialized RESP bypass pattern]

key-files:
  created: []
  modified:
    - src/protocol/frame.rs
    - src/protocol/serialize.rs
    - src/scripting/types.rs

key-decisions:
  - "Keep GET on BulkString path: PreSerialized adds BytesMut alloc (30ns) that exceeds serialize savings (4ns match+itoa)"
  - "as_bytes_owned() is 4ns Arc bump for heap strings, not 18ns as estimated in analysis"
  - "PreSerialized infrastructure retained for future OPT-9 writev zero-copy"

patterns-established:
  - "PreSerialized bypass: Frame::PreSerialized(Bytes) skips serialize match, codec writes directly"
  - "Cross-variant PartialEq: preserialized_eq_bulk_string extracts payload from wire format for comparison"

requirements-completed: [OPT-5]

duration: 57min
completed: 2026-03-26
---

# Phase 38 Plan 02: Direct GET Serialization Summary

**Frame::PreSerialized(Bytes) variant added for direct RESP output bypass; GET hot path retained on BulkString after benchmarks proved alloc cost (30ns) exceeds serialize savings (4ns)**

## Performance

- **Duration:** 57 min
- **Started:** 2026-03-26T05:51:57Z
- **Completed:** 2026-03-26T06:48:55Z
- **Tasks:** 1
- **Files modified:** 3

## Accomplishments
- Added Frame::PreSerialized(Bytes) variant that bypasses serialize() match and writes directly to output
- Added cross-variant PartialEq so PreSerialized("$5\r\nhello\r\n") == BulkString("hello")
- Added PreSerialized handler in scripting bridge frame_to_lua_value for Lua script compatibility
- Conducted thorough benchmark analysis proving current architecture is optimal for 256B values
- Infrastructure in place for future OPT-9 writev zero-copy optimization

## Benchmark Analysis

| Stage | Measured | Analysis Notes |
|-------|----------|----------------|
| DashTable lookup | 50ns | Immovable baseline |
| as_bytes_owned | 4ns | Arc bump only (not 18ns as estimated) |
| Frame construct | ~1ns | Trivial |
| serialize (256B) | 15ns | Match+itoa+memcpy, memcpy dominates |
| Full pipeline | 159ns | No regression from 170ns baseline |

**Why PreSerialized didn't help for GET:**
- PreSerialized requires BytesMut alloc (~30ns) + memcpy data + freeze
- It saves only match+itoa in serialize (~4ns)
- Net: 26ns slower per GET response
- The profiling report's "54.5ns as_bytes_owned" actually included db.get() (50ns)
- Actual as_bytes_owned standalone = 4ns Arc bump for heap strings

**Result:** GET responses stay on BulkString. PreSerialized infrastructure available for OPT-9 writev.

## Task Commits

1. **Task 1: Create PreSerialized variant + GET analysis** - `a232409` (feat) - infrastructure committed as part of 38-01 execution

**Note:** The PreSerialized infrastructure was committed atomically with 38-01 zero-copy parsing due to execution overlap. All changes verified independently.

## Files Created/Modified
- `src/protocol/frame.rs` - Added PreSerialized(Bytes) variant, preserialized_eq_bulk_string, cross-variant PartialEq
- `src/protocol/serialize.rs` - Added PreSerialized branch in serialize() and serialize_resp3()
- `src/scripting/types.rs` - Added PreSerialized handler in frame_to_lua_value for Lua scripts

## Decisions Made
- **Keep GET on BulkString:** Benchmark-driven decision. PreSerialized adds 30ns alloc per response vs 4ns saved from skipping serialize match. Net negative for 256B values.
- **as_bytes_owned is optimal:** At 4ns (Arc bump for heap strings), it's already near-optimal. The analysis report overestimated at 54.5ns by including DashTable lookup time.
- **Retain infrastructure:** PreSerialized variant stays as zero-cost infrastructure for future optimizations (writev, batch responses).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Scripting bridge PreSerialized handling**
- **Found during:** Task 1 (test verification)
- **Issue:** frame_to_lua_value catch-all returned false for PreSerialized frames from GET in Lua scripts
- **Fix:** Added explicit PreSerialized handler that extracts bulk string payload from wire format
- **Files modified:** src/scripting/types.rs
- **Verification:** test_run_script_with_redis_call passes

**2. [Rule 1 - Analysis] GET handler reverted to BulkString**
- **Found during:** Task 1 (benchmark verification)
- **Issue:** PreSerialized GET responses were 47% slower (251ns vs 170ns) due to BytesMut alloc cost
- **Fix:** Kept GET handlers on original as_bytes_owned() + BulkString path
- **Verification:** 9_full_pipeline_get_256b: 159ns (no regression)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** GET optimization deferred to OPT-9 writev approach which can avoid alloc entirely. Infrastructure correctly in place.

## Issues Encountered
- Benchmark showed PreSerialized approach is net-negative for 256B values. Root cause: BytesMut::with_capacity + freeze costs ~30ns allocation overhead, but serialize match+itoa savings are only ~4ns. The fundamental constraint is that Frame must be owned (goes through codec), so any pre-serialization adds an allocation.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- PreSerialized variant available for OPT-9 writev zero-copy
- Frame enum, serialize, and scripting all handle PreSerialized correctly
- No regression to existing performance (159ns full pipeline, stable)

---
*Phase: 38-core-optimizations*
*Completed: 2026-03-26*
