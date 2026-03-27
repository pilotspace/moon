---
phase: 01-protocol-foundation
plan: 02
subsystem: protocol
tags: [resp2, inline-commands, criterion, benchmarks, parsing]

# Dependency graph
requires:
  - phase: 01-protocol-foundation/01-01
    provides: "Frame enum, ParseConfig, ParseError, parse(), serialize()"
provides:
  - "Inline command parser (parse_inline) for telnet/redis-cli plain text input"
  - "First-byte dispatch in parse() routing RESP vs inline"
  - "Criterion benchmark baselines for RESP2 parse throughput"
affects: [02-networking, 03-core-commands]

# Tech tracking
tech-stack:
  added: [criterion]
  patterns: [first-byte-dispatch, whitespace-split-inline-parsing]

key-files:
  created:
    - src/protocol/inline.rs
    - benches/resp_parsing.rs
  modified:
    - src/protocol/parse.rs
    - src/protocol/mod.rs
    - Cargo.toml

key-decisions:
  - "Inline tokens split on spaces and tabs with empty-slice filtering for multi-space collapse"
  - "Non-RESP prefix bytes route to inline parser; RESP prefixes (+, -, :, $, *) route to RESP parser"
  - "Bytes::copy_from_slice for inline token extraction (consistent with RESP parser)"

patterns-established:
  - "First-byte dispatch: parse() checks buf[0] against RESP prefix set before delegating"
  - "Inline parser returns Ok(None) for empty/whitespace-only lines (no frame produced)"

requirements-completed: [PROTO-02]

# Metrics
duration: 4min
completed: 2026-03-23
---

# Phase 1 Plan 2: Inline Commands and Benchmarks Summary

**Inline command parser with whitespace-split tokenization, first-byte dispatch in parse(), and criterion benchmarks establishing ~82ns inline / ~128ns array parse baselines**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-23T05:46:15Z
- **Completed:** 2026-03-23T05:50:38Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Inline command parser handles plain text commands (PING, SET key value), multiple spaces, tabs, leading whitespace, and empty lines
- Public parse() dispatches to inline or RESP parser based on first byte prefix
- 7 criterion benchmarks established throughput baselines: simple_string ~46ns, bulk_string ~59ns, integer ~41ns, array_3elem ~128ns, inline ~82ns, serialize ~111ns, roundtrip ~228ns
- 65 total unit tests pass (14 new: 11 inline + 3 integration dispatch)

## Task Commits

Each task was committed atomically:

1. **Task 1: Inline command parser (RED)** - `51979c3` (test)
2. **Task 1: Inline command parser (GREEN)** - `a0394fa` (feat)
3. **Task 2: Criterion benchmarks** - `44aa0dc` (feat)

_Note: Task 1 used TDD with RED/GREEN commits. No refactor needed._

## Files Created/Modified
- `src/protocol/inline.rs` - Inline command parser with parse_inline() and find_crlf_position()
- `src/protocol/parse.rs` - First-byte dispatch added to parse(), integration tests for inline routing
- `src/protocol/mod.rs` - Registered inline module and re-exported parse_inline
- `benches/resp_parsing.rs` - 7 criterion benchmarks for parse throughput baseline
- `Cargo.toml` - Added [[bench]] section for criterion

## Decisions Made
- Used byte slice split with empty-filter pattern for whitespace tokenization (simpler than manual state machine, handles all edge cases)
- Non-RESP prefix bytes route to inline parser -- this means bytes like `!` or `0` are treated as inline commands, which matches Redis server behavior
- Used `std::hint::black_box` instead of deprecated `criterion::black_box`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated test_parse_invalid_type_byte for inline dispatch**
- **Found during:** Task 1 (GREEN phase)
- **Issue:** Existing test expected `ParseError::Invalid` for `!foo\r\n`, but with inline dispatch this is now a valid inline command
- **Fix:** Renamed test to `test_parse_non_resp_prefix_routes_to_inline` and changed assertion to expect `Frame::Array([BulkString("!foo")])`
- **Files modified:** src/protocol/parse.rs
- **Verification:** All 65 tests pass
- **Committed in:** a0394fa (Task 1 GREEN commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Test expectation corrected to match new dispatch semantics. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Complete RESP2 protocol library with both RESP-framed and inline command support
- Performance baselines established for tracking regressions in future phases
- Ready for Phase 2 networking integration (tokio-util codec wrapping parse/serialize)

---
*Phase: 01-protocol-foundation*
*Completed: 2026-03-23*
