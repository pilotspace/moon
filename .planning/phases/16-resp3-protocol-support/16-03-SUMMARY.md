---
phase: 16-resp3-protocol-support
plan: 03
subsystem: protocol
tags: [resp3, client-tracking, invalidation, push-frames, client-side-caching]

# Dependency graph
requires:
  - phase: 16-01
    provides: "Frame::Push variant and RESP3 serialization"
  - phase: 16-02
    provides: "HELLO command, client_id, protocol-aware codec"
provides:
  - "TrackingTable per-shard data structure for key->client tracking"
  - "CLIENT TRACKING ON/OFF command with BCAST, NOLOOP, PREFIX, REDIRECT"
  - "Invalidation Push frame delivery on key modification"
  - "CLIENT ID command returns unique connection identifier"
affects: [17-blocking-commands, 18-streams, 19-replication]

# Tech tracking
tech-stack:
  added: []
  patterns: [per-shard-tracking-table, push-invalidation-delivery, bcast-prefix-matching]

key-files:
  created:
    - src/tracking/mod.rs
    - src/tracking/invalidation.rs
    - src/command/client.rs
  modified:
    - src/server/connection.rs
    - src/shard/mod.rs
    - src/command/mod.rs
    - src/lib.rs

key-decisions:
  - "TrackingTable uses HashMap<Bytes, Vec<(u64, bool)>> for normal mode and Vec<(u64, Bytes, bool)> for BCAST prefixes"
  - "Invalidation delivery uses try_send (non-blocking) matching pub/sub pattern"
  - "Tracking integrated into existing CLIENT subcommand handler (merged with ID/SETNAME/GETNAME from plan 16-02)"

patterns-established:
  - "Per-shard TrackingTable: Rc<RefCell> in sharded handler, Arc<Mutex> in standard handler"
  - "Push frame delivery via mpsc channel with tokio::select! arm"

requirements-completed: [ADVP-01, ADVP-02]

# Metrics
duration: 10min
completed: 2026-03-24
---

# Phase 16 Plan 03: Client Tracking Summary

**Per-shard TrackingTable with CLIENT TRACKING ON/OFF, BCAST prefix matching, NOLOOP self-skip, REDIRECT, and Push invalidation frame delivery on key modification**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-24T13:38:37Z
- **Completed:** 2026-03-24T13:49:09Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- TrackingTable data structure with normal mode (key->clients) and BCAST mode (prefix matching)
- CLIENT TRACKING ON/OFF command supporting BCAST, NOLOOP, PREFIX, REDIRECT, OPTIN, OPTOUT options
- Invalidation Push frames (Frame::Push) delivered to tracking clients on key writes
- Tracking cleanup on connection disconnect in both standard and sharded handlers
- 22 unit tests covering all tracking, invalidation, and parsing behaviors

## Task Commits

Each task was committed atomically:

1. **Task 1: TrackingTable data structure and CLIENT TRACKING command** - `0a99e31` (feat)
2. **Task 2: Wire CLIENT TRACKING into connection loops with invalidation delivery** - `4c5402e` (feat)

## Files Created/Modified
- `src/tracking/mod.rs` - TrackingTable with track_key, invalidate_key, BCAST prefix, NOLOOP, REDIRECT, untrack_all
- `src/tracking/invalidation.rs` - invalidation_push() constructs RESP3 Push frames
- `src/command/client.rs` - parse_tracking_args() for CLIENT TRACKING ON/OFF options
- `src/command/mod.rs` - Added client module declaration
- `src/lib.rs` - Added tracking module declaration
- `src/server/connection.rs` - CLIENT TRACKING intercept, key tracking on reads, invalidation on writes, Push delivery via select
- `src/shard/mod.rs` - Per-shard tracking_rc creation and passing to sharded handler

## Decisions Made
- TrackingTable uses HashMap<Bytes, Vec<(u64, bool)>> for normal mode key->client mapping with noloop flag
- BCAST prefixes stored as Vec<(u64, Bytes, bool)> for linear scan (prefix count typically small)
- Invalidation delivery uses try_send matching existing pub/sub non-blocking pattern
- Merged TRACKING into existing CLIENT subcommand handler alongside ID/SETNAME/GETNAME from plan 16-02
- TrackingTable bounded at 1M keys (max_keys field) -- eviction deferred to future optimization

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Merged duplicate CLIENT handler blocks**
- **Found during:** Task 2 (connection.rs wiring)
- **Issue:** Plan 16-02 already added CLIENT ID/SETNAME/GETNAME handler. Adding a second CLIENT block would have caused the first to always match and fall through incorrectly.
- **Fix:** Merged TRACKING into the existing CLIENT subcommand handler, added catch-all for unknown subcommands.
- **Files modified:** src/server/connection.rs
- **Verification:** cargo build succeeds, all 738 unit tests pass
- **Committed in:** 4c5402e (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Necessary merge with plan 16-02's CLIENT handler. No scope creep.

## Issues Encountered
- 5 pre-existing integration test failures from plan 16-02 (test_hash_commands, test_hscan, test_list_commands, test_aof_restore_on_startup, test_sharded_concurrent_clients) -- confirmed by stashing changes and running tests against previous commit.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Client-side caching infrastructure complete
- TrackingTable ready for future OPTIN/OPTOUT optimization
- Push frame delivery path established for future server-push features

---
*Phase: 16-resp3-protocol-support*
*Completed: 2026-03-24*
