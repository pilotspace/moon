---
phase: 02-working-server
plan: 01
subsystem: server
tags: [tokio, tcp, codec, clap, tracing, resp2, storage]

# Dependency graph
requires:
  - phase: 01-protocol-foundation
    provides: RESP2 parser/serializer (protocol::parse, protocol::serialize, Frame, ParseConfig)
provides:
  - Storage engine (Database, Entry, RedisValue) with lazy expiration
  - RESP codec wrapping Phase 1 parser as tokio-util Decoder/Encoder
  - TCP server with per-connection task spawning and graceful shutdown
  - Command dispatch with PING/ECHO/QUIT/SELECT/COMMAND/INFO handlers
  - ServerConfig with CLI args via clap
  - DispatchResult enum for connection lifecycle signaling
affects: [02-02, 02-03, 02-04, 03-data-structures]

# Tech tracking
tech-stack:
  added: [tokio, tokio-util, clap, tracing, tracing-subscriber, anyhow, futures, redis (dev)]
  patterns: [tokio-util Framed codec, Arc<Mutex<Vec<Database>>> shared state, CancellationToken shutdown, pure function command handlers]

key-files:
  created:
    - src/config.rs
    - src/storage/mod.rs
    - src/storage/entry.rs
    - src/storage/db.rs
    - src/server/mod.rs
    - src/server/codec.rs
    - src/server/connection.rs
    - src/server/listener.rs
    - src/server/shutdown.rs
    - src/command/mod.rs
    - src/command/connection.rs
  modified:
    - Cargo.toml
    - src/lib.rs
    - src/main.rs

key-decisions:
  - "Used futures::StreamExt/SinkExt for Framed stream/sink traits (tokio_stream not in deps)"
  - "DispatchResult enum with Response/Quit variants for clean QUIT connection lifecycle"
  - "Database::check_expired as static method to avoid borrow conflicts in get/exists"
  - "Pre-compute db_count before mutable borrow to satisfy borrow checker in connection handler"

patterns-established:
  - "Command handlers are pure functions: fn handler(args: &[Frame]) -> Frame"
  - "Dispatch via match on uppercase command name bytes"
  - "Lazy expiration checked on every key access (get, exists)"
  - "Per-connection tokio::select! loop with shutdown cancellation"

requirements-completed: [PROTO-03, NET-01, NET-02, NET-03, CONN-01, CONN-02, CONN-03, CONN-04, CONN-05, CONN-07]

# Metrics
duration: 5min
completed: 2026-03-23
---

# Phase 2 Plan 1: Server Foundation Summary

**TCP server with RESP codec, storage engine, graceful shutdown, and PING/ECHO/SELECT/COMMAND/INFO command handlers -- redis-cli connectable**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-23T06:40:08Z
- **Completed:** 2026-03-23T06:44:54Z
- **Tasks:** 3
- **Files modified:** 14

## Accomplishments
- Storage engine with Database/Entry/RedisValue types, lazy expiration, and 10 unit tests
- RESP codec wrapping Phase 1 parser/serializer as tokio-util Decoder/Encoder with 6 tests
- TCP server accepting concurrent connections with per-connection tasks and Ctrl+C graceful shutdown
- Command dispatch routing 6 commands (PING, ECHO, QUIT, SELECT, COMMAND, INFO) with 20 tests
- 103 total library tests passing

## Task Commits

Each task was committed atomically:

1. **Task 1: Dependencies, storage engine, entry types, and config** - `e818436` (feat)
2. **Task 2: RESP codec, server listener, connection handler, and graceful shutdown** - `4924631` (feat)
3. **Task 3: Command dispatch and connection command handlers** - `74419a1` (feat)

## Files Created/Modified
- `Cargo.toml` - Added tokio, tokio-util, clap, tracing, anyhow, futures, redis (dev) dependencies
- `src/config.rs` - ServerConfig with bind/port/databases CLI args
- `src/storage/entry.rs` - Entry struct with RedisValue enum, expiration metadata, constructors
- `src/storage/db.rs` - Database with HashMap, lazy expiry, get/set/remove/exists/len/keys/expires_count
- `src/storage/mod.rs` - Storage module re-exports
- `src/server/codec.rs` - RespCodec implementing Decoder and Encoder wrapping protocol layer
- `src/server/connection.rs` - Per-connection handler with Framed stream, dispatch loop, shutdown
- `src/server/listener.rs` - TCP accept loop with connection spawning and signal-based shutdown
- `src/server/shutdown.rs` - CancellationToken re-export from tokio-util
- `src/server/mod.rs` - Server module re-exports
- `src/command/mod.rs` - DispatchResult enum and dispatch function matching 6 commands
- `src/command/connection.rs` - PING/ECHO/SELECT/COMMAND/INFO/QUIT handlers with 14 tests
- `src/lib.rs` - Added command, config, server, storage module declarations
- `src/main.rs` - Wired tokio runtime, tracing, clap, and server listener

## Decisions Made
- Used `futures::StreamExt/SinkExt` for Framed stream/sink traits since tokio_stream is not a direct dependency
- Created `DispatchResult` enum with `Response(Frame)` and `Quit(Frame)` variants for clean QUIT handling (cleaner than sentinel values)
- Used static method `Database::check_expired` to avoid borrow conflicts between expiry check and data removal
- Pre-compute `db_count = dbs.len()` before mutable index to satisfy Rust borrow checker in connection handler

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed tokio_stream import error**
- **Found during:** Task 2 (connection handler)
- **Issue:** Plan used `tokio_stream::StreamExt` but tokio_stream is not a direct dependency
- **Fix:** Used `futures::StreamExt` instead (futures crate already added to Cargo.toml)
- **Files modified:** src/server/connection.rs
- **Verification:** cargo build succeeds
- **Committed in:** 4924631 (Task 2 commit)

**2. [Rule 1 - Bug] Fixed borrow checker error in connection handler**
- **Found during:** Task 2 (connection handler)
- **Issue:** `dispatch(&mut dbs[selected_db], frame, &mut selected_db, dbs.len())` borrows `dbs` mutably and immutably simultaneously
- **Fix:** Pre-compute `let db_count = dbs.len()` before the mutable borrow
- **Files modified:** src/server/connection.rs
- **Verification:** cargo build succeeds
- **Committed in:** 4924631 (Task 2 commit)

**3. [Rule 1 - Bug] Fixed Vec<u8>::as_ref() match type mismatch**
- **Found during:** Task 3 (dispatch function)
- **Issue:** `to_ascii_uppercase()` returns `Vec<u8>`, and `as_ref()` resolves to fixed-size array reference causing match failures
- **Fix:** Changed `cmd_name.as_ref()` to `cmd_name.as_slice()` for correct byte slice matching
- **Files modified:** src/command/mod.rs
- **Verification:** cargo test --lib command passes all 20 tests
- **Committed in:** 74419a1 (Task 3 commit)

---

**Total deviations:** 3 auto-fixed (2 bugs, 1 blocking)
**Impact on plan:** All auto-fixes necessary for compilation. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Server foundation complete with storage, codec, TCP, and dispatch infrastructure
- Ready for Plan 02-02 (string commands: GET, SET, MGET, MSET, INCR/DECR family)
- Ready for Plan 02-03 (key management: DEL, EXISTS, EXPIRE/TTL, PERSIST, TYPE, KEYS, RENAME)
- Command handlers follow established pure function pattern for easy extension

---
*Phase: 02-working-server*
*Completed: 2026-03-23*
