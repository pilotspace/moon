# Phase 43: RDB Snapshot Hardening — Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Harden the forkless async snapshot engine (Phase 14) for production readiness: add Redis-compatible RDB format export/import for PSYNC2 full resync and migration, replace blocking `std::fs::write` with async file I/O, fix BGSAVE completion tracking (currently fire-and-forget), add streaming RDB generation for replication, and implement missing persistence commands (SAVE, LASTSAVE, INFO persistence section).

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key technical constraints from existing codebase:
- Phase 14 RRDSHARD format (custom) is the internal snapshot format — keep it as primary
- Redis RDB format needed as secondary export for PSYNC2 full resync (master.rs:67-109 already sends per-shard .rrdshard files)
- `bgsave_start_sharded()` creates oneshot channels but clears SAVE_IN_PROGRESS immediately (persistence.rs:131)
- Replication master already awaits per-shard snapshot completion via oneshot receivers (master.rs:98-105)
- `snapshot.rs:finalize()` uses blocking `std::fs::write()` — needs async path
- Both Tokio and Monoio runtimes must be supported (cfg-gated)
- Blueprint specifies io_uring DMA file write path for persistence

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/persistence/snapshot.rs` — SnapshotState cooperative state machine (687 lines), segment-level COW, RRDSHARD format save/load
- `src/persistence/rdb.rs` — Entry-level serialization (write_entry/read_entry) for all 6 data types, used by both rdb::save() and snapshot.rs
- `src/persistence/wal.rs` — WalWriter with batched fsync, truncate_after_snapshot
- `src/persistence/auto_save.rs` — Auto-save rules, watch channel trigger for sharded mode
- `src/command/persistence.rs` — bgsave_start (legacy), bgsave_start_sharded, SNAPSHOT_EPOCH, SAVE_IN_PROGRESS
- `src/runtime/traits.rs` — FileIo trait for runtime-agnostic file operations
- `src/replication/master.rs` — Full resync path sends per-shard .rrdshard files as bulk strings

### Established Patterns
- Dual runtime via `#[cfg(feature = "runtime-tokio")]` / `#[cfg(feature = "runtime-monoio")]`
- Per-shard independence: each shard snapshots its own databases to shard-{id}.rrdshard
- Cooperative scheduling: 1ms timer tick advances one segment per tick
- Oneshot channels for completion signaling (already in SnapshotBegin message)
- Atomic file write: write to .tmp, then rename

### Integration Points
- `shard/mod.rs` — Snapshot state machine driven by spsc_interval tick, COW intercept before dispatch
- `replication/master.rs:67-109` — Full resync triggers SnapshotBegin, awaits completion, sends files
- `command/persistence.rs` — BGSAVE command entry point
- `main.rs` — Watch channel setup, restore_from_persistence per shard, auto-save wiring

</code_context>

<specifics>
## Specific Ideas

- Blueprint (architect-blue-print.md Section 6) specifies: "forkless, async snapshotting leveraging DashTable's compartmentalized structure" with "io_uring's DMA file write path" and "RDB-format export"
- Replication full resync currently sends .rrdshard files as bulk strings — could stream RDB on-the-fly instead
- SAVE_IN_PROGRESS completion tracking can use existing oneshot channels already created in bgsave_start_sharded

</specifics>

<deferred>
## Deferred Ideas

- Redis RDB format v10 full compatibility (only need enough for PSYNC2 bulk transfer)
- redis-check-rdb / rdbtools interop testing
- Point-in-time recovery from WAL + snapshot combination

</deferred>
