# Phase 14: Forkless Compartmentalized Persistence - Context

**Gathered:** 2026-03-24
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace clone-on-snapshot persistence with forkless async snapshotting leveraging DashTable's segmented structure. Each segment is independently serialized to a point-in-time snapshot while the shard continues serving requests. Keys modified in not-yet-serialized segments have their old values captured via a segment-level write-ahead log. Also implement per-shard WAL with io_uring DMA writes and batched fsync for AOF-style durability.

</domain>

<decisions>
## Implementation Decisions

### Forkless snapshot mechanism
- [auto] Mark snapshot epoch → iterate DashTable segments → serialize each segment's current state before any new modifications touch it
- For modified-but-not-yet-serialized segments: capture old value before overwrite (copy-on-write at segment granularity)
- Serialization runs as low-priority cooperative task within shard's event loop, yielding between segments
- Near-zero memory overhead: only one segment's worth of COW data at a time (vs Redis's entire-dataset clone)

### Per-shard parallel snapshots
- [auto] Each shard snapshots independently, writing its own snapshot file
- Parallel per-shard files (DFS-style format) — much faster than single-file sequential
- Optional: single interleaved RDB-compatible export for backward compatibility (merge per-shard files)
- Snapshot coordination: global epoch counter incremented, each shard snapshots at that epoch

### Segment-level COW strategy
- [auto] When a key in a not-yet-serialized segment is modified during snapshot:
  1. Check segment's "snapshot pending" flag
  2. If pending: copy old entry to a small per-segment overflow buffer before overwriting
  3. Serializer reads from overflow buffer for modified entries, DashTable for unmodified
- Overflow buffer is tiny — only keys modified during the brief window between epoch mark and segment serialization

### Per-shard WAL (replacing AOF)
- [auto] Each shard maintains its own write-ahead log file
- Writes via io_uring DMA file write path (Glommio's `DmaStreamWriter` or direct `io_uring_prep_write`)
- Batched fsync: accumulate writes across commands, fsync once per millisecond (matching Redis appendfsync everysec)
- WAL format: RESP wire format (same as current AOF) for compatibility

### Snapshot format
- [auto] Keep current RUSTREDIS binary format with CRC32 footer — extend for per-shard files
- Header includes: shard ID, epoch number, segment count
- Each segment serialized as a self-contained block with its own CRC32
- Enables partial recovery: corrupt segment can be identified and skipped

### Startup restore
- [auto] Load per-shard snapshot files in parallel (one per shard thread)
- Then replay per-shard WAL from the snapshot's epoch forward
- Priority: WAL over snapshot (WAL is more recent), same as current AOF-over-RDB logic

### Claude's Discretion
- Exact snapshot scheduling (manual BGSAVE trigger, auto-save rules, or both)
- COW overflow buffer sizing strategy (pre-allocated vs dynamic)
- Whether to support mixing old RDB format and new per-shard format during transition
- Compression of snapshot segments (LZ4 for speed, or none for simplicity)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture blueprint
- `.planning/architect-blue-print.md` §Persistence without fork: async compartmentalized snapshots — Full forkless snapshot algorithm, segment COW, per-shard WAL

### Phase dependencies
- `.planning/phases/10-dashtable-segmented-hash-table-with-swiss-table-simd-probing/10-CONTEXT.md` — DashTable segments that enable compartmentalized snapshots
- `.planning/phases/11-thread-per-core-shared-nothing-architecture/11-CONTEXT.md` — Per-shard architecture
- `.planning/phases/12-io-uring-networking-layer/12-CONTEXT.md` — io_uring for DMA file writes

### Current implementation (to be replaced)
- `src/persistence/rdb.rs` — Current clone-on-snapshot RDB (668 lines) — core serialization logic reusable
- `src/persistence/aof.rs` — Current AOF (726 lines) — replaced by per-shard WAL
- `src/persistence/auto_save.rs` — Auto-save task (129 lines) — adapt for per-shard snapshots
- `src/command/persistence.rs` — BGSAVE/BGREWRITEAOF commands (106 lines) — update for new mechanism

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- RDB serialization logic (type tags, CRC32 footer, atomic .tmp+rename) — extend for per-shard format
- RESP wire format for WAL — same as current AOF format
- `crc32fast` crate — already a dependency for checksum

### Established Patterns
- BGSAVE clone-on-snapshot: clone data under lock, serialize in background — replaced by segment iteration
- AOF uses RESP wire format with pre-dispatch serialization — WAL uses same format
- Atomic .tmp+rename for crash safety — keep this pattern for per-shard files

### Integration Points
- DashTable segment iterator — new API needed for snapshot traversal (iterate by segment, not by key)
- Shard event loop — snapshot task runs as cooperative low-priority fiber
- BGSAVE/BGREWRITEAOF commands — must trigger per-shard snapshot coordination
- Startup loader in listener.rs — must handle per-shard file loading

</code_context>

<specifics>
## Specific Ideas

- Blueprint: Redis fork() blocks main thread for ~60ms on 25GB dataset, COW can double memory
- Target: < 1ms main-thread stall on 25GB dataset, < 5% memory spike during snapshot
- Dragonfly proves this works in production with its DFS format and compartmentalized snapshots

</specifics>

<deferred>
## Deferred Ideas

- Incremental snapshots (only serialize modified segments since last snapshot) — future optimization
- Cloud storage backend for snapshots (S3, GCS) — future feature

</deferred>

---

*Phase: 14-forkless-compartmentalized-persistence*
*Context gathered: 2026-03-24*
