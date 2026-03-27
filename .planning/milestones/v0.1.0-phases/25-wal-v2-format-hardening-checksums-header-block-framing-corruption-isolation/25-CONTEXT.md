# Phase 25: WAL v2 Format Hardening — Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Harden the per-shard WAL format with a versioned header, block-level CRC32 checksums, and corruption isolation. Current WAL is raw concatenated RESP with no integrity checks — a single bit-flip or torn write corrupts all subsequent entries during replay with no detection.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion
All implementation choices are at Claude's discretion — pure infrastructure phase.

Key constraints from deep dive analysis:
- Current WAL: raw RESP commands, no header, no checksums, no entry boundaries
- Snapshot format (RRDSHARD) already has per-segment CRC32 + global CRC32 — proven pattern to follow
- WAL writer: 8KB in-memory buffer, flushed every 1ms tick, fsync every 1 second (EverySec)
- Block-level CRC (not per-command) aligns with 1ms flush tick — one CRC per flush batch
- crc32fast crate already in dependencies (used by snapshot.rs)
- V1→V2 auto-detection on startup via magic bytes check
- Replication offset is monotonic and NEVER resets on WAL truncation (must preserve)
- WAL file per shard: `shard-{id}.wal`
- Recovery must stop on first corrupted block (not skip — data ordering matters)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/persistence/snapshot.rs` — Per-segment CRC32 pattern (proven, copy approach)
- `src/persistence/wal.rs` — Current WAL writer (8KB buffer, flush/sync separation)
- `src/persistence/aof.rs` — `replay_aof()` parser, `serialize_command()` serializer
- `crc32fast` crate already in Cargo.toml

### Established Patterns
- WAL append is hot path: `buf.extend_from_slice(data)` (~5-10ns)
- Flush every 1ms tick: `write_all()` to page cache
- Fsync every 1 second: `sync_data()` call
- `truncate_after_snapshot()` rotates WAL on snapshot completion
- `wal_append_and_fanout()` atomically: WAL + backlog + offset + replica fan-out

### Integration Points
- `src/shard/mod.rs` — calls `wal.append()`, `wal.flush_if_needed()`, `wal.sync_to_disk()`
- `src/persistence/aof.rs` — `replay_aof()` used by WAL replay
- `src/shard/mod.rs` — `restore_from_persistence()` loads snapshot then replays WAL

</code_context>

<specifics>
## Specific Ideas

WAL v2 format from deep dive analysis:
- Header: "RRDWAL" (6B) + version (1B) + shard_id (2B LE) + epoch (8B LE) + reserved (13B) = 32 bytes
- Write block: block_len (4B LE) + cmd_count (2B LE) + db_idx (1B) + RESP payload (var) + CRC32 (4B LE)
- CRC covers: cmd_count + db_idx + RESP payload
- Block_len covers: cmd_count + db_idx + RESP payload + CRC32

</specifics>

<deferred>
## Deferred Ideas

- Size-based WAL rotation (add in later phase if needed)
- Per-entry timestamps for point-in-time recovery
- LZ4 compression for large values

</deferred>
