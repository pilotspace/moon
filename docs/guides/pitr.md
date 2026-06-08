---
title: "Point-in-Time Recovery (PITR)"
description: "Restart Moon at a specific WAL LSN or wall-clock time."
---

# Point-in-Time Recovery (PITR)

Moon v0.2 adds **point-in-time recovery** on top of the existing per-shard WAL +
RDB snapshot stack. Operators can rewind a shard to any LSN that still lives in
the WAL, or to a wall-clock time anchored by a temporal record.

> **Status:** PITR ships as flags-only — there is no separate restore binary.
> Stop the server, restart with a target flag, and the recovery pipeline does
> the rest.

## Flags

| Flag | Type | Meaning |
|------|------|---------|
| `--recovery-target-lsn <N>` | `u64` | Stop replay at the last record with `lsn <= N`. |
| `--recovery-target-time <RFC3339>` | string | Resolve to an LSN via the WAL's temporal anchors, then stop. |

The two flags are mutually exclusive in practice — if both are set, the
explicit LSN wins. Omitting both yields a normal full-replay restart.

```bash
# Rewind to LSN 12345 (one shard)
./moon --port 6399 --dir /var/lib/moon --recovery-target-lsn 12345

# Rewind to a wall-clock instant (UTC, RFC3339)
./moon --port 6399 --dir /var/lib/moon \
       --recovery-target-time 2026-05-12T09:30:00Z
```

## How it works

1. **Snapshot selection.** Recovery scans available `.rdb` files and picks the
   newest snapshot whose embedded `last_lsn` is `<= target_lsn`. v1 snapshots
   (pre-0.2) ship with `last_lsn = 0` and are **skipped conservatively** —
   replay falls back to a full WAL scan from segment 0 in that case.
2. **WAL replay.** `replay_wal_v3_dir_until(target_lsn)` walks segments in
   order. The loop breaks on the first record with `lsn > target_lsn`, and the
   resumed `wal_flush_lsn` is *not* advanced past the target. This keeps the
   control file truthful in case a subsequent restart drops the flag.
3. **Time resolution.** `resolve_target_time_to_lsn` scans the WAL for
   `TemporalUpsert` and `GraphTemporal` records (the only record types that
   carry `system_from` timestamps) and returns the LSN of the last record with
   `ts <= target_time`. Workloads without temporal commands have no anchors;
   for those, prefer `--recovery-target-lsn`.

## Snapshot LSN provenance

The snapshot file header was bumped to v2 (`SHARD_RDB_VERSION = 2`) and now
carries:

| Field | Bytes | Purpose |
|-------|------:|---------|
| `last_lsn` | 8 | WAL LSN captured at snapshot time |
| `created_at_unix_ms` | 8 | Wall-clock when snapshot was sealed |

v1 snapshots load with `last_lsn = 0` and `created_at_unix_ms = 0`. That value
is a safety signal — PITR refuses to use them as a starting point.

> **Operator note.** Live snapshots produced by the persistence tick still
> embed `last_lsn = 0` in v0.2 — the wiring to `wal_flush_lsn` ships in P3c.
> Until then, PITR effectively performs a full WAL replay up to the target.
> Plan retention accordingly: keep enough WAL segments to cover the recovery
> window.

## Verification

The CI suite asserts:

- `test_recovery_stops_at_target_lsn` — write 100 commands, restart with
  `target_lsn = 50`, only the first 50 SETs are visible.
- `test_recovery_target_time_resolves` — temporal upserts at known
  timestamps; restart with a mid-stream target time; correct cutoff.
- `test_v1_snapshot_loads_with_zero_lsn` — backward-compat round-trip.

## What is *not* in v0.2

- ❌ Cross-shard global cutoff. Each shard PITRs independently; cross-shard
  transactional consistency lands with the v0.3 distributed-txn work.
- ❌ Logical undo. PITR is forward replay up to a cutoff, not rollback of
  individual operations.
- ❌ Mixed `target_lsn` + `target_time` arbitration UI — LSN wins silently.
