---
title: "Persistence"
description: "Configure RDB snapshots and AOF write-ahead logging for durability."
---

# Persistence

Moon supports two persistence mechanisms: RDB point-in-time snapshots and AOF (append-only file) write-ahead logging. Both can be used together.

## AOF (recommended)

AOF logs every write operation to per-shard WAL files. This is the recommended persistence method for durability.

```bash
./target/release/moon --appendonly yes --appendfsync everysec --dir /var/lib/moon
```

### Fsync policies

| Policy | Durability | Performance |
|--------|-----------|-------------|
| `always` | Every write fsynced | Highest durability, lowest throughput |
| `everysec` | Fsync every second | Good balance (recommended) |
| `no` | OS-controlled flush | Highest throughput, risk of data loss |

### When the AOF writer falls behind (backpressure)

Each shard hands its AOF records to a writer thread through a bounded queue.
If the disk is slow -- a stalled fsync, a saturated device, a paused VM -- the
queue fills. What a write does then depends on `appendfsync`:

- **`everysec` / `no`**: the write waits for room in the queue for up to
  `--aof-fsync-timeout-ms` (default 2000 ms; `0` waits forever). If the writer
  is still backlogged when the bound elapses, the write is **refused** with

  ```text
  -MOONERR AOF backpressure: write applied in memory but not queued for persistence; the AOF writer is backlogged
  ```

  The same refusal is returned right away when a rewrite is in progress and
  its overflow buffer is full. Redis does not refuse here: it writes on
  without waiting for the slow fsync and counts `aof_delayed_fsync`. Moon
  refuses because acknowledging a record the writer never received could
  lose an acked write on restart. Moon never answers `+OK` for such a write.
- **`always`**: the write waits for its fsync for up to
  `--aof-fsync-timeout-ms`, and a write that is not confirmed in time answers
  `-ERR AOF fsync failed; write not durable`, as it did before. Writes whose
  batch fsync barrier (MULTI/EXEC, scripts, pipelined batches) cannot even be
  queued because the writer is backlogged answer

  ```text
  -MOONERR AOF backpressure: write applied in memory and queued, but not confirmed durable; the AOF writer is backlogged
  ```

  Their records did reach the writer and will be fsynced with the backlog;
  only the confirmation is missing. They count in the same
  `aof_append_backpressure_refusals`.

What the backpressure refusal means:

- **Nothing failed on disk.** No fsync ran or failed. A real write or fsync
  error answers `-ERR AOF fsync failed; write not durable` and sets
  `aof_fsync_failures` / `aof_last_fsync_status:err`. The refusal changes
  neither. (Before moon#1272 the refusal used the fsync-failure text.)
- **The write is applied in memory, but its record is not in the AOF.** A
  later read sees it. A restart before the next AOF rewrite would not replay
  it, so `aof_last_write_status` reads `err` until a rewrite folds it into the
  new base. Retry only idempotent writes (`SET` of a fixed value, `DEL`): a
  retried `INCR` would be applied twice. (A client can very rarely get this
  reply for a record that did reach the writer at the moment of the timeout.
  That is the safe direction: the client sees an error, never a false `+OK`.)
- **It is counted.** INFO `persistence` field `aof_append_backpressure_refusals`
  and Prometheus `moon_aof_append_backpressure_refusals_total` count every such
  refusal. The server logs one `WARN` line (`AOF writer backlogged ...`) when a
  stall begins and at most one summary every 10 s while it lasts.

A write routed to another shard is checked for room in that shard's AOF queue
*before* it runs (moon#769). If there is no room within the bound, it is
refused unapplied with `-MOONERR AOF backpressure: command not executed, ...;
retry` and counted in `aof_backpressure_refused`. That write is safe to retry.

A non-zero `aof_append_backpressure_refusals` means the disk cannot keep up
with the write rate. Look at device latency, or give the writer more time with
`--aof-fsync-timeout-ms`.

The graph plane's rollback has the same contract. A `TXN.ABORT` logs the
records that undo its graph writes into the shard's WAL, and when the WAL
append channel is full (or its writer is gone) the records that do not fit
are counted in INFO `persistence` field `txn_rollback_wal_dropped`, logged,
and the abort answers

```text
-MOONERR WAL backpressure: TXN rolled back in memory, but its graph rollback records were not all queued for persistence; a restart may replay the aborted graph writes
```

instead of `+OK`. The rollback is applied in memory and a retry has nothing
left to roll back; only its durability is missing.

### Per-shard WAL advantage

Unlike Redis's single global AOF file, Moon writes a separate WAL per shard. This eliminates the global serialization bottleneck:

Measured on Linux (GCE c3-standard-8, Redis 7.0.15, moon `--shards 2`, 3
alternated reps — `BENCHMARK.md` §7.3):

| `appendfsync` / depth | Moon vs Redis |
|:-:|:-:|
| `everysec` SET p=1 | 0.99x (parity) |
| `everysec` SET p=16 | **1.32x** |
| `always` SET p=1 | parity (fsync-device-bound) |
| `always` SET p=16 | 0.91x |

The advantage grows with pipeline depth because each shard appends independently
with no lock contention. A higher set of ratios (2.21x at p=16, 2.75x at p=64)
appears in `BENCHMARK.md` §7.1; that table is an Apple M4 Pro development
reference and has not been reproduced on Linux.

### WAL v3 format

Moon uses WAL v3 (`src/persistence/wal_v3/`; v2 was removed) with:
- **Segmented files** (16MB default, `--wal-segment-size`) with a 64-byte
  header carrying epoch, redo LSN, and base LSN
- **Per-record LSNs** — the foundation for PITR and CDC cursors
- **Checksums** for corruption detection
- **Full-page images (FPI)** with lz4 compression for torn-page recovery
- **Corruption isolation** per shard (one shard's corruption does not affect others)
- **Off-loop fsync** — a per-shard sync agent thread owns the fsync so the
  shard event loop never blocks on durability waits

The hot-path cost of WAL append is ~5ns (`buf.extend_from_slice()`), with batch `write_all` every 1ms tick and fsync on the configured schedule.

### One KV log at a time (`--wal-kv-log`)

With `--appendonly yes` the **AOF is the crash-recovery authority**: startup
replays the AOF over a wiped keyspace, discarding whatever the WAL replayed
first. Moon therefore skips the WAL copy of each KV command by default
(`--wal-kv-log auto`) — one durable KV log instead of two, halving on-disk
write volume at `--shards >= 2` with zero recovery loss. The WAL still carries
checkpoint/FPI and feature records, and KV logging re-engages automatically
when a CDC subscriber attaches. Set `--wal-kv-log on` if you need
point-in-time recovery or full CDC history alongside the AOF.

> **⚠ The WAL is not a standalone durability log.** With `--appendonly no`,
> only cross-shard (SPSC-dispatched) writes reach the WAL — writes local to a
> connection's own shard are not logged anywhere, so crash recovery loses
> roughly `1/num_shards` of writes at `--shards >= 2` (measured: 79% recovered
> at 4 shards) and **everything** at `--shards 1`. Keep `--appendonly yes`
> (the default) whenever you need KV durability; the WAL's KV stream exists
> for CDC, PITR, and disk-offload — not as an AOF replacement.

## RDB snapshots

RDB creates point-in-time snapshots of the entire dataset.

```bash
# Auto-save: snapshot after 3600 seconds if at least 1 key changed,
# or after 300 seconds if at least 100 keys changed
./target/release/moon --save "3600 1 300 100" --dir /var/lib/moon

# Manual trigger
redis-cli BGSAVE
```

### Forkless snapshots

Moon uses forkless compartmentalized snapshots instead of Redis's `fork()` approach. This means:

- No copy-on-write memory spike (Redis can temporarily double memory usage during BGSAVE)
- DashTable segments are iterated asynchronously
- Snapshot runs alongside normal operations without blocking

### Automatic snapshots with disk offload

With `--appendonly no` and `--disk-offload enable`, a cold key's spill file
that is no longer referenced (the key was deleted, overwritten or promoted)
is **held** on disk until a snapshot that started after it went unused has
completed: until then that file is the key's only durable copy, and deleting
it early could bring a deleted key back after a crash, or lose a live one.

Nobody may ever run `BGSAVE` on such a server, so Moon requests the snapshot
itself (moon#1289). After three cold orphan sweeps with a held file (about
two minutes at the default `--cold-orphan-sweep-interval-secs 60`), it starts
one, at most one per ten sweep intervals (about ten minutes). This happens
**even with `save ""`**, and the snapshot is an ordinary `BGSAVE`: it
overwrites the dump file in `--dir`, and moves `LASTSAVE`.

Like every snapshot (`BGSAVE`, the `--save` rules, `SHUTDOWN`'s save), the
automatic snapshot never contains a `TXN`'s uncommitted writes (moon#1300): a
key an open transaction holds is saved at its pre-transaction value, whether
the transaction wrote it before the snapshot started or during it. So the
snapshot runs whenever it is due, transactions open or not.

Watch it with `INFO`: `cold_held_files_stale_databases` and
`cold_held_release_snapshots_requested`. With `--appendonly yes` no
snapshot is taken: an AOF rewrite releases the held files instead
(`cold_held_release_folds_requested`).

The no-AOF cold reclaim (moon#1297) asks for a snapshot the same way. A
mostly-dead spill file is compacted into a new file, and the compaction is
adopted (the old file deleted) only once a snapshot that started after it
has completed. When compactions have waited three sweeps with no snapshot,
Moon requests one, under the same spacing: one snapshot serves both reasons.
It runs with transactions open too: a key a `TXN` wrote is saved at its
pre-transaction value, including a cold key the transaction read back from
a spill file. While compactions wait, the old file and its compacted copy
are both on disk (at most 64 waiting compactions per database), and the
bookkeeping of the waiting compactions counts toward `maxmemory`: a shard
keeps it under a sixteenth of its memory budget, so the reclaim never
makes a write fail. `INFO`: `cold_reclaim_compactions_pending`,
`cold_reclaim_pending_bytes` and `cold_reclaim_snapshots_requested`.

## Using both

For maximum durability, enable both AOF and RDB:

```bash
./target/release/moon \
  --appendonly yes \
  --appendfsync everysec \
  --save "3600 1 300 100" \
  --dir /var/lib/moon
```

AOF provides point-of-failure recovery, while RDB provides compact backups for disaster recovery or cloning.
