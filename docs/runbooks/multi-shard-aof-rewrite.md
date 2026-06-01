# Runbook — multi-shard AOF (per-shard layout)

**Status:** Resolved. The startup refusal gate introduced in v0.1.13 was lifted
in v0.1.13-patch (PR #129) once the per-shard AOF replay path was shipped and
verified (CRASH-01-LITE: 200/200 SIGKILL recovery, 0 data loss).

---

## Background (historical context)

Prior to PR #129, Moon refused to start with `--shards >= 2 + --appendonly yes`
because the single-writer AOF implementation lost ~50 % of writes on SIGKILL
in that configuration. The fix was a full per-shard AOF architecture (Option B):
each shard owns its own writer task and recovery walks every shard's segment
manifest independently.

If you are running Moon ≤ v0.1.13 and hit the old startup error, see the
**Upgrading** section below.

---

## Current architecture (v0.1.13-patch / PR #129 and later)

### Per-shard AOF layout

```
<persistence_dir>/
  appendonlydir/
    moon.aof.manifest          ← top-level manifest (layout: PerShard)
    shard-0/
      moon.aof.1.base.rdb      ← base snapshot for shard 0
      moon.aof.1.incr.aof      ← incremental log for shard 0
    shard-1/
      moon.aof.1.base.rdb
      moon.aof.1.incr.aof
    shard-N/
      ...
```

Each shard's writer task appends to its own `.incr.aof` file. On restart, Moon
opens the manifest, discovers all shard directories, and replays each shard's
log independently. Shard replay is parallel — recovery time does not grow
linearly with shard count.

### Durability invariants

| appendfsync         | Guarantee                                                              |
|---------------------|------------------------------------------------------------------------|
| `always`            | Write is on durable storage before +OK (AppendSync rendezvous)         |
| `everysec` (default)| Fsync runs every second; at most ~1 s of writes at risk on crash        |
| `no`                | OS decides when to flush; fastest but weakest guarantee                |

### BGREWRITEAOF in per-shard mode

`BGREWRITEAOF` fans out to every shard's writer task. Each shard compacts its
own log independently. All N acks are awaited before returning `+Background
append only file rewriting started`.

---

## Upgrading from v0.1.13 (old TopLevel AOF) to per-shard layout

If you have an existing deployment with a **TopLevel** AOF manifest (single
writer, `layout: TopLevel`) and want to migrate to per-shard layout:

### Option A — cold migration (recommended, zero-risk)

1. Stop the server.
2. Run `BGSAVE` on the last healthy instance, or copy `dump.rdb` from
   `--dir`.
3. Remove `appendonlydir/` entirely.
4. Restart with `--shards N --appendonly yes`. Moon creates a fresh per-shard
   manifest. Recovery loads from RDB; the incremental AOF starts empty.

### Option B — in-place migration (future tooling)

A `moon migrate-aof --from-top-level` CLI subcommand is planned for v0.2. Until
then, use Option A.

### Safety guard — TopLevel manifest with multi-shard startup

If Moon detects an existing **TopLevel** AOF manifest at startup with
`--shards >= 2`, it refuses to start and prints:

```
REFUSING TO START: legacy TopLevel AOF manifest at <path> detected with
--shards N (>= 2). A TopLevel (single-writer) AOF cannot safely serve
as the persistence log for a multi-shard instance. Options:
  1. Use --shards 1 (single-shard, fully compatible with TopLevel layout).
  2. Remove appendonlydir/ and restart to create a fresh per-shard manifest.
  3. Run: moon migrate-aof --from-top-level  (planned for v0.2).
```

This is intentional — a TopLevel log does not capture per-shard ordering, so
replaying it on a multi-shard instance would produce incorrect key routing.

---

## Deprecated flag: --unsafe-multishard-aof

The `--unsafe-multishard-aof` flag was introduced in v0.1.13 as an escape hatch
to acknowledge the known ~50 % data-loss risk. It is now **deprecated** and will
be removed in v0.2:

- The underlying bug is fixed — the flag no longer suppresses any safety gate.
- Passing it emits a `[DEPRECATED]` warning at startup.
- If you have scripts or systemd units that pass `--unsafe-multishard-aof`,
  remove that flag — it is a no-op.

---

## Monitoring and telemetry

### INFO Persistence fields added in PR #129

```
aof_backpressure_dropped:<N>   ← count of writes dropped due to full AOF channel
```

A non-zero value indicates the AOF writer is falling behind write throughput.
Investigate disk I/O or increase `aof-rewrite-min-size`.

### Prometheus / alerting

A dedicated gauge for `aof_backpressure_dropped` is planned for v0.2. Until
then, monitor via `INFO persistence` polling.

---

## Crash recovery verification

Run the CRASH-01-LITE suite to verify your configuration recovers cleanly:

```bash
# From the moon-dev OrbStack VM:
cargo test --release crash_01_lite 2>&1 | tail -20
```

Expected: 200/200 entries recovered across all shards after SIGKILL.

---

## Escalation

If you observe data loss after a crash on v0.1.13-patch or later, collect:

1. `appendonlydir/moon.aof.manifest` contents
2. `appendonlydir/shard-*/` file sizes and modification times
3. Server log from the crashed process (look for AOF writer task exit reason)
4. `INFO persistence` output from the recovered instance

File a P0 with these artifacts attached.
