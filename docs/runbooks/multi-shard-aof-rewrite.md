# Runbook — multi-shard + AOF refused at startup

**Status:** active (Moon ≥ v0.1.13). Lifted in v2.0 once multi-shard
AOF replay walks every shard's segment manifest on recovery.

## What you saw

### At startup

```
REFUSING TO START: --shards 2 + --appendonly yes has a known data-loss
bug on SIGKILL (~50 % loss verified 2026-05-26). Fix: use --shards 1,
or pass --appendonly no for cache-only deployments, or pass
--unsafe-multishard-aof to acknowledge the risk and start anyway. See
docs/runbooks/multi-shard-aof-rewrite.md.
```

### Or at command time (defence-in-depth, fires under any escape-hatch)

```
> BGREWRITEAOF
(error) ERR BGREWRITEAOF is unsafe with --shards >= 2 + --disk-offload enable
        + --appendonly yes (known data-loss bug; see
        docs/runbooks/multi-shard-aof-rewrite.md). Use --shards 1, set
        --disk-offload disable, or wait for v2.0 multi-part AOF replay.
```

## Why this gate exists

Verified on `main` at commit `6e49050` (2026-05-26), reproducers in
[`tmp/p0-no-rewrite.sh`](../../tmp/p0-no-rewrite.sh),
[`tmp/p0-always.sh`](../../tmp/p0-always.sh),
[`tmp/p0-multishard-no-offload.sh`](../../tmp/p0-multishard-no-offload.sh):

| Configuration                                                              | Result                       |
|----------------------------------------------------------------------------|------------------------------|
| `--shards 1 --appendonly yes --appendfsync always` (control)               | ✅ Recovers 5000 / 5000       |
| `--shards 1 --disk-offload enable --appendonly yes` (control)              | ✅ Recovers 12 714 / 12 714   |
| `--shards 2 --disk-offload enable --appendonly yes --appendfsync everysec` | ❌ Loses 38 % (12 662 → 7 892) |
| `--shards 2 --disk-offload enable --appendonly yes --appendfsync always`   | ❌ Loses 50 % (5000 → 2474)   |
| `--shards 2 --disk-offload disable --appendonly yes --appendfsync always`  | ❌ Loses 50 % (5000 → 2453)   |

**The bug is in the multi-shard AOF durability path itself**, not the
rewrite path and not the disk-offload tier. `--appendfsync always` and
`--disk-offload disable` do not save you — only `--shards 1` does.

The rewrite-specific gate (the `BGREWRITEAOF` error above) is still
shipped as defence-in-depth for anyone who passes
`--unsafe-multishard-aof`.

## How to recover from a triggered loss (if you hit this on < v0.1.13)

1. If a recent RDB snapshot exists in `--dir`, stop the server, move
   `appendonlydir/` aside, and let recovery rebuild from RDB +
   surviving per-shard WAL only. RPO equals the time since the RDB
   snapshot.
2. If replication was running, promote a non-affected replica
   (`REPLICAOF NO ONE`) and re-sync the affected node.
3. If neither: data is lost. File a `P0` with the AOF manifest
   contents (`appendonlydir/moon.aof.manifest`) and per-shard WAL
   sizes.

## How to avoid the gate

Pick whichever matches your deployment:

| Option                                             | Trade-off                                                                                |
|----------------------------------------------------|------------------------------------------------------------------------------------------|
| `--shards 1`                                       | **Recommended.** Best throughput on non-pipelined workloads; gives up multi-shard fan-out |
| `--appendonly no`                                  | Cache-only deployments; durability falls back to `save` rules + RDB recovery             |
| `--unsafe-multishard-aof`                          | **Discouraged.** Acknowledges ~50 % loss on crash; suitable only for ephemeral caches    |

The first option also clears the `BGREWRITEAOF` defence-in-depth gate.

## When will this be removed?

v2.0 ships multi-shard AOF replay that walks every shard's segment
manifest on recovery. Both gates (startup refusal + `BGREWRITEAOF`
error) are removed at the same time. Track progress at
[`tmp/SHIP-PLAN-v1.0-rc1-single-node.md`](../../tmp/SHIP-PLAN-v1.0-rc1-single-node.md)
§ Track B.

## Telemetry

When `--unsafe-multishard-aof` is passed AND the suspect config is set,
the BGREWRITEAOF-specific gate also logs at startup at `WARN`:

```
BGREWRITEAOF gated for this config (known data-loss path; see
docs/runbooks/multi-shard-aof-rewrite.md). Use --shards 1 or
--disk-offload disable to re-enable rewrite.
```

Each gated `BGREWRITEAOF` invocation also returns the documented `ERR`
line at the wire, so any operator dashboard tailing `slowlog` or
client-side error counters will surface the refusal immediately. A
dedicated Prometheus gauge for both gates is on the v1.0-rc1 backlog.
