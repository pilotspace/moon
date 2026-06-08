# Runbook: Shard Count Change

## Symptoms

Moon refuses to start with:

```
ERR shard count changed (manifest=N, config=M); refusing to start to avoid data loss.
See https://github.com/pilotspace/moon/blob/main/docs/runbooks/shard-count-change.md
```

## Root Cause

The per-shard AOF layout partitions persisted data by shard index
(`shard-0/`, `shard-1/`, …) using `key_to_shard(key, num_shards)`. The
manifest records the shard count that wrote the data. Starting with a
different `--shards` value would route keys to different shards than the
ones whose AOF files actually contain them — silently dropping data from
shards that no longer exist, or replaying a shard's data into the wrong
table. Moon refuses to start instead.

Common triggers:

- **Upgrading to v0.2.0+:** the default shard count changed from
  auto-detect (CPU count) to **1**. A deployment that previously started
  with no `--shards` flag on a 12-core machine wrote `manifest=12`;
  restarting on the new default yields `config=1` and this error.
- Changing `--shards` (or the `shards` conf directive) on an existing
  data directory.
- Moving a data directory between machines with different core counts
  when the old auto-detect default was in effect.

## Recovery Steps

### Option A — keep the existing shard count (fastest, no data movement)

Start with `--shards` matching the `manifest=N` value from the error:

```bash
moon --dir <dir> --appendonly yes --shards <N>
# or pin it in moon.conf:  shards N
```

### Option B — actually reshard the data

There is no offline N→M reshard tool. Reshard logically, over the wire:

1. Start the old instance with its original count (Option A).
2. Start a fresh instance with the desired count on an empty directory
   and a different port.
3. Replicate or copy the keyspace (e.g. `REPLICAOF`, or a
   `DUMP`/`RESTORE` / `MIGRATE` script for selective copies).
4. Verify (`DBSIZE`, `INFO keyspace`), then cut clients over and retire
   the old instance.

For **legacy v1 single-file AOF** specifically (pre-v0.1.12 layout),
use the offline migration tool instead:

```bash
moon --migrate-aof-from <old-dir> --migrate-aof-to <new-dir> --migrate-aof-shards <M>
moon --dir <new-dir> --appendonly yes --shards <M>
```

### Option C — data is disposable

```bash
# back up first if in doubt
mv <dir> <dir>.bak
moon --dir <dir> --appendonly yes --shards <M>
```

## Prevention

- Pin `shards N` explicitly in `/etc/moon/moon.conf` for reproducible
  deployments — don't rely on defaults across version upgrades.
- Single shard (`shards 1`, the default since v0.2.0) gives the best
  throughput for non-pipelined workloads; use higher counts for
  pipelined/AOF-heavy multi-core deployments and `{tag}` hash tags to
  co-locate related keys.
