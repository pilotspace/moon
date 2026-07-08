# Runbook: Upgrading to v0.6.0

Audience: operators upgrading a running Moon instance from v0.5.x.
Read alongside [`docs/guides/isolation.md`](../guides/isolation.md) and
[`docs/guides/tuning.md`](../guides/tuning.md).

## TL;DR

1. v0.6.0 is drop-in on the wire and on disk (WAL v3 / AOF / RDB v2 formats
   unchanged from v0.5.1). A rolling restart per
   [`rolling-restart.md`](rolling-restart.md) is sufficient.
2. Two behavior changes can surface as "new" errors or RSS movement on an
   unchanged workload — both are the server telling the truth where it
   previously didn't. Read the sections below before paging yourself.
3. If you ever used workspaces (`WS CREATE`/`WS DROP`) together with
   `SELECT`, run the leaked-key sweep in section 4 once after upgrading.

## 1. Memory accounting is now truthful under container growth

Before v0.6.0, growing an EXISTING hash/list/set/zset (`HSET` adding fields,
`LPUSH`, `SADD`, `ZADD`, ...) was invisible to `used_memory` — only key
creation/deletion moved the counter. A workload dominated by container growth
could exceed `--maxmemory` indefinitely without a single eviction or OOM
error.

After upgrading, `used_memory` reflects real container size:

- **Expect `used_memory` to climb** toward its true value as containers are
  touched. On an instance sized against the old (under-reporting) counter,
  `noeviction` OOM errors or evictions may appear for the first time on a
  workload that "worked fine" before. The workload did not regress — the
  meter did.
- **Shrink commands are never blocked** (Redis `deny-oom` parity, new in
  v0.6.0): `DEL`, `UNLINK`, `HDEL`, `SREM`, `SPOP`, `LPOP`, `RPOP`, `LREM`,
  `LTRIM`, `ZREM`, `ZPOP*`, `GETDEL`, `EXPIRE`-family, `PERSIST`,
  `FLUSHDB`/`FLUSHALL` always pass the OOM gate, so a client at the limit can
  always shrink its way back under it.
- **Action:** compare `INFO memory`'s `used_memory` against your alerting
  thresholds after 24h of normal traffic; re-size `--maxmemory` if it was
  calibrated against pre-v0.6.0 numbers.

## 2. Idle engine segments now offload by default

Immutable vector-index segments (HNSW) with no search traffic for
`--engine-offload-idle-secs` (default **3600s**) demote HOT→WARM (mmap) and
then to a COLD unloaded stub; age-based demotion via `--segment-warm-after`
(default 3600s) also applies — whichever threshold hits first.

- **Expect lower steady-state RSS** on instances with rarely-queried indexes
  (measured −26% on a 40K×768d corpus).
- **First search after a COLD unload reloads the segment.** The reload is a
  shard-wide stall for its duration (~80ms measured on a 15K×768d segment) —
  every connection on that shard waits, not just the querying one. If a
  latency-sensitive index must never pay this, set
  `--engine-offload-idle-secs 0` (disables idle demotion; age-based
  `--segment-warm-after` still applies — raise it too if you want segments
  pinned HOT).
- Observability: `FT.INFO <idx>` exposes `graph_segments`, `warm_segments`,
  `unloaded_segments`.

## 3. Per-db quotas and db-scoped indexes (opt-in)

- `--db-maxmemory <db>:<bytes>` (repeatable) / `CONFIG SET db-maxmemory <db>
  <bytes>` cap a single SELECT-able db. Unset = no change from v0.5.x.
- `FT.*`/graph/full-text indexes created after the upgrade are scoped to the
  db that created them. Indexes recovered from a pre-v0.6.0 data dir load
  into db 0 (the only db that could create them before).

## 4. One-time sweep: keys leaked by pre-v0.6.0 `WS DROP`

Before v0.6.0, `WS DROP`'s cleanup only swept logical db 0. A workspace whose
connection `SELECT`ed a non-zero db before writing leaked those keys
permanently on drop. The sweep is fixed, but keys from PAST drops are still
resident. Once, after upgrading:

```bash
# For each non-zero db n:
redis-cli -p 6379 -n <n> --scan --pattern '<ws-uuid>:*'
# Compare the prefixes found against `WS LIST`; DEL anything whose workspace
# no longer exists (or FLUSHDB if the db held nothing else).
```

## 5. New tuning shortcut

For single-instance deployments, `--profile standalone` fills in the measured
best flags for a shard-1 node (`--shards 1 --io-busy-poll-us 40 --io-driver
epoll`) without overriding anything you set explicitly. Only use busy-poll on
pinned/dedicated cores — on shared/oversubscribed hosts it regresses (see
`docs/guides/tuning.md`).

## Rollback

v0.6.0 makes no on-disk format changes; rolling back to v0.5.1 is a binary
swap + restart. Caveats: indexes created in a non-zero db while on v0.6.0
will load into the keyspace-global (db 0) view under v0.5.1, and per-db
quotas/`--profile` flags are unknown to the old binary (remove them from the
start command before rollback).
