# Horizontal Scale for Standalone Moon (Durable Mode) — Deep Dive

**Status:** Owner-approved draft · **Date:** 2026-07-10 · **Parent:** [scale-ha-architecture.md](scale-ha-architecture.md)
**Scope:** how to scale Moon **beyond one node without cluster mode** (which GA's at v0.8), while
keeping durability guarantees intact. This is the deployment guidance for the v0.6–v0.7 window and
remains valid afterward for teams that prefer operational simplicity over cluster automation.

---

## 1. The scaling ladder (exhaust each rung before the next)

```
 rung 0: vertical            --shards N, busy-poll, NUMA pinning          1 node
 rung 1: read replicas       1 durable master + K read replicas (v0.7)    1 write node
 rung 2: functional split    cache node / vector node / graph node        per-workload nodes
 rung 3: client-side shards  M independent durable masters, hashed client-side
 rung 4: cluster mode        slots + auto-failover + live rebalance (v0.8)
```

Most workloads never need rung 3+. A single c4a/c3 8-core Moon at P=16+ sustains millions of
ops/s (BENCHMARK.md §1); the usual reason to leave rung 0 is **blast radius and HA**, not throughput.

## 2. Rung 0 — vertical, durable (the baseline everyone starts at)

Recommended durable standalone config:

```
moon --shards <phys_cores> \
     --appendonly yes --appendfsync everysec \        # RPO ≤ 1s
     --dir /var/lib/moon \                            # dedicated filesystem, >5% free (diskfull guard)
     --io-busy-poll-us 40                             # only on pinned, dedicated cores
```

- `everysec` + per-shard WAL group commit is the price/perf sweet spot (1.32× Redis at P16).
- `always` when RPO=0 matters on one node (0.91× Redis at P16 — near-parity; use `WAIT`-style
  app-level confirmation once replicas exist).
- Sizing: shards ≈ physical cores; leave ≥1 core for the listener/admin runtime under heavy
  pipelined load. Non-pipelined random-key workloads: prefer fewer shards until L4 lands (v0.7).
- Know the write-amp knobs: `--wal-kv-log` off (default) avoids KV double-logging; multi-shard
  BGREWRITEAOF stays behind `--experimental-per-shard-rewrite` until v0.7 soak.

**Vertical ceiling signals** (Prometheus): shard CPU saturated while others idle (hot shard —
consider hash-tag redesign first), `moon_aof_fsync_lag_seconds` climbing (disk ceiling — NVMe or
`everysec`), RSS near maxmemory with eviction churn (memory ceiling — offload tiers or a bigger box).

## 3. Rung 1 — read replicas (v0.7, the first horizontal step)

Topology: one durable master, K replicas, application-level read/write split (Moon replicas are
full engines: FT.SEARCH, GRAPH.QUERY, and BM25 all work on replicas — that is the big win vs
"cache replica" thinking: **ship your expensive vector/graph read traffic to replicas**).

```
                    writes, WAIT 1
 app ───────────────► master (--shards N, appendonly yes)
  │                     │  PSYNC2 multi-shard stream (v0.7)
  │ reads (vector/graph/│  per-shard backlogs, partial resync
  │  FTS/KV, stale-ok)  ▼
  └────────► replica-1 … replica-K   (each independently durable: appendonly yes)
```

Rules:
1. **Replicas are durable too** — `appendonly yes` on replicas makes promotion lossless from the
   replica's own applied prefix, and restarts don't trigger full resync storms (partial resync
   from per-shard backlogs).
2. **Consistency contract**: replica reads are eventually consistent (lag is observable via
   `moon_repl_lag_bytes`). For read-your-writes flows, either pin those reads to the master or
   `WAIT K` the write first.
3. **Session affinity for TRACKING**: CLIENT TRACKING invalidation is per-connection — client-side
   caches must subscribe on the node they read from.
4. **Capacity math**: replicas scale *reads* linearly and *writes* not at all. If write volume is
   the ceiling, skip to rung 3.
5. **Failover without cluster mode**: orchestrator-driven (systemd unit + healthcheck, or k8s):
   detect master down → pick replica with max applied offset → `REPLICAOF NO ONE` → repoint
   writers (DNS/VIP/service). Runbook to ship with v0.7: `docs/runbooks/standalone-failover.md`.
   RTO = detection + promotion, typically 5–15s with a 3s healthcheck; RPO = 0 for WAIT-acked writes.

**Resize-via-replica-swap** (the durable vertical-resize playbook, enabled by D-3 — replica shard
count is independent of the master's): stand up a replica on the bigger box with more shards →
let it sync → promote → repoint. Zero-downtime vertical scaling without cluster mode.

## 4. Rung 2 — functional split (multi-model becomes an advantage)

Moon's engines share one process by default, but nothing forces one node to serve every model.
Split by workload when their resource shapes conflict:

| Node | Workload | Why split |
|---|---|---|
| `moon-cache` | KV/session/rate-limit, small values, eviction on | latency-critical, benefits from busy-poll + dedicated cores |
| `moon-vector` | FT.* indexes, HOT→WARM→COLD tiering | RAM/IO heavy, background compaction/merges, different maxmemory profile |
| `moon-graph` | GRAPH.* (single-shard-per-graph anyway) | CPU-bursty traversals; isolate from cache p99 |
| `moon-streams` | X* consumer groups, MQ, CDC tail | disk-bandwidth heavy (WAL append + reads) |

Each node is an independent durable standalone (rung 0 config) with its own replica pair (rung 1).
This is "microservices for data" — the blast-radius win of cluster mode without slot management.
Cost: the application owns the routing map (usually a config table, changes rarely).

## 5. Rung 3 — client-side sharding across independent durable masters

When write throughput or dataset size exceeds one master, run M independent Moon masters and
shard in the client — exactly how large Redis fleets ran for a decade pre-Cluster.

- **Hashing**: use the client library's consistent-hash ring (ketama) or slot-style
  `CRC16(key) % M`. **Adopt `{hash_tag}` key design from day one** — it is Redis-Cluster-compatible
  and makes the later migration to cluster mode (rung 4) a topology change, not an application change.
- **Multi-key discipline**: treat cross-node ops as forbidden — same CROSSSLOT discipline cluster
  mode enforces. Do MGET/MSET fan-out and pipelined merges in the client; keep transactions and
  Lua single-tag.
- **Each master is a rung-1 cell**: durable + replicas + orchestrated failover. The fleet is M
  cells, not one system — failure of a cell degrades 1/M of the keyspace.
- **Proxy option**: a RESP proxy (Envoy redis_proxy, twemproxy) centralizes the ring so
  polyglot services don't reimplement hashing. Cost: +1 hop (~50–200µs), lose CLIENT TRACKING
  and MULTI/EXEC through most proxies. Prefer smart clients where possible.
- **Resharding** (the honest weakness of rung 3): adding a node moves ~1/M of keys with ketama.
  Approaches, worst→best: (a) accept cache-miss rehydration (pure-cache only — with durable data
  this silently orphans keys, so it is NOT acceptable once Moon is a source of record);
  (b) double-write window + backfill scan per moved tag; (c) **don't reshard — go to rung 4**:
  cluster mode's live slot migration (v0.8) exists precisely because client-side resharding of
  durable data is miserable. Owner guidance: if you expect >1 reshard/year, deploy cluster mode.

## 6. Durability contract per rung (RPO/RTO summary)

| Rung | Config | RPO | RTO | Notes |
|---|---|---|---|---|
| 0 | everysec | ≤1s | restart + WAL replay (sec–min) | diskfull guard pauses writes loudly |
| 0 | always + group commit | 0 | same | near-Redis-parity throughput at P16 |
| 1 | everysec + WAIT 1 | 0 across node loss (acked writes) | 5–15s orchestrated promotion | replicas durable too |
| 2 | per-node as above | per-node | per-node | blast radius = one workload |
| 3 | per-cell as rung 1 | per-cell | per-cell | blast radius = 1/M keyspace |
| 4 | cluster (v0.8) | 0 (WAIT-acked) | < 2×node_timeout, automatic | see scale-ha-architecture.md §10 |

Backups are orthogonal and mandatory at every rung: BGSAVE snapshot + WAL archive per node
(`moon-backup` CLI lands v0.9; until then: cron BGSAVE + copy `--dir` snapshot set per the
existing runbooks, verify restore quarterly).

## 7. Standalone → cluster migration path (protect the investment)

Everything above is designed to make rung 4 adoption cheap later:
1. Hash-tag key design (rung 3) is already slot-correct.
2. CROSSSLOT discipline is already enforced by sharded MULTI/EXEC (PR #247) at any shard count.
3. Replica cells (rung 1) become cluster master+replica pairs; `CLUSTER MEET` + `ADDSLOTS` maps
   each cell's tag ranges to real slots.
4. The orchestrated-failover runbook retires in favor of gossip auto-failover.

The only rung-3 artifact that does not carry over is the client-side ring itself — swap the client
to cluster mode (MOVED-aware) and delete the ring config.

## 8. Anti-patterns (seen in the field, do not do these)

- **Replicas with `appendonly no`** to "save IO" — a restarted replica full-resyncs, hammering the
  master's backlogs and network exactly when the fleet is degraded.
- **Two masters behind a VIP with client-side "failover"** — split-brain by construction; without
  cluster epochs, never point writers at two nodes.
- **Sharding by database index (SELECT 0..15)** across nodes — DB indexes are not a scaling
  primitive; use workspaces (`WS.*`) or key prefixes with hash tags.
- **Cross-node MGET fan-out in a hot loop without pipelining** — rung 3 read amplification is only
  cheap when pipelined per node.
- **Skipping WAIT on writes you can't lose** — async replication without WAIT has a nonzero RPO
  no matter what the marketing page says. Moon's WAIT becomes real in v0.7; use it.
