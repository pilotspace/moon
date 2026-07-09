# Moon Scale & High-Availability Architecture

**Status:** Owner-approved draft · **Date:** 2026-07-09 · **Companion:** [ROADMAP.md](ROADMAP.md) §4
**Prior art:** `.planning/rfcs/multi-shard-replication-design.md`, `.planning/rfcs/v02-enterprise-architecture.md`,
`.planning/rfcs/d5-replica-compaction.md` — this document is the umbrella architecture those RFCs implement.

---

## 1. Architecture today (baseline)

```
                       ┌──────────────────────── moon process ───────────────────────┐
 clients ──SO_REUSEPORT┤  shard 0 (core 0)   shard 1 (core 1)  …  shard N (core N)   │
   RESP2/3             │  ┌─────────────┐    ┌─────────────┐                          │
                       │  │ event loop  │◄──►│ event loop  │   SPSC rings (flume/     │
                       │  │ DashTable   │    │ DashTable   │   ringbuf), no locks     │
                       │  │ VectorStore │    │ VectorStore │   on write path          │
                       │  │ Graph/FTS   │    │ Graph/FTS   │                          │
                       │  │ WAL v3+AOF  │    │ WAL v3+AOF  │   per-shard durability   │
                       │  └─────────────┘    └─────────────┘                          │
                       │        listener runtime: admin HTTP, cluster bus*, gossip*   │
                       └──────────────────────────────────────────────────────────────┘
                        * today spawned only under runtime-tokio (gap: monoio wiring)
```

Load-bearing properties to preserve in every scale-out design:
- **Shared-nothing shards** — `Shard` owns its databases/engines with no `Arc<Mutex>`; cross-shard
  traffic is message passing (SPSC) only.
- **Unified WAL v3** — one log format carries KV, vector, graph, temporal, text, and Xact records
  with a global LSN space. Any log-shipping replication transports *all* engines for free.
- **Global LSN authority** — `ReplicationState::issue_lsn` / `OffsetHandle` already issue
  lock-free per-write offsets; `master_repl_offset` = Σ per-shard offsets.
- **Keyspace routing is already cluster-shaped** — `{hash-tag}` extraction is Redis-Cluster
  compatible; CRC16 slot hashing exists and is test-verified against Redis vectors.

## 2. Vertical scale (scale-up)

Moon's primary scaling axis. Guidance and invariants:

| Lever | Effect | Caveat |
|---|---|---|
| `--shards N` (thread-per-core) | Linear for pipelined + hash-tagged workloads | Cross-shard hop ≈10µs structural residual; non-pipelined random-key workloads prefer fewer shards until L4 lands |
| `--io-busy-poll-us 40` | p=1 win vs Redis (x86 1.66×) | Only on pinned, disjoint cores; regression on shared cores |
| Hash tags `{tenant}` | Removes cross-shard dispatch for MGET/MSET | Application key-design discipline |
| NUMA pinning + `numa::system_parallelism()` | Avoids the pinned-thread parallelism trap | Never trust `available_parallelism()` from a pinned thread |
| jemalloc decay tuning (baked `_rjem_malloc_conf`) | RSS returns to OS quickly | `--memory-arenas-cap` no-ops if operator env sets `_RJEM_MALLOC_CONF` |
| Disk offload (vector HOT→WARM→COLD) | −26% RSS measured | KV cold tier still has deferred perf items (PageCache dormant, blocking cold reads) |

**Open work (roadmap v0.7):** L4 lock-free cross-shard reads — removes the last multi-shard tax on
non-pipelined workloads and retires the shardslice waiver (expires 2026-08-01).

## 3. Replication (master → replica), target design

### 3.1 What exists (GA today)
Single-shard master PSYNC2: full/partial resync via per-shard `ReplicationBacklog`, replica apply
loop routing frames through `key_to_shard`, exponential-backoff reconnect, `REPLICAOF NO ONE`
promotion. Replicas may run any `--shards M` (fan-in works); masters are capped at `--shards 1`.

### 3.2 v0.7 target: multi-shard master PSYNC ("one wire view over N logs")

```
 master (shards 0..N)                                  replica (shards 0..M)
 ┌────────┐ WAL v3 stream 0 ─┐                       ┌─► apply → shard j = key_to_shard
 │shard 0 │                  │   ┌──────────────┐    │
 ├────────┤ WAL v3 stream 1 ─┼──►│ PSYNC mux:   │────┤   single TCP conn, PSYNC2 framing
 │shard 1 │                  │   │ interleave by│    │   headers carry (shard_id, lsn)
 ├────────┤        …         │   │ global LSN   │    └─► ACK: REPLCONF ACK <global-offset>
 │shard N │ WAL v3 stream N ─┘   └──────────────┘
 └────────┘
```

Decisions (locking in the RFC's Option 1B direction):
1. **Transport**: one PSYNC2 connection per replica; frames tagged with `shard_id` so the replica's
   apply loop dispatches to its local shard by key (replica shard count independent of master's).
2. **Full sync**: `ShardMessage::PrepareReplicaSync` triggers cooperative per-shard snapshot
   (forkless RDB v2 machinery), streamed as one RRDSHARD-format payload. Redis-RDB conversion for
   heterogeneous (real-Redis) replicas stays a non-goal until demanded.
3. **Partial resync**: eligibility = every per-shard backlog still covers the replica's per-shard
   offsets (existing `evaluate_psync` already reasons across all backlogs — extend, don't replace).
4. **ACK plumbing (blocking bug today)**: master's post-PSYNC loop must `select!` on socket reads,
   parse `REPLCONF ACK`, and store into `ReplicaInfo.ack_offsets` (today initialized to 0 and never
   written). `WAIT numreplicas timeout` then compares acked global offset ≥ issue-time offset.
5. **Semi-sync tier (v0.8)**: `WAIT`-gated writes give per-command quorum; a config knob
   (`min-replicas-to-write` parity) gives server-side enforcement. No consensus protocol in the
   data path — Moon stays async-replication + quorum-ack, like Redis, by design.
6. **Cascading replicas (v0.8+)**: replica re-exposes its applied stream as a master; needs
   `repl_id2` lineage handling already present in PSYNC2 state.

### 3.3 Failure semantics (document and test, never imply more)

| Mode | Guarantee |
|---|---|
| `appendonly yes` + `always` + `WAIT 1` | RPO=0 for acknowledged writes across single-node loss |
| `everysec` + async replication | RPO ≤ 1s local, ≤ replication lag remote |
| Replica promotion (manual or cluster failover) | Acknowledged-but-unreplicated writes may be lost — bounded by `WAIT`/semi-sync usage |

## 4. Cluster mode (scale-out), path to GA

### 4.1 What exists (alpha)
16,384-slot model, CRC16 keyslots, gossip bus (`port+10000`, PING/PONG/MEET, PFAIL→FAIL consensus),
MOVED/ASK routing, MIGRATING/IMPORTING slot states, `nodes.conf` persistence, Raft-like quorum
failover election with epoch fencing and replica-rank jitter. ~3,600 lines, unit-tested, protocol-compatible.

### 4.2 Blockers to GA (v0.8 workstreams)

1. **Runtime gap**: bus/gossip/failover tasks spawn only in the tokio startup block; the monoio
   (production) path must wire them (dedicated std-thread tokio-current-thread island, or monoio-native
   port of the ticker — monoio variants of gossip functions already exist).
2. **Slot migration under load**: verify/complete the per-key transfer loop (arch review confidence
   was 0.55 that live MIGRATE dispatch is complete); then soak: migration during writes, ASK
   correctness, crash mid-migration (both directions), migration of hash-tagged graph units.
3. **Failover ↔ replication integration**: winning an election must update the promoted node's
   `ReplicationRole` + `repl_id/repl_id2` so clients and remaining replicas PSYNC2-continue.
4. **Client compatibility**: `CLUSTER SHARDS` (Redis 7), `cluster-announce-ip/port` (NAT/k8s),
   `cluster-config-file` path control, cluster-aware client CI (redis-py/go-redis/redis-rs cluster modes).
5. **Multi-engine slot semantics**: graphs are single-slot by design (hash the graph name — good);
   FT indexes are keyspace-global — define per-slot document ownership: index entries migrate with
   their hash's slot; FT.SEARCH scatter-gathers across cluster nodes (v0.9, after single-cluster GA).

### 4.3 Verification bar (jepsen-style, CI-owned)

- 3×3 cluster: kill -9 a master → auto-failover < 2× node_timeout, zero acknowledged-write loss with WAIT.
- Network partition (iptables) minority/majority: minority masters stop accepting writes (FAIL flag),
  heal → converge, no split-brain epoch regressions.
- 72h soak with migration churn + client load; assert slot-coverage invariant continuously.
- Fuzz: `gossip_deser` target already exists — extend with failover-message state-machine fuzzing.

## 5. Durability & recovery (RPO/RTO contract)

| Config | RPO (crash) | RTO | Notes |
|---|---|---|---|
| `everysec` (default) | ≤1s | WAL replay, seconds–minutes | group commit ships; idle-fsync fixed (PR #233) |
| `always` + group commit | 0 | same | 0.91× Redis at P16 — near-parity after PRs #239–242 |
| + replica, `WAIT 1` | 0 across node loss | promote: seconds | v0.7 makes this real |
| + cluster auto-failover | 0 (WAIT-acked) | < 2× node_timeout, no operator | v0.8 |
| Snapshots (BGSAVE RDB v2) | snapshot age | load snapshot + WAL tail | 6-phase recovery, FPI, CLOG rollback |
| PITR (v0.9) | any LSN/timestamp | WAL replay to point | machinery exists; needs CLI + retention policy |

Standing invariants:
- WAL v3 is the ordering authority; AOF-authority recovery decision (PR #236/#238) stands.
- `--wal-kv-log` gate prevents double-logging (KV in both WAL and AOF) — keep default off for KV.
- Diskfull guard (5% free) pauses writes loudly (`MOONERR diskfull`) — never silent loss.
- Multi-shard BGREWRITEAOF stays gated until the v0.7 replication soak also proves rewrite safety.

## 6. Fault-tolerance matrix (behavior we commit to)

| Failure | Today | Target (v0.8) |
|---|---|---|
| Process kill -9 | Recovery replays WAL, zero acked loss at `always`; kill-9 suites green (KV+vector+graph) | unchanged |
| Single shard thread panic | Process aborts (fail-fast, shared-nothing means no partial state) | unchanged, documented |
| Disk full | Write pause + MOONERR | + cluster: replica promotion if master degraded |
| Node loss (replicated) | Manual REPLICAOF NO ONE | Auto-failover via cluster election |
| Network partition | N/A single-node | Majority-side availability; minority read-only/FAIL |
| Slow/hung replica | Backlog overflow → full resync on reconnect | + backlog sizing metrics, replica-lag alerts |
| Corrupted WAL/AOF tail | CRC32C detection, truncate-at-corruption recovery, runbook | + audit event |
| Clock skew | No wall-clock ordering dependency (LSN-based) | keep it that way (no TTL-based fencing) |

## 7. Deployment topologies (documented + CI-tested tiers)

1. **Standalone** (`--profile standalone`) — dev/edge; everysec durability. *(GA today)*
2. **HA pair** — master (shards N) + replica (shards M), WAIT-gated writes, orchestrator-driven
   failover (systemd/k8s). *(GA at v0.7)*
3. **Cluster 3×3** — three masters × one replica each, auto-failover, slot rebalancing. *(GA at v0.8)*
4. **Multi-DC DR** — cluster per DC + async cross-DC replica feed, observer promotion. *(v0.9/EE)*
5. **Kubernetes** — Helm/StatefulSet (v0.8) → operator with CRDs for backup/upgrade/failover (v0.9;
   fleet features EE).

## 8. Design principles (constraints on all future work)

1. **Never trade the shared-nothing core** — no global locks on the write path, ever; replication
   and cluster attach at the WAL/event-loop seams, not inside engines.
2. **One log to ship** — every new engine writes WAL v3 records; replication/CDC/PITR come from the
   same stream. (CDC's Debezium decoder already proves the pattern.)
3. **Truthful guarantees** — WAIT that returns 0 replicas must mean 0; every durability claim maps
   to a crash-matrix CI test; publish RPO/RTO tables, not adjectives.
4. **Both runtimes or explicitly gated** — anything HA-critical (bus, gossip, failover, PSYNC) must
   run under monoio (production) and tokio (CI); no more tokio-only control planes.
5. **Bench on pinned Linux hardware, n≥3, both arches** — per the established benchmarking discipline;
   spin/poll optimizations are judged only on pinned disjoint cores.

---

## 9. Deep dive: multi-shard PSYNC wire format (v0.7 spec draft)

Capability negotiation keeps the Redis-shaped wire for single-shard and upgrades only between
Moon peers:

```
replica → master:  REPLCONF CAPA eof capa psync2 capa moon-multishard-1
master  → replica: +FULLRESYNC <replid> <global-offset> shards=<N>
```

Full sync payload (one TCP stream):

```
+------------------+----------------------------------------------+
| SYNC MANIFEST    | magic "MSYN1", shard_count, per-shard         |
|                  | (shard_id, snapshot_lsn, byte_len)            |
+------------------+----------------------------------------------+
| RRDSHARD seg 0   | existing forkless RDB v2 per-shard format     |
| ...              | (streamed sequentially; replica routes each   |
| RRDSHARD seg N-1 | key via its own key_to_shard — master/replica |
|                  | shard counts are independent)                 |
+------------------+----------------------------------------------+
```

Steady-state stream frames (after full/partial resync):

```
+--------+----------+------------+--------------------------------+
| len u32| shard u16| lsn u64    | WAL v3 record bytes (verbatim, |
|        |          |            | CRC32C already inside record)  |
+--------+----------+------------+--------------------------------+
```

- The mux task (listener runtime) merge-reads N backlog cursors ordered by global LSN; per-shard
  ordering is what correctness requires (cross-shard ordering has no invariant to preserve —
  cross-shard atomicity is XactCommitV2's job, whose records carry their own coordination).
- Partial resync: `PSYNC <replid> <global-offset>` plus `REPLCONF SHARD-OFFSETS o0 o1 … oN-1`;
  master grants `+CONTINUE` only if **every** per-shard backlog covers its offset (extends the
  existing all-backlogs check in `evaluate_psync`).
- ACK: `REPLCONF ACK <global> SHARDS o0 … oN-1` every second and on demand (GETACK). `WAIT`
  compares global acked offsets; DR observers (EE) read the same channel.

### 9.1 Backlog sizing math

`repl-backlog-size` default today is per-shard. Rule of thumb to survive a replica
disconnect of `T` seconds at write bandwidth `W` MB/s spread over `S` shards with hot-key skew
factor `k` (worst shard share): `per_shard_backlog ≥ T × W × k`. Defaults to ship: 64 MiB/shard,
auto-grow to 256 MiB under sustained fan-out, Prometheus gauges
`moon_repl_backlog_bytes{shard}` + `moon_repl_partial_resync_denied_total` to tune it in the field.

## 10. Deep dive: failover timeline & safety (v0.8)

```
t0      master stops PONGing (crash / partition)
t0+NT   peers mark PFAIL (node_timeout NT, gossip-observed)
t0+NT+ε quorum of masters agrees → FAIL flag broadcast
        replicas of failed master: delay = 500ms + rank×1s + jitter   (rank = replication offset order,
        best-offset replica moves first — already implemented in compute_failover_delay)
t1      candidate bumps currentEpoch, requests FailoverAuth from masters
t1+δ    quorum ACKs (epoch-fenced, one vote per epoch per master)
t2      winner: takes over slots, rolls repl_id→repl_id2 (PSYNC2 continuity),
        broadcasts PONG with new config epoch
t2+γ    other replicas re-attach via partial resync; clients follow MOVED
```

Commitments: `RTO ≤ 2×node_timeout + 2s` under quorum reachability; a partitioned minority master
stops accepting writes when it cannot reach quorum within `node_timeout` (prevents split-brain
writes beyond the async-replication window). Both become CI assertions in the jepsen-lite suite,
not doc prose.

**Fencing invariants** (each gets a dedicated chaos test):
1. One FailoverAuth vote per master per epoch (`failover_vote_eligible` — exists, needs partition tests).
2. Slot ownership changes only with a higher config epoch; stale-epoch UPDATE messages rejected.
3. Promoted replica must have applied ≥ the offset it advertised when soliciting votes.
4. Old master rejoining with stale epoch demotes itself to replica of the new owner.

## 11. Observability catalog for HA (ship with the features, not after)

| Metric / INFO field | Meaning | Alert rule of thumb |
|---|---|---|
| `moon_repl_lag_bytes{replica}` / `master_repl_offset - acked` | replication lag | > backlog/2 sustained 60s |
| `moon_repl_link_status{replica}` | up/down | down > 30s |
| `moon_repl_full_resyncs_total` | full syncs (expensive) | > 1/hour |
| `moon_wait_timeouts_total` | WAIT quorum misses | any sustained rate |
| `moon_cluster_state` (ok/fail), `moon_cluster_slots_unassigned` | cluster health | != ok, > 0 |
| `moon_failover_elections_total`, `moon_failover_duration_seconds` | election churn/latency | elections > 0/day unexpected |
| `moon_slot_migration_active`, `_keys_remaining` | migration progress | stalled 10min |
| `moon_aof_fsync_lag_seconds`, `moon_wal_group_commit_batch` | durability health | fsync lag > 2s at everysec |
| `moon_license_days_remaining` (EE) | entitlement | < 30 |

## 12. Decision log

| # | Decision | Rationale | Date |
|---|---|---|---|
| D-1 | Async replication + quorum-ACK (`WAIT`), no consensus in the data path | Redis-compatible semantics; consensus reserved for control plane (elections) only | 2026-07-09 |
| D-2 | Cluster control plane on a dedicated std-thread tokio island under monoio | Control plane is not latency-critical; avoids porting bus/gossip to monoio now | 2026-07-09 |
| D-3 | Replica shard count independent of master's (route-on-apply) | Enables resize-via-replica-swap as the standalone vertical-resize story | 2026-07-09 |
| D-4 | RRDSHARD (Moon-native) full-sync format; no Redis-RDB conversion for replicas | Real-Redis replicas are a non-goal until demanded; conversion path documented as future work (`master.rs:148,312` TODOs) | 2026-07-09 |
| D-5 | Multi-shard wire is capability-negotiated; single-shard wire stays Redis-shaped | Preserves ecosystem tooling (redis-cli --rdb etc.) on the simple topology | 2026-07-09 |
