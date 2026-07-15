---
title: "Clustering and replication"
description: "Set up replication, cluster mode, and automatic failover."
---

# Clustering and replication

Moon supports Redis-compatible replication and cluster mode for high availability and horizontal scaling.

## Replication

Moon implements PSYNC2-compatible replication with per-shard WAL streaming and partial resync support.

!!! info
    **Supported deployment shape (v0.7):**

    - **Master:** any `--shards N` (multi-core writer). Multi-shard masters serve
      a full resync as ONE merged Redis-format RDB followed by the merged live
      stream from all shards; every record carries its own `SELECT` framing, so
      multi-db workloads replicate exactly. Requires the default `runtime-monoio`
      build — a `runtime-tokio` master answers PSYNC with
      `-ERR PSYNC requires runtime-monoio on the master (this build runs runtime-tokio)`.
    - **Replicas:** `--shards 1` each (scale reads by adding replicas, not
      replica shards). A multi-shard replica refuses to start replication.
    - **Partial resync:** supported on single-shard masters (backlog window);
      a multi-shard master answers every reconnect with a full resync (a single
      scalar offset cannot be mapped back onto N per-shard backlogs).

    **Observability:**
    - `WAIT N timeout` reflects real replica ACKs (1s `REPLCONF ACK` cadence).
    - `master_link_status` in `INFO replication` reflects the handshake state — use it to detect a failed REPLICAOF.
    - `CLIENT LIST TYPE replica` has no predicate yet; returns all clients.
    - **All six data planes replicate** (v0.7 GA): KV, vector/text index, graph,
      workspace (WS.\*), message-queue (MQ.\*), and temporal. A full resync ships
      each plane's snapshot; the live stream carries every plane's effect records.

### Set up a replica

**Topology (v0.7):** the **master** runs any `--shards N` (multi-core writer); each
**replica** runs `--shards 1`. Scale reads by adding replicas, not replica shards.
Replication is initiated at runtime with the `REPLICAOF` command — there is no
startup flag; operators script it after the replica is up (e.g. via an init hook).

#### 1. Start the master (any shard count)

```bash
./target/release/moon --port 6379 --shards 4 --appendonly yes --appendfsync always
```

#### 2. Start the replica (must be --shards 1)

```bash
./target/release/moon --port 6380 --shards 1 --appendonly yes --appendfsync always
```

`--appendfsync always` on the replica is what makes a `WAIT`-acknowledged write
survive a **replica** crash: the replica ACKs when it *applies* a write, not when it
fsyncs, so without durable replica persistence a replica can ACK and then lose the
write on its own crash. Drop it to `everysec` only if the replica is read-scaling/DR
and you accept ≤1 s of replica-side loss.

#### 3. Attach the replica to the master

```bash
redis-cli -p 6380 REPLICAOF 127.0.0.1 6379
```

The replica performs a full resync (one merged Redis-format RDB across all planes),
then applies the live stream. Replicas are **read-only**: writes return
`-READONLY` (`INFO replication` reports `slave_read_only:1`).

#### 4. Verify the link

```bash
redis-cli -p 6380 INFO replication | grep master_link_status
# Expect: master_link_status:up   (anything else = handshake not complete)
```

### Acknowledged writes with WAIT

`WAIT numreplicas timeout` blocks until at least `numreplicas` replicas have ACKed
every write issued on the connection (replicas send `REPLCONF ACK` on a ~1 s
cadence). Combine it with `appendfsync always` on both sides for a **zero-RPO,
cross-node durable write**:

`WAIT` counts ACKs only for writes issued on **its own connection**, so the `SET`
and the `WAIT` must run on the *same* connection — two separate `redis-cli`
invocations open two connections, and the `WAIT` would see no pending write. Use one
interactive session (or pipe both commands into a single `redis-cli`):

```bash
redis-cli -p 6379 <<'EOF'
SET k v
WAIT 1 1000
EOF
# WAIT returns the number of replicas that ACKed the SET within 1000 ms
```

On the master, `INFO replication` lists each replica's `offset` and `lag`; on the
replica it reports `slave_repl_offset`. See the [tuning guide](tuning.md#replication-durability)
for the durability/latency trade-offs.

### Promote a replica (failover)

```bash
redis-cli -p 6380 REPLICAOF NO ONE   # replica stops applying and becomes a writable master
```

Repoint surviving replicas at the new master with `REPLICAOF <new-host> <new-port>`.
There is no automatic replication failover outside cluster mode (see below).

### Read/write splitting

Moon has **no built-in read/write-splitting proxy** — each node is a single server,
and a replica is read-only. Sending a write to a replica returns `-READONLY`. To
serve writes from the master and reads from replicas behind one logical service,
split at the client or with an external Redis-aware proxy. A plain L4/TCP load
balancer **cannot** do this — it can't see commands, so it can't tell a read from a
write; it only helps for failover.

**Client-side (recommended, no extra hop).** Open one connection to the master for
writes and one (or a pool) to the replicas for reads; most clients have a built-in
replica-read mode:

```bash
# writes → master
redis-cli -p 6379 SET session:42 '{"user":"alice"}'
# reads → replica  (eventually consistent — may lag; see the caveat below)
redis-cli -p 6380 GET session:42
```

!!! warning "Replica reads are eventually consistent"
    Replication is **asynchronous**, so a `GET` on a replica issued right after a
    `SET` on the master can return the **old value or a miss** until the write
    streams across (the example above is exactly that race). Route
    **read-after-write** and **session-critical** reads to the *master*; send only
    staleness-tolerant reads (caches, analytics, browse traffic) to replicas. If you
    must read your own writes from a replica, gate the read on catch-up: `WAIT 1
    <timeout>` on the master, then confirm the replica's `slave_repl_offset` (from
    `INFO replication`) has reached the master's `master_repl_offset`.

Most clients expose a **non-cluster** replica-read mode, or you can simply hold
separate master and replica clients:

- **lettuce (Java):** a `MasterReplica` connection with `ReadFrom.REPLICA_PREFERRED`
- **redis-py:** a dedicated replica `Redis(host=<replica>, port=6380)` client for reads
- **ioredis (Node):** separate `Redis` clients for the master and the replica
- **go-redis:** explicit master and replica `redis.NewClient(...)` instances

The cluster-client read-routing modes — redis-py `RedisCluster(read_from_replicas=True)`,
ioredis `scaleReads: "slave"`, go-redis `ClusterOptions{ReadOnly, RouteRandomly}` —
apply only in **[Cluster mode](#cluster-mode)**, not to a standalone master/replica pair.

Keep write-path connections pointed at the master so `WAIT`-based cross-node
durability still works — `WAIT` only counts ACKs for writes issued on the master.

**External proxy (one endpoint).** Front the pair with a **command-aware** RESP proxy
that routes by command flag (writes → master, reads → replica pool): e.g. Envoy's
`redis_proxy` filter with a read policy, or a purpose-built RESP router. The trade-off
is an extra network hop and another component to operate and fail over.

**Failover note.** However you split, reads and writes must re-point when a replica
is promoted (`REPLICAOF NO ONE`) or a master is lost. In a standalone master/replica
pair there is **no built-in topology discovery**: `REPLICAOF NO ONE` promotes a
replica but does not notify clients, and health checks alone only detect
liveness — they cannot tell a client *which* node is the new master. Repointing
therefore requires **Moon's [Cluster mode](#cluster-mode)** (automatic promotion +
`MOVED` redirection) or an **external orchestrator** (k8s/systemd health-managed
endpoints) that rewrites the client's target; a static split does not self-heal.
Moon does not implement the Redis Sentinel protocol.

### Replication features

- **All six data planes** — KV, vector/text index, graph, WS, MQ, temporal (v0.7 GA)
- **PSYNC2 protocol** — compatible with Redis replication clients
- **Per-shard WAL streaming** — each master shard streams its own WAL; a multi-shard master merges them into one exactly-once feed
- **Partial resync** — single-shard masters resume reconnecting replicas from the backlog window; multi-shard masters answer every reconnect with a full resync
- **Lazy backlog** — allocated only when the first replica handshake begins (REPLCONF), saving ~12 MB baseline memory
- **Validated** — 24 h continuous-load kill-9 soak (alternating master/replica restarts), zero loss of any WAIT-acknowledged write

!!! note "Replica TTL semantics (deterministic since v0.7.1)"
    Relative-expire commands (`EXPIRE`, `SETEX`, `PEXPIRE`, `SET … EX/PX`, `GETEX`
    with a relative TTL) are rewritten to absolute deadlines on the master
    (`PEXPIREAT`/`SET … PXAT`) **before** they enter the durable log and replication
    stream, using the master's per-tick cached clock — the exact value its command
    handler stored. The replica (and an AOF replay after a restart) therefore
    reproduces the master's expiry **instant**, not a countdown restarted at apply
    time, so apply delay no longer shifts a key's expiry moment. A replica also no
    longer runs its own active-expiry deletion sweep: it holds a logically-expired
    key resident (reads still see it as gone) until the master streams the
    authoritative removal, so both nodes delete at the same point in the stream.
    Master/replica clock skew is thus the only remaining source of divergence, and it
    affects only the wall-clock instant of deletion, not which node deletes first.

## Cluster mode

Moon implements the Redis Cluster specification with 16,384 hash slots, gossip protocol, and automatic failover.

### Start a cluster

```bash
# Start three nodes
./target/release/moon --port 7000 --cluster-enabled true --shards 2
./target/release/moon --port 7001 --cluster-enabled true --shards 2
./target/release/moon --port 7002 --cluster-enabled true --shards 2

# Join nodes
redis-cli -p 7000 CLUSTER MEET 127.0.0.1 7001
redis-cli -p 7000 CLUSTER MEET 127.0.0.1 7002

# Assign slots (roughly equal distribution)
redis-cli -p 7000 CLUSTER ADDSLOTS {0..5461}
redis-cli -p 7001 CLUSTER ADDSLOTS {5462..10922}
redis-cli -p 7002 CLUSTER ADDSLOTS {10923..16383}
```

### Cluster features

- **16,384 hash slots** with CRC16-based routing
- **Gossip protocol** for node discovery and failure detection
- **MOVED/ASK redirections** for client-side routing
- **Live slot migration** for rebalancing without downtime
- **Majority consensus failover** with automatic promotion

### Hash tags

Use `{tag}` in key names to co-locate related keys on the same slot:

```bash
# All these keys route to the same slot
SET user:{1234}:name "Alice"
SET user:{1234}:email "alice@example.com"
MGET user:{1234}:name user:{1234}:email
```

This eliminates cross-shard dispatch overhead for multi-key operations like MGET and MSET.

### Cluster commands

`CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER MEET`, `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER SETSLOT`, `CLUSTER FAILOVER`, `CLUSTER MYID`

### Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-enabled` | `false` | Enable cluster mode |
| `--cluster-node-timeout` | `15000` | Node timeout in ms before failover |
