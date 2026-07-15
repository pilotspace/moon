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
./target/release/moon --port 6380 --shards 1 --appendonly yes
```

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

```bash
redis-cli -p 6379 SET k v
redis-cli -p 6379 WAIT 1 1000     # returns the number of replicas that ACKed within 1000 ms
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

### Replication features

- **All six data planes** — KV, vector/text index, graph, WS, MQ, temporal (v0.7 GA)
- **PSYNC2 protocol** — compatible with Redis replication clients
- **Per-shard WAL streaming** — each master shard streams its own WAL; a multi-shard master merges them into one exactly-once feed
- **Partial resync** — single-shard masters resume reconnecting replicas from the backlog window; multi-shard masters answer every reconnect with a full resync
- **Lazy backlog** — allocated only when the first replica handshake begins (REPLCONF), saving ~12 MB baseline memory
- **Validated** — 24 h continuous-load kill-9 soak (alternating master/replica restarts), zero loss of any WAIT-acknowledged write

!!! warning "Replica TTL semantics (v0.7)"
    Relative-expire commands (`EXPIRE`, `SETEX`, `PEXPIRE`, `GETEX` with a relative
    TTL) replicate verbatim rather than being rewritten to absolute `PEXPIREAT`, and
    replicas run their own active-expiry cycle. Under normal clock sync keys expire
    correctly on both sides, but master/replica clock skew can shift a relative-TTL
    key's expiry moment by up to that skew. For exact cross-node expiry parity, set
    absolute deadlines with `PEXPIREAT`. Absolute-rewrite + role-gated expiry land in
    v0.7.1.

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
