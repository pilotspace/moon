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
    - WS.\*/MQ.\* planes are **not replicated yet** (the master logs one warning
      when a replica is attached); vector/text/graph planes replicate fully.

### Set up a replica

### Start the leader (must be --shards 1 in v0.1.x)

```bash
./target/release/moon --port 6379 --shards 1
```

### Start the replica (any shard count)

```bash
./target/release/moon --port 6380 --shards 4
```

### Connect the replica to the leader

```bash
redis-cli -p 6380 REPLICAOF 127.0.0.1 6379
```

### Verify link status

```bash
redis-cli -p 6380 INFO replication | grep master_link_status
# Expect: master_link_status:up
```

### Replication features

- **PSYNC2 protocol** — compatible with Redis replication clients
- **Per-shard WAL streaming** — each shard streams its own WAL independently (once connected)
- **Partial resync** — reconnecting replicas resume from where they left off via the replication backlog
- **Lazy backlog** — the replication backlog is only allocated when the first replica handshake begins (REPLCONF), saving ~12 MB baseline memory

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
