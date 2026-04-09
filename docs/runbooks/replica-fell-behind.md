# Runbook: Replica Fell Behind

## Symptoms

- `INFO replication` shows increasing `repl_backlog_first_byte_offset` gap
- Replica returns stale data
- Replication lag metric (`moon_replication_lag_bytes`) growing

## Root Cause

Replica is consuming the replication stream slower than the master produces it. Common causes:
- Network bandwidth limitation between master and replica
- Replica under heavy read load (blocking the replication loop)
- Replica disk I/O bottleneck (persistence writes competing with replication)

## Recovery Steps

### Step 1: Check replication status

```bash
# On master
redis-cli -p 6379 INFO replication

# On replica
redis-cli -p 6380 INFO replication
```

### Step 2: If replica is still connected (partial sync possible)

Wait — the replica will catch up if the backlog hasn't overflowed.

### Step 3: If replica disconnected (backlog overflow)

The replica needs a full resync:
```bash
# On replica — force reconnection
redis-cli -p 6380 REPLICAOF NO ONE
redis-cli -p 6380 REPLICAOF <master_host> <master_port>
```

### Step 4: If full resync is too slow

```bash
# Option A: Increase replication backlog (future)
# Moon does not yet support repl-backlog-size configuration.
# When implemented, restart the primary with a larger backlog:
#   moon --port 6379 --shards 4 --repl-backlog-size 64mb

# Option B: Rebuild replica from scratch
redis-cli -p 6379 BGSAVE
# Copy RDB to replica, restart replica with the snapshot
```

### Step 5: Prevent recurrence

- Size the replication backlog to hold 2x the maximum expected write volume during a partition
- Monitor replication lag metric in Prometheus
- Ensure replica has sufficient CPU/disk bandwidth
