# Rolling Restart (Zero-Downtime Upgrade)

Upgrade Moon binaries across a primary + replica topology without client-visible
downtime.

## Prerequisites

- At least 1 replica configured and in sync with the primary
- New Moon binary available on all nodes
- Clients use a load balancer or Sentinel-aware driver that follows promotions

## Topology

```
[Client] --> [LB / Sentinel]
                |
          +-----+------+
          |            |
      [Primary]   [Replica]
```

## Steps

### 1. Verify replica is in sync

```bash
redis-cli -h replica-host -p 6399 INFO replication
```

Confirm `master_link_status:up` and `master_last_io_seconds_ago` is small (< 2).

### 2. Drain the replica

Remove the replica from the load balancer or mark it as unhealthy so no new
read traffic is routed to it.

```bash
# Example: if using HAProxy
echo "disable server moon-backend/replica-1" | socat stdio /var/run/haproxy.sock
```

Wait for in-flight requests to complete (~5 seconds).

### 3. Stop the replica

```bash
redis-cli -h replica-host -p 6399 SHUTDOWN NOSAVE
# or: kill -TERM $(pidof moon)
```

### 4. Upgrade the replica binary

```bash
cp moon-new /usr/local/bin/moon
chmod +x /usr/local/bin/moon
```

### 5. Start the replica

```bash
moon --port 6399 --shards 4 --replicaof primary-host 6399 &
```

### 6. Wait for sync to complete

```bash
# Poll until replica reports sync complete
while true; do
  STATUS=$(redis-cli -h replica-host -p 6399 INFO replication | grep master_link_status)
  echo "$STATUS"
  echo "$STATUS" | grep -q "up" && break
  sleep 1
done
```

### 7. Promote the replica to primary

```bash
redis-cli -h replica-host -p 6399 REPLICAOF NO ONE
```

Update the load balancer to send writes to the new primary.

```bash
# Example: switch HAProxy backend
echo "enable server moon-backend/replica-1" | socat stdio /var/run/haproxy.sock
```

### 8. Drain the old primary

Remove the old primary from the load balancer.

```bash
echo "disable server moon-backend/primary-1" | socat stdio /var/run/haproxy.sock
```

Wait for in-flight requests to complete (~5 seconds).

### 9. Stop and upgrade the old primary

```bash
redis-cli -h old-primary-host -p 6399 SHUTDOWN NOSAVE
cp moon-new /usr/local/bin/moon
chmod +x /usr/local/bin/moon
```

### 10. Start as replica of the new primary

```bash
moon --port 6399 --shards 4 --replicaof replica-host 6399 &
```

Wait for sync (same as step 6).

### 11. (Optional) Re-promote original primary

If you want the original node to be primary again:

```bash
redis-cli -h old-primary-host -p 6399 REPLICAOF NO ONE
redis-cli -h replica-host -p 6399 REPLICAOF old-primary-host 6399
```

Update the load balancer accordingly.

### 12. Re-enable in load balancer

```bash
echo "enable server moon-backend/primary-1" | socat stdio /var/run/haproxy.sock
```

## Rollback

If the upgraded node fails to start or sync:

1. Stop the upgraded node
2. Restore the old binary: `cp moon-old /usr/local/bin/moon`
3. Start with the old binary
4. Re-add to load balancer

No data loss occurs because the other node was never stopped.

## Notes

- Each step preserves at least one healthy node at all times.
- The `SHUTDOWN NOSAVE` avoids writing an unnecessary RDB snapshot during upgrades.
- If AOF/WAL persistence is enabled, the replica will replay from its own WAL after restart; a full resync from the new primary only happens if the WAL gap is too large.
- For 3+ node topologies, upgrade replicas one at a time before touching the primary.
