# Runbook: OOM During Snapshot (BGSAVE)

## Symptoms

- Moon process killed by OOM killer during BGSAVE
- `dmesg | grep oom` shows moon process
- Snapshot file is incomplete or missing

## Root Cause

BGSAVE requires serializing all data to disk. Unlike Redis (which forks), Moon uses forkless compartmentalized snapshots, but the serialization buffers can spike memory usage.

## Recovery Steps

### Step 1: Restart Moon

```bash
# Moon should recover from WAL/AOF on restart
./moon --dir <dir> --appendonly yes --port 6379
```

### Step 2: Verify data integrity

```bash
redis-cli -p 6379 DBSIZE
redis-cli -p 6379 INFO persistence
```

### Step 3: Address the OOM root cause

```bash
# Check current memory usage
redis-cli -p 6379 INFO memory

# Option A: Increase available memory
# Option B: Set maxmemory to leave headroom for snapshots
#   Rule of thumb: maxmemory = 75% of available RAM
redis-cli -p 6379 CONFIG SET maxmemory <bytes>

# Option C: Use AOF-only persistence (no BGSAVE spikes)
redis-cli -p 6379 CONFIG SET save ""
```

### Step 4: Monitor

- Set up RSS alerts at 80% of available memory
- Monitor `moon_rss_bytes` Prometheus metric (if admin port enabled)
