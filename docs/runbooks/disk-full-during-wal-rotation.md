# Runbook: Disk Full During WAL Rotation

## Symptoms

- Moon logs: `Error: WAL segment rotation failed: No space left on device`
- Write commands start returning errors
- AOF/WAL directory fills the partition

## Root Cause

WAL v3 rotates segment files when they reach the configured size. If the disk partition is full, the new segment file cannot be created.

## Recovery Steps

### Step 1: Free disk space immediately

```bash
# Check disk usage
df -h <dir>

# Remove old WAL segments (if Moon is not running)
ls -la <dir>/wal-v3/
# Sealed segments older than the latest checkpoint can be removed

# Remove old RDB snapshots
ls -la <dir>/dump.rdb*
```

### Step 2: Restart Moon

```bash
./moon --dir <dir> --appendonly yes --port 6379
```

### Step 3: Trigger compaction

```bash
# Compact AOF to reclaim space
redis-cli -p 6379 BGREWRITEAOF
```

### Step 4: Prevent recurrence

- Monitor disk space with alerts at 70% and 85% usage
- Set `--max-wal-size` to bound WAL growth
- Place WAL on a dedicated partition
- Enable disk-offload to tier cold data to NVMe
