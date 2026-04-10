# Runbook: Corrupted AOF Recovery

## Symptoms

- Moon fails to start with: `Error: AOF file corrupted at offset N`
- Moon starts but reports partial data loss in logs

## Root Cause

AOF file has corrupted bytes, typically from:
- Power loss during `appendfsync=no` or `everysec`
- Disk full during AOF write
- Filesystem corruption

## Recovery Steps

### Step 1: Identify the corruption

```bash
# Check AOF file integrity
ls -la <dir>/appendonly.aof
# Look for the error offset in Moon's startup log
RUST_LOG=moon=debug ./moon --dir <dir> --appendonly yes 2>&1 | grep -i corrupt
```

### Step 2: Attempt automatic recovery

Moon's AOF loader truncates at the first corrupted record and loads everything before it:
```bash
# Start normally — Moon will load valid prefix and log truncation point
./moon --dir <dir> --appendonly yes --port 6379
```

### Step 3: If automatic recovery fails

```bash
# Back up the corrupted file
cp <dir>/appendonly.aof <dir>/appendonly.aof.corrupt

# Use redis-check-aof equivalent (if available) or truncate manually
# Find the last valid \r\n boundary before the corruption offset
head -c <offset> <dir>/appendonly.aof > <dir>/appendonly.aof.fixed
mv <dir>/appendonly.aof.fixed <dir>/appendonly.aof

# Restart
./moon --dir <dir> --appendonly yes
```

### Step 4: Verify data integrity

```bash
redis-cli -p 6379 DBSIZE
redis-cli -p 6379 INFO keyspace
```

### Step 5: Prevent recurrence

- Use `appendfsync=always` for zero-loss (at write throughput cost)
- Use `appendfsync=everysec` for ≤1s loss window (recommended)
- Monitor disk space (alert at 80% usage)
- Use UPS/battery-backed storage for production
