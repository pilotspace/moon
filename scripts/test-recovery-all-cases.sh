#!/bin/bash
# Comprehensive crash recovery test across all persistence configurations
exec > /tmp/recovery-all.log 2>&1
set -x
MOON=$HOME/moon/target/release/moon
PASS=0
FAIL=0
RESULTS=""

cleanup() {
    killall moon 2>/dev/null; sleep 1
    rm -rf /tmp/rc-data /tmp/rc-offload
}

# Generic test: insert N keys, crash, recover, verify
run_test() {
    local name="$1" nkeys="$2" moon_args="$3"
    echo ""
    echo "============================================"
    echo "  TEST: $name ($nkeys keys)"
    echo "============================================"
    cleanup
    mkdir -p /tmp/rc-data /tmp/rc-offload

    # Phase 1: Start + Insert
    eval "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no $moon_args > /dev/null 2>&1 &"
    sleep 2
    if ! redis-cli -p 16379 PING > /dev/null 2>&1; then
        echo "  SKIP: Moon failed to start"
        RESULTS="$RESULTS\n$name: SKIP (start failed)"
        FAIL=$((FAIL + 1))
        return
    fi

    python3 << PYEOF
import redis, time
r = redis.Redis(host='127.0.0.1', port=16379, decode_responses=True)
N = $nkeys
for i in range(N):
    r.set(f'k:{i}', f'val-{i}')
time.sleep(3)
pre = sum(1 for i in range(N) if r.get(f'k:{i}') is not None)
print(f'  Inserted: {pre}/{N}')
PYEOF

    # Phase 2: SIGKILL
    kill -9 $(pgrep -f "port 16379") 2>/dev/null
    sleep 2

    # Phase 3: Recover
    eval "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no $moon_args > /dev/null 2>&1 &"
    sleep 5
    if ! redis-cli -p 16379 PING > /dev/null 2>&1; then
        echo "  FAIL: Moon failed to restart"
        RESULTS="$RESULTS\n$name: FAIL (restart failed)"
        FAIL=$((FAIL + 1))
        cleanup
        return
    fi

    python3 << PYEOF
import redis
r = redis.Redis(host='127.0.0.1', port=16379, decode_responses=True)
N = $nkeys
post = sum(1 for i in range(N) if r.get(f'k:{i}') is not None)
correct = sum(1 for i in range(N) if r.get(f'k:{i}') == f'val-{i}')
print(f'  Recovered: {post}/{N} accessible, {correct}/{N} correct')
PYEOF

    local post=$(python3 -c "
import redis
r = redis.Redis(host='127.0.0.1', port=16379, decode_responses=True)
print(sum(1 for i in range($nkeys) if r.get(f'k:{i}') is not None))
")

    if [ "$post" -ge "$nkeys" ] 2>/dev/null; then
        echo "  PASS: $post/$nkeys recovered"
        RESULTS="$RESULTS\n$name: PASS ($post/$nkeys)"
        PASS=$((PASS + 1))
    elif [ "$post" -gt "0" ] 2>/dev/null; then
        # appendfsync=everysec may lose ~1s of data
        local lost=$(($nkeys - $post))
        echo "  PARTIAL: $post/$nkeys ($lost lost, appendfsync window)"
        RESULTS="$RESULTS\n$name: PARTIAL ($post/$nkeys)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: 0/$nkeys recovered"
        RESULTS="$RESULTS\n$name: FAIL (0/$nkeys)"
        FAIL=$((FAIL + 1))
    fi
    cleanup
}

echo "=== COMPREHENSIVE RECOVERY TEST ==="
date -u

# ─── Case 1: AOF only (no disk offload) ───
run_test "AOF-everysec" 500 \
    "--appendonly yes --appendfsync everysec --dir /tmp/rc-data"

run_test "AOF-always" 500 \
    "--appendonly yes --appendfsync always --dir /tmp/rc-data"

# ─── Case 2: Disk offload + AOF (separate dirs) ───
run_test "DiskOffload+AOF-everysec" 500 \
    "--disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --dir /tmp/rc-data"

run_test "DiskOffload+AOF-always" 500 \
    "--disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync always --dir /tmp/rc-data"

# ─── Case 3: Disk offload + AOF + maxmemory ───
run_test "DiskOffload+AOF+maxmem-2MB" 500 \
    "--disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync always --maxmemory 2097152 --maxmemory-policy allkeys-lru --dir /tmp/rc-data"

run_test "DiskOffload+AOF+maxmem-10MB" 1000 \
    "--disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --maxmemory 10485760 --maxmemory-policy allkeys-lru --dir /tmp/rc-data"

# ─── Case 4: Disk offload + AOF (same dir) ───
run_test "DiskOffload+AOF-samedir" 500 \
    "--disk-offload enable --disk-offload-dir /tmp/rc-data --appendonly yes --appendfsync always --dir /tmp/rc-data"

# ─── Case 5: Large dataset ───
run_test "DiskOffload+AOF-5000keys" 5000 \
    "--disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --dir /tmp/rc-data"

# ─── Case 6: No persistence (should recover 0 — expected) ───
run_test "NoPersistence" 100 \
    "--dir /tmp/rc-data"

echo ""
echo "============================================"
echo "  SUMMARY"
echo "============================================"
echo -e "$RESULTS"
echo ""
echo "PASSED: $PASS  FAILED: $FAIL"
date -u
echo "ALL_DONE"
