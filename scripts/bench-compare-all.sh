#!/bin/bash
# Full comparison: Moon vs Redis vs Qdrant
# Benchmark (throughput) + Recovery (crash consistency)
exec > /tmp/bench-compare.log 2>&1
set -x
ulimit -n 65536 2>/dev/null || true
MOON=$HOME/moon/target/release/moon
R=/tmp/bench-compare-results
rm -rf "$R"; mkdir -p "$R"

cleanup() {
    pkill -9 -f "target/release/moon" 2>/dev/null || true
    pkill -9 -f redis-server 2>/dev/null || true
    pkill -9 -f qdrant 2>/dev/null || true
    sleep 2
}

bench() {
    local label=$1 port=$2 c=$3 p=$4
    local n=$((c * p * 500))
    [ $n -lt 100000 ] && n=100000
    [ $n -gt 1000000 ] && n=1000000
    timeout 45 taskset -c 4-7 redis-benchmark -p "$port" -c $c -n $n -P $p -t set,get -d 64 --csv -q 2>&1 | \
        grep -v WARNING | sed "s/^/c=$c,p=$p,/" >> "$R/${label}.csv"
}

echo "=== SYSTEM ==="
lscpu | grep "Model name"; echo "Cores: $(nproc)"; date -u

############################################
# PART 1: THROUGHPUT BENCHMARK
############################################
echo ""
echo "####################################################"
echo "  PART 1: THROUGHPUT (c=10 p=64, CPU-pinned)"
echo "####################################################"

# --- Redis: No Persist ---
cleanup; rm -rf /tmp/redis-data/*; mkdir -p /tmp/redis-data
echo "--- Redis NoPersist ---"
taskset -c 0-3 redis-server --port 6379 --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1; taskset -c 4-7 redis-benchmark -p 6379 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "redis-np" 6379 $c $p; done; done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; cleanup

# --- Redis: AOF ---
rm -rf /tmp/redis-data/*; mkdir -p /tmp/redis-data
echo "--- Redis AOF ---"
taskset -c 0-3 redis-server --port 6379 --save "" --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1; taskset -c 4-7 redis-benchmark -p 6379 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "redis-aof" 6379 $c $p; done; done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; cleanup

# --- Moon: No Persist ---
rm -rf /tmp/moon-data/*; mkdir -p /tmp/moon-data
echo "--- Moon NoPersist ---"
taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no --dir /tmp/moon-data >/dev/null 2>&1 &
sleep 2; taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "moon-np" 6399 $c $p; done; done
pkill -9 -f "target/release/moon"; cleanup

# --- Moon: AOF ---
rm -rf /tmp/moon-data/*; mkdir -p /tmp/moon-data
echo "--- Moon AOF ---"
taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data >/dev/null 2>&1 &
sleep 2; taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "moon-aof" 6399 $c $p; done; done
pkill -9 -f "target/release/moon"; cleanup

# --- Moon: Disk Offload + AOF ---
rm -rf /tmp/moon-data/* /tmp/moon-offload/*; mkdir -p /tmp/moon-data /tmp/moon-offload
echo "--- Moon Disk Offload ---"
taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no \
    --disk-offload enable --disk-offload-dir /tmp/moon-offload \
    --appendonly yes --appendfsync everysec --dir /tmp/moon-data >/dev/null 2>&1 &
sleep 2; taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "moon-offload" 6399 $c $p; done; done
pkill -9 -f "target/release/moon"; cleanup

############################################
# PART 2: CRASH RECOVERY
############################################
echo ""
echo "####################################################"
echo "  PART 2: CRASH RECOVERY (SIGKILL + verify)"
echo "####################################################"

recovery_test() {
    local name="$1" port="$2" nkeys="$3" start_cmd="$4" recover_cmd="$5"
    echo ""
    echo "--- Recovery: $name ($nkeys keys) ---"
    cleanup; rm -rf /tmp/rc-data /tmp/rc-offload; mkdir -p /tmp/rc-data /tmp/rc-offload

    # Start + insert
    eval "$start_cmd" &
    sleep 3
    if ! redis-cli -p $port PING > /dev/null 2>&1; then
        echo "  SKIP: failed to start"
        return
    fi

    python3 << PYEOF
import redis, time
r = redis.Redis(host='127.0.0.1', port=$port, decode_responses=True)
pipe = r.pipeline(transaction=False)
for i in range($nkeys):
    pipe.set(f'k:{i}', f'v-{i}')
    if (i+1) % 500 == 0:
        pipe.execute()
        pipe = r.pipeline(transaction=False)
pipe.execute()
time.sleep(3)
pre = sum(1 for i in range($nkeys) if r.get(f'k:{i}') is not None)
print(f'  Inserted: {pre}/$nkeys')
PYEOF

    # SIGKILL
    kill -9 $(pgrep -f "port $port" | head -1) 2>/dev/null
    sleep 2

    # Recover
    eval "$recover_cmd" &
    sleep 5
    if ! redis-cli -p $port PING > /dev/null 2>&1; then
        echo "  $name: FAIL (restart failed)"
        cleanup; return
    fi

    python3 << PYEOF
import redis
r = redis.Redis(host='127.0.0.1', port=$port, decode_responses=True)
N = $nkeys
post = sum(1 for i in range(N) if r.get(f'k:{i}') is not None)
correct = sum(1 for i in range(N) if r.get(f'k:{i}') == f'v-{i}')
loss_pct = round((1 - post/N) * 100, 1) if N > 0 else 0
print(f'  {post}/{N} recovered ({correct} correct, {loss_pct}% loss)')
PYEOF
    cleanup
}

# Redis AOF everysec
recovery_test "Redis-AOF-everysec" 6379 5000 \
    "taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync everysec --protected-mode no --daemonize no --loglevel warning --dir /tmp/rc-data" \
    "taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync everysec --protected-mode no --daemonize no --loglevel warning --dir /tmp/rc-data"

# Redis AOF always
recovery_test "Redis-AOF-always" 6379 5000 \
    "taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync always --protected-mode no --daemonize no --loglevel warning --dir /tmp/rc-data" \
    "taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync always --protected-mode no --daemonize no --loglevel warning --dir /tmp/rc-data"

# Moon AOF everysec
recovery_test "Moon-AOF-everysec" 16379 5000 \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/rc-data > /dev/null 2>&1" \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/rc-data > /dev/null 2>&1"

# Moon AOF always
recovery_test "Moon-AOF-always" 16379 5000 \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --appendonly yes --appendfsync always --dir /tmp/rc-data > /dev/null 2>&1" \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --appendonly yes --appendfsync always --dir /tmp/rc-data > /dev/null 2>&1"

# Moon Disk Offload + AOF everysec
recovery_test "Moon-DiskOffload-everysec" 16379 5000 \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --dir /tmp/rc-data > /dev/null 2>&1" \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --dir /tmp/rc-data > /dev/null 2>&1"

# Moon Disk Offload + AOF always
recovery_test "Moon-DiskOffload-always" 16379 5000 \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync always --dir /tmp/rc-data > /dev/null 2>&1" \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync always --dir /tmp/rc-data > /dev/null 2>&1"

# Moon Disk Offload + maxmemory
recovery_test "Moon-DiskOffload+maxmem" 16379 5000 \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --maxmemory 10485760 --maxmemory-policy allkeys-lru --dir /tmp/rc-data > /dev/null 2>&1" \
    "taskset -c 0-3 $MOON --port 16379 --shards 1 --protected-mode no --disk-offload enable --disk-offload-dir /tmp/rc-offload --appendonly yes --appendfsync everysec --maxmemory 10485760 --maxmemory-policy allkeys-lru --dir /tmp/rc-data > /dev/null 2>&1"

############################################
# REPORT
############################################
echo ""
echo "####################################################"
echo "  RESULTS"
echo "####################################################"
date -u

echo ""
echo "=== THROUGHPUT ==="
for f in "$R"/*.csv; do
    label=$(basename "$f" .csv)
    echo "--- $label ---"
    grep "SET\|GET" "$f" | awk -F, '{printf "  %s %s %-5s %12s  p99=%s\n", $1,$2,$3,$4,$7}'
    echo
done

echo "=== RECOVERY ==="
grep "recovered\|Inserted\|SKIP\|FAIL" /tmp/bench-compare.log | grep -v "^+"

echo ""
echo "BENCHMARK_COMPLETE"
