#!/bin/bash
# Final 3-tier benchmark: In-Memory / AOF / Disk Offload
# c=10 p=64 N=500K (stable), plus c=1/c=50 key configs
exec > /tmp/bench-final.log 2>&1
set -x
ulimit -n 65536 2>/dev/null || true
MOON=$HOME/moon/target/release/moon
R=/tmp/bench-final-results
rm -rf "$R"; mkdir -p "$R" /tmp/moon-data /tmp/redis-data /tmp/moon-offload

cleanup() {
    pkill -9 -f "target/release/moon" 2>/dev/null || true
    pkill -9 -f redis-server 2>/dev/null || true
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
# TIER 1: IN-MEMORY
############################################
echo ""; echo "========== TIER 1: IN-MEMORY =========="

cleanup; rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
taskset -c 4-7 redis-benchmark -p 6379 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "T1-redis" 6379 $c $p; done; done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; cleanup

rm -rf /tmp/moon-data/*
taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no --dir /tmp/moon-data >/dev/null 2>&1 &
sleep 2
taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "T1-moon" 6399 $c $p; done; done
pkill -9 -f "target/release/moon"; cleanup

############################################
# TIER 2: AOF EVERYSEC
############################################
echo ""; echo "========== TIER 2: AOF EVERYSEC =========="

cleanup; rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save "" --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
taskset -c 4-7 redis-benchmark -p 6379 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "T2-redis-aof" 6379 $c $p; done; done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; cleanup

rm -rf /tmp/moon-data/*
taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data >/dev/null 2>&1 &
sleep 2
taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "T2-moon-aof" 6399 $c $p; done; done
pkill -9 -f "target/release/moon"; cleanup

############################################
# TIER 3: DISK OFFLOAD + AOF (maxmem=200MB)
############################################
echo ""; echo "========== TIER 3: DISK OFFLOAD =========="

cleanup; rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data --maxmemory 209715200 --maxmemory-policy allkeys-lru
sleep 1
taskset -c 4-7 redis-benchmark -p 6379 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "T3-redis" 6379 $c $p; done; done
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null; cleanup

rm -rf /tmp/moon-data/* /tmp/moon-offload/*
taskset -c 0-3 $MOON --port 6399 --shards 1 --protected-mode no \
    --maxmemory 209715200 --maxmemory-policy allkeys-lru \
    --disk-offload enable --disk-offload-dir /tmp/moon-offload \
    --appendonly yes --appendfsync everysec --dir /tmp/moon-data >/dev/null 2>&1 &
sleep 2
taskset -c 4-7 redis-benchmark -p 6399 -c 10 -n 100000 -P 64 -t set -d 64 -q >/dev/null 2>&1; sleep 1
for c in 1 10 50; do for p in 1 16 64; do bench "T3-moon-offload" 6399 $c $p; done; done
echo "Offload: $(du -sh /tmp/moon-offload/ 2>/dev/null | cut -f1)"
pkill -9 -f "target/release/moon"; cleanup

############################################
# REPORT
############################################
echo ""; echo "########## FINAL 3-TIER RESULTS ##########"; date -u
for f in "$R"/*.csv; do
    echo "=== $(basename "$f" .csv) ==="
    cat "$f"
    echo ""
done
echo "BENCHMARK_COMPLETE"
