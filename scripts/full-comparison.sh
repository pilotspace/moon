#!/bin/bash
# Full comparison: Moon monoio vs Redis — all configs, all pipeline depths
# Each service runs alone with CPU pinning
set -euo pipefail
exec > ~/full-comparison.log 2>&1
set -x

R=~/full-results
rm -rf "$R"; mkdir -p "$R" /tmp/moon-data /tmp/redis-data
ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || true

cleanup() {
    pkill -9 -f 'target/release/moon' 2>/dev/null || true
    pkill -9 -f redis-server 2>/dev/null || true
    sleep 2
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>/dev/null || true
    sleep 1
}

wait_port() {
    for i in $(seq 1 30); do
        redis-cli -p "$1" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.5
    done
    echo "TIMEOUT port $1" && return 1
}

# Bench with progressive clients: c=1, c=5, c=10, c=50
bench_full() {
    local label=$1 port=$2
    echo "--- $label ---"
    # Warmup
    taskset -c 4-7 redis-benchmark -p "$port" -c 10 -n 50000 -P 16 -t set -d 64 -q > /dev/null 2>&1
    sleep 1
    # All combos
    for c in 1 5 10 50; do
        for p in 1 8 16 32 64; do
            local n=$((c * p * 200))
            [ $n -lt 5000 ] && n=5000
            [ $n -gt 500000 ] && n=500000
            timeout 20 taskset -c 4-7 redis-benchmark -p "$port" -c $c -n $n -P $p -t set,get -d 64 --csv -q 2>&1 | \
                grep -v WARNING | sed "s/^/c=$c,p=$p,/" | tee -a "$R/${label}.csv"
        done
    done
    echo ""
}

echo "=== SYSTEM ==="
echo "CPU: $(lscpu | grep 'Model name' | awk -F: '{print $2}' | xargs)"
echo "Cores: $(nproc)"
echo "Kernel: $(uname -r)"
date -u
echo ""

cleanup

########################################
# REDIS
########################################

echo '########## REDIS NO PERSIST ##########'
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379
bench_full "redis-nopersist" 6379
redis-cli -p 6379 INFO memory | grep used_memory_human >> "$R/redis-nopersist-mem.txt"
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

echo '########## REDIS AOF EVERYSEC ##########'
rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
wait_port 6379
bench_full "redis-aof-everysec" 6379
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

########################################
# MOON MONOIO
########################################

echo '########## MOON MONOIO 1 SHARD NO PERSIST ##########'
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3
bench_full "moon-monoio-s1-nopersist" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

echo '########## MOON MONOIO 4 SHARDS NO PERSIST ##########'
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 4 --protected-mode no > /dev/null 2>&1 &
sleep 3
bench_full "moon-monoio-s4-nopersist" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

echo '########## MOON MONOIO 1 SHARD AOF EVERYSEC ##########'
rm -rf /tmp/moon-data/*
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 3
bench_full "moon-monoio-s1-aof-everysec" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

echo '########## MOON MONOIO 4 SHARDS AOF EVERYSEC ##########'
rm -rf /tmp/moon-data/*
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 4 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 3
bench_full "moon-monoio-s4-aof-everysec" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

########################################
# REPORT
########################################
echo ""
echo "########## ALL RESULTS ##########"
date -u
for f in "$R"/*.csv; do
    [ -f "$f" ] && echo "=== $(basename "$f" .csv) ===" && cat "$f" && echo ""
done
echo "BENCHMARK_COMPLETE"
