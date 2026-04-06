#!/bin/bash
# Full comparison v2: skips known-timeout combos for Moon monoio (c>1, p<64)
set -euo pipefail
exec > ~/full-comparison-v2.log 2>&1
set -x

R=~/full-results-v2
rm -rf "$R"; mkdir -p "$R" /tmp/moon-data /tmp/redis-data
ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || true

cleanup() {
    pkill -9 -f 'target/release/moon' 2>/dev/null || true
    pkill -9 -f redis-server 2>/dev/null || true
    sleep 2
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>/dev/null || true
}

echo "=== SYSTEM ==="
echo "CPU: $(lscpu | grep 'Model name' | awk -F: '{print $2}' | xargs)"
echo "Cores: $(nproc), Kernel: $(uname -r)"
date -u

########################################
# REDIS — full matrix (no timeouts)
########################################
bench_redis() {
    local label=$1 port=$2
    taskset -c 4-7 redis-benchmark -p "$port" -c 10 -n 50000 -P 16 -t set -d 64 -q > /dev/null 2>&1
    sleep 1
    for p in 1 8 16 32 64; do
        taskset -c 4-7 redis-benchmark -p "$port" -c 50 -n 500000 -P $p -t set,get -d 64 --csv -q 2>&1 | \
            grep -v WARNING | sed "s/^/p=$p,/" >> "$R/${label}.csv"
    done
}

cleanup
echo '=== REDIS NO PERSIST ==='
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
bench_redis "redis-nopersist" 6379
redis-cli -p 6379 INFO memory | grep used_memory_human >> "$R/redis-nopersist-mem.txt" 2>/dev/null
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

echo '=== REDIS AOF EVERYSEC ==='
rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync everysec --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
bench_redis "redis-aof" 6379
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

echo '=== REDIS AOF ALWAYS ==='
rm -rf /tmp/redis-data/*
taskset -c 0-3 redis-server --port 6379 --save '' --appendonly yes --appendfsync always --protected-mode no --daemonize yes --loglevel warning --dir /tmp/redis-data
sleep 1
bench_redis "redis-aof-always" 6379
redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
cleanup

########################################
# MOON MONOIO — working configs
########################################
bench_moon() {
    local label=$1 port=$2
    taskset -c 4-7 redis-benchmark -p "$port" -c 1 -n 5000 -P 16 -t set -d 64 -q > /dev/null 2>&1
    sleep 1
    # c=1: all pipeline depths work
    for p in 1 8 16 32 64; do
        local n=$((p * 1000))
        [ $n -lt 5000 ] && n=5000
        timeout 15 taskset -c 4-7 redis-benchmark -p "$port" -c 1 -n $n -P $p -t set,get -d 64 --csv -q 2>&1 | \
            grep -v WARNING | sed "s/^/c=1,p=$p,/" >> "$R/${label}.csv"
    done
    # c=5,10,50 with p=64 (known working)
    for c in 5 10 25 50; do
        local n=$((c * 64 * 100))
        [ $n -gt 500000 ] && n=500000
        timeout 20 taskset -c 4-7 redis-benchmark -p "$port" -c $c -n $n -P 64 -t set,get -d 64 --csv -q 2>&1 | \
            grep -v WARNING | sed "s/^/c=$c,p=64,/" >> "$R/${label}.csv"
    done
    # c=10 with p=16 (worked in earlier test)
    timeout 15 taskset -c 4-7 redis-benchmark -p "$port" -c 10 -n 100000 -P 16 -t set,get -d 64 --csv -q 2>&1 | \
        grep -v WARNING | sed "s/^/c=10,p=16,/" >> "$R/${label}.csv"
}

echo '=== MOON MONOIO 1S NO PERSIST ==='
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no > /dev/null 2>&1 &
sleep 3
bench_moon "moon-s1-nopersist" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

echo '=== MOON MONOIO 4S NO PERSIST ==='
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 4 --protected-mode no > /dev/null 2>&1 &
sleep 3
bench_moon "moon-s4-nopersist" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

echo '=== MOON MONOIO 1S AOF EVERYSEC ==='
rm -rf /tmp/moon-data/*
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 1 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 3
bench_moon "moon-s1-aof" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

echo '=== MOON MONOIO 4S AOF EVERYSEC ==='
rm -rf /tmp/moon-data/*
taskset -c 0-3 ~/moon/target/release/moon --port 6399 --shards 4 --protected-mode no --appendonly yes --appendfsync everysec --dir /tmp/moon-data > /dev/null 2>&1 &
sleep 3
bench_moon "moon-s4-aof" 6399
pkill -9 -f 'target/release/moon' 2>/dev/null || true
cleanup

########################################
# REPORT
########################################
echo ''
echo '########## ALL RESULTS ##########'
date -u
for f in "$R"/*.csv; do
    [ -f "$f" ] && echo "=== $(basename "$f" .csv) ===" && cat "$f" && echo ''
done
echo "BENCHMARK_COMPLETE"
