#!/usr/bin/env bash
set -euo pipefail
# Moon vs Redis 8.0.2 — All Commands Benchmark
# Runs on Linux VM, writes results to /tmp/bench-all.txt

cd /Users/tindang/workspaces/tind-repo/moon
OUT=/tmp/bench-all.txt
: > "$OUT"

# Kill stale processes
pkill -9 moon 2>/dev/null || true
pkill -9 redis-server 2>/dev/null || true
pkill -9 redis-benchmark 2>/dev/null || true
sleep 2

# Start servers
redis-server --port 6399 --bind 127.0.0.1 --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning
./target/release/moon --port 6400 --shards 1 &
MOON_PID=$!
sleep 2

redis-cli -p 6399 PING > /dev/null || { echo "Redis failed"; exit 1; }
redis-cli -p 6400 PING > /dev/null || { echo "Moon failed"; exit 1; }

echo "# Moon vs Redis 8.0.2 — All Commands Benchmark" >> "$OUT"
echo "" >> "$OUT"
echo "**Date:** $(date -Iseconds)" >> "$OUT"
echo "**Platform:** $(uname -srm)" >> "$OUT"
echo "**Redis:** $(redis-server --version | grep -oE 'v=[0-9.]+' | cut -d= -f2)" >> "$OUT"
echo "**Moon:** v0.1.0, 1 shard, monoio io_uring" >> "$OUT"
echo "" >> "$OUT"

# Helper: run redis-benchmark with timeout, extract first non-nan burst RPS for Moon
bench_moon() {
    local cmd="$1" pipeline="$2" clients="$3" requests="$4"
    local raw
    raw=$(timeout 8 redis-benchmark -p 6400 -c "$clients" -n "$requests" -t "$cmd" -P "$pipeline" 2>&1 || true)
    # Extract first non-zero, non-nan RPS line
    local rps=$(echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "rps=0.0" | grep -v "nan" | head -1 | grep -oE 'overall: [0-9.]+' | grep -oE '[0-9.]+')
    local lat=$(echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "nan" | grep -v "rps=0.0" | head -1 | grep -oE 'avg_msec=[0-9.]+' | grep -oE '[0-9.]+' | tail -1)
    echo "${rps:-HANG}|${lat:--}"
}

bench_redis() {
    local cmd="$1" pipeline="$2" clients="$3" requests="$4"
    local raw
    raw=$(redis-benchmark -p 6399 -c "$clients" -n "$requests" -t "$cmd" -P "$pipeline" --csv 2>&1 | grep -v '^"test"' | head -1)
    local rps=$(echo "$raw" | cut -d'"' -f4)
    local lat=$(echo "$raw" | cut -d'"' -f6)
    echo "${rps:-ERR}|${lat:--}"
}

run_section() {
    local title="$1" pipeline="$2" clients="$3" requests="$4"
    shift 4
    local commands=("$@")

    echo "## $title" >> "$OUT"
    echo "" >> "$OUT"
    echo "| Command | Redis RPS | Moon RPS | Ratio | Redis p50 | Moon avg |" >> "$OUT"
    echo "|---------|----------:|----------:|------:|----------:|--------:|" >> "$OUT"

    for cmd in "${commands[@]}"; do
        local CMD_UPPER=$(echo "$cmd" | tr 'a-z' 'A-Z')
        local redis_result=$(bench_redis "$cmd" "$pipeline" "$clients" "$requests")
        local moon_result=$(bench_moon "$cmd" "$pipeline" "$clients" "$requests")
        local r_rps=$(echo "$redis_result" | cut -d'|' -f1)
        local r_lat=$(echo "$redis_result" | cut -d'|' -f2)
        local m_rps=$(echo "$moon_result" | cut -d'|' -f1)
        local m_lat=$(echo "$moon_result" | cut -d'|' -f2)

        local ratio="-"
        if [ "$m_rps" != "HANG" ] && [ "$r_rps" != "ERR" ] && [ -n "$m_rps" ] && [ -n "$r_rps" ]; then
            ratio=$(echo "scale=2; $m_rps / $r_rps" | bc 2>/dev/null || echo "-")
        fi

        printf "| %-7s | %12s | %12s | %5sx | %9s | %7s |\n" \
            "$CMD_UPPER" "$r_rps" "$m_rps" "$ratio" "${r_lat}ms" "${m_lat}ms" >> "$OUT"
    done
    echo "" >> "$OUT"
}

# === p=1 (single command latency) ===
run_section "Single Command (p=1, 50 clients, 100K)" 1 50 100000 \
    get set incr lpush rpush lpop rpop sadd spop hset zadd

# === p=16 (medium pipeline) ===
run_section "Pipelined (p=16, 50 clients, 200K)" 16 50 200000 \
    get set incr lpush rpush lpop rpop sadd spop hset zadd

# === p=64 (high throughput) ===
run_section "High Throughput (p=64, 100 clients, 1M)" 64 100 1000000 \
    get set

# === MSET (multi-key) ===
echo "## Multi-Key Commands (p=1, 50 clients, 100K)" >> "$OUT"
echo "" >> "$OUT"
echo "| Command | Redis RPS | Moon RPS | Ratio |" >> "$OUT"
echo "|---------|----------:|----------:|------:|" >> "$OUT"
r_mset=$(bench_redis "mset" 1 50 100000)
m_mset=$(bench_moon "mset" 1 50 100000)
r_rps=$(echo "$r_mset" | cut -d'|' -f1)
m_rps=$(echo "$m_mset" | cut -d'|' -f1)
ratio="-"
if [ "$m_rps" != "HANG" ] && [ -n "$m_rps" ] && [ -n "$r_rps" ]; then
    ratio=$(echo "scale=2; $m_rps / $r_rps" | bc 2>/dev/null || echo "-")
fi
printf "| MSET(10)| %12s | %12s | %5sx |\n" "$r_rps" "$m_rps" "$ratio" >> "$OUT"
echo "" >> "$OUT"

# Cleanup
kill $MOON_PID 2>/dev/null || true
redis-cli -p 6399 SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

echo "=== DONE ===" >> "$OUT"
cat "$OUT"
