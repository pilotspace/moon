#!/usr/bin/env bash
set -euo pipefail

cd /Users/tindang/workspaces/tind-repo/moon

# Kill leftovers
pkill -f "redis-server --port 6399" 2>/dev/null || true
pkill -f "moon --port 6400" 2>/dev/null || true
sleep 1

# Start servers
redis-server --port 6399 --save "" --appendonly no --daemonize yes --loglevel warning
./target/release/moon --port 6400 --shards 1 &
MOON_PID=$!
sleep 2

# Verify
redis-cli -p 6399 PING > /dev/null 2>&1 || { echo "Redis failed to start"; exit 1; }
redis-cli -p 6400 PING > /dev/null 2>&1 || { echo "Moon failed to start"; exit 1; }

OUT="/tmp/bench-results.md"
cat > "$OUT" <<'HEADER'
# Moon vs Redis 8.0.2 — Linux aarch64 Benchmark

**Platform:** Ubuntu 25.10, kernel 6.17, aarch64 (OrbStack VM on Apple Silicon)
**Moon:** v0.1.0, 1 shard, MoonStore v2 (Phases 75-84)
**Redis:** 8.0.2, jemalloc

HEADER

run_bench() {
    local label="$1" port="$2" args="$3"
    redis-benchmark -p "$port" $args -q 2>&1 | tr -d '\r' | grep -i "requests per second" | head -1
}

echo "## KV Operations (p=1, 50 clients, 200K requests)" >> "$OUT"
echo "" >> "$OUT"
echo "| Command | Redis RPS | Moon RPS | Moon/Redis |" >> "$OUT"
echo "|---------|-----------|----------|------------|" >> "$OUT"

for cmd in get set incr lpush lpop sadd spop hset; do
    R=$(run_bench "Redis" 6399 "-c 50 -n 200000 -t $cmd")
    M=$(run_bench "Moon" 6400 "-c 50 -n 200000 -t $cmd")
    R_NUM=$(echo "$R" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    M_NUM=$(echo "$M" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    if [ -n "$R_NUM" ] && [ -n "$M_NUM" ]; then
        RATIO=$(echo "scale=2; $M_NUM / $R_NUM" | bc 2>/dev/null || echo "N/A")
        printf "| %-7s | %12s | %12s | %sx |\n" "$cmd" "$R_NUM" "$M_NUM" "$RATIO" >> "$OUT"
    else
        echo "| $cmd | $R | $M | — |" >> "$OUT"
    fi
done

echo "" >> "$OUT"
echo "## Pipelined Operations (p=16, 50 clients, 200K requests)" >> "$OUT"
echo "" >> "$OUT"
echo "| Command | Redis RPS | Moon RPS | Moon/Redis |" >> "$OUT"
echo "|---------|-----------|----------|------------|" >> "$OUT"

for cmd in get set incr lpush hset; do
    R=$(run_bench "Redis" 6399 "-c 50 -n 200000 -t $cmd -P 16")
    M=$(run_bench "Moon" 6400 "-c 50 -n 200000 -t $cmd -P 16")
    R_NUM=$(echo "$R" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    M_NUM=$(echo "$M" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    if [ -n "$R_NUM" ] && [ -n "$M_NUM" ]; then
        RATIO=$(echo "scale=2; $M_NUM / $R_NUM" | bc 2>/dev/null || echo "N/A")
        printf "| %-7s | %12s | %12s | %sx |\n" "$cmd" "$R_NUM" "$M_NUM" "$RATIO" >> "$OUT"
    fi
done

echo "" >> "$OUT"
echo "## High-Throughput Pipeline (p=64, 100 clients, 1M requests)" >> "$OUT"
echo "" >> "$OUT"
echo "| Command | Redis RPS | Moon RPS | Moon/Redis |" >> "$OUT"
echo "|---------|-----------|----------|------------|" >> "$OUT"

for cmd in get set; do
    R=$(run_bench "Redis" 6399 "-c 100 -n 1000000 -t $cmd -P 64")
    M=$(run_bench "Moon" 6400 "-c 100 -n 1000000 -t $cmd -P 64")
    R_NUM=$(echo "$R" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    M_NUM=$(echo "$M" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    if [ -n "$R_NUM" ] && [ -n "$M_NUM" ]; then
        RATIO=$(echo "scale=2; $M_NUM / $R_NUM" | bc 2>/dev/null || echo "N/A")
        printf "| %-7s | %12s | %12s | %sx |\n" "$cmd" "$R_NUM" "$M_NUM" "$RATIO" >> "$OUT"
    fi
done

echo "" >> "$OUT"
echo "## Memory Efficiency" >> "$OUT"
echo "" >> "$OUT"

# 100K keys, 100B values
redis-cli -p 6399 FLUSHALL > /dev/null 2>&1
redis-cli -p 6400 FLUSHALL > /dev/null 2>&1
redis-benchmark -p 6399 -c 1 -n 100000 -t set -d 100 -q > /dev/null 2>&1
redis-benchmark -p 6400 -c 1 -n 100000 -t set -d 100 -q > /dev/null 2>&1
R_MEM=$(redis-cli -p 6399 INFO memory 2>&1 | tr -d '\r' | grep "used_memory:" | cut -d: -f2)
M_MEM=$(redis-cli -p 6400 INFO memory 2>&1 | tr -d '\r' | grep "used_memory:" | cut -d: -f2)
echo "### 100K keys × 100B values" >> "$OUT"
echo "- Redis: $((R_MEM / 1024 / 1024)) MB ($R_MEM bytes)" >> "$OUT"
echo "- Moon: $((M_MEM / 1024 / 1024)) MB ($M_MEM bytes)" >> "$OUT"
if [ -n "$R_MEM" ] && [ -n "$M_MEM" ] && [ "$M_MEM" -gt 0 ]; then
    SAVINGS=$(echo "scale=1; (1 - $M_MEM / $R_MEM) * 100" | bc 2>/dev/null || echo "N/A")
    echo "- Moon savings: ${SAVINGS}%" >> "$OUT"
fi
echo "" >> "$OUT"

# 100K keys, 1KB values
redis-cli -p 6399 FLUSHALL > /dev/null 2>&1
redis-cli -p 6400 FLUSHALL > /dev/null 2>&1
redis-benchmark -p 6399 -c 1 -n 100000 -t set -d 1024 -q > /dev/null 2>&1
redis-benchmark -p 6400 -c 1 -n 100000 -t set -d 1024 -q > /dev/null 2>&1
R_MEM=$(redis-cli -p 6399 INFO memory 2>&1 | tr -d '\r' | grep "used_memory:" | cut -d: -f2)
M_MEM=$(redis-cli -p 6400 INFO memory 2>&1 | tr -d '\r' | grep "used_memory:" | cut -d: -f2)
echo "### 100K keys × 1KB values" >> "$OUT"
echo "- Redis: $((R_MEM / 1024 / 1024)) MB ($R_MEM bytes)" >> "$OUT"
echo "- Moon: $((M_MEM / 1024 / 1024)) MB ($M_MEM bytes)" >> "$OUT"
if [ -n "$R_MEM" ] && [ -n "$M_MEM" ] && [ "$M_MEM" -gt 0 ]; then
    SAVINGS=$(echo "scale=1; (1 - $M_MEM / $R_MEM) * 100" | bc 2>/dev/null || echo "N/A")
    echo "- Moon savings: ${SAVINGS}%" >> "$OUT"
fi
echo "" >> "$OUT"

# Cleanup
kill $MOON_PID 2>/dev/null || true
redis-cli -p 6399 SHUTDOWN NOSAVE 2>/dev/null || true

cat "$OUT"
