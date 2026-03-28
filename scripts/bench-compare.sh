#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-compare.sh -- Side-by-side Moon vs Redis benchmark for ALL commands
#
# Usage:
#   ./scripts/bench-compare.sh                # Full run
#   ./scripts/bench-compare.sh --requests N   # Custom request count
#   ./scripts/bench-compare.sh --shards N     # Moon shard count
###############################################################################

PORT_REDIS=6399
PORT_MOON=6400
REQUESTS=100000
CLIENTS=50
SHARDS=1
RUST_BINARY="./target/release/moon"

REDIS_PID=""
MOON_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests) REQUESTS="$2"; shift 2 ;;
        --shards)   SHARDS="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    [[ -n "${MOON_PID:-}" ]] && kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_MOON}" 2>/dev/null || true
}
trap cleanup EXIT

parse_rps() {
    tr '\r' '\n' | grep -i "requests per second" | tail -1 | sed 's/.*: \([0-9.]*\) requests.*/\1/' | sed 's/,//g'
}

bench() {
    local port="$1"
    shift
    redis-benchmark -p "$port" -n "$REQUESTS" -c "$CLIENTS" -q "$@" 2>/dev/null | parse_rps
}

bench_cmd() {
    local desc="$1"
    shift
    local redis_rps moon_rps ratio
    redis_rps=$(bench "$PORT_REDIS" "$@")
    moon_rps=$(bench "$PORT_MOON" "$@")
    redis_rps="${redis_rps:-0}"
    moon_rps="${moon_rps:-0}"
    if [[ "$redis_rps" != "0" ]] && [[ "$moon_rps" != "0" ]]; then
        ratio=$(awk "BEGIN { printf \"%.2f\", $moon_rps / $redis_rps }")
    else
        ratio="N/A"
    fi
    printf "| %-30s | %12s | %12s | %6sx |\n" "$desc" "$redis_rps" "$moon_rps" "$ratio"
}

# ===========================================================================
# Start Servers
# ===========================================================================

log "Starting Redis on port $PORT_REDIS..."
redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning --protected-mode no &
REDIS_PID=$!

log "Starting moon on port $PORT_MOON ($SHARDS shards)..."
RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no &
MOON_PID=$!

sleep 1

redis-cli -p "$PORT_REDIS" PING >/dev/null 2>&1 || { echo "Redis failed to start"; exit 1; }
redis-cli -p "$PORT_MOON" PING >/dev/null 2>&1 || { echo "Moon failed to start"; exit 1; }

log "Servers ready."

# ===========================================================================
# Header
# ===========================================================================

REDIS_VER=$(redis-cli -p "$PORT_REDIS" INFO server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '\r')
PLATFORM="$(uname -s) $(uname -m)"
if [[ -f /etc/os-release ]]; then
    PLATFORM="$PLATFORM / $(grep PRETTY_NAME /etc/os-release | cut -d= -f2 | tr -d '"')"
fi

echo "# Moon vs Redis Benchmark"
echo ""
echo "**Date:** $(date +%Y-%m-%d)"
echo "**Redis:** $REDIS_VER"
echo "**Moon:** $SHARDS shard(s)"
echo "**Requests:** $REQUESTS per test, $CLIENTS clients"
echo "**Platform:** $PLATFORM"
echo ""

# ===========================================================================
# Core Commands (p=1)
# ===========================================================================

log "Benchmarking core commands..."

echo "## Core Commands (p=1, $CLIENTS clients)"
echo ""
printf "| %-30s | %12s | %12s | %7s |\n" "Command" "Redis RPS" "Moon RPS" "Ratio"
printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"

bench_cmd "PING inline"               -t ping_inline
bench_cmd "PING mbulk"                -t ping_mbulk
bench_cmd "SET"                        -t set
bench_cmd "GET"                        -t get
bench_cmd "INCR"                       -t incr
bench_cmd "MSET (10 keys)"             -t mset
bench_cmd "LPUSH"                      -t lpush
bench_cmd "RPUSH"                      -t rpush
bench_cmd "LPOP"                       -t lpop
bench_cmd "RPOP"                       -t rpop
bench_cmd "LRANGE 100"                 -t lrange_100
bench_cmd "LRANGE 300"                 -t lrange_300
bench_cmd "LRANGE 500"                 -t lrange_500
bench_cmd "LRANGE 600"                 -t lrange_600
bench_cmd "SADD"                       -t sadd
bench_cmd "SPOP"                       -t spop
bench_cmd "HSET"                       -t hset
bench_cmd "ZADD"                       -t zadd
bench_cmd "ZPOPMIN"                    -t zpopmin

# ===========================================================================
# Pipeline Scaling
# ===========================================================================

log "Benchmarking pipeline scaling..."

echo ""
echo "## Pipeline Scaling (SET)"
echo ""
printf "| %-30s | %12s | %12s | %7s |\n" "Pipeline Depth" "Redis RPS" "Moon RPS" "Ratio"
printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"

for p in 1 2 4 8 16 32 64 128; do
    bench_cmd "SET p=$p" -t set -P "$p"
done

echo ""
echo "## Pipeline Scaling (GET)"
echo ""
printf "| %-30s | %12s | %12s | %7s |\n" "Pipeline Depth" "Redis RPS" "Moon RPS" "Ratio"
printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"

for p in 1 2 4 8 16 32 64 128; do
    bench_cmd "GET p=$p" -t get -P "$p"
done

# ===========================================================================
# Data Size Scaling
# ===========================================================================

log "Benchmarking data sizes..."

echo ""
echo "## Data Size Scaling (SET)"
echo ""
printf "| %-30s | %12s | %12s | %7s |\n" "Value Size" "Redis RPS" "Moon RPS" "Ratio"
printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"

for size in 8 64 256 1024 4096 16384 65536; do
    bench_cmd "SET ${size}B" -t set -d "$size"
done

echo ""
echo "## Data Size Scaling (GET)"
echo ""
printf "| %-30s | %12s | %12s | %7s |\n" "Value Size" "Redis RPS" "Moon RPS" "Ratio"
printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"

for size in 8 64 256 1024 4096 16384 65536; do
    # Pre-populate
    redis-cli -p "$PORT_REDIS" SET "bench:data" "$(head -c "$size" /dev/urandom | base64 | head -c "$size")" >/dev/null 2>&1
    redis-cli -p "$PORT_MOON" SET "bench:data" "$(head -c "$size" /dev/urandom | base64 | head -c "$size")" >/dev/null 2>&1
    bench_cmd "GET ${size}B" -t get -d "$size"
done

# ===========================================================================
# Connection Scaling
# ===========================================================================

log "Benchmarking connection scaling..."

echo ""
echo "## Connection Scaling (SET)"
echo ""
printf "| %-30s | %12s | %12s | %7s |\n" "Clients" "Redis RPS" "Moon RPS" "Ratio"
printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"

for c in 1 10 50 100 200 500; do
    CLIENTS_SAVE=$CLIENTS
    CLIENTS=$c
    bench_cmd "SET c=$c" -t set -c "$c"
    CLIENTS=$CLIENTS_SAVE
done

# ===========================================================================
# Summary
# ===========================================================================

echo ""
echo "---"
echo "*Generated by bench-compare.sh*"
log "Done."
