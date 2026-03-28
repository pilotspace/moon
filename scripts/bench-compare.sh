#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-compare.sh -- Side-by-side Moon vs Redis benchmark for ALL commands
#
# Usage:
#   ./scripts/bench-compare.sh                # Full run
#   ./scripts/bench-compare.sh --requests N   # Custom request count
#   ./scripts/bench-compare.sh --shards N     # Moon shard count
#   ./scripts/bench-compare.sh --clients N    # Client count
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
        --requests)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --requests requires a numeric value"; exit 1
            fi
            REQUESTS="$2"; shift 2 ;;
        --shards)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --shards requires a numeric value"; exit 1
            fi
            SHARDS="$2"; shift 2 ;;
        --clients)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --clients requires a numeric value"; exit 1
            fi
            CLIENTS="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
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
    tr '\r' '\n' \
        | awk '/[Rr]equests per second/ { for (i=1; i<=NF; i++) { gsub(/,/, "", $i); if ($i+0 == $i && $i > 0) { print $i; exit } } }' \
        | head -1 \
        || sed -n 's/.*[[:space:]]\([0-9][0-9.]*\)[[:space:]]*requests per second.*/\1/p' \
        | sed 's/,//g' | tail -1
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

# Wait for servers with retry loop (max 10s)
wait_for_server() {
    local port="$1" name="$2" max_wait=10 elapsed=0
    while (( elapsed < max_wait )); do
        if redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    echo "$name failed to start on port $port within ${max_wait}s"
    exit 1
}

wait_for_server "$PORT_REDIS" "Redis"
wait_for_server "$PORT_MOON" "Moon"

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
    # Seed data: run a quick SET pass so GET reads real values (not nils)
    redis-benchmark -p "$PORT_REDIS" -n "$REQUESTS" -t set -d "$size" -q >/dev/null 2>&1
    redis-benchmark -p "$PORT_MOON" -n "$REQUESTS" -t set -d "$size" -q >/dev/null 2>&1
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
    bench_cmd "SET c=$c" -t set -c "$c"
done

# ===========================================================================
# Summary
# ===========================================================================

echo ""
echo "---"
echo "*Generated by bench-compare.sh*"
log "Done."
