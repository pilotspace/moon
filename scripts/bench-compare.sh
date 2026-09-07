#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-compare.sh -- Side-by-side Moon vs Redis benchmark for ALL commands
#
# Usage:
#   ./scripts/bench-compare.sh                # Full run
#   ./scripts/bench-compare.sh --requests N   # Custom request count
#   ./scripts/bench-compare.sh --shards N     # Moon shard count (tuned row only)
#   ./scripts/bench-compare.sh --clients N    # Client count
#   ./scripts/bench-compare.sh --skip-default # Omit the default-config moon row
#
# Two moon rows (moon#833). The TUNED row is the historical one: `--shards N
# --protected-mode no --appendonly no --disk-offload disable`, i.e. Redis's
# in-memory shape. The DEFAULT row is what a user gets from `moon --port N`
# with no tuning flags: `--shards 1`, `--appendonly yes` (everysec),
# `--disk-offload enable`, `--maxmemory` auto-capped by the guardrail. The
# only flag it adds is `--dir <fresh tempdir>`, because an omitted --dir
# resolves to the platform user-data directory, where a second default
# instance collides on the dir lock and reloads whatever the last run left.
#
# The delta between the two rows is the signal this script exists to show:
# for months every benchmark passed `--disk-offload disable`, which is the one
# flag that hid a ~53% default-only SET deficit (moon#812).
###############################################################################

PORT_REDIS=6399
PORT_MOON=6400
PORT_MOON_DEFAULT=6401
SKIP_DEFAULT=0
REQUESTS=100000
CLIENTS=50
SHARDS=1
KEYSPACE="${KEYSPACE:-}"   # -r random keyspace; empty = redis-benchmark default (1 hot key). Set for large-scale.
RUST_BINARY="./target/release/moon"

REDIS_PID=""
MOON_PID=""
MOON_DEFAULT_PID=""
MOON_DEFAULT_DIR=""

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
        --skip-default)
            SKIP_DEFAULT=1; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    [[ -n "${MOON_PID:-}" ]] && kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    [[ -n "${MOON_DEFAULT_PID:-}" ]] && kill "$MOON_DEFAULT_PID" 2>/dev/null; wait "$MOON_DEFAULT_PID" 2>/dev/null || true
    [[ -n "${MOON_DEFAULT_DIR:-}" ]] && rm -rf "$MOON_DEFAULT_DIR"
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
    local rflag=()
    [[ -n "$KEYSPACE" ]] && rflag=(-r "$KEYSPACE")   # large-scale: spread ops across a real keyspace
    # `${arr[@]+"${arr[@]}"}`: an EMPTY array is "unbound" to `set -u` on bash < 4.4
    # (macOS ships 3.2), which aborted the first bench() call — the #634 class.
    redis-benchmark -p "$port" -n "$REQUESTS" -c "$CLIENTS" -q ${rflag[@]+"${rflag[@]}"} "$@" 2>/dev/null | parse_rps
}

ratio_of() {
    # $1 / $2 to two decimals, or N/A when either side is missing.
    if [[ "${1:-0}" != "0" ]] && [[ "${2:-0}" != "0" ]]; then
        awk "BEGIN { printf \"%.2f\", $1 / $2 }"
    else
        echo "N/A"
    fi
}

table_header() {
    local first_col="$1"
    if (( SKIP_DEFAULT )); then
        printf "| %-30s | %12s | %12s | %7s |\n" "$first_col" "Redis RPS" "Moon RPS" "Ratio"
        printf "|%-32s|%14s|%14s|%9s|\n" "--------------------------------" "--------------" "--------------" "---------"
    else
        printf "| %-30s | %12s | %14s | %16s | %11s | %13s |\n" \
            "$first_col" "Redis RPS" "Moon tuned RPS" "Moon default RPS" "tuned/Redis" "default/tuned"
        printf "|%-32s|%14s|%16s|%18s|%13s|%15s|\n" \
            "--------------------------------" "--------------" "----------------" "------------------" "-------------" "---------------"
    fi
}

bench_cmd() {
    local desc="$1"
    shift
    local redis_rps moon_rps default_rps
    redis_rps=$(bench "$PORT_REDIS" "$@")
    moon_rps=$(bench "$PORT_MOON" "$@")
    redis_rps="${redis_rps:-0}"
    moon_rps="${moon_rps:-0}"
    if (( SKIP_DEFAULT )); then
        printf "| %-30s | %12s | %12s | %6sx |\n" "$desc" "$redis_rps" "$moon_rps" "$(ratio_of "$moon_rps" "$redis_rps")"
        return
    fi
    default_rps=$(bench "$PORT_MOON_DEFAULT" "$@")
    default_rps="${default_rps:-0}"
    printf "| %-30s | %12s | %14s | %16s | %10sx | %12sx |\n" \
        "$desc" "$redis_rps" "$moon_rps" "$default_rps" \
        "$(ratio_of "$moon_rps" "$redis_rps")" "$(ratio_of "$default_rps" "$moon_rps")"
}

# ===========================================================================
# Start Servers
# ===========================================================================

log "Starting Redis on port $PORT_REDIS..."
redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning --protected-mode no &
REDIS_PID=$!

log "Starting moon on port $PORT_MOON ($SHARDS shards)..."
# Match Redis's in-memory config (--save "" --appendonly no): disable persistence so the
# comparison is AOF-off on both sides. Without this, moon runs AOF-on while Redis does not —
# under SET load moon's AOF channel saturates, floods "AOF append dropped … channel full"
# WARN logs (can be ~1M lines), and is unfairly handicapped. Redirect moon's stdout/stderr
# to keep the benchmark report clean.
RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no --appendonly no --disk-offload disable >/dev/null 2>&1 &
MOON_PID=$!

if (( ! SKIP_DEFAULT )); then
    # moon#833: the SHIPPED DEFAULT, alongside the tuned row. No `--shards`, no
    # persistence or offload flags — whatever `moon --port N` resolves to on
    # this host is what gets measured. `--dir` is the one addition, and it is
    # not a tuning flag: without it the server takes the dir lock in the
    # platform user-data directory (colliding with any default instance already
    # running there) and reloads whatever AOF the previous run left behind.
    # AOF is ON in this row, so expect the auto-rewrite (64mb floor, 100%
    # growth) to fire mid-run on long SET legs — that is part of the default.
    MOON_DEFAULT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/moon-bench-default-XXXXXX")
    log "Starting moon DEFAULT CONFIG on port $PORT_MOON_DEFAULT (no tuning flags; --dir $MOON_DEFAULT_DIR)..."
    RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON_DEFAULT" --dir "$MOON_DEFAULT_DIR" >/dev/null 2>&1 &
    MOON_DEFAULT_PID=$!
fi

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
(( SKIP_DEFAULT )) || wait_for_server "$PORT_MOON_DEFAULT" "Moon (default config)"

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
echo "**Moon tuned:** \`--shards $SHARDS --protected-mode no --appendonly no --disk-offload disable\`"
if (( ! SKIP_DEFAULT )); then
    DEFAULT_MAXMEM=$(redis-cli -p "$PORT_MOON_DEFAULT" CONFIG GET maxmemory 2>/dev/null | tail -1 | tr -d '\r')
    DEFAULT_POLICY=$(redis-cli -p "$PORT_MOON_DEFAULT" CONFIG GET maxmemory-policy 2>/dev/null | tail -1 | tr -d '\r')
    DEFAULT_SHARDS=$(redis-cli -p "$PORT_MOON_DEFAULT" INFO server 2>/dev/null | tr -d '\r' | sed -n 's/^num_shards://p')
    echo "**Moon default:** no tuning flags (\`--port $PORT_MOON_DEFAULT --dir <tempdir>\`) — resolved on this host to shards=${DEFAULT_SHARDS:-?}, appendonly=yes/everysec, disk-offload=enable, maxmemory=${DEFAULT_MAXMEM:-?} (${DEFAULT_POLICY:-?})"
    if [[ "$SHARDS" != "1" ]]; then
        echo "**Note:** the tuned row runs $SHARDS shards, the default row runs the shipped default (1); the default/tuned column mixes the shard count into the delta."
    fi
fi
echo "**Requests:** $REQUESTS per test, $CLIENTS clients"
echo "**Platform:** $PLATFORM"
echo ""

# ===========================================================================
# Core Commands (p=1)
# ===========================================================================

log "Benchmarking core commands..."

echo "## Core Commands (p=1, $CLIENTS clients)"
echo ""
table_header "Command"

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
table_header "Pipeline Depth"

for p in 1 2 4 8 16 32 64 128; do
    bench_cmd "SET p=$p" -t set -P "$p"
done

echo ""
echo "## Pipeline Scaling (GET)"
echo ""
table_header "Pipeline Depth"

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
table_header "Value Size"

for size in 8 64 256 1024 4096 16384 65536; do
    bench_cmd "SET ${size}B" -t set -d "$size"
done

echo ""
echo "## Data Size Scaling (GET)"
echo ""
table_header "Value Size"

for size in 8 64 256 1024 4096 16384 65536; do
    # Seed data: run a quick SET pass so GET reads real values (not nils)
    redis-benchmark -p "$PORT_REDIS" -n "$REQUESTS" -t set -d "$size" -q >/dev/null 2>&1
    redis-benchmark -p "$PORT_MOON" -n "$REQUESTS" -t set -d "$size" -q >/dev/null 2>&1
    (( SKIP_DEFAULT )) || redis-benchmark -p "$PORT_MOON_DEFAULT" -n "$REQUESTS" -t set -d "$size" -q >/dev/null 2>&1
    bench_cmd "GET ${size}B" -t get -d "$size"
done

# ===========================================================================
# Connection Scaling
# ===========================================================================

log "Benchmarking connection scaling..."

echo ""
echo "## Connection Scaling (SET)"
echo ""
table_header "Clients"

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
