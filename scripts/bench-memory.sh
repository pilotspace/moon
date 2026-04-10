#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-memory.sh -- RSS memory regression gate
#
# Starts Moon, writes 1M keys via redis-benchmark, reads RSS from
# /proc/PID/status, calculates RSS-per-key, compares against baseline.
# Exits 1 if RSS-per-key exceeds baseline by >10%.
#
# Usage:
#   ./scripts/bench-memory.sh                        # Default settings
#   ./scripts/bench-memory.sh --keys 500000          # Custom key count
#   ./scripts/bench-memory.sh --shards 1             # Single shard
#   ./scripts/bench-memory.sh --skip-build           # Skip cargo build
#   ./scripts/bench-memory.sh --port 6401            # Custom port
#   ./scripts/bench-memory.sh --baseline 120         # Custom baseline (bytes/key)
###############################################################################

PORT=6401
SHARDS=1
KEYS=1000000
SKIP_BUILD=false
RUST_BINARY="./target/release/moon"
MOON_PID=""
# Baseline: expected RSS bytes per key for 1M keys, 1 shard, 8-byte values.
# Moon's HeapString SSO = 23 bytes inline key + DashTable overhead + value.
# Empirical baseline ~110 bytes/key. Set to 120 for headroom.
BASELINE_BYTES_PER_KEY=120

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)       PORT="$2"; shift 2 ;;
        --shards)     SHARDS="$2"; shift 2 ;;
        --keys)       KEYS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --baseline)   BASELINE_BYTES_PER_KEY="$2"; shift 2 ;;
        --help|-h)    sed -n '3,16p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    if [[ -n "$MOON_PID" ]]; then
        kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    fi
    pkill -f "moon.*${PORT}" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
    local port=$1
    for ((i=0; i<30; i++)); do
        redis-cli -p "$port" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.2
    done
    log "ERROR: port $port not ready"; return 1
}

get_rss_kb() {
    # Read RSS from /proc on Linux
    if [[ -f "/proc/$1/status" ]]; then
        grep VmRSS "/proc/$1/status" | awk '{print $2}'
    else
        # Fallback to ps (macOS / non-Linux)
        ps -o rss= -p "$1" 2>/dev/null | tr -d ' '
    fi
}

human_bytes() {
    local bytes=$1
    if (( bytes >= 1073741824 )); then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc)GB"
    elif (( bytes >= 1048576 )); then
        echo "$(echo "scale=2; $bytes / 1048576" | bc)MB"
    elif (( bytes >= 1024 )); then
        echo "$(echo "scale=2; $bytes / 1024" | bc)KB"
    else
        echo "${bytes}B"
    fi
}

# ===========================================================================
# Build
# ===========================================================================

if [[ "$SKIP_BUILD" == "false" ]]; then
    log "Building Moon (release)..."
    cargo build --release 2>&1 | tail -3
fi

# ===========================================================================
# Kill any lingering instances
# ===========================================================================

pkill -f "moon.*${PORT}" 2>/dev/null || true
sleep 0.3

# ===========================================================================
# Start Moon
# ===========================================================================

log "Starting Moon on port $PORT (shards=$SHARDS)..."
"$RUST_BINARY" --port "$PORT" --shards "$SHARDS" &
MOON_PID=$!
wait_for_port "$PORT"
log "Moon ready (PID=$MOON_PID)"

# ===========================================================================
# Measure baseline RSS (empty server)
# ===========================================================================

sleep 0.5
RSS_EMPTY_KB=$(get_rss_kb "$MOON_PID")
RSS_EMPTY_BYTES=$((RSS_EMPTY_KB * 1024))
log "Empty server RSS: ${RSS_EMPTY_KB}KB ($(human_bytes $RSS_EMPTY_BYTES))"

# ===========================================================================
# Write keys via redis-benchmark
# ===========================================================================

log "Writing $KEYS unique keys (8-byte values)..."
redis-benchmark -p "$PORT" -t SET -n "$KEYS" -r "$KEYS" -d 8 -q --csv 2>/dev/null | tail -1
log "Write complete."

# Verify key count
sleep 1
DBSIZE=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null | awk '{print $NF}' | tr -d '\r')
log "DBSIZE reports: $DBSIZE keys"

# ===========================================================================
# Measure loaded RSS
# ===========================================================================

sleep 1
RSS_LOADED_KB=$(get_rss_kb "$MOON_PID")
RSS_LOADED_BYTES=$((RSS_LOADED_KB * 1024))
log "Loaded server RSS: ${RSS_LOADED_KB}KB ($(human_bytes $RSS_LOADED_BYTES))"

# ===========================================================================
# Calculate per-key overhead
# ===========================================================================

RSS_DELTA_BYTES=$((RSS_LOADED_BYTES - RSS_EMPTY_BYTES))
if [[ "$DBSIZE" -gt 0 ]]; then
    BYTES_PER_KEY=$((RSS_DELTA_BYTES / DBSIZE))
else
    log "ERROR: DBSIZE is 0, cannot compute per-key overhead"
    exit 1
fi

THRESHOLD_BYTES=$(echo "$BASELINE_BYTES_PER_KEY * 110 / 100" | bc)

# ===========================================================================
# Results table
# ===========================================================================

echo ""
echo "==========================================="
echo "  Moon RSS Memory Regression Gate"
echo "==========================================="
echo ""
printf "%-28s %s\n" "Metric" "Value"
printf "%-28s %s\n" "----------------------------" "----------"
printf "%-28s %s\n" "Port"                  "$PORT"
printf "%-28s %s\n" "Shards"                "$SHARDS"
printf "%-28s %s\n" "Keys written"          "$KEYS"
printf "%-28s %s\n" "Keys in DB (DBSIZE)"   "$DBSIZE"
printf "%-28s %s\n" "RSS empty"             "$(human_bytes $RSS_EMPTY_BYTES)"
printf "%-28s %s\n" "RSS loaded"            "$(human_bytes $RSS_LOADED_BYTES)"
printf "%-28s %s\n" "RSS delta"             "$(human_bytes $RSS_DELTA_BYTES)"
printf "%-28s %s\n" "Bytes/key (actual)"    "${BYTES_PER_KEY}B"
printf "%-28s %s\n" "Bytes/key (baseline)"  "${BASELINE_BYTES_PER_KEY}B"
printf "%-28s %s\n" "Threshold (+10%)"      "${THRESHOLD_BYTES}B"
echo ""

# ===========================================================================
# Pass / Fail
# ===========================================================================

if (( BYTES_PER_KEY <= THRESHOLD_BYTES )); then
    echo "RESULT: PASS -- ${BYTES_PER_KEY}B/key <= ${THRESHOLD_BYTES}B threshold"
    echo ""
    exit 0
else
    REGRESSION_PCT=$(echo "scale=1; ($BYTES_PER_KEY - $BASELINE_BYTES_PER_KEY) * 100 / $BASELINE_BYTES_PER_KEY" | bc)
    echo "RESULT: FAIL -- ${BYTES_PER_KEY}B/key exceeds baseline by ${REGRESSION_PCT}%"
    echo "  Baseline: ${BASELINE_BYTES_PER_KEY}B/key"
    echo "  Actual:   ${BYTES_PER_KEY}B/key"
    echo "  Allowed:  ${THRESHOLD_BYTES}B/key (+10%)"
    echo ""
    exit 1
fi
