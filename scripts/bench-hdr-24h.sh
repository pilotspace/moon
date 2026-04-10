#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-hdr-24h.sh -- 24-hour HDR tail-latency benchmark (PERF-02)
#
# Runs redis-benchmark against moon for 24 hours, capturing HDR histogram
# percentiles (p50/p99/p999/p9999) at hourly intervals. Produces a final
# report with drift analysis.
#
# Usage:
#   ./scripts/bench-hdr-24h.sh                    # Defaults: 24h, port 6400
#   ./scripts/bench-hdr-24h.sh --duration 3600    # 1h test run
#   ./scripts/bench-hdr-24h.sh --port 6400        # Moon port
#   ./scripts/bench-hdr-24h.sh --shards 4         # Moon shard count
#   ./scripts/bench-hdr-24h.sh --skip-start       # Moon already running
#   ./scripts/bench-hdr-24h.sh --output DIR       # Output directory
#
# Requirements: redis-benchmark, redis-cli, moon binary at ./target/release/moon
###############################################################################

PORT=6400
SHARDS=4
DURATION=$((24 * 3600))
SKIP_START=false
OUTPUT_DIR="benches/history"
RUST_BINARY="./target/release/moon"
INTERVAL=3600  # hourly checkpoint
PIPELINE=16
CLIENTS=50

MOON_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)   DURATION="$2"; shift 2 ;;
        --port)       PORT="$2"; shift 2 ;;
        --shards)     SHARDS="$2"; shift 2 ;;
        --skip-start) SKIP_START=true; shift ;;
        --output)     OUTPUT_DIR="$2"; shift 2 ;;
        --pipeline)   PIPELINE="$2"; shift 2 ;;
        --clients)    CLIENTS="$2"; shift 2 ;;
        --help|-h)    sed -n '3,17p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
REPORT_FILE="${OUTPUT_DIR}/hdr-24h-${TIMESTAMP}.md"
DATA_DIR="${OUTPUT_DIR}/hdr-24h-${TIMESTAMP}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    if [[ "$SKIP_START" == false ]] && [[ -n "${MOON_PID:-}" ]]; then
        kill "$MOON_PID" 2>/dev/null
        wait "$MOON_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_server() {
    local retries=30
    while ! redis-cli -p "$PORT" PING &>/dev/null; do
        retries=$((retries - 1))
        [[ $retries -le 0 ]] && { log "ERROR: moon on port $PORT not responding"; exit 1; }
        sleep 0.5
    done
    log "Moon responding on port $PORT"
}

get_rss_kb() {
    local pid=$1
    if [[ -r "/proc/$pid/status" ]]; then
        awk '/^VmRSS:/ { print $2 }' "/proc/$pid/status" 2>/dev/null || echo "0"
    else
        ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0"
    fi
}

get_fd_count() {
    local pid=$1
    if [[ -d "/proc/$pid/fd" ]]; then
        ls "/proc/$pid/fd" 2>/dev/null | wc -l
    else
        lsof -p "$pid" 2>/dev/null | wc -l || echo "0"
    fi
}

# ===========================================================================
# Setup
# ===========================================================================

mkdir -p "$OUTPUT_DIR" "$DATA_DIR"

if [[ "$SKIP_START" == false ]]; then
    log "Starting moon: port=$PORT shards=$SHARDS"
    pkill -f "moon.*${PORT}" 2>/dev/null || true
    sleep 0.3
    "$RUST_BINARY" --port "$PORT" --shards "$SHARDS" &
    MOON_PID=$!
    log "Moon PID: $MOON_PID"
fi

wait_for_server

if [[ "$SKIP_START" == false ]]; then
    EFFECTIVE_PID="$MOON_PID"
else
    EFFECTIVE_PID=$(pgrep -f "moon.*${PORT}" | head -1 || true)
fi

# Seed some data so GET has keys to hit
log "Seeding 100K keys..."
redis-benchmark -p "$PORT" -t set -n 100000 -r 100000 -d 64 -q >/dev/null 2>&1

TOTAL_HOURS=$((DURATION / INTERVAL))
[[ $TOTAL_HOURS -lt 1 ]] && TOTAL_HOURS=1

log "Starting 24h HDR benchmark: ${TOTAL_HOURS} intervals of $((INTERVAL))s each"
log "Config: clients=$CLIENTS pipeline=$PIPELINE"
log "Report: $REPORT_FILE"

# ===========================================================================
# Header
# ===========================================================================

BASELINE_RSS=$(get_rss_kb "$EFFECTIVE_PID")
BASELINE_FD=$(get_fd_count "$EFFECTIVE_PID")

cat > "$REPORT_FILE" << EOF
# HDR 24h Tail-Latency Report (PERF-02)

**Date:** $(date '+%Y-%m-%d %H:%M:%S %Z')
**Duration:** ${TOTAL_HOURS}h (${DURATION}s)
**Server:** moon --port $PORT --shards $SHARDS
**Workload:** redis-benchmark -c $CLIENTS -P $PIPELINE -t get,set
**Baseline RSS:** ${BASELINE_RSS} KB
**Baseline FDs:** ${BASELINE_FD}

## Hourly Checkpoints

| Hour | GET RPS | GET p50 | GET p99 | GET p999 | SET RPS | SET p50 | SET p99 | SET p999 | RSS (KB) | FDs |
|------|---------|---------|---------|----------|---------|---------|---------|----------|----------|-----|
EOF

# ===========================================================================
# Main loop
# ===========================================================================

parse_percentile() {
    # Extract percentile from redis-benchmark --csv output
    local file=$1 pct=$2
    # redis-benchmark percentile output: "GET","p50","0.103"
    grep -i "\"$pct\"" "$file" 2>/dev/null | tail -1 | cut -d',' -f3 | tr -d '"' || echo "N/A"
}

parse_rps_from_csv() {
    # redis-benchmark CSV: "GET","100000","25.00","12345.67"
    # Last field on summary line
    tr '\r' '\n' < "$1" | grep "requests per second" | tail -1 \
        | sed -n 's/.*[^0-9]\([0-9][0-9]*\.[0-9]*\) requests per second.*/\1/p' || echo "0"
}

HOUR=0
ELAPSED=0
ERRORS=0

while [[ $ELAPSED -lt $DURATION ]]; do
    HOUR=$((HOUR + 1))
    CHUNK=$INTERVAL
    REMAINING=$((DURATION - ELAPSED))
    [[ $CHUNK -gt $REMAINING ]] && CHUNK=$REMAINING

    log "Hour $HOUR/$TOTAL_HOURS — running ${CHUNK}s interval..."

    GET_OUT="${DATA_DIR}/hour-${HOUR}-get.txt"
    SET_OUT="${DATA_DIR}/hour-${HOUR}-set.txt"

    # Run GET and SET benchmarks for this interval
    REQUESTS_PER_INTERVAL=$(( (CHUNK * CLIENTS * PIPELINE) / 2 ))
    [[ $REQUESTS_PER_INTERVAL -lt 100000 ]] && REQUESTS_PER_INTERVAL=100000

    redis-benchmark -p "$PORT" -t get -c "$CLIENTS" -P "$PIPELINE" \
        -n "$REQUESTS_PER_INTERVAL" -r 100000 -d 64 --csv 2>/dev/null > "$GET_OUT" || true

    redis-benchmark -p "$PORT" -t set -c "$CLIENTS" -P "$PIPELINE" \
        -n "$REQUESTS_PER_INTERVAL" -r 100000 -d 64 --csv 2>/dev/null > "$SET_OUT" || true

    # Parse results
    GET_RPS=$(parse_rps_from_csv "$GET_OUT")
    SET_RPS=$(parse_rps_from_csv "$SET_OUT")

    # Parse percentiles from --csv output (redis-benchmark prints percentile table)
    GET_P50=$(parse_percentile "$GET_OUT" "50.000")
    GET_P99=$(parse_percentile "$GET_OUT" "99.000")
    GET_P999=$(parse_percentile "$GET_OUT" "99.900")
    SET_P50=$(parse_percentile "$SET_OUT" "50.000")
    SET_P99=$(parse_percentile "$SET_OUT" "99.000")
    SET_P999=$(parse_percentile "$SET_OUT" "99.900")

    # Resource checks
    CURRENT_RSS=$(get_rss_kb "$EFFECTIVE_PID")
    CURRENT_FD=$(get_fd_count "$EFFECTIVE_PID")

    # Check for server health
    if ! redis-cli -p "$PORT" PING &>/dev/null; then
        log "ERROR: Moon not responding at hour $HOUR!"
        ERRORS=$((ERRORS + 1))
        echo "| $HOUR | **DEAD** | - | - | - | **DEAD** | - | - | - | - | - |" >> "$REPORT_FILE"
        break
    fi

    printf "| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n" \
        "$HOUR" "$GET_RPS" "$GET_P50" "$GET_P99" "$GET_P999" \
        "$SET_RPS" "$SET_P50" "$SET_P99" "$SET_P999" \
        "$CURRENT_RSS" "$CURRENT_FD" >> "$REPORT_FILE"

    log "  GET: ${GET_RPS} rps (p99=${GET_P99}ms) | SET: ${SET_RPS} rps (p99=${SET_P99}ms) | RSS: ${CURRENT_RSS}KB | FDs: ${CURRENT_FD}"

    ELAPSED=$((ELAPSED + CHUNK))
done

# ===========================================================================
# Summary & drift analysis
# ===========================================================================

FINAL_RSS=$(get_rss_kb "$EFFECTIVE_PID")
FINAL_FD=$(get_fd_count "$EFFECTIVE_PID")

# Compute drift
if [[ "$BASELINE_RSS" -gt 0 ]]; then
    RSS_DRIFT=$(( (FINAL_RSS - BASELINE_RSS) * 100 / BASELINE_RSS ))
else
    RSS_DRIFT=0
fi

FD_DELTA=$((FINAL_FD - BASELINE_FD))

cat >> "$REPORT_FILE" << EOF

## Summary

| Metric | Baseline | Final | Delta |
|--------|----------|-------|-------|
| RSS (KB) | $BASELINE_RSS | $FINAL_RSS | ${RSS_DRIFT}% |
| FDs | $BASELINE_FD | $FINAL_FD | ${FD_DELTA} |
| Errors | - | $ERRORS | - |

## Verdict

EOF

PASS=true

if [[ $RSS_DRIFT -gt 50 ]]; then
    echo "- **FAIL**: RSS drift ${RSS_DRIFT}% exceeds 50% threshold" >> "$REPORT_FILE"
    PASS=false
fi

if [[ $FD_DELTA -gt 100 ]]; then
    echo "- **FAIL**: FD leak detected (${FD_DELTA} new FDs)" >> "$REPORT_FILE"
    PASS=false
fi

if [[ $ERRORS -gt 0 ]]; then
    echo "- **FAIL**: Server died during benchmark ($ERRORS errors)" >> "$REPORT_FILE"
    PASS=false
fi

if [[ "$PASS" == true ]]; then
    echo "- **PASS**: 24h tail-latency benchmark completed cleanly" >> "$REPORT_FILE"
fi

log "Done. Report: $REPORT_FILE"
log "Verdict: $( [[ "$PASS" == true ]] && echo "PASS" || echo "FAIL" )"
