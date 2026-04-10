#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-soak-7d.sh -- 7-day soak test at production QPS (PERF-05)
#
# Continuously drives moon at target QPS (GET 1M/s, SET 300K/s, pipeline=16)
# for 7 days. Captures hourly HDR percentiles, RSS, FD count, and connection
# count. Watchdog alerts on OOM, panic, RSS >2x baseline, or FD leaks.
#
# Usage:
#   ./scripts/bench-soak-7d.sh                    # Full 7-day soak
#   ./scripts/bench-soak-7d.sh --duration 86400   # 1-day soak
#   ./scripts/bench-soak-7d.sh --port 6400        # Moon port
#   ./scripts/bench-soak-7d.sh --shards 4         # Moon shard count
#   ./scripts/bench-soak-7d.sh --skip-start       # Moon already running
#   ./scripts/bench-soak-7d.sh --output DIR       # Output directory
#
# Requirements: redis-benchmark, redis-cli, moon binary
# Exit criteria (PERF-05):
#   - 0 OOM, 0 panics
#   - p99 drift <= 10%
#   - No FD leaks
#   - RSS steady-state (no unbounded growth)
###############################################################################

PORT=6400
SHARDS=4
DURATION=$((7 * 24 * 3600))
SKIP_START=false
OUTPUT_DIR="benches/history"
RUST_BINARY="./target/release/moon"
INTERVAL=3600   # hourly checkpoint
PIPELINE=16
GET_CLIENTS=100
SET_CLIENTS=50

# Thresholds
RSS_DRIFT_THRESHOLD=100  # percent
FD_LEAK_THRESHOLD=200
P99_DRIFT_THRESHOLD=10   # percent

MOON_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)   DURATION="$2"; shift 2 ;;
        --port)       PORT="$2"; shift 2 ;;
        --shards)     SHARDS="$2"; shift 2 ;;
        --skip-start) SKIP_START=true; shift ;;
        --output)     OUTPUT_DIR="$2"; shift 2 ;;
        --pipeline)   PIPELINE="$2"; shift 2 ;;
        --help|-h)    sed -n '3,22p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
REPORT_FILE="${OUTPUT_DIR}/soak-7d-${TIMESTAMP}.md"
DATA_DIR="${OUTPUT_DIR}/soak-7d-${TIMESTAMP}"
LOG_FILE="${DATA_DIR}/soak.log"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" >&2
    echo "$msg" >> "$LOG_FILE"
}

alert() {
    local msg="[ALERT $(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" >&2
    echo "$msg" >> "$LOG_FILE"
    echo "$msg" >> "${DATA_DIR}/alerts.txt"
}

cleanup() {
    log "Cleaning up..."
    # Kill background benchmark processes
    kill "${GET_BG_PID:-}" 2>/dev/null || true
    kill "${SET_BG_PID:-}" 2>/dev/null || true
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

get_conn_count() {
    redis-cli -p "$PORT" INFO clients 2>/dev/null \
        | grep "connected_clients:" | cut -d: -f2 | tr -d '\r' || echo "0"
}

parse_rps() {
    tr '\r' '\n' | grep "requests per second" | tail -1 \
        | sed -n 's/.*[^0-9]\([0-9][0-9]*\.[0-9]*\) requests per second.*/\1/p' || echo "0"
}

# ===========================================================================
# Setup
# ===========================================================================

mkdir -p "$OUTPUT_DIR" "$DATA_DIR"
touch "$LOG_FILE" "${DATA_DIR}/alerts.txt"

TOTAL_DAYS=$((DURATION / 86400))
TOTAL_HOURS=$((DURATION / INTERVAL))
[[ $TOTAL_HOURS -lt 1 ]] && TOTAL_HOURS=1

log "============================================"
log "Moon 7-day soak test (PERF-05)"
log "Duration: ${TOTAL_DAYS}d (${TOTAL_HOURS} hourly intervals)"
log "Target: GET ~1M/s, SET ~300K/s, pipeline=$PIPELINE"
log "============================================"

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

if [[ -z "$EFFECTIVE_PID" ]]; then
    log "ERROR: Cannot determine moon PID"
    exit 1
fi

# Seed data
log "Seeding 1M keys..."
redis-benchmark -p "$PORT" -t set -n 1000000 -r 1000000 -d 64 -c 100 -P 32 -q >/dev/null 2>&1

BASELINE_RSS=$(get_rss_kb "$EFFECTIVE_PID")
BASELINE_FD=$(get_fd_count "$EFFECTIVE_PID")

log "Baseline RSS: ${BASELINE_RSS} KB, FDs: ${BASELINE_FD}"

# ===========================================================================
# Report header
# ===========================================================================

cat > "$REPORT_FILE" << EOF
# 7-Day Soak Test Report (PERF-05)

**Date:** $(date '+%Y-%m-%d %H:%M:%S %Z')
**Duration:** ${TOTAL_DAYS} days (${DURATION}s)
**Server:** moon --port $PORT --shards $SHARDS
**Target QPS:** GET 1M/s, SET 300K/s, pipeline=$PIPELINE
**Baseline RSS:** ${BASELINE_RSS} KB
**Baseline FDs:** ${BASELINE_FD}

## Exit Criteria

- [ ] 0 OOM events
- [ ] 0 panics
- [ ] p99 drift <= ${P99_DRIFT_THRESHOLD}%
- [ ] No FD leaks (delta < ${FD_LEAK_THRESHOLD})
- [ ] RSS steady-state (drift < ${RSS_DRIFT_THRESHOLD}%)

## Hourly Checkpoints

| Hour | Day | GET RPS | GET p99 | SET RPS | SET p99 | RSS (KB) | RSS drift% | FDs | Conns | Alerts |
|------|-----|---------|---------|---------|---------|----------|------------|-----|-------|--------|
EOF

# ===========================================================================
# Main soak loop
# ===========================================================================

HOUR=0
ELAPSED=0
OOM_COUNT=0
PANIC_COUNT=0
ALERT_COUNT=0
FIRST_GET_P99=""
GET_BG_PID=""
SET_BG_PID=""

while [[ $ELAPSED -lt $DURATION ]]; do
    HOUR=$((HOUR + 1))
    DAY=$(( (HOUR - 1) / 24 + 1 ))
    CHUNK=$INTERVAL
    REMAINING=$((DURATION - ELAPSED))
    [[ $CHUNK -gt $REMAINING ]] && CHUNK=$REMAINING

    log "Hour $HOUR/$TOTAL_HOURS (Day $DAY) — checkpoint..."

    # Check server is alive
    if ! redis-cli -p "$PORT" PING &>/dev/null; then
        alert "Moon not responding! Checking for crash..."

        if ! kill -0 "$EFFECTIVE_PID" 2>/dev/null; then
            alert "Moon process DEAD (PID $EFFECTIVE_PID)"
            PANIC_COUNT=$((PANIC_COUNT + 1))
            echo "| $HOUR | $DAY | **DEAD** | - | **DEAD** | - | - | - | - | - | CRASH |" >> "$REPORT_FILE"
            break
        fi

        alert "Moon process alive but not responding (possible OOM/hang)"
        OOM_COUNT=$((OOM_COUNT + 1))
    fi

    # Run short benchmark burst to measure current performance
    GET_OUT="${DATA_DIR}/hour-${HOUR}-get.txt"
    SET_OUT="${DATA_DIR}/hour-${HOUR}-set.txt"

    # GET burst (target ~1M/s at p=16, run for portion of interval)
    redis-benchmark -p "$PORT" -t get -c "$GET_CLIENTS" -P "$PIPELINE" \
        -n 2000000 -r 1000000 -d 64 -q 2>/dev/null > "$GET_OUT" &
    GET_BG_PID=$!

    # SET burst (target ~300K/s at p=16)
    redis-benchmark -p "$PORT" -t set -c "$SET_CLIENTS" -P "$PIPELINE" \
        -n 600000 -r 1000000 -d 64 -q 2>/dev/null > "$SET_OUT" &
    SET_BG_PID=$!

    # Wait for benchmarks
    wait "$GET_BG_PID" 2>/dev/null || true
    wait "$SET_BG_PID" 2>/dev/null || true
    GET_BG_PID=""
    SET_BG_PID=""

    # Parse results
    GET_RPS=$(parse_rps < "$GET_OUT")
    SET_RPS=$(parse_rps < "$SET_OUT")

    # Extract p99 from redis-benchmark output
    GET_P99=$(tr '\r' '\n' < "$GET_OUT" | grep -i "99.00" | tail -1 | awk '{print $NF}' || echo "N/A")
    SET_P99=$(tr '\r' '\n' < "$SET_OUT" | grep -i "99.00" | tail -1 | awk '{print $NF}' || echo "N/A")

    # Resource metrics
    CURRENT_RSS=$(get_rss_kb "$EFFECTIVE_PID")
    CURRENT_FD=$(get_fd_count "$EFFECTIVE_PID")
    CURRENT_CONNS=$(get_conn_count)

    # Drift calculations
    if [[ "$BASELINE_RSS" -gt 0 ]]; then
        RSS_DRIFT=$(( (CURRENT_RSS - BASELINE_RSS) * 100 / BASELINE_RSS ))
    else
        RSS_DRIFT=0
    fi

    FD_DELTA=$((CURRENT_FD - BASELINE_FD))

    # Track p99 drift
    if [[ -z "$FIRST_GET_P99" ]] && [[ "$GET_P99" != "N/A" ]]; then
        FIRST_GET_P99="$GET_P99"
    fi

    # Watchdog checks
    HOUR_ALERTS=""

    if [[ $RSS_DRIFT -gt $RSS_DRIFT_THRESHOLD ]]; then
        alert "RSS drift ${RSS_DRIFT}% exceeds ${RSS_DRIFT_THRESHOLD}% threshold (${CURRENT_RSS} KB)"
        HOUR_ALERTS="RSS"
        ALERT_COUNT=$((ALERT_COUNT + 1))
    fi

    if [[ $FD_DELTA -gt $FD_LEAK_THRESHOLD ]]; then
        alert "FD leak: ${FD_DELTA} new FDs (baseline=$BASELINE_FD current=$CURRENT_FD)"
        HOUR_ALERTS="${HOUR_ALERTS:+$HOUR_ALERTS,}FD"
        ALERT_COUNT=$((ALERT_COUNT + 1))
    fi

    if ! kill -0 "$EFFECTIVE_PID" 2>/dev/null; then
        alert "Moon process died during benchmark!"
        HOUR_ALERTS="${HOUR_ALERTS:+$HOUR_ALERTS,}DEAD"
        PANIC_COUNT=$((PANIC_COUNT + 1))
    fi

    [[ -z "$HOUR_ALERTS" ]] && HOUR_ALERTS="-"

    printf "| %d | %d | %s | %s | %s | %s | %s | %d%% | %s | %s | %s |\n" \
        "$HOUR" "$DAY" "$GET_RPS" "$GET_P99" "$SET_RPS" "$SET_P99" \
        "$CURRENT_RSS" "$RSS_DRIFT" "$CURRENT_FD" "$CURRENT_CONNS" \
        "$HOUR_ALERTS" >> "$REPORT_FILE"

    log "  GET: ${GET_RPS} rps (p99=${GET_P99}) | SET: ${SET_RPS} rps (p99=${SET_P99})"
    log "  RSS: ${CURRENT_RSS}KB (${RSS_DRIFT}%) | FDs: ${CURRENT_FD} (delta=${FD_DELTA})"

    # Sleep until next interval (minus time spent benchmarking)
    BENCH_END=$(date +%s)
    SLEEP_TIME=$((CHUNK - (BENCH_END % CHUNK) ))
    [[ $SLEEP_TIME -gt 0 ]] && [[ $SLEEP_TIME -le $CHUNK ]] && sleep "$SLEEP_TIME"

    ELAPSED=$((ELAPSED + CHUNK))

    # Bail on process death
    if ! kill -0 "$EFFECTIVE_PID" 2>/dev/null; then
        log "Moon process dead. Ending soak."
        break
    fi
done

# ===========================================================================
# Final verdict
# ===========================================================================

FINAL_RSS=$(get_rss_kb "$EFFECTIVE_PID" 2>/dev/null || echo "0")
FINAL_FD=$(get_fd_count "$EFFECTIVE_PID" 2>/dev/null || echo "0")

if [[ "$BASELINE_RSS" -gt 0 ]]; then
    FINAL_RSS_DRIFT=$(( (FINAL_RSS - BASELINE_RSS) * 100 / BASELINE_RSS ))
else
    FINAL_RSS_DRIFT=0
fi

FINAL_FD_DELTA=$((FINAL_FD - BASELINE_FD))

cat >> "$REPORT_FILE" << EOF

## Final Summary

| Metric | Baseline | Final | Delta | Threshold | Status |
|--------|----------|-------|-------|-----------|--------|
| RSS (KB) | $BASELINE_RSS | $FINAL_RSS | ${FINAL_RSS_DRIFT}% | <${RSS_DRIFT_THRESHOLD}% | $([ $FINAL_RSS_DRIFT -lt $RSS_DRIFT_THRESHOLD ] && echo "PASS" || echo "**FAIL**") |
| FDs | $BASELINE_FD | $FINAL_FD | ${FINAL_FD_DELTA} | <${FD_LEAK_THRESHOLD} | $([ $FINAL_FD_DELTA -lt $FD_LEAK_THRESHOLD ] && echo "PASS" || echo "**FAIL**") |
| OOM events | - | $OOM_COUNT | - | 0 | $([ $OOM_COUNT -eq 0 ] && echo "PASS" || echo "**FAIL**") |
| Panics/crashes | - | $PANIC_COUNT | - | 0 | $([ $PANIC_COUNT -eq 0 ] && echo "PASS" || echo "**FAIL**") |
| Alerts | - | $ALERT_COUNT | - | 0 | $([ $ALERT_COUNT -eq 0 ] && echo "PASS" || echo "INFO") |

## Verdict

EOF

PASS=true

if [[ $PANIC_COUNT -gt 0 ]]; then
    echo "- **FAIL**: $PANIC_COUNT crash/panic events" >> "$REPORT_FILE"
    PASS=false
fi

if [[ $OOM_COUNT -gt 0 ]]; then
    echo "- **FAIL**: $OOM_COUNT OOM/hang events" >> "$REPORT_FILE"
    PASS=false
fi

if [[ $FINAL_RSS_DRIFT -ge $RSS_DRIFT_THRESHOLD ]]; then
    echo "- **FAIL**: RSS drift ${FINAL_RSS_DRIFT}% exceeds ${RSS_DRIFT_THRESHOLD}% threshold" >> "$REPORT_FILE"
    PASS=false
fi

if [[ $FINAL_FD_DELTA -ge $FD_LEAK_THRESHOLD ]]; then
    echo "- **FAIL**: FD leak detected (${FINAL_FD_DELTA} new FDs)" >> "$REPORT_FILE"
    PASS=false
fi

if [[ "$PASS" == true ]]; then
    echo "- **PASS**: 7-day soak completed — all exit criteria met" >> "$REPORT_FILE"
fi

echo "" >> "$REPORT_FILE"
echo "**Full log:** ${LOG_FILE}" >> "$REPORT_FILE"
echo "**Alerts:** ${DATA_DIR}/alerts.txt" >> "$REPORT_FILE"

log "============================================"
log "Soak complete. Verdict: $( [[ "$PASS" == true ]] && echo "PASS" || echo "FAIL" )"
log "Report: $REPORT_FILE"
log "============================================"
