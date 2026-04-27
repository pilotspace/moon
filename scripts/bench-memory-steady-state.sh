#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-memory-steady-state.sh -- Memory regression gate for CI
#
# Boots a Moon server, populates a fixed workload (1M string keys, 10K vectors,
# 100 graph nodes), waits for steady state, captures per-kind memory from both
# MEMORY DOCTOR and Prometheus /metrics, compares against a committed baseline.
#
# Exits 0 when every kind and RSS are within +/-5% of baseline.
# Exits 1 when ANY kind grows >5% (prints the offending kind + delta).
# Exits 2 when --self-test detects that the gate itself is broken.
#
# Usage:
#   bash scripts/bench-memory-steady-state.sh                 # Compare vs baseline
#   bash scripts/bench-memory-steady-state.sh --self-test     # Run bench + injection test
#   bash scripts/bench-memory-steady-state.sh --write-baseline tests/fixtures/memory-baseline.json
#   bash scripts/bench-memory-steady-state.sh --threshold 10  # Custom tolerance %
#   bash scripts/bench-memory-steady-state.sh --help
#
# Requirements: redis-cli, redis-benchmark, jq, curl on PATH.
# Assumes Moon binary at ./target/debug/moon (built with runtime-tokio,jemalloc,graph,text-index).
###############################################################################

BASELINE_PATH="tests/fixtures/memory-baseline.json"
THRESHOLD=5
SELF_TEST=false
WRITE_BASELINE=""
SKIP_BUILD=false

PORT=6391
ADMIN_PORT=9091
SHARDS=1
SERVER_PID=""
MOON_BINARY="./target/debug/moon"

# Number of string keys (redis-benchmark)
NUM_STRINGS=1000000
# Number of vector docs
NUM_VECTORS=10000
# Vector dimension
VEC_DIM=16
# Number of graph nodes
NUM_GRAPH_NODES=100
# Steady-state wait (seconds)
STEADY_STATE_WAIT=60

while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline)        BASELINE_PATH="$2"; shift 2 ;;
        --threshold)       THRESHOLD="$2"; shift 2 ;;
        --self-test)       SELF_TEST=true; shift ;;
        --write-baseline)  WRITE_BASELINE="$2"; shift 2 ;;
        --skip-build)      SKIP_BUILD=true; shift ;;
        --port)            PORT="$2"; shift 2 ;;
        --admin-port)      ADMIN_PORT="$2"; shift 2 ;;
        --help|-h)
            sed -n '3,24p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    # Safety: kill any lingering instance on our ports
    pkill -f "moon.*--port ${PORT}" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_server() {
    local max_wait=30
    for ((i=0; i<max_wait; i++)); do
        if redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 1
    done
    log "ERROR: Moon server did not become ready within ${max_wait}s"
    return 1
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
build_moon() {
    if [[ "$SKIP_BUILD" == true ]]; then
        log "Skipping build (--skip-build)"
        return 0
    fi
    log "Building Moon (debug, features: runtime-tokio,jemalloc,graph,text-index)..."
    cargo build --no-default-features --features runtime-tokio,jemalloc,graph,text-index 2>&1 | tail -5
}

# ---------------------------------------------------------------------------
# Start server
# ---------------------------------------------------------------------------
start_server() {
    log "Starting Moon on port $PORT, admin-port $ADMIN_PORT, shards $SHARDS..."
    pkill -f "moon.*--port ${PORT}" 2>/dev/null || true
    sleep 0.3

    MOON_NO_URING=1 "$MOON_BINARY" \
        --port "$PORT" \
        --admin-port "$ADMIN_PORT" \
        --shards "$SHARDS" \
        --disk-offload disable &>/dev/null &
    SERVER_PID=$!

    wait_for_server
    log "Server ready (PID=$SERVER_PID)"
}

# ---------------------------------------------------------------------------
# Populate workload
# ---------------------------------------------------------------------------
populate_workload() {
    log "Populating $NUM_STRINGS string keys via redis-benchmark..."
    redis-benchmark -p "$PORT" -t set -n "$NUM_STRINGS" -r "$NUM_STRINGS" \
        -c 50 -P 16 -q &>/dev/null

    log "Creating vector index (HNSW, dim=$VEC_DIM, FLOAT32, L2)..."
    redis-cli -p "$PORT" FT.CREATE memidx SCHEMA v VECTOR HNSW 6 \
        TYPE FLOAT32 DIM "$VEC_DIM" DISTANCE_METRIC L2 >/dev/null

    # Generate a fixed 16-dim float32 blob (64 bytes hex) for all vectors.
    # We care about memory accounting, not vector entropy.
    local blob
    blob=$(python3 -c "
import struct
vals = [float(i) / 100.0 for i in range($VEC_DIM)]
print(''.join(struct.pack('<f', v).hex() for v in vals))
")

    log "Populating $NUM_VECTORS vector docs via redis-cli pipe..."
    # Pipe commands into single redis-cli to avoid 10K forks.
    # Individual HSET (not pipelined) so auto-indexing fires per doc.
    { for i in $(seq 1 "$NUM_VECTORS"); do
        echo "HSET vec:$i v $blob"
    done; } | redis-cli -p "$PORT" --pipe-mode >/dev/null 2>&1 || \
    { for i in $(seq 1 "$NUM_VECTORS"); do
        echo "HSET vec:$i v $blob"
    done; } | redis-cli -p "$PORT" >/dev/null 2>&1

    log "Populating $NUM_GRAPH_NODES graph nodes..."
    { for i in $(seq 1 "$NUM_GRAPH_NODES"); do
        echo "GRAPH.QUERY memg \"CREATE (a:N {id:$i})\""
    done; } | redis-cli -p "$PORT" >/dev/null 2>&1

    log "Workload populated. Waiting ${STEADY_STATE_WAIT}s for steady state..."
    sleep "$STEADY_STATE_WAIT"
}

# ---------------------------------------------------------------------------
# Capture memory snapshot
# ---------------------------------------------------------------------------
capture_snapshot() {
    local doctor_file="/tmp/moon-doctor.txt"
    local metrics_file="/tmp/moon-metrics.txt"
    local rss_file="/tmp/moon-rss.txt"

    log "Capturing MEMORY DOCTOR..."
    redis-cli -p "$PORT" MEMORY DOCTOR > "$doctor_file"

    log "Capturing /metrics..."
    curl -s "http://127.0.0.1:${ADMIN_PORT}/metrics" | grep moon_memory_bytes > "$metrics_file" || true

    # Capture RSS
    local rss_bytes=0
    if [[ -f "/proc/${SERVER_PID}/statm" ]]; then
        # Linux: statm field 2 = resident pages
        rss_bytes=$(awk '{print $2 * 4096}' "/proc/${SERVER_PID}/statm")
    else
        # macOS fallback: ps reports RSS in KB
        local rss_kb
        rss_kb=$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' ')
        rss_bytes=$((rss_kb * 1024))
    fi
    echo "$rss_bytes" > "$rss_file"

    log "RSS = $rss_bytes bytes"

    # --- Parse MEMORY DOCTOR ---
    # Lines look like: "  DashTable + entries:    12345  (12.3%)"
    # Map display names to JSON keys
    local -A doctor_values
    doctor_values[dashtable]=$(grep -i "DashTable" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)
    doctor_values[hnsw]=$(grep -i "HNSW" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)
    doctor_values[csr]=$(grep -i "CSR" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)
    doctor_values[wal]=$(grep -i "WAL" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)
    doctor_values[sealed]=$(grep -i "Sealed" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)
    doctor_values[replication_backlog]=$(grep -i "Replication" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)
    doctor_values[allocator_overhead]=$(grep -i "Allocator" "$doctor_file" | grep -oE '[0-9]+ +\(' | grep -oE '[0-9]+' | head -1 || echo 0)

    # --- Parse Prometheus /metrics ---
    # Lines: moon_memory_bytes{kind="dashtable"} 1234.0
    local -A prom_values
    for kind in dashtable hnsw csr wal sealed replication_backlog allocator_overhead; do
        local val
        val=$(grep "kind=\"${kind}\"" "$metrics_file" | awk '{print $2}' | head -1 || echo "0")
        # Prometheus may emit float; truncate to integer
        prom_values[$kind]=$(python3 -c "print(int(float('${val:-0}')))")
    done

    # --- Build JSON ---
    local snapshot
    snapshot=$(jq -n \
        --argjson rss "$rss_bytes" \
        --argjson dt_d "${doctor_values[dashtable]:-0}" \
        --argjson dt_p "${prom_values[dashtable]:-0}" \
        --argjson hnsw_d "${doctor_values[hnsw]:-0}" \
        --argjson hnsw_p "${prom_values[hnsw]:-0}" \
        --argjson csr_d "${doctor_values[csr]:-0}" \
        --argjson csr_p "${prom_values[csr]:-0}" \
        --argjson wal_d "${doctor_values[wal]:-0}" \
        --argjson wal_p "${prom_values[wal]:-0}" \
        --argjson sealed_d "${doctor_values[sealed]:-0}" \
        --argjson sealed_p "${prom_values[sealed]:-0}" \
        --argjson rb_d "${doctor_values[replication_backlog]:-0}" \
        --argjson rb_p "${prom_values[replication_backlog]:-0}" \
        --argjson ao_d "${doctor_values[allocator_overhead]:-0}" \
        --argjson ao_p "${prom_values[allocator_overhead]:-0}" \
        '{
            rss: $rss,
            kinds: {
                dashtable:          { doctor: $dt_d,   prom: $dt_p },
                hnsw:               { doctor: $hnsw_d, prom: $hnsw_p },
                csr:                { doctor: $csr_d,  prom: $csr_p },
                wal:                { doctor: $wal_d,  prom: $wal_p },
                sealed:             { doctor: $sealed_d, prom: $sealed_p },
                replication_backlog:{ doctor: $rb_d,   prom: $rb_p },
                allocator_overhead: { doctor: $ao_d,   prom: $ao_p }
            }
        }')

    echo "$snapshot"
}

# ---------------------------------------------------------------------------
# Cross-reporter check (MEMORY DOCTOR vs Prometheus)
# ---------------------------------------------------------------------------
check_cross_reporter() {
    local snapshot="$1"
    local warnings=0

    log "Cross-reporter agreement check (MEMORY DOCTOR vs Prometheus, +/-2%)..."

    for kind in dashtable hnsw csr wal sealed replication_backlog allocator_overhead; do
        local doctor prom
        doctor=$(echo "$snapshot" | jq -r ".kinds.${kind}.doctor")
        prom=$(echo "$snapshot" | jq -r ".kinds.${kind}.prom")

        # Both zero = agree
        if [[ "$doctor" == "0" ]] && [[ "$prom" == "0" ]]; then
            continue
        fi

        # One zero, other not = warn
        if [[ "$doctor" == "0" ]] || [[ "$prom" == "0" ]]; then
            log "  WARN: ${kind} cross-reporter mismatch: doctor=$doctor prom=$prom (one is zero)"
            warnings=$((warnings + 1))
            continue
        fi

        local delta_pct
        delta_pct=$(python3 -c "
d = $doctor
p = $prom
if p == 0:
    print(999.0)
else:
    print(abs(d - p) / p * 100)
")
        local exceeds
        exceeds=$(python3 -c "print('yes' if $delta_pct > 2.0 else 'no')")
        if [[ "$exceeds" == "yes" ]]; then
            log "  WARN: ${kind} cross-reporter delta=${delta_pct}% (doctor=$doctor, prom=$prom)"
            warnings=$((warnings + 1))
        fi
    done

    if [[ "$warnings" -gt 0 ]]; then
        log "  $warnings cross-reporter warnings (non-fatal)"
    else
        log "  All kinds agree within +/-2%"
    fi
}

# ---------------------------------------------------------------------------
# Compare snapshot against baseline
# Returns 0 if all within threshold, 1 if any regression detected
# ---------------------------------------------------------------------------
compare_snapshot() {
    local snapshot="$1"
    local baseline_file="$2"
    local threshold="$3"
    local failures=0
    local failure_msgs=""

    if [[ ! -f "$baseline_file" ]]; then
        log "ERROR: Baseline file not found: $baseline_file"
        return 1
    fi

    local baseline
    baseline=$(cat "$baseline_file")

    log "Comparing against baseline (threshold: +/-${threshold}%)..."

    # Compare RSS
    local measured_rss baseline_rss
    measured_rss=$(echo "$snapshot" | jq -r '.rss')
    baseline_rss=$(echo "$baseline" | jq -r '.rss')

    if [[ "$baseline_rss" -gt 0 ]]; then
        local rss_delta_pct
        rss_delta_pct=$(python3 -c "
m = $measured_rss
b = $baseline_rss
print(round((m - b) / b * 100, 2))
")
        local rss_abs
        rss_abs=$(python3 -c "print(abs($rss_delta_pct))")
        local rss_exceeds
        rss_exceeds=$(python3 -c "print('yes' if $rss_abs > $threshold else 'no')")

        if [[ "$rss_exceeds" == "yes" ]]; then
            failure_msgs="${failure_msgs}  FAIL: rss delta=${rss_delta_pct}% (measured=$measured_rss, baseline=$baseline_rss)\n"
            failures=$((failures + 1))
        else
            log "  OK: rss delta=${rss_delta_pct}% (within +/-${threshold}%)"
        fi
    fi

    # Compare each kind (use prom value for comparison)
    for kind in dashtable hnsw csr wal sealed replication_backlog allocator_overhead; do
        local measured_val baseline_val
        measured_val=$(echo "$snapshot" | jq -r ".kinds.${kind}.prom")
        baseline_val=$(echo "$baseline" | jq -r ".kinds.${kind}.prom")

        # Handle baseline=0: if measured > 1024 bytes, flag as regression
        if [[ "$baseline_val" == "0" ]]; then
            if [[ "$measured_val" -gt 1024 ]]; then
                failure_msgs="${failure_msgs}  FAIL: ${kind} was 0 in baseline, now ${measured_val} bytes\n"
                failures=$((failures + 1))
            else
                log "  OK: ${kind} baseline=0, measured=$measured_val (below 1KB epsilon)"
            fi
            continue
        fi

        local delta_pct
        delta_pct=$(python3 -c "
m = $measured_val
b = $baseline_val
print(round((m - b) / b * 100, 2))
")
        local abs_delta
        abs_delta=$(python3 -c "print(abs($delta_pct))")
        local exceeds
        exceeds=$(python3 -c "print('yes' if $abs_delta > $threshold else 'no')")

        if [[ "$exceeds" == "yes" ]]; then
            failure_msgs="${failure_msgs}  FAIL: ${kind} delta=${delta_pct}% (measured=$measured_val, baseline=$baseline_val)\n"
            failures=$((failures + 1))
        else
            log "  OK: ${kind} delta=${delta_pct}% (within +/-${threshold}%)"
        fi
    done

    if [[ "$failures" -gt 0 ]]; then
        log ""
        log "=== MEMORY REGRESSION DETECTED ==="
        echo -e "$failure_msgs" >&2
        log "=== $failures kind(s) exceeded +/-${threshold}% threshold ==="
        return 1
    else
        log ""
        log "=== ALL KINDS WITHIN +/-${threshold}% === PASS ==="
        return 0
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    log "============================================"
    log "  Memory Steady-State Gate"
    log "============================================"

    # Check dependencies
    for cmd in redis-cli redis-benchmark jq curl python3; do
        command -v "$cmd" &>/dev/null || { log "ERROR: $cmd not found on PATH"; exit 1; }
    done

    build_moon

    if [[ ! -x "$MOON_BINARY" ]]; then
        log "ERROR: Moon binary not found at $MOON_BINARY"
        exit 1
    fi

    # --- Run the bench ONCE ---
    start_server
    populate_workload

    local snapshot
    snapshot=$(capture_snapshot)

    log "Captured snapshot:"
    echo "$snapshot" | jq . >&2

    # Cross-reporter check (warnings only, non-fatal)
    check_cross_reporter "$snapshot"

    # --- Write baseline mode ---
    if [[ -n "$WRITE_BASELINE" ]]; then
        log "Writing baseline to $WRITE_BASELINE"
        echo "$snapshot" | jq . > "$WRITE_BASELINE"
        log "Baseline written. Done."
        exit 0
    fi

    # --- Self-test mode: parse-once, compare-twice ---
    # Self-test: parse-once, compare-twice (with original + with +6% mutation)
    if [[ "$SELF_TEST" == true ]]; then
        log ""
        log "=== SELF-TEST: parse-once, compare-twice ==="
        log ""

        # Write snapshot to a temporary baseline for self-test comparison.
        # The self-test uses the JUST-CAPTURED snapshot as its own baseline,
        # so the first comparison (snapshot vs itself) should trivially pass.
        local tmp_baseline="/tmp/moon-selftest-baseline.json"
        echo "$snapshot" | jq . > "$tmp_baseline"

        # First comparison: original snapshot vs itself -> must pass
        log "Self-test step 1: comparing original snapshot vs self (expect PASS)..."
        if ! compare_snapshot "$snapshot" "$tmp_baseline" "$THRESHOLD"; then
            log "SELF-TEST FAILED: original snapshot does not match itself!"
            exit 2
        fi
        log "Self-test step 1: PASSED (original matches self)"

        # Mutate: inflate dashtable prom by +6%
        local mutated
        mutated=$(echo "$snapshot" | jq '.kinds.dashtable.prom = (.kinds.dashtable.prom * 1.06 | floor)')
        log "Self-test step 2: injected +6% into dashtable.prom"
        log "  original: $(echo "$snapshot" | jq '.kinds.dashtable.prom')"
        log "  mutated:  $(echo "$mutated" | jq '.kinds.dashtable.prom')"

        # Second comparison: mutated snapshot vs original baseline -> must FAIL
        log "Self-test step 2: comparing mutated snapshot vs baseline (expect FAIL)..."
        if compare_snapshot "$mutated" "$tmp_baseline" "$THRESHOLD"; then
            log ""
            log "SELF-TEST FAILED -- gate is broken: +6% injection was not detected!"
            exit 2
        fi
        log "Self-test step 2: PASSED (gate correctly detected +6% injection)"

        log ""
        log "=== SELF-TEST PASSED: gate is functional ==="
        rm -f "$tmp_baseline"
        exit 0
    fi

    # --- Normal mode: compare against committed baseline ---
    if compare_snapshot "$snapshot" "$BASELINE_PATH" "$THRESHOLD"; then
        exit 0
    else
        exit 1
    fi
}

main
