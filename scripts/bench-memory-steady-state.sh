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
        --disk-offload disable \
        --appendonly no \
        --protected-mode no &>/dev/null &
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
    redis-cli -p "$PORT" FT.CREATE memidx ON HASH PREFIX 1 vec: SCHEMA v VECTOR HNSW 6 \
        TYPE FLOAT32 DIM "$VEC_DIM" DISTANCE_METRIC L2 >/dev/null

    log "Populating $NUM_VECTORS vector docs + $NUM_GRAPH_NODES graph nodes via python3..."
    # Use python3 with raw socket to send proper binary vector data.
    # redis-cli cannot send raw binary blobs from stdin easily.
    # Individual HSET (not pipelined) so auto-indexing fires per doc.
    local py_script="/tmp/moon-bench-populate.py"
    cat > "$py_script" << 'PYEOF'
import socket, struct, sys

def send_resp(sock, parts):
    """Send a RESP command with mixed str/bytes args and read response."""
    msg = ("*%d\r\n" % len(parts)).encode()
    for p in parts:
        if isinstance(p, bytes):
            msg += ("$%d\r\n" % len(p)).encode() + p + b"\r\n"
        else:
            s = str(p)
            msg += ("$%d\r\n%s\r\n" % (len(s), s)).encode()
    sock.sendall(msg)
    resp = b""
    while b"\r\n" not in resp:
        resp += sock.recv(4096)

port = int(sys.argv[1])
num_vectors = int(sys.argv[2])
vec_dim = int(sys.argv[3])
num_graph = int(sys.argv[4])

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", port))
sock.settimeout(30)

# Insert vectors: float32 binary blobs
blob = struct.pack("<" + "f" * vec_dim, *[float(i) / 100.0 for i in range(vec_dim)])

for i in range(1, num_vectors + 1):
    send_resp(sock, ["HSET", "vec:%d" % i, "v", blob])
    if i % 2000 == 0:
        print("  vectors: %d/%d" % (i, num_vectors), file=sys.stderr)

# Insert graph nodes: GRAPH.CREATE first, then GRAPH.ADDNODE
send_resp(sock, ["GRAPH.CREATE", "memg"])
for i in range(1, num_graph + 1):
    send_resp(sock, ["GRAPH.ADDNODE", "memg", ":N"])

sock.close()
print("Done: %d vectors + %d graph nodes" % (num_vectors, num_graph), file=sys.stderr)
PYEOF
    python3 "$py_script" "$PORT" "$NUM_VECTORS" "$VEC_DIM" "$NUM_GRAPH_NODES" 2>&1 | \
        while read -r line; do log "  $line"; done

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
    # Lines look like: "  DashTable + entries:    24.74 KB  (0.3%)"
    # or "  WAL writers:            0 B  (0.0%)"
    # We parse the humanized byte value and convert back to bytes.
    parse_doctor_bytes() {
        local pattern="$1"
        local line
        line=$(grep -i "$pattern" "$doctor_file" | head -1 || echo "")
        if [[ -z "$line" ]]; then
            echo 0
            return
        fi
        # Extract the value + unit before the parenthesized percentage
        # Format: "  Label:    12.34 KB  (0.3%)" or "  Label:    0 B  (0.0%)"
        python3 -c "
import re
line = '''$line'''
# Match number (possibly float) followed by unit before '('
m = re.search(r'([\d.]+)\s+(B|KB|MB|GB|TB)\s+\(', line)
if not m:
    print(0)
else:
    val = float(m.group(1))
    unit = m.group(2)
    multipliers = {'B': 1, 'KB': 1024, 'MB': 1048576, 'GB': 1073741824, 'TB': 1099511627776}
    print(int(val * multipliers.get(unit, 1)))
"
    }

    local doc_dashtable doc_hnsw doc_csr doc_wal doc_sealed doc_repl doc_alloc
    doc_dashtable=$(parse_doctor_bytes "DashTable")
    doc_hnsw=$(parse_doctor_bytes "HNSW")
    doc_csr=$(parse_doctor_bytes "CSR")
    doc_wal=$(parse_doctor_bytes "WAL")
    doc_sealed=$(parse_doctor_bytes "Sealed")
    doc_repl=$(parse_doctor_bytes "Replication")
    doc_alloc=$(parse_doctor_bytes "Allocator overhead")

    # --- Parse Prometheus /metrics ---
    # Lines: moon_memory_bytes{kind="dashtable"} 1234.0
    parse_prom_bytes() {
        local kind="$1"
        local val
        val=$(grep "kind=\"${kind}\"" "$metrics_file" | awk '{print $2}' | head -1 || echo "0")
        python3 -c "print(int(float('${val:-0}')))"
    }

    local prom_dashtable prom_hnsw prom_csr prom_wal prom_sealed prom_repl prom_alloc
    prom_dashtable=$(parse_prom_bytes "dashtable")
    prom_hnsw=$(parse_prom_bytes "hnsw")
    prom_csr=$(parse_prom_bytes "csr")
    prom_wal=$(parse_prom_bytes "wal")
    prom_sealed=$(parse_prom_bytes "sealed")
    prom_repl=$(parse_prom_bytes "replication_backlog")
    prom_alloc=$(parse_prom_bytes "allocator_overhead")

    # --- Build JSON ---
    local snapshot
    snapshot=$(jq -n \
        --argjson rss "$rss_bytes" \
        --argjson dt_d "${doc_dashtable}" \
        --argjson dt_p "${prom_dashtable}" \
        --argjson hnsw_d "${doc_hnsw}" \
        --argjson hnsw_p "${prom_hnsw}" \
        --argjson csr_d "${doc_csr}" \
        --argjson csr_p "${prom_csr}" \
        --argjson wal_d "${doc_wal}" \
        --argjson wal_p "${prom_wal}" \
        --argjson sealed_d "${doc_sealed}" \
        --argjson sealed_p "${prom_sealed}" \
        --argjson rb_d "${doc_repl}" \
        --argjson rb_p "${prom_repl}" \
        --argjson ao_d "${doc_alloc}" \
        --argjson ao_p "${prom_alloc}" \
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
