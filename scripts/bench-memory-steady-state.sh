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
# Exits 2 when the gate CANNOT run: --self-test found the comparison broken,
#   or the committed baseline was captured on a different platform than the one
#   measuring now (comparing a Linux snapshot to a macOS baseline is noise, not
#   a regression signal).
#
# --self-test is a PHASE, not a mode: it proves the comparison can detect an
# injected regression, then falls through to the real baseline comparison. It
# used to `exit 0` right after the injection check, so CI ran the gate for
# months without ever comparing against the committed baseline (moon#764).
#
# Usage:
#   bash scripts/bench-memory-steady-state.sh                 # Compare vs baseline
#   bash scripts/bench-memory-steady-state.sh --self-test     # Self-test THEN compare
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
# Written unconditionally, every run, pass or fail, self-test or
# --write-baseline (moon#764 follow-up). Before this the only way to see
# what a run actually measured was to read the failure text out of a CI log
# -- CI now uploads this path as an artifact on every invocation.
SNAPSHOT_OUT_PATH="/tmp/moon-memory-snapshot.json"

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
    # Provenance: a memory baseline is only comparable to a snapshot taken on
    # the same platform. RSS, allocator behaviour and struct padding all differ
    # across OS and arch, so a cross-platform delta measures the runner, not the
    # code.
    #
    # os/arch alone is NOT enough (moon#764 follow-up): a GCE c3-standard-8
    # and a GitHub-hosted `ubuntu-latest` runner are both Linux/x86_64 and
    # still produced a 243% "allocator_overhead regression" that was purely
    # the machine, not the code (RSS +30.51%, allocator_overhead +243.22%,
    # while dashtable -- unaffected by machine class -- held at -0.14%).
    # cpu_count is the field that would have caught it: GCE c3-standard-8 is
    # 8 vCPU, GitHub's hosted `ubuntu-latest` is a fixed, documented, smaller
    # vCPU count. runner_environment/runner_name are recorded for a human
    # reading a failure to see AT A GLANCE which runner produced which
    # number; they are not gated on (RUNNER_NAME is a fresh random string
    # every single hosted run by design, so gating on it would make the gate
    # permanently exit 2).
    local prov_os prov_arch prov_cpu_model prov_cpu_count prov_mem_kb
    local prov_runner_env prov_runner_name
    prov_os=$(uname -s)
    prov_arch=$(uname -m)
    if [[ "$prov_os" == "Linux" ]]; then
        prov_cpu_model=$(grep -m1 '^model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')
        prov_cpu_count=$(nproc 2>/dev/null || echo 0)
        prov_mem_kb=$(grep -m1 '^MemTotal' /proc/meminfo 2>/dev/null | awk '{print $2}')
    elif [[ "$prov_os" == "Darwin" ]]; then
        prov_cpu_model=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)
        prov_cpu_count=$(sysctl -n hw.ncpu 2>/dev/null || echo 0)
        prov_mem_kb=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 ))
    fi
    prov_cpu_model="${prov_cpu_model:-unknown}"
    prov_cpu_count="${prov_cpu_count:-0}"
    prov_mem_kb="${prov_mem_kb:-0}"
    prov_runner_env="${RUNNER_ENVIRONMENT:-unknown}"
    prov_runner_name="${RUNNER_NAME:-unknown}"

    snapshot=$(jq -n \
        --arg prov_os "$prov_os" \
        --arg prov_arch "$prov_arch" \
        --arg prov_cpu_model "$prov_cpu_model" \
        --argjson prov_cpu_count "$prov_cpu_count" \
        --argjson prov_mem_kb "$prov_mem_kb" \
        --arg prov_runner_env "$prov_runner_env" \
        --arg prov_runner_name "$prov_runner_name" \
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
            platform: {
                os: $prov_os, arch: $prov_arch, profile: "debug",
                cpu_model: $prov_cpu_model, cpu_count: $prov_cpu_count,
                mem_total_kb: $prov_mem_kb,
                runner_environment: $prov_runner_env, runner_name: $prov_runner_name
            },
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
# Baseline provenance guard
# ---------------------------------------------------------------------------
# A memory baseline is only meaningful against the platform it was captured on.
# The committed fixture was taken on macOS aarch64 while this gate runs on
# ubuntu-latest, so every delta would have been runner noise rather than a code
# regression -- which nobody noticed, because --self-test exited before the
# comparison ever ran. Refuse to compare rather than emit a number that looks
# like a measurement and is not one.
#
# Returns 0 to proceed, 2 when the gate cannot legitimately run.
check_baseline_provenance() {
    local snapshot="$1"
    local baseline_file="$2"

    if [[ ! -f "$baseline_file" ]]; then
        log "GATE CANNOT RUN: baseline not found: $baseline_file"
        return 2
    fi

    local b_os b_arch m_os m_arch
    b_os=$(jq -r '.platform.os   // "MISSING"' "$baseline_file")
    b_arch=$(jq -r '.platform.arch // "MISSING"' "$baseline_file")
    m_os=$(echo "$snapshot" | jq -r '.platform.os')
    m_arch=$(echo "$snapshot" | jq -r '.platform.arch')

    if [[ "$b_os" == "MISSING" || "$b_arch" == "MISSING" ]]; then
        log "GATE CANNOT RUN: $baseline_file records no platform provenance."
        log "  It predates the provenance field, so there is no way to tell"
        log "  which OS/arch it was captured on. Regenerate it ON THE PLATFORM"
        log "  THIS GATE RUNS ON (currently ${m_os}/${m_arch}):"
        log "    bash scripts/bench-memory-steady-state.sh --write-baseline $baseline_file"
        return 2
    fi

    if [[ "$b_os" != "$m_os" || "$b_arch" != "$m_arch" ]]; then
        log "GATE CANNOT RUN: baseline/runner platform mismatch."
        log "  baseline: ${b_os}/${b_arch}"
        log "  measured: ${m_os}/${m_arch}"
        log "  Cross-platform memory deltas measure the runner, not the code."
        log "  Regenerate on ${m_os}/${m_arch}:"
        log "    bash scripts/bench-memory-steady-state.sh --write-baseline $baseline_file"
        return 2
    fi

    # os/arch alone is NOT enough (moon#764 follow-up): a GCE c3-standard-8
    # and GitHub's hosted `ubuntu-latest` are both Linux/x86_64 and still
    # produced a 243% "allocator_overhead regression" that was purely the
    # machine (RSS +30.51%, allocator_overhead +243.22%), not the code
    # (dashtable, unaffected by machine class, held at -0.14% on that same
    # run). cpu_count is the strongest, most stable signal available: GitHub
    # documents a fixed vCPU count per hosted runner label, so gating on it
    # will not flap red across legitimate ubuntu-latest reruns the way
    # gating on the ephemeral, always-different RUNNER_NAME would.
    local b_cpu_count m_cpu_count b_cpu_model m_cpu_model
    local b_runner_env b_runner_name m_runner_env m_runner_name
    b_cpu_count=$(jq -r '.platform.cpu_count // "MISSING"' "$baseline_file")
    m_cpu_count=$(echo "$snapshot" | jq -r '.platform.cpu_count // "MISSING"')
    b_cpu_model=$(jq -r '.platform.cpu_model // "MISSING"' "$baseline_file")
    m_cpu_model=$(echo "$snapshot" | jq -r '.platform.cpu_model // "MISSING"')
    b_runner_env=$(jq -r '.platform.runner_environment // "unknown"' "$baseline_file")
    b_runner_name=$(jq -r '.platform.runner_name // "unknown"' "$baseline_file")
    m_runner_env=$(echo "$snapshot" | jq -r '.platform.runner_environment // "unknown"')
    m_runner_name=$(echo "$snapshot" | jq -r '.platform.runner_name // "unknown"')

    if [[ "$b_cpu_count" == "MISSING" ]]; then
        log "GATE CANNOT RUN: $baseline_file has os/arch but no cpu_count."
        log "  It predates the machine-class provenance check (moon#764"
        log "  follow-up), so an os/arch match alone cannot prove it came"
        log "  from a comparable machine -- that gap is exactly what let a"
        log "  GCE box silently pass as a stand-in for ubuntu-latest once."
        log "  Regenerate on the runner this gate actually runs on:"
        log "    bash scripts/bench-memory-steady-state.sh --write-baseline $baseline_file"
        return 2
    fi

    if [[ "$b_cpu_count" != "$m_cpu_count" ]]; then
        log "GATE CANNOT RUN: baseline/runner machine-class mismatch (cpu_count)."
        log "  baseline: cpu_count=${b_cpu_count} cpu_model=${b_cpu_model} runner=${b_runner_env}/${b_runner_name}"
        log "  measured: cpu_count=${m_cpu_count} cpu_model=${m_cpu_model} runner=${m_runner_env}/${m_runner_name}"
        log "  Same os/arch, different machine class -- RSS and allocator"
        log "  overhead do not transfer (this is the exact failure mode that"
        log "  motivated this check: a GCE c3-standard-8 baseline compared"
        log "  against ubuntu-latest read +243% allocator_overhead)."
        log "  Regenerate on this machine class:"
        log "    bash scripts/bench-memory-steady-state.sh --write-baseline $baseline_file"
        return 2
    fi

    # cpu_model is logged and compared, but only WARNED on, not gated: GitHub
    # does not document (or guarantee stability of) the exact CPU SKU behind
    # a hosted runner label the way it documents vCPU count, so hard-failing
    # here risked the opposite failure mode -- a gate that goes permanently
    # red because the hosted fleet legitimately rotated silicon under an
    # unchanged runner label, which is not a code regression either.
    if [[ "$b_cpu_model" != "MISSING" && "$m_cpu_model" != "MISSING" && "$b_cpu_model" != "$m_cpu_model" ]]; then
        log "WARN: cpu_model differs (same cpu_count, comparing anyway):"
        log "  baseline: ${b_cpu_model}"
        log "  measured: ${m_cpu_model}"
    fi

    log "Baseline provenance OK: ${b_os}/${b_arch}, cpu_count=${m_cpu_count} (baseline runner=${b_runner_env}/${b_runner_name}, this runner=${m_runner_env}/${m_runner_name})"
    return 0
}

# ---------------------------------------------------------------------------
# Per-kind noise floor (moon#764 follow-up)
# ---------------------------------------------------------------------------
# `hnsw` and `allocator_overhead` are not measured directly -- `hnsw`'s prom
# value tracks a growable mutable buffer whose realized jemalloc size class
# depends on concurrent insertion ordering (the doctor-computed estimate for
# the same kind is bit-identical run over run; only the real allocation
# isn't), and `allocator_overhead` is `max(0, RSS - sum(other 6))`, a residual
# that inherits every other kind's noise plus RSS's own page-level jitter.
#
# Measured on moon-bench-x86 (GCE c3-standard-8, Ubuntu 24.04.4,
# Linux 6.17.0-1022-gcp, x86_64, debug build, idle host, 10 back-to-back
# real runs of this exact workload, 2026-09-08): dashtable/rss/csr held
# under 2% every time; hnsw swung -13.55%..+15.68% and allocator_overhead
# swung -18.98%..+9.15%, purely from run-to-run noise with ZERO code change
# between runs. A flat +/-5% would make this gate report a regression on
# these two kinds roughly every other real run -- a false-positive rate
# that trains reviewers to click "re-run" without reading the failure,
# which is a different route to the same outcome #764 was filed over: a
# gate nobody trusts. See tmp/perf-campaign/FIX-764.md for the raw
# transcripts this floor is derived from.
#
# The floor sits comfortably above the measured noise ceiling (~16% / ~19%)
# so a real regression several times the noise floor is still caught; it
# does NOT touch the other 5 kinds or RSS, which stayed noise-free.
kind_threshold() {
    local kind="$1"
    local base="$2"
    local floor=0
    case "$kind" in
        hnsw)                floor=20 ;;
        allocator_overhead)  floor=25 ;;
        *)                   floor=0  ;;
    esac
    python3 -c "print($base if $base > $floor else $floor)"
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

    log "Comparing against baseline (threshold: +/-${threshold}%, wider floor for hnsw/allocator_overhead -- see kind_threshold())..."

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
        local measured_val baseline_val kind_thr
        measured_val=$(echo "$snapshot" | jq -r ".kinds.${kind}.prom")
        baseline_val=$(echo "$baseline" | jq -r ".kinds.${kind}.prom")
        kind_thr=$(kind_threshold "$kind" "$threshold")

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
        exceeds=$(python3 -c "print('yes' if $abs_delta > $kind_thr else 'no')")

        if [[ "$exceeds" == "yes" ]]; then
            failure_msgs="${failure_msgs}  FAIL: ${kind} delta=${delta_pct}% (measured=$measured_val, baseline=$baseline_val, threshold=+/-${kind_thr}%)\n"
            failures=$((failures + 1))
        else
            log "  OK: ${kind} delta=${delta_pct}% (within +/-${kind_thr}%)"
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

    # Always written, regardless of mode or outcome, so CI can upload it as
    # an artifact unconditionally -- pass, fail, self-test, or
    # --write-baseline all produce the same measured-this-run record.
    echo "$snapshot" | jq . > "$SNAPSHOT_OUT_PATH"
    log "Snapshot written to $SNAPSHOT_OUT_PATH"

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
        # NO exit here. The self-test only proves the comparison WORKS; it
        # compares the snapshot against itself, which can never fail for a real
        # regression. Falling through to the committed-baseline comparison
        # below is the part that actually gates. This `exit 0` used to sit
        # right here and was the bug that made the job vacuous (moon#764):
        # every PR "passed" a check that had never read the committed baseline.
        log ""
    fi

    # --- Compare against the committed baseline ---
    # Provenance first: an incomparable baseline means the gate cannot run
    # (exit 2), which is a different failure mode from "compared cleanly"
    # (exit 0) or "a kind regressed" (exit 1) -- silently passing a
    # cross-platform comparison would just replace one vacuous gate with
    # another.
    local prov_rc=0
    check_baseline_provenance "$snapshot" "$BASELINE_PATH" || prov_rc=$?
    if [[ "$prov_rc" -ne 0 ]]; then
        exit "$prov_rc"
    fi

    if compare_snapshot "$snapshot" "$BASELINE_PATH" "$THRESHOLD"; then
        exit 0
    else
        exit 1
    fi
}

main
