#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-allocator-ab.sh -- A/B benchmark of jemalloc vs mimalloc allocators
#
# Phase 191 PERF-11: Side-by-side RPS comparison on identical 8-shard
# SET/GET p=64 workload. Run on Linux (OrbStack moon-dev or GCloud) for
# production-grade numbers.
#
# Usage:
#   ./scripts/bench-allocator-ab.sh           # full run (5M ops per command)
#   ./scripts/bench-allocator-ab.sh --quick   # quick run (500K ops)
#   ./scripts/bench-allocator-ab.sh --requests N  # custom request count
#   ./scripts/bench-allocator-ab.sh --shards N    # custom shard count (default 8)
#   ./scripts/bench-allocator-ab.sh --clients N   # custom client count (default 200)
###############################################################################

NREQ=5000000
SHARDS=8
CLIENTS=200
PIPELINE=64
PORT=6399

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)
            NREQ=500000; shift ;;
        --requests)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --requests requires a numeric value"; exit 1
            fi
            NREQ="$2"; shift 2 ;;
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
        *)
            echo "Unknown option: $1"; exit 1 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$REPO_ROOT/target/allocator-ab"
mkdir -p "$OUT_DIR"
LOG="$REPO_ROOT/tmp/allocator-ab-$(date +%Y%m%d-%H%M%S).txt"
mkdir -p "$REPO_ROOT/tmp"

MOON_PID=""

cleanup() {
    if [[ -n "$MOON_PID" ]]; then
        kill "$MOON_PID" 2>/dev/null || true
        wait "$MOON_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

build_variant() {
    local label="$1"
    local features="$2"
    local out="$OUT_DIR/moon-$label"
    echo ">>> Building moon-$label with features: $features" | tee -a "$LOG"
    ( cd "$REPO_ROOT" && cargo build --release --no-default-features --features "$features" )
    cp "$REPO_ROOT/target/release/moon" "$out"
    echo "    Binary: $out ($(du -h "$out" | awk '{print $1}'))" | tee -a "$LOG"
}

parse_rps() {
    # redis-benchmark 8.x uses \r for progress lines -- must convert before grep
    tr '\r' '\n' | grep "requests per second" | tail -1 | awk '{print $2}'
}

run_bench() {
    local binary="$1"
    local label="$2"
    echo "" | tee -a "$LOG"
    echo "=== $label (shards=$SHARDS, clients=$CLIENTS, pipeline=$PIPELINE, n=$NREQ) ===" | tee -a "$LOG"

    "$binary" --port $PORT --shards "$SHARDS" --disk-offload disable >/dev/null 2>&1 &
    MOON_PID=$!

    # Wait for server to become ready
    local ready=0
    for i in $(seq 1 50); do
        if redis-cli -p $PORT PING 2>/dev/null | grep -q PONG; then
            ready=1
            break
        fi
        sleep 0.1
    done
    if [[ $ready -eq 0 ]]; then
        echo "ERROR: moon-$label failed to start on port $PORT" | tee -a "$LOG"
        kill "$MOON_PID" 2>/dev/null || true
        wait "$MOON_PID" 2>/dev/null || true
        MOON_PID=""
        return 1
    fi

    for op in SET GET; do
        local rps
        rps=$(redis-benchmark -p $PORT -t "$op" -P "$PIPELINE" -c "$CLIENTS" -n "$NREQ" -q \
              | parse_rps)
        printf "  %-12s %-6s RPS=%s\n" "$label" "$op" "${rps:-N/A}" | tee -a "$LOG"
    done

    kill "$MOON_PID" 2>/dev/null || true
    wait "$MOON_PID" 2>/dev/null || true
    MOON_PID=""
    sleep 1
}

echo "============================================================" | tee "$LOG"
echo "Allocator A/B Benchmark" | tee -a "$LOG"
echo "  Date:     $(date -u +"%Y-%m-%d %H:%M:%S UTC")" | tee -a "$LOG"
echo "  Requests: $NREQ per command" | tee -a "$LOG"
echo "  Shards:   $SHARDS" | tee -a "$LOG"
echo "  Clients:  $CLIENTS" | tee -a "$LOG"
echo "  Pipeline: $PIPELINE" | tee -a "$LOG"
echo "============================================================" | tee -a "$LOG"

# Build both variants
build_variant jemalloc  "runtime-monoio,jemalloc,graph,text-index"
build_variant mimalloc  "runtime-monoio,mimalloc-alt,graph,text-index"

# Run benchmarks
run_bench "$OUT_DIR/moon-jemalloc" "jemalloc"
run_bench "$OUT_DIR/moon-mimalloc" "mimalloc"

echo "" | tee -a "$LOG"
echo "============================================================" | tee -a "$LOG"
echo "Done. Full log: $LOG" | tee -a "$LOG"
