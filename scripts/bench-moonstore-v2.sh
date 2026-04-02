#!/usr/bin/env bash
# =============================================================================
# MoonStore v2 Comprehensive Benchmark
# =============================================================================
#
# Tests ALL MoonStore v2 capabilities with real MiniLM embeddings:
#
#   Part 1: KV Persistence (WAL v3 vs WAL v2, disk-offload on/off)
#   Part 2: Vector Search (Moon vs Redis 8.x vs Qdrant) with MiniLM-384d
#   Part 3: Warm Tier (HOT->WARM transition, mmap search quality)
#   Part 4: Crash Recovery (kill -9, measure recovery time + data integrity)
#   Part 5: Memory Efficiency (per-key overhead comparison)
#
# Usage:
#   ./scripts/bench-moonstore-v2.sh                    # Full (10K vectors)
#   ./scripts/bench-moonstore-v2.sh 50000              # 50K vectors
#   ./scripts/bench-moonstore-v2.sh 10000 quick        # Skip Qdrant
#
# Prerequisites:
#   - redis-server 8.x (redis-cli, redis-benchmark)
#   - Docker (for Qdrant, unless "quick" mode)
#   - Python3 with: numpy, redis, sentence-transformers, qdrant-client, requests

set -euo pipefail

N_VECTORS="${1:-10000}"
MODE="${2:-full}"  # "full" or "quick"
K=10
EF=200
N_QUERIES=200
DIM=384  # MiniLM-L6-v2

MOON_PORT=16379
REDIS_PORT=16400
QDRANT_PORT=16333
MOON_BIN="target/release/moon"

RESULTS_DIR="target/moonstore-v2-bench"
DATA_DIR="target/moonstore-v2-data"
REPORT=".planning/MOONSTORE-V2-BENCHMARK-REPORT.md"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

mkdir -p "$RESULTS_DIR" "$DATA_DIR"

# ── Pids for cleanup ────────────────────────────────────────────────────
MOON_PID=""
MOON2_PID=""
REDIS_PID=""

cleanup() {
    echo ""
    echo ">>> Cleaning up..."
    [ -n "$MOON_PID" ]  && kill "$MOON_PID"  2>/dev/null && wait "$MOON_PID"  2>/dev/null || true
    [ -n "$MOON2_PID" ] && kill "$MOON2_PID" 2>/dev/null && wait "$MOON2_PID" 2>/dev/null || true
    [ -n "$REDIS_PID" ] && kill "$REDIS_PID" 2>/dev/null && wait "$REDIS_PID" 2>/dev/null || true
    docker rm -f qdrant-bench 2>/dev/null || true
    echo ">>> Done."
}
trap cleanup EXIT

# ── System info ──────────────────────────────────────────────────────────
if [[ "$(uname)" == "Darwin" ]]; then
    HW_CPU=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
    HW_CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "?")
    HW_MEM=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 ))
else
    HW_CPU=$(lscpu 2>/dev/null | grep "Model name" | cut -d: -f2 | xargs || echo "unknown")
    HW_CORES=$(nproc 2>/dev/null || echo "?")
    HW_MEM=$(( $(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 0) / 1024 / 1024 ))
fi

echo "================================================================="
echo " MoonStore v2 — Comprehensive Benchmark"
echo "================================================================="
echo " Vectors: $N_VECTORS | Dim: $DIM (MiniLM) | K: $K | ef: $EF"
echo " CPU: $HW_CPU | Cores: $HW_CORES | RAM: ${HW_MEM}GB"
echo " Mode: $MODE"
echo "================================================================="

# ── Build Moon release ───────────────────────────────────────────────────
echo ""
echo ">>> Building Moon (release, target-cpu=native)..."
RUSTFLAGS="-C target-cpu=native" cargo build --release \
    --no-default-features --features runtime-tokio,jemalloc 2>&1 | tail -3

# ── Generate MiniLM embeddings ───────────────────────────────────────────
echo ""
echo ">>> Generating $N_VECTORS MiniLM-L6-v2 embeddings (${DIM}d)..."

python3 "$SCRIPT_DIR/bench-moonstore-v2-generate.py" \
    --vectors "$N_VECTORS" --queries "$N_QUERIES" --dim "$DIM" \
    --output "$DATA_DIR"

echo "  Data ready in $DATA_DIR/"

# ── Part 1: KV Persistence Benchmark ────────────────────────────────────
echo ""
echo "================================================================="
echo " Part 1: KV Persistence (WAL v3 disk-offload vs default)"
echo "================================================================="

python3 "$SCRIPT_DIR/bench-moonstore-v2-kv.py" \
    --moon-bin "$MOON_BIN" \
    --port "$MOON_PORT" \
    --keys 100000 --pipeline 16 \
    --output "$RESULTS_DIR/kv.json"

# ── Part 2: Vector Search — Moon vs Redis vs Qdrant ─────────────────────
echo ""
echo "================================================================="
echo " Part 2: Vector Search (Moon vs Redis 8.x vs Qdrant)"
echo "================================================================="

python3 "$SCRIPT_DIR/bench-moonstore-v2-vector.py" \
    --moon-bin "$MOON_BIN" \
    --data-dir "$DATA_DIR" \
    --moon-port "$MOON_PORT" \
    --redis-port "$REDIS_PORT" \
    --qdrant-port "$QDRANT_PORT" \
    --k "$K" --ef "$EF" \
    --mode "$MODE" \
    --output "$RESULTS_DIR/vector.json"

# ── Part 3: Warm Tier ───────────────────────────────────────────────────
echo ""
echo "================================================================="
echo " Part 3: Warm Tier (HOT->WARM transition + mmap search)"
echo "================================================================="

python3 "$SCRIPT_DIR/bench-moonstore-v2-warm.py" \
    --moon-bin "$MOON_BIN" \
    --data-dir "$DATA_DIR" \
    --port "$MOON_PORT" \
    --output "$RESULTS_DIR/warm.json"

# ── Part 4: Crash Recovery ──────────────────────────────────────────────
echo ""
echo "================================================================="
echo " Part 4: Crash Recovery (kill -9, measure recovery)"
echo "================================================================="

python3 "$SCRIPT_DIR/bench-moonstore-v2-recovery.py" \
    --moon-bin "$MOON_BIN" \
    --port "$MOON_PORT" \
    --keys 50000 \
    --output "$RESULTS_DIR/recovery.json"

# ── Part 5: Generate Report ─────────────────────────────────────────────
echo ""
echo "================================================================="
echo " Generating Report"
echo "================================================================="

python3 "$SCRIPT_DIR/bench-moonstore-v2-report.py" \
    --results-dir "$RESULTS_DIR" \
    --output "$REPORT" \
    --hw-cpu "$HW_CPU" \
    --hw-cores "$HW_CORES" \
    --hw-mem "${HW_MEM}GB" \
    --vectors "$N_VECTORS" \
    --dim "$DIM"

echo ""
echo "================================================================="
echo " BENCHMARK COMPLETE"
echo "================================================================="
echo " Report: $REPORT"
echo " Raw data: $RESULTS_DIR/"
echo "================================================================="
